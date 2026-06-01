import 'dart:async';
import 'dart:convert';
import 'dart:math';

import 'package:broadcast_queue_shim_for_ndk/broadcast_queue_shim_for_ndk.dart';
import 'package:ndk/ndk.dart';
import 'package:sembast/sembast.dart' as sembast;

import 'models/job_status.dart';
import 'models/scheduled_job.dart';
import 'models/status_update.dart';
import 'models/sync_state.dart';
import 'scheduler_store.dart';

/// Local-first scheduler for Nostr events via Scheduler DVMs.
class EventScheduler {
  final Ndk _ndk;
  final OfflineBroadcast _broadcast;
  final SchedulerStore _store;

  bool _listening = false;
  final List<NdkResponse> _syncResponses = [];
  final List<StreamSubscription<Nip01Event>> _syncSubscriptions = [];
  String? _feedbackRequestId;
  StreamSubscription<Nip01Event>? _feedbackSubscription;
  Timer? _feedbackUpdateTimer;

  final _statusController = StreamController<StatusUpdate>.broadcast();
  final _syncController = StreamController<SyncState>.broadcast();

  /// Creates a new [EventScheduler].
  ///
  /// The caller must provide a started [OfflineBroadcast] instance.
  EventScheduler({
    required this._ndk,
    required this._broadcast,
    required sembast.Database db,
  }) : _store = SchedulerStore(db);

  // --------------------------------------------------------------------------
  // Streams
  // --------------------------------------------------------------------------

  Stream<StatusUpdate> get statusUpdates => _statusController.stream;
  Stream<SyncState> get syncState => _syncController.stream;

  // --------------------------------------------------------------------------
  // Network control
  // --------------------------------------------------------------------------

  /// Starts listening to the network for multi-device sync and DVM feedbacks.
  /// Triggers an initial [resync].
  Future<void> startListening() async {
    if (_listening) return;
    _listening = true;

    if (await _store.needsMigration()) {
      await _rebuildComputed();
    }

    await _startSync();
    _scheduleFeedbackSubscriptionUpdate();
    await resync();
  }

  /// Stops all network subscriptions.
  /// The scheduler remains fully usable offline.
  Future<void> stopListening() async {
    if (!_listening) return;
    _listening = false;

    for (final sub in _syncSubscriptions) {
      await sub.cancel();
    }
    _syncSubscriptions.clear();

    for (final response in _syncResponses) {
      await _ndk.requests.closeSubscription(response.requestId);
    }
    _syncResponses.clear();

    if (_feedbackRequestId != null) {
      await _ndk.requests.closeSubscription(_feedbackRequestId!);
      _feedbackRequestId = null;
    }
    await _feedbackSubscription?.cancel();
    _feedbackSubscription = null;

    _feedbackUpdateTimer?.cancel();
    _feedbackUpdateTimer = null;
  }

  /// Forces a manual resync of schedule requests, deletions, and feedbacks.
  Future<void> resync() async {
    final signer = _ndk.accounts.getLoggedAccount()?.signer;
    if (signer == null) {
      _syncController.add(
        const SyncState(
          status: SyncStatus.error,
          error: 'No logged in account',
        ),
      );
      return;
    }

    _syncController.add(const SyncState(status: SyncStatus.syncing));

    final pubkey = signer.getPublicKey();
    final now = DateTime.now().millisecondsSinceEpoch ~/ 1000;

    // kind:5905
    await _queryWithFetchedRanges(
      Filter(authors: [pubkey], kinds: [5905]),
      0,
      now,
      _onScheduleEvent,
    );

    // kind:5
    await _queryWithFetchedRanges(
      Filter(authors: [pubkey], kinds: [5]),
      0,
      now,
      _onDeletionEvent,
    );

    // kind:7000
    final jobs = await _store.listJobs();
    final jobIds = jobs.map((j) => j.jobId).toList();
    if (jobIds.isNotEmpty) {
      final feedbackFilter = Filter(kinds: [7000]);
      feedbackFilter.setTag('r', jobIds);
      await _queryWithFetchedRanges(feedbackFilter, 0, now, _onFeedbackEvent);
    }

    _syncController.add(
      SyncState(status: SyncStatus.synced, lastSyncAt: DateTime.now()),
    );
  }

  // --------------------------------------------------------------------------
  // Local data management
  // --------------------------------------------------------------------------

  /// Decrypts events waiting in the [pending_decryption] queue.
  Future<void> decryptPending() async {
    final signer = _ndk.accounts.getLoggedAccount()?.signer;
    if (signer == null) throw StateError('No logged in account');

    final pendingIds = await _store.listPendingDecryption();
    for (final eventId in pendingIds) {
      final response = _ndk.requests.query(
        filter: Filter(ids: [eventId], kinds: [5905, 7000]),
        cacheRead: true,
        cacheWrite: false,
      );
      final events = await response.future;
      if (events.isEmpty) continue;

      final event = events.first;
      String? decrypted;
      try {
        if (event.kind == 5905) {
          final dvmPubkey = event.getFirstTag('p');
          if (dvmPubkey == null) continue;
          decrypted = await signer.decryptNip44(
            ciphertext: event.content,
            senderPubKey: dvmPubkey,
          );
        } else if (event.kind == 7000) {
          final ephemeralPubkey = event.getFirstTag('ephemeral-pubkey');
          if (ephemeralPubkey == null) continue;
          decrypted = await signer.decryptNip44(
            ciphertext: event.content,
            senderPubKey: ephemeralPubkey,
          );
        }
      } catch (_) {
        continue;
      }

      if (decrypted == null) continue;

      await _store.putDecryptedPayload(eventId, decrypted);
      await _store.removePendingDecryption(eventId);

      if (event.kind == 5905) {
        await _processSchedulePayload(event, decrypted);
      } else if (event.kind == 7000) {
        await _processFeedbackPayload(event, decrypted);
      }
    }
  }

  // --------------------------------------------------------------------------
  // CRUD
  // --------------------------------------------------------------------------

  /// Schedules an event to be published later by a Scheduler DVM.
  ///
  /// [relays] specifies where the DVM should publish (payload). If omitted,
  /// falls back to the user's NIP-65 write relays.
  ///
  /// The kind:5905 request is broadcast via the [OfflineBroadcast] shim to
  /// all the user's NIP-65 relays (read + write).
  Future<ScheduledJob> schedule(
    Nip01Event event,
    String dvmPubkey, {
    DateTime? at,
    List<String>? relays,
  }) async {
    final signer = _ndk.accounts.getLoggedAccount()?.signer;
    if (signer == null) throw StateError('No logged in account');

    final jobId = _generateJobId();
    final scheduleAt =
        (at ?? DateTime.fromMillisecondsSinceEpoch(event.createdAt * 1000))
            .millisecondsSinceEpoch ~/
        1000;

    // Target relays (payload)
    List<String> targetRelays;
    if (relays != null && relays.isNotEmpty) {
      targetRelays = relays;
    } else {
      final userRelayList = await _ndk.userRelayLists.getSingleUserRelayList(
        signer.getPublicKey(),
      );
      targetRelays = userRelayList?.writeUrls.toList() ?? [];
    }

    // Build and encrypt payload
    final payload = jsonEncode({
      'job_id': jobId,
      'schedule_at': scheduleAt,
      'signed_event': {
        'id': event.id,
        'pubkey': event.pubKey,
        'created_at': event.createdAt,
        'kind': event.kind,
        'tags': event.tags,
        'content': event.content,
        'sig': event.sig,
      },
      'relays': targetRelays,
    });

    final encrypted = await signer.encryptNip44(
      plaintext: payload,
      recipientPubKey: dvmPubkey,
    );
    if (encrypted == null) throw StateError('Failed to encrypt payload');

    // Create and sign kind:5905
    final requestEvent = Nip01Event(
      pubKey: signer.getPublicKey(),
      kind: 5905,
      tags: [
        ['p', dvmPubkey],
        ['encrypted'],
      ],
      content: encrypted,
      createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
    );
    final signedRequestEvent = await signer.sign(requestEvent);

    // Determine broadcast relays (all user nip65 relays)
    final userRelayList = await _ndk.userRelayLists.getSingleUserRelayList(
      signer.getPublicKey(),
    );
    final broadcastRelays = userRelayList?.urls.toList() ?? [];
    if (broadcastRelays.isEmpty) {
      throw StateError('No relays found for broadcast');
    }

    // Broadcast via shim
    await _broadcast.broadcast(signedRequestEvent, relays: broadcastRelays);

    // Persist locally
    final now = DateTime.now().millisecondsSinceEpoch ~/ 1000;
    final job = ScheduledJob(
      jobId: jobId,
      requestEventId: signedRequestEvent.id,
      dvmPubkey: dvmPubkey,
      scheduleAt: scheduleAt,
      targetEvent: event,
      targetRelays: targetRelays,
      status: JobStatus.pending,
      createdAt: now,
      updatedAt: now,
    );

    await _store.putDecryptedPayload(signedRequestEvent.id, payload);
    await _store.putJob(job);

    _scheduleFeedbackSubscriptionUpdate();

    return job;
  }

  /// Cancels a scheduled job by broadcasting a kind:5 deletion event.
  Future<void> cancel(String jobId) async {
    final signer = _ndk.accounts.getLoggedAccount()?.signer;
    if (signer == null) throw StateError('No logged in account');

    final job = await _store.getJob(jobId);
    if (job == null) throw ArgumentError('Job not found: $jobId');

    // Create and sign kind:5
    final deletionEvent = Nip01Event(
      pubKey: signer.getPublicKey(),
      kind: 5,
      tags: [
        ['e', job.requestEventId],
      ],
      content: 'cancel',
      createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
    );
    final signedDeletion = await signer.sign(deletionEvent);

    // Determine broadcast relays
    final userRelayList = await _ndk.userRelayLists.getSingleUserRelayList(
      signer.getPublicKey(),
    );
    final broadcastRelays = userRelayList?.urls.toList() ?? [];
    if (broadcastRelays.isEmpty) {
      throw StateError('No relays found for broadcast');
    }

    // Broadcast via shim
    await _broadcast.broadcast(signedDeletion, relays: broadcastRelays);

    // Update local state
    await _store.putTombstone(
      job.requestEventId,
      deletionEventId: signedDeletion.id,
    );
    job.status = JobStatus.cancelled;
    job.updatedAt = DateTime.now().millisecondsSinceEpoch ~/ 1000;
    await _store.putJob(job);
  }

  /// Lists all scheduled jobs from the local computed store.
  Future<List<ScheduledJob>> listJobs() => _store.listJobs();

  /// Live stream of all scheduled jobs.
  Stream<List<ScheduledJob>> get jobsStream => _store.watchJobs();

  // --------------------------------------------------------------------------
  // Lifecycle
  // --------------------------------------------------------------------------

  /// Disposes all resources.
  Future<void> dispose() async {
    await stopListening();
    await _statusController.close();
    await _syncController.close();
  }

  // --------------------------------------------------------------------------
  // Internals - Network sync
  // --------------------------------------------------------------------------

  Future<void> _startSync() async {
    final signer = _ndk.accounts.getLoggedAccount()?.signer;
    if (signer == null) return;

    final pubkey = signer.getPublicKey();

    // kind:5905 subscription
    final scheduleResponse = _ndk.requests.subscription(
      filter: Filter(authors: [pubkey], kinds: [5905]),
      id: 'scheduler-sync-5905',
    );
    _syncResponses.add(scheduleResponse);
    _syncSubscriptions.add(scheduleResponse.stream.listen(_onScheduleEvent));

    // kind:5 subscription
    final deletionResponse = _ndk.requests.subscription(
      filter: Filter(authors: [pubkey], kinds: [5]),
      id: 'scheduler-sync-5',
    );
    _syncResponses.add(deletionResponse);
    _syncSubscriptions.add(deletionResponse.stream.listen(_onDeletionEvent));
  }

  Future<void> _queryWithFetchedRanges(
    Filter filter,
    int since,
    int until,
    Future<void> Function(Nip01Event) handler,
  ) async {
    final optimized = await _ndk.fetchedRanges.getOptimizedFilters(
      filter: filter,
      since: since,
      until: until,
    );

    if (optimized.isEmpty) {
      final response = _ndk.requests.query(
        filter: filter,
        cacheRead: true,
        cacheWrite: false,
      );
      await for (final event in response.stream) {
        await handler(event);
      }
      return;
    }

    for (final entry in optimized.entries) {
      final relay = entry.key;
      final filters = entry.value;
      for (final f in filters) {
        final response = _ndk.requests.query(
          filter: f,
          explicitRelays: [relay],
          cacheRead: true,
          cacheWrite: true,
        );
        await for (final event in response.stream) {
          await handler(event);
        }
      }
    }
  }

  // --------------------------------------------------------------------------
  // Internals - Event handlers
  // --------------------------------------------------------------------------

  Future<void> _onScheduleEvent(Nip01Event event) async {
    if (await _store.getDecryptedPayload(event.id) != null) return;
    if (await _store.isPendingDecryption(event.id)) return;

    final signer = _ndk.accounts.getLoggedAccount()?.signer;
    if (signer == null) {
      await _store.addPendingDecryption(event.id);
      return;
    }

    final dvmPubkey = event.getFirstTag('p');
    if (dvmPubkey == null) return;

    String? decrypted;
    try {
      decrypted = await signer.decryptNip44(
        ciphertext: event.content,
        senderPubKey: dvmPubkey,
      );
    } catch (_) {
      await _store.addPendingDecryption(event.id);
      return;
    }

    if (decrypted == null) {
      await _store.addPendingDecryption(event.id);
      return;
    }

    await _store.putDecryptedPayload(event.id, decrypted);
    await _store.removePendingDecryption(event.id);
    await _processSchedulePayload(event, decrypted);
  }

  Future<void> _onDeletionEvent(Nip01Event event) async {
    final deletedEventIds = event.getTags('e');
    for (final requestEventId in deletedEventIds) {
      await _store.putTombstone(requestEventId, deletionEventId: event.id);
      final jobs = await _store.listJobs();
      final job = jobs
          .where((j) => j.requestEventId == requestEventId)
          .firstOrNull;
      if (job != null) {
        job.status = JobStatus.cancelled;
        job.updatedAt = DateTime.now().millisecondsSinceEpoch ~/ 1000;
        await _store.putJob(job);
      }
    }
  }

  Future<void> _onFeedbackEvent(Nip01Event event) async {
    if (await _store.getDecryptedPayload(event.id) != null) return;
    if (await _store.isPendingDecryption(event.id)) return;

    final ephemeralPubkey = event.getFirstTag('ephemeral-pubkey');
    if (ephemeralPubkey == null) return;

    final signer = _ndk.accounts.getLoggedAccount()?.signer;
    if (signer == null) {
      await _store.addPendingDecryption(event.id);
      return;
    }

    String? decrypted;
    try {
      decrypted = await signer.decryptNip44(
        ciphertext: event.content,
        senderPubKey: ephemeralPubkey,
      );
    } catch (_) {
      await _store.addPendingDecryption(event.id);
      return;
    }

    if (decrypted == null) {
      await _store.addPendingDecryption(event.id);
      return;
    }

    await _store.putDecryptedPayload(event.id, decrypted);
    await _store.removePendingDecryption(event.id);
    await _processFeedbackPayload(event, decrypted);
  }

  // --------------------------------------------------------------------------
  // Internals - Payload processing
  // --------------------------------------------------------------------------

  Future<void> _processSchedulePayload(Nip01Event event, String payload) async {
    try {
      final json = jsonDecode(payload) as Map<String, dynamic>;
      final jobId = json['job_id'] as String;
      final scheduleAt = json['schedule_at'] as int;
      final signedEventMap = json['signed_event'] as Map<String, dynamic>;
      final targetRelays = (json['relays'] as List<dynamic>)
          .map((e) => e as String)
          .toList();

      final targetEvent = Nip01Event(
        id: signedEventMap['id'] as String,
        pubKey: signedEventMap['pubkey'] as String,
        createdAt: signedEventMap['created_at'] as int,
        kind: signedEventMap['kind'] as int,
        tags: (signedEventMap['tags'] as List<dynamic>)
            .map((t) => (t as List<dynamic>).map((e) => e as String).toList())
            .toList(),
        content: signedEventMap['content'] as String,
        sig: signedEventMap['sig'] as String,
      );

      final dvmPubkey = event.getFirstTag('p') ?? '';
      final isCancelled = await _store.isTombstoned(event.id);

      final now = DateTime.now().millisecondsSinceEpoch ~/ 1000;
      final job = ScheduledJob(
        jobId: jobId,
        requestEventId: event.id,
        dvmPubkey: dvmPubkey,
        scheduleAt: scheduleAt,
        targetEvent: targetEvent,
        targetRelays: targetRelays,
        status: isCancelled ? JobStatus.cancelled : JobStatus.pending,
        createdAt: now,
        updatedAt: now,
      );

      await _store.putJob(job);
    } catch (_) {
      // Invalid payload, ignore
    }
  }

  Future<void> _processFeedbackPayload(Nip01Event event, String payload) async {
    try {
      final json = jsonDecode(payload) as Map<String, dynamic>;
      final statusStr = json['status'] as String;
      final message = json['message'] as String?;
      final jobId = event.getFirstTag('r');
      if (jobId == null) return;

      final status = JobStatus.values.byName(statusStr);
      final job = await _store.getJob(jobId);
      if (job == null) return;

      job.status = status;
      job.lastMessage = message;
      job.updatedAt = DateTime.now().millisecondsSinceEpoch ~/ 1000;
      await _store.putJob(job);

      _statusController.add(
        StatusUpdate(
          jobId: jobId,
          status: status,
          message: message,
          receivedAt: DateTime.now(),
        ),
      );
    } catch (_) {
      // Invalid payload, ignore
    }
  }

  // --------------------------------------------------------------------------
  // Internals - Feedback subscription
  // --------------------------------------------------------------------------

  void _scheduleFeedbackSubscriptionUpdate() {
    _feedbackUpdateTimer?.cancel();
    _feedbackUpdateTimer = Timer(const Duration(milliseconds: 500), () {
      _updateFeedbackSubscription();
    });
  }

  Future<void> _updateFeedbackSubscription() async {
    if (!_listening) return;

    if (_feedbackRequestId != null) {
      await _ndk.requests.closeSubscription(_feedbackRequestId!);
      _feedbackRequestId = null;
    }
    await _feedbackSubscription?.cancel();
    _feedbackSubscription = null;

    final jobs = await _store.listJobs();
    final jobIds = jobs.map((j) => j.jobId).toList();
    if (jobIds.isEmpty) return;

    final filter = Filter(kinds: [7000]);
    filter.setTag('r', jobIds);

    final response = _ndk.requests.subscription(
      filter: filter,
      id: 'scheduler-feedback',
    );
    _feedbackRequestId = response.requestId;
    _feedbackSubscription = response.stream.listen(_onFeedbackEvent);
  }

  // --------------------------------------------------------------------------
  // Internals - Computed rebuild
  // --------------------------------------------------------------------------

  Future<void> _rebuildComputed() async {
    await _store.rebuildComputed((eventId, payload) async {
      try {
        final json = jsonDecode(payload) as Map<String, dynamic>;
        final jobId = json['job_id'] as String;
        final scheduleAt = json['schedule_at'] as int;
        final signedEventMap = json['signed_event'] as Map<String, dynamic>;
        final targetRelays = (json['relays'] as List<dynamic>)
            .map((e) => e as String)
            .toList();

        final targetEvent = Nip01Event(
          id: signedEventMap['id'] as String,
          pubKey: signedEventMap['pubkey'] as String,
          createdAt: signedEventMap['created_at'] as int,
          kind: signedEventMap['kind'] as int,
          tags: (signedEventMap['tags'] as List<dynamic>)
              .map((t) => (t as List<dynamic>).map((e) => e as String).toList())
              .toList(),
          content: signedEventMap['content'] as String,
          sig: signedEventMap['sig'] as String,
        );

        final isCancelled = await _store.isTombstoned(eventId);

        // For rebuild, we don't know the dvmPubkey from the payload alone.
        // We try to fetch the kind:5905 event from NDK cache to get the p tag.
        String dvmPubkey = '';
        final response = _ndk.requests.query(
          filter: Filter(ids: [eventId], kinds: [5905]),
          cacheRead: true,
          cacheWrite: false,
        );
        final events = await response.future;
        if (events.isNotEmpty) {
          dvmPubkey = events.first.getFirstTag('p') ?? '';
        }

        final now = DateTime.now().millisecondsSinceEpoch ~/ 1000;
        return ScheduledJob(
          jobId: jobId,
          requestEventId: eventId,
          dvmPubkey: dvmPubkey,
          scheduleAt: scheduleAt,
          targetEvent: targetEvent,
          targetRelays: targetRelays,
          status: isCancelled ? JobStatus.cancelled : JobStatus.pending,
          createdAt: now,
          updatedAt: now,
        );
      } catch (_) {
        return null;
      }
    });

    // Apply feedbacks from cache
    final jobs = await _store.listJobs();
    for (final job in jobs) {
      final feedbackResponse = _ndk.requests.query(
        filter: Filter(kinds: [7000])..setTag('r', [job.jobId]),
        cacheRead: true,
        cacheWrite: false,
      );
      final feedbacks = await feedbackResponse.future;
      if (feedbacks.isEmpty) continue;

      // Sort by created_at desc to get latest feedback
      feedbacks.sort((a, b) => b.createdAt.compareTo(a.createdAt));
      final latest = feedbacks.first;

      final decrypted = await _store.getDecryptedPayload(latest.id);
      if (decrypted != null) {
        await _processFeedbackPayload(latest, decrypted);
      }
    }
  }

  // --------------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------------

  String _generateJobId() {
    final random = Random.secure();
    final bytes = List<int>.generate(32, (_) => random.nextInt(256));
    return bytes.map((b) => b.toRadixString(16).padLeft(2, '0')).join();
  }
}
