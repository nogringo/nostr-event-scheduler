import 'dart:async';
import 'dart:convert';
import 'dart:math';

import 'package:broadcast_queue_shim_for_ndk/broadcast_queue_shim_for_ndk.dart';
import 'package:ndk/ndk.dart';
import 'package:sembast/sembast.dart' as sembast;

import 'models/job_status.dart';
import 'models/schedule_package_item.dart';
import 'models/scheduled_item.dart';
import 'models/scheduled_job.dart';
import 'models/scheduled_package.dart';
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

    // kind:31234 package manifests
    await _queryWithFetchedRanges(
      Filter(authors: [pubkey], kinds: [31234]),
      0,
      now,
      _onPackageEvent,
    );

    // kind:5 (deletions of kind 5905 requests and kind 31234 manifests)
    final deletionFilter = Filter(authors: [pubkey], kinds: [5]);
    await _queryWithFetchedRanges(deletionFilter, 0, now, _onDeletionEvent);

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
        filter: Filter(ids: [eventId], kinds: [5905, 7000, 31234]),
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
        } else if (event.kind == 31234) {
          decrypted = await signer.decryptNip44(
            ciphertext: event.content,
            senderPubKey: event.pubKey,
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
      } else if (event.kind == 31234) {
        await _processPackagePayload(event, decrypted);
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
    List<String>? dvmReadRelays,
  }) async {
    final signer = _ndk.accounts.getLoggedAccount()?.signer;
    if (signer == null) throw StateError('No logged in account');

    final result = await _createScheduleRequest(
      event,
      dvmPubkey,
      at: at,
      relays: relays,
      dvmReadRelays: dvmReadRelays,
    );

    await _broadcast.broadcast(result.requestEvent, relays: result.relays);
    await _store.putDecryptedPayload(result.requestEvent.id, result.payload);
    await _store.putJob(result.job);
    _scheduleFeedbackSubscriptionUpdate();

    return result.job;
  }

  /// Schedules multiple events as one logical package.
  Future<ScheduledPackage> schedulePackage(
    List<SchedulePackageItem> items, {
    required String content,
  }) async {
    if (items.isEmpty) {
      throw ArgumentError('Package must contain at least one job');
    }

    final signer = _ndk.accounts.getLoggedAccount()?.signer;
    if (signer == null) throw StateError('No logged in account');

    final created = <_CreatedScheduleRequest>[];
    for (final item in items) {
      created.add(
        await _createScheduleRequest(
          item.event,
          item.dvmPubkey,
          at: item.at,
          relays: item.relays,
          dvmReadRelays: item.dvmReadRelays,
        ),
      );
    }

    final packageId = _generateJobId();
    final encrypted = await signer.encryptNip44(
      plaintext: content,
      recipientPubKey: signer.getPublicKey(),
    );
    if (encrypted == null) {
      throw StateError('Failed to encrypt package content');
    }

    final manifest = Nip01Event(
      pubKey: signer.getPublicKey(),
      kind: 31234,
      tags: [
        ['d', packageId],
        ['k', '5905'],
        ...created.map((result) => ['e', result.requestEvent.id]),
      ],
      content: encrypted,
      createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
    );
    final signedManifest = await signer.sign(manifest);
    final userRelays = await _userBroadcastRelays();

    final now = DateTime.now().millisecondsSinceEpoch ~/ 1000;
    final package = ScheduledPackage(
      packageId: packageId,
      manifestEventId: signedManifest.id,
      content: content,
      requestEventIds: created.map((result) => result.requestEvent.id).toList(),
      jobs: created.map((result) => result.job).toList(),
      createdAt: now,
      updatedAt: now,
    );

    for (final result in created) {
      await _store.putDecryptedPayload(result.requestEvent.id, result.payload);
      await _store.putJob(result.job);
    }
    await _store.putDecryptedPayload(signedManifest.id, content);
    await _store.putPackage(package);

    await Future.wait([
      for (final result in created)
        _broadcast.broadcast(result.requestEvent, relays: result.relays),
      _broadcast.broadcast(signedManifest, relays: userRelays),
    ]);

    _scheduleFeedbackSubscriptionUpdate();

    return package;
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
        ['k', '5905'],
      ],
      content: 'cancel',
      createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
    );
    final signedDeletion = await signer.sign(deletionEvent);

    final broadcastRelays = await _deletionBroadcastRelays(job.dvmPubkey);

    // Broadcast via shim
    await _broadcast.broadcast(signedDeletion, relays: broadcastRelays);

    // Update local state
    await _store.putTombstone(
      job.requestEventId,
      deletionEventId: signedDeletion.id,
    );
    await _store.removeJob(job.jobId);
  }

  /// Cancels a scheduled package and all Scheduler DVM jobs it links.
  Future<void> cancelPackage(String packageId) async {
    final signer = _ndk.accounts.getLoggedAccount()?.signer;
    if (signer == null) throw StateError('No logged in account');

    final package = await _store.getPackage(packageId);
    if (package == null) throw ArgumentError('Package not found: $packageId');

    final deletionEvent = Nip01Event(
      pubKey: signer.getPublicKey(),
      kind: 5,
      tags: [
        for (final requestEventId in package.requestEventIds)
          ['e', requestEventId],
        ['e', package.manifestEventId],
        ['k', '5905'],
        ['k', '31234'],
      ],
      content: 'cancel package',
      createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
    );
    final signedDeletion = await signer.sign(deletionEvent);
    await _broadcast.broadcast(
      signedDeletion,
      relays: await _packageDeletionBroadcastRelays(package.requestEventIds),
    );

    for (final requestEventId in package.requestEventIds) {
      await _store.putTombstone(
        requestEventId,
        deletionEventId: signedDeletion.id,
      );
      final job = await _jobByRequestEventId(requestEventId);
      if (job != null) {
        await _store.removeJob(job.jobId);
      }
    }
    await _store.putTombstone(
      package.manifestEventId,
      deletionEventId: signedDeletion.id,
    );
    await _store.removePackage(package.packageId);
  }

  /// Lists all scheduled packages from the local computed store.
  Future<List<ScheduledPackage>> listPackages() => _store.listPackages();

  /// Lists all scheduled jobs from the local computed store.
  Future<List<ScheduledJob>> listJobs() => _store.listJobs();

  /// Live stream of all scheduled jobs.
  Stream<List<ScheduledJob>> get jobsStream => _store.watchJobs();

  /// Lists all logical schedules.
  ///
  /// Standalone jobs are returned as one item each. Jobs linked by a scheduled
  /// package manifest are returned as a single package item.
  Future<List<ScheduledItem>> listSchedules() => _store.listSchedules();

  /// Live stream of all logical schedules.
  Stream<List<ScheduledItem>> get schedulesStream {
    late StreamController<List<ScheduledItem>> controller;
    StreamSubscription<List<ScheduledJob>>? jobsSub;
    StreamSubscription<List<ScheduledPackage>>? packagesSub;

    Future<void> emit() async {
      if (!controller.isClosed) {
        controller.add(await _store.listSchedules());
      }
    }

    controller = StreamController<List<ScheduledItem>>.broadcast(
      onListen: () {
        jobsSub = _store.watchJobs().listen((_) => emit());
        packagesSub = _store.watchPackages().listen((_) => emit());
        emit();
      },
      onCancel: () async {
        await jobsSub?.cancel();
        await packagesSub?.cancel();
      },
    );
    return controller.stream;
  }

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

    // kind:31234 package manifest subscription
    final packageResponse = _ndk.requests.subscription(
      filter: Filter(authors: [pubkey], kinds: [31234]),
      id: 'scheduler-sync-31234',
    );
    _syncResponses.add(packageResponse);
    _syncSubscriptions.add(packageResponse.stream.listen(_onPackageEvent));

    // kind:5 subscription (deletions of jobs and package manifests)
    final deletionFilter = Filter(authors: [pubkey], kinds: [5]);
    final deletionResponse = _ndk.requests.subscription(
      filter: deletionFilter,
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

  Future<void> _onPackageEvent(Nip01Event event) async {
    if (await _store.getDecryptedPayload(event.id) != null) return;
    if (await _store.isPendingDecryption(event.id)) return;

    final signer = _ndk.accounts.getLoggedAccount()?.signer;
    if (signer == null) {
      await _store.addPendingDecryption(event.id);
      return;
    }

    String? decrypted;
    try {
      decrypted = await signer.decryptNip44(
        ciphertext: event.content,
        senderPubKey: event.pubKey,
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
    await _processPackagePayload(event, decrypted);
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
        await _store.removeJob(job.jobId);
      }
      await _store.removePackageByManifestEventId(requestEventId);
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
      if (await _store.isTombstoned(event.id)) {
        return;
      }

      final now = DateTime.now().millisecondsSinceEpoch ~/ 1000;
      final job = ScheduledJob(
        jobId: jobId,
        requestEventId: event.id,
        dvmPubkey: dvmPubkey,
        scheduleAt: scheduleAt,
        targetEvent: targetEvent,
        targetRelays: targetRelays,
        status: JobStatus.pending,
        createdAt: now,
        updatedAt: now,
      );

      await _store.putJob(job);
    } catch (_) {
      // Invalid payload, ignore
    }
  }

  Future<void> _processPackagePayload(Nip01Event event, String content) async {
    try {
      if (await _store.isTombstoned(event.id)) {
        return;
      }

      final packageId = event.getFirstTag('d');
      if (packageId == null || packageId.isEmpty) return;
      if (event.getFirstTag('k') != '5905') return;

      final requestEventIds = event.getTags('e');
      if (requestEventIds.isEmpty) return;

      final now = DateTime.now().millisecondsSinceEpoch ~/ 1000;
      final jobs = (await _store.listJobs())
          .where((job) => requestEventIds.contains(job.requestEventId))
          .toList();

      await _store.putPackage(
        ScheduledPackage(
          packageId: packageId,
          manifestEventId: event.id,
          content: content,
          requestEventIds: requestEventIds,
          jobs: jobs,
          createdAt: now,
          updatedAt: now,
        ),
      );
    } catch (_) {
      // Invalid package manifest, ignore
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
    await _store.rebuildComputed(
      (eventId, payload) async {
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
                .map(
                  (t) => (t as List<dynamic>).map((e) => e as String).toList(),
                )
                .toList(),
            content: signedEventMap['content'] as String,
            sig: signedEventMap['sig'] as String,
          );

          if (await _store.isTombstoned(eventId)) {
            return null;
          }

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
            status: JobStatus.pending,
            createdAt: now,
            updatedAt: now,
          );
        } catch (_) {
          return null;
        }
      },
      buildPackage: (eventId, payload) async {
        try {
          if (await _store.isTombstoned(eventId)) return null;

          final response = _ndk.requests.query(
            filter: Filter(ids: [eventId], kinds: [31234]),
            cacheRead: true,
            cacheWrite: false,
          );
          final events = await response.future;
          if (events.isEmpty) return null;

          final event = events.first;
          final packageId = event.getFirstTag('d');
          if (packageId == null || packageId.isEmpty) return null;
          if (event.getFirstTag('k') != '5905') return null;

          final requestEventIds = event.getTags('e');
          if (requestEventIds.isEmpty) return null;

          final now = DateTime.now().millisecondsSinceEpoch ~/ 1000;
          final jobs = (await _store.listJobs())
              .where((job) => requestEventIds.contains(job.requestEventId))
              .toList();

          return ScheduledPackage(
            packageId: packageId,
            manifestEventId: eventId,
            content: payload,
            requestEventIds: requestEventIds,
            jobs: jobs,
            createdAt: now,
            updatedAt: now,
          );
        } catch (_) {
          return null;
        }
      },
    );

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

  Future<_CreatedScheduleRequest> _createScheduleRequest(
    Nip01Event event,
    String dvmPubkey, {
    DateTime? at,
    List<String>? relays,
    List<String>? dvmReadRelays,
  }) async {
    final signer = _ndk.accounts.getLoggedAccount()?.signer;
    if (signer == null) throw StateError('No logged in account');

    final jobId = _generateJobId();
    final scheduleAt =
        (at ?? DateTime.fromMillisecondsSinceEpoch(event.createdAt * 1000))
            .millisecondsSinceEpoch ~/
        1000;

    final targetRelays = await _targetRelays(relays);
    if (targetRelays.isEmpty) {
      throw StateError('No target relays found for scheduled event');
    }

    final payload = jsonEncode({
      'job_id': jobId,
      'schedule_at': scheduleAt,
      'signed_event': _eventToJson(event),
      'relays': targetRelays,
    });

    final encrypted = await signer.encryptNip44(
      plaintext: payload,
      recipientPubKey: dvmPubkey,
    );
    if (encrypted == null) throw StateError('Failed to encrypt payload');

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

    return _CreatedScheduleRequest(
      job: job,
      requestEvent: signedRequestEvent,
      payload: payload,
      relays: await _requestBroadcastRelays(dvmPubkey, dvmReadRelays),
    );
  }

  Future<List<String>> _targetRelays(List<String>? relays) async {
    if (relays != null && relays.isNotEmpty) {
      return relays;
    }

    final signer = _ndk.accounts.getLoggedAccount()?.signer;
    if (signer == null) throw StateError('No logged in account');

    final userRelayList = await _ndk.userRelayLists.getSingleUserRelayList(
      signer.getPublicKey(),
    );
    return userRelayList?.writeUrls.toList() ?? [];
  }

  Future<List<String>> _requestBroadcastRelays(
    String dvmPubkey,
    List<String>? dvmReadRelays,
  ) async {
    final userRelays = await _userBroadcastRelays();

    final dvmRelayList = await _ndk.userRelayLists.getSingleUserRelayList(
      dvmPubkey,
    );
    final resolvedDvmRelays = dvmRelayList?.readUrls.toList() ?? [];
    final dvmRelays = resolvedDvmRelays.isNotEmpty
        ? resolvedDvmRelays
        : (dvmReadRelays ?? []);
    if (dvmRelays.isEmpty) {
      throw StateError('No read relays found for DVM $dvmPubkey');
    }

    return {...userRelays, ...dvmRelays}.toList();
  }

  Future<List<String>> _deletionBroadcastRelays(String dvmPubkey) async {
    final userRelays = await _userBroadcastRelays();
    final dvmRelayList = await _ndk.userRelayLists.getSingleUserRelayList(
      dvmPubkey,
    );
    return {...userRelays, ...?dvmRelayList?.readUrls}.toList();
  }

  Future<ScheduledJob?> _jobByRequestEventId(String requestEventId) async {
    final jobs = await _store.listJobs();
    return jobs
        .where((job) => job.requestEventId == requestEventId)
        .firstOrNull;
  }

  Future<String?> _dvmPubkeyForRequestEventId(String requestEventId) async {
    final job = await _jobByRequestEventId(requestEventId);
    if (job != null && job.dvmPubkey.isNotEmpty) {
      return job.dvmPubkey;
    }

    final response = _ndk.requests.query(
      filter: Filter(ids: [requestEventId], kinds: [5905]),
      cacheRead: true,
      cacheWrite: false,
    );
    final events = await response.future;
    if (events.isEmpty) return null;
    return events.first.getFirstTag('p');
  }

  Future<List<String>> _packageDeletionBroadcastRelays(
    Iterable<String> requestEventIds,
  ) async {
    final relays = {...await _userBroadcastRelays()};
    for (final requestEventId in requestEventIds) {
      final dvmPubkey = await _dvmPubkeyForRequestEventId(requestEventId);
      if (dvmPubkey == null || dvmPubkey.isEmpty) continue;
      final dvmRelayList = await _ndk.userRelayLists.getSingleUserRelayList(
        dvmPubkey,
      );
      relays.addAll(dvmRelayList?.readUrls ?? const []);
    }
    return relays.toList();
  }

  Future<List<String>> _userBroadcastRelays() async {
    final signer = _ndk.accounts.getLoggedAccount()?.signer;
    if (signer == null) throw StateError('No logged in account');

    final userRelayList = await _ndk.userRelayLists.getSingleUserRelayList(
      signer.getPublicKey(),
    );
    final userRelays = userRelayList?.urls.toList() ?? [];
    if (userRelays.isEmpty) {
      throw StateError('No user NIP-65 relays found for broadcast');
    }
    return userRelays;
  }

  Map<String, dynamic> _eventToJson(Nip01Event event) {
    return {
      'id': event.id,
      'pubkey': event.pubKey,
      'created_at': event.createdAt,
      'kind': event.kind,
      'tags': event.tags,
      'content': event.content,
      'sig': event.sig,
    };
  }

  String _generateJobId() {
    final random = Random.secure();
    final bytes = List<int>.generate(32, (_) => random.nextInt(256));
    return bytes.map((b) => b.toRadixString(16).padLeft(2, '0')).join();
  }
}

class _CreatedScheduleRequest {
  final ScheduledJob job;
  final Nip01Event requestEvent;
  final String payload;
  final List<String> relays;

  _CreatedScheduleRequest({
    required this.job,
    required this.requestEvent,
    required this.payload,
    required this.relays,
  });
}
