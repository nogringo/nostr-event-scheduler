import 'dart:async';
import 'dart:convert';
import 'dart:math';

import 'package:broadcast_queue_shim_for_ndk/broadcast_queue_shim_for_ndk.dart';
import 'package:ndk/ndk.dart' hide RelaySet;
import 'package:sembast/sembast.dart' as sembast;

import 'kinds.dart';
import 'models/job_status.dart';
import 'models/schedule_package_item.dart';
import 'models/scheduled_item.dart';
import 'models/scheduled_job.dart';
import 'models/scheduled_job_request.dart';
import 'models/scheduled_package.dart';
import 'models/status_update.dart';
import 'models/sync_state.dart';
import 'scheduler_store.dart';

/// Local-first scheduler for Nostr events via Scheduler DVMs.
///
/// Every method is scoped to an explicit account [pubkey], so a host may drive
/// several accounts against a single instance. The account must be loaded in
/// [Ndk.accounts]; the logged account is never used implicitly.
///
/// Local data is split in two tiers. Raw holds definitive facts: the signed
/// events, kept in the NDK cache, plus their decrypted payloads and deletion
/// tombstones, kept by the scheduler. Computed holds the projections rebuilt
/// from raw, dropped and recomputed whenever their schema changes. Rebuilding
/// therefore reads the NDK cache, and the host must give [Ndk] a persistent
/// [CacheManager] for the scheduler to survive a restart.
class EventScheduler {
  final Ndk _ndk;
  final OfflineBroadcast _broadcast;
  final SchedulerStore _store;

  final Map<String, _AccountSync> _sync = {};
  final Map<String, Future<void>> _building = {};

  final _statusController = StreamController<StatusUpdate>.broadcast();
  final _syncController = StreamController<SyncState>.broadcast();

  /// Creates a new [EventScheduler].
  ///
  /// The caller must provide a started [OfflineBroadcast] instance. It must be
  /// able to resolve relay lists, so build it with [OfflineBroadcast.withNdk]
  /// or pass a `relayListFn`: the scheduler targets NIP-65 relay lists rather
  /// than fixed URLs.
  EventScheduler({
    required this._ndk,
    required this._broadcast,
    required sembast.Database db,
  }) : _store = SchedulerStore(db);

  // --------------------------------------------------------------------------
  // Streams
  // --------------------------------------------------------------------------

  /// DVM feedbacks for every account being listened to.
  Stream<StatusUpdate> get statusUpdates => _statusController.stream;

  /// Sync progress for every account being listened to.
  Stream<SyncState> get syncState => _syncController.stream;

  // --------------------------------------------------------------------------
  // Network control
  // --------------------------------------------------------------------------

  /// Starts listening to the network for [pubkey], for multi-device sync and
  /// DVM feedbacks. Triggers an initial [resync].
  ///
  /// Each listened account costs its own relay subscriptions, so only start
  /// the accounts whose schedules are actually in use.
  Future<void> startListening({required String pubkey}) async {
    if (_sync.containsKey(pubkey)) return;
    if (!_ndk.accounts.hasAccount(pubkey)) {
      throw StateError('No account loaded for $pubkey');
    }

    final state = _AccountSync();
    _sync[pubkey] = state;

    try {
      await _ensureBuilt(pubkey);

      for (final kind in [
        kindScheduleRequest,
        kindPackageManifest,
        kindDeletion,
      ]) {
        final response = _ndk.requests.subscription(
          filter: Filter(authors: [pubkey], kinds: [kind]),
          cacheWrite: true,
        );
        state.responses.add(response);
        state.subscriptions.add(response.stream.listen(_onRawEvent));
      }
    } catch (_) {
      // Never leave a half-started account registered: the guard above would
      // then make every retry a no-op.
      await stopListening(pubkey: pubkey);
      rethrow;
    }

    _scheduleFeedbackSubscriptionUpdate(pubkey);
    await resync(pubkey: pubkey);
  }

  /// Stops the network subscriptions of [pubkey], or of every account when it
  /// is omitted. The scheduler remains fully usable offline.
  Future<void> stopListening({String? pubkey}) async {
    if (pubkey == null) {
      for (final key in _sync.keys.toList()) {
        await stopListening(pubkey: key);
      }
      return;
    }
    await _sync.remove(pubkey)?.dispose(_ndk);
  }

  /// Forces a manual resync of [pubkey]'s schedule requests, deletions, and
  /// feedbacks.
  /// A failure is reported on [syncState] rather than thrown: losing the
  /// network is expected, and the local state stays usable.
  Future<void> resync({required String pubkey}) async {
    _syncController.add(SyncState(pubkey: pubkey, status: SyncStatus.syncing));

    try {
      await _ensureBuilt(pubkey);

      final now = DateTime.now().millisecondsSinceEpoch ~/ 1000;
      for (final kind in [
        kindScheduleRequest,
        kindPackageManifest,
        kindDeletion,
      ]) {
        await _queryWithFetchedRanges(
          Filter(authors: [pubkey], kinds: [kind]),
          0,
          now,
          _onRawEvent,
        );
      }

      final jobIds = (await _store.listJobs(
        pubkey,
      )).map((j) => j.jobId).toList();
      if (jobIds.isNotEmpty) {
        await _queryWithFetchedRanges(
          _feedbackFilter(jobIds),
          0,
          now,
          _onRawEvent,
        );
      }
    } catch (e) {
      _syncController.add(
        SyncState(
          pubkey: pubkey,
          status: SyncStatus.error,
          error: e.toString(),
        ),
      );
      return;
    }

    _syncController.add(
      SyncState(
        pubkey: pubkey,
        status: SyncStatus.synced,
        lastSyncAt: DateTime.now(),
      ),
    );
  }

  // --------------------------------------------------------------------------
  // Local data management
  // --------------------------------------------------------------------------

  /// Decrypts [pubkey]'s events left queued while its signer was unavailable.
  Future<void> decryptPending({required String pubkey}) async {
    _requireSigner(pubkey);

    final pendingIds = await _store.listPendingDecryption(pubkey);
    if (pendingIds.isEmpty) return;

    for (final event in await _ndk.config.cache.loadEvents(ids: pendingIds)) {
      await _onRawEvent(event);
    }
  }

  /// Removes every local trace of [pubkey]: its projections, the decrypted
  /// payloads and tombstones derived from its events, its scheduler events in
  /// the NDK cache, and the fetched ranges that would refill them.
  ///
  /// Listening for [pubkey] is stopped first. This is a local reset, not a
  /// protocol-level forget: the requests still live on the relays, so an
  /// account whose signer is still loaded rebuilds them on its next [resync].
  /// Use [cancel] to actually retract a schedule.
  ///
  /// The account's kind:5 deletions are left in the NDK cache: they are
  /// generic and dropping them would resurrect cancelled jobs.
  Future<void> clearLocalAccountData({required String pubkey}) async {
    await stopListening(pubkey: pubkey);

    final jobIds = (await _store.listJobs(pubkey)).map((j) => j.jobId).toSet();
    final eventIds = <String>{};

    for (final event in await _ndk.config.cache.loadEvents(
      pubKeys: [pubkey],
      kinds: [kindScheduleRequest, kindPackageManifest],
    )) {
      eventIds.add(event.id);
      final payload = await _store.getDecryptedPayload(event.id);
      final jobId = payload == null ? null : _jobIdOf(payload);
      if (jobId != null) jobIds.add(jobId);
    }

    // Cancelled requests are hidden from loadEvents by the cache visibility
    // rules, so their ids are recovered from the deletions that hid them.
    for (final deletion in await _ndk.config.cache.loadEvents(
      pubKeys: [pubkey],
      kinds: [kindDeletion],
    )) {
      eventIds.addAll(deletion.getTags('e'));
    }

    final feedbackIds = jobIds.isEmpty
        ? <String>[]
        : (await _ndk.config.cache.loadEvents(
            kinds: [kindFeedback],
            tags: {'r': jobIds.toList()},
          )).map((e) => e.id).toList();
    eventIds.addAll(feedbackIds);

    await _store.clearRaw(eventIds);
    await _store.clearComputed(pubkey);

    await _ndk.config.cache.removeEvents(
      pubKeys: [pubkey],
      kinds: [kindScheduleRequest, kindPackageManifest],
    );
    if (feedbackIds.isNotEmpty) {
      await _ndk.config.cache.removeEvents(ids: feedbackIds);
    }
    await _clearFetchedRanges(pubkey, jobIds.toList());
  }

  /// Removes every local trace of every account, unattributed records
  /// included. See [clearLocalAccountData] for what a local reset does and
  /// does not guarantee.
  Future<void> clearAllLocalData() async {
    final pubkeys = {
      ..._sync.keys,
      ..._ndk.accounts.accounts.keys,
      ...await _store.listKnownPubkeys(),
    };
    final jobIdsByPubkey = {
      for (final pubkey in pubkeys)
        pubkey: (await _store.listJobs(pubkey)).map((j) => j.jobId).toList(),
    };

    await stopListening();
    await _store.clearAll();

    await _ndk.config.cache.removeEvents(
      kinds: [kindScheduleRequest, kindPackageManifest, kindFeedback],
    );
    for (final entry in jobIdsByPubkey.entries) {
      await _clearFetchedRanges(entry.key, entry.value);
    }
  }

  // --------------------------------------------------------------------------
  // CRUD
  // --------------------------------------------------------------------------

  /// Schedules an event to be published later by one or more Scheduler DVMs,
  /// on behalf of [pubkey].
  ///
  /// Every DVM in [dvmPubkeys] receives its own kind:5905 request carrying
  /// the same job_id and payload, so publishing does not depend on a single
  /// DVM's uptime or policy. Publication stays deterministic because all
  /// DVMs publish the same already-signed [event]; relays deduplicate it by
  /// ID.
  ///
  /// [relays] specifies where the DVMs should publish (payload). If omitted,
  /// falls back to the account's NIP-65 write relays.
  ///
  /// The kind:5905 requests are broadcast via the [OfflineBroadcast] shim to
  /// all the account's NIP-65 relays (read + write) plus each DVM's read
  /// relays. Both are described as a [RelaySet] and resolved by the shim's
  /// worker, so a DVM whose relay list cannot be read right now delays
  /// delivery instead of failing the call. [dvmReadRelays] is used only for a
  /// DVM whose NIP-65 resolves to no read relay.
  Future<ScheduledJob> schedule(
    Nip01Event event,
    List<String> dvmPubkeys, {
    required String pubkey,
    DateTime? at,
    List<String>? relays,
    List<String>? dvmReadRelays,
  }) async {
    await _ensureBuilt(pubkey);

    final created = await _createJob(
      event,
      dvmPubkeys,
      pubkey: pubkey,
      at: at,
      relays: relays,
      dvmReadRelays: dvmReadRelays,
    );

    for (final request in created.requests) {
      await _saveRaw(request.event, created.payload);
    }
    await _store.putJob(created.job);

    await Future.wait([
      for (final request in created.requests)
        _broadcast.broadcast(
          request.event,
          relaySet: request.relaySet,
          pubkey: pubkey,
        ),
    ]);
    _scheduleFeedbackSubscriptionUpdate(pubkey);

    return created.job;
  }

  /// Schedules multiple events as one logical package on behalf of [pubkey].
  Future<ScheduledPackage> schedulePackage(
    List<SchedulePackageItem> items, {
    required String content,
    required String pubkey,
  }) async {
    if (items.isEmpty) {
      throw ArgumentError('Package must contain at least one job');
    }
    await _ensureBuilt(pubkey);

    final signer = _requireSigner(pubkey);

    final created = <_CreatedJob>[];
    for (final item in items) {
      created.add(
        await _createJob(
          item.event,
          item.dvmPubkeys,
          pubkey: pubkey,
          at: item.at,
          relays: item.relays,
          dvmReadRelays: item.dvmReadRelays,
        ),
      );
    }
    final requestEvents = created.expand((job) => job.requests).toList();

    final packageId = _generateJobId();
    final encrypted = await signer.encryptNip44(
      plaintext: content,
      recipientPubKey: pubkey,
    );
    if (encrypted == null) {
      throw StateError('Failed to encrypt package content');
    }

    final manifest = Nip01Event(
      pubKey: pubkey,
      kind: kindPackageManifest,
      tags: [
        ['d', packageId],
        ['k', '$kindScheduleRequest'],
        ...requestEvents.map((request) => ['e', request.event.id]),
      ],
      content: encrypted,
      createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
    );
    final signedManifest = await signer.sign(manifest);

    final now = DateTime.now().millisecondsSinceEpoch ~/ 1000;
    final package = ScheduledPackage(
      pubkey: pubkey,
      packageId: packageId,
      manifestEventId: signedManifest.id,
      content: content,
      requestEventIds: requestEvents
          .map((request) => request.event.id)
          .toList(),
      jobs: created.map((result) => result.job).toList(),
      createdAt: now,
      updatedAt: now,
    );

    for (final result in created) {
      for (final request in result.requests) {
        await _saveRaw(request.event, result.payload);
      }
      await _store.putJob(result.job);
    }
    await _saveRaw(signedManifest, content);
    await _store.putPackage(package);

    await Future.wait([
      for (final request in requestEvents)
        _broadcast.broadcast(
          request.event,
          relaySet: request.relaySet,
          pubkey: pubkey,
        ),
      _broadcast.broadcast(
        signedManifest,
        relaySet: RelaySet.nip65(pubkey),
        pubkey: pubkey,
      ),
    ]);

    _scheduleFeedbackSubscriptionUpdate(pubkey);

    return package;
  }

  /// Cancels a scheduled job of [pubkey] by broadcasting a kind:5 deletion.
  ///
  /// One deletion tags every kind:5905 request of the job, so all its DVMs
  /// are cancelled at once.
  Future<void> cancel(String jobId, {required String pubkey}) async {
    await _ensureBuilt(pubkey);

    final job = await _store.getJob(jobId);
    if (job == null || job.pubkey != pubkey) {
      throw ArgumentError('Job not found for $pubkey: $jobId');
    }

    final signedDeletion = await _signDeletion(
      pubkey: pubkey,
      eventIds: job.requestEventIds,
      kinds: const [kindScheduleRequest],
      content: 'cancel',
    );

    await _broadcast.broadcast(
      signedDeletion,
      relaySet: _deletionRelaySet(pubkey, job.dvmPubkeys),
      pubkey: pubkey,
    );

    for (final requestEventId in job.requestEventIds) {
      await _store.putTombstone(
        requestEventId,
        deletionEventId: signedDeletion.id,
      );
    }
    await _store.removeJob(job.jobId);
  }

  /// Cancels a scheduled package of [pubkey] and all Scheduler DVM jobs it
  /// links.
  Future<void> cancelPackage(String packageId, {required String pubkey}) async {
    await _ensureBuilt(pubkey);

    final package = await _store.getPackage(packageId);
    if (package == null || package.pubkey != pubkey) {
      throw ArgumentError('Package not found for $pubkey: $packageId');
    }

    final relaySet = await _packageDeletionRelaySet(
      pubkey,
      package.requestEventIds,
    );
    final signedDeletion = await _signDeletion(
      pubkey: pubkey,
      eventIds: [...package.requestEventIds, package.manifestEventId],
      kinds: const [kindScheduleRequest, kindPackageManifest],
      content: 'cancel package',
    );

    await _broadcast.broadcast(
      signedDeletion,
      relaySet: relaySet,
      pubkey: pubkey,
    );

    for (final requestEventId in package.requestEventIds) {
      await _store.putTombstone(
        requestEventId,
        deletionEventId: signedDeletion.id,
      );
      await _removeRequestFromJobs(requestEventId, pubkey);
    }
    await _store.putTombstone(
      package.manifestEventId,
      deletionEventId: signedDeletion.id,
    );
    await _store.removePackage(package.packageId);
  }

  /// Lists [pubkey]'s scheduled packages from the local computed store.
  Future<List<ScheduledPackage>> listPackages({required String pubkey}) async {
    await _ensureBuilt(pubkey);
    return _store.listPackages(pubkey);
  }

  /// Lists [pubkey]'s scheduled jobs from the local computed store.
  Future<List<ScheduledJob>> listJobs({required String pubkey}) async {
    await _ensureBuilt(pubkey);
    return _store.listJobs(pubkey);
  }

  /// Live stream of [pubkey]'s scheduled jobs.
  Stream<List<ScheduledJob>> jobsStream({required String pubkey}) {
    return _rebuiltFirst(pubkey, () => _store.watchJobs(pubkey));
  }

  /// Lists [pubkey]'s logical schedules.
  ///
  /// Standalone jobs are returned as one item each. Jobs linked by a scheduled
  /// package manifest are returned as a single package item.
  Future<List<ScheduledItem>> listSchedules({required String pubkey}) async {
    await _ensureBuilt(pubkey);
    return _store.listSchedules(pubkey);
  }

  /// Live stream of [pubkey]'s logical schedules.
  Stream<List<ScheduledItem>> schedulesStream({required String pubkey}) {
    late StreamController<List<ScheduledItem>> controller;
    StreamSubscription<List<ScheduledJob>>? jobsSub;
    StreamSubscription<List<ScheduledPackage>>? packagesSub;

    Future<void> emit() async {
      if (!controller.isClosed) {
        controller.add(await _store.listSchedules(pubkey));
      }
    }

    controller = StreamController<List<ScheduledItem>>.broadcast(
      onListen: () async {
        await _ensureBuilt(pubkey);
        if (!controller.hasListener) return;
        jobsSub = _store.watchJobs(pubkey).listen((_) => emit());
        packagesSub = _store.watchPackages(pubkey).listen((_) => emit());
        await emit();
      },
      onCancel: () async {
        await jobsSub?.cancel();
        await packagesSub?.cancel();
        jobsSub = null;
        packagesSub = null;
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
  // Internals - Accounts
  // --------------------------------------------------------------------------

  EventSigner? _signerOrNull(String pubkey) =>
      _ndk.accounts.accounts[pubkey]?.signer;

  EventSigner _requireSigner(String pubkey) {
    final signer = _signerOrNull(pubkey);
    if (signer == null) throw StateError('No account loaded for $pubkey');
    if (!signer.canSign()) throw StateError('Account $pubkey cannot sign');
    return signer;
  }

  // --------------------------------------------------------------------------
  // Internals - Computed projections
  // --------------------------------------------------------------------------

  /// Rebuilds [pubkey]'s projections from raw when their schema changed.
  ///
  /// Concurrent callers share the same rebuild.
  Future<void> _ensureBuilt(String pubkey) {
    final running = _building[pubkey];
    if (running != null) return running;

    final future = _rebuildIfNeeded(pubkey);
    _building[pubkey] = future;
    return future.whenComplete(() => _building.remove(pubkey));
  }

  Future<void> _rebuildIfNeeded(String pubkey) async {
    if (!await _store.needsRebuild(pubkey)) return;

    await _store.clearComputed(pubkey);

    final requests = await _ndk.config.cache.loadEvents(
      pubKeys: [pubkey],
      kinds: [kindScheduleRequest],
    );
    requests.sort((a, b) => a.createdAt.compareTo(b.createdAt));
    for (final event in requests) {
      await _onRawEvent(event);
    }

    // Manifests come second: they link the jobs the first pass just built.
    for (final event in await _ndk.config.cache.loadEvents(
      pubKeys: [pubkey],
      kinds: [kindPackageManifest],
    )) {
      await _onRawEvent(event);
    }

    for (final job in await _store.listJobs(pubkey)) {
      await _applyCachedFeedbacks(job);
    }

    await _store.markBuilt(pubkey);
  }

  /// Defers [open] until [pubkey]'s projections are current.
  Stream<T> _rebuiltFirst<T>(String pubkey, Stream<T> Function() open) {
    late StreamController<T> controller;
    StreamSubscription<T>? sub;

    controller = StreamController<T>.broadcast(
      onListen: () async {
        await _ensureBuilt(pubkey);
        if (!controller.hasListener) return;
        sub = open().listen(controller.add);
      },
      onCancel: () async {
        await sub?.cancel();
        sub = null;
      },
    );
    return controller.stream;
  }

  // --------------------------------------------------------------------------
  // Internals - Network sync
  // --------------------------------------------------------------------------

  Filter _feedbackFilter(List<String> jobIds) {
    final filter = Filter(kinds: [kindFeedback]);
    filter.setTag('r', jobIds);
    return filter;
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
        cacheWrite: true,
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

  Future<void> _clearFetchedRanges(String pubkey, List<String> jobIds) async {
    for (final kind in [
      kindScheduleRequest,
      kindPackageManifest,
      kindDeletion,
    ]) {
      await _ndk.fetchedRanges.clearForFilter(
        Filter(authors: [pubkey], kinds: [kind]),
      );
    }
    if (jobIds.isNotEmpty) {
      await _ndk.fetchedRanges.clearForFilter(_feedbackFilter(jobIds));
    }
  }

  // --------------------------------------------------------------------------
  // Internals - Event handling
  // --------------------------------------------------------------------------

  Future<void> _onRawEvent(Nip01Event event) async {
    switch (event.kind) {
      case kindScheduleRequest:
        await _onScheduleEvent(event);
      case kindPackageManifest:
        await _onPackageEvent(event);
      case kindFeedback:
        await _onFeedbackEvent(event);
      case kindDeletion:
        await _onDeletionEvent(event);
    }
  }

  Future<void> _onScheduleEvent(Nip01Event event) async {
    final payload = await _decryptedPayload(event, event.pubKey);
    if (payload == null) return;
    await _processSchedulePayload(event, payload);
  }

  Future<void> _onPackageEvent(Nip01Event event) async {
    final payload = await _decryptedPayload(event, event.pubKey);
    if (payload == null) return;
    await _processPackagePayload(event, payload);
  }

  /// A kind:7000 is signed by the DVM, so it is attributed to an account
  /// through the job its `r` tag points to.
  Future<void> _onFeedbackEvent(Nip01Event event) async {
    final jobId = event.getFirstTag('r');
    if (jobId == null) return;

    final job = await _store.getJob(jobId);
    if (job == null) return;

    final payload = await _decryptedPayload(event, job.pubkey);
    if (payload == null) return;
    await _processFeedbackPayload(event, payload, job: job);
  }

  Future<void> _onDeletionEvent(Nip01Event event) async {
    for (final requestEventId in event.getTags('e')) {
      await _store.putTombstone(requestEventId, deletionEventId: event.id);
      await _removeRequestFromJobs(requestEventId, event.pubKey);
      await _store.removePackageByManifestEventId(requestEventId);
    }
  }

  /// Removes the request identified by [requestEventId] from [pubkey]'s job.
  /// The job itself is removed once its last request is gone.
  Future<void> _removeRequestFromJobs(
    String requestEventId,
    String pubkey,
  ) async {
    final job = (await _store.listJobs(pubkey))
        .where((job) => job.requestForEventId(requestEventId) != null)
        .firstOrNull;
    if (job == null) return;

    job.requests.removeWhere((r) => r.requestEventId == requestEventId);
    if (job.requests.isEmpty) {
      await _store.removeJob(job.jobId);
    } else {
      job.updatedAt = DateTime.now().millisecondsSinceEpoch ~/ 1000;
      await _store.putJob(job);
    }
  }

  // --------------------------------------------------------------------------
  // Internals - Decryption
  // --------------------------------------------------------------------------

  /// The plaintext of [event] as seen by [pubkey], from raw when already
  /// known. Queues the event for [decryptPending] when no signer can read it.
  Future<String?> _decryptedPayload(Nip01Event event, String pubkey) async {
    final known = await _store.getDecryptedPayload(event.id);
    if (known != null) return known;

    final counterparty = _counterpartyOf(event);
    if (counterparty == null) return null;

    String? decrypted;
    final signer = _signerOrNull(pubkey);
    if (signer != null) {
      try {
        decrypted = await signer.decryptNip44(
          ciphertext: event.content,
          senderPubKey: counterparty,
        );
      } catch (_) {
        decrypted = null;
      }
    }

    if (decrypted == null) {
      await _store.addPendingDecryption(event.id, pubkey: pubkey);
      return null;
    }

    await _store.putDecryptedPayload(event.id, decrypted);
    await _store.removePendingDecryption(event.id);
    return decrypted;
  }

  /// The pubkey [event] was NIP-44 encrypted against.
  String? _counterpartyOf(Nip01Event event) => switch (event.kind) {
    kindScheduleRequest => event.getFirstTag('p'),
    kindFeedback => event.getFirstTag('ephemeral-pubkey'),
    kindPackageManifest => event.pubKey,
    _ => null,
  };

  // --------------------------------------------------------------------------
  // Internals - Payload processing
  // --------------------------------------------------------------------------

  Future<void> _processSchedulePayload(Nip01Event event, String payload) async {
    try {
      if (await _store.isTombstoned(event.id)) return;

      final json = jsonDecode(payload) as Map<String, dynamic>;
      final jobId = json['job_id'] as String;
      final signedEventMap = json['signed_event'] as Map<String, dynamic>;

      final now = DateTime.now().millisecondsSinceEpoch ~/ 1000;
      final request = ScheduledJobRequest(
        dvmPubkey: event.getFirstTag('p') ?? '',
        requestEventId: event.id,
        updatedAt: now,
      );

      // Requests sharing a job_id schedule the same event: merge them.
      final existing = await _store.getJob(jobId);
      ScheduledJob job;
      if (existing != null) {
        if (existing.requestForEventId(event.id) != null) return;
        existing.requests.add(request);
        existing.updatedAt = now;
        job = existing;
      } else {
        job = ScheduledJob(
          pubkey: event.pubKey,
          jobId: jobId,
          scheduleAt: json['schedule_at'] as int,
          targetEvent: _eventFromJson(signedEventMap),
          targetRelays: (json['relays'] as List<dynamic>)
              .map((e) => e as String)
              .toList(),
          requests: [request],
          createdAt: now,
          updatedAt: now,
        );
      }

      await _store.putJob(job);
      await _applyCachedFeedbacks(job);
    } catch (_) {
      // Invalid payload, ignore
    }
  }

  Future<void> _processPackagePayload(Nip01Event event, String content) async {
    try {
      if (await _store.isTombstoned(event.id)) return;

      final packageId = event.getFirstTag('d');
      if (packageId == null || packageId.isEmpty) return;
      if (event.getFirstTag('k') != '$kindScheduleRequest') return;

      final requestEventIds = event.getTags('e');
      if (requestEventIds.isEmpty) return;

      final existing = await _store.getPackage(packageId);
      if (existing != null && existing.manifestEventId == event.id) return;

      final now = DateTime.now().millisecondsSinceEpoch ~/ 1000;
      final jobs = (await _store.listJobs(event.pubKey))
          .where((job) => job.requestEventIds.any(requestEventIds.contains))
          .toList();

      await _store.putPackage(
        ScheduledPackage(
          pubkey: event.pubKey,
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

  Future<void> _processFeedbackPayload(
    Nip01Event event,
    String payload, {
    required ScheduledJob job,
    bool notify = true,
  }) async {
    try {
      final json = jsonDecode(payload) as Map<String, dynamic>;
      final status = JobStatus.values.byName(json['status'] as String);
      final message = json['message'] as String?;

      // The kind:7000 is signed by the DVM, so its pubkey attributes the
      // feedback to one of the job's requests.
      final request = job.requestForDvm(event.pubKey);
      if (request == null) return;

      request.status = status;
      request.lastMessage = message;
      request.updatedAt = DateTime.now().millisecondsSinceEpoch ~/ 1000;
      job.updatedAt = request.updatedAt;
      await _store.putJob(job);

      if (notify) {
        _statusController.add(
          StatusUpdate(
            pubkey: job.pubkey,
            jobId: job.jobId,
            dvmPubkey: event.pubKey,
            status: status,
            message: message,
            receivedAt: DateTime.now(),
          ),
        );
      }
    } catch (_) {
      // Invalid payload, ignore
    }
  }

  /// Applies feedbacks already sitting in the NDK cache to [job].
  ///
  /// Covers requests learned after their feedback (multi-device sync or
  /// rebuild). Feedbacks are applied oldest first so every request converges
  /// to its latest known status.
  Future<void> _applyCachedFeedbacks(ScheduledJob job) async {
    final feedbacks = await _ndk.config.cache.loadEvents(
      kinds: [kindFeedback],
      tags: {
        'r': [job.jobId],
      },
    );
    if (feedbacks.isEmpty) return;

    feedbacks.sort((a, b) => a.createdAt.compareTo(b.createdAt));
    for (final feedback in feedbacks) {
      final payload = await _decryptedPayload(feedback, job.pubkey);
      if (payload == null) continue;
      await _processFeedbackPayload(feedback, payload, job: job, notify: false);
    }
  }

  // --------------------------------------------------------------------------
  // Internals - Feedback subscription
  // --------------------------------------------------------------------------

  void _scheduleFeedbackSubscriptionUpdate(String pubkey) {
    final state = _sync[pubkey];
    if (state == null) return;

    state.feedbackUpdateTimer?.cancel();
    state.feedbackUpdateTimer = Timer(const Duration(milliseconds: 500), () {
      _updateFeedbackSubscription(pubkey);
    });
  }

  Future<void> _updateFeedbackSubscription(String pubkey) async {
    final state = _sync[pubkey];
    if (state == null) return;

    await state.closeFeedback(_ndk);

    final jobIds = (await _store.listJobs(pubkey)).map((j) => j.jobId).toList();
    if (jobIds.isEmpty) return;

    final response = _ndk.requests.subscription(
      filter: _feedbackFilter(jobIds),
      cacheWrite: true,
    );
    state.feedbackRequestId = response.requestId;
    state.feedbackSubscription = response.stream.listen(_onRawEvent);
  }

  // --------------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------------

  /// Creates one job for [dvmPubkeys]: a single job_id and payload, one
  /// signed kind:5905 request per DVM.
  Future<_CreatedJob> _createJob(
    Nip01Event event,
    List<String> dvmPubkeys, {
    required String pubkey,
    DateTime? at,
    List<String>? relays,
    List<String>? dvmReadRelays,
  }) async {
    final signer = _requireSigner(pubkey);

    final dvms = {...dvmPubkeys}.toList();
    if (dvms.isEmpty) {
      throw ArgumentError('At least one DVM pubkey is required');
    }

    final jobId = _generateJobId();
    final scheduleAt =
        (at ?? DateTime.fromMillisecondsSinceEpoch(event.createdAt * 1000))
            .millisecondsSinceEpoch ~/
        1000;

    final targetRelays = await _targetRelays(pubkey, relays);
    if (targetRelays.isEmpty) {
      throw StateError('No target relays found for scheduled event');
    }

    final payload = jsonEncode({
      'job_id': jobId,
      'schedule_at': scheduleAt,
      'signed_event': _eventToJson(event),
      'relays': targetRelays,
    });

    final now = DateTime.now().millisecondsSinceEpoch ~/ 1000;
    final requests = <_CreatedJobRequest>[];
    for (final dvmPubkey in dvms) {
      final encrypted = await signer.encryptNip44(
        plaintext: payload,
        recipientPubKey: dvmPubkey,
      );
      if (encrypted == null) throw StateError('Failed to encrypt payload');

      final requestEvent = Nip01Event(
        pubKey: pubkey,
        kind: kindScheduleRequest,
        tags: [
          ['p', dvmPubkey],
          ['encrypted'],
        ],
        content: encrypted,
        createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
      );
      requests.add(
        _CreatedJobRequest(
          dvmPubkey: dvmPubkey,
          event: await signer.sign(requestEvent),
          relaySet: _requestRelaySet(pubkey, dvmPubkey, dvmReadRelays),
        ),
      );
    }

    final job = ScheduledJob(
      pubkey: pubkey,
      jobId: jobId,
      scheduleAt: scheduleAt,
      targetEvent: event,
      targetRelays: targetRelays,
      requests: [
        for (final request in requests)
          ScheduledJobRequest(
            dvmPubkey: request.dvmPubkey,
            requestEventId: request.event.id,
            updatedAt: now,
          ),
      ],
      createdAt: now,
      updatedAt: now,
    );

    return _CreatedJob(job: job, requests: requests, payload: payload);
  }

  Future<Nip01Event> _signDeletion({
    required String pubkey,
    required Iterable<String> eventIds,
    required List<int> kinds,
    required String content,
  }) async {
    final signer = _requireSigner(pubkey);
    final deletion = Nip01Event(
      pubKey: pubkey,
      kind: kindDeletion,
      tags: [
        for (final eventId in eventIds) ['e', eventId],
        for (final kind in kinds) ['k', '$kind'],
      ],
      content: content,
      createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
    );
    final signed = await signer.sign(deletion);
    await _ndk.config.cache.saveEvent(signed);
    return signed;
  }

  /// Writes a locally created event and its plaintext to the raw tier.
  ///
  /// The broadcast shim may hold the event offline for a long time, so the
  /// scheduler cannot wait for the network to make it part of raw.
  Future<void> _saveRaw(Nip01Event event, String payload) async {
    await _ndk.config.cache.saveEvent(event);
    await _store.putDecryptedPayload(event.id, payload);
  }

  Future<List<String>> _targetRelays(
    String pubkey,
    List<String>? relays,
  ) async {
    if (relays != null && relays.isNotEmpty) {
      return relays;
    }

    final userRelayList = await _ndk.userRelayLists.getSingleUserRelayList(
      pubkey,
    );
    return userRelayList?.writeUrls.toList() ?? [];
  }

  /// Where a kind:5905 request goes: the account's own relays plus the read
  /// relays the DVM publishes, with [dvmReadRelays] as a last resort.
  RelaySet _requestRelaySet(
    String pubkey,
    String dvmPubkey,
    List<String>? dvmReadRelays,
  ) {
    return RelaySet.union([
      RelaySet.nip65(pubkey),
      RelaySet.fallback([
        RelaySet.inbox([dvmPubkey]),
        RelaySet.explicit(dvmReadRelays ?? const []),
      ]),
    ]);
  }

  RelaySet _deletionRelaySet(String pubkey, Iterable<String> dvmPubkeys) {
    return RelaySet.union([
      RelaySet.nip65(pubkey),
      RelaySet.inbox([
        for (final dvmPubkey in {...dvmPubkeys})
          if (dvmPubkey.isNotEmpty) dvmPubkey,
      ]),
    ]);
  }

  Future<String?> _dvmPubkeyForRequestEventId(
    String pubkey,
    String requestEventId,
  ) async {
    final job = (await _store.listJobs(pubkey))
        .where((job) => job.requestForEventId(requestEventId) != null)
        .firstOrNull;
    final dvmPubkey = job?.requestForEventId(requestEventId)?.dvmPubkey;
    if (dvmPubkey != null && dvmPubkey.isNotEmpty) {
      return dvmPubkey;
    }

    final events = await _ndk.config.cache.loadEvents(
      ids: [requestEventId],
      kinds: [kindScheduleRequest],
    );
    if (events.isEmpty) return null;
    return events.first.getFirstTag('p');
  }

  Future<RelaySet> _packageDeletionRelaySet(
    String pubkey,
    Iterable<String> requestEventIds,
  ) async {
    final dvmPubkeys = <String>{};
    for (final requestEventId in requestEventIds) {
      final dvmPubkey = await _dvmPubkeyForRequestEventId(
        pubkey,
        requestEventId,
      );
      if (dvmPubkey == null || dvmPubkey.isEmpty) continue;
      dvmPubkeys.add(dvmPubkey);
    }
    return _deletionRelaySet(pubkey, dvmPubkeys);
  }

  String? _jobIdOf(String payload) {
    try {
      return (jsonDecode(payload) as Map<String, dynamic>)['job_id'] as String?;
    } catch (_) {
      return null;
    }
  }

  Nip01Event _eventFromJson(Map<String, dynamic> json) {
    return Nip01Event(
      id: json['id'] as String,
      pubKey: json['pubkey'] as String,
      createdAt: json['created_at'] as int,
      kind: json['kind'] as int,
      tags: (json['tags'] as List<dynamic>)
          .map((t) => (t as List<dynamic>).map((e) => e as String).toList())
          .toList(),
      content: json['content'] as String,
      sig: json['sig'] as String,
    );
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

/// Relay subscriptions held for one listened account.
class _AccountSync {
  final List<NdkResponse> responses = [];
  final List<StreamSubscription<Nip01Event>> subscriptions = [];
  String? feedbackRequestId;
  StreamSubscription<Nip01Event>? feedbackSubscription;
  Timer? feedbackUpdateTimer;

  Future<void> dispose(Ndk ndk) async {
    for (final sub in subscriptions) {
      await sub.cancel();
    }
    subscriptions.clear();

    for (final response in responses) {
      await ndk.requests.closeSubscription(response.requestId);
    }
    responses.clear();

    await closeFeedback(ndk);
    feedbackUpdateTimer?.cancel();
    feedbackUpdateTimer = null;
  }

  Future<void> closeFeedback(Ndk ndk) async {
    if (feedbackRequestId != null) {
      await ndk.requests.closeSubscription(feedbackRequestId!);
      feedbackRequestId = null;
    }
    await feedbackSubscription?.cancel();
    feedbackSubscription = null;
  }
}

class _CreatedJob {
  final ScheduledJob job;
  final List<_CreatedJobRequest> requests;
  final String payload;

  _CreatedJob({
    required this.job,
    required this.requests,
    required this.payload,
  });
}

class _CreatedJobRequest {
  final String dvmPubkey;
  final Nip01Event event;
  final RelaySet relaySet;

  _CreatedJobRequest({
    required this.dvmPubkey,
    required this.event,
    required this.relaySet,
  });
}
