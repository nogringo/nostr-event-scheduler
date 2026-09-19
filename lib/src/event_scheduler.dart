import 'dart:async';
import 'dart:convert';
import 'dart:math';

import 'package:broadcast_queue_shim_for_ndk/broadcast_queue_shim_for_ndk.dart';
import 'package:ndk/ndk.dart' hide RelaySet;
import 'package:sembast/sembast.dart' as sembast;
import 'package:sync_engine_shim_for_ndk/sync_engine_shim_for_ndk.dart';

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
  final SyncEngine _syncEngine;
  final SchedulerStore _store;

  final Map<String, _AccountSync> _sync = {};
  final Map<String, _AccountDeclaration> _declarations = {};
  final Map<String, Timer> _feedbackTimers = {};
  final Map<String, Future<void>> _declareLocks = {};
  final Map<String, Future<void>> _building = {};
  final Map<String, Future<void>> _replaying = {};

  final _statusController = StreamController<StatusUpdate>.broadcast();
  final _syncController = StreamController<SyncState>.broadcast();

  /// Creates a new [EventScheduler].
  ///
  /// The caller must provide a started [OfflineBroadcast] instance. It must be
  /// able to resolve relay lists, so build it with [OfflineBroadcast.withNdk]
  /// or pass a `relayListFn`: the scheduler targets NIP-65 relay lists rather
  /// than fixed URLs.
  ///
  /// The [SyncEngine] must be started too. It is caller-owned and may be
  /// shared with the rest of the app: the scheduler declares its own requests
  /// and only ever forgets those. Disposing the engine, or clearing everything
  /// it persisted, is the caller's call.
  ///
  /// Reading an account's NIP-37 private relays is the caller's job in one
  /// respect: the scheduler only ever looks for its kind:10013 in the NDK
  /// cache, never on a relay. Fetch it like any other relay list of the
  /// account, at login or alongside its NIP-65, and manifests are read from
  /// the private relays as soon as it lands. Without it they are read from the
  /// NIP-65 relays only, which is correct but blind to another device's
  /// private manifests.
  ///
  /// Writing needs nothing: [OfflineBroadcast] resolves the list itself, and
  /// leaves it in the NDK cache for the read side to find.
  EventScheduler({
    required this._ndk,
    required this._broadcast,
    required this._syncEngine,
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

      await _holdSync(pubkey);
      state.syncHeld = true;

      for (final kind in [
        kindScheduleRequest,
        kindPackageManifest,
        kindDeletion,
      ]) {
        final response = _ndk.requests.subscription(
          filter: Filter(authors: [pubkey], kinds: [kind]),
          cacheWrite: true,
          auth: _authOf(pubkey),
        );
        state.responses.add(response);
        state.subscriptions.add(response.stream.listen(_onRawEvent));
      }

      final privateRelays = await _privateSyncRelays(pubkey);
      if (privateRelays.isNotEmpty) {
        final response = _ndk.requests.subscription(
          filter: _manifestFilter(pubkey),
          explicitRelays: privateRelays,
          cacheWrite: true,
          auth: _authOf(pubkey),
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

    _scheduleFeedbackUpdate(pubkey);
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

    final state = _sync.remove(pubkey);
    if (state == null) return;

    _feedbackTimers.remove(pubkey)?.cancel();
    await state.dispose(_ndk);
    if (state.syncHeld) await _releaseSync(pubkey);
  }

  /// Forces a manual resync of [pubkey]'s schedule requests, deletions, and
  /// feedbacks.
  ///
  /// This is the pull to refresh gesture: the sync engine goes to the relays
  /// however fresh its coverage is, where it otherwise revisits on its own.
  /// A failure is reported on [syncState] rather than thrown: losing the
  /// network is expected, and the local state stays usable.
  Future<void> resync({required String pubkey}) async {
    try {
      await _ensureBuilt(pubkey);
      await _holdSync(pubkey);
    } catch (e) {
      _emitSyncError(pubkey, e);
      return;
    }

    try {
      final declared = _declarations[pubkey]?.requests.toList() ?? [];
      for (final request in declared) {
        await _syncEngine.refresh(request.handle);
      }
      await _replay(pubkey);
      _emitSyncState(pubkey);
    } catch (e) {
      _emitSyncError(pubkey, e);
    } finally {
      await _releaseSync(pubkey);
    }
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
  /// the NDK cache, and the sync coverage that would keep them from being
  /// fetched again.
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
    await _forgetCoverage(pubkey, jobIds.toList());
  }

  /// Removes every local trace of every account, unattributed records
  /// included. See [clearLocalAccountData] for what a local reset does and
  /// does not guarantee.
  ///
  /// Only the scheduler's own sync coverage is forgotten: the engine is shared,
  /// so wiping everything it persisted is the caller's call.
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
      await _forgetCoverage(entry.key, entry.value);
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
    _scheduleFeedbackUpdate(pubkey);

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
        relaySet: _manifestRelaySet(pubkey),
        pubkey: pubkey,
      ),
    ]);

    _scheduleFeedbackUpdate(pubkey);

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
      dvmPubkeys: job.dvmPubkeys,
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

    // Two deletions rather than one: the DVMs must see the requests retracted
    // on their inbox relays, where a deletion tagging them all would reveal the
    // package and its size.
    if (package.requestEventIds.isNotEmpty) {
      final dvmPubkeys = await _dvmPubkeysForRequestEventIds(
        pubkey,
        package.requestEventIds,
      );
      final requestDeletion = await _signDeletion(
        pubkey: pubkey,
        eventIds: package.requestEventIds,
        kinds: const [kindScheduleRequest],
        content: 'cancel package',
        dvmPubkeys: dvmPubkeys,
      );
      await _broadcast.broadcast(
        requestDeletion,
        relaySet: _deletionRelaySet(pubkey, dvmPubkeys),
        pubkey: pubkey,
      );

      for (final requestEventId in package.requestEventIds) {
        await _store.putTombstone(
          requestEventId,
          deletionEventId: requestDeletion.id,
        );
        await _removeRequestFromJobs(requestEventId, pubkey);
      }
    }

    final manifestDeletion = await _signDeletion(
      pubkey: pubkey,
      eventIds: [package.manifestEventId],
      kinds: const [kindPackageManifest],
      content: 'cancel package',
    );
    await _broadcast.broadcast(
      manifestDeletion,
      relaySet: _manifestRelaySet(pubkey),
      pubkey: pubkey,
    );

    await _store.putTombstone(
      package.manifestEventId,
      deletionEventId: manifestDeletion.id,
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

  /// Disposes all resources. The sync engine is caller-owned and outlives this.
  Future<void> dispose() async {
    await stopListening();

    for (final timer in _feedbackTimers.values) {
      timer.cancel();
    }
    _feedbackTimers.clear();

    // A resync still in flight holds declarations of its own.
    for (final declaration in _declarations.values.toList()) {
      await declaration.dispose(_syncEngine);
    }
    _declarations.clear();
    _declareLocks.clear();

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
    await _replayFromCache(pubkey);
    await _store.markBuilt(pubkey);
  }

  /// Serializes [pubkey]'s replays: two passes walking the same cache would
  /// race on the projections they both rebuild.
  Future<void> _replay(String pubkey) {
    final pass = (_replaying[pubkey] ?? Future<void>.value()).then(
      (_) => _replayFromCache(pubkey),
    );
    _replaying[pubkey] = pass.then((_) {}, onError: (_) {});
    return pass;
  }

  /// Rebuilds [pubkey]'s projections from the events sitting in the NDK cache.
  ///
  /// The sync engine hands back handles, never events, so what it lands in the
  /// cache only reaches the projections through here.
  Future<void> _replayFromCache(String pubkey) async {
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

    // Deletions last, so a cancellation landing in the same pass as the
    // request it retracts still wins.
    for (final event in await _ndk.config.cache.loadEvents(
      pubKeys: [pubkey],
      kinds: [kindDeletion],
    )) {
      await _onRawEvent(event);
    }

    final jobIds = (await _store.listJobs(pubkey)).map((j) => j.jobId).toList();
    if (jobIds.isEmpty) return;
    for (final event in await _ndk.config.cache.loadEvents(
      kinds: [kindFeedback],
      tags: {'r': jobIds},
    )) {
      await _onRawEvent(event);
    }
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

  Filter _ownFilter(String pubkey) => Filter(
    authors: [pubkey],
    kinds: [kindScheduleRequest, kindPackageManifest, kindDeletion],
  );

  /// The manifest branch, read on the private relays. [_ownFilter] still reads
  /// kind:31234 on the NIP-65 relays, for the manifests written before the
  /// account had a kind:10013, or by a client without NIP-37.
  Filter _manifestFilter(String pubkey) =>
      Filter(authors: [pubkey], kinds: [kindPackageManifest, kindDeletion]);

  /// Serializes [pubkey]'s declarations: two of them racing would register the
  /// same request twice and leave a handle no release can match.
  Future<void> _declaring(String pubkey, Future<void> Function() body) {
    final pass = (_declareLocks[pubkey] ?? Future<void>.value()).then(
      (_) => body(),
    );
    _declareLocks[pubkey] = pass.then((_) {}, onError: (_) {});
    return pass;
  }

  /// Declares [pubkey]'s sync requests and takes a hold on them. Cheap to call
  /// again: the engine hands the same handles back.
  Future<void> _holdSync(String pubkey) =>
      _declaring(pubkey, () => _holdSyncNow(pubkey));

  Future<void> _holdSyncNow(String pubkey) async {
    final declaration = _declarations.putIfAbsent(
      pubkey,
      _AccountDeclaration.new,
    );
    declaration.holders++;

    if (declaration.own == null) {
      final relays = await _ownSyncRelays(pubkey);
      declaration.own ??= _register(
        pubkey,
        SyncRequest(
          filters: [_ownFilter(pubkey)],
          relays: relays,
          authPubkey: pubkey,
        ),
      );
    }
    if (declaration.manifests == null) {
      final relays = await _privateSyncRelays(pubkey);
      if (relays.isNotEmpty) {
        declaration.manifests = _register(
          pubkey,
          SyncRequest(
            filters: [_manifestFilter(pubkey)],
            relays: relays,
            authPubkey: pubkey,
          ),
        );
      }
    }
    await _declareFeedbackNow(pubkey);
  }

  Future<void> _releaseSync(String pubkey) =>
      _declaring(pubkey, () => _releaseSyncNow(pubkey));

  Future<void> _releaseSyncNow(String pubkey) async {
    final declaration = _declarations[pubkey];
    if (declaration == null) return;

    declaration.holders--;
    if (declaration.holders > 0) return;

    _declarations.remove(pubkey);
    await declaration.dispose(_syncEngine);
  }

  /// Declares the feedback request of [pubkey], or redeclares it when the jobs
  /// it follows changed: the job ids are part of what identifies a request, so
  /// a new set is a new handle rather than an update of the held one.
  Future<void> _declareFeedbackNow(String pubkey) async {
    if (_declarations[pubkey] == null) return;

    final jobIds = (await _store.listJobs(pubkey)).map((j) => j.jobId).toList()
      ..sort();
    final relays = jobIds.isEmpty
        ? const <String>[]
        : await _feedbackSyncRelays(pubkey);

    final declaration = _declarations[pubkey];
    if (declaration == null) return;

    final existing = declaration.feedback;
    if (existing != null &&
        _sameStrings(existing.request.filters.first.tags?['#r'], jobIds) &&
        _sameStrings(existing.request.relays, relays)) {
      return;
    }

    if (existing != null) {
      declaration.feedback = null;
      await existing.dispose(_syncEngine);
    }
    if (jobIds.isEmpty || relays.isEmpty) return;

    declaration.feedback = _register(
      pubkey,
      SyncRequest(
        filters: [_feedbackFilter(jobIds)],
        relays: relays,
        authPubkey: pubkey,
      ),
    );
  }

  _DeclaredRequest _register(String pubkey, SyncRequest request) {
    final handle = _syncEngine.ensure(request);
    late final _DeclaredRequest declared;
    declared = _DeclaredRequest(
      handle: handle,
      request: request,
      subscription: _syncEngine.watchStatus(handle).listen((status) {
        _emitSyncState(pubkey);

        final progress = status.progress;
        if (progress == null || progress.eventCount == 0) return;
        if (identical(progress, declared.lastProgress)) return;
        declared.lastProgress = progress;
        unawaited(_replay(pubkey));
      }),
    );
    return declared;
  }

  Future<void> _forgetCoverage(String pubkey, List<String> jobIds) async {
    await _syncEngine.forgetFilter(_ownFilter(pubkey), authPubkey: pubkey);
    await _syncEngine.forgetFilter(_manifestFilter(pubkey), authPubkey: pubkey);
    if (jobIds.isEmpty) return;
    await _syncEngine.forgetFilter(
      _feedbackFilter(jobIds..sort()),
      authPubkey: pubkey,
    );
  }

  /// Where [pubkey]'s own scheduler events were published.
  Future<List<String>> _ownSyncRelays(String pubkey) async {
    final relayList = await _ndk.userRelayLists.getSingleUserRelayList(pubkey);
    return _orBootstrap(relayList?.writeUrls);
  }

  /// [pubkey]'s NIP-37 private relays, where its manifests go. Empty when the
  /// NDK cache holds no kind:10013 for it, or when no signer can open the one
  /// it holds.
  ///
  /// The cache is the only source, so this costs nothing and is re-read on
  /// every declaration: a list that lands in the cache is used at once,
  /// whoever put it there. Keeping it there is the caller's job, see
  /// [EventScheduler.new].
  Future<List<String>> _privateSyncRelays(String pubkey) async {
    final lists = await _ndk.config.cache.loadEvents(
      pubKeys: [pubkey],
      kinds: [kindPrivateRelays],
    );
    if (lists.isEmpty) return const [];

    final list = lists.reduce((a, b) => b.createdAt > a.createdAt ? b : a);
    if (list.content.isEmpty) return const [];

    final payloads = _ndk.decryptedEventPayloads;
    var plaintext = await payloads.loadCachedPlaintext(
      eventId: list.id,
      viewerPubKey: pubkey,
    );
    if (plaintext == null) {
      final signer = _signerOrNull(pubkey);
      if (signer == null) return const [];
      try {
        plaintext = await payloads.loadOrDecrypt(
          event: list,
          viewerPubKey: pubkey,
          scheme: DecryptedPayloadScheme.nip44,
          decrypt: () => signer.decryptNip44(
            ciphertext: list.content,
            senderPubKey: list.pubKey,
          ),
        );
      } catch (_) {
        return const [];
      }
    }
    return plaintext == null ? const [] : _relaysOfPrivateTags(plaintext);
  }

  /// Relay URLs of the decrypted tags of a kind:10013. Anything that is not a
  /// JSON list of tags yields no relay.
  List<String> _relaysOfPrivateTags(String plaintext) {
    final Object? tags;
    try {
      tags = jsonDecode(plaintext);
    } on FormatException {
      return const [];
    }
    if (tags is! List) return const [];
    return [
      for (final tag in tags)
        if (tag is List &&
            tag.length >= 2 &&
            tag[0] == 'relay' &&
            tag[1] is String)
          tag[1] as String,
    ]..sort();
  }

  /// Where the DVMs of [pubkey]'s jobs publish their kind:7000: their own
  /// NIP-65 write relays, a feedback carrying no `p` tag to route on.
  Future<List<String>> _feedbackSyncRelays(String pubkey) async {
    final dvmPubkeys = {
      for (final job in await _store.listJobs(pubkey))
        for (final request in job.requests)
          if (request.dvmPubkey.isNotEmpty) request.dvmPubkey,
    };

    final relays = <String>{};
    for (final dvmPubkey in dvmPubkeys) {
      final relayList = await _ndk.userRelayLists.getSingleUserRelayList(
        dvmPubkey,
      );
      relays.addAll(relayList?.writeUrls ?? const []);
    }
    return _orBootstrap(relays);
  }

  List<String> _orBootstrap(Iterable<String>? relays) {
    final urls = (relays?.toList() ?? [])..sort();
    return urls.isEmpty ? [..._ndk.config.bootstrapRelays] : urls;
  }

  bool _sameStrings(Iterable<String>? a, Iterable<String>? b) {
    final left = a?.toList() ?? const [];
    final right = b?.toList() ?? const [];
    if (left.length != right.length) return false;
    for (var i = 0; i < left.length; i++) {
      if (left[i] != right[i]) return false;
    }
    return true;
  }

  RelayAuth? _authOf(String pubkey) {
    final account = _ndk.accounts.accounts[pubkey];
    return account == null ? null : RelayAuth.require(account);
  }

  void _emitSyncError(String pubkey, Object error) {
    _syncController.add(
      SyncState(
        pubkey: pubkey,
        status: SyncStatus.error,
        error: error.toString(),
      ),
    );
  }

  /// Reports [pubkey]'s sync state from the phases of its declared requests,
  /// so the engine's own passes surface too and not just a manual [resync].
  void _emitSyncState(String pubkey) {
    final declaration = _declarations[pubkey];
    if (declaration == null) return;

    final statuses = [
      for (final declared in declaration.requests)
        _syncEngine.status(declared.handle),
    ];
    if (statuses.isEmpty) return;

    final failed = statuses
        .where((s) => s.phase == SyncRequestPhase.failed)
        .firstOrNull;
    final SyncState state;
    if (failed != null) {
      state = SyncState(
        pubkey: pubkey,
        status: SyncStatus.error,
        error: (failed.lastError ?? 'sync failed').toString(),
      );
    } else if (statuses.any((s) => s.phase == SyncRequestPhase.syncing)) {
      state = SyncState(pubkey: pubkey, status: SyncStatus.syncing);
    } else if (statuses.every((s) => s.phase == SyncRequestPhase.synced)) {
      state = SyncState(
        pubkey: pubkey,
        status: SyncStatus.synced,
        lastSyncAt: DateTime.now(),
      );
    } else {
      return;
    }

    if (declaration.lastStatus == state.status &&
        declaration.lastError == state.error) {
      return;
    }
    declaration.lastStatus = state.status;
    declaration.lastError = state.error;
    _syncController.add(state);
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
  ///
  /// A feedback whose payload is already known was seen before, and replaying
  /// the cache must not notify a second time for it.
  Future<void> _onFeedbackEvent(Nip01Event event) async {
    final jobId = event.getFirstTag('r');
    if (jobId == null) return;

    final job = await _store.getJob(jobId);
    if (job == null) return;

    final known = await _store.getDecryptedPayload(event.id);
    final payload = known ?? await _decryptedPayload(event, job.pubkey);
    if (payload == null) return;
    await _processFeedbackPayload(
      event,
      payload,
      job: job,
      notify: known == null,
    );
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
      if (!request.isSupersededBy(event.createdAt, status)) return;

      request.status = status;
      request.lastMessage = message;
      request.feedbackAt = event.createdAt;
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

  void _scheduleFeedbackUpdate(String pubkey) {
    _feedbackTimers[pubkey]?.cancel();
    _feedbackTimers[pubkey] = Timer(const Duration(milliseconds: 500), () {
      _feedbackTimers.remove(pubkey);
      unawaited(_updateFeedbackTargets(pubkey));
    });
  }

  /// Retargets what follows [pubkey]'s jobs after the set of jobs changed: the
  /// live subscription and, through it, the feedback sync request.
  Future<void> _updateFeedbackTargets(String pubkey) async {
    await _declaring(pubkey, () => _declareFeedbackNow(pubkey));

    final state = _sync[pubkey];
    if (state == null) return;

    await state.closeFeedback(_ndk);

    final jobIds = (await _store.listJobs(pubkey)).map((j) => j.jobId).toList();
    if (jobIds.isEmpty) return;

    final response = _ndk.requests.subscription(
      filter: _feedbackFilter(jobIds),
      cacheWrite: true,
      auth: _authOf(pubkey),
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
    Iterable<String> dvmPubkeys = const [],
  }) async {
    final signer = _requireSigner(pubkey);
    final deletion = Nip01Event(
      pubKey: pubkey,
      kind: kindDeletion,
      tags: [
        for (final eventId in eventIds) ['e', eventId],
        for (final kind in kinds) ['k', '$kind'],
        for (final dvmPubkey in {...dvmPubkeys})
          if (dvmPubkey.isNotEmpty) ['p', dvmPubkey],
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

  /// Where a kind:31234 manifest and the kind:5 retracting it go: the NIP-37
  /// private relays of [pubkey], or its NIP-65 when it publishes none. One set
  /// and never both, so a public relay never learns a package exists.
  RelaySet _manifestRelaySet(String pubkey) {
    return RelaySet.fallback([
      RelaySet.private(pubkey),
      RelaySet.nip65(pubkey),
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

  Future<Set<String>> _dvmPubkeysForRequestEventIds(
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
    return dvmPubkeys;
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

  /// Whether listening took a hold on the account's sync requests.
  bool syncHeld = false;

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

/// Sync requests declared for one account, held until the last holder goes.
class _AccountDeclaration {
  int holders = 0;
  _DeclaredRequest? own;
  _DeclaredRequest? manifests;
  _DeclaredRequest? feedback;

  /// Last state pushed on `syncState`, so a stream of progress pages does not
  /// repeat it.
  SyncStatus? lastStatus;
  String? lastError;

  Iterable<_DeclaredRequest> get requests => [?own, ?manifests, ?feedback];

  Future<void> dispose(SyncEngine engine) async {
    for (final declared in requests) {
      await declared.dispose(engine);
    }
    own = null;
    manifests = null;
    feedback = null;
  }
}

/// One registered [SyncRequest], with what it was declared from.
class _DeclaredRequest {
  final SyncHandle handle;
  final SyncRequest request;
  final StreamSubscription<SyncRequestStatus> subscription;

  /// The last page reconciled, so a status re-emitting it does not replay the
  /// cache a second time.
  SyncProgress? lastProgress;

  _DeclaredRequest({
    required this.handle,
    required this.request,
    required this.subscription,
  });

  Future<void> dispose(SyncEngine engine) async {
    await subscription.cancel();
    engine.release(handle);
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
