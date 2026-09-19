import 'dart:async';
import 'dart:convert';
import 'dart:math';

import 'package:broadcast_queue_shim_for_ndk/broadcast_queue_shim_for_ndk.dart';
import 'package:ndk/ndk.dart';
import 'package:ndk/shared/nips/nip01/bip340.dart';
import 'package:ndk/shared/nips/nip01/key_pair.dart';
import 'package:ndk/shared/nips/nip44/nip44.dart';
import 'package:ndk/domain_layer/entities/nip_65.dart';
import 'package:ndk/domain_layer/entities/read_write_marker.dart';
import 'mocks/mock_relay.dart';
import 'package:nostr_event_scheduler/nostr_event_scheduler.dart';
import 'package:nostr_scheduler_dvm/nostr_scheduler_dvm.dart';
import 'package:sembast/sembast.dart' as sembast;
import 'package:sembast/sembast_memory.dart' hide Filter;
import 'package:sync_engine_shim_for_ndk/sync_engine_shim_for_ndk.dart';
import 'package:test/test.dart';

Future<(EventScheduler, SyncEngine)> createScheduler({
  required Ndk ndk,
  required Database broadcastDb,
  required Database schedulerDb,
  required List<String> relayListDiscoveryRelays,
}) async {
  final broadcast = OfflineBroadcast.withNdk(
    ndk,
    db: broadcastDb,
    relayListDiscoveryRelays: relayListDiscoveryRelays,
  );
  broadcast.start();

  final syncEngine = SyncEngine(ndk, db: schedulerDb);
  syncEngine.start();

  final scheduler = EventScheduler(
    ndk: ndk,
    broadcast: broadcast,
    syncEngine: syncEngine,
    db: schedulerDb,
  );

  return (scheduler, syncEngine);
}

Future<void> _waitFor(
  FutureOr<bool> Function() condition, {
  Duration timeout = const Duration(seconds: 5),
}) async {
  final deadline = DateTime.now().add(timeout);
  while (DateTime.now().isBefore(deadline)) {
    if (await condition()) return;
    await Future<void>.delayed(const Duration(milliseconds: 50));
  }
  throw TimeoutException('Condition not met after $timeout');
}

void main() {
  late MockRelay relay;
  late KeyPair clientKey;
  late KeyPair client2Key;
  late KeyPair dvmKey;
  late KeyPair dvm2Key;
  late Ndk ndk;
  late Database broadcastDb;
  late Database schedulerDb;
  late EventScheduler scheduler;
  late SyncEngine syncEngine;

  Future<List<Nip01Event>> relayQueryOn(MockRelay target, Filter filter) {
    return ndk.requests
        .query(
          filter: filter,
          explicitRelays: [target.url],
          cacheRead: false,
          cacheWrite: false,
        )
        .future;
  }

  Future<List<Nip01Event>> relayQuery(Filter filter) =>
      relayQueryOn(relay, filter);

  // The shim resolves relay sets in the background, so a clear racing the
  // first attempt would see the request written back to the NDK cache.
  Future<void> waitForRequestsOnRelay(int count) => _waitFor(
    () async => (await relayQuery(Filter(kinds: [5905]))).length >= count,
  );

  Future<void> publishFeedback({
    required KeyPair dvm,
    required String jobId,
    required String status,
    String? message,
    int? createdAt,
  }) async {
    final ephemeralKey = Bip340.generatePrivateKey();
    final payload = jsonEncode({'status': status, 'message': ?message});
    final encrypted = await Nip44.encryptMessage(
      payload,
      ephemeralKey.privateKey!,
      clientKey.publicKey,
    );
    final feedbackEvent = Nip01Event(
      pubKey: dvm.publicKey,
      kind: 7000,
      tags: [
        ['r', jobId],
        ['ephemeral-pubkey', ephemeralKey.publicKey],
      ],
      content: encrypted,
      createdAt: createdAt ?? DateTime.now().millisecondsSinceEpoch ~/ 1000,
    );
    final signedFeedback = Nip01Utils.signWithPrivateKey(
      event: feedbackEvent,
      privateKey: dvm.privateKey!,
    );
    await ndk.broadcast
        .broadcast(nostrEvent: signedFeedback, specificRelays: [relay.url])
        .broadcastDoneFuture;
  }

  /// Puts an encrypted kind:5905 on the relay behind the scheduler's back, as
  /// another device would, and answers its job id.
  Future<String> publishScheduleRequest({required KeyPair dvm}) async {
    final targetEvent = Nip01Event(
      pubKey: clientKey.publicKey,
      kind: 1,
      tags: [],
      content: 'sync test',
      createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
    );
    final signedTarget = await ndk.accounts.getLoggedAccount()!.signer.sign(
      targetEvent,
    );

    final jobId = List.generate(
      32,
      (_) => Random.secure().nextInt(256).toRadixString(16).padLeft(2, '0'),
    ).join();

    final payload = jsonEncode({
      'job_id': jobId,
      'schedule_at': signedTarget.createdAt,
      'signed_event': {
        'id': signedTarget.id,
        'pubkey': signedTarget.pubKey,
        'created_at': signedTarget.createdAt,
        'kind': signedTarget.kind,
        'tags': signedTarget.tags,
        'content': signedTarget.content,
        'sig': signedTarget.sig,
      },
      'relays': [relay.url],
    });

    final encrypted = await Nip44.encryptMessage(
      payload,
      clientKey.privateKey!,
      dvm.publicKey,
    );

    final requestEvent = Nip01Event(
      pubKey: clientKey.publicKey,
      kind: 5905,
      tags: [
        ['p', dvm.publicKey],
        ['encrypted'],
      ],
      content: encrypted,
      createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
    );
    final signedRequest = await ndk.accounts.getLoggedAccount()!.signer.sign(
      requestEvent,
    );

    await ndk.broadcast
        .broadcast(nostrEvent: signedRequest, specificRelays: [relay.url])
        .broadcastDoneFuture;

    return jobId;
  }

  setUp(() async {
    relay = MockRelay(name: 'test relay', explicitPort: 9090);

    clientKey = Bip340.generatePrivateKey();
    client2Key = Bip340.generatePrivateKey();
    dvmKey = Bip340.generatePrivateKey();
    dvm2Key = Bip340.generatePrivateKey();

    // Serve NIP-65s so the scheduler can find relays for broadcast
    final nip65 = Nip65(
      pubKey: clientKey.publicKey,
      relays: {relay.url: ReadWriteMarker.readWrite},
      createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
    );
    final client2Nip65 = Nip65(
      pubKey: client2Key.publicKey,
      relays: {relay.url: ReadWriteMarker.readWrite},
      createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
    );
    final dvmNip65 = Nip65(
      pubKey: dvmKey.publicKey,
      relays: {relay.url: ReadWriteMarker.readOnly},
      createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
    );
    final dvm2Nip65 = Nip65(
      pubKey: dvm2Key.publicKey,
      relays: {relay.url: ReadWriteMarker.readOnly},
      createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
    );
    await relay.startServer(
      nip65s: {
        clientKey: nip65,
        client2Key: client2Nip65,
        dvmKey: dvmNip65,
        dvm2Key: dvm2Nip65,
      },
    );

    ndk = Ndk(
      NdkConfig(
        eventVerifier: Bip340EventVerifier(),
        cache: MemCacheManager(),
        bootstrapRelays: [relay.url],
      ),
    );
    ndk.accounts.loginPrivateKey(
      pubkey: clientKey.publicKey,
      privkey: clientKey.privateKey!,
    );

    final dbSuffix =
        '${DateTime.now().microsecondsSinceEpoch}_${Random.secure().nextInt(1 << 32)}';
    broadcastDb = await databaseFactoryMemory.openDatabase(
      'broadcast_test_$dbSuffix.db',
    );
    schedulerDb = await databaseFactoryMemory.openDatabase(
      'scheduler_test_$dbSuffix.db',
    );
    (scheduler, syncEngine) = await createScheduler(
      ndk: ndk,
      broadcastDb: broadcastDb,
      schedulerDb: schedulerDb,
      relayListDiscoveryRelays: [relay.url],
    );
  });

  tearDown(() async {
    await scheduler.dispose();
    await syncEngine.dispose();
    await ndk.destroy();
    await relay.stopServer();
    await broadcastDb.close();
    await schedulerDb.close();
  });

  group('schedule', () {
    test('broadcasts a kind:5905 event', () async {
      final event = Nip01Event(
        pubKey: clientKey.publicKey,
        kind: 1,
        tags: [],
        content: 'hello world',
        createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
      );
      final signedEvent = await ndk.accounts.getLoggedAccount()!.signer.sign(
        event,
      );

      final job = await scheduler.schedule(
        signedEvent,
        [dvmKey.publicKey],
        relays: [relay.url],
        pubkey: clientKey.publicKey,
      );

      expect(job.jobId, isNotEmpty);
      expect(job.status, JobStatus.pending);

      // Give the shim time to broadcast
      await Future.delayed(const Duration(milliseconds: 500));

      // Verify the kind:5905 was received by the relay
      final stored = await relayQuery(
        Filter(kinds: [5905], authors: [clientKey.publicKey]),
      );
      expect(stored, isNotEmpty);

      final requestEvent = stored.first;
      expect(requestEvent.pubKey, clientKey.publicKey);
      expect(requestEvent.getFirstTag('p'), dvmKey.publicKey);
    });
  });

  group('redundant scheduling', () {
    test('broadcasts one kind:5905 per DVM sharing one job_id', () async {
      final event = Nip01Event(
        pubKey: clientKey.publicKey,
        kind: 1,
        tags: [],
        content: 'redundant hello',
        createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
      );
      final signedEvent = await ndk.accounts.getLoggedAccount()!.signer.sign(
        event,
      );

      final job = await scheduler.schedule(
        signedEvent,
        [dvmKey.publicKey, dvm2Key.publicKey],
        relays: [relay.url],
        pubkey: clientKey.publicKey,
      );

      expect(job.requests, hasLength(2));
      expect(
        job.dvmPubkeys,
        containsAll([dvmKey.publicKey, dvm2Key.publicKey]),
      );
      expect(job.status, JobStatus.pending);
      expect(
        await scheduler.listJobs(pubkey: clientKey.publicKey),
        hasLength(1),
      );
      expect(
        await scheduler.listSchedules(pubkey: clientKey.publicKey),
        hasLength(1),
      );

      // Give the shim time to broadcast
      await Future.delayed(const Duration(milliseconds: 500));

      final stored = await relayQuery(
        Filter(kinds: [5905], authors: [clientKey.publicKey]),
      );
      expect(stored, hasLength(2));

      final byDvm = {for (final event in stored) event.getFirstTag('p'): event};
      expect(byDvm.keys, containsAll([dvmKey.publicKey, dvm2Key.publicKey]));

      // Every DVM receives the exact same payload, job_id included
      final payloadA = await Nip44.decryptMessage(
        byDvm[dvmKey.publicKey]!.content,
        dvmKey.privateKey!,
        clientKey.publicKey,
      );
      final payloadB = await Nip44.decryptMessage(
        byDvm[dvm2Key.publicKey]!.content,
        dvm2Key.privateKey!,
        clientKey.publicKey,
      );
      expect(payloadA, payloadB);

      final decoded = jsonDecode(payloadA) as Map<String, dynamic>;
      expect(decoded['job_id'], job.jobId);
      final signedEventMap = decoded['signed_event'] as Map<String, dynamic>;
      expect(signedEventMap['id'], signedEvent.id);
    });

    test('aggregates per-DVM feedback statuses', () async {
      final event = Nip01Event(
        pubKey: clientKey.publicKey,
        kind: 1,
        tags: [],
        content: 'aggregate test',
        createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
      );
      final signedEvent = await ndk.accounts.getLoggedAccount()!.signer.sign(
        event,
      );

      final job = await scheduler.schedule(
        signedEvent,
        [dvmKey.publicKey, dvm2Key.publicKey],
        relays: [relay.url],
        pubkey: clientKey.publicKey,
      );

      await scheduler.startListening(pubkey: clientKey.publicKey);
      await Future.delayed(const Duration(milliseconds: 500));

      final updates = <StatusUpdate>[];
      final sub = scheduler.statusUpdates.listen(updates.add);

      // First DVM fails: the other one is still pending, so the job is too
      await publishFeedback(dvm: dvmKey, jobId: job.jobId, status: 'failed');
      await _waitFor(() async {
        final jobs = await scheduler.listJobs(pubkey: clientKey.publicKey);
        return jobs.single.requestForDvm(dvmKey.publicKey)!.status ==
            JobStatus.failed;
      });
      var current = (await scheduler.listJobs(
        pubkey: clientKey.publicKey,
      )).single;
      expect(current.status, JobStatus.pending);

      // Second DVM accepts: one acceptance is enough
      await publishFeedback(
        dvm: dvm2Key,
        jobId: job.jobId,
        status: 'scheduled',
      );
      await _waitFor(() async {
        final jobs = await scheduler.listJobs(pubkey: clientKey.publicKey);
        return jobs.single.requestForDvm(dvm2Key.publicKey)!.status ==
            JobStatus.scheduled;
      });
      current = (await scheduler.listJobs(pubkey: clientKey.publicKey)).single;
      expect(current.status, JobStatus.scheduled);

      await sub.cancel();

      expect(
        updates.map((update) => update.dvmPubkey),
        containsAll([dvmKey.publicKey, dvm2Key.publicKey]),
      );
    });

    test('cancel tags every kind:5905 request in one kind:5', () async {
      final event = Nip01Event(
        pubKey: clientKey.publicKey,
        kind: 1,
        tags: [],
        content: 'cancel redundant',
        createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
      );
      final signedEvent = await ndk.accounts.getLoggedAccount()!.signer.sign(
        event,
      );

      final job = await scheduler.schedule(
        signedEvent,
        [dvmKey.publicKey, dvm2Key.publicKey],
        relays: [relay.url],
        pubkey: clientKey.publicKey,
      );
      expect(job.requestEventIds, hasLength(2));

      await scheduler.cancel(job.jobId, pubkey: clientKey.publicKey);

      await Future.delayed(const Duration(milliseconds: 500));

      final deletions = await relayQuery(
        Filter(kinds: [5], authors: [clientKey.publicKey]),
      );
      expect(deletions, hasLength(1));
      expect(deletions.single.getTags('e'), containsAll(job.requestEventIds));
      expect(await scheduler.listJobs(pubkey: clientKey.publicKey), isEmpty);
    });

    test('merges kind:5905 requests sharing a job_id from the relay', () async {
      final targetEvent = Nip01Event(
        pubKey: clientKey.publicKey,
        kind: 1,
        tags: [],
        content: 'merge sync test',
        createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
      );
      final signedTarget = await ndk.accounts.getLoggedAccount()!.signer.sign(
        targetEvent,
      );

      final jobId = List.generate(
        32,
        (_) => Random.secure().nextInt(256).toRadixString(16).padLeft(2, '0'),
      ).join();

      final payload = jsonEncode({
        'job_id': jobId,
        'schedule_at': signedTarget.createdAt,
        'signed_event': {
          'id': signedTarget.id,
          'pubkey': signedTarget.pubKey,
          'created_at': signedTarget.createdAt,
          'kind': signedTarget.kind,
          'tags': signedTarget.tags,
          'content': signedTarget.content,
          'sig': signedTarget.sig,
        },
        'relays': [relay.url],
      });

      for (final dvm in [dvmKey, dvm2Key]) {
        final encrypted = await Nip44.encryptMessage(
          payload,
          clientKey.privateKey!,
          dvm.publicKey,
        );
        final requestEvent = Nip01Event(
          pubKey: clientKey.publicKey,
          kind: 5905,
          tags: [
            ['p', dvm.publicKey],
            ['encrypted'],
          ],
          content: encrypted,
          createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
        );
        final signedRequest = await ndk.accounts
            .getLoggedAccount()!
            .signer
            .sign(requestEvent);
        await ndk.broadcast
            .broadcast(nostrEvent: signedRequest, specificRelays: [relay.url])
            .broadcastDoneFuture;
      }

      await scheduler.startListening(pubkey: clientKey.publicKey);
      await scheduler.resync(pubkey: clientKey.publicKey);

      await _waitFor(() async {
        final jobs = await scheduler.listJobs(pubkey: clientKey.publicKey);
        return jobs.length == 1 && jobs.single.requests.length == 2;
      });

      final job = (await scheduler.listJobs(
        pubkey: clientKey.publicKey,
      )).single;
      expect(job.jobId, jobId);
      expect(
        job.dvmPubkeys,
        containsAll([dvmKey.publicKey, dvm2Key.publicKey]),
      );
      expect(
        await scheduler.listSchedules(pubkey: clientKey.publicKey),
        hasLength(1),
      );
    });
  });

  group('schedulePackage', () {
    test('broadcasts package manifest and lists logical schedules', () async {
      final eventA = Nip01Event(
        pubKey: clientKey.publicKey,
        kind: 1,
        tags: [],
        content: 'standalone',
        createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
      );
      final eventB = Nip01Event(
        pubKey: clientKey.publicKey,
        kind: 1,
        tags: [],
        content: 'package B',
        createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
      );
      final eventC = Nip01Event(
        pubKey: clientKey.publicKey,
        kind: 1,
        tags: [],
        content: 'package C',
        createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
      );
      final signer = ndk.accounts.getLoggedAccount()!.signer;
      final signedA = await signer.sign(eventA);
      final signedB = await signer.sign(eventB);
      final signedC = await signer.sign(eventC);

      await scheduler.schedule(
        signedA,
        [dvmKey.publicKey],
        relays: [relay.url],
        pubkey: clientKey.publicKey,
      );
      final package = await scheduler.schedulePackage(
        [
          SchedulePackageItem(
            event: signedB,
            dvmPubkeys: [dvmKey.publicKey],
            relays: [relay.url],
          ),
          SchedulePackageItem(
            event: signedC,
            dvmPubkeys: [dvmKey.publicKey],
            relays: [relay.url],
          ),
        ],
        content: 'opaque display context',
        pubkey: clientKey.publicKey,
      );

      expect(package.jobs, hasLength(2));
      expect(package.content, 'opaque display context');

      await Future.delayed(const Duration(milliseconds: 500));

      final requests = await relayQuery(
        Filter(kinds: [5905], authors: [clientKey.publicKey]),
      );
      expect(requests, hasLength(greaterThanOrEqualTo(3)));

      final manifests = await relayQuery(
        Filter(kinds: [31234], authors: [clientKey.publicKey]),
      );
      expect(manifests, isNotEmpty);
      final manifest = manifests.first;
      expect(manifest.getFirstTag('d'), package.packageId);
      expect(manifest.getFirstTag('k'), '5905');
      expect(manifest.getTags('e'), containsAll(package.requestEventIds));

      final decrypted = await Nip44.decryptMessage(
        manifest.content,
        clientKey.privateKey!,
        clientKey.publicKey,
      );
      expect(decrypted, 'opaque display context');

      final jobs = await scheduler.listJobs(pubkey: clientKey.publicKey);
      expect(jobs, hasLength(3));

      final schedules = await scheduler.listSchedules(
        pubkey: clientKey.publicKey,
      );
      expect(schedules, hasLength(2));
      expect(
        schedules.where((item) => item.type == ScheduledItemType.package),
        hasLength(1),
      );
      expect(
        schedules.where((item) => item.type == ScheduledItemType.job),
        hasLength(1),
      );
    });

    test('supports DVM read relay fallback per package item', () async {
      final fallbackDvmKey = Bip340.generatePrivateKey();

      final event = Nip01Event(
        pubKey: clientKey.publicKey,
        kind: 1,
        tags: [],
        content: 'fallback dvm relay',
        createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
      );
      final signedEvent = await ndk.accounts.getLoggedAccount()!.signer.sign(
        event,
      );

      final package = await scheduler.schedulePackage(
        [
          SchedulePackageItem(
            event: signedEvent,
            dvmPubkeys: [fallbackDvmKey.publicKey],
            relays: [relay.url],
            dvmReadRelays: [relay.url],
          ),
        ],
        content: 'fallback context',
        pubkey: clientKey.publicKey,
      );

      expect(package.jobs.single.dvmPubkeys, [fallbackDvmKey.publicKey]);

      await Future.delayed(const Duration(milliseconds: 500));

      final requests = await relayQuery(
        Filter(kinds: [5905], authors: [clientKey.publicKey]),
      );
      final request = requests.firstWhere(
        (event) => event.getFirstTag('p') == fallbackDvmKey.publicKey,
      );
      expect(request.id, package.requestEventIds.single);
    });

    test('fans out package items to multiple DVMs', () async {
      final signer = ndk.accounts.getLoggedAccount()!.signer;
      final signedB = await signer.sign(
        Nip01Event(
          pubKey: clientKey.publicKey,
          kind: 1,
          tags: [],
          content: 'redundant package B',
          createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
        ),
      );
      final signedC = await signer.sign(
        Nip01Event(
          pubKey: clientKey.publicKey,
          kind: 1,
          tags: [],
          content: 'redundant package C',
          createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
        ),
      );

      final package = await scheduler.schedulePackage(
        [
          SchedulePackageItem(
            event: signedB,
            dvmPubkeys: [dvmKey.publicKey, dvm2Key.publicKey],
            relays: [relay.url],
          ),
          SchedulePackageItem(
            event: signedC,
            dvmPubkeys: [dvmKey.publicKey],
            relays: [relay.url],
          ),
        ],
        content: 'redundant package context',
        pubkey: clientKey.publicKey,
      );

      expect(package.jobs, hasLength(2));
      expect(package.requestEventIds, hasLength(3));
      final redundantJob = package.jobs.firstWhere(
        (job) => job.targetEvent.id == signedB.id,
      );
      expect(
        redundantJob.dvmPubkeys,
        containsAll([dvmKey.publicKey, dvm2Key.publicKey]),
      );

      await Future.delayed(const Duration(milliseconds: 500));

      final manifests = await relayQuery(
        Filter(kinds: [31234], authors: [clientKey.publicKey]),
      );
      expect(
        manifests.single.getTags('e'),
        containsAll(package.requestEventIds),
      );

      final requests = await relayQuery(
        Filter(kinds: [5905], authors: [clientKey.publicKey]),
      );
      expect(requests, hasLength(3));

      // The package stays one logical schedule
      final schedules = await scheduler.listSchedules(
        pubkey: clientKey.publicKey,
      );
      expect(schedules, hasLength(1));
      expect(schedules.single.type, ScheduledItemType.package);
    });

    test('is accepted by a real scheduler DVM implementation', () async {
      final dvmNdk = Ndk(
        NdkConfig(
          eventVerifier: Bip340EventVerifier(useIsolate: false),
          cache: MemCacheManager(),
          bootstrapRelays: [relay.url],
          defaultQueryTimeout: const Duration(seconds: 2),
          defaultBroadcastTimeout: const Duration(seconds: 2),
        ),
      );
      dvmNdk.accounts.loginPrivateKey(
        pubkey: dvmKey.publicKey,
        privkey: dvmKey.privateKey!,
      );

      final dvmDb = await databaseFactoryMemory.openDatabase(
        'dvm_integration_${DateTime.now().microsecondsSinceEpoch}.db',
      );
      final dvm = SchedulerDvm(
        SchedulerDvmConfig(
          ndk: dvmNdk,
          database: dvmDb,
          bootstrapRelays: [relay.url],
          announceNip89: false,
        ),
      );

      addTearDown(() async {
        await dvm.dispose();
        await dvmDb.close();
        await dvmNdk.destroy();
      });

      await dvm.start();
      await scheduler.startListening(pubkey: clientKey.publicKey);

      final signer = ndk.accounts.getLoggedAccount()!.signer;
      final eventB = await signer.sign(
        Nip01Event(
          pubKey: clientKey.publicKey,
          kind: 1,
          tags: [],
          content: 'real dvm package B',
          createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
        ),
      );
      final eventC = await signer.sign(
        Nip01Event(
          pubKey: clientKey.publicKey,
          kind: 1,
          tags: [],
          content: 'real dvm package C',
          createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
        ),
      );

      final package = await scheduler.schedulePackage(
        [
          SchedulePackageItem(
            event: eventB,
            dvmPubkeys: [dvmKey.publicKey],
            at: DateTime.now().add(const Duration(minutes: 1)),
            relays: [relay.url],
          ),
          SchedulePackageItem(
            event: eventC,
            dvmPubkeys: [dvmKey.publicKey],
            at: DateTime.now().add(const Duration(minutes: 1)),
            relays: [relay.url],
          ),
        ],
        content: 'real dvm package context',
        pubkey: clientKey.publicKey,
      );

      await _waitFor(() async {
        for (final job in package.jobs) {
          final stored = await dvm.config.store.getJob(job.jobId);
          if (stored?.status != DvmJobStatus.scheduled) return false;
        }
        return true;
      });

      await scheduler.resync(pubkey: clientKey.publicKey);
      await _waitFor(() async {
        final jobs = await scheduler.listJobs(pubkey: clientKey.publicKey);
        final packageJobIds = package.jobs.map((job) => job.jobId).toSet();
        return jobs
            .where((job) => packageJobIds.contains(job.jobId))
            .every((job) => job.status == JobStatus.scheduled);
      });

      final schedules = await scheduler.listSchedules(
        pubkey: clientKey.publicKey,
      );
      final packageItem = schedules.firstWhere(
        (item) => item.type == ScheduledItemType.package,
      );
      expect(packageItem.package!.jobs, hasLength(2));
      expect(
        packageItem.package!.jobs.map((job) => job.status),
        everyElement(JobStatus.scheduled),
      );
    });

    test('redundant job is accepted by two real scheduler DVMs', () async {
      Future<SchedulerDvm> startDvm(KeyPair key) async {
        final dvmNdk = Ndk(
          NdkConfig(
            eventVerifier: Bip340EventVerifier(useIsolate: false),
            cache: MemCacheManager(),
            bootstrapRelays: [relay.url],
            defaultQueryTimeout: const Duration(seconds: 2),
            defaultBroadcastTimeout: const Duration(seconds: 2),
          ),
        );
        dvmNdk.accounts.loginPrivateKey(
          pubkey: key.publicKey,
          privkey: key.privateKey!,
        );

        final dvmDb = await databaseFactoryMemory.openDatabase(
          'dvm_redundant_${key.publicKey}_${DateTime.now().microsecondsSinceEpoch}.db',
        );
        final dvm = SchedulerDvm(
          SchedulerDvmConfig(
            ndk: dvmNdk,
            database: dvmDb,
            bootstrapRelays: [relay.url],
            announceNip89: false,
          ),
        );

        addTearDown(() async {
          await dvm.dispose();
          await dvmDb.close();
          await dvmNdk.destroy();
        });

        await dvm.start();
        return dvm;
      }

      final dvmA = await startDvm(dvmKey);
      await scheduler.startListening(pubkey: clientKey.publicKey);

      final signer = ndk.accounts.getLoggedAccount()!.signer;
      final signedEvent = await signer.sign(
        Nip01Event(
          pubKey: clientKey.publicKey,
          kind: 1,
          tags: [],
          content: 'redundant real dvm',
          createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
        ),
      );

      // The second DVM is offline when the job is scheduled
      final job = await scheduler.schedule(
        signedEvent,
        [dvmKey.publicKey, dvm2Key.publicKey],
        at: DateTime.now().add(const Duration(minutes: 1)),
        relays: [relay.url],
        pubkey: clientKey.publicKey,
      );

      await _waitFor(() async {
        final stored = await dvmA.config.store.getJob(job.jobId);
        return stored?.status == DvmJobStatus.scheduled;
      });

      // The second DVM comes online and picks the pending request up on
      // resync, accepting the same job_id independently
      final dvmB = await startDvm(dvm2Key);
      await _waitFor(() async {
        final stored = await dvmB.config.store.getJob(job.jobId);
        return stored?.status == DvmJobStatus.scheduled;
      });

      await scheduler.resync(pubkey: clientKey.publicKey);
      await _waitFor(() async {
        final jobs = await scheduler.listJobs(pubkey: clientKey.publicKey);
        final synced = jobs.where((j) => j.jobId == job.jobId).firstOrNull;
        return synced != null &&
            synced.requests.every(
              (request) => request.status == JobStatus.scheduled,
            );
      });

      final updated = (await scheduler.listJobs(
        pubkey: clientKey.publicKey,
      )).firstWhere((j) => j.jobId == job.jobId);
      expect(updated.requests, hasLength(2));
      expect(updated.status, JobStatus.scheduled);
    });
  });

  group('cancel', () {
    test('broadcasts a kind:5 deletion event', () async {
      final event = Nip01Event(
        pubKey: clientKey.publicKey,
        kind: 1,
        tags: [],
        content: 'hello world',
        createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
      );
      final signedEvent = await ndk.accounts.getLoggedAccount()!.signer.sign(
        event,
      );

      final job = await scheduler.schedule(
        signedEvent,
        [dvmKey.publicKey],
        relays: [relay.url],
        pubkey: clientKey.publicKey,
      );

      await scheduler.cancel(job.jobId, pubkey: clientKey.publicKey);

      // Give the shim time to broadcast
      await Future.delayed(const Duration(milliseconds: 500));

      final deletions = await relayQuery(
        Filter(kinds: [5], authors: [clientKey.publicKey]),
      );
      expect(deletions, isNotEmpty);

      final deletion = deletions.first;
      expect(deletion.getTags('e'), containsAll(job.requestEventIds));
    });

    test('cancelPackage deletes linked jobs and manifest', () async {
      final eventB = Nip01Event(
        pubKey: clientKey.publicKey,
        kind: 1,
        tags: [],
        content: 'cancel package B',
        createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
      );
      final eventC = Nip01Event(
        pubKey: clientKey.publicKey,
        kind: 1,
        tags: [],
        content: 'cancel package C',
        createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
      );
      final signer = ndk.accounts.getLoggedAccount()!.signer;
      final signedB = await signer.sign(eventB);
      final signedC = await signer.sign(eventC);

      final package = await scheduler.schedulePackage(
        [
          SchedulePackageItem(
            event: signedB,
            dvmPubkeys: [dvmKey.publicKey],
            relays: [relay.url],
          ),
          SchedulePackageItem(
            event: signedC,
            dvmPubkeys: [dvmKey.publicKey],
            relays: [relay.url],
          ),
        ],
        content: 'cancel me',
        pubkey: clientKey.publicKey,
      );

      await scheduler.cancelPackage(
        package.packageId,
        pubkey: clientKey.publicKey,
      );

      await Future.delayed(const Duration(milliseconds: 500));

      final deletions = await relayQuery(
        Filter(kinds: [5], authors: [clientKey.publicKey]),
      );
      final manifestDeletion = deletions.singleWhere(
        (event) => event.getTags('e').contains(package.manifestEventId),
      );
      expect(manifestDeletion.getTags('e'), [package.manifestEventId]);
      expect(manifestDeletion.getTags('k'), ['31234']);

      final requestDeletion = deletions.singleWhere(
        (event) => event.getTags('e').contains(package.requestEventIds.first),
      );
      expect(
        requestDeletion.getTags('e'),
        containsAll(package.requestEventIds),
      );
      expect(
        requestDeletion.getTags('e'),
        isNot(contains(package.manifestEventId)),
      );
      expect(requestDeletion.getTags('k'), ['5905']);
      expect(await scheduler.listJobs(pubkey: clientKey.publicKey), isEmpty);
      expect(
        await scheduler.listPackages(pubkey: clientKey.publicKey),
        isEmpty,
      );
      expect(
        await scheduler.listSchedules(pubkey: clientKey.publicKey),
        isEmpty,
      );
    });

    test(
      'cancelPackage deletes request ids when computed jobs are missing',
      () async {
        final eventB = Nip01Event(
          pubKey: clientKey.publicKey,
          kind: 1,
          tags: [],
          content: 'missing computed job B',
          createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
        );
        final eventC = Nip01Event(
          pubKey: clientKey.publicKey,
          kind: 1,
          tags: [],
          content: 'missing computed job C',
          createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
        );
        final signer = ndk.accounts.getLoggedAccount()!.signer;
        final signedB = await signer.sign(eventB);
        final signedC = await signer.sign(eventC);

        final package = await scheduler.schedulePackage(
          [
            SchedulePackageItem(
              event: signedB,
              dvmPubkeys: [dvmKey.publicKey],
              relays: [relay.url],
            ),
            SchedulePackageItem(
              event: signedC,
              dvmPubkeys: [dvmKey.publicKey],
              relays: [relay.url],
            ),
          ],
          content: 'cancel even without computed jobs',
          pubkey: clientKey.publicKey,
        );

        await sembast.stringMapStoreFactory
            .store('nostr_event_scheduler/jobs')
            .delete(schedulerDb);

        expect(await scheduler.listJobs(pubkey: clientKey.publicKey), isEmpty);
        final packages = await scheduler.listPackages(
          pubkey: clientKey.publicKey,
        );
        expect(packages.single.requestEventIds, package.requestEventIds);
        expect(packages.single.jobs, isEmpty);

        await scheduler.cancelPackage(
          package.packageId,
          pubkey: clientKey.publicKey,
        );

        await Future.delayed(const Duration(milliseconds: 500));

        final deletions = await relayQuery(
          Filter(kinds: [5], authors: [clientKey.publicKey]),
        );
        final manifestDeletion = deletions.singleWhere(
          (event) => event.getTags('e').contains(package.manifestEventId),
        );
        expect(manifestDeletion.getTags('e'), [package.manifestEventId]);
        expect(manifestDeletion.getTags('k'), ['31234']);

        final requestDeletion = deletions.singleWhere(
          (event) => event.getTags('e').contains(package.requestEventIds.first),
        );
        expect(
          requestDeletion.getTags('e'),
          containsAll(package.requestEventIds),
        );
        expect(requestDeletion.getTags('k'), ['5905']);
        expect(
          await scheduler.listPackages(pubkey: clientKey.publicKey),
          isEmpty,
        );
        expect(
          await scheduler.listSchedules(pubkey: clientKey.publicKey),
          isEmpty,
        );
      },
    );
  });

  group('multi-device sync', () {
    test('recovers a kind:5905 from the relay', () async {
      final jobId = await publishScheduleRequest(dvm: dvmKey);

      await scheduler.startListening(pubkey: clientKey.publicKey);
      await scheduler.resync(pubkey: clientKey.publicKey);

      await _waitFor(() async {
        final jobs = await scheduler.listJobs(pubkey: clientKey.publicKey);
        return jobs.any((j) => j.jobId == jobId);
      });
    });

    test('resync catches up without listening, and notifies once', () async {
      final jobId = await publishScheduleRequest(dvm: dvmKey);

      final updates = <StatusUpdate>[];
      final subscription = scheduler.statusUpdates.listen(updates.add);
      addTearDown(subscription.cancel);

      await scheduler.resync(pubkey: clientKey.publicKey);

      final jobs = await scheduler.listJobs(pubkey: clientKey.publicKey);
      expect(jobs.any((j) => j.jobId == jobId), isTrue);

      await publishFeedback(dvm: dvmKey, jobId: jobId, status: 'scheduled');
      await scheduler.resync(pubkey: clientKey.publicKey);
      await _waitFor(() => updates.length == 1);

      // Replaying the cache must not report a feedback already seen.
      await scheduler.resync(pubkey: clientKey.publicKey);
      await Future.delayed(const Duration(milliseconds: 300));
      expect(updates.length, 1);
      expect(updates.single.jobId, jobId);
    });
  });

  group('NIP-37 private relays', () {
    late MockRelay privateRelay;

    Future<ScheduledPackage> schedulePackageOfTwo(String content) async {
      final signer = ndk.accounts.getLoggedAccount()!.signer;
      final items = <SchedulePackageItem>[];
      for (final label in ['first', 'second']) {
        final signed = await signer.sign(
          Nip01Event(
            pubKey: clientKey.publicKey,
            kind: 1,
            tags: [],
            content: '$content $label',
            createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
          ),
        );
        items.add(
          SchedulePackageItem(
            event: signed,
            dvmPubkeys: [dvmKey.publicKey],
            relays: [relay.url],
          ),
        );
      }
      return scheduler.schedulePackage(
        items,
        content: content,
        pubkey: clientKey.publicKey,
      );
    }

    /// Publishes the account's kind:10013 pointing at [privateRelay].
    Future<void> publishPrivateRelayList() async {
      final encrypted = await Nip44.encryptMessage(
        jsonEncode([
          ['relay', privateRelay.url],
        ]),
        clientKey.privateKey!,
        clientKey.publicKey,
      );
      final list = Nip01Utils.signWithPrivateKey(
        event: Nip01Event(
          pubKey: clientKey.publicKey,
          kind: 10013,
          tags: [],
          content: encrypted,
          createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
        ),
        privateKey: clientKey.privateKey!,
      );
      await ndk.broadcast
          .broadcast(nostrEvent: list, specificRelays: [relay.url])
          .broadcastDoneFuture;
    }

    setUp(() async {
      privateRelay = MockRelay(name: 'private relay', explicitPort: 9091);
      await privateRelay.startServer();
    });

    tearDown(() => privateRelay.stopServer());

    test('keeps the manifest off the public relays', () async {
      await publishPrivateRelayList();
      final package = await schedulePackageOfTwo('private manifest');

      await _waitFor(() async {
        final stored = await relayQueryOn(
          privateRelay,
          Filter(kinds: [31234], authors: [clientKey.publicKey]),
        );
        return stored.any((event) => event.id == package.manifestEventId);
      });

      expect(
        await relayQuery(
          Filter(kinds: [31234], authors: [clientKey.publicKey]),
        ),
        isEmpty,
      );
      // The requests themselves stay public: the DVMs must read them.
      await waitForRequestsOnRelay(2);
    });

    test('recovers a manifest from the private relay', () async {
      await publishPrivateRelayList();
      final package = await schedulePackageOfTwo('recovered manifest');
      await waitForRequestsOnRelay(2);
      await _waitFor(() async {
        final stored = await relayQueryOn(
          privateRelay,
          Filter(kinds: [31234], authors: [clientKey.publicKey]),
        );
        return stored.any((event) => event.id == package.manifestEventId);
      });

      await scheduler.clearLocalAccountData(pubkey: clientKey.publicKey);
      expect(
        await scheduler.listPackages(pubkey: clientKey.publicKey),
        isEmpty,
      );

      await scheduler.resync(pubkey: clientKey.publicKey);

      await _waitFor(() async {
        final packages = await scheduler.listPackages(
          pubkey: clientKey.publicKey,
        );
        return packages.any((p) => p.packageId == package.packageId);
      });
    });

    test('cancels the manifest privately and the requests publicly', () async {
      await publishPrivateRelayList();
      final package = await schedulePackageOfTwo('split cancel');
      await waitForRequestsOnRelay(2);

      await scheduler.cancelPackage(
        package.packageId,
        pubkey: clientKey.publicKey,
      );

      await _waitFor(() async {
        final deletions = await relayQueryOn(
          privateRelay,
          Filter(kinds: [5], authors: [clientKey.publicKey]),
        );
        return deletions.any(
          (event) => event.getTags('e').contains(package.manifestEventId),
        );
      });

      final publicDeletions = await relayQuery(
        Filter(kinds: [5], authors: [clientKey.publicKey]),
      );
      expect(
        publicDeletions.single.getTags('e'),
        containsAll(package.requestEventIds),
      );
      expect(
        publicDeletions.single.getTags('e'),
        isNot(contains(package.manifestEventId)),
      );
    });

    test('never asks a relay for the kind:10013', () async {
      // An account that publishes no kind:10013 caches nothing either, so a
      // lookup that went to the relays would go there again on every
      // declaration, for the majority of accounts.
      //
      // Nothing is broadcast here on purpose: the write side resolves the list
      // itself, through the shim, and that query is a legitimate one.
      await scheduler.startListening(pubkey: clientKey.publicKey);
      await scheduler.resync(pubkey: clientKey.publicKey);

      expect(relay.reqsPerKind[10013], isNull);
    });

    test('ignores a kind:10013 the cache does not hold', () async {
      // The other side of that contract, and the reason it is the caller's
      // job to fetch the list: on the relay is not good enough.
      await publishPrivateRelayList();
      await ndk.config.cache.removeEvents(
        pubKeys: [clientKey.publicKey],
        kinds: [10013],
      );

      await scheduler.startListening(pubkey: clientKey.publicKey);
      await scheduler.resync(pubkey: clientKey.publicKey);

      expect(privateRelay.reqsPerKind, isEmpty);
    });

    test('picks up a kind:10013 that reaches the cache mid-session', () async {
      await scheduler.startListening(pubkey: clientKey.publicKey);
      await schedulePackageOfTwo('before the list');

      await publishPrivateRelayList();
      await Future.delayed(const Duration(milliseconds: 300));

      expect(
        privateRelay.reqsPerKind,
        isEmpty,
        reason: 'nothing should have been asked of the private relay yet',
      );

      // No restart and no cache invalidation to arrange: the next declaration
      // re-reads the cache, which now holds the list.
      await scheduler.resync(pubkey: clientKey.publicKey);
      final after = await schedulePackageOfTwo('after the list');
      await Future.delayed(const Duration(milliseconds: 800));

      expect(
        privateRelay.reqsPerKind.keys,
        contains(31234),
        reason: 'the manifest branch should now read the private relay',
      );
      final onPrivate = await relayQueryOn(
        privateRelay,
        Filter(kinds: [31234], authors: [clientKey.publicKey]),
      );
      expect(onPrivate.map((e) => e.id), contains(after.manifestEventId));
    });

    test('keeps a manifest cancelled after the kind:10013 appeared', () async {
      // Written without NIP-37, so the manifest is public; its deletion, signed
      // once the kind:10013 exists, only ever reaches the private relays.
      final package = await schedulePackageOfTwo('legacy manifest');
      await waitForRequestsOnRelay(2);
      await _waitFor(() async {
        final stored = await relayQuery(
          Filter(kinds: [31234], authors: [clientKey.publicKey]),
        );
        return stored.any((event) => event.id == package.manifestEventId);
      });

      await publishPrivateRelayList();
      await scheduler.cancelPackage(
        package.packageId,
        pubkey: clientKey.publicKey,
      );
      await _waitFor(() async {
        final deletions = await relayQueryOn(
          privateRelay,
          Filter(kinds: [5], authors: [clientKey.publicKey]),
        );
        return deletions.any(
          (event) => event.getTags('e').contains(package.manifestEventId),
        );
      });

      // The public manifest survives, which is the accepted leak.
      expect(
        (await relayQuery(
          Filter(kinds: [31234], authors: [clientKey.publicKey]),
        )).map((e) => e.id),
        contains(package.manifestEventId),
      );

      // What must not happen: a device that never saw the cancellation reads
      // the manifest back from the NIP-65 branch and resurrects the package.
      // Dropping the kind:5 too is what makes this a fresh device rather than
      // a local reset, where NDK's cache would hide the manifest on its own.
      await scheduler.clearLocalAccountData(pubkey: clientKey.publicKey);
      await ndk.config.cache.removeEvents(
        pubKeys: [clientKey.publicKey],
        kinds: [5],
      );

      await scheduler.resync(pubkey: clientKey.publicKey);
      await Future.delayed(const Duration(milliseconds: 500));

      expect(
        await scheduler.listPackages(pubkey: clientKey.publicKey),
        isEmpty,
      );
    });
  });

  group('DVM feedback', () {
    test('emits a StatusUpdate when kind:7000 is received', () async {
      final event = Nip01Event(
        pubKey: clientKey.publicKey,
        kind: 1,
        tags: [],
        content: 'feedback test',
        createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
      );
      final signedEvent = await ndk.accounts.getLoggedAccount()!.signer.sign(
        event,
      );

      final job = await scheduler.schedule(
        signedEvent,
        [dvmKey.publicKey],
        relays: [relay.url],
        pubkey: clientKey.publicKey,
      );

      await scheduler.startListening(pubkey: clientKey.publicKey);

      // Wait for feedback subscription to be established
      await Future.delayed(const Duration(milliseconds: 500));

      // Capture status update
      final updates = <StatusUpdate>[];
      final sub = scheduler.statusUpdates.listen(updates.add);

      // Publish a feedback kind:7000 from the DVM
      await publishFeedback(
        dvm: dvmKey,
        jobId: job.jobId,
        status: 'scheduled',
        message: 'Job accepted',
      );

      // Wait for processing
      await Future.delayed(const Duration(milliseconds: 500));

      await sub.cancel();

      expect(updates, isNotEmpty);
      expect(updates.first.jobId, job.jobId);
      expect(updates.first.dvmPubkey, dvmKey.publicKey);
      expect(updates.first.status, JobStatus.scheduled);

      final jobs = await scheduler.listJobs(pubkey: clientKey.publicKey);
      final updatedJob = jobs.firstWhere((j) => j.jobId == job.jobId);
      expect(updatedJob.status, JobStatus.scheduled);
    });

    test(
      'an older feedback received later does not roll the status back',
      () async {
        final signedEvent = await ndk.accounts.getLoggedAccount()!.signer.sign(
          Nip01Event(
            pubKey: clientKey.publicKey,
            kind: 1,
            tags: [],
            content: 'out of order',
            createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
          ),
        );
        final job = await scheduler.schedule(
          signedEvent,
          [dvmKey.publicKey],
          relays: [relay.url],
          pubkey: clientKey.publicKey,
        );
        await scheduler.startListening(pubkey: clientKey.publicKey);
        await Future.delayed(const Duration(milliseconds: 500));

        final now = DateTime.now().millisecondsSinceEpoch ~/ 1000;
        await publishFeedback(
          dvm: dvmKey,
          jobId: job.jobId,
          status: 'published',
          createdAt: now,
        );
        await _waitFor(() async {
          final jobs = await scheduler.listJobs(pubkey: clientKey.publicKey);
          return jobs.single.status == JobStatus.published;
        });

        await publishFeedback(
          dvm: dvmKey,
          jobId: job.jobId,
          status: 'scheduled',
          createdAt: now - 60,
        );
        await Future.delayed(const Duration(milliseconds: 500));

        final jobs = await scheduler.listJobs(pubkey: clientKey.publicKey);
        expect(jobs.single.status, JobStatus.published);
      },
    );
  });

  group('decryptPending', () {
    test('decrypts queued events when signer becomes available', () async {
      // This test verifies the pending_decryption queue works
      final event = Nip01Event(
        pubKey: clientKey.publicKey,
        kind: 1,
        tags: [],
        content: 'pending test',
        createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
      );
      final signedEvent = await ndk.accounts.getLoggedAccount()!.signer.sign(
        event,
      );

      await scheduler.schedule(
        signedEvent,
        [dvmKey.publicKey],
        relays: [relay.url],
        pubkey: clientKey.publicKey,
      );

      // decryptPending should be a no-op since signer is already available
      await scheduler.decryptPending(pubkey: clientKey.publicKey);

      // Nothing should fail
      expect(await scheduler.listJobs(pubkey: clientKey.publicKey), isNotEmpty);
    });
  });

  Future<ScheduledJob> scheduleFor(KeyPair key, String content) async {
    final signer = ndk.accounts.accounts[key.publicKey]!.signer;
    final signed = await signer.sign(
      Nip01Event(
        pubKey: key.publicKey,
        kind: 1,
        tags: [],
        content: content,
        createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
      ),
    );
    return scheduler.schedule(
      signed,
      [dvmKey.publicKey],
      relays: [relay.url],
      pubkey: key.publicKey,
    );
  }

  group('multi-account', () {
    setUp(() {
      ndk.accounts.loginPrivateKey(
        pubkey: client2Key.publicKey,
        privkey: client2Key.privateKey!,
      );
    });

    test('keeps the schedules of two accounts apart', () async {
      final first = await scheduleFor(clientKey, 'account one');
      final second = await scheduleFor(client2Key, 'account two');

      final firstJobs = await scheduler.listJobs(pubkey: clientKey.publicKey);
      final secondJobs = await scheduler.listJobs(pubkey: client2Key.publicKey);

      expect(firstJobs.single.jobId, first.jobId);
      expect(firstJobs.single.pubkey, clientKey.publicKey);
      expect(secondJobs.single.jobId, second.jobId);
      expect(secondJobs.single.pubkey, client2Key.publicKey);

      expect(
        (await scheduler.listSchedules(pubkey: clientKey.publicKey)).single.id,
        first.jobId,
      );
    });

    test('refuses to cancel a job owned by another account', () async {
      final job = await scheduleFor(clientKey, 'not yours');

      expect(
        () => scheduler.cancel(job.jobId, pubkey: client2Key.publicKey),
        throwsArgumentError,
      );
    });
  });

  group('clearLocalAccountData', () {
    setUp(() {
      ndk.accounts.loginPrivateKey(
        pubkey: client2Key.publicKey,
        privkey: client2Key.privateKey!,
      );
    });

    test('drops one account and leaves the other untouched', () async {
      await scheduleFor(clientKey, 'to be cleared');
      final kept = await scheduleFor(client2Key, 'to be kept');
      await waitForRequestsOnRelay(2);

      await scheduler.clearLocalAccountData(pubkey: clientKey.publicKey);

      expect(await scheduler.listJobs(pubkey: clientKey.publicKey), isEmpty);
      expect(
        (await scheduler.listJobs(pubkey: client2Key.publicKey)).single.jobId,
        kept.jobId,
      );
    });

    test('is idempotent', () async {
      await scheduleFor(clientKey, 'cleared twice');
      final kept = await scheduleFor(client2Key, 'kept');
      await waitForRequestsOnRelay(2);

      await scheduler.clearLocalAccountData(pubkey: clientKey.publicKey);
      await scheduler.clearLocalAccountData(pubkey: clientKey.publicKey);

      expect(await scheduler.listJobs(pubkey: clientKey.publicKey), isEmpty);
      expect(
        (await scheduler.listJobs(pubkey: client2Key.publicKey)).single.jobId,
        kept.jobId,
      );
    });

    test('purges raw so the account is not rebuilt from cache', () async {
      await scheduleFor(clientKey, 'no resurrection');
      await waitForRequestsOnRelay(1);

      await scheduler.clearLocalAccountData(pubkey: clientKey.publicKey);

      expect(
        await ndk.config.cache.loadEvents(
          pubKeys: [clientKey.publicKey],
          kinds: [5905],
        ),
        isEmpty,
      );
      expect(await scheduler.listJobs(pubkey: clientKey.publicKey), isEmpty);
    });
  });

  group('clearAllLocalData', () {
    test('empties every account', () async {
      ndk.accounts.loginPrivateKey(
        pubkey: client2Key.publicKey,
        privkey: client2Key.privateKey!,
      );
      await scheduleFor(clientKey, 'first');
      await scheduleFor(client2Key, 'second');
      await waitForRequestsOnRelay(2);

      await scheduler.clearAllLocalData();
      await scheduler.clearAllLocalData();

      expect(await scheduler.listJobs(pubkey: clientKey.publicKey), isEmpty);
      expect(await scheduler.listJobs(pubkey: client2Key.publicKey), isEmpty);
    });
  });

  group('computed projections', () {
    test('are rebuilt from raw once dropped', () async {
      final job = await scheduleFor(clientKey, 'rebuild me');

      // Drop the computed tier the way a schema bump does.
      await sembast.StoreRef<String, Map<String, dynamic>>(
        'nostr_event_scheduler/jobs',
      ).drop(schedulerDb);
      await sembast.StoreRef<String, int>(
        'nostr_event_scheduler/schema_version',
      ).record('built/${clientKey.publicKey}').delete(schedulerDb);

      final rebuilt = await scheduler.listJobs(pubkey: clientKey.publicKey);

      expect(rebuilt.single.jobId, job.jobId);
      expect(rebuilt.single.pubkey, clientKey.publicKey);
      expect(rebuilt.single.requests.single.dvmPubkey, dvmKey.publicKey);
      expect(rebuilt.single.targetEvent.content, 'rebuild me');
    });

    test('keep a cancelled job cancelled across a rebuild', () async {
      final job = await scheduleFor(clientKey, 'cancelled');
      await scheduler.cancel(job.jobId, pubkey: clientKey.publicKey);

      await sembast.StoreRef<String, int>(
        'nostr_event_scheduler/schema_version',
      ).record('built/${clientKey.publicKey}').delete(schedulerDb);

      expect(await scheduler.listJobs(pubkey: clientKey.publicKey), isEmpty);
    });
  });

  group('JobStatus.aggregate', () {
    test('most advanced status wins', () {
      expect(JobStatus.aggregate([]), isNull);
      expect(
        JobStatus.aggregate([JobStatus.pending, JobStatus.failed]),
        JobStatus.pending,
      );
      expect(
        JobStatus.aggregate([JobStatus.failed, JobStatus.scheduled]),
        JobStatus.scheduled,
      );
      expect(
        JobStatus.aggregate([JobStatus.scheduled, JobStatus.published]),
        JobStatus.published,
      );
      expect(
        JobStatus.aggregate([JobStatus.error, JobStatus.failed]),
        JobStatus.failed,
      );
      expect(
        JobStatus.aggregate([JobStatus.cancelled, JobStatus.error]),
        JobStatus.error,
      );
    });
  });

  group('ScheduledJobRequest.isSupersededBy', () {
    ScheduledJobRequest applied(JobStatus status, int? at) =>
        ScheduledJobRequest(
          dvmPubkey: 'dvm',
          requestEventId: 'request',
          status: status,
          feedbackAt: at,
          updatedAt: 0,
        );

    test('any feedback replaces no feedback', () {
      expect(
        applied(JobStatus.pending, null).isSupersededBy(1, JobStatus.scheduled),
        isTrue,
      );
    });

    test('only a newer feedback replaces the applied one', () {
      final request = applied(JobStatus.published, 100);
      expect(request.isSupersededBy(99, JobStatus.scheduled), isFalse);
      expect(request.isSupersededBy(101, JobStatus.failed), isTrue);
    });

    test('in the same second, only scheduled gives way', () {
      expect(
        applied(
          JobStatus.scheduled,
          100,
        ).isSupersededBy(100, JobStatus.published),
        isTrue,
      );
      expect(
        applied(
          JobStatus.published,
          100,
        ).isSupersededBy(100, JobStatus.scheduled),
        isFalse,
      );
    });
  });
}
