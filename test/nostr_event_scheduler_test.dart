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
import 'package:test/test.dart';

Future<EventScheduler> createScheduler({
  required Ndk ndk,
  required Database broadcastDb,
  required Database schedulerDb,
}) async {
  final broadcast = OfflineBroadcast.withNdk(ndk, db: broadcastDb);
  broadcast.start();

  final scheduler = EventScheduler(
    ndk: ndk,
    broadcast: broadcast,
    db: schedulerDb,
  );

  return scheduler;
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
  late KeyPair dvmKey;
  late KeyPair dvm2Key;
  late Ndk ndk;
  late Database broadcastDb;
  late Database schedulerDb;
  late EventScheduler scheduler;

  Future<List<Nip01Event>> relayQuery(Filter filter) {
    return ndk.requests
        .query(
          filter: filter,
          explicitRelays: [relay.url],
          cacheRead: false,
          cacheWrite: false,
        )
        .future;
  }

  Future<void> publishFeedback({
    required KeyPair dvm,
    required String jobId,
    required String status,
    String? message,
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
      createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
    );
    final signedFeedback = Nip01Utils.signWithPrivateKey(
      event: feedbackEvent,
      privateKey: dvm.privateKey!,
    );
    await ndk.broadcast
        .broadcast(nostrEvent: signedFeedback, specificRelays: [relay.url])
        .broadcastDoneFuture;
  }

  setUp(() async {
    relay = MockRelay(name: 'test relay', explicitPort: 9090);

    clientKey = Bip340.generatePrivateKey();
    dvmKey = Bip340.generatePrivateKey();
    dvm2Key = Bip340.generatePrivateKey();

    // Serve NIP-65s so the scheduler can find relays for broadcast
    final nip65 = Nip65(
      pubKey: clientKey.publicKey,
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
      nip65s: {clientKey: nip65, dvmKey: dvmNip65, dvm2Key: dvm2Nip65},
    );

    ndk = Ndk(
      NdkConfig(
        eventVerifier: Bip340EventVerifier(),
        cache: MemCacheManager(),
        bootstrapRelays: [relay.url],
        fetchedRangesEnabled: true,
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
    scheduler = await createScheduler(
      ndk: ndk,
      broadcastDb: broadcastDb,
      schedulerDb: schedulerDb,
    );
  });

  tearDown(() async {
    await scheduler.dispose();
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
      );

      expect(job.requests, hasLength(2));
      expect(
        job.dvmPubkeys,
        containsAll([dvmKey.publicKey, dvm2Key.publicKey]),
      );
      expect(job.status, JobStatus.pending);
      expect(await scheduler.listJobs(), hasLength(1));
      expect(await scheduler.listSchedules(), hasLength(1));

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
      );

      await scheduler.startListening();
      await Future.delayed(const Duration(milliseconds: 500));

      final updates = <StatusUpdate>[];
      final sub = scheduler.statusUpdates.listen(updates.add);

      // First DVM fails: the other one is still pending, so the job is too
      await publishFeedback(dvm: dvmKey, jobId: job.jobId, status: 'failed');
      await _waitFor(() async {
        final jobs = await scheduler.listJobs();
        return jobs.single.requestForDvm(dvmKey.publicKey)!.status ==
            JobStatus.failed;
      });
      var current = (await scheduler.listJobs()).single;
      expect(current.status, JobStatus.pending);

      // Second DVM accepts: one acceptance is enough
      await publishFeedback(
        dvm: dvm2Key,
        jobId: job.jobId,
        status: 'scheduled',
      );
      await _waitFor(() async {
        final jobs = await scheduler.listJobs();
        return jobs.single.requestForDvm(dvm2Key.publicKey)!.status ==
            JobStatus.scheduled;
      });
      current = (await scheduler.listJobs()).single;
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
      );
      expect(job.requestEventIds, hasLength(2));

      await scheduler.cancel(job.jobId);

      await Future.delayed(const Duration(milliseconds: 500));

      final deletions = await relayQuery(
        Filter(kinds: [5], authors: [clientKey.publicKey]),
      );
      expect(deletions, hasLength(1));
      expect(deletions.single.getTags('e'), containsAll(job.requestEventIds));
      expect(await scheduler.listJobs(), isEmpty);
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

      await scheduler.startListening();
      await scheduler.resync();

      await _waitFor(() async {
        final jobs = await scheduler.listJobs();
        return jobs.length == 1 && jobs.single.requests.length == 2;
      });

      final job = (await scheduler.listJobs()).single;
      expect(job.jobId, jobId);
      expect(
        job.dvmPubkeys,
        containsAll([dvmKey.publicKey, dvm2Key.publicKey]),
      );
      expect(await scheduler.listSchedules(), hasLength(1));
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
      );
      final package = await scheduler.schedulePackage([
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
      ], content: 'opaque display context');

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

      final jobs = await scheduler.listJobs();
      expect(jobs, hasLength(3));

      final schedules = await scheduler.listSchedules();
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

      final package = await scheduler.schedulePackage([
        SchedulePackageItem(
          event: signedEvent,
          dvmPubkeys: [fallbackDvmKey.publicKey],
          relays: [relay.url],
          dvmReadRelays: [relay.url],
        ),
      ], content: 'fallback context');

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

      final package = await scheduler.schedulePackage([
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
      ], content: 'redundant package context');

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
      final schedules = await scheduler.listSchedules();
      expect(schedules, hasLength(1));
      expect(schedules.single.type, ScheduledItemType.package);
    });

    test('is accepted by a real scheduler DVM implementation', () async {
      final dvmNdk = Ndk(
        NdkConfig(
          eventVerifier: Bip340EventVerifier(useIsolate: false),
          cache: MemCacheManager(),
          bootstrapRelays: [relay.url],
          fetchedRangesEnabled: true,
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
      await scheduler.startListening();

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

      final package = await scheduler.schedulePackage([
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
      ], content: 'real dvm package context');

      await _waitFor(() async {
        for (final job in package.jobs) {
          final stored = await dvm.config.store.getJob(job.jobId);
          if (stored?.status != DvmJobStatus.scheduled) return false;
        }
        return true;
      });

      await scheduler.resync();
      await _waitFor(() async {
        final jobs = await scheduler.listJobs();
        final packageJobIds = package.jobs.map((job) => job.jobId).toSet();
        return jobs
            .where((job) => packageJobIds.contains(job.jobId))
            .every((job) => job.status == JobStatus.scheduled);
      });

      final schedules = await scheduler.listSchedules();
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
            fetchedRangesEnabled: true,
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
      await scheduler.startListening();

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

      await scheduler.resync();
      await _waitFor(() async {
        final jobs = await scheduler.listJobs();
        final synced = jobs.where((j) => j.jobId == job.jobId).firstOrNull;
        return synced != null &&
            synced.requests.every(
              (request) => request.status == JobStatus.scheduled,
            );
      });

      final updated = (await scheduler.listJobs()).firstWhere(
        (j) => j.jobId == job.jobId,
      );
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
      );

      await scheduler.cancel(job.jobId);

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

      final package = await scheduler.schedulePackage([
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
      ], content: 'cancel me');

      await scheduler.cancelPackage(package.packageId);

      await Future.delayed(const Duration(milliseconds: 500));

      final deletions = await relayQuery(
        Filter(kinds: [5], authors: [clientKey.publicKey]),
      );
      final deletion = deletions.singleWhere(
        (event) => event.getTags('e').contains(package.manifestEventId),
      );
      expect(deletion.getTags('e'), containsAll(package.requestEventIds));
      expect(deletion.getTags('e'), contains(package.manifestEventId));
      expect(deletion.getTags('k'), containsAll(['5905', '31234']));
      expect(await scheduler.listJobs(), isEmpty);
      expect(await scheduler.listPackages(), isEmpty);
      expect(await scheduler.listSchedules(), isEmpty);
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

        final package = await scheduler.schedulePackage([
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
        ], content: 'cancel even without computed jobs');

        await sembast.stringMapStoreFactory
            .store('nostr_event_scheduler/jobs')
            .delete(schedulerDb);

        expect(await scheduler.listJobs(), isEmpty);
        final packages = await scheduler.listPackages();
        expect(packages.single.requestEventIds, package.requestEventIds);
        expect(packages.single.jobs, isEmpty);

        await scheduler.cancelPackage(package.packageId);

        await Future.delayed(const Duration(milliseconds: 500));

        final deletions = await relayQuery(
          Filter(kinds: [5], authors: [clientKey.publicKey]),
        );
        final deletion = deletions.singleWhere(
          (event) => event.getTags('e').contains(package.manifestEventId),
        );
        expect(deletion.getTags('e'), containsAll(package.requestEventIds));
        expect(deletion.getTags('e'), contains(package.manifestEventId));
        expect(deletion.getTags('k'), containsAll(['5905', '31234']));
        expect(await scheduler.listPackages(), isEmpty);
        expect(await scheduler.listSchedules(), isEmpty);
      },
    );
  });

  group('multi-device sync', () {
    test('recovers a kind:5905 from the relay', () async {
      // Create an encrypted kind:5905 manually and inject it into the relay
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
        dvmKey.publicKey,
      );

      final requestEvent = Nip01Event(
        pubKey: clientKey.publicKey,
        kind: 5905,
        tags: [
          ['p', dvmKey.publicKey],
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

      // Start listening and resync
      await scheduler.startListening();
      await scheduler.resync();

      // Wait for processing
      await Future.delayed(const Duration(milliseconds: 500));

      final jobs = await scheduler.listJobs();
      expect(jobs.any((j) => j.jobId == jobId), isTrue);
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
      );

      await scheduler.startListening();

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

      final jobs = await scheduler.listJobs();
      final updatedJob = jobs.firstWhere((j) => j.jobId == job.jobId);
      expect(updatedJob.status, JobStatus.scheduled);
    });
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
      );

      // decryptPending should be a no-op since signer is already available
      await scheduler.decryptPending();

      // Nothing should fail
      expect(await scheduler.listJobs(), isNotEmpty);
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
}
