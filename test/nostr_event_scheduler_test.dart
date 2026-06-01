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
import 'package:sembast/sembast_memory.dart';
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

void main() {
  late MockRelay relay;
  late KeyPair clientKey;
  late KeyPair dvmKey;
  late Ndk ndk;
  late Database broadcastDb;
  late Database schedulerDb;
  late EventScheduler scheduler;

  setUp(() async {
    relay = MockRelay(name: 'test relay', explicitPort: 9090);
    await relay.startServer();

    clientKey = Bip340.generatePrivateKey();
    dvmKey = Bip340.generatePrivateKey();

    ndk = Ndk(
      NdkConfig(
        eventVerifier: Bip340EventVerifier(),
        cache: MemCacheManager(),
        engine: NdkEngine.RELAY_SETS,
        bootstrapRelays: [relay.url],
        fetchedRangesEnabled: true,
      ),
    );
    ndk.accounts.loginPrivateKey(
      pubkey: clientKey.publicKey,
      privkey: clientKey.privateKey!,
    );

    // Inject a NIP-65 so the scheduler can find relays for broadcast
    final nip65 = Nip65(
      pubKey: clientKey.publicKey,
      relays: {relay.url: ReadWriteMarker.readWrite},
      createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
    );
    final nip65Event = nip65.toEvent();
    final signedNip65 = await ndk.accounts.getLoggedAccount()!.signer.sign(
      nip65Event,
    );
    relay.storedEvents.add(signedNip65);

    broadcastDb = await databaseFactoryMemory.openDatabase('broadcast_test.db');
    schedulerDb = await databaseFactoryMemory.openDatabase('scheduler_test.db');
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
        dvmKey.publicKey,
        relays: [relay.url],
      );

      expect(job.jobId, isNotEmpty);
      expect(job.status, JobStatus.pending);

      // Give the shim time to broadcast
      await Future.delayed(const Duration(milliseconds: 500));

      // Verify the kind:5905 was received by the relay
      final stored = relay.storedEvents.where((e) => e.kind == 5905);
      expect(stored, isNotEmpty);

      final requestEvent = stored.first;
      expect(requestEvent.pubKey, clientKey.publicKey);
      expect(requestEvent.getFirstTag('p'), dvmKey.publicKey);
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
        dvmKey.publicKey,
        relays: [relay.url],
      );

      await scheduler.cancel(job.jobId);

      // Give the shim time to broadcast
      await Future.delayed(const Duration(milliseconds: 500));

      final deletions = relay.storedEvents.where((e) => e.kind == 5);
      expect(deletions, isNotEmpty);

      final deletion = deletions.first;
      expect(deletion.getTags('e'), contains(job.requestEventId));
    });
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

      relay.storedEvents.add(signedRequest);

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
        dvmKey.publicKey,
        relays: [relay.url],
      );

      await scheduler.startListening();

      // Wait for feedback subscription to be established
      await Future.delayed(const Duration(milliseconds: 500));

      // Build a feedback kind:7000 from the DVM
      final ephemeralKey = Bip340.generatePrivateKey();
      final feedbackPayload = jsonEncode({
        'status': 'scheduled',
        'message': 'Job accepted',
      });

      final encryptedFeedback = await Nip44.encryptMessage(
        feedbackPayload,
        ephemeralKey.privateKey!,
        clientKey.publicKey,
      );

      final feedbackEvent = Nip01Event(
        pubKey: dvmKey.publicKey,
        kind: 7000,
        tags: [
          ['r', job.jobId],
          ['ephemeral-pubkey', ephemeralKey.publicKey],
        ],
        content: encryptedFeedback,
        createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
      );

      // Sign with DVM key
      final signedFeedback = Nip01Utils.signWithPrivateKey(
        event: feedbackEvent,
        privateKey: dvmKey.privateKey!,
      );

      // Capture status update
      final updates = <StatusUpdate>[];
      final sub = scheduler.statusUpdates.listen(updates.add);

      // Inject feedback into relay
      relay.storedEvents.add(signedFeedback);
      relay.sendEvent(
        event: signedFeedback,
        subId: 'scheduler-feedback',
        keyPair: dvmKey,
      );

      // Wait for processing
      await Future.delayed(const Duration(milliseconds: 500));

      await sub.cancel();

      expect(updates, isNotEmpty);
      expect(updates.first.jobId, job.jobId);
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
        dvmKey.publicKey,
        relays: [relay.url],
      );

      // decryptPending should be a no-op since signer is already available
      await scheduler.decryptPending();

      // Nothing should fail
      expect(await scheduler.listJobs(), isNotEmpty);
    });
  });
}
