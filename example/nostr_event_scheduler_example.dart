import 'package:broadcast_queue_shim_for_ndk/broadcast_queue_shim_for_ndk.dart';
import 'package:ndk/ndk.dart';
import 'package:nostr_event_scheduler/nostr_event_scheduler.dart';
import 'package:sembast/sembast_io.dart';
import 'package:sync_engine_shim_for_ndk/sync_engine_shim_for_ndk.dart';

Future<void> main() async {
  final db = await databaseFactoryIo.openDatabase('scheduler.db');

  // The cache must be persistent: it holds the raw events the scheduler
  // recomputes its projections from.
  final ndk = Ndk(
    NdkConfig(
      eventVerifier: Bip340EventVerifier(),
      cache: SembastCacheManager(db),
    ),
  );

  final broadcast = OfflineBroadcast.withNdk(ndk, db: db);
  broadcast.start();

  final syncEngine = SyncEngine(ndk, db: db);
  syncEngine.start();

  final scheduler = EventScheduler(
    ndk: ndk,
    broadcast: broadcast,
    syncEngine: syncEngine,
    db: db,
  );

  final pubkey = ndk.accounts.getPublicKey()!;
  await scheduler.startListening(pubkey: pubkey);

  // Listen to status updates
  scheduler.statusUpdates.listen((update) {
    print('Job ${update.jobId} of ${update.pubkey}: ${update.status}');
  });

  // List existing jobs
  final jobs = await scheduler.listJobs(pubkey: pubkey);
  print('Existing jobs: ${jobs.length}');

  // Dispose when done
  await scheduler.dispose();
  await syncEngine.dispose();
  await broadcast.dispose();
  await db.close();
}
