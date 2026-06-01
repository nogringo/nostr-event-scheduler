import 'package:broadcast_queue_shim_for_ndk/broadcast_queue_shim_for_ndk.dart';
import 'package:ndk/ndk.dart';
import 'package:nostr_event_scheduler/nostr_event_scheduler.dart';
import 'package:sembast/sembast_io.dart';

Future<void> main() async {
  final db = await databaseFactoryIo.openDatabase('scheduler.db');

  final ndk = Ndk(
    NdkConfig(
      eventVerifier: Bip340EventVerifier(),
      cache: SembastCacheManager(db),
      fetchedRangesEnabled: true,
    ),
  );

  final broadcast = OfflineBroadcast.withNdk(ndk, db: db);
  broadcast.start();

  final scheduler = EventScheduler(ndk: ndk, broadcast: broadcast, db: db);

  await scheduler.startListening();

  // Listen to status updates
  scheduler.statusUpdates.listen((update) {
    print('Job ${update.jobId}: ${update.status}');
  });

  // List existing jobs
  final jobs = await scheduler.listJobs();
  print('Existing jobs: ${jobs.length}');

  // Dispose when done
  await scheduler.dispose();
  await broadcast.dispose();
  await db.close();
}
