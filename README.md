# nostr_event_scheduler

Local-first Dart package for scheduling Nostr events via Scheduler DVMs.

This package implements the Scheduler DVM protocol and provides a robust, offline-first API for creating, tracking, and cancelling scheduled Nostr events.

## Features

- **Local-first** - Every operation is persisted locally before any network attempt
- **Offline signer support** - Works even when your signer (e.g. NIP-46) is temporarily unavailable
- **Multi-device sync** - Automatically syncs scheduled jobs across devices
- **Scheduled packages** - Group several DVM jobs into one logical schedule with private display context
- **Real-time DVM feedback** - Receives status updates from Scheduler DVMs (`scheduled`, `published`, `failed`, etc.)
- **No raw event duplication** - Relies on the NDK persistent cache for raw events; only stores decrypted payloads and computed state in Sembast
- **Controlled network access** - Explicit `startListening` / `stopListening` for fine-grained relay connectivity control

## Quick start

```dart
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

  final scheduler = EventScheduler(
    ndk: ndk,
    broadcast: broadcast,
    db: db,
  );

  await scheduler.startListening();

  // Listen to status updates from the DVM
  scheduler.statusUpdates.listen((update) {
    print('Job ${update.jobId}: ${update.status}');
  });

  // Schedule an event
  final scheduleAt = DateTime.now().add(const Duration(hours: 1));

  final event = Nip01Event(
    pubKey: myPubKey,
    kind: 1,
    tags: [],
    content: 'Hello from the future!',
    createdAt: scheduleAt.millisecondsSinceEpoch ~/ 1000,
  );
  final signedEvent = await ndk.accounts.getLoggedAccount()!.signer.sign(event);

  final job = await scheduler.schedule(
    signedEvent,
    dvmPubkey,
    at: scheduleAt,
    relays: ['wss://relay.damus.io'],
  );

  print('Scheduled job: ${job.jobId}');

  // List all jobs
  final jobs = await scheduler.listJobs();
  print('Total jobs: ${jobs.length}');

  // Group multiple DVM jobs as one logical schedule
  final signedEventB = await ndk.accounts.getLoggedAccount()!.signer.sign(
    Nip01Event(
      pubKey: myPubKey,
      kind: 1,
      tags: [],
      content: 'Package item B',
      createdAt: scheduleAt.millisecondsSinceEpoch ~/ 1000,
    ),
  );
  final signedEventC = await ndk.accounts.getLoggedAccount()!.signer.sign(
    Nip01Event(
      pubKey: myPubKey,
      kind: 1,
      tags: [],
      content: 'Package item C',
      createdAt:
          scheduleAt.add(const Duration(minutes: 5)).millisecondsSinceEpoch ~/
          1000,
    ),
  );

  final package = await scheduler.schedulePackage(
    [
      SchedulePackageItem(
        event: signedEventB,
        dvmPubkey: dvmPubkey,
        at: scheduleAt,
        relays: ['wss://relay.damus.io'],
      ),
      SchedulePackageItem(
        event: signedEventC,
        dvmPubkey: anotherDvmPubkey,
        at: scheduleAt.add(const Duration(minutes: 5)),
        relays: ['wss://nos.lol'],
        dvmReadRelays: ['wss://dvm-inbox.example'],
      ),
    ],
    content: 'Private app context for displaying this package later',
  );

  print('Scheduled package: ${package.packageId}');

  // List logical schedules: standalone jobs + packages
  final schedules = await scheduler.listSchedules();
  print('Total schedules: ${schedules.length}');

  // Cancel a job
  await scheduler.cancel(job.jobId);

  // Cancel a package and all linked DVM jobs
  await scheduler.cancelPackage(package.packageId);

  // Dispose when done
  await scheduler.dispose();
  await broadcast.dispose();
  await db.close();
}
```

## API Overview

### EventScheduler

The main entry point.

| Method | Description |
|--------|-------------|
| `startListening()` | Starts network subscriptions for sync and DVM feedbacks |
| `stopListening()` | Stops network subscriptions (scheduler remains usable offline) |
| `resync()` | Forces a manual resync of schedule requests, deletions, and feedbacks |
| `schedule(event, dvmPubkey, {at, relays, dvmReadRelays})` | Creates a new scheduled job |
| `schedulePackage(items, {content})` | Creates a logical schedule backed by multiple DVM jobs |
| `cancel(jobId)` | Cancels a scheduled job by broadcasting a kind:5 deletion |
| `cancelPackage(packageId)` | Cancels all jobs in a package and deletes its manifest |
| `listJobs()` | Lists all scheduled jobs from the local store |
| `listPackages()` | Lists all scheduled packages from the local store |
| `listSchedules()` | Lists logical schedules: standalone jobs plus packages |
| `jobsStream` | Live stream of all scheduled jobs |
| `schedulesStream` | Live stream of logical schedules |
| `statusUpdates` | Stream of DVM feedback status updates |
| `syncState` | Stream of synchronization state (initial / syncing / synced / error) |

### Models

- `ScheduledJob` - Represents a scheduled event with its current status
- `SchedulePackageItem` - Input model for one job inside `schedulePackage`
- `ScheduledPackage` - Represents a package manifest and its linked jobs
- `ScheduledItem` - Logical schedule item, either a standalone job or a package
- `JobStatus` - Enum: `pending`, `scheduled`, `published`, `failed`, `cancelled`, `error`
- `StatusUpdate` - Emitted when a DVM feedback is received
- `SyncState` - Tracks whether the local state is up-to-date with the network

## Architecture

The package follows a strict **raw vs computed** architecture:

- **Raw events** (kind:5905, kind:31234, kind:5, kind:7000) are stored in the **NDK persistent cache**
- **Decrypted payloads**, **computed job state**, and **computed package state** are stored in **Sembast**

This means the computed `jobs` and `packages` stores can be dropped and rebuilt at any time without network access or user action. See [ARCHITECTURE.md](ARCHITECTURE.md) for the full design document.

## Testing

The package includes integration tests using a minimal `MockRelay` implementation and an in-process `nostr_scheduler_dvm` instance.

```bash
dart test
```
