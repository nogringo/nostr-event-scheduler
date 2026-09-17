# nostr_event_scheduler

Local-first Dart package for scheduling Nostr events via Scheduler DVMs.

This package implements the Scheduler DVM protocol and provides a robust, offline-first API for creating, tracking, and cancelling scheduled Nostr events.

## Features

- **Local-first** - Every operation is persisted locally before any network attempt
- **Multi-account** - Every call takes an explicit account pubkey, so one instance serves as many accounts as you load in `ndk.accounts`
- **Offline signer support** - Works even when your signer (e.g. NIP-46) is temporarily unavailable
- **Multi-device sync** - Automatically syncs scheduled jobs across devices
- **Redundant scheduling** - Send the same job to several Scheduler DVMs (one kind:5905 per DVM, same job_id) so publishing does not depend on a single DVM's uptime or policy
- **Scheduled packages** - Group several DVM jobs into one logical schedule with private display context, written to and read from your NIP-37 private relays when you publish a kind:10013
- **Real-time DVM feedback** - Receives status updates from Scheduler DVMs (`scheduled`, `published`, `failed`, etc.)
- **No raw event duplication** - Relies on the NDK persistent cache for raw events; only stores decrypted payloads, tombstones and projections in Sembast
- **No migrations** - A schema change drops the projections and recomputes them from raw, offline
- **Controlled network access** - Explicit per-account `startListening` / `stopListening` for fine-grained relay connectivity control

## Quick start

```dart
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

  // Caller-owned and shareable: the scheduler declares its own sync requests
  // and only ever forgets those.
  final syncEngine = SyncEngine(ndk, db: db);
  syncEngine.start();

  final scheduler = EventScheduler(
    ndk: ndk,
    broadcast: broadcast,
    syncEngine: syncEngine,
    db: db,
  );

  await scheduler.startListening(pubkey: myPubKey);

  // Listen to status updates from the DVM
  scheduler.statusUpdates.listen((update) {
    print('Job ${update.jobId} of ${update.pubkey}: ${update.status}');
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
    [dvmPubkey],
    pubkey: myPubKey,
    at: scheduleAt,
    relays: ['wss://relay.damus.io'],
  );

  print('Scheduled job: ${job.jobId}');

  // List several DVMs to schedule redundantly: one kind:5905 per DVM, all
  // sharing the same job_id and payload. One publication is enough, relays
  // deduplicate the signed event by ID.
  final signedEventR = await ndk.accounts.getLoggedAccount()!.signer.sign(
    Nip01Event(
      pubKey: myPubKey,
      kind: 1,
      tags: [],
      content: 'Published even if one DVM is down',
      createdAt: scheduleAt.millisecondsSinceEpoch ~/ 1000,
    ),
  );

  final redundantJob = await scheduler.schedule(
    signedEventR,
    [dvmPubkey, anotherDvmPubkey],
    pubkey: myPubKey,
    at: scheduleAt,
    relays: ['wss://relay.damus.io'],
  );

  // One logical job, one request per DVM, aggregated status
  print('Job ${redundantJob.jobId} via ${redundantJob.dvmPubkeys.length} DVMs');
  print('Status: ${redundantJob.status}');

  // List all jobs
  final jobs = await scheduler.listJobs(pubkey: myPubKey);
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
        // A package item can also fan out to several DVMs
        dvmPubkeys: [dvmPubkey, anotherDvmPubkey],
        at: scheduleAt,
        relays: ['wss://relay.damus.io'],
      ),
      SchedulePackageItem(
        event: signedEventC,
        dvmPubkeys: [anotherDvmPubkey],
        at: scheduleAt.add(const Duration(minutes: 5)),
        relays: ['wss://nos.lol'],
        dvmReadRelays: ['wss://dvm-inbox.example'],
      ),
    ],
    content: 'Private app context for displaying this package later',
    pubkey: myPubKey,
  );

  print('Scheduled package: ${package.packageId}');

  // List logical schedules: standalone jobs + packages
  final schedules = await scheduler.listSchedules(pubkey: myPubKey);
  print('Total schedules: ${schedules.length}');

  // Cancel a job
  await scheduler.cancel(job.jobId, pubkey: myPubKey);

  // Cancel a package and all linked DVM jobs
  await scheduler.cancelPackage(package.packageId, pubkey: myPubKey);

  // Wipe everything this account stored locally
  await scheduler.clearLocalAccountData(pubkey: myPubKey);

  // Dispose when done
  await scheduler.dispose();
  await broadcast.dispose();
  await db.close();
}
```

## API Overview

### EventScheduler

The main entry point.

Every method takes the account it acts for. The signer is resolved from `ndk.accounts`, so the logged account is never used implicitly.

| Method | Description |
|--------|-------------|
| `startListening({pubkey})` | Starts network subscriptions for one account's sync and DVM feedbacks |
| `stopListening({pubkey})` | Stops one account's subscriptions, or all of them when omitted |
| `resync({pubkey})` | Pull to refresh: fetches now, however fresh the sync engine's coverage is |
| `decryptPending({pubkey})` | Decrypts what was queued while the account's signer was unavailable |
| `schedule(event, dvmPubkeys, {pubkey, at, relays, dvmReadRelays})` | Creates one scheduled job through one or more DVMs |
| `schedulePackage(items, {content, pubkey})` | Creates a logical schedule backed by multiple DVM jobs |
| `cancel(jobId, {pubkey})` | Cancels a job (all its DVM requests) with one kind:5 deletion |
| `cancelPackage(packageId, {pubkey})` | Cancels all jobs in a package and deletes its manifest |
| `listJobs({pubkey})` | Lists the account's scheduled jobs from the local store |
| `listPackages({pubkey})` | Lists the account's scheduled packages from the local store |
| `listSchedules({pubkey})` | Lists logical schedules: standalone jobs plus packages |
| `jobsStream({pubkey})` | Live stream of the account's scheduled jobs |
| `schedulesStream({pubkey})` | Live stream of the account's logical schedules |
| `clearLocalAccountData({pubkey})` | Removes every local trace of one account |
| `clearAllLocalData()` | Removes every local trace of every account |
| `statusUpdates` | Stream of DVM feedback status updates, tagged with the owning account |
| `syncState` | Stream of per-account sync state (initial / syncing / synced / error) |

`clearLocalAccountData` is a local reset, not a protocol-level forget: the requests still live on the relays, so an account whose signer is still loaded rebuilds them on its next `resync()`. Use `cancel` to actually retract a schedule.

### Models

- `ScheduledJob` - One logical scheduled event, with one request per DVM and an aggregated status
- `ScheduledJobRequest` - One kind:5905 request to a single DVM, with the status that DVM reported
- `SchedulePackageItem` - Input model for one job inside `schedulePackage`
- `ScheduledPackage` - Represents a package manifest and its linked jobs
- `ScheduledItem` - Logical schedule item, either a standalone job or a package
- `JobStatus` - Enum: `pending`, `scheduled`, `published`, `failed`, `cancelled`, `error`; `JobStatus.aggregate` combines the statuses of one job's requests
- `StatusUpdate` - Emitted when a DVM feedback is received, with the reporting DVM's pubkey and the owning account
- `SyncState` - Tracks whether one account's local state is up-to-date with the network

## Architecture

The package follows a strict **raw vs computed** architecture:

- **Raw** holds definitive facts. The signed events (kind:5905, kind:31234, kind:5, kind:7000) live in the **NDK persistent cache**; their decrypted payloads and deletion tombstones live in **Sembast**, keyed by event id. Raw is never dropped and never migrated.
- **Computed** holds the projections (jobs, packages, and the pending decryption queue), in **Sembast**, each record tagged with its owning account.

A schema change therefore needs no migration script: the projections are dropped and recomputed from raw, per account and lazily, without network access or user action. This does mean the host must give `Ndk` a **persistent** `CacheManager`. See [ARCHITECTURE.md](ARCHITECTURE.md) for the full design document.

Raw is filled from two sides. `sync_engine_shim_for_ndk` walks the history and keeps the NDK cache in step with the relays, tracking its own coverage per relay and per account, while the NDK subscriptions carry what is happening right now. `clearLocalAccountData` and `clearAllLocalData` forget the scheduler's coverage along with the events, since a cache emptied under a coverage that survived is never fetched again.

## Testing

The package includes integration tests using a minimal `MockRelay` implementation and an in-process `nostr_scheduler_dvm` instance.

```bash
dart test
```
