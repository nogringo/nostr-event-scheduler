# Nostr Event Scheduler - Architecture

## Overview

`nostr_event_scheduler` is a local-first Dart package that lets clients schedule Nostr events to be published at a future time through a Scheduler DVM (Data Vending Machine), as defined by the Scheduler DVM protocol.

A job can be scheduled redundantly through several DVMs: one `kind:5905` request per DVM, all sharing the same `job_id` and the exact same payload. Every `kind:5905` with the same `job_id` schedules the same event at the same time. Since the target event is signed before fan-out, publication stays deterministic: whichever DVM publishes first wins and relays deduplicate the event by ID.

The package is built around a strict separation between **raw data** (handled by the NDK persistent cache) and **computed/denormalized data** (stored in Sembast). This mirrors the Nostr philosophy: because everything is predictable, computed stores can be dropped and rebuilt from raw data without network access or user action.

---

## Core Principles

1. **Local-first**: Every operation is persisted locally before any network attempt. The app works fully offline.
2. **Raw vs Computed**: Raw encrypted events live in the NDK cache. Decrypted payloads, tombstones, and computed job states live in Sembast. Computed stores are droppable and rebuildable.
3. **Offline signer support**: If the signer is unavailable (e.g. NIP-46 remote signer disconnected), encrypted events are queued for later decryption. The app continues to work with already-decrypted data.
4. **Controlled network access**: Listening to relays is explicit (`startListening` / `stopListening`). The scheduler instance can be used entirely offline or on-demand via manual `resync()`.

---

## Dependencies

- [`ndk`](https://pub.dev/packages/ndk) `^0.8.4-dev.2`: Nostr Dev Kit for event signing, encryption (NIP-44), relay communication, and persistent caching.
- [`sembast`](https://pub.dev/packages/sembast): Local NoSQL database for package-specific state.
- [`broadcast_queue_shim_for_ndk`](https://pub.dev/packages/broadcast_queue_shim_for_ndk): Offline-first broadcast queue. The caller provides a configured `OfflineBroadcast` instance.

---

## Data Stores

### NDK Cache (external, persistent)

The NDK maintains its own persistent cache of Nostr events. The scheduler does **not** duplicate raw events in Sembast. Instead, it queries the NDK cache for:

- `kind:5905` (schedule requests)
- `kind:5` (deletions / cancellations)
- `kind:7000` (DVM feedbacks)

This cache survives app restarts and can be queried offline with `cacheRead: true`.

### Sembast Stores

The scheduler owns four Sembast stores:

| Store | Key | Value | Type |
|-------|-----|-------|------|
| `decrypted_payloads` | Event ID (`String`) | Decrypted JSON payload (`String`) | **Raw** (decryption cache) |
| `pending_decryption` | Event ID (`String`) | `true` (`bool`) | **Raw** (queue of events waiting for signer) |
| `tombstones` | Request Event ID (`String`) | Deletion metadata (`Map`) | **Raw** (known deletions) |
| `jobs` | Job ID (`String`) | `ScheduledJob` (`Map`) | **Computed** (rebuildable) |

#### `decrypted_payloads`

Caches the decrypted content of `kind:5905` and `kind:7000` events. This avoids asking the signer to decrypt the same payload multiple times, which is especially important for remote signers (NIP-46) that may require user interaction.

#### `pending_decryption`

Tracks event IDs that have been received but could not yet be decrypted (signer offline). When `decryptPending()` is called, it processes this queue. If the queue is empty, the call is a no-op.

#### `tombstones`

Records known deletions (`kind:5` tagging a `kind:5905` request). This allows the scheduler to instantly skip processing a cancelled event without needing to query the network.

#### `jobs`

The main computed view. It contains the current state of every scheduled job, including its status (`pending`, `scheduled`, `published`, `failed`, `cancelled`, `error`).

This store is **droppable**. It can be rebuilt entirely from:
- The NDK cache (`kind:5905`, `kind:7000`)
- `decrypted_payloads`
- `tombstones`

Because Nostr data is immutable and predictable, no migration scripts are needed. If the schema changes, drop `jobs` and call the private `rebuildComputed()` method.

---

## Key Models

### `ScheduledJob`

Represents a single logical scheduled event. The `job_id` is the identity of
the job: every `kind:5905` request sharing it schedules the same event at the
same time, each request targeting one DVM.

```dart
class ScheduledJob {
  final String jobId;              // 64 hex chars, shared by all requests
  final int scheduleAt;            // Unix timestamp when a DVM should publish
  final Nip01Event targetEvent;    // The original signed event to publish
  final List<String> targetRelays; // Relays where a DVM should publish
  final List<ScheduledJobRequest> requests; // One per Scheduler DVM
  JobStatus get status;            // Aggregated across requests
  String? get lastMessage;         // Message of the status-defining request
  final int createdAt;
  int updatedAt;
}
```

### `ScheduledJobRequest`

One `kind:5905` request sent to a single Scheduler DVM.

```dart
class ScheduledJobRequest {
  final String dvmPubkey;      // Target Scheduler DVM
  final String requestEventId; // ID of the kind:5905 request event
  JobStatus status;            // Status reported by this DVM
  String? lastMessage;         // Optional message from this DVM
  int updatedAt;
}
```

### `JobStatus`

```dart
enum JobStatus { pending, scheduled, published, failed, cancelled, error }
```

- `pending`: The `kind:5905` has been broadcast but no DVM feedback has been received yet.
- `scheduled`: DVM has accepted the job.
- `published`: The event has been broadcast by the DVM.
- `failed`: DVM could not publish to any relay.
- `cancelled`: The job was cancelled via `kind:5`.
- `error`: The job request was invalid.

`JobStatus.aggregate` combines the statuses of one job's requests. One
publication is enough, so the most advanced status wins:
`published` > `scheduled` > `pending` > `failed` > `error` > `cancelled`.
A job with one DVM failed and one DVM scheduled is `scheduled`.

### `StatusUpdate`

Emitted on the `statusUpdates` stream whenever a DVM feedback is received and processed. `status` is the reporting DVM's request status; the job-level status is the aggregate on `ScheduledJob`.

```dart
class StatusUpdate {
  final String jobId;
  final String dvmPubkey; // The reporting DVM
  final JobStatus status;
  final String? message;
  final DateTime receivedAt;
}
```

### `SyncState`

Tracks whether the local state is up-to-date with the network.

```dart
enum SyncStatus { initial, syncing, synced, error }

class SyncState {
  final SyncStatus status;
  final DateTime? lastSyncAt;
  final String? error;
}
```

---

## Public API

### `EventScheduler`

The main entry point.

```dart
EventScheduler({
  required Ndk ndk,
  required OfflineBroadcast broadcast,
  required Database db,
});
```

The caller is responsible for:
- Providing a configured `Ndk` instance with a logged-in account (signer).
- Providing a started `OfflineBroadcast` instance (the shim handles its own persistence and retry logic).
- Providing an open Sembast `Database`.

#### Network Control

```dart
Future<void> startListening();
Future<void> stopListening();
Future<void> resync();
```

- `startListening()`: Starts real-time NDK subscriptions for multi-device sync (`kind:5905`, `kind:5`) and DVM feedbacks (`kind:7000`). Triggers an initial `resync()`.
- `stopListening()`: Closes all network subscriptions. The scheduler remains fully usable offline.
- `resync()`: Forces a manual network fetch. Uses `ndk.fetchedRanges` to avoid re-downloading already-known data. Fetches `kind:5905`, `kind:5`, and `kind:7000`.

#### Local Operations

```dart
Future<void> decryptPending();
```

Processes the `pending_decryption` queue. For each event ID, fetches the raw event from the NDK cache, attempts decryption with the signer, and if successful stores the payload in `decrypted_payloads` and updates the corresponding job.

#### CRUD

```dart
Future<ScheduledJob> schedule(
  Nip01Event event,
  List<String> dvmPubkeys, {
  DateTime? at,
  List<String>? relays,
  List<String>? dvmReadRelays,
});
```

One method covers both cases: a single-DVM schedule is `schedule(event, [dvm])`.

1. Generates a 64-char hex `jobId`, shared by every request of the job.
2. `scheduleAt` falls back to `event.createdAt` if `at` is not provided.
3. `relays` (payload DVM) falls back to the user's NIP-65 write relays.
4. Builds the JSON payload **once** (`job_id`, `schedule_at`, `signed_event`, `relays`): every DVM receives the exact same payload.
5. For each DVM (duplicates removed): encrypts the payload with NIP-44 for that DVM and creates and signs a `kind:5905` event.
6. Persists the decrypted payload in `decrypted_payloads` (one entry per request event) and the computed job in `jobs`.
7. Broadcasts every request via `OfflineBroadcast` to the user's NIP-65 relays (read + write) plus the target DVM's read relays. Throws if no relays are found.
8. Updates the live `kind:7000` subscription to include the new job ID (a single `r` value regardless of the number of DVMs).

```dart
Future<void> cancel(String jobId);
```

1. Looks up the job's `requestEventIds`.
2. Creates and signs **one** `kind:5` deletion event tagging every `kind:5905` request of the job.
3. Broadcasts it via `OfflineBroadcast` to the user's relays plus every DVM's read relays.
4. Records one tombstone per request and removes the job.

```dart
Future<List<ScheduledJob>> listJobs();
Stream<List<ScheduledJob>> get jobsStream;
```

Reads from the computed `jobs` store. `jobsStream` emits live updates.

#### Streams

```dart
Stream<StatusUpdate> get statusUpdates;
Stream<SyncState> get syncState;
```

- `statusUpdates`: Emits whenever a new `kind:7000` feedback is received and processed.
- `syncState`: Emits `syncing` when a sync starts and `synced` after EOSE is received.

---

## Internal Architecture

### SchedulerStore

Private Sembast wrapper around the four stores. Handles JSON serialization and deserialization.

### Multi-device Sync

When `startListening()` or `resync()` is called, the scheduler:

1. Queries the network for `kind:5905` authored by the user, using `ndk.fetchedRanges` to only request missing time ranges.
2. Queries the network for `kind:5` authored by the user (same optimization).
3. Queries the network for `kind:7000` filtered by `#r` tags (all known job IDs).

For each received event:
- `kind:5905`: Try to decrypt immediately. If the signer is available, store in `decrypted_payloads` and update/create the job in `jobs`. Requests sharing a `job_id` merge into one job, one `ScheduledJobRequest` per `kind:5905`. If the signer is unavailable, queue in `pending_decryption`.
- `kind:5`: Store in `tombstones`. Remove the tombstoned request from its job; remove the job once its last request is gone.
- `kind:7000`: Try to decrypt with the ephemeral public key. The feedback is attributed to one of the job's requests by the event's signature pubkey (the DVM signs feedbacks with its main key). A feedback signed by a pubkey that is not one of the job's DVMs is ignored. Update that request's status and emit a `StatusUpdate`.

When a request is learned after its feedback (out-of-order sync), feedbacks already sitting in the NDK cache are re-applied to the job, oldest first, so every request converges to its latest known status.

### Feedback Subscription Strategy (Solution A)

DVM feedbacks (`kind:7000`) use a `#r` tag containing the job ID. The scheduler maintains a single active NDK subscription for `kind:7000` filtered by all currently known job IDs. Redundancy does not grow this filter: all requests of a job share one `job_id`, so one `r` value covers every DVM.

Whenever a new job is created:
1. The old subscription is closed via `ndk.requests.closeSubscription(requestId)`.
2. A new subscription is opened with the updated list of `#r` values.

This avoids receiving and filtering every `kind:7000` on the network.

Subscription IDs are generated by NDK. Explicit IDs must not be passed: NDK keeps a process-wide global state, so two instances subscribing with the same ID silently clobber each other.

### Computed Rebuild

`rebuildComputed()` is a **private** method invoked only when the schema version changes (migration detection). It:

1. Drops the `jobs` and `packages` stores.
2. For each entry in `decrypted_payloads`, parses the payload into a single-request job fragment (the `dvmPubkey` comes from the cached `kind:5905` event's `p` tag). Tombstoned requests are skipped.
3. Merges fragments sharing a `job_id` into one job, one request per `kind:5905` event.
4. Rebuilds packages from the cached `kind:31234` manifests.
5. Reads matching `kind:7000` feedbacks from the NDK cache and applies their statuses per request.
6. Writes all jobs back into the `jobs` store.

All lookups go directly through the NDK cache manager (`ndk.config.cache`), so this operation requires **zero network access** and **zero user action**.

---

## Flow Diagrams

### Creating a Schedule

```
User calls schedule(event, [dvmA, dvmB])
  |
  v
Generate ONE jobId
  |
  v
Determine scheduleAt (at || event.createdAt)
Determine targetRelays (relays || user write relays)
  |
  v
Build JSON payload ONCE (job_id, schedule_at, signed_event, relays)
  |
  v
For each DVM:
  encrypt payload with NIP-44 for that DVM
  create & sign kind:5905 with ["p", dvm]
  |
  v
Persist:
  - decrypted_payloads: requestEventId -> payload (one per request)
  - jobs: jobId -> ScheduledJob(requests: [dvmA, dvmB], pending)
  |
  v
OfflineBroadcast.broadcast(each request, relays: user nip65 + dvm read relays)
(NDK broadcast also saves the raw kind:5905 to its cache)
  |
  v
Update kind:7000 subscription with the new jobId (one r value)
```

### Receiving a DVM Feedback

```
kind:7000 received from subscription
  |
  v
Extract ephemeral-pubkey and r tag (jobId)
  |
  v
Decrypt content with signer.decryptNip44
  |
  v
Store in decrypted_payloads
  |
  v
Attribute to the job request whose dvmPubkey == feedback pubkey
(unknown pubkey -> ignored)
  |
  v
Update that request's status; job status = aggregate of requests
  |
  v
Emit StatusUpdate (jobId, dvmPubkey, status) on statusUpdates stream
```

### App Restart

```
App starts
  |
  v
EventScheduler instantiated (no network yet)
  |
  v
listJobs() reads from jobs store -> shows current state
  |
  v
User or app calls startListening()
  |
  v
resync() fetches missing kind:5905 / kind:5 / kind:7000
  |
  v
SyncState -> synced after EOSE
```

---

## Testing Strategy

The package uses the `MockRelay` from the `ndk` test suite for integration tests. Typical test scenarios include:

- Scheduling an event and verifying the `kind:5905` payload.
- Scheduling redundantly and verifying one `kind:5905` per DVM with identical payloads and a shared `job_id`.
- Simulating per-DVM feedbacks and verifying the aggregated status.
- Cancelling a job and verifying the single multi-tag `kind:5` broadcast.
- Testing offline behavior: signer disconnected, `decryptPending()` called later.
- Testing multi-device sync: injecting `kind:5905` requests sharing a `job_id` and verifying they merge into one job.
- Verifying interop against real `nostr_scheduler_dvm` instances, including a DVM that was offline at schedule time and picks the request up on resync.
