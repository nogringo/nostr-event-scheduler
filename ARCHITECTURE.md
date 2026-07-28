# Nostr Event Scheduler - Architecture

## Overview

`nostr_event_scheduler` is a local-first Dart package that lets clients schedule Nostr events to be published at a future time through a Scheduler DVM (Data Vending Machine), as defined by the Scheduler DVM protocol.

A job can be scheduled redundantly through several DVMs: one `kind:5905` request per DVM, all sharing the same `job_id` and the exact same payload. Every `kind:5905` with the same `job_id` schedules the same event at the same time. Since the target event is signed before fan-out, publication stays deterministic: whichever DVM publishes first wins and relays deduplicate the event by ID.

The package is built around a strict separation between **raw data** and **computed data**. This mirrors the Nostr philosophy: raw facts are immutable and definitive, so anything derived from them can be dropped and recomputed without network access or user action.

---

## Core Principles

1. **Local-first**: Every operation is persisted locally before any network attempt. The app works fully offline.
2. **Raw vs Computed**: Raw holds definitive facts, that is the signed events plus what is derived from them and is itself immutable (their decrypted payloads and deletion tombstones). Computed holds the projections. Raw is never dropped and never migrated; computed is dropped and recomputed on every schema change, so a schema bump needs no migration script.
3. **Multi-account**: Every method is scoped to an explicit account `pubkey`. The logged account is never used implicitly, so one instance serves as many accounts as the host loads in `ndk.accounts`.
4. **Offline signer support**: If the signer is unavailable (e.g. NIP-46 remote signer disconnected), encrypted events are queued for later decryption. The app continues to work with already-decrypted data.
5. **Controlled network access**: Listening to relays is explicit and per account (`startListening` / `stopListening`). The scheduler instance can be used entirely offline or on-demand via manual `resync()`.

---

## Dependencies

- [`ndk`](https://pub.dev/packages/ndk) `^0.8.4-dev.5`: Nostr Dev Kit for event signing, encryption (NIP-44), relay communication, and persistent caching.
- [`sembast`](https://pub.dev/packages/sembast): Local NoSQL database for package-specific state.
- [`broadcast_queue_shim_for_ndk`](https://pub.dev/packages/broadcast_queue_shim_for_ndk): Offline-first broadcast queue. The caller provides a configured `OfflineBroadcast` instance.

---

## Data Stores

Raw lives in two places: the signed events in the NDK cache, and what the scheduler derives from them that is itself definitive, in Sembast. Computed lives entirely in Sembast.

### NDK Cache (external, persistent), raw

The NDK maintains its own persistent cache of Nostr events. The scheduler does **not** duplicate raw events in Sembast. Instead, it queries the NDK cache for:

- `kind:5905` (schedule requests)
- `kind:31234` (package manifests)
- `kind:5` (deletions / cancellations)
- `kind:7000` (DVM feedbacks)

Because recomputing reads this cache, the host **must** give `Ndk` a persistent `CacheManager`: with an in-memory one, projections cannot survive a restart. Everything the scheduler creates is written there explicitly (`cache.saveEvent`) rather than waiting for the broadcast to succeed, and every subscription and query runs with `cacheWrite: true`, so raw stays complete even when the device is offline for a long time.

### Sembast Stores

The scheduler owns six Sembast stores, all prefixed with `nostr_event_scheduler/`:

| Store | Key | Value | Tier |
|-------|-----|-------|------|
| `decrypted_payloads` | Event ID (`String`) | Decrypted JSON payload (`String`) | **Raw** |
| `tombstones` | Request Event ID (`String`) | Deletion metadata (`Map`) | **Raw** |
| `jobs` | Job ID (`String`) | `ScheduledJob` (`Map`) | **Computed** |
| `packages` | Package ID (`String`) | `ScheduledPackage` (`Map`) | **Computed** |
| `pending_decryption` | Event ID (`String`) | Owner pubkey (`String`) | **Computed** |
| `schema_version` | `computed_schema` / `built/<pubkey>` | Version (`int`) | Meta |

Raw records are keyed by event id and carry **no account**: attribution to an account is always read back from the signed events themselves. Computed records carry a `pubkey` field, which is what makes an account-scoped read or clear a simple `Finder`.

#### `decrypted_payloads` (raw)

The plaintext of `kind:5905`, `kind:31234` and `kind:7000` events. A given event id always decrypts to the same plaintext, so this is definitive rather than a cache. It is also expensive to recompute: it needs the signer, which may be remote (NIP-46) and require user interaction per event. It is therefore never dropped on a schema change.

#### `tombstones` (raw)

Known deletions (`kind:5` tagging a request or a manifest). Lets the scheduler skip a cancelled event without querying the network, and keeps a cancelled job cancelled across a recompute.

#### `jobs` and `packages` (computed)

The projections read by the public API, including the aggregated job status (`pending`, `scheduled`, `published`, `failed`, `cancelled`, `error`).

#### `pending_decryption` (computed)

Event IDs received but not yet decrypted (signer offline), holding the pubkey of the account that awaits them. `decryptPending()` processes this queue for one account. It is a projection of "raw events this account cannot read yet", so it is dropped and refilled by a recompute.

#### `schema_version` (meta)

`computed_schema` is the shape version of the computed stores. When it lags, every computed store is dropped via `StoreRef.drop`, which removes records without decoding them, so an older value shape can never raise a type error. `built/<pubkey>` records the version each account's projections were last built at, which is what makes recomputing lazy and per account.

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
  final String pubkey; // The account owning the job
  final String jobId;
  final String dvmPubkey; // The reporting DVM
  final JobStatus status;
  final String? message;
  final DateTime receivedAt;
}
```

A `kind:7000` is signed by the DVM, so it is attributed to an account through the job its `r` tag points to.

### `SyncState`

Tracks whether the local state is up-to-date with the network.

```dart
enum SyncStatus { initial, syncing, synced, error }

class SyncState {
  final String pubkey;
  final SyncStatus status;
  final DateTime? lastSyncAt;
  final String? error;
}
```

The stream carries every listened account, so `pubkey` says which one a state refers to.

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
- Providing a configured `Ndk` instance with a **persistent** `CacheManager` and the accounts to be scheduled for loaded in `ndk.accounts`.
- Providing a started `OfflineBroadcast` instance (the shim handles its own persistence and retry logic).
- Providing an open Sembast `Database`.

Every method takes an explicit account `pubkey`. The signer is resolved with `ndk.accounts.accounts[pubkey]`, so no account switching is ever needed and the logged account is irrelevant.

#### Network Control

```dart
Future<void> startListening({required String pubkey});
Future<void> stopListening({String? pubkey});
Future<void> resync({required String pubkey});
```

- `startListening()`: Starts real-time NDK subscriptions for one account: multi-device sync (`kind:5905`, `kind:31234`, `kind:5`) and DVM feedbacks (`kind:7000`). Triggers an initial `resync()`. Each listened account costs its own subscriptions, so only start the ones actually in use.
- `stopListening()`: Closes the subscriptions of one account, or of every account when `pubkey` is omitted. The scheduler remains fully usable offline.
- `resync()`: Forces a manual network fetch for one account. Uses `ndk.fetchedRanges` to avoid re-downloading already-known data.

#### Local Operations

```dart
Future<void> decryptPending({required String pubkey});
Future<void> clearLocalAccountData({required String pubkey});
Future<void> clearAllLocalData();
```

`decryptPending()` processes the `pending_decryption` queue of one account. For each event ID, it reads the raw event from the NDK cache, attempts decryption, and on success stores the payload in `decrypted_payloads` and updates the projections.

`clearLocalAccountData()` removes every local trace of one account: its projections, the raw records derived from its events, its scheduler events in the NDK cache, and the fetched ranges that would refill them. Listening for that account is stopped first. Cancelled requests are hidden from `loadEvents` by the cache visibility rules, so their ids are recovered from the `e` tags of the account's `kind:5` deletions. Those deletions are themselves left in the cache: they are generic, and dropping them would resurrect cancelled jobs.

This is a local reset, not a protocol-level forget. The requests still live on the relays, so an account whose signer is still loaded rebuilds them on its next `resync()`. Use `cancel()` to actually retract a schedule.

`clearAllLocalData()` does the same for every account, unattributed records included.

#### CRUD

```dart
Future<ScheduledJob> schedule(
  Nip01Event event,
  List<String> dvmPubkeys, {
  required String pubkey,
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
6. Writes each request to raw (`cache.saveEvent` plus its plaintext in `decrypted_payloads`) and the computed job in `jobs`.
7. Broadcasts every request via `OfflineBroadcast` to the account's NIP-65 relays (read + write) plus the target DVM's read relays. Throws if no relays are found.
8. Updates the live `kind:7000` subscription to include the new job ID (a single `r` value regardless of the number of DVMs).

```dart
Future<void> cancel(String jobId, {required String pubkey});
```

1. Looks up the job and rejects it if it belongs to another account.
2. Resolves the broadcast relays, then creates and signs **one** `kind:5` deletion event tagging every `kind:5905` request of the job. Relays are resolved first because saving the deletion hides its targets from the cache's visibility rules.
3. Broadcasts it via `OfflineBroadcast` to the account's relays plus every DVM's read relays.
4. Records one tombstone per request and removes the job.

```dart
Future<List<ScheduledJob>> listJobs({required String pubkey});
Stream<List<ScheduledJob>> jobsStream({required String pubkey});
```

Reads the account's slice of the computed `jobs` store, recomputing it first if its schema is stale. `jobsStream` emits live updates.

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

Private Sembast wrapper around the six stores. Handles JSON serialization, the raw and computed split, and account-scoped reads and deletes.

### Multi-device Sync

When `startListening()` or `resync()` is called for an account, the scheduler:

1. Queries the network for `kind:5905` authored by that account, using `ndk.fetchedRanges` to only request missing time ranges.
2. Queries the network for `kind:31234` and `kind:5` authored by that account (same optimization).
3. Queries the network for `kind:7000` filtered by `#r` tags (the account's known job IDs).

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

### Computed Recompute

Recomputing is **private**, lazy and per account. Every entry point that reads or writes projections first awaits `_ensureBuilt(pubkey)`, which is a no-op once `built/<pubkey>` matches the current version; concurrent callers share the same in-flight rebuild.

When an account's projections are stale, the scheduler:

1. Drops that account's `jobs`, `packages` and `pending_decryption` records.
2. Replays its cached `kind:5905` events, oldest first, through the ordinary event handler. Requests sharing a `job_id` merge into one job, one request per event; tombstoned ones are skipped, and ones that cannot be decrypted go back to `pending_decryption`.
3. Replays its cached `kind:31234` manifests, which link the jobs the first pass just built.
4. Applies the matching `kind:7000` feedbacks from the NDK cache, oldest first.

Anchoring on raw rather than on `decrypted_payloads` is what makes the owner and the DVM pubkey exact: they are read from the signed event itself (`event.pubKey` and its `p` tag) instead of being guessed. Replaying through the normal handlers also means there is a single code path to keep correct.

All lookups go through the NDK cache manager (`ndk.config.cache`), so this requires **zero network access** and **zero user action**.

---

## Flow Diagrams

### Creating a Schedule

```
User calls schedule(event, [dvmA, dvmB], pubkey: account)
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
  - raw: cache.saveEvent(request) + decrypted_payloads[requestEventId] = payload
  - computed: jobs[jobId] = ScheduledJob(pubkey, requests: [dvmA, dvmB], pending)
  |
  v
OfflineBroadcast.broadcast(each request, relays: account nip65 + dvm read relays)
  |
  v
Update kind:7000 subscription with the new jobId (one r value)
```

### Receiving a DVM Feedback

```
kind:7000 received from subscription
  |
  v
Extract r tag (jobId) -> look up the job -> owner pubkey
  |
  v
Decrypt content with that account's signer (ephemeral-pubkey as counterparty)
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
Emit StatusUpdate (pubkey, jobId, dvmPubkey, status) on statusUpdates stream
```

### App Restart

```
App starts
  |
  v
EventScheduler instantiated (no network yet)
  |
  v
listJobs(pubkey) -> recomputes from raw if the schema changed, then reads
  |
  v
User or app calls startListening(pubkey)
  |
  v
resync() fetches missing kind:5905 / kind:31234 / kind:5 / kind:7000
  |
  v
SyncState(pubkey) -> synced after EOSE
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
- Testing multi-account isolation: two accounts on one `Database`, each seeing only its own schedules, and a cancel rejected across accounts.
- Testing the clears: one account dropped while the other is untouched, and raw purged so the cleared account is not rebuilt from cache.
- Testing the recompute: dropping the computed tier and verifying the projections come back from raw with the right owner and DVM, and that a cancelled job stays cancelled.
