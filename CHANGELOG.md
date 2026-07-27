## 0.3.0

- Support redundant scheduling across multiple Scheduler DVMs (#1).
  **Breaking**: `schedule` now takes a list of DVM pubkeys,
  `schedule(event, [dvmA, dvmB])`, and sends one `kind:5905` request per
  DVM, all sharing the same `job_id` and the exact same payload: every
  `kind:5905` with the same `job_id` schedules the same event at the same
  time. Publication stays deterministic because relays deduplicate the signed
  target event by ID. A single-DVM schedule is `schedule(event, [dvm])`.
- **Breaking**: `ScheduledJob` now models one logical job with one
  `ScheduledJobRequest` per DVM in `requests`. `dvmPubkey`, `requestEventId`,
  and the mutable `status`/`lastMessage` fields moved to the request level.
  The job exposes `dvmPubkeys`, `requestEventIds`, and computed
  `status`/`lastMessage` aggregated across requests (most advanced wins:
  `published` > `scheduled` > `pending` > `failed` > `error` > `cancelled`,
  see `JobStatus.aggregate`). `copyWith` was removed.
- **Breaking**: `StatusUpdate` gains a required `dvmPubkey` identifying the
  reporting DVM; `status` is that DVM's request status.
- **Breaking**: `SchedulePackageItem` takes `dvmPubkeys` (a list) instead of
  `dvmPubkey`, so a package item can fan out to several DVMs too.
- `cancel(jobId)` tags every `kind:5905` request of the job in one `kind:5`
  deletion, cancelling all its DVMs at once.
- Feedbacks are attributed to a request by the `kind:7000` signature pubkey.
  A feedback signed by a pubkey that is not one of the job's DVMs is ignored.
- Feedbacks cached before their request is known (multi-device sync, rebuild)
  are applied once the request appears.
- Stop passing explicit NDK subscription ids: NDK generates unique ids,
  avoiding collisions through the process-wide NDK global state when several
  instances subscribe with the same id.
- Use direct NDK cache reads for rebuild lookups and feedback reconciliation
  instead of network-capable queries, removing query timeouts from these
  paths.
- Bump the computed store schema to v3. Existing installs rebuild
  automatically on the next `startListening()`.

## 0.2.3

- Relax the `ndk` constraint to `^0.8.4-dev.2` so compatible newer versions
  resolve.

## 0.2.2

- Prefix Sembast store names with `nostr_event_scheduler/` to avoid collisions
  on the host app's shared `Database`. Existing installs resync once.

## 0.2.1

- Bump package version to `0.2.1`.
- Update `ndk`, `sembast`, `lints`, and `test` dependency versions.

## 0.2.0

- Add scheduled package support via `kind:31234` manifests that group one or
  more Scheduler DVM `kind:5905` jobs.
- Add `schedulePackage`, `cancelPackage`, `listPackages`, `listSchedules`, and
  `schedulesStream`.
- Add `ScheduledPackage`, `SchedulePackageItem`, and `ScheduledItem` models.
- Broadcast Scheduler DVM requests to both the user's NIP-65 relays and the
  target DVM's read relays. `dvmReadRelays` can be supplied as a fallback when
  the DVM NIP-65 list is unavailable.
- Cancel scheduled packages with one multi-tag `kind:5` that references all
  linked `kind:5905` requests and the package manifest.
- Add integration coverage against `nostr_scheduler_dvm` to verify client/DVM
  interoperability.

## 0.1.1

- Fix `kind:5` sync to filter by `#k` tag (`5905`). Previously all user deletion events were fetched, which is unnecessary and potentially huge.
- Treat `kind:5` as a hard delete. Jobs are now removed from the local store when cancelled by the user, instead of being kept with `status: cancelled`. This prevents divergence between devices when a schedule is created and deleted while another device is offline.

## 0.1.0

- Initial version.
