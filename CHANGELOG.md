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
