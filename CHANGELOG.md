## 0.1.1

- Fix `kind:5` sync to filter by `#k` tag (`5905`). Previously all user deletion events were fetched, which is unnecessary and potentially huge.
- Treat `kind:5` as a hard delete. Jobs are now removed from the local store when cancelled by the user, instead of being kept with `status: cancelled`. This prevents divergence between devices when a schedule is created and deleted while another device is offline.

## 0.1.0

- Initial version.
