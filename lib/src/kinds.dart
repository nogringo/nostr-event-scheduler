/// Event kinds of the Scheduler DVM protocol, plus NIP-09 deletions.
library;

const int kindDeletion = 5;
const int kindScheduleRequest = 5905;
const int kindFeedback = 7000;

/// NIP-37 private relays, encrypted to their owner.
const int kindPrivateRelays = 10013;
const int kindPackageManifest = 31234;
