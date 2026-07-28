/// Represents the current synchronization state with the network.
enum SyncStatus { initial, syncing, synced, error }

class SyncState {
  /// Public key of the account this state refers to.
  final String pubkey;

  final SyncStatus status;
  final DateTime? lastSyncAt;
  final String? error;

  const SyncState({
    required this.pubkey,
    required this.status,
    this.lastSyncAt,
    this.error,
  });
}
