/// Represents the current synchronization state with the network.
enum SyncStatus { initial, syncing, synced, error }

class SyncState {
  final SyncStatus status;
  final DateTime? lastSyncAt;
  final String? error;

  const SyncState({required this.status, this.lastSyncAt, this.error});
}
