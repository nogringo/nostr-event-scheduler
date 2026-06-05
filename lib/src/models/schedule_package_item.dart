import 'package:ndk/entities.dart' show Nip01Event;

/// Describes one Scheduler DVM job to create inside a scheduled package.
class SchedulePackageItem {
  /// The already-signed event that the DVM should publish.
  final Nip01Event event;

  /// Public key of the Scheduler DVM for this job.
  final String dvmPubkey;

  /// Optional publication time for this job.
  ///
  /// If omitted, falls back to [event.createdAt].
  final DateTime? at;

  /// Relay URLs where this job's DVM should publish [event].
  ///
  /// If omitted, falls back to the user's NIP-65 write relays.
  final List<String>? relays;

  /// Fallback DVM read relays used when the DVM's NIP-65 list is unavailable.
  final List<String>? dvmReadRelays;

  SchedulePackageItem({
    required this.event,
    required this.dvmPubkey,
    this.at,
    this.relays,
    this.dvmReadRelays,
  });
}
