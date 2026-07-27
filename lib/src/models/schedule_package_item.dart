import 'package:ndk/entities.dart' show Nip01Event;

/// Describes one Scheduler DVM job to create inside a scheduled package.
class SchedulePackageItem {
  /// The already-signed event that the DVMs should publish.
  final Nip01Event event;

  /// Public keys of the Scheduler DVMs for this job.
  ///
  /// The job is sent redundantly to every listed DVM, one kind:5905 request
  /// per DVM, all sharing the same job_id.
  final List<String> dvmPubkeys;

  /// Optional publication time for this job.
  ///
  /// If omitted, falls back to [event.createdAt].
  final DateTime? at;

  /// Relay URLs where this job's DVMs should publish [event].
  ///
  /// If omitted, falls back to the user's NIP-65 write relays.
  final List<String>? relays;

  /// Fallback DVM read relays used when a DVM's NIP-65 list is unavailable.
  final List<String>? dvmReadRelays;

  SchedulePackageItem({
    required this.event,
    required List<String> dvmPubkeys,
    this.at,
    this.relays,
    this.dvmReadRelays,
  }) : dvmPubkeys = List.unmodifiable(dvmPubkeys) {
    if (this.dvmPubkeys.isEmpty) {
      throw ArgumentError('dvmPubkeys must not be empty');
    }
  }
}
