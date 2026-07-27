import 'job_status.dart';

/// One kind:5905 request sent to a single Scheduler DVM for a job.
///
/// A job scheduled redundantly through several DVMs holds one request per
/// DVM. Every request carries the same decrypted payload (`job_id`,
/// `schedule_at`, `signed_event`, `relays`); only the encryption recipient
/// differs.
class ScheduledJobRequest {
  /// Public key of the Scheduler DVM this request targets.
  final String dvmPubkey;

  /// ID of the kind:5905 request event that was broadcast.
  final String requestEventId;

  /// Status reported by this DVM.
  JobStatus status;

  /// Optional human-readable message from this DVM.
  String? lastMessage;

  /// Unix timestamp of the last update for this request.
  int updatedAt;

  ScheduledJobRequest({
    required this.dvmPubkey,
    required this.requestEventId,
    this.status = JobStatus.pending,
    this.lastMessage,
    required this.updatedAt,
  });

  Map<String, dynamic> toJson() {
    return {
      'dvmPubkey': dvmPubkey,
      'requestEventId': requestEventId,
      'status': status.name,
      'lastMessage': lastMessage,
      'updatedAt': updatedAt,
    };
  }

  factory ScheduledJobRequest.fromJson(Map<String, dynamic> json) {
    return ScheduledJobRequest(
      dvmPubkey: json['dvmPubkey'] as String,
      requestEventId: json['requestEventId'] as String,
      status: JobStatus.values.byName(json['status'] as String),
      lastMessage: json['lastMessage'] as String?,
      updatedAt: json['updatedAt'] as int,
    );
  }
}
