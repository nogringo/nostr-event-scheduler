import 'job_status.dart';

/// Emitted whenever a DVM feedback is received and processed.
class StatusUpdate {
  /// The stable job identifier.
  final String jobId;

  /// The new status reported by the DVM.
  final JobStatus status;

  /// Optional human-readable message from the DVM.
  final String? message;

  /// When the feedback was processed locally.
  final DateTime receivedAt;

  StatusUpdate({
    required this.jobId,
    required this.status,
    this.message,
    required this.receivedAt,
  });
}
