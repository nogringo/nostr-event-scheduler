import 'job_status.dart';

/// Emitted whenever a DVM feedback is received and processed.
class StatusUpdate {
  /// Public key of the account owning the job this feedback belongs to.
  final String pubkey;

  /// The stable job identifier.
  final String jobId;

  /// Public key of the Scheduler DVM that reported this status.
  final String dvmPubkey;

  /// The new status reported by the DVM for its request.
  ///
  /// The job-level status is [ScheduledJob.status], aggregated across all
  /// the job's requests.
  final JobStatus status;

  /// Optional human-readable message from the DVM.
  final String? message;

  /// When the feedback was processed locally.
  final DateTime receivedAt;

  StatusUpdate({
    required this.pubkey,
    required this.jobId,
    required this.dvmPubkey,
    required this.status,
    this.message,
    required this.receivedAt,
  });
}
