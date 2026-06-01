/// Represents the lifecycle status of a scheduled job.
enum JobStatus {
  /// The kind:5905 has been broadcast but no DVM feedback has been received yet.
  pending,

  /// DVM has accepted and queued the job.
  scheduled,

  /// The event has been broadcast to the requested relays by the DVM.
  published,

  /// All relays rejected or were unreachable.
  failed,

  /// The job was cancelled via kind:5.
  cancelled,

  /// The job request was invalid.
  error,
}
