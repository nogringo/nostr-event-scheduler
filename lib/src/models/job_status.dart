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
  error;

  /// Aggregates the statuses of the requests of one job.
  ///
  /// One publication is enough, so the most advanced status wins:
  /// `published` > `scheduled` > `pending` > `failed` > `error` > `cancelled`.
  /// Returns null when [statuses] is empty.
  static JobStatus? aggregate(Iterable<JobStatus> statuses) {
    const priority = [
      JobStatus.published,
      JobStatus.scheduled,
      JobStatus.pending,
      JobStatus.failed,
      JobStatus.error,
      JobStatus.cancelled,
    ];
    final present = statuses.toSet();
    for (final status in priority) {
      if (present.contains(status)) return status;
    }
    return null;
  }
}
