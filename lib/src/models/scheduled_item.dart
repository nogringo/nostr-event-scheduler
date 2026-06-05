import 'scheduled_job.dart';
import 'scheduled_package.dart';

/// The type of logical scheduled item.
enum ScheduledItemType {
  /// A standalone Scheduler DVM job.
  job,

  /// A package grouping multiple Scheduler DVM jobs.
  package,
}

/// A logical scheduled item shown to callers.
class ScheduledItem {
  /// Whether this item is a standalone job or a package.
  final ScheduledItemType type;

  /// Standalone job when [type] is [ScheduledItemType.job].
  final ScheduledJob? job;

  /// Scheduled package when [type] is [ScheduledItemType.package].
  final ScheduledPackage? package;

  ScheduledItem.job(this.job) : type = ScheduledItemType.job, package = null;

  ScheduledItem.package(this.package)
    : type = ScheduledItemType.package,
      job = null;

  /// Stable identifier for this logical item.
  String get id {
    return switch (type) {
      ScheduledItemType.job => job!.jobId,
      ScheduledItemType.package => package!.packageId,
    };
  }

  /// Unix timestamp when this item was first created locally.
  int get createdAt {
    return switch (type) {
      ScheduledItemType.job => job!.createdAt,
      ScheduledItemType.package => package!.createdAt,
    };
  }
}
