import 'scheduled_job.dart';

/// Represents a logical scheduled package backed by one or more DVM jobs.
class ScheduledPackage {
  /// 64-char hex identifier from the manifest `d` tag.
  final String packageId;

  /// ID of the kind:31234 manifest event.
  final String manifestEventId;

  /// Private application-defined package context.
  final String content;

  /// IDs of the linked kind:5905 request events.
  final List<String> requestEventIds;

  /// Current computed jobs linked by [requestEventIds].
  final List<ScheduledJob> jobs;

  /// Unix timestamp when the package was first created locally.
  final int createdAt;

  /// Unix timestamp of the last local update.
  final int updatedAt;

  ScheduledPackage({
    required this.packageId,
    required this.manifestEventId,
    required this.content,
    required this.requestEventIds,
    required this.jobs,
    required this.createdAt,
    required this.updatedAt,
  });

  Map<String, dynamic> toJson() {
    return {
      'packageId': packageId,
      'manifestEventId': manifestEventId,
      'content': content,
      'requestEventIds': requestEventIds,
      'createdAt': createdAt,
      'updatedAt': updatedAt,
    };
  }

  factory ScheduledPackage.fromJson(
    Map<String, dynamic> json, {
    List<ScheduledJob> jobs = const [],
  }) {
    return ScheduledPackage(
      packageId: json['packageId'] as String,
      manifestEventId: json['manifestEventId'] as String,
      content: json['content'] as String,
      requestEventIds: (json['requestEventIds'] as List<dynamic>)
          .map((e) => e as String)
          .toList(),
      jobs: jobs,
      createdAt: json['createdAt'] as int,
      updatedAt: json['updatedAt'] as int,
    );
  }
}
