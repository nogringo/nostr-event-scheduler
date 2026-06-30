import 'dart:async';

import 'package:sembast/sembast.dart';

import 'models/scheduled_item.dart';
import 'models/scheduled_job.dart';
import 'models/scheduled_package.dart';

/// Internal Sembast store wrapper.
///
/// All store names are prefixed with `nostr_event_scheduler/` to avoid
/// colliding with stores owned by the host app on the shared [Database].
///
/// Manages six stores (prefix omitted below):
/// - `decrypted_payloads`: eventId -> decrypted JSON payload (cache)
/// - `pending_decryption`: eventId -> true (queue of events waiting for signer)
/// - `tombstones`: requestEventId -> deletion metadata
/// - `jobs`: jobId -> ScheduledJob (computed, droppable)
/// - `packages`: packageId -> ScheduledPackage metadata (computed, droppable)
/// - `schema_version`: migration metadata
class SchedulerStore {
  final Database _db;

  static const String _kStorePrefix = 'nostr_event_scheduler/';
  static const String _kDecryptedPayloads =
      '${_kStorePrefix}decrypted_payloads';
  static const String _kPendingDecryption =
      '${_kStorePrefix}pending_decryption';
  static const String _kTombstones = '${_kStorePrefix}tombstones';
  static const String _kJobs = '${_kStorePrefix}jobs';
  static const String _kPackages = '${_kStorePrefix}packages';
  static const String _kSchemaVersion = '${_kStorePrefix}schema_version';
  static const int _currentSchemaVersion = 2;

  final StoreRef<String, String> _decryptedPayloads;
  final StoreRef<String, bool> _pendingDecryption;
  final StoreRef<String, Map<String, dynamic>> _tombstones;
  final StoreRef<String, Map<String, dynamic>> _jobs;
  final StoreRef<String, Map<String, dynamic>> _packages;
  final StoreRef<String, int> _meta;

  SchedulerStore(this._db)
    : _decryptedPayloads = StoreRef<String, String>(_kDecryptedPayloads),
      _pendingDecryption = StoreRef<String, bool>(_kPendingDecryption),
      _tombstones = stringMapStoreFactory.store(_kTombstones),
      _jobs = stringMapStoreFactory.store(_kJobs),
      _packages = stringMapStoreFactory.store(_kPackages),
      _meta = StoreRef<String, int>(_kSchemaVersion);

  // --------------------------------------------------------------------------
  // Schema / Migration
  // --------------------------------------------------------------------------

  Future<int?> getSchemaVersion() async {
    final record = _meta.record('version');
    return record.get(_db);
  }

  Future<void> setSchemaVersion(int version) async {
    final record = _meta.record('version');
    await record.put(_db, version);
  }

  Future<bool> needsMigration() async {
    final version = await getSchemaVersion();
    return version == null || version < _currentSchemaVersion;
  }

  // --------------------------------------------------------------------------
  // Decrypted payloads
  // --------------------------------------------------------------------------

  Future<void> putDecryptedPayload(String eventId, String payload) async {
    await _decryptedPayloads.record(eventId).put(_db, payload);
  }

  Future<String?> getDecryptedPayload(String eventId) async {
    return _decryptedPayloads.record(eventId).get(_db);
  }

  Future<void> removeDecryptedPayload(String eventId) async {
    await _decryptedPayloads.record(eventId).delete(_db);
  }

  // --------------------------------------------------------------------------
  // Pending decryption
  // --------------------------------------------------------------------------

  Future<void> addPendingDecryption(String eventId) async {
    await _pendingDecryption.record(eventId).put(_db, true);
  }

  Future<void> removePendingDecryption(String eventId) async {
    await _pendingDecryption.record(eventId).delete(_db);
  }

  Future<List<String>> listPendingDecryption() async {
    final records = await _pendingDecryption.find(_db);
    return records.map((r) => r.key).toList();
  }

  Future<bool> isPendingDecryption(String eventId) async {
    final value = await _pendingDecryption.record(eventId).get(_db);
    return value == true;
  }

  // --------------------------------------------------------------------------
  // Tombstones
  // --------------------------------------------------------------------------

  Future<void> putTombstone(
    String requestEventId, {
    String? deletionEventId,
    int? deletedAt,
  }) async {
    await _tombstones.record(requestEventId).put(_db, {
      'deletionEventId': deletionEventId,
      'deletedAt': deletedAt ?? DateTime.now().millisecondsSinceEpoch ~/ 1000,
    });
  }

  Future<bool> isTombstoned(String requestEventId) async {
    final value = await _tombstones.record(requestEventId).get(_db);
    return value != null;
  }

  Future<void> removeTombstone(String requestEventId) async {
    await _tombstones.record(requestEventId).delete(_db);
  }

  Future<List<String>> listTombstonedRequestEventIds() async {
    final records = await _tombstones.find(_db);
    return records.map((r) => r.key).toList();
  }

  // --------------------------------------------------------------------------
  // Jobs (computed)
  // --------------------------------------------------------------------------

  Future<void> putJob(ScheduledJob job) async {
    await _jobs.record(job.jobId).put(_db, job.toJson());
  }

  Future<ScheduledJob?> getJob(String jobId) async {
    final record = await _jobs.record(jobId).get(_db);
    return record == null ? null : ScheduledJob.fromJson(record);
  }

  Future<List<ScheduledJob>> listJobs() async {
    final records = await _jobs.find(_db);
    return records.map((r) => ScheduledJob.fromJson(r.value)).toList();
  }

  Stream<List<ScheduledJob>> watchJobs() {
    return _jobs
        .query()
        .onSnapshots(_db)
        .map(
          (snapshots) =>
              snapshots.map((s) => ScheduledJob.fromJson(s.value)).toList(),
        );
  }

  Future<void> removeJob(String jobId) async {
    await _jobs.record(jobId).delete(_db);
  }

  Future<void> clearJobs() async {
    await _jobs.delete(_db);
  }

  // --------------------------------------------------------------------------
  // Packages (computed)
  // --------------------------------------------------------------------------

  Future<void> putPackage(ScheduledPackage package) async {
    await _packages.record(package.packageId).put(_db, package.toJson());
  }

  Future<ScheduledPackage?> getPackage(String packageId) async {
    final record = await _packages.record(packageId).get(_db);
    if (record == null) return null;
    return ScheduledPackage.fromJson(
      record,
      jobs: await _jobsForRequestEventIds(
        (record['requestEventIds'] as List<dynamic>).map((e) => e as String),
      ),
    );
  }

  Future<List<ScheduledPackage>> listPackages() async {
    final records = await _packages.find(_db);
    final packages = <ScheduledPackage>[];
    for (final record in records) {
      packages.add(
        ScheduledPackage.fromJson(
          record.value,
          jobs: await _jobsForRequestEventIds(
            (record.value['requestEventIds'] as List<dynamic>).map(
              (e) => e as String,
            ),
          ),
        ),
      );
    }
    return packages;
  }

  Stream<List<ScheduledPackage>> watchPackages() {
    return _packages.query().onSnapshots(_db).asyncMap((_) => listPackages());
  }

  Future<void> removePackage(String packageId) async {
    await _packages.record(packageId).delete(_db);
  }

  Future<void> removePackageByManifestEventId(String manifestEventId) async {
    final records = await _packages.find(_db);
    for (final record in records) {
      if (record.value['manifestEventId'] == manifestEventId) {
        await _packages.record(record.key).delete(_db);
      }
    }
  }

  Future<void> clearPackages() async {
    await _packages.delete(_db);
  }

  Future<List<ScheduledItem>> listSchedules() async {
    final packages = await listPackages();
    final packagedRequestIds = packages
        .expand((p) => p.requestEventIds)
        .toSet();
    final standaloneJobs = (await listJobs())
        .where((job) => !packagedRequestIds.contains(job.requestEventId))
        .map(ScheduledItem.job);
    final items = <ScheduledItem>[
      ...standaloneJobs,
      ...packages.map(ScheduledItem.package),
    ];
    items.sort((a, b) => a.createdAt.compareTo(b.createdAt));
    return items;
  }

  // --------------------------------------------------------------------------
  // Rebuild computed
  // --------------------------------------------------------------------------

  /// Drops and rebuilds computed stores from decrypted payloads and tombstones.
  ///
  /// [buildJob] is a callback that receives (eventId, decryptedPayload)
  /// and returns a [ScheduledJob] or null if the payload is invalid.
  Future<void> rebuildComputed(
    Future<ScheduledJob?> Function(String eventId, String payload) buildJob, {
    Future<ScheduledPackage?> Function(String eventId, String payload)?
    buildPackage,
  }) async {
    await clearJobs();
    await clearPackages();

    final payloads = await _decryptedPayloads.find(_db);
    for (final record in payloads) {
      final eventId = record.key;
      final payload = record.value;
      final job = await buildJob(eventId, payload);
      if (job != null) {
        await putJob(job);
      }
    }

    if (buildPackage != null) {
      for (final record in payloads) {
        final eventId = record.key;
        final payload = record.value;
        final package = await buildPackage(eventId, payload);
        if (package != null) {
          await putPackage(package);
        }
      }
    }

    await setSchemaVersion(_currentSchemaVersion);
  }

  Future<List<ScheduledJob>> _jobsForRequestEventIds(
    Iterable<String> requestEventIds,
  ) async {
    final ids = requestEventIds.toSet();
    final jobs = await listJobs();
    return jobs.where((job) => ids.contains(job.requestEventId)).toList();
  }
}
