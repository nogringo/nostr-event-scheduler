import 'dart:async';

import 'package:sembast/sembast.dart';

import 'models/scheduled_job.dart';

/// Internal Sembast store wrapper.
///
/// Manages four stores:
/// - `decrypted_payloads`: eventId -> decrypted JSON payload (cache)
/// - `pending_decryption`: eventId -> true (queue of events waiting for signer)
/// - `tombstones`: requestEventId -> deletion metadata
/// - `jobs`: jobId -> ScheduledJob (computed, droppable)
class SchedulerStore {
  final Database _db;

  static const String _kDecryptedPayloads = 'decrypted_payloads';
  static const String _kPendingDecryption = 'pending_decryption';
  static const String _kTombstones = 'tombstones';
  static const String _kJobs = 'jobs';
  static const String _kSchemaVersion = 'schema_version';
  static const int _currentSchemaVersion = 1;

  final StoreRef<String, String> _decryptedPayloads;
  final StoreRef<String, bool> _pendingDecryption;
  final StoreRef<String, Map<String, dynamic>> _tombstones;
  final StoreRef<String, Map<String, dynamic>> _jobs;
  final StoreRef<String, int> _meta;

  SchedulerStore(this._db)
    : _decryptedPayloads = StoreRef<String, String>(_kDecryptedPayloads),
      _pendingDecryption = StoreRef<String, bool>(_kPendingDecryption),
      _tombstones = stringMapStoreFactory.store(_kTombstones),
      _jobs = stringMapStoreFactory.store(_kJobs),
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
  // Rebuild computed
  // --------------------------------------------------------------------------

  /// Drops and rebuilds the [jobs] store from decrypted payloads and tombstones.
  ///
  /// [buildJob] is a callback that receives (eventId, decryptedPayload)
  /// and returns a [ScheduledJob] or null if the payload is invalid.
  Future<void> rebuildComputed(
    Future<ScheduledJob?> Function(String eventId, String payload) buildJob,
  ) async {
    await clearJobs();

    final payloads = await _decryptedPayloads.find(_db);
    for (final record in payloads) {
      final eventId = record.key;
      final payload = record.value;
      final job = await buildJob(eventId, payload);
      if (job != null) {
        await putJob(job);
      }
    }

    await setSchemaVersion(_currentSchemaVersion);
  }
}
