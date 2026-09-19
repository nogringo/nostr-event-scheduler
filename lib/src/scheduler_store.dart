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
/// Records fall in two tiers (prefix omitted below).
///
/// Raw: definitive facts derived from immutable Nostr events. Keyed by event
/// id, never dropped, never migrated, and carrying no account: attribution to
/// an account is always read back from the signed events themselves.
/// - `decrypted_payloads`: eventId -> decrypted payload
/// - `tombstones`: requestEventId -> deletion metadata
///
/// Computed: projections rebuilt from raw. Dropped and recomputed on every
/// schema bump, so their shape may change freely.
/// - `jobs`: jobId -> [ScheduledJob]
/// - `packages`: packageId -> [ScheduledPackage] metadata
/// - `pending_decryption`: eventId -> pubkey of the account awaiting a signer
/// - `schema_version`: shape version of the computed stores, and the version
///   each account's projections were last built at
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

  static const String _kComputedSchemaKey = 'computed_schema';
  static const String _kBuiltPrefix = 'built/';
  static const int _currentSchemaVersion = 5;

  final StoreRef<String, String> _decryptedPayloads;
  final StoreRef<String, String> _pendingDecryption;
  final StoreRef<String, Map<String, dynamic>> _tombstones;
  final StoreRef<String, Map<String, dynamic>> _jobs;
  final StoreRef<String, Map<String, dynamic>> _packages;
  final StoreRef<String, int> _meta;

  SchedulerStore(this._db)
    : _decryptedPayloads = StoreRef<String, String>(_kDecryptedPayloads),
      _pendingDecryption = StoreRef<String, String>(_kPendingDecryption),
      _tombstones = stringMapStoreFactory.store(_kTombstones),
      _jobs = stringMapStoreFactory.store(_kJobs),
      _packages = stringMapStoreFactory.store(_kPackages),
      _meta = StoreRef<String, int>(_kSchemaVersion);

  // --------------------------------------------------------------------------
  // Schema
  // --------------------------------------------------------------------------

  /// Drops every computed store whose shape predates [_currentSchemaVersion].
  ///
  /// Uses [SembastStoreRefExtension.drop] rather than a filtered delete so no
  /// record from an older shape is ever decoded.
  Future<void> _ensureComputedSchema() async {
    final version = await _meta.record(_kComputedSchemaKey).get(_db);
    if (version == _currentSchemaVersion) return;

    await _jobs.drop(_db);
    await _packages.drop(_db);
    await _pendingDecryption.drop(_db);
    await _meta.drop(_db);
    await _meta.record(_kComputedSchemaKey).put(_db, _currentSchemaVersion);
  }

  /// Whether [pubkey]'s projections must be rebuilt from raw.
  Future<bool> needsRebuild(String pubkey) async {
    await _ensureComputedSchema();
    final built = await _meta.record('$_kBuiltPrefix$pubkey').get(_db);
    return built != _currentSchemaVersion;
  }

  Future<void> markBuilt(String pubkey) async {
    await _meta.record('$_kBuiltPrefix$pubkey').put(_db, _currentSchemaVersion);
  }

  // --------------------------------------------------------------------------
  // Raw - decrypted payloads
  // --------------------------------------------------------------------------

  Future<void> putDecryptedPayload(String eventId, String payload) async {
    await _decryptedPayloads.record(eventId).put(_db, payload);
  }

  Future<String?> getDecryptedPayload(String eventId) async {
    return _decryptedPayloads.record(eventId).get(_db);
  }

  Future<void> _removeDecryptedPayloads(Iterable<String> eventIds) async {
    await _decryptedPayloads.records(eventIds.toList()).delete(_db);
  }

  // --------------------------------------------------------------------------
  // Raw - tombstones
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

  Future<void> _removeTombstones(Iterable<String> requestEventIds) async {
    await _tombstones.records(requestEventIds.toList()).delete(_db);
  }

  // --------------------------------------------------------------------------
  // Computed - pending decryption
  // --------------------------------------------------------------------------

  Future<void> addPendingDecryption(
    String eventId, {
    required String pubkey,
  }) async {
    await _pendingDecryption.record(eventId).put(_db, pubkey);
  }

  Future<void> removePendingDecryption(String eventId) async {
    await _pendingDecryption.record(eventId).delete(_db);
  }

  Future<List<String>> listPendingDecryption(String pubkey) async {
    final records = await _pendingDecryption.find(
      _db,
      finder: Finder(filter: Filter.equals(Field.value, pubkey)),
    );
    return records.map((r) => r.key).toList();
  }

  // --------------------------------------------------------------------------
  // Computed - jobs
  // --------------------------------------------------------------------------

  Future<void> putJob(ScheduledJob job) async {
    await _jobs.record(job.jobId).put(_db, job.toJson());
  }

  Future<ScheduledJob?> getJob(String jobId) async {
    final record = await _jobs.record(jobId).get(_db);
    return record == null ? null : ScheduledJob.fromJson(record);
  }

  Future<List<ScheduledJob>> listJobs(String pubkey) async {
    final records = await _jobs.find(_db, finder: _byPubkey(pubkey));
    return records.map((r) => ScheduledJob.fromJson(r.value)).toList();
  }

  Stream<List<ScheduledJob>> watchJobs(String pubkey) {
    return _jobs
        .query(finder: _byPubkey(pubkey))
        .onSnapshots(_db)
        .map(
          (snapshots) =>
              snapshots.map((s) => ScheduledJob.fromJson(s.value)).toList(),
        );
  }

  Future<void> removeJob(String jobId) async {
    await _jobs.record(jobId).delete(_db);
  }

  // --------------------------------------------------------------------------
  // Computed - packages
  // --------------------------------------------------------------------------

  Future<void> putPackage(ScheduledPackage package) async {
    await _packages.record(package.packageId).put(_db, package.toJson());
  }

  Future<ScheduledPackage?> getPackage(String packageId) async {
    final record = await _packages.record(packageId).get(_db);
    if (record == null) return null;
    return _hydratePackage(record);
  }

  Future<List<ScheduledPackage>> listPackages(String pubkey) async {
    final records = await _packages.find(_db, finder: _byPubkey(pubkey));
    final packages = <ScheduledPackage>[];
    for (final record in records) {
      packages.add(await _hydratePackage(record.value));
    }
    return packages;
  }

  Stream<List<ScheduledPackage>> watchPackages(String pubkey) {
    return _packages
        .query(finder: _byPubkey(pubkey))
        .onSnapshots(_db)
        .asyncMap((_) => listPackages(pubkey));
  }

  Future<void> removePackage(String packageId) async {
    await _packages.record(packageId).delete(_db);
  }

  Future<void> removePackageByManifestEventId(String manifestEventId) async {
    final records = await _packages.find(
      _db,
      finder: Finder(filter: Filter.equals('manifestEventId', manifestEventId)),
    );
    await _packages.records(records.map((r) => r.key).toList()).delete(_db);
  }

  Future<List<ScheduledItem>> listSchedules(String pubkey) async {
    final packages = await listPackages(pubkey);
    final packagedRequestIds = packages
        .expand((p) => p.requestEventIds)
        .toSet();
    final standaloneJobs = (await listJobs(pubkey))
        .where((job) => !job.requestEventIds.any(packagedRequestIds.contains))
        .map(ScheduledItem.job);
    final items = <ScheduledItem>[
      ...standaloneJobs,
      ...packages.map(ScheduledItem.package),
    ];
    items.sort((a, b) => a.createdAt.compareTo(b.createdAt));
    return items;
  }

  // --------------------------------------------------------------------------
  // Clearing
  // --------------------------------------------------------------------------

  /// Drops [pubkey]'s projections, forcing a rebuild from raw on next use.
  Future<void> clearComputed(String pubkey) async {
    await _ensureComputedSchema();
    await _jobs.delete(_db, finder: _byPubkey(pubkey));
    await _packages.delete(_db, finder: _byPubkey(pubkey));
    await _pendingDecryption.delete(
      _db,
      finder: Finder(filter: Filter.equals(Field.value, pubkey)),
    );
    await _meta.record('$_kBuiltPrefix$pubkey').delete(_db);
  }

  /// Removes the raw records derived from [eventIds].
  Future<void> clearRaw(Iterable<String> eventIds) async {
    final ids = eventIds.toList();
    await _removeDecryptedPayloads(ids);
    await _removeTombstones(ids);
  }

  /// Drops every store owned by the package, all accounts included.
  Future<void> clearAll() async {
    await _decryptedPayloads.drop(_db);
    await _tombstones.drop(_db);
    await _jobs.drop(_db);
    await _packages.drop(_db);
    await _pendingDecryption.drop(_db);
    await _meta.drop(_db);
  }

  /// Public keys owning at least one projection.
  Future<Set<String>> listKnownPubkeys() async {
    await _ensureComputedSchema();
    final jobs = await _jobs.find(_db);
    final packages = await _packages.find(_db);
    return {
      ...jobs.map((r) => r.value['pubkey'] as String),
      ...packages.map((r) => r.value['pubkey'] as String),
    };
  }

  // --------------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------------

  Finder _byPubkey(String pubkey) =>
      Finder(filter: Filter.equals('pubkey', pubkey));

  Future<ScheduledPackage> _hydratePackage(Map<String, dynamic> record) async {
    final requestEventIds = (record['requestEventIds'] as List<dynamic>)
        .map((e) => e as String)
        .toSet();
    final jobs = (await listJobs(record['pubkey'] as String))
        .where((job) => job.requestEventIds.any(requestEventIds.contains))
        .toList();
    return ScheduledPackage.fromJson(record, jobs: jobs);
  }
}
