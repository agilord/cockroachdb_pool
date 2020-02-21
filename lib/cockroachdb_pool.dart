library cockroachdb_pool;

import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:http/http.dart';
import 'package:meta/meta.dart';
import 'package:retry/retry.dart';
import 'package:postgres_pool/postgres_pool.dart';

export 'package:postgres_pool/postgres_pool.dart';

/// Settings for [CrdbPool].
class CrdbPoolSettings extends PgPoolSettings {
  /// The time between automatic updates of the cluster information (e.g.
  /// detecting new nodes or re-enabling blacklisted nodes).
  Duration clusterUpdateThreshold = Duration(minutes: 15);

  /// The maximum time after a node is no longer considered part of the cluster.
  Duration nodeInactivityThreshold = Duration(minutes: 10);

  @override
  void applyFrom(PgPoolSettings other) {
    super.applyFrom(other);
    if (other is CrdbPoolSettings) {
      clusterUpdateThreshold = other.clusterUpdateThreshold;
    }
  }
}

/// A function that returns the base HTTP URI of a Postgres endpoint for node
/// discovery and other cluster status data.
typedef CrdbApiBaseUriFn = FutureOr<String> Function(PgEndpoint endpoint);

String _defaultCrdbApiBaseUrlFn(PgEndpoint endpoint) {
  final scheme = endpoint.requireSsl ? 'https' : 'http';
  final httpPort = endpoint.port == 26257 ? 8080 : endpoint.port + 1;
  return '$scheme://${endpoint.host}:$httpPort';
}

/// CockroachDB connection pool for cluster access.
class CrdbPool implements PgPool {
  final _events = StreamController<CrdbPoolEvent>.broadcast();
  final _nodes = <_CrdbNode>[];
  final _random = Random.secure();
  final CrdbApiBaseUriFn _baseUriFn;
  final _endpoints = <PgEndpoint>[];
  DateTime _lastNodeUpdated;
  CrdbPoolSettings _settings;

  CrdbPool({
    @required List<PgEndpoint> endpoints,
    CrdbPoolSettings settings,
    CrdbApiBaseUriFn baseUriFn,
  })  : _settings = settings ?? CrdbPoolSettings(),
        _baseUriFn = baseUriFn ?? _defaultCrdbApiBaseUrlFn {
    _endpoints.addAll(endpoints);
  }

  @override
  CrdbPoolSettings get settings => _settings;

  @override
  set settings(PgPoolSettings value) {
    if (value is CrdbPoolSettings) {
      _settings = value;
    } else {
      _settings.applyFrom(value);
    }
  }

  @override
  Stream<CrdbPoolEvent> get events => _events.stream;

  @override
  CrdbPoolStatus status() {
    return CrdbPoolStatus(
      nodes: _nodes.map((n) => n.status()).toList(),
    );
  }

  @override
  Future<int> execute(
    String fmtString, {
    Map<String, dynamic> substitutionValues,
    int timeoutInSeconds,
    String sessionId,
    String traceId,
  }) {
    return run(
      (c) => c.execute(
        fmtString,
        substitutionValues: substitutionValues,
        timeoutInSeconds: timeoutInSeconds,
      ),
      sessionId: sessionId,
      traceId: traceId,
    );
  }

  @override
  Future<PostgreSQLResult> query(
    String fmtString, {
    Map<String, dynamic> substitutionValues,
    bool allowReuse = true,
    int timeoutInSeconds,
    String sessionId,
    String traceId,
  }) {
    return run(
      (c) => c.query(
        fmtString,
        substitutionValues: substitutionValues,
        allowReuse: allowReuse,
        timeoutInSeconds: timeoutInSeconds,
      ),
      sessionId: sessionId,
      traceId: traceId,
    );
  }

  @override
  Future<List<Map<String, Map<String, dynamic>>>> mappedResultsQuery(
    String fmtString, {
    Map<String, dynamic> substitutionValues,
    bool allowReuse = true,
    int timeoutInSeconds,
    String sessionId,
    String traceId,
  }) {
    return run(
      (c) => c.mappedResultsQuery(
        fmtString,
        substitutionValues: substitutionValues,
        allowReuse: allowReuse,
        timeoutInSeconds: timeoutInSeconds,
      ),
      sessionId: sessionId,
      traceId: traceId,
    );
  }

  @override
  int get queueSize => _nodes.fold<int>(0, (a, b) => a + b._pgPool.queueSize);

  @override
  Future<R> run<R>(
    PgSessionFn<R> fn, {
    RetryOptions retryOptions,
    FutureOr<R> Function() orElse,
    FutureOr<bool> Function(Exception) retryIf,
    String sessionId,
    String traceId,
  }) async {
    retryOptions ??= settings.retryOptions;
    try {
      return await retryOptions.retry(
        () async {
          return await _withNode((node) async {
            return await node._pgPool.run(
              fn,
              retryOptions: const RetryOptions(maxAttempts: 1),
              sessionId: sessionId,
              traceId: traceId,
            );
          });
        },
        retryIf: (e) async =>
            e is! PostgreSQLException &&
            e is! IOException &&
            (retryIf == null || await retryIf(e)),
      );
    } catch (e) {
      if (orElse != null) {
        return await orElse();
      }
      rethrow;
    }
  }

  @override
  Future<R> runTx<R>(
    PgSessionFn<R> fn, {
    RetryOptions retryOptions,
    FutureOr<R> Function() orElse,
    FutureOr<bool> Function(Exception) retryIf,
    String sessionId,
    String traceId,
  }) async {
    retryOptions ??= settings.retryOptions;
    try {
      return await retryOptions.retry(
        () async {
          return await _withNode((node) async {
            return await node._pgPool.runTx(
              fn,
              retryOptions: const RetryOptions(maxAttempts: 1),
              sessionId: sessionId,
              traceId: traceId,
            );
          });
        },
        retryIf: (e) async =>
            e is! PostgreSQLException &&
            e is! IOException &&
            (retryIf == null || await retryIf(e)),
      );
    } catch (e) {
      if (orElse != null) {
        return await orElse();
      }
      rethrow;
    }
  }

  @override
  void cancelTransaction({String reason}) {
    // no-op
  }

  @override
  Future close() async {
    while (_nodes.isNotEmpty) {
      final node = _nodes.removeLast();
      await node.close();
    }
    await _events.close();
  }

  _CrdbNode _selectFrom(Iterable<_CrdbNode> items) {
    final list = items.toList();
    if (list.isEmpty) return null;
    return list[_random.nextInt(list.length)];
  }

  Future<R> _withNode<R>(Future<R> Function(_CrdbNode node) fn) async {
    await _updateNodesIfNeeded();
    final node = _selectFrom(_nodes.where((n) => n.isFree)) ??
        _selectFrom(_nodes.where((n) => n.isWorking));
    if (node == null) {
      throw Exception('No working node to use.');
    }
    try {
      node.active++;
      final r = await fn(node);
      node.maybeWorking();
      return r;
    } on IOException catch (_) {
      node.markBad();
      rethrow;
    } on PostgreSQLException catch (_) {
      node.markBad();
      rethrow;
    } on TimeoutException catch (_) {
      node.maybeNotWorking();
      rethrow;
    } finally {
      node.active--;
    }
  }

  Completer _nodeUpdateCompleter;
  Future _updateNodesIfNeeded() async {
    // wait for pending node update
    while (_nodeUpdateCompleter != null) {
      await _nodeUpdateCompleter.future;
    }

    final now = DateTime.now();
    if (_lastNodeUpdated != null &&
        now.difference(_lastNodeUpdated) < settings.clusterUpdateThreshold) {
      return;
    }

    // Setting this to now will remove the race condition for multiple updates
    // running at once.
    _lastNodeUpdated = now;

    _nodeUpdateCompleter = Completer();
    try {
      await _updateEndpoints();
      await _updateNodes();
      _lastNodeUpdated = DateTime.now();
    } finally {
      final c = _nodeUpdateCompleter;
      _nodeUpdateCompleter = null;
      c.complete();
    }
  }

  Future<void> _updateEndpoints() async {
    final shuffled = [..._endpoints]..shuffle();
    for (final ep in shuffled) {
      try {
        final baseUri = await _baseUriFn(ep);
        if (baseUri == null) continue;
        final uri = Uri.parse(baseUri).replace(path: '/_status/nodes');
        final rs = await get(uri);
        final body = json.decode(rs.body) as Map;
        final nodes = body['nodes'] as List;

        final now = DateTime.now();

        final results = <PgEndpoint>[];
        for (final node in nodes.cast<Map>()) {
          final updatedAt = DateTime.fromMillisecondsSinceEpoch(
              int.parse(node['updatedAt'] as String) ~/ 1000000);
          if (now.difference(updatedAt) > settings.nodeInactivityThreshold) {
            continue;
          }
          final desc = node['desc'] as Map;
          final address = (desc['address'] as Map)['addressField'] as String;
          final uri = Uri.parse('pg://$address');

          results.add(ep.replace(host: uri.host, port: uri.port));
        }

        if (results.isNotEmpty) {
          _endpoints.clear();
          _endpoints.addAll(results);
          return;
        }
      } catch (e, st) {
        print(e);
        print(st);
        // TODO: report/log?
      }
    }
    throw Exception('Unable to query nodes.');
  }

  Future<void> _updateNodes() async {
    final oldIds = _nodes.map((n) => n.nodeId).toSet();

    for (final ep in _endpoints) {
      final baseUri = await _baseUriFn(ep);
      if (baseUri == null) continue;
      final apiBaseUri = Uri.parse(baseUri);
      final uri = apiBaseUri.replace(path: '/_status/nodes/local');
      final rs = await get(uri);
      final body = json.decode(rs.body) as Map;
      final desc = body['desc'] as Map;
      final nodeId = desc['nodeId'] as int;

      final oldNode =
          _nodes.firstWhere((a) => a.nodeId == nodeId, orElse: () => null);
      if (oldNode != null) {
        oldNode.markWorking();
      } else {
        _nodes.add(_CrdbNode(nodeId, ep, apiBaseUri, PgPool(ep), _events.sink));
      }
    }

    for (final node in _nodes.where((n) => oldIds.contains(n.nodeId))) {
      node.markBad();
    }

    for (var i = _nodes.length - 1; i >= 0; i--) {
      if (_nodes[i].shouldRemove) {
        final n = _nodes.removeAt(i);
        await n.close();
      }
    }

    _nodes.sort((a, b) => a.nodeId.compareTo(b.nodeId));
  }
}

class CrdbPoolEvent extends PgPoolEvent {
  final int nodeId;
  final PgEndpoint pgEndpoint;
  final Uri apiBaseUri;

  CrdbPoolEvent({
    @required this.nodeId,
    @required this.pgEndpoint,
    @required this.apiBaseUri,
    @required PgPoolEvent event,
  }) : super.fromPgPoolEvent(event);
}

class CrdbNodeStatus extends PgPoolStatus {
  final int nodeId;
  final PgEndpoint pgEndpoint;
  final Uri apiBaseUri;

  CrdbNodeStatus({
    @required this.nodeId,
    @required this.pgEndpoint,
    @required this.apiBaseUri,
    @required PgPoolStatus poolStatus,
  }) : super.fromPgPoolStatus(poolStatus);
}

class CrdbPoolStatus implements PgPoolStatus {
  final List<CrdbNodeStatus> nodes;

  CrdbPoolStatus({
    this.nodes,
  });

  @override
  int get activeSessionCount =>
      nodes.map((a) => a.activeSessionCount).fold<int>(0, (a, b) => a + b);

  @override
  int get pendingSessionCount =>
      nodes.map((a) => a.pendingSessionCount).fold<int>(0, (a, b) => a + b);

  @override
  List<PgConnectionStatus> get connections =>
      nodes.expand((n) => n.connections).toList();
}

class _CrdbNode {
  final int nodeId;
  final PgEndpoint pgEndpoint;
  final Uri apiBaseUri;
  final PgPool _pgPool;
  int active = 0;
  StreamSubscription _eventsSubs;
  int _badScore = 0;
  DateTime _badSince;

  _CrdbNode(this.nodeId, this.pgEndpoint, this.apiBaseUri, this._pgPool,
      Sink<CrdbPoolEvent> sink) {
    _eventsSubs = _pgPool.events.listen((e) {
      sink.add(CrdbPoolEvent(
        nodeId: nodeId,
        pgEndpoint: pgEndpoint,
        apiBaseUri: apiBaseUri,
        event: e,
      ));
    });
  }

  Future<void> close() async {
    await _pgPool.close();
    await _eventsSubs.cancel();
  }

  CrdbNodeStatus status() => CrdbNodeStatus(
        nodeId: nodeId,
        pgEndpoint: pgEndpoint,
        apiBaseUri: apiBaseUri,
        poolStatus: _pgPool.status(),
      );

  void markBad() {
    _badSince ??= DateTime.now();
  }

  void maybeNotWorking() {
    _badScore++;
    if (_badScore >= 100) {
      markBad();
    }
  }

  void maybeWorking() {
    if (_badScore > 0) {
      _badScore--;
    }
  }

  void markWorking() {
    _badScore = 0;
    _badSince = null;
  }

  bool get isBad => !isWorking;
  bool get isWorking => _badSince == null;
  bool get isFree => isWorking && active < _pgPool.settings.concurrency;

  bool get shouldRemove =>
      _badSince != null &&
      DateTime.now().difference(_badSince) > Duration(hours: 1);
}
