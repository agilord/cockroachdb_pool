import 'dart:io';

import 'package:docker_process/containers/cockroachdb.dart';
import 'package:test/test.dart';

import 'package:cockroachdb_pool/cockroachdb_pool.dart';

void main() {
  group('basic tests for small cluster', () {
    final initPort = 11200;
    late String hostIp;
    late CrdbPool pool;
    late DockerProcess _dp1;
    late DockerProcess _dp2;
    late DockerProcess _dp3;

    setUpAll(() async {
      final list = await NetworkInterface.list();
      hostIp = list.first.addresses.first.address;
    });

    tearDownAll(() async {
      try {
        await pool.close();
      } catch (e, st) {
        print([e, st]);
      }
      for (final p in [_dp1, _dp2, _dp3]) {
        await p.stop();
        await p.kill();
      }
    });

    Future<DockerProcess> createNode(int i) async {
      final pgPort = initPort + i * 2;
      final httpPort = pgPort + 1;
      return await startCockroachDB(
        name: 'crdb_$pgPort',
        version: 'v20.1.4',
        cleanup: true,
        httpPort: httpPort,
        pgPort: pgPort,
        advertiseHost: hostIp,
        advertisePort: pgPort,
        join: List<int>.generate(3, (x) => x)
            .where((x) => x != i)
            .map((x) => '$hostIp:${initPort + x * 2}')
            .toList(),
        initialize: i == 0,
      );
    }

    test('init', () async {
      _dp1 = await createNode(0);
      _dp2 = await createNode(1);
      _dp3 = await createNode(2);
      pool = CrdbPool(
        endpoints: List<int>.generate(3, (x) => x)
            .map((x) => PgEndpoint(
                  host: hostIp,
                  port: initPort + x * 2,
                  database: 'root',
                  username: 'root',
                  password: 'root',
                ))
            .toList(),
      );
    });

    test('init database', () async {
      await pool.execute('CREATE DATABASE root;');
      expect(pool.status().connections, hasLength(1));
      await pool.execute('CREATE TABLE tbl(id TEXT PRIMARY KEY);');
    });

    test('putting in some data', () async {
      final futures = <Future>[];
      for (var i = 0; i < 100; i++) {
        final f = pool.execute('INSERT INTO tbl VALUES (@id);',
            substitutionValues: {'id': i.toString()});
        futures.add(f);
      }
      await futures.first;
      expect(pool.status().pendingSessionCount, greaterThan(0));
      await Future.wait(futures);
      expect(pool.status().activeSessionCount, 0);
      expect(pool.status().pendingSessionCount, 0);
    });

    test('node status', () async {
      final status = pool.status();
      expect(status.nodes, hasLength(3));
      expect(status.nodes.first.nodeId, 1);
    });
  });
}
