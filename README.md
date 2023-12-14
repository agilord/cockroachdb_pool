A library to control a maximized number of connections to a CockroachDB cluster server.

Features:
- Scans nodes list (via HTTP API) periodically, automatically add/remove nodes.
- On critical errors or elevated timeouts it marks the node as bad and uses other nodes instead.

## Discontinued

`package:postgres` from `^3.0.0` supports connection pooling as part of the core
library. Development and advanced pooling support will be migrated to that package.

## Usage

Once you've created the `CrdbPool` object, you can:

- Use it as `PostgreSQLExecutionContext` (from `package:postgres`).
- Use it as `PgPool` (from `package:postgres_pool`).
- Use `CrdbPool.run` for non-transactional batches with optional retry.
- Use `CrdbPool.runTx` for transactional batches with optional retry.
