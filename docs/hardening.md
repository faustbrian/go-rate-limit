# Hardening evidence

The local release gate maps product risks to executable evidence:

- reference and conformance: ratelimittest compares exact allow, remaining,
  limit, reason, reset, retry-after, and error results across memory, live
  Valkey, and live PostgreSQL;
- arithmetic: weighted, epoch, boundary, frozen-time, rollback, large-jump,
  exact-integer, overflow, and revision tests;
- atomicity: Valkey Lua and PostgreSQL transaction/lock fault injection;
- resources: memory MaxKeys/Sweep, Valkey TTL, PostgreSQL indexed Cleanup;
- budgets: construction and request validation enforce every limit listed in
  the performance guide's resource-budget table;
- concurrency: the shared 64-worker harness proves many-key independence,
  exact same-key capacity, and idempotent lease retries on every backend;
- rolling revisions: shared conformance lowers and raises every algorithm's
  capacity while retaining consumption and active lease ownership;
- hostile input: key, proxy, persisted-state, and reply fuzz targets;
- outages: timeout, client loss, script cache, lock, write, commit, and cleanup;
- security: redaction, controlled labels, proxy bounds, opaque persisted keys;
- coverage: scripts/check-coverage.sh requires exact 100.0% in every production
  package with live backend fixtures enabled;
- mutation: scripts/check-mutation.sh targets allow/reject and failure branches.
- performance: scripts/check-benchmarks.sh records three samples and blocks
  latency, allocation-byte, or allocation-count budget regressions.

Hosted workflow status is intentionally separate from local completion. Run
make check locally first, then verify the exact release commit in GitHub Actions.

NilAway always runs and prints its complete output, but its findings are
advisory by release policy. The `nilaway` target reports a non-zero analyzer
result and then returns success; all other quality, test, security, mutation,
performance, API, and integration gates remain blocking.
