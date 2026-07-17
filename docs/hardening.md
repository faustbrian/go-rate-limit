# Hardening evidence

The local release gate maps product risks to executable evidence:

- reference and conformance: ratelimittest plus memory/Valkey/PostgreSQL suites;
- arithmetic: weighted, boundary, rollback, overflow, and revision tests;
- atomicity: Valkey Lua and PostgreSQL transaction/lock fault injection;
- resources: memory MaxKeys/Sweep, Valkey TTL, PostgreSQL indexed Cleanup;
- concurrency: race tests, hot-key parallel benchmark, weighted lease tests;
- hostile input: key, proxy, persisted-state, and reply fuzz targets;
- outages: timeout, client loss, script cache, lock, write, commit, and cleanup;
- security: redaction, controlled labels, proxy bounds, opaque persisted keys;
- coverage: scripts/check-coverage.sh requires exact 100.0% in every production
  package with live backend fixtures enabled;
- mutation: scripts/check-mutation.sh targets allow/reject and failure branches.

Hosted workflow status is intentionally separate from local completion. Run
make check locally first, then verify the exact release commit in GitHub Actions.
