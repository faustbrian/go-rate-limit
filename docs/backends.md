# Backends and consistency

## Memory

Memory is process-local. MaxKeys and Shards are mandatory. Each shard has a
deterministic quota and evicts the least recently observed key, breaking ties
lexicographically. Sweep removes idle state, prunes expired leases, and
preserves live leases. Close prevents new work. The race suite and hot-key
benchmarks exercise contention.

## Valkey

Valkey 9+ is required and Redis compatibility is not claimed. valkey-go is the
native client. One opaque hash-tagged key contains all state for a policy/key
pair, so cluster execution is single-slot. Lua provides atomic mutation,
bounded TTL, revision carry-forward, lease ownership, and clock clamping.
valkey-go handles script loading and NOSCRIPT fallback. Open verifies
noeviction.

Concurrency state checks HLEN before HGETALL and rejects more than 1,024 lease
fields as corrupt. This makes even externally corrupted state fail before a
full field scan.

ClientClock honors Request.Now and enables deterministic tests. ServerClock
uses TIME inside the script and is recommended when client clock skew is a
larger risk than server-clock dependence. Clock selection is a separate script
argument, so the Unix epoch is never mistaken for a server-clock sentinel.
Large integers are encoded as fixed decimal strings at the script boundary.

## PostgreSQL

pgx is the native client. A per-key advisory transaction lock plus row lock
makes mutation atomic. LockTimeout and Timeout bound contention. State keys are
SHA-256 digests. The indexed expires_at column supports Cleanup with
SKIP LOCKED. SchemaMigration and GoMigration assign migration ownership to
migrations.

PostgreSQL is intended for transactional coordination workloads. It is not the
default high-throughput backend; use it only after workload-specific benchmark
evidence.

## Regions and partitions

Strong consistency is limited to the selected backend's authoritative
deployment. Asynchronous replicas are not admission authorities. Independent
regions enforce independent capacity unless requests share one synchronous
authority. Partitions become explicit backend errors and follow policy failure
mode; they never merge hidden counters later.
