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

Strict mutations use separate Lua script identities for strict revision and
corruption validation. Fixed/sliding strict state records the established
period, preserves full same-period consumption through capacity revisions, and
rejects future, duplicate, or wrong-slot active segments before mutation.
Released `Admit` and lease methods retain their v1 state
transition behavior, including legacy policy-revision carry semantics.

NewStrict and OpenStrict reject nil clients. Strict mutation methods declare
any command failure after Lua dispatch as outcome unknown; the supplied client
remains caller-owned and is never closed by the store.
NewStrict performs no client operation. OpenStrict performs one caller-bounded
check and cleans up temporary command resources before returning; failed open
also leaves the borrowed client open.

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

NewStrict and OpenStrict require a positive RollbackTimeout in StrictOptions.
Once a strict operation owns a transaction, every non-commit exit performs one
synchronous rollback under that bounded detached cleanup context. A commit or
rollback failure is outcome unknown; the supplied pool remains caller-owned.
NewStrict performs no pool operation. OpenStrict performs one caller-bounded
schema check, releases temporary query resources before return, and leaves the
borrowed pool open on failure. Only owned transaction rollback detaches caller
cancellation; checks and non-transactional cleanup remain caller-bounded.
Strict state reads reject JSON documents larger than 128 KiB in SQL before the
document bytes cross the driver boundary. The released methods retain their v1
read behavior. Fixed/sliding strict state stores period and carried-usage
provenance in algorithm-unused fields from the released v1 document shape, so
a released v1 reader can decode and round-trip strict-written rows during
caller rollback without interpreting that metadata.

## Regions and partitions

Strong consistency is limited to the selected backend's authoritative
deployment. Asynchronous replicas are not admission authorities. Independent
regions enforce independent capacity unless requests share one synchronous
authority. Partitions become explicit backend errors and follow policy failure
mode; they never merge hidden counters later.
