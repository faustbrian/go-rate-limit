# Operations runbook

## Signals

Alert on rejection ratio by controlled policy ID, fail-open decisions,
backend-unavailable and deadline errors, p95/p99 decision latency, PostgreSQL
lock waits, Valkey command latency, memory eviction/sweep volume, and state-key
cardinality. Never label metrics with raw subjects.

## Valkey incident

1. Confirm Valkey 9+, cluster slot health, noeviction, memory, command latency,
   and script errors.
2. Distinguish a timeout from an unknown committed result.
3. Apply each policy's documented fail mode; do not globally flip security
   limits open.
4. After recovery, verify TTLs, NOSCRIPT recovery, and hot-key latency.

## PostgreSQL incident

1. Check pool saturation, lock_timeout, statement latency, deadlocks, and table
   growth.
2. Run Cleanup in bounded batches; it uses SKIP LOCKED.
   Each call accepts at most 10,000 rows.
3. Confirm the expires_at index exists and autovacuum keeps pace.
4. Move high-throughput policies to Valkey only after conformance and load
   testing; changing authority during an incident changes capacity.

## Memory incident

Confirm MaxKeys, shard distribution, eviction, sweep cadence, and replica
count. Memory state cannot be reconstructed as cluster-wide truth.

Administrative inspection should show policy metadata and aggregate counts.
Do not return raw credentials, IPs, principals, tenants, hashes that can be
joined to sensitive data, or individual lease IDs by default.
