# Performance guide

Use memory for the lowest latency when process-local semantics are correct.
Use Valkey for high-throughput shared limits. PostgreSQL is for transactional
coordination and must not be selected as the default without workload evidence.

Run:

    make benchmark

The suite reports allocations and throughput for hot-key contention,
high-cardinality churn, batch sizes 1/16/64/256, and live backend round trips.
Capture p50/p95/p99 in a stable external harness because go test benchmark
output reports aggregate nanoseconds per operation.

Benchmark with production-like key skew, policy mix, cost distribution,
cardinality, batch size, connection pools, Valkey cluster topology, and
PostgreSQL lock contention. Race builds are correctness tools, not latency
baselines.

The blocking benchmark gate runs three 10,000-operation samples so parallel
harness setup is amortized before enforcing these portable ceilings:

| Path | Latency | Bytes/op | Allocs/op |
| --- | ---: | ---: | ---: |
| memory hot key | 5 us | 256 | 2 |
| high-cardinality churn | 200 us | 8,192 | 16 |
| batch 1 | 5 us | 512 | 8 |
| batch 16 | 50 us | 8,192 | 64 |
| batch 64 | 200 us | 32,768 | 256 |
| batch 256 | 800 us | 131,072 | 1,024 |

The July 17 Apple M4 Max evidence remained below 1.1 us for a hot key and below
the documented ceilings for cardinality and batch workloads. Longer
steady-state reports remain useful for trend analysis, while the generous
blocking ceilings detect orders-of-magnitude regressions without pretending
that developer and CI hardware are identical.

## Enforced resource budgets

| Resource | Hard limit | Enforcement |
| --- | ---: | --- |
| policy ID | 64 bytes | Policy construction |
| policy revision | 64 bytes | Policy construction |
| raw subject | 256 bytes | Key construction |
| batch decisions | 256 | Service.Batch |
| observers per service | 16 | Service construction |
| memory keys | 1,000,000 configured | memory.New |
| memory shards | 1,024 | memory.New |
| active leases per policy/key | 1,024 | Policy construction |
| Valkey prefix | 64 bytes | valkey.New |
| trusted proxy prefixes | 64 | ClientIP extractor construction |
| forwarded header | 4,096 bytes and 32 hops | request parsing |
| PostgreSQL cleanup batch | 10,000 rows | Store.Cleanup |

Memory keys remain bounded by the deployment's lower configured MaxKeys.
Valkey state has a bounded TTL and lease field count. PostgreSQL cleanup uses
the expires_at index, LIMIT, and SKIP LOCKED, so reclamation never requires a
full unbounded application scan. Connection and goroutine budgets remain
owned by pgx, valkey-go, and the application; this package starts no background
goroutines and performs no internal retry or wait loop.
