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

The first local Apple M4 Max smoke baseline (not a release budget) measured
approximately 270 ns/op for a memory hot key, 31 microseconds for adversarial
high-cardinality churn, and 0.4/5/18/70 microseconds for batch sizes
1/16/64/256. Re-establish baselines on release hardware before defining
regression budgets.
