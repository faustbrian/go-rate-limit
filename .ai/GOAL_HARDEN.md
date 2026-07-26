# Hardening Goal: Application Rate Limiting

## Objective

Prove that `rate-limit` cannot be bypassed, over-admit silently, corrupt
state, leak sensitive keys, or consume unbounded resources under concurrency,
clock anomalies, backend failure, adversarial cardinality, and rolling deploys.

## Required Audits

### Algorithm And Arithmetic Audit

- Differential-test every algorithm against a simple reference model.
- Exhaust capacity, burst, weighted cost, refill, window boundaries, reset,
  retry-after, overflow, rounding, and long-idle behavior.
- Inject clock rollback, leap, large jump, skew, and frozen time.
- Mutation-test every branch capable of changing rejection into admission.

### Concurrency And Distribution Audit

- Race and stress-test same-key and many-key admission, cleanup, and shutdown.
- Prove Valkey scripts and PostgreSQL statements are atomic under contention.
- Exercise failover, reconnect, `NOSCRIPT`, transaction abort, deadlock retry,
  replication lag, partitions, cancellation, and rolling policy revisions.
- Verify retries cannot duplicate admission or grant unintended capacity.

### Cardinality And Resource Audit

- Attack key count, key length, policy count, batch size, waiter count, metric
  labels, cleanup lag, and hot-key contention.
- Enforce memory, storage, goroutine, connection, script, latency, and response
  size budgets.
- Prove expired distributed state is reclaimed without full unbounded scans.

### Security And Integration Audit

- Threat-model proxy spoofing, credential-source ambiguity, key collision,
  tenant crossover, script injection, denial of service, and fail-open abuse.
- Prove raw keys, IPs, credentials, and principals do not leak through errors,
  logs, metrics, traces, or inspection APIs.
- Verify HTTP/RPC headers and retry metadata are exact, bounded, and safe.
- Prove queue rejection does not lose, acknowledge, or hot-loop durable jobs.

## Required Deliverables

- Algorithm truth tables and cross-backend conformance report.
- Threat model, consistency model, outage matrix, and resource budgets.
- Valkey/PostgreSQL fault-injection and rolling-upgrade evidence.
- Race, fuzz, mutation, cardinality, and benchmark reports.
- Updated security, operations, performance, migration, and troubleshooting docs.

## Release Blockers

- Any bypass, unintended over-admission, tenant/key crossover, arithmetic drift,
  non-atomic update, sensitive key leak, panic, race, or unbounded growth.
- Any undocumented difference between memory, Valkey, and PostgreSQL decisions.
- Any hidden or ambiguous fail-open behavior.
- Missing meaningful 100% coverage, mutation evidence, or green blocking CI.

## Completion Criteria

- Algorithm, backend, transport, outage, and adversarial suites pass.
- Performance and cardinality budgets are measured and enforced.
- Race, fuzz, vulnerability, compatibility, and integration gates pass.
- NilAway runs visibly as advisory without blocking findings.
- No release blocker remains and the changelog is current.
