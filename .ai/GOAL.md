# Goal: Application Rate Limiting

## Objective

Build a production-grade, transport-neutral rate-limiting package for inbound
requests, RPC methods, queue admission, and application operations with
first-class local, Valkey, and PostgreSQL implementations.

This package MUST replace Laravel's shared inbound `RateLimiter` behavior and
job throttling without overlapping outbound vendor throttling owned by
`http-client`.

## Product Principles

- Admission decisions are typed, deterministic, observable, and bounded.
- Every policy has explicit capacity, refill/window, burst, cost, key, clock,
  consistency, and failure behavior.
- Distributed limits MUST have atomic server-side semantics.
- Fail-open versus fail-closed behavior is explicit per policy and operation.
- Rate limiting MUST NOT silently become authorization or billing metering.
- Cardinality, storage growth, cleanup, and hot-key behavior are first-class.

## Core Model

- Immutable policy identity, revision, algorithm, capacity, period, and burst.
- Typed subject/key and bounded key derivation.
- Weighted admission requests with context and explicit current time.
- Decisions containing allowed, remaining, limit, reset, retry-after, reason,
  backend, and policy revision.
- Reservation and cancellation only for algorithms that can guarantee them.
- Stable errors separating rejection, invalid policy, unavailable backend,
  deadline, overflow, and internal corruption.
- Batch decisions with explicitly documented atomicity.

## Algorithms

- Token bucket with bounded burst and precise refill arithmetic.
- Fixed window with deterministic boundaries.
- Sliding window counter or log only when storage and cost are strictly bounded.
- Concurrency/semaphore admission as a separate policy kind with explicit lease
  and release behavior.
- Algorithms MUST have reference models and cross-backend conformance suites.
- Floating-point drift MUST NOT cause cross-backend decision divergence.

## Backends

### Memory

- Bounded cardinality, deterministic eviction, cleanup, and shutdown.
- Sharded or otherwise contention-aware implementation with race proof.
- Explicit process-local semantics; never presented as cluster-wide.

### Valkey

- Native `valkey-go` adapter with atomic scripts or functions.
- Cluster-safe key strategy and explicit hash-tag behavior.
- Script loading, `NOSCRIPT`, failover, reconnect, timeout, and rolling-version
  compatibility.
- Bounded key TTL, stale-key cleanup, and server/client clock policy.

### PostgreSQL

- Native `pgx` adapter for use cases requiring transactional coordination.
- Atomic statements, indexed schema, cleanup, lock-contention limits, and
  migration ownership through `migrations`.
- PostgreSQL MUST NOT be the default high-throughput backend without benchmark
  evidence supporting the workload.

## Integration

- HTTP middleware with standard limit, remaining, reset, and `Retry-After`
  behavior and safe proxy-aware key extraction.
- JSON-RPC middleware supporting global, principal, method, tenant, and custom
  operation policies.
- Queue middleware for rate-limited admission without changing durable retry
  semantics or acknowledging rejected work prematurely.
- `authentication` principal integration without a reverse dependency.
- `authorization` remains responsible for permission decisions.
- `service`, `queue`, `log`, and `telemetry` adapters.
- Outbound vendor policies remain in `http-client`, which MAY consume this
  package through a narrow adapter if semantics align.

## Distributed And Operational Semantics

- Policy revisions and rolling deployments MUST not silently double capacity.
- Multi-region behavior, replication lag, partitions, and consistency limits
  MUST be explicit.
- Backend outage strategy is selected, observable, and bounded per policy.
- High-cardinality and adversarial keys MUST not create unbounded backend state.
- Hot-key contention and fairness MUST be benchmarked.
- Administrative inspection can report policy and aggregate state but MUST NOT
  expose raw credentials, IPs, or tenant-sensitive keys by default.

## Security

- Trusted proxy configuration and client-IP extraction are strict and explicit.
- Keys are length-bounded, namespaced, versioned, and optionally irreversibly
  hashed before persistence or telemetry.
- No attacker-controlled value appears as an unbounded metric label.
- Integer overflow, clock rollback, replay, key collision, script injection,
  and bypass through alternate credential sources are threat-modeled.
- Rejection responses MUST not disclose protected identity or policy internals.

## Non-Goals

- No authorization, quota billing, usage ledger, API gateway, or WAF.
- No hidden retries or sleeping inside core admission decisions.
- No outbound pagination or `Retry-After` retry orchestration.
- No queue ownership, job uniqueness, scheduler overlap, or general locking.
- No Redis compatibility implemented through the Valkey adapter; native Redis
  MAY be added as an independent adapter with conformance proof.

## Package Shape

- Root: policies, requests, decisions, algorithms, errors, observations.
- `memory`, `valkey`, and `postgres`: native backend adapters.
- `ratelimithttp`, `ratelimitrpc`, and `ratelimitqueue`: integrations.
- `ratelimittest`: clocks, reference models, fixtures, and conformance suites.

## Testing And Quality Standard

Meaningful 100% production statement coverage is mandatory. Required evidence:

- mathematical property and reference-model tests for every algorithm
- cross-backend decision and precision conformance
- race and stress tests under hot-key and high-cardinality contention
- Valkey and PostgreSQL integration, failover, timeout, and fault injection
- clock rollback/jump, overflow, boundary, weighted-cost, and policy-revision
  tests
- hostile key and transport fuzzing
- mutation testing of allow/reject and fail-open/fail-closed branches
- benchmarks for throughput, latency percentiles, allocations, contention,
  cardinality, batch size, and backend round trips

## Documentation Deliverables

- Five-minute local and Valkey quickstarts.
- Complete algorithm, decision, backend, consistency, and failure API reference.
- Guides for HTTP, JSON-RPC, queues, principals, tenants, trusted proxies,
  weighted costs, deployment, policy revisions, and outage behavior.
- Laravel RateLimiter migration guide, operations runbook, security model,
  performance guide, FAQ, troubleshooting, examples, and changelog.
- Every user-facing scenario and exported API MUST be documented.

## Automation And Release

Use the latest stable Go release as the minimum at implementation time. Pin all
tools and dependencies. GitHub Actions MUST run formatting, vet, Staticcheck,
strict golangci-lint, advisory NilAway, tests, meaningful 100% coverage, race,
fuzz smoke, mutation checks, Valkey and PostgreSQL matrices, vulnerability
scans, benchmarks, docs, API compatibility, and releases. All blocking commands
MUST be reproducible locally through documented `make` targets.

## Execution Plan

1. Specify policies, arithmetic, decisions, keys, errors, and reference models.
2. Implement bounded memory algorithms and transport-neutral conformance.
3. Implement native Valkey and PostgreSQL adapters.
4. Implement HTTP, RPC, queue, identity, logging, and telemetry integrations.
5. Complete fault, security, race, mutation, and performance hardening.
6. Publish complete adoption documentation and release v1.

## Acceptance Criteria

- Decisions are deterministic and conform across supported backends.
- Distributed operations are atomic within their documented consistency model.
- Failure, cardinality, clocks, and resource growth are bounded and observable.
- Inbound, outbound, queue, authorization, and lease ownership remain distinct.
- Meaningful 100% coverage and every required CI gate pass.
