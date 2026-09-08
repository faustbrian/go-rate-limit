# Changelog

All notable changes follow Keep a Changelog. The project uses semantic
versioning after v1.0.0.

## Unreleased

## 1.1.0 - 2026-09-07

### Added

- Add strict admission, batch, lease, cancellation, deadline, and
  outcome-unknown contracts with strict memory, PostgreSQL, Valkey, and
  reference backends.
- Add checked HTTP and queue middleware plus successor authentication, slog,
  and OpenTelemetry adapters under `adapters/`.

### Deprecated

- Prefer `adapters/http`, `adapters/queue`, `adapters/slog`,
  `adapters/otel`, and `adapters/authentication` over `ratelimithttp`,
  `ratelimitqueue`, `ratelimitlog`, `ratelimittelemetry`, and
  `ratelimitprincipal`. The legacy package paths remain supported through the
  documented compatibility interval.

### Changed

- Direct new callers to strict constructors and successor adapter paths while
  preserving released v1 APIs and compatibility behavior.
- Saturate strict Valkey state TTLs for valid very-long policy
  periods instead of allowing duration overflow to collapse retention to one
  second.
- Make strict PostgreSQL state handling fail closed with `ErrCorrupt` for malformed
  sliding-window shape, out-of-range timestamps, remainders, token or lease
  costs, overflowing sliding usage, and state documents exceeding 128 KiB;
  oversized documents are rejected in SQL before their bytes cross the driver
  boundary.
- Reject strict transitions whose rollback-clamped effective time exceeds the
  exact backend range, and reject impossible same-revision fixed- or
  sliding-window usage or active sliding segment positions as corrupt state in
  PostgreSQL and Valkey. Same-period revisions retain full carried window
  consumption across capacity decreases and later increases; strict backends
  reject same-ID window-period changes instead of reinterpreting stored
  indexes. Valkey fixed windows derive their boundary from the clamped time.
- Preserve released reference and Valkey revision transitions by isolating
  strict revision and corruption validation from legacy mutation paths.
- Bound forwarded header bytes and hops while preserving the released
  `http.Header.Get` first-value behavior for repeated `X-Forwarded-For` lines.

- Replace copied repository tooling with the pinned `go-library-tools` v1.0.5
  contract while retaining package-owned policy and verification evidence.
- Replace copied repository tooling with the checksum-pinned
  `go-library-tools` v1.0.13 contract while retaining package-owned policy and
  verification evidence.
- Adopt the checksum-verified `go-library-tools` v1.4.0 CLI, schema-v2 cohesion
  metadata, repository-local cohesion gate, and immutable W14-enforcement
  workflow while retaining package-owned source and evidence.
- Reconcile the `go-migrations` dependency with the checksum published by the
  Go checksum database for its immutable v1.0.0 commit.

### Documentation

- Replace archived monorepo links and completed execution artifacts with a
  standalone, human-oriented documentation structure.
- Link the module to the immutable v1.4.0 Golib ecosystem index and resilience
  family guidance.

## 1.0.0 - 2026-08-25

### Changed

- Exclude intentional nested modules from root local-proxy archives so local,
  bootstrap, CI, and public module checksums describe the same source
  boundary.

- Track the pinned documentation-tool lockfile so clean CI checkouts install
  the exact validated cspell dependency.

- Reconcile standalone dependency checksums against deterministic current
  module archives so CI, local verification, and release consumers resolve
  identical content.

- Harden standalone documentation validation with deterministic spelling and
  link checks, package-specific documentation gates, and repository-local
  contributor guidance.

### Documentation

- Link the package README to package-owned documentation.

### Changed

- Publish the module from its standalone `github.com/faustbrian/go-rate-limit` identity while preserving its documented API and behavior.
- Refresh local `v0.0.0` owned-module checksums after dependency manifests and
  release notes were normalized; runtime behavior and public APIs are
  unchanged.
- Isolate live Valkey verification from interrupted mutation subprocesses by
  re-establishing the required `noeviction` policy before each process uses the
  disposable backend.
- Strengthen exact mutation boundaries across core validation, in-memory and
  distributed backends, and transport adapters without changing accepted
  inputs or admission semantics.
- Replace package-specific mutation floors and accepted timeouts with the
  canonical exact-100 repository runner.
- Require owned sibling modules at local `v0.0.0`; clean external consumers
  pin each module to an exact main pseudo-version.

- Clarify that applications may use distributed concurrency leases around
  complete outbound provider operations while `http-client` continues to own
  request pacing, retries, and transport policy.
- Use a deterministic execution budget for default fuzz smoke campaigns while
  retaining explicit duration overrides for extended fuzzing.
- Normalized standalone module metadata against the canonical owned dependency
  graph, including complete checksums for clean consumer resolution.
- Increase blocking benchmark samples from 100 to 10,000 operations so
  parallel harness setup is amortized before strict allocation checks.

### Added

- Immutable policies, bounded keys, typed decisions, stable errors, batch
  admission, observations, and guaranteed concurrency leases.
- Bounded memory, native Valkey 9, and native PostgreSQL implementations.
- HTTP, JSON-RPC, queue, principal, slog, and OpenTelemetry adapters.
- Reference models, cross-backend conformance, live fault tests, fuzzing,
  benchmarks, exact production coverage, documentation, and release workflows.

### Security

- Preserve live memory-backend leases under cardinality pressure instead of
  evicting them and reopening concurrency capacity.
- Bound policy IDs and revisions to telemetry-safe 64-byte identifiers so
  backend state and observations cannot carry arbitrary oversized labels.
- Reject arithmetic outside Valkey's exact integer range and bound each
  concurrency key to 1,024 live leases across every backend.
- Distinguish Valkey server-clock selection from Unix epoch timestamps and use
  fixed decimal encoding for large script values.
- Canonicalize PostgreSQL client time to the documented microsecond precision
  so reset metadata matches memory and Valkey exactly.
- Redact backend and driver details from public errors while preserving stable
  `errors.Is` classifications.
- Enforce hard bounds for observers, memory keys and shards, Valkey prefixes,
  trusted proxies, and PostgreSQL cleanup batches.
- Add blocking latency and allocation budgets for hot-key, cardinality, and
  batch benchmarks.
- Keep NilAway visible in local and hosted checks while treating findings as
  advisory rather than release-blocking.
- Prevent concurrency-capacity underflow during rolling policy reductions and
  reject oversized Valkey lease hashes before scanning their fields.
- Reject LeaseID retries whose weighted cost differs from the stored lease,
  while preserving exact old-revision retries during rolling deployments.
- Prevent fail-open policies from admitting on state corruption or arithmetic
  overflow; only availability and deadline failures may fail open.
