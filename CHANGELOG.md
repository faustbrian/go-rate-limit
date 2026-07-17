# Changelog

All notable changes follow Keep a Changelog. The project uses semantic
versioning after v1.0.0.

## Unreleased

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
