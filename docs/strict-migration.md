# Strict API and adapter migration

[Documentation index](README.md)

The current major is v3. The adapters are packages in
`github.com/faustbrian/go-rate-limit/v3`; they have no independent module, tag,
or release version.

```sh
go get github.com/faustbrian/go-rate-limit/v3@latest
```

Import the root API and selected successor packages from that same module:

```go
import (
	ratelimit "github.com/faustbrian/go-rate-limit/v3"
	ratelimitauthentication "github.com/faustbrian/go-rate-limit/v3/adapters/authentication"
	ratelimithttp "github.com/faustbrian/go-rate-limit/v3/adapters/http"
	ratelimitotel "github.com/faustbrian/go-rate-limit/v3/adapters/otel"
	ratelimitqueue "github.com/faustbrian/go-rate-limit/v3/adapters/queue"
	ratelimitslog "github.com/faustbrian/go-rate-limit/v3/adapters/slog"
)
```

## Package paths

| Removed path | Successor path |
| --- | --- |
| `ratelimithttp` | `adapters/http` |
| `ratelimitlog` | `adapters/slog` |
| `ratelimitprincipal` | `adapters/authentication` |
| `ratelimitqueue` | `adapters/queue` |
| `ratelimittelemetry` | `adapters/otel` |

Version 3 removed the legacy paths before their announced support window
ended; published v2 releases keep them and no further v2 releases are planned.
The HTTP and queue adapters take a `*ratelimit.StrictService` built from a
`StrictBackend` and return a `*Middleware` whose `Wrap` reports an error,
instead of taking a `*ratelimit.Service` and returning a plain wrapper;
`ratelimittelemetry.NewStrict` becomes `ratelimitotel.New`. Successors own their named Go types, so reflection identity and type
assertions change at migration. The OTel instrumentation scope follows the
module major and is now `github.com/faustbrian/go-rate-limit/v3/adapters/otel`;
update dashboards and views that select an earlier scope. `adapters/otel` also
records the `rate_limit.error.kind` attribute, which `ratelimittelemetry` did
not, so series split by error kind; update attribute allowlists and
exact-series queries. `adapters/slog` reports `outcome_unknown`, `canceled`,
`unsupported` and `invalid` as their own `error_kind` values, where
`ratelimitlog` logged them as `internal`, and classifies a deadline before a
rejection; update queries and alerts that match `error_kind="internal"`.
`ratelimitauthentication.Key(nil)` fails with `ErrInvalidPolicy`, where
`ratelimitprincipal.Key(nil)` failed with `ErrInvalidKey`. The PostgreSQL
`GoMigration` returns a `go-migrations/v4` migration with the same checksum;
complete the [ledger name upgrade](https://github.com/faustbrian/go-migrations/blob/v4.0.0/docs/operations.md#ledger-name-upgrade) before the first v4 run.

`ratelimitrpc` remains supported and is not deprecated. A JSON-RPC successor
and its protocol/type redesign are deferred. `memory` also keeps its current
path.

## Service migration

Construct `StrictService` with a `StrictBackend` and use successor HTTP and
queue middleware. A strict backend name is retained once and must contain 1 to
64 lowercase ASCII bytes matching
`^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$`. This name stamps decisions, leases, logs,
and metric attributes; changing it changes telemetry series and lease
ownership.

Strict backend errors are either direct stable sentinels or values built with
`NewBackendError`. Only direct, method-applicable safe categories are known.
After invocation, every indirect, wrapped, joined, arbitrary, contradictory,
or method-inapplicable error/result becomes a fresh fail-closed
`ErrOutcomeUnknown`; it never inherits `ErrUnavailable` fail-open behavior.
Strict wrappers intentionally discard backend and cancellation-cause text.
Always check `ErrOutcomeUnknown`, then `ErrCanceled`, then `ErrDeadline`.
Normalized backend error text is bounded by `MaxBackendErrorBytes`; a complete
batch error is bounded by `MaxStrictBatchErrorBytes`.

The service accepts complete backend results as follows:

| Operation | Nil error | `ErrRejected` | Other applicable known category | Unknown or invalid |
| --- | --- | --- | --- | --- |
| Admit | complete allowed decision | complete limited decision | zero raw decision; service applies policy | zero raw data returned as stamped outcome-unknown denial |
| Acquire | complete allowed decision and matching lease | complete limited decision and zero lease | zero lease and decision; always fail closed | zero lease and stamped outcome-unknown denial |
| Release | success | not applicable | only unavailable, corrupt, lease-not-found, or lease-not-owned | outcome unknown |

A complete allowed decision has `Allowed=true`, `ReasonAllowed`, the exact
policy limit, remaining no greater than `limit-cost`, a nonzero reset, and zero
retry delay. A complete rejection has `Allowed=false`, `ReasonLimited`, the
exact limit, remaining below cost, a nonzero reset, and a nonnegative retry
delay. A successful lease repeats the request ID, key, policy ID, revision, and
cost, has a nonzero expiry equal to the decision reset, and is stamped with the
retained backend. Every other known non-rejection category requires entirely
zero raw result values.

The exact method applicability is:

| Category | Admit | Acquire | Release |
| --- | ---: | ---: | ---: |
| `ErrRejected` | yes | yes | no |
| `ErrUnavailable` | yes | yes | yes |
| `ErrOverflow` | yes | yes | no |
| `ErrCorrupt` | yes | yes | yes |
| `ErrUnsupported` | yes | no after dispatch | no after dispatch |
| `ErrLeaseNotFound` | no | no | yes |
| `ErrLeaseNotOwned` | no | yes | yes |

Observers run exactly once after normalization and stamping for Admit and
Acquire; Release has no observer. HTTP maps strict unknown outcomes to the
bounded 503 response and does not call the handler. Queue returns the same
classifiable error and neither calls nor acknowledges downstream work. Batch
records only dispatched indexes in `Attempted`, keeps zero decisions for
unattempted indexes, and reports ordered `BatchItemError` values.

The service validates shapes but is clock-authority neutral: it does not
compare reset or expiry with caller time. Memory and the reference model check
their clamped per-key time. PostgreSQL and Valkey check the exact clamped
client time or the authoritative server time obtained inside the locked
transaction or Lua script. A reset before that effective time is invalid; a
successful lease expiry must be later than it and equal the decision reset.

## Construction and ownership

Strict APIs reject typed-nil required interfaces before invocation. The
authentication adapter's `ratelimitauthentication.Key` snapshots `Subject`
once and keys on that value, where the removed `ratelimitprincipal.Key` read it
twice and keyed on the second read, and `ratelimitotel.New` rejects a typed-nil provider. The HTTP and queue middleware expose
checked `Wrap` methods that reject nil and typed-nil handlers before calling
them. Optional callbacks retain their documented defaults.

PostgreSQL pools and Valkey clients are borrowed. Callers keep them valid for
all store calls and remain solely responsible for closing them. `NewStrict`
performs validation and retention only. `OpenStrict` performs exactly one
caller-bounded check and returns no store on failure without closing the
collaborator. PostgreSQL non-commit exits synchronously roll back with
an absolute deadline fixed immediately after transaction acquisition. The
rollback uses the remaining bounded budget on a context detached from caller
cancellation; no other operation detaches cancellation. Rollback failure or
timeout makes the outcome unknown.

The service, middleware, and adapter callback ordering, retention,
concurrency, blocking, re-entry, panic, and request/result ownership contracts
are defined in [API contracts](api.md#callback-concurrency-and-ownership).

## Compatibility and rollback

Legacy root constructors, services, backend methods, batch continuation,
cancellation-to-deadline classification, fail-open decisions, observer fields,
and typed-nil behavior remain unchanged. Direct legacy backend methods cannot
promise strict unknown-outcome classification. These are compatibility
surfaces, not strict guarantees. Version 3 removed the legacy handler-wrapper,
principal, logging, queue and telemetry packages.

Within v3, callers of the root service and `ratelimitrpc` can roll back by
restoring `NewService`; no persisted schema downgrade is required. The HTTP and
queue adapters accept only `*ratelimit.StrictService` and have no non-strict
path in v3; reverting them means returning to v2. Do not run v2 binaries after the go-migrations
[ledger name upgrade](https://github.com/faustbrian/go-migrations/blob/v4.0.0/docs/operations.md#ledger-name-upgrade): they read the previous ledger table. The
upgrade has no documented rollback: returning to v2 requires stopping every
go-migrations v4 runner and restoring the previous ledger table name with its rows, constraints,
indexes and grants before any v2 binary runs. Do not interpret the rollback as reconciliation
of an unknown mutation. Resolve each unknown PostgreSQL or Valkey operation
before retrying or reverting behavior.

The companion `go-retry` remediation keeps generic `retryadapter` supported.
Its legacy `retryhttp` validation and cause formatting, classifier-panic
behavior, and legacy `Do` cause disclosure remain compatibility exceptions;
new bounded known-terminal behavior belongs to `DoStrict`. They are not
rate-limit APIs and are not changed by this module.
