# Compatibility and residuals

[Documentation index](README.md)

Version 3 removed the legacy `ratelimithttp`, `ratelimitlog`,
`ratelimitprincipal`, `ratelimitqueue` and `ratelimittelemetry` packages; the
v2 line keeps them. The root `NewService` and direct backend methods remain
beside the strict APIs.

The remaining known residuals are:

1. The root-v1 pgx and OpenTelemetry dependency bundles remain intentional.
2. Generic `go-retry/retryadapter` remains a supported path.
3. `ratelimitrpc` naming and semantic redesign are deferred.
4. Released `go-retry/retryhttp` permissive validation and cause formatting
   remain outside this module and unchanged.
5. Public searches cannot prove that no external consumers exist.
6. Phase 4 resilience-stack composition ordering is deferred.
7. Legacy root-service typed-nil, cancellation, batch continuation and
   cancellation-text behavior remains supported and characterized.
8. Direct legacy backend methods do not classify post-dispatch unknown
   outcomes under the strict contract.
9. `ratelimittest.Reference` implements strict parity in this release; no
   reference-parity work is deferred.
10. Legacy `go-retry.Do` known-terminal/classifier-panic cause formatting,
    potentially unbounded disclosure, and direct/joined traversal remain
    beside the bounded `DoStrict` behavior and are not changed here.

The v3 module returns a `go-migrations/v4.Migration` from
`postgres.GoMigration` with the checksum v2 recorded. Callers must update
rate-limit and migrations import paths together and complete the
[ledger name upgrade](https://github.com/faustbrian/go-migrations/blob/v4.0.0/docs/operations.md#ledger-name-upgrade) before the first v4 run. No
`adapters/jsonrpc` package, nested module, or independent successor tag is
part of this release.

Continue with [Concepts and API](api.md).
