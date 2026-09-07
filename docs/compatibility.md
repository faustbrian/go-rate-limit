# Compatibility and residuals

The strict APIs and successor adapters are additive. The legacy packages stay
supported for the longer of 180 days after the replacements are publicly
consumable or two published stable minor releases containing both old and new
paths. Removal requires a separately reviewed major-version decision and
consumer evidence.

The remaining known residuals are:

1. The root-v1 pgx and OpenTelemetry dependency bundles remain intentional.
2. Generic `go-retry/retryadapter` remains a supported path.
3. `ratelimitrpc` naming and semantic redesign are deferred.
4. Released `go-retry/retryhttp` permissive validation and cause formatting
   remain outside this module and unchanged.
5. Public searches cannot prove that no external consumers exist.
6. Phase 4 resilience-stack composition ordering is deferred.
7. Legacy typed-nil, cancellation, batch continuation, handler-wrapping, and
   cancellation-text behavior remains supported and characterized.
8. Direct legacy backend methods do not classify post-dispatch unknown
   outcomes under the strict contract.
9. `ratelimittest.Reference` implements strict parity in this release; no
   reference-parity work is deferred.
10. Legacy `go-retry.Do` known-terminal/classifier-panic cause formatting,
    potentially unbounded disclosure, and direct/joined traversal remain
    beside the bounded `DoStrict` behavior and are not changed here.

No `adapters/jsonrpc` package, nested module, independent successor tag, or
immediate `/v2` module is part of this release.
