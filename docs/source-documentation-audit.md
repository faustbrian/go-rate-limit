# Source documentation audit

[Documentation index](README.md)

## Owner, acceptance and selected proof

This Tier A pass owns source comments and directly contradicted maintained prose
from fresh remote main `104dc25eff4a78a8fdfba31aa37e4734a272e4f6`. It preserves
runtime AST/tokens/literals/directives, API signatures, dependencies, authority,
Lua/SQL payloads and fixtures. The original unrelated checkout is untouched;
the coordinator owns policy rollout, publication and exact-main CI.

Callers are transport-neutral policy/admission/lease users, strict services and
backend implementers, canonical HTTP/principal/queue/logging/telemetry adapters,
compatibility package consumers and deterministic assurance helpers. Named risks
are definitive versus unknown mutation outcomes, safe fail-open eligibility,
caller/backend clock and lease ownership, locking/cancellation, state expiry and
cardinality, backend transactions/script atomicity, key privacy, HTTP proxy trust,
borrowed callbacks/client cleanup and compatibility deviations.

Before source edits, `git ls-files '*.go' '**/go.mod'` inventories one root module,
137 Go files (58 source, 79 tests) and 16 directories. `golib source-docs inventory
--json` reports six malformed/missing name-leading exported/method comments;
package structure is initially covered but semantic adequacy is not certified.
All private/public source, tests/helpers/fuzz/bench/examples and assurance code
are in scope. Missing comments and source contracts needing investigation remain
separate: strict declarations, dispatch ambiguity, fail-open, store time/locking,
leases, client/resource ownership and intentionally different legacy surfaces.

The native `.golib.yaml` API mode is `go-doc` for all 16 packages at
`api/baseline.txt`; use the existing producer and inspect full renders rather
than edit the snapshot by hand. The native docs target validates required files
and renders each build-selected package. Root Examples require a focused run
when approved. Lua string constants are executable artifacts, not prose, and
must remain byte-identical. No generator execution is authorized by structure.

After an explicit serialized Go 1.27.2 grant: three task-owned caches, native
formatting/all-file token+AST+directive proof, full 16 package GoDocs/native API
update/check and selected docs/Examples. Current structural/localdocs/offline
spec applicability, pinned full/changed/staged secret scans, frozen-input
comparison and one ordinary full-diff truth/usefulness inspection finish the
local pass. No live PostgreSQL/Valkey, race/fuzz/mutation, peer/performance or
release qualification campaign is selected for unchanged executable behavior.

## Inventory and source investigation

The inventory has no tracked generated Go, nested module, platform/tag variant,
vendor directory or local generation directive. PostgreSQL migrations and Valkey
Lua are hand-maintained executable inputs; their byte strings remain unchanged.
The configured API snapshot is the sole generated artifact updated by this pass.
Historical `.verification` evidence and released changelog entries are frozen.
No specification decisions or official conformance corpus are configured.

| Source boundary | Contracts traced and documentation disposition |
| --- | --- |
| Root policy/key/request/decision | Zero-value validation, exact integer/microsecond ranges, immutable values, consistency declaration versus capability, deterministic domain-separated hashing and non-secret labels; hashes are not encryption. |
| Service and batches | Compatibility normalization can fail open without mutation certainty; synchronous fan-out, borrowed observers, panic isolation only for observers, no shutdown or retries, per-item effects without rollback. |
| Strict service/outcomes/leases | Direct identity/type trust rather than arbitrary error traversal, complete known result wins cancellation race, unknown fails closed, attempted indexes/defensive error slices, expiry is accounting not a side-effect fence; empty backend lease identity remains accepted. |
| memory | Per-shard quotas, active-lease eviction protection versus discarded counters, rationalized integer rounding, revision carry/period validation, pre-lock legacy cancellation versus strict interruptible waits, explicit sweeping and non-joining Close. |
| postgres | Borrowed pool, table-only startup check, caller-bounded checks/cleanup, advisory prefix collisions serialize but full digest identifies rows, transaction-ownership rollback deadline, unknown commit/rollback, strict bounded JSON and version-1 wire metadata. |
| valkey | Borrowed native client, startup Valkey/noeviction observation, one slot/key and separate strict script identities, server-selected effective time, post-dispatch ambiguity, saturated TTL and script limits; atomic Lua execution is not rollback on script error. |
| Canonical authentication/HTTP/queue | Nil/typed-nil checks, once-per-call ordered callbacks, caller identity/proxy trust, bounded selected key not every message string, no acknowledgement/retries, handler/context lifetime and propagated operational panics. |
| Canonical slog/OpenTelemetry | Synchronous background-context sink calls, borrowed logger/instruments, zero/nil no-op observers, provider panics not contained, direct labels unvalidated and no export/shutdown ownership. |
| Five deprecated packages | Deprecation markers preserved; legacy principal reads Subject twice, unchecked handler/receiver behavior and distinct telemetry scope/attributes retained. Strict helpers reject typed nil but do not recover collaborator panics. |
| JSON-RPC | Ordered rule consumption not transactionality, shallow ID/result ownership, caller-verified identity, synchronous borrowed handlers and generic admission responses. |
| ratelimittest | Mutex-backed mutable clock and unbounded rational reference map, shared sliding-window design, fresh-fixture/cleanup ownership and 64-worker joined tests without independent call deadlines; selected characterization is not general conformance. |

All 79 test files were inventoried by scenario and read at their relevant contract
assertions: core validation/outcome matrices, observer/callback lifecycle,
compatibility identity, context-lock cancellation, strict temporal/state and
revision boundaries, rollback ownership, native protocol replies and corruption,
telemetry view/collection compatibility, and optional backend integration. Five
fuzz targets cover key construction, two proxy paths, PostgreSQL decoding and
Valkey replies. Three benchmarks cover batch sizes, hot keys and cardinality.
One output-bearing root Example exercises the strict memory admission path.
These existing tests inform prose; unchanged broad campaigns are not rerun.

Directly contradicted current prose was corrected for hashing privacy, schema
checks, panic-safe wording, memory Close/eviction, rollback-budget start and
lease retries after expiry. Input time canonicalization is not certification
of every accepted timestamp or segment-boundary representation: memory/reference
use Unix nanoseconds in window calculations and distributed state uses
microseconds. This source-only limitation is recorded without runtime repair or
rewriting desired compatibility expectations. Deployment consistency, identity
authentication, callback deadlines and post-expiry application fencing remain
caller responsibilities, not undocumented guarantees.

## Local proof and limitations

Go 1.27.2 was used exclusively in one serialized, task-owned three-cache session.
Native formatting and an independent stdlib scanner/parser comparison passed
all 137 runtime ASTs/signatures/literals and raw tokens. Exactly one
grammar-optional final-interface implicit semicolon at
`adapters/authentication/authentication.go:13` is added by multiline Subject
documentation; only that delimiter was normalized after independent AST equality.
All other raw tokens, directives, analyzer annotations and example output markers
remain exact. All 540 comment groups parsed/rendered as HTML.

All 16 packages rendered with `go doc -all`; the native `golib api update`
producer regenerated the configured full-prose `api/baseline.txt`. `golib api
check` passed, and repeated producer output was byte-stable.
`make -f verification/package.mk docs` passed its required-file/package-render
checks. `go test -run '^Example' -count=1 -timeout=2m .` passed the one
output-bearing strict-memory Example; compiling other root test sources is not
execution of their scenarios. Final root label/duration wording was followed by
affected root render, all-file identity and stable API refresh; unchanged
Example/docs/other package render proof was reused. All exact caches, helper and
render outputs were removed and absence verified before releasing the Go slot.

`golib source-docs check` has zero findings (six initial method comments resolved),
`golib docs check --local` validates local documentation, and `golib specification
check` reports no configured module decisions. Frozen dependency, fixture,
compatibility evidence and Lua/SQL runtime inputs remain unchanged. Pinned
changed/current/baseline/staged secret checks and final ordinary complete-diff
inspection are completed before the local commit; details are in the local
handoff rather than a second evidence framework. No service, race, active fuzz,
mutation or performance qualification is claimed; remote delivery and exact-main
CI remain coordinator-owned.

## Objective Enforcement

The checksum-pinned public Tools v2.4.0 verifier now supplies
`make source-docs-check` as a prerequisite of the existing local and CI Make
routes. Ordinary shared CI selects strict source-documentation checking and
requires its result. Native prerequisites, operation budgets, security and
release controls are retained. Hosted exact-head evidence is recorded by the
coordination ledger; configuration alone does not establish green delivery.

Continue with [Concepts and API](api.md).
