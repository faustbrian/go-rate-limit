# Application rate-limiting hardening audit

This report records the executable evidence for the release-blocking security,
correctness, availability, and resource properties of rate-limit. Evidence
is reproducible with `make check`; live backend checks require the environment
documented in the operations runbook.

## Decision truth tables

All algorithms use integer microseconds. A request whose cost is zero, exceeds
the policy maximum, or would require arithmetic beyond the exact distributed
range is rejected before state mutation.

### Fixed window

| Prior use plus cost | Decision | Remaining | Reset |
| --- | --- | ---: | --- |
| at most capacity | allow | capacity minus new use | window end |
| over capacity | reject | capacity minus prior use | window end |
| exactly at a new boundary | evaluate a fresh window | capacity minus cost | next window end |
| clock moves backward | evaluate at last observed time | never mints capacity | unchanged or later |

### Token bucket

| Available tokens after bounded refill | Decision | State change |
| --- | --- | --- |
| at least cost | allow | subtract cost |
| below cost | reject | retain refilled balance |
| frozen or backward clock | no refill | clamp to last observed time |
| long idle or large jump | refill | cap at capacity plus burst |

Fractional refill is rounded down. Retry-after is rounded up to the first
microsecond at which the complete weighted cost is available.

### Sliding window

| Effective current plus weighted previous use | Decision | State change |
| --- | --- | --- |
| at most capacity | allow | add cost to current segment |
| over capacity | reject | retain counters |
| one window elapsed | evaluate prior contribution | rotate once |
| two windows elapsed | fresh state | clear both segments |

The weighted previous contribution uses conservative integer rounding, so a
rounding difference cannot turn rejection into admission.

### Concurrency

| Lease condition | Decision | State change |
| --- | --- | --- |
| new ID and live cost fits | allow | store bounded lease |
| new ID would exceed capacity | reject | none |
| same ID, policy, key, and cost | allow idempotently | none |
| same ID with different cost or owner | error | none |
| expired ID | evaluate as new | replace only if admitted |

Reducing capacity below live leased cost clamps remaining to zero. Cleanup and
cardinality eviction preserve live leases, so pressure cannot reopen capacity.

## Cross-backend conformance and consistency

The shared reference harness compares allow, reason, limit, remaining, reset,
retry-after, and stable error class for memory, live Valkey, and live
PostgreSQL. It covers capacity exhaustion, burst, weighted costs, refill,
window boundaries, resets, Unix epoch, frozen time, rollback, large jumps,
long idle periods, exact-integer limits, and policy revisions.

| Property | Memory | Valkey | PostgreSQL |
| --- | --- | --- | --- |
| mutation atomicity | shard mutex | one single-slot Lua script | advisory transaction lock and row lock |
| clock precision | integer microseconds | integer microseconds | canonical integer microseconds |
| rollback handling | clamp | clamp in script | clamp in persisted transition |
| expiry | bounded sweep or eviction | key TTL | indexed bounded cleanup |
| lease retry | exact idempotency | exact idempotency | exact idempotency |
| rolling revision | conservative carry-forward | conservative carry-forward | conservative carry-forward |

These are three strong but differently scoped authorities. Memory is local to
one process. Valkey and PostgreSQL are strong only when all decisions use the
same authoritative deployment. Asynchronous replicas and independent regions
are never combined into an eventually consistent global allowance.

## Outage and retry matrix

The service performs no internal admission retry. An unknown result is exposed
as unavailable rather than repeated, which prevents duplicate admission.

| Fault | Evidence | Required result |
| --- | --- | --- |
| Valkey `NOSCRIPT` | live script-cache flush | reload and one atomic decision |
| Valkey connection loss | named connection killed live | first operation unavailable; later reconnect succeeds |
| Valkey timeout or cancellation | injected and live contexts | stable deadline or unavailable error, no leaked detail |
| Valkey bad or oversized state | hostile replies and lease hash | corrupt error before admission or unbounded field scan |
| PostgreSQL connection loss | backend PID terminated live | first operation unavailable; later pool reconnect succeeds |
| PostgreSQL deadlock | injected SQLSTATE `40P01` | unavailable after one call; no hidden retry |
| PostgreSQL transaction failure | begin, query, write, delete, commit injection | rollback or failed commit, never an allowed decision |
| contention and cancellation | live advisory locks and contexts | bounded by lock and request timeouts |
| replication lag | topology rule | replicas are not admission authorities |
| partition | transport classification | explicit failure-mode decision; no counter merge |
| rolling revision | shared all-algorithm suite | consumption and lease ownership retained |

Fail-open applies only to availability and deadline failures. Corruption and
overflow always fail closed. Concurrency policies prohibit fail-open entirely.

## Threat model

| Threat | Control and executable evidence |
| --- | --- |
| proxy spoofing | explicit trusted prefixes, bounded header and hop parsing |
| credential-source ambiguity | caller selects one principal extractor and tenant namespace |
| key collision or tenant crossover | versioned length-prefixed key derivation and digest tests |
| script injection | values are arguments; script and hash slot are library owned |
| raw key or credential disclosure | opaque backend keys and public-error redaction tests |
| fail-open abuse | explicit policy mode, typed observation, integrity failures excluded |
| cardinality denial of service | hard key, policy, lease, observer, batch, and cleanup bounds |
| hot-key denial of service | atomic bounded work, timeouts, race stress, benchmark ceilings |
| metric-label explosion | controlled policy/reason labels and bounded identifiers |
| queue loss or hot loop | typed deferral leaves acknowledgement and retry ownership to adapter |

Logs, metrics, traces, errors, and inspection surfaces are tested with sentinel
raw subjects, IPs, policy text, and backend detail. Only stable classifications
and bounded controlled labels may cross a public boundary.

## Resource budgets

The normative hard limits are maintained in the performance guide. In
addition to those input and state bounds, the package starts no background
goroutines, owns no retry loop, and delegates connection-pool limits to the
application. Valkey cleanup is TTL-based. PostgreSQL cleanup uses the
`expires_at` index, a maximum 10,000-row batch, and `SKIP LOCKED`; neither
backend requires a full application-level scan.

The blocking benchmark gate takes three 10,000-operation samples and enforces
latency, bytes, and allocations for hot-key, high-cardinality, and batch paths.
The longer sample amortizes parallel harness setup while preserving the
portable ceilings.

## Verification reports

`make check` is the release-equivalent local gate. It includes formatting,
module tidiness, vet, golangci-lint, Staticcheck, advisory NilAway, unit tests,
exact production coverage, race tests, fuzz targets, documentation and API
checks, vulnerability scanning, workflow validation, live integration tests,
blocking benchmarks, and mutation testing.

The mutation gate requires 100% mutation coverage for admission decisions and
package-specific efficacy floors. The July 17 complete run reported:

| Package | Killed | Lived | Uncovered | Timed out | Efficacy | Coverage |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| root admission boundaries | 119 | 0 | 0 | 1 | 100.00% | 100.00% |
| memory | 80 | 26 | 0 | 5 | 75.47% | 100.00% |
| postgres | 164 | 9 | 9 | 6 | 94.80% | 95.05% |
| ratelimithttp | 23 | 7 | 0 | 0 | 76.67% | 100.00% |
| ratelimitlog | 1 | 0 | 1 | 0 | 100.00% | 50.00% |
| ratelimitprincipal | 2 | 0 | 0 | 0 | 100.00% | 100.00% |
| ratelimitqueue | 15 | 0 | 0 | 0 | 100.00% | 100.00% |
| ratelimitrpc | 12 | 1 | 4 | 0 | 92.31% | 76.47% |
| ratelimittelemetry | 3 | 0 | 0 | 0 | 100.00% | 100.00% |
| valkey | 61 | 3 | 0 | 0 | 95.31% | 100.00% |

A fresh complete report is required from `make check` for each release
candidate; saved partial output is not release evidence.

The race and shared atomicity suites use 64 workers to exercise same-key exact
capacity, many-key independence, cleanup, shutdown, and exact lease replay.
Fuzz targets cover key construction, policy and request boundaries, persisted
state, proxy input, transport replies, and adapter metadata. Coverage is
blocked unless every production package reports exactly 100.0% statements.

Hosted CI is evidence for the exact pushed commit and remains distinct from a
local working tree. A release requires both a green local `make check` and all
blocking GitHub Actions checks green for the release commit. NilAway remains
visible and advisory; any reported finding must still be reviewed.
