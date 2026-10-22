# Contributing

## Before Editing

1. Read [`AGENTS.md`](AGENTS.md) and the affected package's docs.
2. Run `go test ./...` or the affected package's narrower tests.
3. Identify owned dependencies and reverse dependants in `modules.json`.
4. Preserve unrelated work and generated/corpus provenance.

## Changes

Keep commits focused and conventional. Update every affected changelog with
the behavior and migration impact. Public API changes require compatibility
evidence and documentation. Specification behavior requires a decision record,
fixture coverage, and interoperability evidence.

New direct dependencies and dependency updates must follow the
[dependency governance policy](AGENTS.md#dependencies-and-supply-chain). Package-local
update bots are forbidden; the root policy owns every module and action update.

Specification-backed changes must follow the
[specification governance contract](AGENTS.md#design), update
the affected stable decision entries, and complete the Specification Decisions
section of the pull request template. An unresolved interpretation or stale
source pin is release-blocking; peer behavior cannot silently select policy.

Mutation is optional local or scheduled assurance, not a required PR status.
Confirmed behavioral and security defects still require a focused regression
and repair; equivalent/invalid mutants retain reviewed narrow dispositions.

Do not add package-local workflows, permanent replacements, machine-specific
paths, bypass flags, broad mutation exclusions, or aggregate quality metrics
that hide a failing package.

## Verification

Ordinary `go test ./...` runs deterministic tests, fuzz seeds and saved corpus
regressions without sibling checkouts or ecosystem ledgers. Live PostgreSQL and
Valkey tests skip when their environment variables are absent; that is not live
backend qualification. Never point those tests at a shared or production service.

With the checksum-pinned public verifier selected by `.golib.yaml`, use:

```bash
make inventory
make check
make ci
```

`check` and `ci` use bounded local checks. The named race selection runs the
memory backend's cancellation/lock-ownership and no-state-mutation assertions
once with a two-minute timeout. The manifest requires PostgreSQL and Valkey, so
the verifier's service-aware Make route differs from standalone Go tests; it
may require Docker. Ordinary CI consumes the public binary, not a source build.
Documentation changes need relevant structure, links and examples. Public API,
persistence and concurrency changes retain affected compatibility, race, live
integration and direct-consumer checks.

Select one target for short active fuzz exploration. Optional local mutation
selects one package; neither command is an all-change requirement:

```bash
GOMAXPROCS=2 go test -run '^$' -fuzz '^FuzzNewKeyNeverLeaksHashedSubject$' -fuzztime=2s -parallel=1 -timeout=2m .
golib mutation --module . --package ./memory
```

For relevant backend changes, provision disposable Valkey 9 and PostgreSQL,
then run the existing live integration boundary:

```bash
VALKEY_ADDRESS=127.0.0.1:6379 \
POSTGRES_URL='postgres://postgres:postgres@127.0.0.1:5432/rate_limit?sslmode=disable' \
make -f verification/package.mk integration
```

`make check-full` retains the complete scheduled/full assurance contract,
including all five registered fuzz budgets, live integration and benchmark
thresholds. Release qualification remains `golib release dry-run --module .`.
Select full and release checks for their named risk or release boundary, not
every commit. Report unavailable, skipped and failed checks honestly; a bounded
local pass is not live-backend, hosted-CI or public-release qualification.

## Adding A Module

Follow [repository structure policy](AGENTS.md#repository-structure). New modules
require an explicit purpose, ownership boundary, dependency review, package
catalog entry, full quality gates, documentation, changelog, license, security
policy, compatibility plan, and release dry-run.

## Source Documentation

Install the checksum-pinned public verifier declared in `.golib.yaml`.
`make source-docs-check` validates objective Go comment, marker, link and
generated-producer structure; `make check`, `make check-full` and `make ci`
require it before their existing checks. Shared CI runs the same strict
contract and includes its result in Required. This is structural enforcement,
not a substitute for source-truth review or behavioral verification.
