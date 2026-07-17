# Contributing

Use Go 1.26.5 or newer in the 1.26 line. Keep core APIs transport neutral and
do not add authorization, billing, outbound retry, queue ownership, or hidden
sleeping.

Behavior changes follow red-green-refactor and require reference/conformance
updates where applicable. Run disposable Valkey 9 and PostgreSQL locally, set
VALKEY_ADDRESS and POSTGRES_URL, then run:

    make check
    make mutation

Commits use Conventional Commits with a body. Do not include generated
coverage profiles or credentials.
