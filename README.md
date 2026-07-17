# go-rate-limit

Transport-neutral inbound rate limiting for Go applications. The package
provides deterministic token-bucket, fixed-window, bounded sliding-counter,
and concurrency-lease policies with memory, native Valkey, and native
PostgreSQL backends.

This library owns inbound request, RPC, queue-admission, and application
operation limits. It does not own authorization, billing quotas, WAF rules,
queue acknowledgement, or outbound vendor throttling. Outbound policy remains
in go-http-client.

## Five-minute memory quickstart

    policy, _ := ratelimit.NewPolicy(ratelimit.PolicySpec{
        ID: "login",
        Revision: "v1",
        Algorithm: ratelimit.TokenBucket,
        Capacity: 10,
        Burst: 2,
        Period: time.Minute,
        MaxCost: 2,
        FailureMode: ratelimit.FailClosed,
    })
    key, _ := ratelimit.NewKey(ratelimit.KeySpec{
        Namespace: "http",
        Version: "v1",
        Subject: ratelimit.Subject{Kind: "principal", Value: "user-42"},
        Hash: true,
    })
    backend, _ := memory.New(memory.Options{MaxKeys: 100_000, Shards: 64})
    service, _ := ratelimit.NewService(backend)
    decision, err := service.Admit(ctx, ratelimit.Request{
        Policy: policy,
        Key: key,
        Cost: 1,
        Now: time.Now().UTC(),
    })

Memory is bounded and process-local. It must not be presented as a
cluster-wide limit.

## Five-minute Valkey quickstart

    client, _ := valkeygo.NewClient(valkeygo.ClientOption{
        InitAddress: []string{"127.0.0.1:6379"},
    })
    defer client.Close()
    backend, _ := valkey.Open(ctx, client, valkey.Options{
        Prefix: "my-service-rate-limit",
        Timeout: 100 * time.Millisecond,
        Clock: valkey.ServerClock,
    })
    service, _ := ratelimit.NewService(backend)

Valkey 9 or newer with maxmemory-policy=noeviction is required. Each state key
uses an opaque SHA-256 hash tag, all mutation is atomic Lua, scripts recover
from NOSCRIPT through valkey-go, and state has a bounded TTL.

## Packages

- Root: policy, request, decision, error, batch, service, and lease contracts.
- memory: bounded sharded process-local backend.
- valkey: native valkey-go scripts and cluster-safe keys.
- postgres: native pgx transactions, cleanup, and go-migrations ownership.
- ratelimithttp, ratelimitrpc, ratelimitqueue: inbound adapters.
- ratelimitprincipal: narrow go-authentication-compatible principal adapter.
- ratelimitlog and ratelimittelemetry: slog and OpenTelemetry observations.
- ratelimittest: deterministic clocks, reference models, and conformance.

## Documentation

Start at [docs/README.md](docs/README.md). The API and adoption guides cover
algorithms, consistency, failure behavior, transports, migrations, operations,
security, performance, and troubleshooting.

## Local verification

    make unit
    make check

Exact production coverage and live integration need disposable services:

    VALKEY_ADDRESS=127.0.0.1:6379 \
    POSTGRES_URL='postgres://postgres:postgres@127.0.0.1:5432/rate_limit?sslmode=disable' \
    make check

No core admission call sleeps or retries. See [CONTRIBUTING.md](CONTRIBUTING.md)
for the full local gate stack.

## License

MIT. See [LICENSE](LICENSE).
