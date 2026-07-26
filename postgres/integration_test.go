package postgres_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
	"github.com/faustbrian/golib/pkg/rate-limit/postgres"
	"github.com/faustbrian/golib/pkg/rate-limit/ratelimittest"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgreSQLAdmissionLeaseAndCleanup(t *testing.T) {
	dsn := os.Getenv("POSTGRES_URL")
	if dsn == "" {
		t.Skip("POSTGRES_URL is required for live PostgreSQL tests")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	migration := postgres.SchemaMigration()
	_, _ = pool.Exec(ctx, migration.Down)
	if _, err := pool.Exec(ctx, migration.Up); err != nil {
		t.Fatalf("migration error = %v", err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), migration.Down) })
	store, err := postgres.Open(ctx, pool, postgres.Options{
		Timeout: time.Second, LockTimeout: 250 * time.Millisecond,
		Clock: postgres.ClientClock,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := postgresIntegrationRequest(t, ratelimit.FixedWindow, "fixed", 2, time.Second)
	if decision, err := store.Admit(ctx, request); err != nil || !decision.Allowed {
		t.Fatalf("first Admit() = %+v, %v", decision, err)
	}
	request.Cost = 2
	if decision, err := store.Admit(ctx, request); !errors.Is(err, ratelimit.ErrRejected) ||
		decision.Remaining != 1 {
		t.Fatalf("rejected Admit() = %+v, %v", decision, err)
	}
	leaseRequest := postgresIntegrationLeaseRequest(t)
	lease, decision, err := store.Acquire(ctx, leaseRequest)
	if err != nil || !decision.Allowed {
		t.Fatalf("Acquire() = %+v, %+v, %v", lease, decision, err)
	}
	if err := store.Release(ctx, lease); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if count, err := store.Cleanup(ctx, 10); err != nil || count < 1 {
		t.Fatalf("Cleanup() = %d, %v", count, err)
	}
	reconnectConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	reconnectConfig.MaxConns = 1
	reconnectPool, err := pgxpool.NewWithConfig(ctx, reconnectConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reconnectPool.Close)
	reconnectStore, err := postgres.New(reconnectPool, postgres.Options{
		Timeout: time.Second, LockTimeout: 250 * time.Millisecond,
		Clock: postgres.ClientClock,
	})
	if err != nil {
		t.Fatal(err)
	}
	reconnect := postgresIntegrationRequest(t, ratelimit.FixedWindow, "reconnect", 2, time.Second)
	if decision, err := reconnectStore.Admit(ctx, reconnect); err != nil ||
		!decision.Allowed || decision.Remaining != 1 {
		t.Fatalf("pre-reconnect Admit() = %+v, %v", decision, err)
	}
	var backendPID int32
	if err := reconnectPool.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&backendPID); err != nil {
		t.Fatalf("backend PID error = %v", err)
	}
	if _, err := pool.Exec(ctx, "SELECT pg_terminate_backend($1)", backendPID); err != nil {
		t.Fatalf("terminate backend error = %v", err)
	}
	if decision, err := reconnectStore.Admit(ctx, reconnect); !errors.Is(err, ratelimit.ErrUnavailable) || decision.Allowed {
		t.Fatalf("disconnected Admit() = %+v, %v", decision, err)
	}
	if decision, err := reconnectStore.Admit(ctx, reconnect); err != nil ||
		!decision.Allowed || decision.Remaining != 0 {
		t.Fatalf("reconnected Admit() = %+v, %v", decision, err)
	}
	ratelimittest.RunBackendConformance(t, func(t testing.TB) ratelimittest.BackendFixture {
		t.Helper()
		conformance, err := postgres.New(pool, postgres.Options{
			Timeout: time.Second, LockTimeout: 250 * time.Millisecond,
			Clock: postgres.ClientClock,
		})
		if err != nil {
			t.Fatal(err)
		}
		return ratelimittest.BackendFixture{Backend: conformance, Leases: conformance}
	})
	ratelimittest.RunBackendAtomicity(t, func(t testing.TB) ratelimittest.BackendFixture {
		t.Helper()
		conformance, err := postgres.New(pool, postgres.Options{
			Timeout: 5 * time.Second, LockTimeout: 5 * time.Second,
			Clock: postgres.ClientClock,
		})
		if err != nil {
			t.Fatal(err)
		}
		return ratelimittest.BackendFixture{Backend: conformance, Leases: conformance}
	})
}

func postgresIntegrationRequest(t *testing.T, algorithm ratelimit.Algorithm, id string, capacity uint64, period time.Duration) ratelimit.Request {
	t.Helper()
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: id + "-" + t.Name(), Revision: "v1", Algorithm: algorithm,
		Capacity: capacity, Period: period, MaxCost: capacity,
		Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	key, err := ratelimit.NewKey(ratelimit.KeySpec{
		Namespace: "test", Version: "v1",
		Subject: ratelimit.Subject{Kind: "case", Value: t.Name()}, Hash: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return ratelimit.Request{Policy: policy, Key: key, Cost: 1, Now: time.Unix(100, 0)}
}

func postgresIntegrationLeaseRequest(t *testing.T) ratelimit.LeaseRequest {
	t.Helper()
	request := postgresIntegrationRequest(t, ratelimit.FixedWindow, "unused", 2, time.Second)
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "lease-" + t.Name(), Revision: "v1", Algorithm: ratelimit.Concurrency,
		Capacity: 2, MaxCost: 2, Lease: time.Second,
		Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	request.Policy = policy
	return ratelimit.LeaseRequest{Request: request, LeaseID: "job-1"}
}
