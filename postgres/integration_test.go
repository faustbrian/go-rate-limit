package postgres_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
	"github.com/faustbrian/go-rate-limit/postgres"
	"github.com/faustbrian/go-rate-limit/ratelimittest"
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
	t.Cleanup(func() { closePoolWithin(t, pool) })
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
	for _, clockCase := range []struct {
		name  string
		clock postgres.ClockPolicy
	}{{name: "client", clock: postgres.ClientClock}, {name: "server", clock: postgres.ServerClock}} {
		strictStore, strictErr := postgres.OpenStrict(ctx, pool, postgres.StrictOptions{
			Options: postgres.Options{
				Timeout: time.Second, LockTimeout: 250 * time.Millisecond, Clock: clockCase.clock,
			},
			RollbackTimeout: time.Second,
		})
		if strictErr != nil {
			t.Fatalf("OpenStrict(%s) error = %v", clockCase.name, strictErr)
		}
		strictRequest := postgresIntegrationRequest(t, ratelimit.FixedWindow, "strict-"+clockCase.name, 2, time.Second)
		if decision, strictErr := strictStore.AdmitStrict(ctx, strictRequest); strictErr != nil || !decision.Allowed {
			t.Fatalf("AdmitStrict(%s) = %+v, %v", clockCase.name, decision, strictErr)
		}
		strictLeaseRequest := postgresIntegrationLeaseRequest(t)
		strictLeasePolicy, policyErr := ratelimit.NewPolicy(ratelimit.PolicySpec{
			ID: "strict-lease-" + clockCase.name + "-" + t.Name(), Revision: "v1", Algorithm: ratelimit.Concurrency,
			Capacity: 2, MaxCost: 2, Lease: time.Second, Consistency: ratelimit.ConsistencyStrong,
		})
		if policyErr != nil {
			t.Fatal(policyErr)
		}
		strictLeaseRequest.Request.Policy = strictLeasePolicy
		strictLeaseRequest.LeaseID = "strict-job-" + clockCase.name
		lease, decision, strictErr := strictStore.AcquireStrict(ctx, strictLeaseRequest)
		if strictErr != nil || !decision.Allowed || lease.ID != strictLeaseRequest.LeaseID {
			t.Fatalf("AcquireStrict(%s) = %+v, %+v, %v", clockCase.name, lease, decision, strictErr)
		}
		if strictErr := strictStore.ReleaseStrict(ctx, lease); strictErr != nil {
			t.Fatalf("ReleaseStrict(%s) error = %v", clockCase.name, strictErr)
		}
		if count, strictErr := strictStore.CleanupStrict(ctx, 10); strictErr != nil || count < 0 {
			t.Fatalf("CleanupStrict(%s) = %d, %v", clockCase.name, count, strictErr)
		}
	}
	canceledOpen, cancelOpen := context.WithCancel(ctx)
	cancelOpen()
	if failed, strictErr := postgres.OpenStrict(canceledOpen, pool, postgres.StrictOptions{
		Options: postgres.Options{Timeout: time.Second, LockTimeout: time.Second}, RollbackTimeout: time.Second,
	}); failed != nil || !errors.Is(strictErr, ratelimit.ErrCanceled) {
		t.Fatalf("canceled OpenStrict() = %+v, %v", failed, strictErr)
	}
	if pingErr := pool.Ping(ctx); pingErr != nil {
		t.Fatalf("borrowed pool after strict open: %v", pingErr)
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
	if err != nil || !decision.Allowed || lease.ID != leaseRequest.LeaseID || lease.PolicyID != leaseRequest.Request.Policy.ID() ||
		lease.Key != leaseRequest.Request.Key || lease.Cost != leaseRequest.Request.Cost || lease.Backend != "postgres" ||
		!lease.ExpiresAt.After(leaseRequest.Request.Now) || !decision.Reset.Equal(lease.ExpiresAt) {
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
	t.Cleanup(func() { closePoolWithin(t, reconnectPool) })
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
	closedPool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	closedPool.Close()
	closedStore, err := postgres.New(closedPool, postgres.Options{Timeout: time.Second, LockTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if decision, err := closedStore.Admit(ctx, postgresIntegrationRequest(t, ratelimit.FixedWindow, "closed", 1, time.Second)); !errors.Is(err, ratelimit.ErrUnavailable) || decision != (ratelimit.Decision{}) {
		t.Fatalf("closed-pool Admit() = %+v, %v", decision, err)
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

func TestPostgreSQLStrictStateBoundariesAndLegacyIsolation(t *testing.T) {
	dsn := os.Getenv("POSTGRES_URL")
	if dsn == "" {
		t.Skip("POSTGRES_URL is required for live PostgreSQL tests")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closePoolWithin(t, pool) })
	migration := postgres.SchemaMigration()
	_, _ = pool.Exec(ctx, migration.Down)
	if _, err := pool.Exec(ctx, migration.Up); err != nil {
		t.Fatalf("migration error = %v", err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), migration.Down) })
	strict, err := postgres.OpenStrict(ctx, pool, postgres.StrictOptions{
		Options:         postgres.Options{Timeout: time.Second, LockTimeout: 250 * time.Millisecond, Clock: postgres.ClientClock},
		RollbackTimeout: time.Second,
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
	policy := func(id, revision string, algorithm ratelimit.Algorithm, capacity uint64, period time.Duration) ratelimit.Policy {
		created, policyErr := ratelimit.NewPolicy(ratelimit.PolicySpec{
			ID: id, Revision: revision, Algorithm: algorithm, Capacity: capacity,
			Period: period, MaxCost: capacity, Lease: time.Second,
			Consistency: ratelimit.ConsistencyStrong,
		})
		if policyErr != nil {
			t.Fatal(policyErr)
		}
		return created
	}
	stateKey := func(request ratelimit.Request) []byte {
		digest := sha256.Sum256([]byte(request.Policy.ID() + "\x00" + request.Key.String()))
		return digest[:]
	}

	boundary := ratelimit.Request{
		Policy: policy("postgres-fixed-boundary", "v1", ratelimit.FixedWindow, 1, 9_007_199_254_740_990*time.Microsecond),
		Key:    key, Cost: 1, Now: time.UnixMicro(-9_007_199_254_740_991),
	}
	if decision, admitErr := strict.AdmitStrict(ctx, boundary); decision != (ratelimit.Decision{}) || !errors.Is(admitErr, ratelimit.ErrOverflow) {
		t.Fatalf("fixed boundary = %+v, %v", decision, admitErr)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM rate_limit_states WHERE state_key = $1", stateKey(boundary)).Scan(&count); err != nil || count != 0 {
		t.Fatalf("fixed boundary state count = %d, %v", count, err)
	}

	revision := ratelimit.Request{
		Policy: policy("postgres-period-revision", "v1", ratelimit.TokenBucket, 1, 10*time.Microsecond),
		Key:    key, Cost: 1, Now: time.UnixMicro(200),
	}
	if _, err := strict.AdmitStrict(ctx, revision); err != nil {
		t.Fatal(err)
	}
	revision.Now = time.UnixMicro(205)
	if _, err := strict.AdmitStrict(ctx, revision); !errors.Is(err, ratelimit.ErrRejected) {
		t.Fatalf("persist fractional remainder = %v", err)
	}
	revision.Policy = policy("postgres-period-revision", "v2", ratelimit.TokenBucket, 1, time.Microsecond)
	if decision, admitErr := strict.AdmitStrict(ctx, revision); !errors.Is(admitErr, ratelimit.ErrRejected) || errors.Is(admitErr, ratelimit.ErrCorrupt) ||
		decision.Allowed || decision.Limit != 1 || decision.Remaining != 0 || !decision.Reset.Equal(time.UnixMicro(206)) ||
		decision.RetryAfter != time.Microsecond || decision.Reason != ratelimit.ReasonLimited || decision.Backend != "" || decision.PolicyRevision != "" {
		t.Fatalf("period revision carry = %+v, %v", decision, admitErr)
	}
	var gotRevision string
	var tokens, remainder uint64
	var last int64
	if err := pool.QueryRow(ctx, `SELECT state->>'revision', (state->>'tokens')::numeric, (state->>'remainder')::numeric, (state->>'last_micros')::bigint FROM rate_limit_states WHERE state_key = $1`, stateKey(revision)).Scan(&gotRevision, &tokens, &remainder, &last); err != nil || gotRevision != "v2" || tokens != 0 || remainder != 0 || last != 205 {
		t.Fatalf("period revision state = revision %q tokens %d remainder %d last %d, %v", gotRevision, tokens, remainder, last, err)
	}

	leaseRequest := ratelimit.LeaseRequest{
		Request: ratelimit.Request{
			Policy: policy("postgres-malformed-lease", "v1", ratelimit.Concurrency, 2, time.Second),
			Key:    key, Cost: 1, Now: time.Unix(100, 0),
		},
		LeaseID: "owned",
	}
	lease, _, err := strict.AcquireStrict(ctx, leaseRequest)
	if err != nil {
		t.Fatal(err)
	}
	leaseKey := stateKey(leaseRequest.Request)
	if _, err := pool.Exec(ctx, `UPDATE rate_limit_states SET state = jsonb_set(state, '{leases,x}', '{"cost":1,"expires_micros":200000000}'::jsonb, true) WHERE state_key = $1`, leaseKey); err != nil {
		t.Fatal(err)
	}
	var before map[string]any
	if err := pool.QueryRow(ctx, "SELECT state FROM rate_limit_states WHERE state_key = $1", leaseKey).Scan(&before); err != nil {
		t.Fatal(err)
	}
	leaseRequest.LeaseID = "other"
	if next, decision, acquireErr := strict.AcquireStrict(ctx, leaseRequest); next != (ratelimit.Lease{}) || decision != (ratelimit.Decision{}) || !errors.Is(acquireErr, ratelimit.ErrCorrupt) {
		t.Fatalf("malformed lease identity acquire = %+v, %+v, %v", next, decision, acquireErr)
	}
	if releaseErr := strict.ReleaseStrict(ctx, lease); !errors.Is(releaseErr, ratelimit.ErrCorrupt) {
		t.Fatalf("malformed lease identity release = %v", releaseErr)
	}
	var after map[string]any
	if err := pool.QueryRow(ctx, "SELECT state FROM rate_limit_states WHERE state_key = $1", leaseKey).Scan(&after); err != nil || !reflect.DeepEqual(after, before) {
		t.Fatalf("malformed lease identity mutated: before=%v after=%v error=%v", before, after, err)
	}

	legacy, err := postgres.Open(ctx, pool, postgres.Options{
		Timeout: time.Second, LockTimeout: 250 * time.Millisecond, Clock: postgres.ClientClock,
	})
	if err != nil {
		t.Fatal(err)
	}
	legacyRequest := ratelimit.Request{
		Policy: policy("postgres-legacy-revision", "v1", ratelimit.TokenBucket, 10, time.Minute),
		Key:    key, Cost: 1, Now: time.Unix(300, 0),
	}
	if decision, admitErr := legacy.Admit(ctx, legacyRequest); admitErr != nil || decision.Remaining != 9 {
		t.Fatalf("initial legacy Admit() = %+v, %v", decision, admitErr)
	}
	legacyRequest.Policy = policy("postgres-legacy-revision", "v2", ratelimit.TokenBucket, 2, time.Minute)
	if decision, admitErr := legacy.Admit(ctx, legacyRequest); admitErr != nil || decision.Remaining != 8 {
		t.Fatalf("legacy revision Admit() = %+v, %v; want released remaining 8", decision, admitErr)
	}
}

func closePoolWithin(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		pool.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Error("PostgreSQL pool cleanup exceeded one second")
	}
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
