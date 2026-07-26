package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
	"github.com/jackc/pgx/v5/pgconn"
)

type fakeExecutor struct {
	decision ratelimit.Decision
	err      error
	key      []byte
	request  ratelimit.Request
	calls    int
}

type fakeLeaseExecutor struct {
	*fakeExecutor
	leaseErr error
}

func (executor *fakeLeaseExecutor) acquire(context.Context, []byte, ratelimit.LeaseRequest, string) (ratelimit.Lease, ratelimit.Decision, error) {
	return ratelimit.Lease{}, ratelimit.Decision{}, executor.leaseErr
}

func (executor *fakeLeaseExecutor) release(context.Context, []byte, ratelimit.Lease, string) error {
	return executor.leaseErr
}

func (executor *fakeExecutor) admit(_ context.Context, key []byte, request ratelimit.Request) (ratelimit.Decision, error) {
	executor.calls++
	executor.key = append([]byte(nil), key...)
	executor.request = request
	return executor.decision, executor.err
}

func TestStoreUsesOpaqueDigestAndClassifiesBackendErrors(t *testing.T) {
	t.Parallel()

	executor := &fakeExecutor{decision: ratelimit.Decision{
		Allowed: true, Remaining: 4, Limit: 5, Reason: ratelimit.ReasonAllowed,
	}}
	store, err := newStore(executor, Options{Timeout: time.Second, Clock: ClientClock})
	if err != nil {
		t.Fatal(err)
	}
	request := postgresRequest(t)
	decision, err := store.Admit(context.Background(), request)
	if err != nil || !decision.Allowed || decision.Remaining != 4 {
		t.Fatalf("Admit() = %+v, %v", decision, err)
	}
	if len(executor.key) != 32 || string(executor.key) == request.Key.String() {
		t.Fatalf("state key = %x", executor.key)
	}

	executor.err = errors.New("database unavailable password=secret")
	if _, err := store.Admit(context.Background(), request); !errors.Is(err, ratelimit.ErrUnavailable) ||
		strings.Contains(err.Error(), "secret") {
		t.Fatalf("outage error = %v", err)
	}
	executor.calls = 0
	executor.err = &pgconn.PgError{Code: "40P01", Message: "deadlock password=secret"}
	if _, err := store.Admit(context.Background(), request); !errors.Is(err, ratelimit.ErrUnavailable) ||
		executor.calls != 1 {
		t.Fatalf("deadlock Admit() error = %v, calls=%d", err, executor.calls)
	}
	for _, classified := range []error{ratelimit.ErrCorrupt, ratelimit.ErrOverflow} {
		executor.err = fmt.Errorf("%w: password=secret", classified)
		if _, err := store.Admit(context.Background(), request); !errors.Is(err, classified) {
			t.Fatalf("classified Admit() error = %v, want %v", err, classified)
		}
	}
	leaseRequest := concurrencyLeaseRequest(t, request.Now, "lease", 1)
	leaseExecutor := &fakeLeaseExecutor{
		fakeExecutor: &fakeExecutor{},
		leaseErr:     fmt.Errorf("%w: password=secret", ratelimit.ErrCorrupt),
	}
	leaseStore, err := newStore(leaseExecutor, Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := leaseStore.Acquire(context.Background(), leaseRequest); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("classified Acquire() error = %v", err)
	}
	lease := ratelimit.Lease{
		ID: "lease", Key: leaseRequest.Request.Key,
		PolicyID: leaseRequest.Request.Policy.ID(), Cost: 1,
	}
	if err := leaseStore.Release(context.Background(), lease); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("classified Release() error = %v", err)
	}
}

func TestStoreValidatesConfigurationAndRequest(t *testing.T) {
	t.Parallel()

	if _, err := newStore(nil, Options{}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("newStore(nil) error = %v", err)
	}
	store, err := newStore(&fakeExecutor{}, Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Admit(context.Background(), ratelimit.Request{}); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("invalid request error = %v", err)
	}
	request := postgresRequest(t)
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "jobs", Revision: "v1", Algorithm: ratelimit.Concurrency,
		Capacity: 1, MaxCost: 1, Lease: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	request.Policy, request.Cost = policy, 1
	if _, err := store.Admit(context.Background(), request); !errors.Is(err, ratelimit.ErrUnsupported) {
		t.Fatalf("concurrency Admit() error = %v", err)
	}
}

func TestSchemaMigrationOwnsIndexedCleanupTable(t *testing.T) {
	t.Parallel()

	migration := SchemaMigration()
	if migration.Version != 1 || migration.Name == "" ||
		!containsAll(migration.Up,
			"CREATE TABLE rate_limit_states",
			"PRIMARY KEY",
			"expires_at",
			"CREATE INDEX rate_limit_states_expires_at_idx",
		) || migration.Down != "DROP TABLE rate_limit_states;" {
		t.Fatalf("migration = %+v", migration)
	}
	if _, err := GoMigration(); err != nil {
		t.Fatalf("GoMigration() error = %v", err)
	}
}

func postgresRequest(t *testing.T) ratelimit.Request {
	t.Helper()
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "login", Revision: "v1", Algorithm: ratelimit.FixedWindow,
		Capacity: 5, Period: time.Minute, MaxCost: 5,
		Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	key, err := ratelimit.NewKey(ratelimit.KeySpec{
		Namespace: "http", Version: "v1",
		Subject: ratelimit.Subject{Kind: "principal", Value: "sensitive"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return ratelimit.Request{Policy: policy, Key: key, Cost: 1, Now: time.Unix(100, 0)}
}

func containsAll(value string, fragments ...string) bool {
	for _, fragment := range fragments {
		if !strings.Contains(value, fragment) {
			return false
		}
	}
	return true
}
