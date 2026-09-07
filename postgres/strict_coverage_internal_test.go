package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func strictRows(now time.Time, state pgx.Row) []pgx.Row {
	return []pgx.Row{
		rowFunc(func(destinations ...any) error { *(destinations[0].(*string)) = "1s"; return nil }),
		rowFunc(func(destinations ...any) error {
			*(destinations[0].(*any)) = nil
			*(destinations[1].(*time.Time)) = now
			return nil
		}),
		state,
	}
}

func TestStrictPostgresOpenAndDispatchBoundaries(t *testing.T) {
	options := StrictOptions{Options: Options{Timeout: time.Second}, RollbackTimeout: time.Second}
	if store, err := OpenStrict(context.Background(), nil, options); store != nil || !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("OpenStrict(nil) = %+v, %v", store, err)
	}
	if store, err := openStrictChecked(context.Background(), func() (*Store, error) { return nil, ratelimit.ErrInvalidPolicy }); store != nil || !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("open construction failure = %+v, %v", store, err)
	}
	database := &fakeDatabase{row: rowFunc(func(destinations ...any) error {
		value := "rate_limit_states"
		*(destinations[0].(**string)) = &value
		return nil
	})}
	store := &Store{executor: &nativeExecutor{database: database, options: options.Options}, options: options.Options, rollbackTimeout: time.Second}
	if opened, err := openStrictChecked(context.Background(), func() (*Store, error) { return store, nil }); opened != store || err != nil {
		t.Fatalf("open success = %+v, %v", opened, err)
	}
	database.row = rowFunc(func(...any) error { return errors.New("offline") })
	if opened, err := openStrictChecked(context.Background(), func() (*Store, error) { return store, nil }); opened != nil || !errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("open check failure = %+v, %v", opened, err)
	}
	if created, err := NewStrict(&pgxpool.Pool{}, options); created == nil || err != nil {
		t.Fatalf("NewStrict success = %+v, %v", created, err)
	}
	if created, err := NewStrict(&pgxpool.Pool{}, StrictOptions{Options: Options{Timeout: time.Second, Clock: ClockPolicy(99)}, RollbackTimeout: time.Second}); created != nil || !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("NewStrict invalid options = %+v, %v", created, err)
	}

	nonnative := &Store{executor: &fakeExecutor{}, options: Options{Timeout: time.Second}, rollbackTimeout: time.Second}
	concurrency := concurrencyLeaseRequest(t, time.Unix(10, 0), "lease", 1).Request
	if _, err := nonnative.AdmitStrict(context.Background(), concurrency); !errors.Is(err, ratelimit.ErrUnsupported) {
		t.Fatalf("concurrency admit = %v", err)
	}
	if _, err := nonnative.AdmitStrict(context.Background(), postgresRequest(t)); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("nonnative admit = %v", err)
	}
	if _, _, err := nonnative.AcquireStrict(context.Background(), concurrencyLeaseRequest(t, time.Unix(10, 0), "lease", 1)); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("nonnative acquire = %v", err)
	}
	lease := postgresLease(concurrencyLeaseRequest(t, time.Unix(10, 0), "lease", 1), time.Unix(11, 0))
	if err := nonnative.ReleaseStrict(context.Background(), lease); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("nonnative release = %v", err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*ratelimit.Lease)
	}{
		{name: "ID", mutate: func(value *ratelimit.Lease) { value.ID = "" }},
		{name: "policy ID", mutate: func(value *ratelimit.Lease) { value.PolicyID = "" }},
		{name: "key", mutate: func(value *ratelimit.Lease) { value.Key = ratelimit.Key{} }},
		{name: "cost", mutate: func(value *ratelimit.Lease) { value.Cost = 0 }},
	} {
		t.Run("invalid release "+test.name, func(t *testing.T) {
			invalid := lease
			test.mutate(&invalid)
			if err := nonnative.ReleaseStrict(context.Background(), invalid); !errors.Is(err, ratelimit.ErrInvalidRequest) {
				t.Fatalf("error=%v", err)
			}
		})
	}
	if err := nonnative.CheckStrict(context.Background()); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("nonnative check = %v", err)
	}
	if _, err := nonnative.CleanupStrict(context.Background(), 1); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("nonnative cleanup = %v", err)
	}
	if err := (&Store{executor: &nativeExecutor{}}).strictReady(context.Background()); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("zero rollback timeout = %v", err)
	}

	deadline, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	for name, call := range map[string]func() error{
		"admit": func() error { _, err := store.AdmitStrict(deadline, postgresRequest(t)); return err },
		"acquire": func() error {
			_, _, err := store.AcquireStrict(deadline, concurrencyLeaseRequest(t, time.Unix(10, 0), "deadline", 1))
			return err
		},
		"release": func() error { return store.ReleaseStrict(deadline, lease) },
		"check":   func() error { return store.CheckStrict(deadline) },
		"cleanup": func() error { _, err := store.CleanupStrict(deadline, 1); return err },
	} {
		if err := call(); !errors.Is(err, ratelimit.ErrDeadline) {
			t.Fatalf("deadline %s = %v", name, err)
		}
	}
	if !nilInterface(nil) {
		t.Fatal("literal nil was not detected")
	}
}

func TestStrictPostgresErrorCategories(t *testing.T) {
	canceled, stop := context.WithCancel(context.Background())
	stop()
	deadline, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if !errors.Is(postgresPredispatchCategory(canceled, errors.New("ignored")), ratelimit.ErrCanceled) {
		t.Fatal("canceled predispatch not normalized")
	}
	if !errors.Is(postgresPredispatchCategory(deadline, errors.New("ignored")), ratelimit.ErrDeadline) {
		t.Fatal("deadline predispatch not normalized")
	}
	for _, test := range []struct {
		ctx   context.Context
		input error
		want  error
	}{
		{context.Background(), nil, nil},
		{canceled, errors.New("ignored"), ratelimit.ErrCanceled},
		{deadline, errors.New("ignored"), ratelimit.ErrDeadline},
		{canceled, ratelimit.ErrCorrupt, ratelimit.ErrCorrupt},
		{deadline, ratelimit.ErrRejected, ratelimit.ErrRejected},
		{context.Background(), ratelimit.ErrRejected, ratelimit.ErrRejected},
		{context.Background(), ratelimit.ErrUnavailable, ratelimit.ErrUnavailable},
		{context.Background(), ratelimit.ErrOverflow, ratelimit.ErrOverflow},
		{context.Background(), ratelimit.ErrCorrupt, ratelimit.ErrCorrupt},
		{context.Background(), ratelimit.ErrLeaseNotFound, ratelimit.ErrLeaseNotFound},
		{context.Background(), ratelimit.ErrLeaseNotOwned, ratelimit.ErrLeaseNotOwned},
		{context.Background(), errors.New("unexpected"), ratelimit.ErrUnavailable},
	} {
		got := postgresPrecommitCategory(test.ctx, test.input)
		if test.want == nil && got != nil || test.want != nil && !errors.Is(got, test.want) {
			t.Fatalf("postgresPrecommitCategory(%v) = %v", test.input, got)
		}
	}
	for _, category := range []error{
		ratelimit.ErrRejected, ratelimit.ErrUnavailable, ratelimit.ErrOverflow,
		ratelimit.ErrCorrupt, ratelimit.ErrUnsupported, ratelimit.ErrLeaseNotFound,
		ratelimit.ErrLeaseNotOwned,
	} {
		if got := postgresPredispatchCategory(context.Background(), category); got != category { //nolint:errorlint // Direct safe sentinel identity is the contract.
			t.Fatalf("postgresPredispatchCategory(%v) = %v", category, got)
		}
	}

	tx := &fakeTransaction{rows: []pgx.Row{
		rowFunc(func(destinations ...any) error { *(destinations[0].(*string)) = "1s"; return nil }),
		rowFunc(func(...any) error { return errors.New("lock") }),
	}}
	executor := &nativeExecutor{database: &fakeDatabase{tx: tx}, options: Options{Timeout: time.Second, LockTimeout: time.Second}}
	if returned, _, _, err := executor.beginLockedStrict(context.Background(), make([]byte, 32), time.Second); returned != tx || !errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("beginLockedStrict lock failure = %+v, %v", returned, err)
	}
	if err := validateStrictLeaseTime(ratelimit.Lease{}, ratelimit.Decision{Reset: time.Unix(1, 0)}, time.Unix(2, 0)); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("lease decision validation = %v", err)
	}
	next := &persistedState{ObservedMicros: time.Unix(2, 0).UnixMicro()}
	if decision, err := strictPostgresDecisionResult(context.Background(), next, ratelimit.Decision{Reset: time.Unix(1, 0)}, nil); decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("decision result = %+v, %v", decision, err)
	}
	if decision, err := strictPostgresDecisionResult(context.Background(), nil, ratelimit.Decision{}, ratelimit.ErrCorrupt); decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("decision mutation error = %+v, %v", decision, err)
	}
	if lease, decision, err := strictPostgresLeaseResult(context.Background(), next, ratelimit.Lease{ExpiresAt: time.Unix(2, 0)}, ratelimit.Decision{Reset: time.Unix(2, 0)}, nil); lease != (ratelimit.Lease{}) || decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("lease result = %+v, %+v, %v", lease, decision, err)
	}
	if lease, decision, err := strictPostgresLeaseResult(context.Background(), nil, ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrCorrupt); lease != (ratelimit.Lease{}) || decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("lease mutation error = %+v, %+v, %v", lease, decision, err)
	}
}

func TestStrictPostgresAdmitFailureBoundaries(t *testing.T) {
	request := postgresRequest(t)
	badState := &persistedState{Schema: stateSchema + 1, PolicyID: request.Policy.ID(), Algorithm: request.Policy.Algorithm()}
	wrongAlgorithm := &persistedState{Schema: stateSchema, PolicyID: request.Policy.ID(), Algorithm: ratelimit.TokenBucket}
	stateRow := func(value *persistedState) pgx.Row {
		return rowFunc(func(destinations ...any) error {
			*(destinations[0].(*[]byte)) = encodeState(value)
			*(destinations[1].(*time.Time)) = request.Now.Add(time.Second)
			return nil
		})
	}
	for _, test := range []struct {
		name string
		row  pgx.Row
		want error
	}{
		{name: "load", row: rowFunc(func(...any) error { return errors.New("load") }), want: ratelimit.ErrUnavailable},
		{name: "corrupt", row: stateRow(badState), want: ratelimit.ErrCorrupt},
		{name: "mutation", row: stateRow(wrongAlgorithm), want: ratelimit.ErrCorrupt},
	} {
		tx := &fakeTransaction{rows: strictRows(request.Now, test.row)}
		executor := &nativeExecutor{database: &fakeDatabase{tx: tx}, options: Options{Timeout: time.Second, LockTimeout: time.Second}}
		if _, err := executor.admitStrict(context.Background(), make([]byte, 32), request, time.Second); !errors.Is(err, test.want) {
			t.Fatalf("%s admit = %v", test.name, err)
		}
	}
}

func TestStrictPostgresAcquireFailureBoundaries(t *testing.T) {
	request := concurrencyLeaseRequest(t, time.Unix(100, 0), "lease", 1)
	key := make([]byte, 32)
	for _, test := range []struct {
		name string
		tx   *fakeTransaction
		db   *fakeDatabase
		want error
	}{
		{name: "begin", db: &fakeDatabase{beginErr: errors.New("down")}, want: ratelimit.ErrOutcomeUnknown},
		{name: "lock", tx: &fakeTransaction{rows: []pgx.Row{rowFunc(func(...any) error { return errors.New("lock") })}}, want: ratelimit.ErrUnavailable},
		{name: "load", tx: &fakeTransaction{rows: append(strictRows(request.Request.Now, rowFunc(func(...any) error { return errors.New("load") })), nil...)}, want: ratelimit.ErrUnavailable},
		{name: "write", tx: &fakeTransaction{rows: strictRows(request.Request.Now, rowFunc(func(...any) error { return pgx.ErrNoRows })), execErrs: []error{errors.New("write")}}, want: ratelimit.ErrUnavailable},
		{name: "commit", tx: &fakeTransaction{rows: strictRows(request.Request.Now, rowFunc(func(...any) error { return pgx.ErrNoRows })), commitErr: errors.New("commit")}, want: ratelimit.ErrOutcomeUnknown},
	} {
		if test.db == nil {
			test.db = &fakeDatabase{tx: test.tx}
		}
		executor := &nativeExecutor{database: test.db, options: Options{Timeout: time.Second, LockTimeout: time.Second}}
		_, _, err := executor.acquireStrict(context.Background(), key, request, "digest", time.Second)
		if !errors.Is(err, test.want) {
			t.Fatalf("%s acquire = %v", test.name, err)
		}
	}

	rollback := &fakeTransaction{rows: []pgx.Row{rowFunc(func(...any) error { return errors.New("lock") })}, rollbackErr: errors.New("rollback")}
	executor := &nativeExecutor{database: &fakeDatabase{tx: rollback}, options: Options{Timeout: time.Second, LockTimeout: time.Second}}
	if _, _, err := executor.acquireStrict(context.Background(), key, request, "digest", time.Second); !errors.Is(err, ratelimit.ErrOutcomeUnknown) {
		t.Fatalf("rollback acquire = %v", err)
	}
	badState := &persistedState{Schema: stateSchema + 1, PolicyID: request.Request.Policy.ID(), Algorithm: ratelimit.Concurrency}
	wrongAlgorithm := &persistedState{Schema: stateSchema, PolicyID: request.Request.Policy.ID(), Algorithm: ratelimit.TokenBucket}
	badRow := rowFunc(func(destinations ...any) error {
		*(destinations[0].(*[]byte)) = encodeState(badState)
		*(destinations[1].(*time.Time)) = request.Request.Now.Add(time.Second)
		return nil
	})
	wrongAlgorithmRow := rowFunc(func(destinations ...any) error {
		*(destinations[0].(*[]byte)) = encodeState(wrongAlgorithm)
		*(destinations[1].(*time.Time)) = request.Request.Now.Add(time.Second)
		return nil
	})
	tx := &fakeTransaction{rows: strictRows(request.Request.Now, badRow)}
	executor = &nativeExecutor{database: &fakeDatabase{tx: tx}, options: Options{Timeout: time.Second, LockTimeout: time.Second}}
	if _, _, err := executor.acquireStrict(context.Background(), key, request, "digest", time.Second); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("corrupt acquire = %v", err)
	}
	tx = &fakeTransaction{rows: strictRows(request.Request.Now, wrongAlgorithmRow)}
	executor = &nativeExecutor{database: &fakeDatabase{tx: tx}, options: Options{Timeout: time.Second, LockTimeout: time.Second}}
	if _, _, err := executor.acquireStrict(context.Background(), key, request, "digest", time.Second); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("mutation acquire = %v", err)
	}
}

func TestStrictPostgresAcquirePersistsDoubleLeaseTTL(t *testing.T) {
	now := time.Unix(100, 0)
	request := concurrencyLeaseRequest(t, now, "lease", 1)
	tx := &fakeTransaction{rows: strictRows(now, rowFunc(func(...any) error { return pgx.ErrNoRows }))}
	executor := &nativeExecutor{database: &fakeDatabase{tx: tx}, options: Options{Timeout: time.Second, LockTimeout: time.Second}}
	if _, _, err := executor.acquireStrict(context.Background(), make([]byte, 32), request, "digest", time.Second); err != nil {
		t.Fatal(err)
	}
	if len(tx.execArgs) != 1 {
		t.Fatalf("writes=%d", len(tx.execArgs))
	}
	expiresAt, _ := tx.execArgs[0][2].(time.Time)
	if want := now.Add(2 * request.Request.Policy.LeaseDuration()); !expiresAt.Equal(want) {
		t.Fatalf("state expiry=%v, want %v", expiresAt, want)
	}
}

func TestStrictPostgresReleaseFailureBoundaries(t *testing.T) {
	request := concurrencyLeaseRequest(t, time.Unix(100, 0), "lease", 1)
	lease := postgresLease(request, request.Request.Now.Add(time.Second))
	key := make([]byte, 32)
	digest := strings.Repeat("a", 64)
	state := &persistedState{Schema: stateSchema, PolicyID: lease.PolicyID, Algorithm: ratelimit.Concurrency, Leases: map[string]persistedLease{
		digest:                  {Cost: lease.Cost, ExpiresMicros: lease.ExpiresAt.UnixMicro()},
		strings.Repeat("b", 64): {Cost: 1, ExpiresMicros: lease.ExpiresAt.Add(time.Second).UnixMicro()},
	}}
	stateRow := func(value *persistedState) pgx.Row {
		return rowFunc(func(destinations ...any) error {
			*(destinations[0].(*[]byte)) = encodeState(value)
			*(destinations[1].(*time.Time)) = lease.ExpiresAt.Add(time.Second)
			return nil
		})
	}
	for _, test := range []struct {
		name string
		tx   *fakeTransaction
		db   *fakeDatabase
		want error
	}{
		{name: "begin", db: &fakeDatabase{beginErr: errors.New("down")}, want: ratelimit.ErrOutcomeUnknown},
		{name: "lock", tx: &fakeTransaction{rows: []pgx.Row{rowFunc(func(...any) error { return errors.New("lock") })}}, want: ratelimit.ErrUnavailable},
		{name: "load", tx: &fakeTransaction{rows: strictRows(request.Request.Now, rowFunc(func(...any) error { return errors.New("load") }))}, want: ratelimit.ErrUnavailable},
		{name: "missing-state", tx: &fakeTransaction{rows: strictRows(request.Request.Now, rowFunc(func(...any) error { return pgx.ErrNoRows }))}, want: ratelimit.ErrLeaseNotFound},
		{name: "missing-lease", tx: &fakeTransaction{rows: strictRows(request.Request.Now, stateRow(&persistedState{Schema: stateSchema, PolicyID: lease.PolicyID, Algorithm: ratelimit.Concurrency, Leases: map[string]persistedLease{}}))}, want: ratelimit.ErrLeaseNotFound},
		{name: "write", tx: &fakeTransaction{rows: strictRows(request.Request.Now, stateRow(state)), execErrs: []error{errors.New("write")}}, want: ratelimit.ErrUnavailable},
		{name: "commit", tx: &fakeTransaction{rows: strictRows(request.Request.Now, stateRow(state)), commitErr: errors.New("commit")}, want: ratelimit.ErrOutcomeUnknown},
	} {
		if test.db == nil {
			test.db = &fakeDatabase{tx: test.tx}
		}
		executor := &nativeExecutor{database: test.db, options: Options{Timeout: time.Second, LockTimeout: time.Second}}
		if err := executor.releaseStrict(context.Background(), key, lease, digest, time.Second); !errors.Is(err, test.want) {
			t.Fatalf("%s release = %v", test.name, err)
		}
	}
	rollback := &fakeTransaction{rows: []pgx.Row{rowFunc(func(...any) error { return errors.New("lock") })}, rollbackErr: errors.New("rollback")}
	executor := &nativeExecutor{database: &fakeDatabase{tx: rollback}, options: Options{Timeout: time.Second, LockTimeout: time.Second}}
	if err := executor.releaseStrict(context.Background(), key, lease, digest, time.Second); !errors.Is(err, ratelimit.ErrOutcomeUnknown) {
		t.Fatalf("rollback release = %v", err)
	}
	forged := lease
	forged.Cost++
	tx := &fakeTransaction{rows: strictRows(request.Request.Now, stateRow(state))}
	executor = &nativeExecutor{database: &fakeDatabase{tx: tx}, options: Options{Timeout: time.Second, LockTimeout: time.Second}}
	if err := executor.releaseStrict(context.Background(), key, forged, digest, time.Second); !errors.Is(err, ratelimit.ErrLeaseNotOwned) {
		t.Fatalf("forged release = %v", err)
	}
	for _, test := range []struct {
		name      string
		leases    map[string]persistedLease
		wantQuery string
	}{
		{name: "last lease", leases: map[string]persistedLease{
			digest: {Cost: lease.Cost, ExpiresMicros: lease.ExpiresAt.UnixMicro()},
		}, wantQuery: deleteStateSQL},
		{name: "remaining lease", leases: map[string]persistedLease{
			digest:                  {Cost: lease.Cost, ExpiresMicros: lease.ExpiresAt.UnixMicro()},
			strings.Repeat("b", 64): {Cost: 1, ExpiresMicros: lease.ExpiresAt.Add(time.Second).UnixMicro()},
		}, wantQuery: upsertStateSQL},
	} {
		t.Run(test.name, func(t *testing.T) {
			current := &persistedState{Schema: stateSchema, PolicyID: lease.PolicyID, Algorithm: ratelimit.Concurrency, Leases: test.leases}
			tx := &fakeTransaction{rows: strictRows(request.Request.Now, stateRow(current))}
			executor := &nativeExecutor{database: &fakeDatabase{tx: tx}, options: Options{Timeout: time.Second, LockTimeout: time.Second}}
			if err := executor.releaseStrict(context.Background(), key, lease, digest, time.Second); err != nil {
				t.Fatal(err)
			}
			if len(tx.execQueries) != 1 || tx.execQueries[0] != test.wantQuery {
				t.Fatalf("queries=%v", tx.execQueries)
			}
		})
	}
}
