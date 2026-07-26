package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestStateCorruptionRollbackAndArithmeticEdges(t *testing.T) {
	t.Parallel()

	leaseRequest := concurrencyLeaseRequest(t, time.Unix(100, 0), "lease", 1)
	corrupt := &persistedState{Schema: stateSchema, PolicyID: "other", Algorithm: ratelimit.Concurrency}
	if _, _, _, err := mutateLease(corrupt, leaseRequest, "x"); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("corrupt mutateLease() error = %v", err)
	}
	nilLeases := &persistedState{
		Schema: stateSchema, PolicyID: leaseRequest.Request.Policy.ID(),
		Algorithm: ratelimit.Concurrency, ObservedMicros: time.Unix(101, 0).UnixMicro(),
	}
	if _, _, _, err := mutateLease(nilLeases, leaseRequest, "x"); err != nil {
		t.Fatalf("rollback mutateLease() error = %v", err)
	}
	overused := &persistedState{
		Schema: stateSchema, PolicyID: leaseRequest.Request.Policy.ID(),
		Algorithm: ratelimit.Concurrency,
		Leases: map[string]persistedLease{
			"a": {Cost: 2, ExpiresMicros: time.Unix(200, 0).UnixMicro()},
			"b": {Cost: 1, ExpiresMicros: time.Unix(200, 0).UnixMicro()},
		},
	}
	if _, _, decision, err := mutateLease(overused, leaseRequest, "x"); !errors.Is(err, ratelimit.ErrRejected) || decision.Remaining != 0 {
		t.Fatalf("overused mutateLease() = %+v, %v", decision, err)
	}
	overbudget := &persistedState{
		Schema: stateSchema, PolicyID: leaseRequest.Request.Policy.ID(),
		Algorithm: ratelimit.Concurrency,
		Leases: map[string]persistedLease{
			"invalid": {Cost: ratelimit.MaxConcurrencyLeases + 1, ExpiresMicros: time.Unix(200, 0).UnixMicro()},
		},
	}
	if _, _, _, err := mutateLease(overbudget, leaseRequest, "x"); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("overbudget mutateLease() error = %v", err)
	}

	request := postgresRequest(t)
	if _, _, err := mutateState(&persistedState{
		Schema: stateSchema, PolicyID: "other", Algorithm: request.Policy.Algorithm(),
	}, request); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("corrupt mutateState() error = %v", err)
	}
	token := postgresTokenRequest(t, time.Unix(100, 0), 1)
	widePolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "wide", Revision: "v1", Algorithm: ratelimit.TokenBucket,
		Capacity: 9_007_199_254_740_991, Period: time.Microsecond, MaxCost: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	token.Policy = widePolicy
	current := &persistedState{
		Schema: stateSchema, PolicyID: widePolicy.ID(), Algorithm: ratelimit.TokenBucket,
		Tokens: 0, LastMicros: 0,
	}
	if _, err := mutateToken(current, token); err != nil || current.Tokens != widePolicy.Limit()-1 {
		t.Fatalf("wide mutateToken() = %+v, %v", current, err)
	}
	partial := &persistedState{Tokens: 0, LastMicros: token.Now.UnixMicro()}
	token = postgresTokenRequest(t, token.Now.Add(100*time.Millisecond), 1)
	if _, err := mutateToken(partial, token); !errors.Is(err, ratelimit.ErrRejected) ||
		partial.Remainder == 0 {
		t.Fatalf("partial mutateToken() = %+v, %v", partial, err)
	}
	if tokenDuration(0, 0, token.Policy) != 0 {
		t.Fatal("zero token duration was nonzero")
	}
	if tokenDuration(math.MaxUint64, 0, token.Policy) != time.Duration(math.MaxInt64) {
		t.Fatal("wide token duration was not clamped")
	}
	ceilPolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "ceil", Revision: "v1", Algorithm: ratelimit.TokenBucket,
		Capacity: 3, Period: time.Second, MaxCost: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if tokenDuration(1, 0, ceilPolicy) != 333334*time.Microsecond {
		t.Fatal("token duration was not rounded up")
	}
	overflowPolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "overflow", Revision: "v1", Algorithm: ratelimit.TokenBucket,
		Capacity: 1, Period: time.Microsecond, MaxCost: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if tokenDuration(uint64(math.MaxInt64/microsecondNanos)+1, 0, overflowPolicy) != time.Duration(math.MaxInt64) {
		t.Fatal("duration multiplication was not clamped")
	}
	capRequest := postgresTokenRequest(t, time.Unix(100, 250_000_000), 1)
	capState := &persistedState{
		Tokens:     capRequest.Policy.Limit() - 1,
		LastMicros: time.Unix(100, 0).UnixMicro(),
	}
	if _, err := mutateToken(capState, capRequest); err != nil ||
		capState.Tokens != capRequest.Policy.Limit()-1 {
		t.Fatalf("capped mutateToken() = %+v, %v", capState, err)
	}
}

func TestStateCodecWindowAndHelperEdges(t *testing.T) {
	t.Parallel()

	request := postgresRequest(t)
	current := &persistedState{Used: request.Policy.Limit()}
	if _, err := consume(current, request, request.Now.Add(-time.Second)); !errors.Is(err, ratelimit.ErrRejected) {
		t.Fatalf("past reset consume() error = %v", err)
	}
	state := &persistedState{
		Schema: stateSchema, PolicyID: "x", Algorithm: ratelimit.FixedWindow,
	}
	if len(encodeState(state)) == 0 {
		t.Fatal("encodeState() is empty")
	}
	encoded, _ := json.Marshal(state)
	encoded = append(encoded, []byte(" {}")...)
	if _, err := decodeState(encoded); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("trailing decodeState() error = %v", err)
	}
	if _, err := decodeState([]byte(`{"schema":1,"unknown":true}`)); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("unknown decodeState() error = %v", err)
	}
	if floor(-1, 10) != -10 || positiveRemainder(-1, 16) != 15 {
		t.Fatal("negative helpers diverged")
	}
}

type rowFunc func(...any) error

func (row rowFunc) Scan(destinations ...any) error { return row(destinations...) }

type fakeDatabase struct {
	beginErr error
	tx       *fakeTransaction
	row      pgx.Row
}

func (database *fakeDatabase) begin(context.Context) (nativeTransaction, error) {
	if database.beginErr != nil {
		return nil, database.beginErr
	}
	return database.tx, nil
}

func (database *fakeDatabase) queryRow(context.Context, string, ...any) pgx.Row {
	return database.row
}

type fakeTransaction struct {
	rows        []pgx.Row
	execErrs    []error
	commitErr   error
	rollbackErr error
}

func (tx *fakeTransaction) queryRow(context.Context, string, ...any) pgx.Row {
	row := tx.rows[0]
	tx.rows = tx.rows[1:]
	return row
}

func (tx *fakeTransaction) exec(context.Context, string, ...any) error {
	if len(tx.execErrs) == 0 {
		return nil
	}
	err := tx.execErrs[0]
	tx.execErrs = tx.execErrs[1:]
	return err
}

func (tx *fakeTransaction) commit(context.Context) error   { return tx.commitErr }
func (tx *fakeTransaction) rollback(context.Context) error { return tx.rollbackErr }

func TestPostgresStoreAndNativeControlEdges(t *testing.T) {
	t.Parallel()

	if _, err := New(nil, Options{}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("New(nil) error = %v", err)
	}
	admission := &fakeExecutor{err: context.DeadlineExceeded}
	store, err := newStore(admission, Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if store.Name() != "postgres" {
		t.Fatalf("Name() = %q", store.Name())
	}
	if _, err := store.Admit(context.Background(), postgresRequest(t)); !errors.Is(err, ratelimit.ErrDeadline) {
		t.Fatalf("deadline Admit() error = %v", err)
	}
	if _, _, err := store.Acquire(context.Background(), ratelimit.LeaseRequest{}); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("invalid Acquire() error = %v", err)
	}
	if _, _, err := store.Acquire(context.Background(), concurrencyLeaseRequest(t, time.Now(), "x", 1)); !errors.Is(err, ratelimit.ErrUnsupported) {
		t.Fatalf("unsupported Acquire() error = %v", err)
	}
	if err := store.Release(context.Background(), ratelimit.Lease{}); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("invalid Release() error = %v", err)
	}
	lease := edgeLease(t)
	if err := store.Release(context.Background(), lease); !errors.Is(err, ratelimit.ErrUnsupported) {
		t.Fatalf("unsupported Release() error = %v", err)
	}
	if err := store.Check(context.Background()); !errors.Is(err, ratelimit.ErrUnsupported) {
		t.Fatalf("unsupported Check() error = %v", err)
	}
	if _, err := store.Cleanup(context.Background(), 0); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("invalid Cleanup() error = %v", err)
	}
	if _, err := store.Cleanup(context.Background(), 10_001); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("oversized Cleanup() error = %v", err)
	}
	if _, err := store.Cleanup(context.Background(), 1); !errors.Is(err, ratelimit.ErrUnsupported) {
		t.Fatalf("unsupported Cleanup() error = %v", err)
	}

	database := &fakeDatabase{row: rowFunc(func(...any) error { return errors.New("query") })}
	nativeStore, err := newStore(&nativeExecutor{database: database, options: Options{
		Timeout: time.Second, LockTimeout: time.Second,
	}}, Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := nativeStore.Check(context.Background()); !errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("query Check() error = %v", err)
	}
	database.row = rowFunc(func(destinations ...any) error {
		*(destinations[0].(**string)) = nil
		return nil
	})
	if err := nativeStore.Check(context.Background()); !errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("missing Check() error = %v", err)
	}
	table := "rate_limit_states"
	database.row = rowFunc(func(destinations ...any) error {
		*(destinations[0].(**string)) = &table
		return nil
	})
	if err := nativeStore.Check(context.Background()); err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	database.row = rowFunc(func(...any) error { return errors.New("cleanup") })
	if _, err := nativeStore.Cleanup(context.Background(), 1); !errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("cleanup error = %v", err)
	}
	database.row = rowFunc(func(destinations ...any) error {
		*(destinations[0].(*int64)) = 2
		return nil
	})
	if count, err := nativeStore.Cleanup(context.Background(), 1); err != nil || count != 2 {
		t.Fatalf("Cleanup() = %d, %v", count, err)
	}
}

func TestNativeTransactionFailureEdges(t *testing.T) {
	t.Parallel()

	request := postgresRequest(t)
	key := make([]byte, 32)
	lockTime := time.Unix(100, 0)
	setRow := rowFunc(func(destinations ...any) error {
		*(destinations[0].(*string)) = "1s"
		return nil
	})
	lockRow := rowFunc(func(destinations ...any) error {
		*(destinations[0].(*any)) = nil
		*(destinations[1].(*time.Time)) = lockTime
		return nil
	})
	noRows := rowFunc(func(...any) error { return pgx.ErrNoRows })
	for _, test := range []struct {
		name string
		db   *fakeDatabase
	}{
		{name: "begin", db: &fakeDatabase{beginErr: errors.New("begin")}},
		{name: "lock-timeout", db: &fakeDatabase{tx: &fakeTransaction{
			rows: []pgx.Row{rowFunc(func(...any) error { return errors.New("timeout") })},
		}}},
		{name: "lock", db: &fakeDatabase{tx: &fakeTransaction{
			rows: []pgx.Row{setRow, rowFunc(func(...any) error { return errors.New("lock") })},
		}}},
		{name: "load", db: &fakeDatabase{tx: &fakeTransaction{
			rows: []pgx.Row{setRow, lockRow, rowFunc(func(...any) error { return errors.New("load") })},
		}}},
		{name: "write", db: &fakeDatabase{tx: &fakeTransaction{
			rows: []pgx.Row{setRow, lockRow, noRows}, execErrs: []error{errors.New("write")},
		}}},
		{name: "commit", db: &fakeDatabase{tx: &fakeTransaction{
			rows: []pgx.Row{setRow, lockRow, noRows}, commitErr: errors.New("commit"),
		}}},
	} {
		executor := &nativeExecutor{database: test.db, options: Options{
			Timeout: time.Second, LockTimeout: time.Second,
		}}
		if _, err := executor.admit(context.Background(), key, request); err == nil {
			t.Fatalf("%s admit() error = nil", test.name)
		}
	}

	expired := encodeState(&persistedState{
		Schema: stateSchema, PolicyID: request.Policy.ID(),
		Algorithm: request.Policy.Algorithm(),
	})
	expiredRow := rowFunc(func(destinations ...any) error {
		*(destinations[0].(*[]byte)) = expired
		*(destinations[1].(*time.Time)) = lockTime.Add(-time.Second)
		return nil
	})
	tx := &fakeTransaction{
		rows:     []pgx.Row{setRow, lockRow, expiredRow},
		execErrs: []error{errors.New("delete")},
	}
	executor := &nativeExecutor{database: &fakeDatabase{tx: tx}, options: Options{
		Timeout: time.Second, LockTimeout: time.Second,
	}}
	if _, err := executor.admit(context.Background(), key, request); err == nil {
		t.Fatal("expired delete admit() error = nil")
	}
	successExpired := &fakeTransaction{rows: []pgx.Row{setRow, lockRow, expiredRow}}
	executor = &nativeExecutor{database: &fakeDatabase{tx: successExpired}, options: Options{
		Timeout: time.Second, LockTimeout: time.Second, Clock: ServerClock,
	}}
	if _, err := executor.admit(context.Background(), key, request); err != nil {
		t.Fatalf("expired server admit() error = %v", err)
	}
	shortPolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "short", Revision: "v1", Algorithm: ratelimit.FixedWindow,
		Capacity: 1, Period: time.Millisecond, MaxCost: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	shortRequest := request
	shortRequest.Policy = shortPolicy
	shortTx := &fakeTransaction{rows: []pgx.Row{setRow, lockRow, noRows}}
	executor = &nativeExecutor{database: &fakeDatabase{tx: shortTx}, options: Options{
		Timeout: time.Second, LockTimeout: time.Second,
	}}
	if _, err := executor.admit(context.Background(), key, shortRequest); err != nil {
		t.Fatalf("short admit() error = %v", err)
	}
	corruptRow := rowFunc(func(destinations ...any) error {
		*(destinations[0].(*[]byte)) = encodeState(&persistedState{
			Schema: stateSchema, PolicyID: "other", Algorithm: request.Policy.Algorithm(),
		})
		*(destinations[1].(*time.Time)) = request.Now.Add(time.Second)
		return nil
	})
	corruptTx := &fakeTransaction{rows: []pgx.Row{setRow, lockRow, corruptRow}}
	executor = &nativeExecutor{database: &fakeDatabase{tx: corruptTx}, options: Options{
		Timeout: time.Second, LockTimeout: time.Second,
	}}
	if _, err := executor.admit(context.Background(), key, request); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("corrupt admit() error = %v", err)
	}
}

func TestNativeLeaseAndStoreFailureEdges(t *testing.T) {
	t.Parallel()

	request := concurrencyLeaseRequest(t, time.Unix(100, 0), "lease", 1)
	key := make([]byte, 32)
	digestBytes := sha256.Sum256([]byte(request.LeaseID))
	digest := hex.EncodeToString(digestBytes[:])
	lockTime := request.Request.Now
	setRow := rowFunc(func(destinations ...any) error {
		*(destinations[0].(*string)) = "1s"
		return nil
	})
	lockRow := rowFunc(func(destinations ...any) error {
		*(destinations[0].(*any)) = nil
		*(destinations[1].(*time.Time)) = lockTime
		return nil
	})
	noRows := rowFunc(func(...any) error { return pgx.ErrNoRows })

	for _, test := range []struct {
		name string
		tx   *fakeTransaction
	}{
		{name: "load", tx: &fakeTransaction{
			rows: []pgx.Row{setRow, lockRow, rowFunc(func(...any) error { return errors.New("load") })},
		}},
		{name: "write", tx: &fakeTransaction{
			rows: []pgx.Row{setRow, lockRow, noRows}, execErrs: []error{errors.New("write")},
		}},
		{name: "commit", tx: &fakeTransaction{
			rows: []pgx.Row{setRow, lockRow, noRows}, commitErr: errors.New("commit"),
		}},
	} {
		executor := &nativeExecutor{database: &fakeDatabase{tx: test.tx}, options: Options{
			Timeout: time.Second, LockTimeout: time.Second,
		}}
		if _, _, err := executor.acquire(context.Background(), key, request, digest); err == nil {
			t.Fatalf("%s acquire() error = nil", test.name)
		}
	}
	serverExecutor := &nativeExecutor{database: &fakeDatabase{tx: &fakeTransaction{
		rows: []pgx.Row{setRow, lockRow, noRows},
	}}, options: Options{Timeout: time.Second, LockTimeout: time.Second, Clock: ServerClock}}
	if _, _, err := serverExecutor.acquire(context.Background(), key, request, digest); err != nil {
		t.Fatalf("server acquire() error = %v", err)
	}
	shortPolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "short", Revision: "v1", Algorithm: ratelimit.Concurrency,
		Capacity: 1, MaxCost: 1, Lease: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	shortRequest := request
	shortRequest.Request.Policy = shortPolicy
	shortExecutor := &nativeExecutor{database: &fakeDatabase{tx: &fakeTransaction{
		rows: []pgx.Row{setRow, lockRow, noRows},
	}}, options: Options{Timeout: time.Second, LockTimeout: time.Second}}
	if _, _, err := shortExecutor.acquire(context.Background(), key, shortRequest, digest); err != nil {
		t.Fatalf("short acquire() error = %v", err)
	}
	corruptRow := rowFunc(func(destinations ...any) error {
		*(destinations[0].(*[]byte)) = encodeState(&persistedState{
			Schema: stateSchema, PolicyID: request.Request.Policy.ID(),
			Algorithm: ratelimit.FixedWindow,
		})
		*(destinations[1].(*time.Time)) = request.Request.Now.Add(time.Second)
		return nil
	})
	corruptExecutor := &nativeExecutor{database: &fakeDatabase{tx: &fakeTransaction{
		rows: []pgx.Row{setRow, lockRow, corruptRow},
	}}, options: Options{Timeout: time.Second, LockTimeout: time.Second}}
	if _, _, err := corruptExecutor.acquire(context.Background(), key, request, digest); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("corrupt acquire() error = %v", err)
	}

	lease := edgeLease(t)
	for _, test := range []struct {
		name string
		tx   *fakeTransaction
		want error
	}{
		{name: "load", tx: &fakeTransaction{
			rows: []pgx.Row{setRow, lockRow, rowFunc(func(...any) error { return errors.New("load") })},
		}, want: errors.New("load")},
		{name: "missing", tx: &fakeTransaction{
			rows: []pgx.Row{setRow, lockRow, noRows},
		}, want: ratelimit.ErrLeaseNotFound},
	} {
		executor := &nativeExecutor{database: &fakeDatabase{tx: test.tx}, options: Options{
			Timeout: time.Second, LockTimeout: time.Second,
		}}
		if err := executor.release(context.Background(), key, lease, digest); err == nil {
			t.Fatalf("%s release() error = nil", test.name)
		}
	}

	base := &persistedState{
		Schema: stateSchema, PolicyID: lease.PolicyID, Algorithm: ratelimit.Concurrency,
		Leases: map[string]persistedLease{
			digest: {Cost: lease.Cost, ExpiresMicros: lease.ExpiresAt.UnixMicro()},
		},
	}
	stateRow := func(state *persistedState) pgx.Row {
		return rowFunc(func(destinations ...any) error {
			*(destinations[0].(*[]byte)) = encodeState(state)
			*(destinations[1].(*time.Time)) = lease.ExpiresAt.Add(time.Second)
			return nil
		})
	}
	for _, test := range []struct {
		name  string
		state *persistedState
		lease ratelimit.Lease
		txErr error
	}{
		{name: "algorithm", state: &persistedState{
			Schema: stateSchema, PolicyID: lease.PolicyID, Algorithm: ratelimit.FixedWindow,
		}, lease: lease},
		{name: "missing-digest", state: &persistedState{
			Schema: stateSchema, PolicyID: lease.PolicyID, Algorithm: ratelimit.Concurrency,
			Leases: map[string]persistedLease{},
		}, lease: lease},
		{name: "not-owned", state: base, lease: func() ratelimit.Lease {
			forged := lease
			forged.Cost++
			return forged
		}()},
	} {
		tx := &fakeTransaction{rows: []pgx.Row{setRow, lockRow, stateRow(test.state)}}
		executor := &nativeExecutor{database: &fakeDatabase{tx: tx}, options: Options{
			Timeout: time.Second, LockTimeout: time.Second,
		}}
		if err := executor.release(context.Background(), key, test.lease, digest); err == nil {
			t.Fatalf("%s release() error = nil", test.name)
		}
	}
	deleteTx := &fakeTransaction{
		rows:     []pgx.Row{setRow, lockRow, stateRow(base)},
		execErrs: []error{errors.New("delete")},
	}
	executor := &nativeExecutor{database: &fakeDatabase{tx: deleteTx}, options: Options{
		Timeout: time.Second, LockTimeout: time.Second,
	}}
	if err := executor.release(context.Background(), key, lease, digest); err == nil {
		t.Fatal("delete release() error = nil")
	}
	multiple := &persistedState{
		Schema: stateSchema, PolicyID: lease.PolicyID, Algorithm: ratelimit.Concurrency,
		Leases: map[string]persistedLease{
			digest:  {Cost: lease.Cost, ExpiresMicros: lease.ExpiresAt.UnixMicro()},
			"other": {Cost: 1, ExpiresMicros: lease.ExpiresAt.Add(time.Second).UnixMicro()},
		},
	}
	commitTx := &fakeTransaction{
		rows:      []pgx.Row{setRow, lockRow, stateRow(multiple)},
		commitErr: errors.New("commit"),
	}
	executor = &nativeExecutor{database: &fakeDatabase{tx: commitTx}, options: Options{
		Timeout: time.Second, LockTimeout: time.Second,
	}}
	if err := executor.release(context.Background(), key, lease, digest); err == nil {
		t.Fatal("commit release() error = nil")
	}
	writeTx := &fakeTransaction{
		rows:     []pgx.Row{setRow, lockRow, stateRow(multiple)},
		execErrs: []error{errors.New("write")},
	}
	executor = &nativeExecutor{database: &fakeDatabase{tx: writeTx}, options: Options{
		Timeout: time.Second, LockTimeout: time.Second,
	}}
	if err := executor.release(context.Background(), key, lease, digest); err == nil {
		t.Fatal("write release() error = nil")
	}

	beginFailure := &nativeExecutor{database: &fakeDatabase{beginErr: context.DeadlineExceeded}, options: Options{
		Timeout: time.Second, LockTimeout: time.Second,
	}}
	nativeStore, err := newStore(beginFailure, Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := nativeStore.Acquire(context.Background(), request); !errors.Is(err, ratelimit.ErrDeadline) {
		t.Fatalf("store Acquire() error = %v", err)
	}
	if err := nativeStore.Release(context.Background(), lease); !errors.Is(err, ratelimit.ErrDeadline) {
		t.Fatalf("store Release() error = %v", err)
	}
	beginFailure.database = &fakeDatabase{beginErr: errors.New("down")}
	if _, _, err := nativeStore.Acquire(context.Background(), request); !errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("unavailable Acquire() error = %v", err)
	}
	if err := nativeStore.Release(context.Background(), lease); !errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("unavailable Release() error = %v", err)
	}
}

func TestNativeOpenAndDefaults(t *testing.T) {
	t.Parallel()

	pool := &pgxpool.Pool{}
	store, err := New(pool, Options{Timeout: time.Second})
	if err != nil || store.options.LockTimeout != time.Second {
		t.Fatalf("New(default lock) = %+v, %v", store, err)
	}
	config, err := pgxpool.ParseConfig("postgres://postgres:postgres@127.0.0.1:1/missing?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	unavailable, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(unavailable.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := Open(ctx, unavailable, Options{Timeout: time.Second}); err == nil {
		t.Fatal("Open(unavailable) error = nil")
	}
	unavailable.Close()
	closedStore, err := New(unavailable, Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := closedStore.Admit(context.Background(), postgresRequest(t)); err == nil {
		t.Fatal("closed pool Admit() error = nil")
	}
	if _, err := Open(context.Background(), nil, Options{}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("Open(nil) error = %v", err)
	}
}

func edgeLease(t *testing.T) ratelimit.Lease {
	t.Helper()
	request := concurrencyLeaseRequest(t, time.Unix(100, 0), "lease", 1)
	return ratelimit.Lease{
		ID: request.LeaseID, Key: request.Request.Key,
		PolicyID: request.Request.Policy.ID(), Cost: 1,
		ExpiresAt: request.Request.Now.Add(time.Second), Backend: "postgres",
	}
}
