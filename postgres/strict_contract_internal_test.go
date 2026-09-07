package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestAdmitStrictRollsBackWhenLockSetupPanics(t *testing.T) {
	panicValue := &struct{ boundary string }{boundary: "borrowed collaborator"}
	for _, test := range []struct {
		name string
		tx   *fakeTransaction
	}{
		{
			name: "query row",
			tx: &fakeTransaction{
				queryPanicAt: 1,
				queryPanic:   panicValue,
			},
		},
		{
			name: "scan",
			tx: &fakeTransaction{rows: []pgx.Row{rowFunc(func(...any) error {
				panic(panicValue)
			})}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				_, _ = strictTestStore(test.tx, 50*time.Millisecond).AdmitStrict(context.Background(), postgresRequest(t))
			}()
			if recovered != panicValue {
				t.Fatalf("panic = %v, want original collaborator panic", recovered)
			}
			if test.tx.rollbackCtx == nil {
				t.Fatal("owned transaction was not rolled back")
			}
			deadline, ok := test.tx.rollbackCtx.Deadline()
			if !ok || time.Until(deadline) > 50*time.Millisecond {
				t.Fatalf("rollback deadline = %v, %v", deadline, ok)
			}
		})
	}

}

func TestBeginLockedStrictRollsBackWhenCollaboratorExitsGoroutine(t *testing.T) {
	tx := &fakeTransaction{queryGoexitAt: 1}
	executor := &nativeExecutor{database: &fakeDatabase{tx: tx}, options: Options{LockTimeout: time.Second}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _, _ = executor.beginLockedStrict(context.Background(), make([]byte, 32), 50*time.Millisecond)
		t.Error("beginLockedStrict returned after collaborator Goexit")
	}()
	<-done
	if tx.rollbackCtx == nil {
		t.Fatal("owned transaction was not rolled back after collaborator Goexit")
	}
	deadline, ok := tx.rollbackCtx.Deadline()
	if !ok || time.Until(deadline) > 50*time.Millisecond {
		t.Fatalf("rollback deadline = %v, %v", deadline, ok)
	}
}

func TestStrictPostgresRollbackBudgetStartsAtTransactionOwnership(t *testing.T) {
	const rollbackTimeout = 50 * time.Millisecond
	var rollbackEntryErr error
	var rollbackDeadline time.Time
	var rollbackEnteredAt time.Time
	database := &fakeDatabase{beginDelay: rollbackTimeout + 25*time.Millisecond, tx: &fakeTransaction{rows: []pgx.Row{
		rowFunc(func(...any) error {
			return errors.New("lock failure")
		}),
	}, rollbackFunc: func(ctx context.Context) error {
		rollbackEnteredAt = time.Now()
		rollbackEntryErr = ctx.Err()
		rollbackDeadline, _ = ctx.Deadline()
		return nil
	}}}
	options := Options{Timeout: time.Second, LockTimeout: time.Second}
	store := &Store{
		executor:        &nativeExecutor{database: database, options: options},
		options:         options,
		rollbackTimeout: rollbackTimeout,
	}
	if _, err := store.AdmitStrict(context.Background(), postgresRequest(t)); !errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("AdmitStrict() error = %v", err)
	}
	if rollbackEntryErr != nil {
		t.Fatalf("rollback entered with expired ownership budget: %v", rollbackEntryErr)
	}
	if expiredPreownershipBudget := database.beginAt.Add(rollbackTimeout); !rollbackDeadline.After(expiredPreownershipBudget) {
		t.Fatalf("rollback deadline = %v, did not start after ownership; preownership bound %v", rollbackDeadline, expiredPreownershipBudget)
	}
	if latest := rollbackEnteredAt.Add(rollbackTimeout); rollbackDeadline.After(latest) {
		t.Fatalf("rollback deadline = %v, later than ownership bound %v", rollbackDeadline, latest)
	}
}

func TestStrictPostgresRollbackRetainsValuesDetachesCancellationAndIsSynchronous(t *testing.T) {
	type contextKey struct{}
	caller, cancel := context.WithCancel(context.WithValue(context.Background(), contextKey{}, "retained"))
	rollbackStarted := make(chan struct{})
	releaseRollback := make(chan struct{})
	var rollbackValue any
	var rollbackEntryErr error
	tx := &fakeTransaction{
		rows: []pgx.Row{rowFunc(func(...any) error {
			cancel()
			return context.Canceled
		})},
		rollbackFunc: func(ctx context.Context) error {
			rollbackValue = ctx.Value(contextKey{})
			rollbackEntryErr = ctx.Err()
			close(rollbackStarted)
			<-releaseRollback
			return nil
		},
	}
	options := Options{Timeout: time.Second, LockTimeout: time.Second}
	store := &Store{
		executor:        &nativeExecutor{database: &fakeDatabase{tx: tx}, options: options},
		options:         options,
		rollbackTimeout: time.Second,
	}
	request := postgresRequest(t)
	done := make(chan error, 1)
	go func() {
		_, err := store.AdmitStrict(caller, request)
		done <- err
	}()
	<-rollbackStarted
	select {
	case err := <-done:
		t.Fatalf("AdmitStrict returned before rollback completed: %v", err)
	default:
	}
	close(releaseRollback)
	if err := <-done; !errors.Is(err, ratelimit.ErrCanceled) {
		t.Fatalf("AdmitStrict() error = %v", err)
	}
	if rollbackValue != "retained" || rollbackEntryErr != nil {
		t.Fatalf("rollback context value/error = %v/%v", rollbackValue, rollbackEntryErr)
	}
	if !errors.Is(tx.rollbackCtx.Err(), context.Canceled) {
		t.Fatalf("rollback timer context remains live after return: %v", tx.rollbackCtx.Err())
	}
}

func TestStrictPostgresRoundsLockTimeoutUpToOneMillisecond(t *testing.T) {
	for _, test := range []struct {
		name string
		give time.Duration
		want string
	}{
		{name: "sub millisecond", give: 500 * time.Microsecond, want: "1ms"},
		{name: "exact millisecond", give: time.Millisecond, want: "1ms"},
		{name: "fractional millisecond", give: 1500 * time.Microsecond, want: "2ms"},
	} {
		t.Run(test.name, func(t *testing.T) {
			tx := &fakeTransaction{rows: []pgx.Row{
				rowFunc(func(destinations ...any) error {
					*(destinations[0].(*string)) = test.want
					return nil
				}),
				rowFunc(func(destinations ...any) error {
					*(destinations[0].(*any)) = nil
					*(destinations[1].(*time.Time)) = time.Unix(100, 0)
					return nil
				}),
			}}
			executor := &nativeExecutor{
				database: &fakeDatabase{tx: tx},
				options:  Options{Timeout: time.Second, LockTimeout: test.give},
			}
			returned, _, rollbackDeadline, err := executor.beginLockedStrict(context.Background(), make([]byte, 32), time.Second)
			if err != nil || returned != tx {
				t.Fatalf("beginLockedStrict() = %v, %v", returned, err)
			}
			if got := tx.queryArgs[0][0]; got != test.want {
				t.Fatalf("serialized lock timeout = %v, want %s", got, test.want)
			}
			if err := rollbackStrict(context.Background(), returned, rollbackDeadline); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStrictPostgresSaturatesStateTTL(t *testing.T) {
	overflowingMicrosecondDuration := time.Duration((math.MaxInt64/2/int64(time.Microsecond) + 1) * int64(time.Microsecond))
	now := time.Unix(100, 0).UTC()
	setRow := rowFunc(func(destinations ...any) error { *(destinations[0].(*string)) = "1s"; return nil })
	lockRow := rowFunc(func(destinations ...any) error {
		*(destinations[0].(*any)) = nil
		*(destinations[1].(*time.Time)) = now
		return nil
	})
	noRows := rowFunc(func(...any) error { return pgx.ErrNoRows })

	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "long-period", Revision: "v1", Algorithm: ratelimit.FixedWindow,
		Capacity: 1, Period: overflowingMicrosecondDuration, MaxCost: 1, FailureMode: ratelimit.FailClosed,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := postgresRequest(t)
	request.Policy = policy
	request.Now = now
	admitTx := &fakeTransaction{rows: []pgx.Row{setRow, lockRow, noRows}}
	if _, err := strictTestStore(admitTx, time.Second).AdmitStrict(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if got, want := admitTx.execArgs[0][2].(time.Time), now.Add(time.Duration(math.MaxInt64)); !got.Equal(want) {
		t.Fatalf("strict admission expiry = %v, want %v", got, want)
	}

	leasePolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "long-lease", Revision: "v1", Algorithm: ratelimit.Concurrency,
		Capacity: 1, Lease: overflowingMicrosecondDuration, MaxCost: 1, FailureMode: ratelimit.FailClosed,
	})
	if err != nil {
		t.Fatal(err)
	}
	leaseRequest := concurrencyLeaseRequest(t, now, "lease", 1)
	leaseRequest.Request.Policy = leasePolicy
	leaseTx := &fakeTransaction{rows: []pgx.Row{setRow, lockRow, noRows}}
	if _, _, err := strictTestStore(leaseTx, time.Second).AcquireStrict(context.Background(), leaseRequest); err != nil {
		t.Fatal(err)
	}
	if got, want := leaseTx.execArgs[0][2].(time.Time), now.Add(time.Duration(math.MaxInt64)); !got.Equal(want) {
		t.Fatalf("strict lease expiry = %v, want %v", got, want)
	}
}

func TestStrictPostgresTokenRevisionCarryIsConservative(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	policy := func(revision string, capacity uint64) ratelimit.Policy {
		t.Helper()
		result, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
			ID: "revision-carry", Revision: revision, Algorithm: ratelimit.TokenBucket,
			Capacity: capacity, Period: time.Minute, MaxCost: capacity,
		})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	request := func(revision string, capacity uint64) ratelimit.Request {
		result := postgresRequest(t)
		result.Policy = policy(revision, capacity)
		result.Now = now
		return result
	}
	state := func(revision string, tokens uint64) *persistedState {
		return &persistedState{
			Schema: stateSchema, PolicyID: "revision-carry", Revision: revision,
			Algorithm: ratelimit.TokenBucket, Tokens: tokens,
			LastMicros: now.UnixMicro(), ObservedMicros: now.UnixMicro(),
		}
	}

	for _, test := range []struct {
		name     string
		current  *persistedState
		request  ratelimit.Request
		wantLeft uint64
		wantErr  error
	}{
		{name: "capacity decrease", current: state("v1", 10), request: request("v2", 2), wantLeft: 1},
		{name: "capacity increase", current: state("v1", 2), request: request("v2", 10), wantLeft: 1},
		{name: "same revision exact limit", current: state("v2", 2), request: request("v2", 2), wantLeft: 1},
		{name: "same revision over limit", current: state("v2", 10), request: request("v2", 2), wantErr: ratelimit.ErrCorrupt},
	} {
		t.Run(test.name, func(t *testing.T) {
			next, decision, err := mutateState(test.current, test.request)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("mutateState() error = %v, want %v", err, test.wantErr)
			}
			if test.wantErr != nil {
				if next != nil || decision != (ratelimit.Decision{}) {
					t.Fatalf("corrupt result = %+v, %+v", next, decision)
				}
				return
			}
			if !decision.Allowed || decision.Remaining != test.wantLeft || next.Tokens != test.wantLeft || next.Revision != "v2" {
				t.Fatalf("revision carry = %+v, %+v", next, decision)
			}
		})
	}

	boundary := state("v1", 2)
	boundary.Remainder = uint64(maxExactMicros)
	next, decision, err := mutateState(boundary, request("v2", 2))
	if err != nil || !decision.Allowed || decision.Remaining != 1 || next.Remainder != 0 {
		t.Fatalf("exact remainder revision carry = %+v, %+v, %v", next, decision, err)
	}
}

func TestStrictPostgresRejectsMalformedAlgorithmStateBeforeMutation(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	tokenRequest := postgresRequest(t)
	tokenPolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "malformed-token", Revision: "v1", Algorithm: ratelimit.TokenBucket,
		Capacity: 2, Period: time.Minute, MaxCost: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	tokenRequest.Policy = tokenPolicy
	tokenRequest.Now = now
	tokenState := &persistedState{
		Schema: stateSchema, PolicyID: tokenPolicy.ID(), Revision: tokenPolicy.Revision(), Algorithm: ratelimit.TokenBucket,
		Tokens: 1, Remainder: uint64(tokenPolicy.Period().Microseconds()), LastMicros: now.UnixMicro(), ObservedMicros: now.UnixMicro(),
	}
	if next, decision, err := mutateState(tokenState, tokenRequest); next != nil || decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("malformed token state = %+v, %+v, %v", next, decision, err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*persistedState)
		want   error
	}{
		{name: "observed below range", mutate: func(state *persistedState) { state.ObservedMicros = -maxExactMicros - 1 }, want: ratelimit.ErrCorrupt},
		{name: "token clock below range", mutate: func(state *persistedState) { state.LastMicros = -maxExactMicros - 1 }, want: ratelimit.ErrCorrupt},
		{name: "exact lower clocks", mutate: func(state *persistedState) { state.ObservedMicros, state.LastMicros = -maxExactMicros, -maxExactMicros }},
		{name: "exact upper clocks", mutate: func(state *persistedState) { state.ObservedMicros, state.LastMicros = maxExactMicros, maxExactMicros }, want: ratelimit.ErrOverflow},
	} {
		t.Run(test.name, func(t *testing.T) {
			current := &persistedState{
				Schema: stateSchema, PolicyID: tokenPolicy.ID(), Revision: tokenPolicy.Revision(), Algorithm: ratelimit.TokenBucket,
				Tokens: 1, LastMicros: now.UnixMicro(), ObservedMicros: now.UnixMicro(),
			}
			test.mutate(current)
			next, decision, err := mutateState(current, tokenRequest)
			if !errors.Is(err, test.want) {
				t.Fatalf("clock state error = %v, want %v", err, test.want)
			}
			if test.want != nil && (next != nil || decision != (ratelimit.Decision{})) {
				t.Fatalf("malformed clock state = %+v, %+v, %v", next, decision, err)
			}
			if test.want == nil && (next == nil || !decision.Allowed) {
				t.Fatalf("exact clock state = %+v, %+v, %v", next, decision, err)
			}
		})
	}

	fixedPolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "malformed-fixed", Revision: "v1", Algorithm: ratelimit.FixedWindow,
		Capacity: 2, Period: time.Minute, MaxCost: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	fixedRequest := postgresRequest(t)
	fixedRequest.Policy = fixedPolicy
	fixedRequest.Now = now
	fixedState := &persistedState{
		Schema: stateSchema, PolicyID: fixedPolicy.ID(), Revision: fixedPolicy.Revision(), Algorithm: ratelimit.FixedWindow,
		ObservedMicros: now.UnixMicro(), Window: -maxExactMicros - 1,
	}
	if next, decision, err := mutateState(fixedState, fixedRequest); next != nil || decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("malformed fixed state = %+v, %+v, %v", next, decision, err)
	}
	fixedState.Window = floor(fixedRequest.Now.UnixMicro(), fixedPolicy.Period().Microseconds())
	fixedState.Used = uint64(maxExactMicros)
	fixedState.Revision = "v0"
	if next, decision, err := mutateState(fixedState, fixedRequest); next == nil || next.Used != uint64(maxExactMicros) || decision.Remaining != 0 || !errors.Is(err, ratelimit.ErrRejected) {
		t.Fatalf("exact fixed usage = %+v, %+v, %v", next, decision, err)
	}

	slidingPolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "malformed-sliding", Revision: "v1", Algorithm: ratelimit.SlidingWindow,
		Capacity: 2, Period: time.Minute, MaxCost: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	slidingRequest := postgresRequest(t)
	slidingRequest.Policy = slidingPolicy
	slidingRequest.Now = now
	slidingState := &persistedState{
		Schema: stateSchema, PolicyID: slidingPolicy.ID(), Revision: slidingPolicy.Revision(), Algorithm: ratelimit.SlidingWindow,
		ObservedMicros: now.UnixMicro(),
	}
	slidingState.Segments[0] = persistedSegment{Index: -maxExactMicros - 1, Used: 1}
	if next, decision, err := mutateState(slidingState, slidingRequest); next != nil || decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("malformed sliding index = %+v, %+v, %v", next, decision, err)
	}
	slidingState.Segments[0] = persistedSegment{}
	slidingState.Segments[10] = persistedSegment{Index: 26, Used: uint64(maxExactMicros) + 1}
	if next, decision, err := mutateState(slidingState, slidingRequest); next != nil || decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCorrupt) || slidingState.Used != 0 {
		t.Fatalf("overflowed sliding state = %+v, %+v, %v", next, decision, err)
	}
	slidingState.Revision = "v0"
	slidingState.Segments[10] = persistedSegment{Index: 26, Used: uint64(maxExactMicros)}
	if next, decision, err := mutateState(slidingState, slidingRequest); next == nil || next.Segments[10].Used != uint64(maxExactMicros) || decision.Remaining != 0 || !errors.Is(err, ratelimit.ErrRejected) {
		t.Fatalf("exact sliding usage = %+v, %+v, %v", next, decision, err)
	}
}

func TestStrictSlidingUsageBoundaries(t *testing.T) {
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "sliding-helper", Revision: "v2", Algorithm: ratelimit.SlidingWindow,
		Capacity: 5, MaxCost: 5, Period: 17 * time.Microsecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := postgresRequest(t)
	request.Policy = policy
	request.Now = time.UnixMicro(35)

	state := &persistedState{}
	state.Segments[0] = persistedSegment{Index: 9, Used: 7}
	state.Segments[1] = persistedSegment{Index: 10, Used: 2}
	state.Segments[2] = persistedSegment{Index: 11, Used: 3}
	if used, overflow := strictSlidingUsage(state, request); used != 5 || overflow {
		t.Fatalf("strictSlidingUsage() = %d, %t", used, overflow)
	}

	state.Segments[1].Used = uint64(maxExactMicros)
	state.Segments[2].Used = 0
	if used, overflow := strictSlidingUsage(state, request); used != uint64(maxExactMicros) || overflow {
		t.Fatalf("exact strictSlidingUsage() = %d, %t", used, overflow)
	}
	state.Segments[2].Used = 1
	if used, overflow := strictSlidingUsage(state, request); used != 0 || !overflow {
		t.Fatalf("overflow strictSlidingUsage() = %d, %t", used, overflow)
	}

	exactPolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "sliding-helper-exact", Revision: "v2", Algorithm: ratelimit.SlidingWindow,
		Capacity: 1, MaxCost: 1, Period: 16 * time.Microsecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	exactRequest := request
	exactRequest.Policy = exactPolicy
	exactState := &persistedState{}
	exactState.Segments[0] = persistedSegment{Index: 10, Used: 1}
	if used, overflow := strictSlidingUsage(exactState, exactRequest); used != 0 || overflow {
		t.Fatalf("exact-width strictSlidingUsage() = %d, %t", used, overflow)
	}
	exactState = &persistedState{
		Schema: stateSchema, PolicyID: exactPolicy.ID(), Revision: "v1", Algorithm: ratelimit.SlidingWindow,
		ObservedMicros: exactRequest.Now.UnixMicro(),
	}
	exactState.Segments[10] = persistedSegment{Index: 10, Used: 1}
	exactState.Segments[4] = persistedSegment{Index: 20, Used: 2}
	next, decision, err := mutateState(exactState, exactRequest)
	if !errors.Is(err, ratelimit.ErrRejected) || next == nil || decision.Remaining != 0 || next.Used != 2 || next.Segments[10].Used != 0 || next.Segments[4].Used != 2 {
		t.Fatalf("exact-width revision carry = %+v, %+v, %v", next, decision, err)
	}
}

func TestStrictPostgresWindowRevisionIntegrity(t *testing.T) {
	policy := func(id, revision string, algorithm ratelimit.Algorithm, capacity uint64, period time.Duration) ratelimit.Policy {
		t.Helper()
		result, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
			ID: id, Revision: revision, Algorithm: algorithm,
			Capacity: capacity, MaxCost: capacity, Period: period,
		})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	for _, algorithm := range []ratelimit.Algorithm{ratelimit.FixedWindow, ratelimit.SlidingWindow} {
		t.Run(string(algorithm), func(t *testing.T) {
			t.Run("period change", func(t *testing.T) {
				request := postgresRequest(t)
				request.Policy = policy("period-change-"+string(algorithm), "v1", algorithm, 1, 10*time.Second)
				request.Cost = 1
				request.Now = time.Unix(15, 0)
				state, decision, err := mutateState(nil, request)
				if err != nil || !decision.Allowed {
					t.Fatalf("initial state = %+v, %+v, %v", state, decision, err)
				}
				before := encodeState(state)
				request.Policy = policy("period-change-"+string(algorithm), "v2", algorithm, 1, time.Minute)
				if next, changed, changeErr := mutateState(state, request); next != nil || changed != (ratelimit.Decision{}) || !errors.Is(changeErr, ratelimit.ErrCorrupt) {
					t.Fatalf("period change = %+v, %+v, %v", next, changed, changeErr)
				}
				if !bytes.Equal(encodeState(state), before) {
					t.Fatalf("period change mutated state: before=%s after=%s", before, encodeState(state))
				}
			})
			t.Run("carry history", func(t *testing.T) {
				request := postgresRequest(t)
				request.Policy = policy("carry-history-"+string(algorithm), "v1", algorithm, 5, time.Minute)
				request.Cost = 4
				request.Now = time.Unix(100, 0)
				state, decision, err := mutateState(nil, request)
				if err != nil || !decision.Allowed || state.Used != 4 {
					t.Fatalf("initial carry = %+v, %+v, %v", state, decision, err)
				}
				request.Policy = policy("carry-history-"+string(algorithm), "v2", algorithm, 2, time.Minute)
				request.Cost = 1
				state, decision, err = mutateState(state, request)
				if !errors.Is(err, ratelimit.ErrRejected) || state == nil || state.Used != 4 || decision.Remaining != 0 {
					t.Fatalf("decreased carry = %+v, %+v, %v", state, decision, err)
				}
				request.Policy = policy("carry-history-"+string(algorithm), "v3", algorithm, 5, time.Minute)
				request.Cost = 3
				state, decision, err = mutateState(state, request)
				if !errors.Is(err, ratelimit.ErrRejected) || state == nil || state.Used != 4 || decision.Remaining != 1 {
					t.Fatalf("increased carry = %+v, %+v, %v", state, decision, err)
				}
			})
			t.Run("exact limit is valid and clears carry", func(t *testing.T) {
				request := postgresRequest(t)
				request.Policy = policy("exact-limit-"+string(algorithm), "v1", algorithm, 5, time.Minute)
				request.Cost = 5
				request.Now = time.Unix(100, 0)
				state, decision, err := mutateState(nil, request)
				if err != nil || !decision.Allowed || state.Used != 5 || state.Carried {
					t.Fatalf("initial exact limit = %+v, %+v, %v", state, decision, err)
				}

				request.Cost = 1
				state, decision, err = mutateState(state, request)
				if !errors.Is(err, ratelimit.ErrRejected) || state == nil || decision.Remaining != 0 || state.Carried {
					t.Fatalf("same-revision exact limit = %+v, %+v, %v", state, decision, err)
				}

				request.Policy = policy("exact-limit-"+string(algorithm), "v2", algorithm, 2, time.Minute)
				state, _, err = mutateState(state, request)
				if !errors.Is(err, ratelimit.ErrRejected) || state == nil || !state.Carried {
					t.Fatalf("decreased exact limit = %+v, %v", state, err)
				}

				request.Policy = policy("exact-limit-"+string(algorithm), "v3", algorithm, 5, time.Minute)
				state, decision, err = mutateState(state, request)
				if !errors.Is(err, ratelimit.ErrRejected) || state == nil || decision.Remaining != 0 || state.Carried {
					t.Fatalf("restored exact limit = %+v, %+v, %v", state, decision, err)
				}
			})
		})
	}
}

func TestStrictPostgresRejectsImpossibleSlidingPositionsWithoutMutation(t *testing.T) {
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "sliding-position", Revision: "v1", Algorithm: ratelimit.SlidingWindow,
		Capacity: 2, MaxCost: 2, Period: 16 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := postgresRequest(t)
	request.Policy = policy
	request.Now = time.Unix(100, 0)
	base := func() *persistedState {
		return &persistedState{
			Schema: stateSchema, PolicyID: policy.ID(), Revision: policy.Revision(), Algorithm: ratelimit.SlidingWindow,
			ObservedMicros: request.Now.UnixMicro(),
		}
	}
	for _, test := range []struct {
		name   string
		mutate func(*persistedState)
	}{
		{name: "future", mutate: func(state *persistedState) { state.Segments[5] = persistedSegment{Index: 101, Used: 1} }},
		{name: "wrong slot", mutate: func(state *persistedState) { state.Segments[0] = persistedSegment{Index: 100, Used: 1} }},
		{name: "duplicate index", mutate: func(state *persistedState) {
			state.Segments[4] = persistedSegment{Index: 100, Used: 1}
			state.Segments[5] = persistedSegment{Index: 100, Used: 1}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := base()
			test.mutate(state)
			before := encodeState(state)
			if next, decision, mutateErr := mutateState(state, request); next != nil || decision != (ratelimit.Decision{}) || !errors.Is(mutateErr, ratelimit.ErrCorrupt) {
				t.Fatalf("mutateState() = %+v, %+v, %v", next, decision, mutateErr)
			}
			if !bytes.Equal(encodeState(state), before) {
				t.Fatalf("corrupt state mutated: before=%s after=%s", before, encodeState(state))
			}
		})
	}
}

func TestStrictPostgresIgnoresInactiveSlidingPositionMetadata(t *testing.T) {
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "inactive-sliding-position", Revision: "v1", Algorithm: ratelimit.SlidingWindow,
		Capacity: 2, MaxCost: 2, Period: 16 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := postgresRequest(t)
	request.Policy = policy
	request.Now = time.Unix(100, 0)
	for _, test := range []struct {
		name    string
		segment persistedSegment
	}{
		{name: "zero-use future position", segment: persistedSegment{Index: 101}},
		{name: "exactly expired wrong slot", segment: persistedSegment{Index: 84, Used: 1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := &persistedState{
				Schema: stateSchema, PolicyID: policy.ID(), Revision: policy.Revision(), Algorithm: ratelimit.SlidingWindow,
				PeriodMicros: policy.Period().Microseconds(), ObservedMicros: request.Now.UnixMicro(),
			}
			state.Segments[0] = test.segment
			next, decision, mutateErr := mutateState(state, request)
			if mutateErr != nil || next == nil || !decision.Allowed || decision.Remaining != 1 {
				t.Fatalf("mutateState() = %+v, %+v, %v", next, decision, mutateErr)
			}
		})
	}
}

func TestStrictPostgresRejectsClampedRangeAndAggregateCorruption(t *testing.T) {
	policy := func(id, revision string, algorithm ratelimit.Algorithm, duration time.Duration, capacity uint64) ratelimit.Policy {
		t.Helper()
		spec := ratelimit.PolicySpec{
			ID: id, Revision: revision, Algorithm: algorithm,
			Capacity: capacity, MaxCost: capacity, Period: duration,
		}
		if algorithm == ratelimit.Concurrency {
			spec.Period = 0
			spec.Lease = duration
		}
		result, err := ratelimit.NewPolicy(spec)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	key := postgresRequest(t).Key

	admitPolicy := policy("clamped-admit", "v2", ratelimit.TokenBucket, 2*time.Microsecond, 2)
	admitRequest := ratelimit.Request{
		Policy: admitPolicy, Key: key, Cost: 1,
		Now: time.UnixMicro(maxExactMicros - 2),
	}
	admitState := &persistedState{
		Schema: stateSchema, PolicyID: admitPolicy.ID(), Revision: "v1", Algorithm: ratelimit.TokenBucket,
		Tokens: 1, LastMicros: maxExactMicros - 1, ObservedMicros: maxExactMicros - 1,
	}
	if next, decision, err := mutateState(admitState, admitRequest); next != nil || decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrOverflow) {
		t.Fatalf("clamped admit = %+v, %+v, %v", next, decision, err)
	}
	if admitState.Revision != "v1" || admitState.Tokens != 1 || admitState.LastMicros != maxExactMicros-1 || admitState.ObservedMicros != maxExactMicros-1 {
		t.Fatalf("clamped admit mutated state = %+v", admitState)
	}

	leasePolicy := policy("clamped-lease", "v2", ratelimit.Concurrency, 2*time.Microsecond, 2)
	leaseRequest := ratelimit.LeaseRequest{
		Request: ratelimit.Request{
			Policy: leasePolicy, Key: key, Cost: 1,
			Now: time.UnixMicro(maxExactMicros - 2),
		},
		LeaseID: "second",
	}
	leaseState := &persistedState{
		Schema: stateSchema, PolicyID: leasePolicy.ID(), Revision: "v1", Algorithm: ratelimit.Concurrency,
		ObservedMicros: maxExactMicros - 1, Leases: make(map[string]persistedLease),
	}
	if next, lease, decision, err := mutateLease(leaseState, leaseRequest, "digest"); next != nil || lease != (ratelimit.Lease{}) || decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrOverflow) {
		t.Fatalf("clamped acquire = %+v, %+v, %+v, %v", next, lease, decision, err)
	}
	if leaseState.Revision != "v1" || leaseState.ObservedMicros != maxExactMicros-1 || len(leaseState.Leases) != 0 {
		t.Fatalf("clamped acquire mutated state = %+v", leaseState)
	}

	fixedPolicy := policy("fixed-corrupt", "v1", ratelimit.FixedWindow, time.Minute, 2)
	fixedRequest := ratelimit.Request{Policy: fixedPolicy, Key: key, Cost: 1, Now: time.Unix(100, 0)}
	fixedState := &persistedState{
		Schema: stateSchema, PolicyID: fixedPolicy.ID(), Revision: fixedPolicy.Revision(), Algorithm: ratelimit.FixedWindow,
		ObservedMicros: fixedRequest.Now.UnixMicro(), Window: floor(fixedRequest.Now.UnixMicro(), fixedPolicy.Period().Microseconds()), Used: 3,
	}
	if next, decision, err := mutateState(fixedState, fixedRequest); next != nil || decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("fixed over-limit state = %+v, %+v, %v", next, decision, err)
	}
	previousFixedPolicy := policy("fixed-carry", "v1", ratelimit.FixedWindow, time.Minute, 5)
	carriedFixedPolicy := policy("fixed-carry", "v2", ratelimit.FixedWindow, time.Minute, 2)
	carriedFixedRequest := ratelimit.Request{Policy: carriedFixedPolicy, Key: key, Cost: 1, Now: time.Unix(100, 0)}
	carriedFixedState := &persistedState{
		Schema: stateSchema, PolicyID: previousFixedPolicy.ID(), Revision: previousFixedPolicy.Revision(), Algorithm: ratelimit.FixedWindow,
		ObservedMicros: carriedFixedRequest.Now.UnixMicro(), Window: floor(carriedFixedRequest.Now.UnixMicro(), carriedFixedPolicy.Period().Microseconds()), Used: 4,
	}
	next, decision, err := mutateState(carriedFixedState, carriedFixedRequest)
	if !errors.Is(err, ratelimit.ErrRejected) || next == nil || next.Used != 4 || decision.Remaining != 0 {
		t.Fatalf("fixed revision carry = %+v, %+v, %v", next, decision, err)
	}
	if next, decision, err = mutateState(next, carriedFixedRequest); !errors.Is(err, ratelimit.ErrRejected) || next == nil || next.Used != 4 || decision.Remaining != 0 {
		t.Fatalf("fixed carried state = %+v, %+v, %v", next, decision, err)
	}

	slidingPolicy := policy("sliding-corrupt", "v1", ratelimit.SlidingWindow, time.Minute, 2)
	slidingRequest := ratelimit.Request{Policy: slidingPolicy, Key: key, Cost: 1, Now: time.Unix(100, 0)}
	slidingState := &persistedState{
		Schema: stateSchema, PolicyID: slidingPolicy.ID(), Revision: slidingPolicy.Revision(), Algorithm: ratelimit.SlidingWindow,
		ObservedMicros: slidingRequest.Now.UnixMicro(),
	}
	slidingState.Segments[10] = persistedSegment{Index: 26, Used: uint64(maxExactMicros)}
	slidingState.Segments[11] = persistedSegment{Index: 27, Used: 1}
	if next, decision, err := mutateState(slidingState, slidingRequest); next != nil || decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("aggregate sliding overflow = %+v, %+v, %v", next, decision, err)
	}
	slidingOverLimit := &persistedState{
		Schema: stateSchema, PolicyID: slidingPolicy.ID(), Revision: slidingPolicy.Revision(), Algorithm: ratelimit.SlidingWindow,
		ObservedMicros: slidingRequest.Now.UnixMicro(),
	}
	slidingOverLimit.Segments[10] = persistedSegment{Index: 26, Used: 2}
	slidingOverLimit.Segments[11] = persistedSegment{Index: 11, Used: 1}
	if next, decision, err := mutateState(slidingOverLimit, slidingRequest); next != nil || decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("same-revision sliding over limit = %+v, %+v, %v", next, decision, err)
	}
	previousSlidingPolicy := policy("sliding-carry", "v1", ratelimit.SlidingWindow, time.Minute, 5)
	carriedSlidingPolicy := policy("sliding-carry", "v2", ratelimit.SlidingWindow, time.Minute, 2)
	carriedSlidingRequest := ratelimit.Request{Policy: carriedSlidingPolicy, Key: key, Cost: 1, Now: time.Unix(100, 0)}
	carriedSlidingState := &persistedState{
		Schema: stateSchema, PolicyID: previousSlidingPolicy.ID(), Revision: previousSlidingPolicy.Revision(), Algorithm: ratelimit.SlidingWindow,
		ObservedMicros: carriedSlidingRequest.Now.UnixMicro(),
	}
	carriedSlidingState.Segments[10] = persistedSegment{Index: 26, Used: 2}
	carriedSlidingState.Segments[11] = persistedSegment{Index: 11, Used: 2}
	next, decision, err = mutateState(carriedSlidingState, carriedSlidingRequest)
	if !errors.Is(err, ratelimit.ErrRejected) || next == nil || next.Used != 4 || decision.Remaining != 0 || next.Segments[10].Used != 2 || next.Segments[11].Used != 2 {
		t.Fatalf("sliding revision carry = %+v, %+v, %v", next, decision, err)
	}
	if next, decision, err = mutateState(next, carriedSlidingRequest); !errors.Is(err, ratelimit.ErrRejected) || next == nil || next.Used != 4 || decision.Remaining != 0 {
		t.Fatalf("sliding carried state = %+v, %+v, %v", next, decision, err)
	}
}

func TestStrictPostgresReleaseRejectsMismatchedOrMalformedState(t *testing.T) {
	request := concurrencyLeaseRequest(t, time.Unix(100, 0), "owned", 1)
	lease := postgresLease(request, request.Request.Now.Add(time.Second))
	digest := sha256.Sum256([]byte(lease.ID))
	owned := persistedLease{Cost: lease.Cost, ExpiresMicros: lease.ExpiresAt.UnixMicro()}
	for _, test := range []struct {
		name  string
		state *persistedState
	}{
		{name: "policy mismatch", state: &persistedState{
			Schema: stateSchema, PolicyID: "other-policy", Revision: request.Request.Policy.Revision(),
			Algorithm: ratelimit.Concurrency, ObservedMicros: request.Request.Now.UnixMicro(),
			Leases: map[string]persistedLease{hex.EncodeToString(digest[:]): owned},
		}},
		{name: "malformed unrelated lease", state: &persistedState{
			Schema: stateSchema, PolicyID: lease.PolicyID, Revision: request.Request.Policy.Revision(),
			Algorithm: ratelimit.Concurrency, ObservedMicros: request.Request.Now.UnixMicro(),
			Leases: map[string]persistedLease{
				hex.EncodeToString(digest[:]): owned,
				"other":                       {Cost: 1, ExpiresMicros: maxExactMicros + 1},
			},
		}},
		{name: "malformed lease digest", state: &persistedState{
			Schema: stateSchema, PolicyID: lease.PolicyID, Revision: request.Request.Policy.Revision(),
			Algorithm: ratelimit.Concurrency, ObservedMicros: request.Request.Now.UnixMicro(),
			Leases: map[string]persistedLease{
				hex.EncodeToString(digest[:]): owned,
				"x":                           {Cost: 1, ExpiresMicros: lease.ExpiresAt.UnixMicro()},
			},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			stateRow := rowFunc(func(destinations ...any) error {
				*(destinations[0].(*[]byte)) = encodeState(test.state)
				*(destinations[1].(*time.Time)) = request.Request.Now.Add(time.Second)
				return nil
			})
			tx := &fakeTransaction{rows: strictRows(request.Request.Now, stateRow)}
			store := strictTestStore(tx, time.Second)
			if err := store.ReleaseStrict(context.Background(), lease); !errors.Is(err, ratelimit.ErrCorrupt) {
				t.Fatalf("ReleaseStrict() error = %v", err)
			}
			if len(tx.execQueries) != 0 || tx.commitErr != nil {
				t.Fatalf("corrupt release mutated state: queries=%v", tx.execQueries)
			}
		})
	}
}

func TestStrictPostgresMethodsRejectOversizedStateBeforeMutation(t *testing.T) {
	t.Parallel()

	now := time.Unix(100, 0).UTC()
	oversizedRow := func(expiresAt time.Time) pgx.Row {
		return strictStateRow{
			withinLimit: false,
			row: rowFunc(func(destinations ...any) error {
				*(destinations[0].(*[]byte)) = nil
				*(destinations[1].(*time.Time)) = expiresAt
				return nil
			}),
		}
	}
	t.Run("admit", func(t *testing.T) {
		tx := &fakeTransaction{rows: strictRows(now, oversizedRow(now.Add(time.Second)))}
		if decision, err := strictTestStore(tx, time.Second).AdmitStrict(context.Background(), postgresRequest(t)); decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCorrupt) {
			t.Fatalf("AdmitStrict() = %+v, %v", decision, err)
		}
		if len(tx.execQueries) != 0 {
			t.Fatalf("AdmitStrict() mutated state: %v", tx.execQueries)
		}
	})
	t.Run("acquire", func(t *testing.T) {
		request := concurrencyLeaseRequest(t, now, "oversized", 1)
		tx := &fakeTransaction{rows: strictRows(now, oversizedRow(now.Add(time.Second)))}
		lease, decision, err := strictTestStore(tx, time.Second).AcquireStrict(context.Background(), request)
		if lease != (ratelimit.Lease{}) || decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCorrupt) {
			t.Fatalf("AcquireStrict() = %+v, %+v, %v", lease, decision, err)
		}
		if len(tx.execQueries) != 0 {
			t.Fatalf("AcquireStrict() mutated state: %v", tx.execQueries)
		}
	})
	t.Run("release", func(t *testing.T) {
		request := concurrencyLeaseRequest(t, now, "oversized", 1)
		lease := postgresLease(request, now.Add(time.Second))
		tx := &fakeTransaction{rows: strictRows(now, oversizedRow(now.Add(time.Second)))}
		if err := strictTestStore(tx, time.Second).ReleaseStrict(context.Background(), lease); !errors.Is(err, ratelimit.ErrCorrupt) {
			t.Fatalf("ReleaseStrict() error = %v", err)
		}
		if len(tx.execQueries) != 0 {
			t.Fatalf("ReleaseStrict() mutated state: %v", tx.execQueries)
		}
	})
}

type nilContext struct{}

func TestStrictPostgresTemporalValidationRejectsStaleResults(t *testing.T) {
	effective := time.Unix(100, 0)
	if err := validateStrictDecisionTime(ratelimit.Decision{Allowed: true, Reset: effective.Add(-time.Microsecond)}, effective); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("stale decision error = %v", err)
	}
	if err := validateStrictLeaseTime(ratelimit.Lease{ExpiresAt: effective}, ratelimit.Decision{Allowed: true, Reset: effective}, effective); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("nonfuture lease error = %v", err)
	}
}

func (*nilContext) Deadline() (time.Time, bool) { panic("called") }
func (*nilContext) Done() <-chan struct{}       { panic("called") }
func (*nilContext) Err() error                  { panic("called") }
func (*nilContext) Value(any) any               { panic("called") }

func TestAdmitStrictTreatsBeginFailureAfterDispatchAsUnknown(t *testing.T) {
	store := &Store{
		executor: &nativeExecutor{
			database: &fakeDatabase{beginErr: errors.New("connection lost secret=value")},
			options:  Options{Timeout: time.Second, LockTimeout: time.Second},
		},
		options:         Options{Timeout: time.Second, LockTimeout: time.Second},
		rollbackTimeout: time.Second,
	}
	_, err := store.AdmitStrict(context.Background(), postgresRequest(t))
	if !errors.Is(err, ratelimit.ErrOutcomeUnknown) {
		t.Fatalf("AdmitStrict() error = %v, want outcome unknown", err)
	}
	if errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("begin failure was incorrectly classified unavailable: %v", err)
	}
}

func TestAdmitStrictBoundsOwnedRollbackAndPreservesKnownFailure(t *testing.T) {
	setRow := rowFunc(func(...any) error { return errors.New("lock setup failed") })
	tx := &fakeTransaction{rows: []pgx.Row{setRow}}
	store := strictTestStore(tx, 50*time.Millisecond)
	_, err := store.AdmitStrict(context.Background(), postgresRequest(t))
	if err != ratelimit.ErrUnavailable { //nolint:errorlint // Predispatch normalization returns the exact stable sentinel.
		t.Fatalf("AdmitStrict() error = %v, want unavailable", err)
	}
	deadline, ok := tx.rollbackCtx.Deadline()
	if !ok || time.Until(deadline) > 50*time.Millisecond {
		t.Fatalf("rollback deadline = %v, %v", deadline, ok)
	}
}

func TestAdmitStrictRollbackFailureOverridesKnownFailure(t *testing.T) {
	tx := &fakeTransaction{
		rows:        []pgx.Row{rowFunc(func(...any) error { return errors.New("lock setup failed") })},
		rollbackErr: errors.New("rollback failed"),
	}
	store := strictTestStore(tx, time.Second)
	_, err := store.AdmitStrict(context.Background(), postgresRequest(t))
	if !errors.Is(err, ratelimit.ErrOutcomeUnknown) || errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("AdmitStrict() error = %v, want only outcome unknown", err)
	}
}

func strictTestStore(tx *fakeTransaction, rollbackTimeout time.Duration) *Store {
	options := Options{Timeout: time.Second, LockTimeout: time.Second}
	return &Store{
		executor: &nativeExecutor{database: &fakeDatabase{tx: tx}, options: options},
		options:  options, rollbackTimeout: rollbackTimeout,
	}
}

func TestStrictPostgresConstructionAndValidation(t *testing.T) {
	options := StrictOptions{Options: Options{Timeout: time.Second}, RollbackTimeout: time.Second}
	for _, ctx := range []context.Context{nil, (*nilContext)(nil)} {
		called := false
		store, err := openStrictChecked(ctx, func() (*Store, error) {
			called = true
			panic("constructor called")
		})
		if store != nil || err == nil || err.Error() != "invalid rate limit request: context is required" || called {
			t.Fatalf("OpenStrict(nil context) = %v, %v, constructor called=%v", store, err, called)
		}
	}
	if store, err := NewStrict(nil, options); store != nil || err == nil || err.Error() != "invalid rate limit policy: PostgreSQL pool is required" {
		t.Fatalf("nil pool=%v,%v", store, err)
	}
	if store, err := NewStrict(&pgxpool.Pool{}, StrictOptions{Options: Options{Timeout: time.Second}}); store != nil || err == nil || err.Error() != "invalid rate limit policy: rollback timeout must be positive" {
		t.Fatalf("rollback=%v,%v", store, err)
	}
	var absent *Store
	var nilCtx *nilContext
	if _, err := absent.AdmitStrict(context.Background(), ratelimit.Request{}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("nil admit=%v", err)
	}
	if _, _, err := absent.AcquireStrict(context.Background(), ratelimit.LeaseRequest{}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("nil acquire=%v", err)
	}
	if err := absent.ReleaseStrict(context.Background(), ratelimit.Lease{}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("nil release=%v", err)
	}
	if err := absent.CheckStrict(context.Background()); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("nil check=%v", err)
	}
	if _, err := absent.CleanupStrict(context.Background(), 1); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("nil cleanup=%v", err)
	}
	store := strictTestStore(&fakeTransaction{}, time.Second)
	if _, err := store.AdmitStrict(nilCtx, ratelimit.Request{}); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("nil context=%v", err)
	}
	if _, err := store.AdmitStrict(context.Background(), ratelimit.Request{}); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("invalid=%v", err)
	}
	if _, _, err := store.AcquireStrict(context.Background(), ratelimit.LeaseRequest{}); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("invalid acquire=%v", err)
	}
	if err := store.ReleaseStrict(context.Background(), ratelimit.Lease{}); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("invalid release=%v", err)
	}
	if _, err := store.CleanupStrict(context.Background(), 0); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("invalid cleanup=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.AdmitStrict(ctx, postgresRequest(t)); !errors.Is(err, ratelimit.ErrCanceled) {
		t.Fatalf("canceled=%v", err)
	}
}

func TestStrictPostgresConstructorOwnership(t *testing.T) {
	t.Parallel()

	options := Options{Timeout: time.Second, LockTimeout: time.Second}
	callerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	available := "rate_limit_states"
	database := &fakeDatabase{row: rowFunc(func(destinations ...any) error {
		*(destinations[0].(**string)) = &available
		return nil
	})}
	store := &Store{executor: &nativeExecutor{database: database, options: options}, options: options, rollbackTimeout: time.Second}
	opened, err := openStrictChecked(callerCtx, func() (*Store, error) { return store, nil })
	if err != nil || opened != store || database.queries != 1 || database.queryCtx != callerCtx {
		t.Fatalf("successful open = %+v, %v, queries=%d, caller context=%v", opened, err, database.queries, database.queryCtx == callerCtx)
	}

	database.queries = 0
	database.row = rowFunc(func(...any) error { return errors.New("offline") })
	if failed, openErr := openStrictChecked(callerCtx, func() (*Store, error) { return store, nil }); failed != nil || !errors.Is(openErr, ratelimit.ErrUnavailable) || database.queries != 1 {
		t.Fatalf("failed open = %+v, %v, queries=%d", failed, openErr, database.queries)
	}

	panicValue := &struct{ owner string }{owner: "caller"}
	database.panicOnQuery = panicValue
	func() {
		defer func() {
			if recovered := recover(); recovered != panicValue {
				t.Fatalf("open panic = %v", recovered)
			}
		}()
		_, _ = openStrictChecked(callerCtx, func() (*Store, error) { return store, nil })
	}()
}

func TestCheckStrictNormalizesDirectPredispatchErrors(t *testing.T) {
	options := Options{Timeout: time.Second, LockTimeout: time.Second}
	for _, test := range []struct {
		name string
		err  error
		want error
	}{
		{name: "canceled", err: context.Canceled, want: ratelimit.ErrCanceled},
		{name: "deadline", err: context.DeadlineExceeded, want: ratelimit.ErrDeadline},
	} {
		t.Run(test.name, func(t *testing.T) {
			database := &fakeDatabase{row: rowFunc(func(...any) error { return test.err })}
			store := &Store{executor: &nativeExecutor{database: database, options: options}, options: options, rollbackTimeout: time.Second}
			if err := store.CheckStrict(context.Background()); !errors.Is(err, test.want) || errors.Is(err, ratelimit.ErrUnavailable) {
				t.Fatalf("CheckStrict() error = %v, want only %v", err, test.want)
			}
		})
	}

	for _, category := range []error{
		ratelimit.ErrRejected, ratelimit.ErrUnavailable, ratelimit.ErrOverflow,
		ratelimit.ErrCorrupt, ratelimit.ErrUnsupported, ratelimit.ErrLeaseNotFound,
		ratelimit.ErrLeaseNotOwned,
	} {
		backendErr, err := ratelimit.NewBackendError(category)
		if err != nil {
			t.Fatal(err)
		}
		database := &fakeDatabase{row: rowFunc(func(...any) error { return backendErr })}
		store := &Store{executor: &nativeExecutor{database: database, options: options}, options: options, rollbackTimeout: time.Second}
		if got := store.CheckStrict(context.Background()); got != backendErr { //nolint:errorlint // Direct BackendError identity is the contract.
			t.Fatalf("CheckStrict(%v) = %v, want original safe backend error", category, got)
		}
	}
	for _, input := range []error{
		ratelimit.ErrCorrupt,
		new(ratelimit.BackendError),
		(*ratelimit.BackendError)(nil),
	} {
		database := &fakeDatabase{row: rowFunc(func(...any) error { return input })}
		store := &Store{executor: &nativeExecutor{database: database, options: options}, options: options, rollbackTimeout: time.Second}
		want := input
		if input != ratelimit.ErrCorrupt { //nolint:errorlint // Selects the one exact safe sentinel from invalid BackendError cases.
			want = ratelimit.ErrUnavailable
		}
		if got := store.CheckStrict(context.Background()); got != want { //nolint:errorlint // Direct safe errors preserve exact identity.
			t.Fatalf("CheckStrict direct/invalid safe error was not normalized")
		}
	}
}

func TestPostgresStateTTLExactSaturationBoundary(t *testing.T) {
	boundary := time.Duration(math.MaxInt64) / 2
	if got := postgresStateTTL(boundary); got != boundary*2 {
		t.Fatalf("boundary TTL = %v, want %v", got, boundary*2)
	}
	if got := postgresStateTTL(boundary + 1); got != time.Duration(math.MaxInt64) {
		t.Fatalf("overflow TTL = %v, want saturation", got)
	}
}

func TestStrictPostgresCheckAndCleanup(t *testing.T) {
	options := Options{Timeout: time.Second, LockTimeout: time.Second}
	database := &fakeDatabase{row: rowFunc(func(destinations ...any) error {
		value := "rate_limit_states"
		*(destinations[0].(**string)) = &value
		return nil
	})}
	store := &Store{executor: &nativeExecutor{database: database, options: options}, options: options, rollbackTimeout: time.Second}
	if err := store.CheckStrict(context.Background()); err != nil {
		t.Fatalf("check=%v", err)
	}
	database.row = rowFunc(func(destinations ...any) error { *(destinations[0].(**string)) = nil; return nil })
	if err := store.CheckStrict(context.Background()); err != ratelimit.ErrUnavailable { //nolint:errorlint // Missing schema returns the exact stable sentinel.
		t.Fatalf("missing=%v", err)
	}
	database.row = rowFunc(func(...any) error { return errors.New("query") })
	if err := store.CheckStrict(context.Background()); err != ratelimit.ErrUnavailable { //nolint:errorlint // Unsafe query errors normalize to the exact stable sentinel.
		t.Fatalf("check error=%v", err)
	}
	if _, err := store.CleanupStrict(context.Background(), 1); !errors.Is(err, ratelimit.ErrOutcomeUnknown) {
		t.Fatalf("cleanup error=%v", err)
	}
	database.row = rowFunc(func(destinations ...any) error { *(destinations[0].(*int64)) = 2; return nil })
	if count, err := store.CleanupStrict(context.Background(), 1); err != nil || count != 2 {
		t.Fatalf("cleanup=%d,%v", count, err)
	}
	database.row = rowFunc(func(destinations ...any) error { *(destinations[0].(*int64)) = int64(MaxCleanupBatch); return nil })
	if count, err := store.CleanupStrict(context.Background(), MaxCleanupBatch); err != nil || count != int64(MaxCleanupBatch) {
		t.Fatalf("maximum cleanup=%d,%v", count, err)
	}
}

func TestStrictPostgresAdmitSuccess(t *testing.T) {
	setRow := rowFunc(func(destinations ...any) error { *(destinations[0].(*string)) = "1s"; return nil })
	lockRow := rowFunc(func(destinations ...any) error {
		*(destinations[0].(*any)) = nil
		*(destinations[1].(*time.Time)) = time.Unix(100, 0)
		return nil
	})
	noRows := rowFunc(func(...any) error { return pgx.ErrNoRows })
	tx := &fakeTransaction{rows: []pgx.Row{setRow, lockRow, noRows}}
	store := strictTestStore(tx, time.Second)
	decision, err := store.AdmitStrict(context.Background(), postgresRequest(t))
	if err != nil || !decision.Allowed {
		t.Fatalf("admit=%+v,%v", decision, err)
	}
}

func TestStrictPostgresPersistsClampedEffectiveTime(t *testing.T) {
	setRow := rowFunc(func(destinations ...any) error { *(destinations[0].(*string)) = "1s"; return nil })
	lockRow := rowFunc(func(destinations ...any) error {
		*(destinations[0].(*any)) = nil
		*(destinations[1].(*time.Time)) = time.Unix(100, 0)
		return nil
	})
	request := postgresRequest(t)
	state := &persistedState{Schema: stateSchema, PolicyID: request.Policy.ID(), Algorithm: request.Policy.Algorithm(), ObservedMicros: time.Unix(200, 0).UnixMicro()}
	stateRow := rowFunc(func(destinations ...any) error {
		*(destinations[0].(*[]byte)) = encodeState(state)
		*(destinations[1].(*time.Time)) = time.Unix(300, 0)
		return nil
	})
	tx := &fakeTransaction{rows: []pgx.Row{setRow, lockRow, stateRow}}
	store := strictTestStore(tx, time.Second)
	if _, err := store.AdmitStrict(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if len(tx.execArgs) != 1 {
		t.Fatalf("writes = %d", len(tx.execArgs))
	}
	expiresAt, _ := tx.execArgs[0][2].(time.Time)
	effective, _ := tx.execArgs[0][3].(time.Time)
	if !effective.Equal(time.Unix(200, 0)) || !expiresAt.Equal(time.Unix(320, 0)) {
		t.Fatalf("persisted times = %v, %v", effective, expiresAt)
	}
}

func TestStrictPostgresServerClockAcceptsResultsBeforeCallerTime(t *testing.T) {
	callerNow := time.Unix(300, 0)
	serverNow := time.Unix(100, 0)
	setRow := rowFunc(func(destinations ...any) error {
		*(destinations[0].(*string)) = "1s"
		return nil
	})
	lockRow := rowFunc(func(destinations ...any) error {
		*(destinations[0].(*any)) = nil
		*(destinations[1].(*time.Time)) = serverNow
		return nil
	})
	stateRow := func(state *persistedState) pgx.Row {
		if state == nil {
			return rowFunc(func(...any) error { return pgx.ErrNoRows })
		}
		return rowFunc(func(destinations ...any) error {
			*(destinations[0].(*[]byte)) = encodeState(state)
			*(destinations[1].(*time.Time)) = time.Unix(400, 0)
			return nil
		})
	}
	serviceFor := func(t *testing.T, state *persistedState) (*ratelimit.StrictService, <-chan ratelimit.Observation) {
		t.Helper()
		options := Options{Timeout: time.Second, LockTimeout: time.Second, Clock: ServerClock}
		tx := &fakeTransaction{rows: []pgx.Row{setRow, lockRow, stateRow(state)}}
		store := &Store{
			executor: &nativeExecutor{database: &fakeDatabase{tx: tx}, options: options},
			options:  options, rollbackTimeout: time.Second,
		}
		observed := make(chan ratelimit.Observation, 1)
		service, err := ratelimit.NewStrictService(store, ratelimit.ObserveFunc(func(observation ratelimit.Observation) { observed <- observation }))
		if err != nil {
			t.Fatal(err)
		}
		return service, observed
	}

	request := postgresRequest(t)
	request.Now = callerNow
	for _, test := range []struct {
		name  string
		state *persistedState
		err   error
	}{
		{name: "allowed"},
		{name: "rejected", state: &persistedState{
			Schema: stateSchema, PolicyID: request.Policy.ID(), Revision: request.Policy.Revision(),
			Algorithm: ratelimit.FixedWindow, ObservedMicros: serverNow.UnixMicro(),
			Window: time.Unix(60, 0).UnixMicro(), Used: request.Policy.Limit(),
		}, err: ratelimit.ErrRejected},
	} {
		t.Run("admit-"+test.name, func(t *testing.T) {
			service, observed := serviceFor(t, test.state)
			decision, err := service.Admit(context.Background(), request)
			if err != test.err || errors.Is(err, ratelimit.ErrOutcomeUnknown) || !decision.Reset.Equal(time.Unix(120, 0)) { //nolint:errorlint // Known outcomes preserve exact identity.
				t.Fatalf("Admit() = %+v, %v", decision, err)
			}
			observation := <-observed
			if observation.Decision != decision || observation.Err != err { //nolint:errorlint // Observer receives the exact returned error object.
				t.Fatalf("observation = %+v/%v", observation.Decision, observation.Err)
			}
		})
	}

	leaseRequest := concurrencyLeaseRequest(t, callerNow, "lease", 1)
	for _, test := range []struct {
		name      string
		state     *persistedState
		err       error
		wantReset time.Time
	}{
		{name: "allowed", wantReset: serverNow.Add(time.Second)},
		{name: "rejected", state: &persistedState{
			Schema: stateSchema, PolicyID: leaseRequest.Request.Policy.ID(), Revision: leaseRequest.Request.Policy.Revision(),
			Algorithm: ratelimit.Concurrency, ObservedMicros: serverNow.UnixMicro(),
			Leases: map[string]persistedLease{strings.Repeat("a", 64): {Cost: leaseRequest.Request.Policy.Limit(), ExpiresMicros: time.Unix(200, 0).UnixMicro()}},
		}, err: ratelimit.ErrRejected, wantReset: time.Unix(200, 0)},
	} {
		t.Run("acquire-"+test.name, func(t *testing.T) {
			service, observed := serviceFor(t, test.state)
			lease, decision, err := service.Acquire(context.Background(), leaseRequest)
			if err != test.err || errors.Is(err, ratelimit.ErrOutcomeUnknown) || !decision.Reset.Equal(test.wantReset) { //nolint:errorlint // Known outcomes preserve exact identity.
				t.Fatalf("Acquire() = %+v, %+v, %v", lease, decision, err)
			}
			if test.err == nil && !lease.ExpiresAt.Equal(test.wantReset) || test.err != nil && lease != (ratelimit.Lease{}) {
				t.Fatalf("lease = %+v", lease)
			}
			observation := <-observed
			if observation.Decision != decision || observation.Err != err { //nolint:errorlint // Observer receives the exact returned error object.
				t.Fatalf("observation = %+v/%v", observation.Decision, observation.Err)
			}
		})
	}
}

func TestStrictPostgresServerClockRejectsOutOfRangeTransitionsBeforeMutation(t *testing.T) {
	serverNow := time.UnixMicro(2_000_000_000_000_000).UTC()
	setRow := rowFunc(func(destinations ...any) error {
		*(destinations[0].(*string)) = "1s"
		return nil
	})
	lockRow := rowFunc(func(destinations ...any) error {
		*(destinations[0].(*any)) = nil
		*(destinations[1].(*time.Time)) = serverNow
		return nil
	})
	noRows := rowFunc(func(...any) error { return pgx.ErrNoRows })
	longDuration := time.Duration(8_000_000_000) * time.Second
	options := Options{Timeout: time.Second, LockTimeout: time.Second, Clock: ServerClock}

	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "server-range-admit", Revision: "v1", Algorithm: ratelimit.FixedWindow,
		Capacity: 1, MaxCost: 1, Period: longDuration, Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := postgresRequest(t)
	request.Policy = policy
	request.Now = time.Unix(0, 0)
	admitTx := &fakeTransaction{rows: []pgx.Row{setRow, lockRow, noRows}}
	admitStore := &Store{executor: &nativeExecutor{database: &fakeDatabase{tx: admitTx}, options: options}, options: options, rollbackTimeout: time.Second}
	if decision, admitErr := admitStore.AdmitStrict(context.Background(), request); decision != (ratelimit.Decision{}) ||
		!errors.Is(admitErr, ratelimit.ErrOverflow) || len(admitTx.execArgs) != 0 {
		t.Fatalf("AdmitStrict(out of range) = %+v, %v, writes=%d", decision, admitErr, len(admitTx.execArgs))
	}

	leasePolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "server-range-lease", Revision: "v1", Algorithm: ratelimit.Concurrency,
		Capacity: 1, MaxCost: 1, Lease: longDuration, Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	leaseRequest := concurrencyLeaseRequest(t, time.Unix(0, 0), "lease", 1)
	leaseRequest.Request.Policy = leasePolicy
	leaseTx := &fakeTransaction{rows: []pgx.Row{setRow, lockRow, noRows}}
	leaseStore := &Store{executor: &nativeExecutor{database: &fakeDatabase{tx: leaseTx}, options: options}, options: options, rollbackTimeout: time.Second}
	lease, decision, acquireErr := leaseStore.AcquireStrict(context.Background(), leaseRequest)
	if lease != (ratelimit.Lease{}) || decision != (ratelimit.Decision{}) ||
		!errors.Is(acquireErr, ratelimit.ErrOverflow) || len(leaseTx.execArgs) != 0 {
		t.Fatalf("AcquireStrict(out of range) = %+v, %+v, %v, writes=%d", lease, decision, acquireErr, len(leaseTx.execArgs))
	}
}

func TestStrictPostgresFixedWindowRejectsUnrepresentableBoundaryBeforeWrite(t *testing.T) {
	now := time.UnixMicro(-9_007_199_254_740_991).UTC()
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "fixed-boundary", Revision: "v1", Algorithm: ratelimit.FixedWindow,
		Capacity: 1, MaxCost: 1, Period: 9_007_199_254_740_990 * time.Microsecond,
		Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := postgresRequest(t)
	request.Policy = policy
	request.Now = now
	tx := &fakeTransaction{rows: strictRows(now, rowFunc(func(...any) error { return pgx.ErrNoRows }))}
	store := strictTestStore(tx, time.Second)
	decision, err := store.AdmitStrict(context.Background(), request)
	if decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrOverflow) || len(tx.execArgs) != 0 || tx.rollbackCtx == nil {
		t.Fatalf("AdmitStrict() = %+v, %v, writes=%d, rollback=%v", decision, err, len(tx.execArgs), tx.rollbackCtx)
	}
}

func TestStrictPostgresLeaseSuccessAndAmbiguity(t *testing.T) {
	setRow := rowFunc(func(destinations ...any) error { *(destinations[0].(*string)) = "1s"; return nil })
	lockRow := rowFunc(func(destinations ...any) error {
		*(destinations[0].(*any)) = nil
		*(destinations[1].(*time.Time)) = time.Unix(100, 0)
		return nil
	})
	noRows := rowFunc(func(...any) error { return pgx.ErrNoRows })
	request := concurrencyLeaseRequest(t, time.Unix(100, 0), "lease", 1)
	acquireTx := &fakeTransaction{rows: []pgx.Row{setRow, lockRow, noRows}}
	store := strictTestStore(acquireTx, time.Second)
	lease, decision, err := store.AcquireStrict(context.Background(), request)
	if err != nil || !decision.Allowed || lease.ID != "lease" {
		t.Fatalf("acquire=%+v,%+v,%v", lease, decision, err)
	}
	digest := sha256.Sum256([]byte(lease.ID))
	state := &persistedState{Schema: stateSchema, PolicyID: lease.PolicyID, Algorithm: ratelimit.Concurrency, Leases: map[string]persistedLease{hex.EncodeToString(digest[:]): {Cost: lease.Cost, ExpiresMicros: lease.ExpiresAt.UnixMicro()}}}
	stateRow := rowFunc(func(destinations ...any) error {
		*(destinations[0].(*[]byte)) = encodeState(state)
		*(destinations[1].(*time.Time)) = lease.ExpiresAt.Add(time.Second)
		return nil
	})
	releaseTx := &fakeTransaction{rows: []pgx.Row{setRow, lockRow, stateRow}}
	store = strictTestStore(releaseTx, time.Second)
	if err := store.ReleaseStrict(context.Background(), lease); err != nil {
		t.Fatalf("release=%v", err)
	}
	commitTx := &fakeTransaction{rows: []pgx.Row{setRow, lockRow, noRows}, commitErr: errors.New("commit")}
	store = strictTestStore(commitTx, time.Second)
	if _, err := store.AdmitStrict(context.Background(), postgresRequest(t)); !errors.Is(err, ratelimit.ErrOutcomeUnknown) {
		t.Fatalf("commit=%v", err)
	}
	writeTx := &fakeTransaction{rows: []pgx.Row{setRow, lockRow, noRows}, execErrs: []error{errors.New("write")}}
	store = strictTestStore(writeTx, time.Second)
	if _, err := store.AdmitStrict(context.Background(), postgresRequest(t)); err != ratelimit.ErrUnavailable { //nolint:errorlint // Safe precommit failures return the exact sentinel.
		t.Fatalf("write=%v", err)
	}
}
