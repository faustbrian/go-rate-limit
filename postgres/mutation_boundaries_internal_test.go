package postgres

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestConfigurationBoundariesAreIndependent(t *testing.T) {
	t.Parallel()

	executor := &fakeExecutor{}
	for _, timeout := range []time.Duration{0, -time.Nanosecond} {
		if _, err := newStore(executor, Options{Timeout: timeout}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
			t.Fatalf("newStore(timeout %s) error = %v", timeout, err)
		}
	}
	store, err := newStore(executor, Options{Timeout: time.Nanosecond, Clock: ServerClock})
	if err != nil || store.options.LockTimeout != time.Nanosecond {
		t.Fatalf("newStore(minimum valid) = %+v, %v", store, err)
	}
	if _, err := newStore(executor, Options{Timeout: time.Second, Clock: ClockPolicy(2)}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("newStore(invalid clock) error = %v", err)
	}
}

func TestStoreLeaseErrorClassificationsAreIndependent(t *testing.T) {
	t.Parallel()

	request := concurrencyLeaseRequest(t, time.Unix(10, 0), "lease", 1)
	executor := &fakeLeaseExecutor{fakeExecutor: &fakeExecutor{}}
	store, err := newStore(executor, Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	executor.lease = postgresLease(request, request.Request.Now.Add(time.Second))
	executor.decision = ratelimit.Decision{Allowed: true, Limit: 2, Remaining: 1, Reset: executor.lease.ExpiresAt, Reason: ratelimit.ReasonAllowed}
	lease, decision, err := store.Acquire(context.Background(), request)
	if err != nil || lease != executor.lease || decision != executor.decision {
		t.Fatalf("Acquire(success) = %+v, %+v, %v", lease, decision, err)
	}
	if err := store.Release(context.Background(), lease); err != nil {
		t.Fatalf("Release(success) error = %v", err)
	}
	for _, classified := range []error{ratelimit.ErrRejected, ratelimit.ErrLeaseNotOwned} {
		executor.leaseErr = classified
		if _, _, err := store.Acquire(context.Background(), request); !errors.Is(err, classified) {
			t.Fatalf("Acquire(%v) error = %v", classified, err)
		}
	}
	lease = postgresLease(request, request.Request.Now.Add(time.Second))
	for _, classified := range []error{ratelimit.ErrLeaseNotFound, ratelimit.ErrLeaseNotOwned} {
		executor.leaseErr = classified
		if err := store.Release(context.Background(), lease); !errors.Is(err, classified) {
			t.Fatalf("Release(%v) error = %v", classified, err)
		}
	}
}

func TestReleaseValidatesEveryOwnershipField(t *testing.T) {
	t.Parallel()

	request := concurrencyLeaseRequest(t, time.Unix(10, 0), "lease", 1)
	valid := postgresLease(request, request.Request.Now.Add(time.Second))
	store, err := newStore(&fakeLeaseExecutor{fakeExecutor: &fakeExecutor{}}, Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	invalid := []ratelimit.Lease{
		func() ratelimit.Lease { value := valid; value.ID = ""; return value }(),
		func() ratelimit.Lease { value := valid; value.PolicyID = ""; return value }(),
		func() ratelimit.Lease { value := valid; value.Key = ratelimit.Key{}; return value }(),
		func() ratelimit.Lease { value := valid; value.Cost = 0; return value }(),
	}
	for index, lease := range invalid {
		if err := store.Release(context.Background(), lease); !errors.Is(err, ratelimit.ErrInvalidRequest) {
			t.Fatalf("Release(invalid %d) error = %v", index, err)
		}
	}
}

func TestPersistedLeaseIdentityAndBudgetBoundaries(t *testing.T) {
	t.Parallel()

	request := concurrencyLeaseRequest(t, time.Unix(100, 0), "new", 1)
	valid := &persistedState{
		Schema: stateSchema, PolicyID: request.Request.Policy.ID(),
		Algorithm: ratelimit.Concurrency, Leases: map[string]persistedLease{},
	}
	corrupt := []*persistedState{
		{Schema: stateSchema + 1, PolicyID: valid.PolicyID, Algorithm: valid.Algorithm},
		{Schema: stateSchema, PolicyID: "other", Algorithm: valid.Algorithm},
		{Schema: stateSchema, PolicyID: valid.PolicyID, Algorithm: ratelimit.FixedWindow},
	}
	for index, state := range corrupt {
		if _, _, _, err := mutateLease(state, request, "new"); !errors.Is(err, ratelimit.ErrCorrupt) {
			t.Fatalf("mutateLease(corrupt %d) error = %v", index, err)
		}
	}
	expires := request.Request.Now.Add(time.Second).UnixMicro()
	existing := *valid
	digest := strings.Repeat("a", 64)
	existing.Leases = map[string]persistedLease{digest: {Cost: 2, ExpiresMicros: expires}}
	returned, lease, decision, err := mutateLease(&existing, request, digest)
	if returned != &existing || lease != (ratelimit.Lease{}) || decision != (ratelimit.Decision{}) ||
		!errors.Is(err, ratelimit.ErrLeaseNotOwned) {
		t.Fatalf("mutateLease(existing cost mismatch) = %+v, %+v, %+v, %v", returned, lease, decision, err)
	}
	for _, leases := range []map[string]persistedLease{
		{strings.Repeat("a", 64): {Cost: 0, ExpiresMicros: expires}},
		{strings.Repeat("a", 64): {Cost: ratelimit.MaxConcurrencyLeases + 1, ExpiresMicros: expires}},
		{
			strings.Repeat("a", 64): {Cost: ratelimit.MaxConcurrencyLeases, ExpiresMicros: expires},
			strings.Repeat("b", 64): {Cost: 1, ExpiresMicros: expires},
		},
		{
			strings.Repeat("a", 64): {Cost: 400, ExpiresMicros: expires},
			strings.Repeat("b", 64): {Cost: 400, ExpiresMicros: expires},
			strings.Repeat("c", 64): {Cost: 400, ExpiresMicros: expires},
		},
	} {
		state := *valid
		state.Leases = leases
		if _, _, _, err := mutateLease(&state, request, strings.Repeat("f", 64)); !errors.Is(err, ratelimit.ErrCorrupt) {
			t.Fatalf("mutateLease(invalid budget) error = %v", err)
		}
	}
	for _, leases := range []map[string]persistedLease{
		{strings.Repeat("a", 64): {Cost: ratelimit.MaxConcurrencyLeases, ExpiresMicros: expires}},
		{
			strings.Repeat("a", 64): {Cost: ratelimit.MaxConcurrencyLeases - 1, ExpiresMicros: expires},
			strings.Repeat("b", 64): {Cost: 1, ExpiresMicros: expires},
		},
	} {
		state := *valid
		state.Leases = leases
		if _, _, _, err := mutateLease(&state, request, strings.Repeat("f", 64)); !errors.Is(err, ratelimit.ErrRejected) {
			t.Fatalf("mutateLease(exact budget) error = %v", err)
		}
	}
	state := *valid
	state.Leases = map[string]persistedLease{
		strings.Repeat("a", 64): {Cost: 1, ExpiresMicros: expires + 10},
		strings.Repeat("b", 64): {Cost: 1, ExpiresMicros: expires},
	}
	request.Request.Cost = 1
	_, _, decision, err = mutateLease(&state, request, strings.Repeat("f", 64))
	if !errors.Is(err, ratelimit.ErrRejected) || decision.Reset.UnixMicro() != expires {
		t.Fatalf("mutateLease(earliest) = %+v, %v", decision, err)
	}
}

func TestLegacyLeaseMutationRetainsReleasedIdentityAndBudgetChecks(t *testing.T) {
	t.Parallel()

	request := concurrencyLeaseRequest(t, time.Unix(100, 0), "new", 1)
	expires := request.Request.Now.Add(time.Second).UnixMicro()
	base := persistedState{
		Schema: stateSchema, PolicyID: request.Request.Policy.ID(), Algorithm: ratelimit.Concurrency,
	}
	for _, state := range []*persistedState{
		{Schema: stateSchema + 1, PolicyID: base.PolicyID, Algorithm: base.Algorithm},
		{Schema: stateSchema, PolicyID: "other", Algorithm: base.Algorithm},
		{Schema: stateSchema, PolicyID: base.PolicyID, Algorithm: ratelimit.FixedWindow},
		{Schema: stateSchema, PolicyID: base.PolicyID, Algorithm: base.Algorithm, Leases: map[string]persistedLease{"zero": {Cost: 0, ExpiresMicros: expires}}},
		{Schema: stateSchema, PolicyID: base.PolicyID, Algorithm: base.Algorithm, Leases: map[string]persistedLease{"large": {Cost: ratelimit.MaxConcurrencyLeases + 1, ExpiresMicros: expires}}},
		{Schema: stateSchema, PolicyID: base.PolicyID, Algorithm: base.Algorithm, Leases: map[string]persistedLease{
			"full":  {Cost: ratelimit.MaxConcurrencyLeases, ExpiresMicros: expires},
			"extra": {Cost: 1, ExpiresMicros: expires},
		}},
	} {
		if _, _, _, err := mutateLeaseLegacy(state, request, "new"); !errors.Is(err, ratelimit.ErrCorrupt) {
			t.Fatalf("mutateLeaseLegacy(%+v) error = %v", state, err)
		}
	}

	valid := &persistedState{
		Schema: stateSchema, PolicyID: base.PolicyID, Algorithm: base.Algorithm,
		ObservedMicros: request.Request.Now.UnixMicro(),
		Leases: map[string]persistedLease{
			strings.Repeat("a", 64): {Cost: ratelimit.MaxConcurrencyLeases - 1, ExpiresMicros: expires},
			strings.Repeat("b", 64): {Cost: 1, ExpiresMicros: expires},
		},
	}
	if err := validateConcurrencyState(valid, base.PolicyID); err != nil {
		t.Fatalf("validateConcurrencyState(exact budget) error = %v", err)
	}
	invalid := *valid
	invalid.Leases = map[string]persistedLease{
		strings.Repeat("a", 64): {Cost: 400, ExpiresMicros: expires},
		strings.Repeat("b", 64): {Cost: 400, ExpiresMicros: expires},
		strings.Repeat("c", 64): {Cost: 400, ExpiresMicros: expires},
	}
	if err := validateConcurrencyState(&invalid, base.PolicyID); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("validateConcurrencyState(over budget) error = %v", err)
	}
	for _, state := range []*persistedState{
		{Schema: stateSchema + 1, PolicyID: base.PolicyID, Algorithm: base.Algorithm},
		{Schema: stateSchema, PolicyID: base.PolicyID, Algorithm: ratelimit.FixedWindow},
		{Schema: stateSchema, PolicyID: base.PolicyID, Algorithm: base.Algorithm, ObservedMicros: maxExactMicros + 1},
		{
			Schema: stateSchema, PolicyID: base.PolicyID, Algorithm: base.Algorithm,
			ObservedMicros: request.Request.Now.UnixMicro(),
			Leases: map[string]persistedLease{
				strings.Repeat("g", 64): {Cost: 1, ExpiresMicros: expires},
			},
		},
		{
			Schema: stateSchema, PolicyID: base.PolicyID, Algorithm: base.Algorithm,
			ObservedMicros: request.Request.Now.UnixMicro(),
			Leases: map[string]persistedLease{
				strings.Repeat("a", 64): {Cost: 1, ExpiresMicros: maxExactMicros + 1},
			},
		},
	} {
		if err := validateConcurrencyState(state, base.PolicyID); !errors.Is(err, ratelimit.ErrCorrupt) {
			t.Fatalf("validateConcurrencyState(%+v) error = %v", state, err)
		}
	}
}

func TestLegacyStateMutationDoesNotApplyStrictOnlyValidation(t *testing.T) {
	t.Parallel()

	now := time.Unix(100, 0).UTC()
	for _, algorithm := range []ratelimit.Algorithm{ratelimit.TokenBucket, ratelimit.FixedWindow, ratelimit.SlidingWindow} {
		policy := postgresPolicyForMutation(t, algorithm, 2, time.Minute)
		request := postgresTokenRequest(t, now, 1)
		request.Policy = policy
		state := &persistedState{
			Schema: stateSchema, PolicyID: policy.ID(), Revision: policy.Revision(), Algorithm: algorithm,
			Tokens: 1, LastMicros: now.UnixMicro(), ObservedMicros: now.UnixMicro(),
		}
		switch algorithm {
		case ratelimit.TokenBucket:
			state.Remainder = uint64(policy.Period().Microseconds())
		case ratelimit.FixedWindow:
			state.Window = maxExactMicros + 1
		case ratelimit.SlidingWindow:
			state.Segments[0] = persistedSegment{Index: -maxExactMicros - 1, Used: 1}
		case ratelimit.Concurrency:
			t.Fatal("unexpected concurrency algorithm")
		}
		if next, _, err := mutateStateLegacy(state, request); next == nil || errors.Is(err, ratelimit.ErrCorrupt) {
			t.Fatalf("mutateStateLegacy(%s) = %+v, %v", algorithm, next, err)
		}
	}

	policy := postgresPolicyForMutation(t, ratelimit.FixedWindow, 2, time.Minute)
	request := postgresTokenRequest(t, time.UnixMicro(-maxExactMicros-1), 1)
	request.Policy = policy
	if next, decision, err := mutateStateLegacy(nil, request); next == nil || !decision.Allowed || errors.Is(err, ratelimit.ErrOverflow) {
		t.Fatalf("legacy fixed out-of-range time = %+v, %+v, %v", next, decision, err)
	}
}

func TestLegacySlidingMutationPreservesSegmentAccumulation(t *testing.T) {
	t.Parallel()

	policy := postgresPolicyForMutation(t, ratelimit.SlidingWindow, 3, time.Second)
	request := postgresTokenRequest(t, time.Unix(20, 0), 1)
	request.Policy = policy
	state := &persistedState{}
	state.Segments[0] = persistedSegment{Index: 320, Used: 1}

	decision, err := mutateSlidingLegacy(state, request)
	if err != nil || !decision.Allowed || decision.Remaining != 1 ||
		state.Segments[0] != (persistedSegment{Index: 320, Used: 2}) || state.Used != 2 {
		t.Fatalf("mutateSlidingLegacy() = %+v, %+v, %v", state, decision, err)
	}
}

func TestStrictStateLoadDeletesExpiredRows(t *testing.T) {
	t.Parallel()

	now := time.Unix(100, 0).UTC()
	state := &persistedState{
		Schema: stateSchema, PolicyID: "expired", Revision: "v1", Algorithm: ratelimit.FixedWindow,
	}
	tx := &fakeTransaction{rows: []pgx.Row{rowFunc(func(destinations ...any) error {
		*(destinations[0].(*[]byte)) = encodeState(state)
		*(destinations[1].(*time.Time)) = now
		return nil
	})}}
	loaded, err := loadStateStrict(context.Background(), tx, []byte("key"), now)
	if err != nil || loaded != nil || len(tx.execQueries) != 1 || tx.execQueries[0] != deleteStateSQL {
		t.Fatalf("loadStateStrict(expired) = %+v, %v, queries=%v", loaded, err, tx.execQueries)
	}
}

func TestStrictStateLoadsBoundEncodedBytesBeforeScan(t *testing.T) {
	t.Parallel()

	if maxEncodedStateBytes != 131_072 {
		t.Fatalf("maxEncodedStateBytes = %d, want 131072", maxEncodedStateBytes)
	}
	now := time.Unix(100, 0).UTC()
	for _, test := range []struct {
		name string
		load func(context.Context, nativeTransaction, []byte, time.Time) (*persistedState, error)
	}{
		{name: "admit", load: loadStateStrict},
		{name: "release", load: func(ctx context.Context, tx nativeTransaction, key []byte, _ time.Time) (*persistedState, error) {
			return loadStateForReleaseStrict(ctx, tx, key)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			stateRow := rowFunc(func(destinations ...any) error {
				*(destinations[0].(*[]byte)) = []byte(`{}`)
				*(destinations[1].(*time.Time)) = now.Add(time.Second)
				return nil
			})
			tx := &fakeTransaction{rows: []pgx.Row{strictStateRow{row: stateRow, withinLimit: false}}}
			if _, err := test.load(context.Background(), tx, []byte("key"), now); !errors.Is(err, ratelimit.ErrCorrupt) {
				t.Fatalf("load error = %v", err)
			}
			if len(tx.queryArgs) != 1 || len(tx.queryArgs[0]) != 2 || tx.queryArgs[0][1] != maxEncodedStateBytes {
				t.Fatalf("strict state query args = %#v", tx.queryArgs)
			}
		})
	}
}

func TestPersistedStateIdentityBoundaries(t *testing.T) {
	t.Parallel()

	request := postgresRequest(t)
	states := []*persistedState{
		{Schema: stateSchema + 1, PolicyID: request.Policy.ID(), Algorithm: request.Policy.Algorithm()},
		{Schema: stateSchema, PolicyID: "other", Algorithm: request.Policy.Algorithm()},
		{Schema: stateSchema, PolicyID: request.Policy.ID(), Algorithm: ratelimit.TokenBucket},
	}
	for index, state := range states {
		if _, _, err := mutateState(state, request); !errors.Is(err, ratelimit.ErrCorrupt) {
			t.Fatalf("mutateState(corrupt %d) error = %v", index, err)
		}
	}
}

func TestPersistedTokenExactArithmeticBoundaries(t *testing.T) {
	t.Parallel()

	start := time.Unix(0, 0)
	widePolicy := postgresPolicyForMutation(t, ratelimit.TokenBucket, 9_000_000_000_000_000, time.Microsecond)
	request := postgresTokenRequest(t, start.Add(2050*time.Microsecond), 1)
	request.Policy = widePolicy
	state := &persistedState{LastMicros: start.UnixMicro()}
	decision, err := mutateToken(state, request)
	if err != nil || state.Tokens != widePolicy.Limit()-1 || state.Remainder != 0 || !decision.Allowed {
		t.Fatalf("wide mutateToken() = %+v, %+v, %v", state, decision, err)
	}

	exactPolicy := postgresPolicyForMutation(t, ratelimit.TokenBucket, 3, time.Second)
	request.Policy = exactPolicy
	request.Now = start.Add(1100 * time.Millisecond)
	state = &persistedState{LastMicros: start.UnixMicro()}
	if _, err := mutateToken(state, request); err != nil || state.Tokens != 2 || state.Remainder != 0 {
		t.Fatalf("exact mutateToken() = %+v, %v", state, err)
	}
	state = &persistedState{Tokens: 1, LastMicros: start.UnixMicro()}
	request.Now = start.Add(700 * time.Millisecond)
	if _, err := mutateToken(state, request); err != nil || state.Tokens != 2 || state.Remainder != 0 {
		t.Fatalf("nonzero exact-gap mutateToken() = %+v, %v", state, err)
	}

	state = &persistedState{Tokens: 1, LastMicros: request.Now.UnixMicro()}
	request.Cost = 1
	decision, err = mutateToken(state, request)
	if err != nil || !decision.Allowed || decision.Remaining != 0 ||
		!decision.Reset.Equal(request.Now.Add(time.Second)) {
		t.Fatalf("exact-cost mutateToken() = %+v, %+v, %v", state, decision, err)
	}

	state = &persistedState{Tokens: exactPolicy.Limit(), LastMicros: start.UnixMicro(), Remainder: 7}
	request.Now = start.Add(time.Second)
	if _, err := mutateToken(state, request); err != nil || state.LastMicros != request.Now.UnixMicro() || state.Remainder != 7 {
		t.Fatalf("full mutateToken() = %+v, %v", state, err)
	}

	microPolicy := postgresPolicyForMutation(t, ratelimit.TokenBucket, 1, 2*time.Microsecond)
	request.Policy = microPolicy
	request.Now = start.Add(time.Microsecond)
	request.Cost = 1
	state = &persistedState{LastMicros: start.UnixMicro()}
	decision, err = mutateToken(state, request)
	if !errors.Is(err, ratelimit.ErrRejected) || decision.RetryAfter != time.Microsecond ||
		state.Remainder != 1 || state.LastMicros != request.Now.UnixMicro() {
		t.Fatalf("one-microsecond mutateToken() = %+v, %+v, %v", state, decision, err)
	}

	request.Policy = exactPolicy
	request.Now = start
	request.Cost = 2
	state = &persistedState{Tokens: 1, LastMicros: start.UnixMicro()}
	decision, err = mutateToken(state, request)
	if !errors.Is(err, ratelimit.ErrRejected) || decision.RetryAfter != 333334*time.Microsecond ||
		decision.Reset.Sub(request.Now) != 666667*time.Microsecond {
		t.Fatalf("partially-empty rejection = %+v, %v", decision, err)
	}
	request.Cost = 1
	state = &persistedState{Tokens: 2, LastMicros: start.UnixMicro()}
	decision, err = mutateToken(state, request)
	if err != nil || decision.Remaining != 1 || decision.Reset.Sub(request.Now) != 666667*time.Microsecond {
		t.Fatalf("partially-empty allowance = %+v, %v", decision, err)
	}
}

func TestPersistedTokenDurationBoundaries(t *testing.T) {
	t.Parallel()

	policy := postgresPolicyForMutation(t, ratelimit.TokenBucket, 1, 2*time.Microsecond)
	if got := tokenDuration(math.MaxUint64, 0, policy); got != time.Duration(math.MaxInt64) {
		t.Fatalf("high tokenDuration() = %s", got)
	}
	policy = postgresPolicyForMutation(t, ratelimit.TokenBucket, 1, time.Microsecond)
	micros := uint64(math.MaxInt64 / microsecondNanos)
	want := time.Duration(micros * uint64(microsecondNanos))
	if got := tokenDuration(micros, 0, policy); got != want {
		t.Fatalf("exact tokenDuration() = %s, want %s", got, want)
	}
}

func TestPersistedWindowAndSignedBoundaries(t *testing.T) {
	t.Parallel()
	if !validPersistedMicros(-maxExactMicros) || !validPersistedMicros(maxExactMicros) ||
		validPersistedMicros(-maxExactMicros-1) || validPersistedMicros(maxExactMicros+1) {
		t.Fatal("persisted microsecond boundary diverged")
	}

	request := postgresRequest(t)
	state := &persistedState{Window: floor(request.Now.UnixMicro(), request.Policy.Period().Microseconds()), Used: 2}
	decision, err := mutateFixed(state, request)
	wantReset := time.UnixMicro(state.Window + request.Policy.Period().Microseconds())
	if err != nil || state.Used != 3 || decision.Remaining != request.Policy.Limit()-3 ||
		!decision.Reset.Equal(wantReset) {
		t.Fatalf("same-window mutateFixed() = %+v, %+v, %v", state, decision, err)
	}
	if floor(-10, 10) != -10 || floor(0, 10) != 0 || positiveRemainder(0, 16) != 0 {
		t.Fatal("signed helper boundary diverged")
	}
}

func TestValidateServerClockRangeBoundaries(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		now      time.Time
		duration time.Duration
		want     error
	}{
		{name: "exact lower", now: time.UnixMicro(-maxExactMicros), duration: time.Microsecond},
		{name: "below lower", now: time.UnixMicro(-maxExactMicros - 1), duration: time.Microsecond, want: ratelimit.ErrOverflow},
		{name: "zero duration", now: time.UnixMicro(0), duration: 0, want: ratelimit.ErrOverflow},
		{name: "negative duration", now: time.UnixMicro(0), duration: -time.Microsecond, want: ratelimit.ErrOverflow},
		{name: "exact sum", now: time.UnixMicro(maxExactMicros - 1), duration: time.Microsecond},
		{name: "sum overflow", now: time.UnixMicro(maxExactMicros), duration: time.Microsecond, want: ratelimit.ErrOverflow},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateServerClockRange(test.now, test.duration)
			if !errors.Is(err, test.want) || test.want == nil && err != nil {
				t.Fatalf("validateServerClockRange(%s, %s) = %v, want %v", test.now, test.duration, err, test.want)
			}
		})
	}
}

func TestPersistedSlidingWindowArithmetic(t *testing.T) {
	t.Parallel()

	policy := postgresPolicyForMutation(t, ratelimit.SlidingWindow, 3, time.Second)
	request := postgresTokenRequest(t, time.Unix(20, 0), 1)
	request.Policy = policy
	fresh, freshDecision, err := mutateState(nil, request)
	if err != nil || !freshDecision.Allowed || freshDecision.Remaining != 2 || !freshDecision.Reset.Equal(request.Now.Add(time.Second)) ||
		fresh.Used != 1 || fresh.Segments[0].Index != 320 || fresh.Segments[0].Used != 1 {
		t.Fatalf("fresh mutateState(sliding) = %+v, %+v, %v", fresh, freshDecision, err)
	}
	state := &persistedState{}
	state.Segments[0] = persistedSegment{Index: 320, Used: 1}
	state.Segments[1] = persistedSegment{Index: 305, Used: 1}
	state.Segments[2] = persistedSegment{Index: 304, Used: 2}
	decision, err := mutateSliding(state, request)
	if err != nil || !decision.Allowed || decision.Remaining != 0 {
		t.Fatalf("mutateSliding() = %+v, %v", decision, err)
	}
	if state.Segments[0] != (persistedSegment{Index: 320, Used: 2}) ||
		state.Segments[2] != (persistedSegment{}) || state.Used != 3 {
		t.Fatalf("sliding state = %+v", state)
	}
	wantReset := time.UnixMicro(306 * 62_500).Add(time.Second)
	if !decision.Reset.Equal(wantReset) {
		t.Fatalf("sliding reset = %s, want %s", decision.Reset, wantReset)
	}
}

func TestPersistedCodecIdentityFieldsAreIndependent(t *testing.T) {
	t.Parallel()

	for _, encoded := range [][]byte{
		[]byte(`{"schema":2,"policy_id":"x","algorithm":"fixed_window"}`),
		[]byte(`{"schema":1,"algorithm":"fixed_window"}`),
		[]byte(`{"schema":1,"policy_id":"x"}`),
	} {
		if _, err := decodeState(encoded); !errors.Is(err, ratelimit.ErrCorrupt) {
			t.Fatalf("decodeState(%s) error = %v", encoded, err)
		}
	}
	valid := encodeState(&persistedState{
		Schema: stateSchema, PolicyID: "x", Algorithm: ratelimit.FixedWindow,
	})
	if _, err := decodeState(valid); err != nil {
		t.Fatalf("decodeState(valid) error = %v", err)
	}
}

func TestNativeLeaseUsesServerClockTTLAndReleaseOwnership(t *testing.T) {
	t.Parallel()

	request := concurrencyLeaseRequest(t, time.Unix(100, 0), "lease", 1)
	serverNow := request.Request.Now.Add(time.Second)
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
	tx := &fakeTransaction{rows: []pgx.Row{setRow, lockRow, noRows}}
	executor := &nativeExecutor{database: &fakeDatabase{tx: tx}, options: Options{
		Timeout: time.Second, LockTimeout: time.Second, Clock: ServerClock,
	}}
	lease, _, err := executor.acquire(context.Background(), make([]byte, 32), request, "digest")
	if err != nil || !lease.ExpiresAt.Equal(serverNow.Add(time.Second)) {
		t.Fatalf("server acquire() lease = %+v, %v", lease, err)
	}
	if len(tx.execArgs) != 1 || !tx.execArgs[0][2].(time.Time).Equal(serverNow.Add(2*time.Second)) ||
		!tx.execArgs[0][3].(time.Time).Equal(serverNow) {
		t.Fatalf("server acquire() args = %+v", tx.execArgs)
	}

	digest := "digest"
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
	wrongAlgorithm := *base
	wrongAlgorithm.Algorithm = ratelimit.FixedWindow
	forgedExpiry := lease
	forgedExpiry.ExpiresAt = forgedExpiry.ExpiresAt.Add(time.Microsecond)
	for _, test := range []struct {
		name  string
		state *persistedState
		lease ratelimit.Lease
		want  error
	}{
		{name: "algorithm", state: &wrongAlgorithm, lease: lease, want: ratelimit.ErrLeaseNotFound},
		{name: "expiry", state: base, lease: forgedExpiry, want: ratelimit.ErrLeaseNotOwned},
	} {
		tx := &fakeTransaction{rows: []pgx.Row{setRow, lockRow, stateRow(test.state)}}
		executor := &nativeExecutor{database: &fakeDatabase{tx: tx}, options: Options{Timeout: time.Second, LockTimeout: time.Second}}
		if err := executor.release(context.Background(), make([]byte, 32), test.lease, digest); !errors.Is(err, test.want) {
			t.Fatalf("release(%s) error = %v", test.name, err)
		}
	}
	tx = &fakeTransaction{rows: []pgx.Row{setRow, lockRow, stateRow(base)}}
	executor = &nativeExecutor{database: &fakeDatabase{tx: tx}, options: Options{Timeout: time.Second, LockTimeout: time.Second}}
	if err := executor.release(context.Background(), make([]byte, 32), lease, digest); err != nil {
		t.Fatalf("release(last) error = %v", err)
	}
	if len(tx.execQueries) != 1 || tx.execQueries[0] != deleteStateSQL {
		t.Fatalf("release(last) queries = %v", tx.execQueries)
	}
}

func TestNativeConfigurationAndCleanupBoundaries(t *testing.T) {
	t.Parallel()

	pool := &pgxpool.Pool{}
	store, err := New(pool, Options{Timeout: time.Second, LockTimeout: time.Nanosecond})
	if err != nil || store.options.LockTimeout != time.Nanosecond {
		t.Fatalf("New(explicit lock) = %+v, %v", store, err)
	}
	database := &fakeDatabase{row: rowFunc(func(destinations ...any) error {
		*(destinations[0].(*int64)) = 0
		return nil
	})}
	store, err = newStore(&nativeExecutor{database: database, options: Options{Timeout: time.Second}}, Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Cleanup(context.Background(), MaxCleanupBatch); err != nil {
		t.Fatalf("Cleanup(maximum) error = %v", err)
	}
}

func TestNativeAdmissionUsesServerClockAndDoublePeriodTTL(t *testing.T) {
	t.Parallel()

	request := postgresRequest(t)
	serverNow := request.Now.Add(time.Second)
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
	tx := &fakeTransaction{rows: []pgx.Row{setRow, lockRow, noRows}}
	executor := &nativeExecutor{database: &fakeDatabase{tx: tx}, options: Options{
		Timeout: time.Second, LockTimeout: time.Second, Clock: ServerClock,
	}}
	decision, err := executor.admit(context.Background(), make([]byte, 32), request)
	if err != nil || !decision.Allowed {
		t.Fatalf("server admit() = %+v, %v", decision, err)
	}
	if len(tx.execArgs) != 1 ||
		!tx.execArgs[0][2].(time.Time).Equal(serverNow.Add(2*request.Policy.Period())) ||
		!tx.execArgs[0][3].(time.Time).Equal(serverNow) {
		t.Fatalf("server admit() args = %+v", tx.execArgs)
	}
}

func TestLatestLeaseExpiryChoosesMaximum(t *testing.T) {
	t.Parallel()

	if got := latestLeaseExpiry(map[string]persistedLease{
		"early": {ExpiresMicros: 1}, "late": {ExpiresMicros: 2},
	}); got.UnixMicro() != 2 {
		t.Fatalf("latestLeaseExpiry() = %s", got)
	}
}

func postgresPolicyForMutation(t *testing.T, algorithm ratelimit.Algorithm, capacity uint64, period time.Duration) ratelimit.Policy {
	t.Helper()
	spec := ratelimit.PolicySpec{
		ID: "mutation", Revision: "v1", Algorithm: algorithm,
		Capacity: capacity, Period: period, MaxCost: 1,
		Consistency: ratelimit.ConsistencyStrong,
	}
	policy, err := ratelimit.NewPolicy(spec)
	if err != nil {
		t.Fatal(err)
	}
	return policy
}
