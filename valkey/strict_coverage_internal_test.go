package valkey

import (
	"context"
	"errors"
	"math"
	"strconv"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
)

func TestCheckStrictPreservesCancellationDuringPreflight(t *testing.T) {
	t.Run("info cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		native := &nativeExecutor{
			info: func(context.Context) (string, error) {
				cancel()
				return "", context.Canceled
			},
		}
		store, err := newStore(native, Options{Prefix: "strict", Timeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.CheckStrict(ctx); !errors.Is(err, ratelimit.ErrCanceled) || errors.Is(err, ratelimit.ErrUnavailable) {
			t.Fatalf("CheckStrict() error = %v, want only canceled", err)
		}
	})

	t.Run("info deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		native := &nativeExecutor{
			info: func(ctx context.Context) (string, error) {
				<-ctx.Done()
				return "", ctx.Err()
			},
		}
		store, err := newStore(native, Options{Prefix: "strict", Timeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.CheckStrict(ctx); !errors.Is(err, ratelimit.ErrDeadline) || errors.Is(err, ratelimit.ErrUnavailable) {
			t.Fatalf("CheckStrict() error = %v, want only deadline", err)
		}
	})

	t.Run("config cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		native := &nativeExecutor{
			info: func(context.Context) (string, error) { return "valkey_version:9.0.0", nil },
			config: func(context.Context) (map[string]string, error) {
				cancel()
				return nil, context.Canceled
			},
		}
		store, err := newStore(native, Options{Prefix: "strict", Timeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.CheckStrict(ctx); !errors.Is(err, ratelimit.ErrCanceled) || errors.Is(err, ratelimit.ErrUnavailable) {
			t.Fatalf("CheckStrict() error = %v, want only canceled", err)
		}
	})

	t.Run("config deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		native := &nativeExecutor{
			info: func(context.Context) (string, error) { return "valkey_version:9.0.0", nil },
			config: func(ctx context.Context) (map[string]string, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}
		store, err := newStore(native, Options{Prefix: "strict", Timeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.CheckStrict(ctx); !errors.Is(err, ratelimit.ErrDeadline) || errors.Is(err, ratelimit.ErrUnavailable) {
			t.Fatalf("CheckStrict() error = %v, want only deadline", err)
		}
	})
}

func TestCheckStrictNormalizesDirectPredispatchErrors(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want error
	}{
		{name: "canceled", err: context.Canceled, want: ratelimit.ErrCanceled},
		{name: "deadline", err: context.DeadlineExceeded, want: ratelimit.ErrDeadline},
	} {
		t.Run(test.name, func(t *testing.T) {
			native := &nativeExecutor{info: func(context.Context) (string, error) { return "", test.err }}
			store, err := newStore(native, Options{Prefix: "strict", Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
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
		native := &nativeExecutor{info: func(context.Context) (string, error) { return "", backendErr }}
		store, err := newStore(native, Options{Prefix: "strict", Timeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		if got := store.CheckStrict(context.Background()); got != backendErr { //nolint:errorlint // Direct BackendError identity is preserved.
			t.Fatalf("CheckStrict(%v) = %v, want original safe backend error", category, got)
		}
	}
	for _, category := range []error{
		ratelimit.ErrRejected, ratelimit.ErrUnavailable, ratelimit.ErrOverflow,
		ratelimit.ErrCorrupt, ratelimit.ErrUnsupported, ratelimit.ErrLeaseNotFound,
		ratelimit.ErrLeaseNotOwned,
	} {
		if got := strictValkeyPredispatchCategory(context.Background(), category); got != category { //nolint:errorlint // Direct safe sentinel identity is the contract.
			t.Fatalf("strictValkeyPredispatchCategory(%v) = %v", category, got)
		}
	}
	for _, input := range []error{
		ratelimit.ErrCorrupt,
		new(ratelimit.BackendError),
		(*ratelimit.BackendError)(nil),
	} {
		native := &nativeExecutor{info: func(context.Context) (string, error) { return "", input }}
		store, err := newStore(native, Options{Prefix: "strict", Timeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		want := input
		if input != ratelimit.ErrCorrupt { //nolint:errorlint // Selects the one exact safe sentinel from invalid BackendError cases.
			want = ratelimit.ErrUnavailable
		}
		if got := store.CheckStrict(context.Background()); got != want { //nolint:errorlint // Direct safe errors preserve exact identity.
			t.Fatalf("CheckStrict direct/invalid safe error was not normalized")
		}
	}
}

func TestValkeyStateTTLExactSaturationBoundary(t *testing.T) {
	boundary := time.Duration(math.MaxInt64) / 2
	if got := valkeyStateTTL(boundary); got != boundary*2 {
		t.Fatalf("boundary TTL = %v, want %v", got, boundary*2)
	}
	if got := valkeyStateTTL(boundary + 1); got != time.Duration(math.MaxInt64) {
		t.Fatalf("overflow TTL = %v, want saturation", got)
	}
}

func TestValkeySaturatesStateTTL(t *testing.T) {
	overflowingMicrosecondDuration := time.Duration((math.MaxInt64/2/int64(time.Microsecond) + 1) * int64(time.Microsecond))
	wantMilliseconds := strconv.FormatInt(time.Duration(math.MaxInt64).Milliseconds(), 10)

	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "long-period", Revision: "v1", Algorithm: ratelimit.FixedWindow,
		Capacity: 1, Period: overflowingMicrosecondDuration, MaxCost: 1, FailureMode: ratelimit.FailClosed,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := valkeyRequest(t)
	request.Policy = policy
	request.Cost = 1
	legacyExecutor := &fakeExecutor{}
	legacyStore, err := newStore(legacyExecutor, Options{Prefix: "legacy", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = legacyStore.Admit(context.Background(), request)
	if got := legacyExecutor.args[10]; got != "1000" {
		t.Fatalf("legacy admission TTL = %q, want released overflow behavior", got)
	}

	executor := &fakeExecutor{}
	store, err := newStore(executor, Options{Prefix: "strict", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = store.AdmitStrict(context.Background(), request)
	if got := executor.args[10]; got != wantMilliseconds {
		t.Fatalf("strict admission TTL = %q, want %q", got, wantMilliseconds)
	}

	leasePolicy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "long-lease", Revision: "v1", Algorithm: ratelimit.Concurrency,
		Capacity: 1, Lease: overflowingMicrosecondDuration, MaxCost: 1, FailureMode: ratelimit.FailClosed,
	})
	if err != nil {
		t.Fatal(err)
	}
	leaseRequest := edgeLeaseRequest(t)
	leaseRequest.Request.Policy = leasePolicy
	leaseExecutor := &fullExecutor{}
	leaseStore, err := newStore(leaseExecutor, Options{Prefix: "strict", Timeout: time.Second, Clock: ServerClock})
	if err != nil {
		t.Fatal(err)
	}
	_, _, _ = leaseStore.AcquireStrict(context.Background(), leaseRequest)
	if got := leaseExecutor.acquireArgs[7]; got != wantMilliseconds {
		t.Fatalf("strict lease TTL = %q, want %q", got, wantMilliseconds)
	}
	if got := leaseExecutor.acquireArgs[8]; got != "1" {
		t.Fatalf("strict lease server clock = %q, want 1", got)
	}
}

func TestStrictValkeyOpenAndDispatchBoundaries(t *testing.T) {
	options := Options{Prefix: "strict", Timeout: time.Second}
	if store, err := OpenStrict(context.Background(), nil, options); store != nil || !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("OpenStrict(nil) = %+v, %v", store, err)
	}
	if store, err := openStrictChecked(context.Background(), func() (*Store, error) { return nil, ratelimit.ErrInvalidPolicy }); store != nil || !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("open construction failure = %+v, %v", store, err)
	}
	native := &nativeExecutor{
		info: func(context.Context) (string, error) { return "valkey_version:9.0.0", nil },
		config: func(context.Context) (map[string]string, error) {
			return map[string]string{"maxmemory-policy": "noeviction"}, nil
		},
	}
	store, err := newStore(native, options)
	if err != nil {
		t.Fatal(err)
	}
	if opened, err := openStrictChecked(context.Background(), func() (*Store, error) { return store, nil }); opened != store || err != nil {
		t.Fatalf("open success = %+v, %v", opened, err)
	}
	native.info = func(context.Context) (string, error) { return "", errors.New("offline") }
	if opened, err := openStrictChecked(context.Background(), func() (*Store, error) { return store, nil }); opened != nil || !errors.Is(err, ratelimit.ErrUnavailable) {
		t.Fatalf("open check failure = %+v, %v", opened, err)
	}

	admissionOnly, _ := newStore(&fakeExecutor{}, options)
	concurrency := edgeLeaseRequest(t).Request
	if _, err := admissionOnly.AdmitStrict(context.Background(), concurrency); !errors.Is(err, ratelimit.ErrUnsupported) {
		t.Fatalf("concurrency admit = %v", err)
	}
	if _, _, err := admissionOnly.AcquireStrict(context.Background(), edgeLeaseRequest(t)); !errors.Is(err, ratelimit.ErrUnsupported) {
		t.Fatalf("unsupported acquire = %v", err)
	}
	full, _ := newStore(&fullExecutor{}, options)
	lease := ratelimit.Lease{ID: "lease", PolicyID: "policy", Key: concurrency.Key, Cost: 1, ExpiresAt: time.Unix(2, 0)}
	for index, invalid := range []ratelimit.Lease{
		{PolicyID: lease.PolicyID, Key: lease.Key, Cost: lease.Cost, ExpiresAt: lease.ExpiresAt},
		{ID: lease.ID, Key: lease.Key, Cost: lease.Cost, ExpiresAt: lease.ExpiresAt},
		{ID: lease.ID, PolicyID: lease.PolicyID, Cost: lease.Cost, ExpiresAt: lease.ExpiresAt},
		{ID: lease.ID, PolicyID: lease.PolicyID, Key: lease.Key, ExpiresAt: lease.ExpiresAt},
		{ID: lease.ID, PolicyID: lease.PolicyID, Key: lease.Key, Cost: lease.Cost},
	} {
		if err := full.ReleaseStrict(context.Background(), invalid); !errors.Is(err, ratelimit.ErrInvalidRequest) {
			t.Fatalf("invalid release %d = %v", index, err)
		}
	}
	if err := admissionOnly.ReleaseStrict(context.Background(), lease); !errors.Is(err, ratelimit.ErrUnsupported) {
		t.Fatalf("unsupported release = %v", err)
	}

	deadline, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if _, err := full.AdmitStrict(deadline, valkeyRequest(t)); !errors.Is(err, ratelimit.ErrDeadline) {
		t.Fatalf("deadline admit = %v", err)
	}
	if _, _, err := full.AcquireStrict(deadline, edgeLeaseRequest(t)); !errors.Is(err, ratelimit.ErrDeadline) {
		t.Fatalf("deadline acquire = %v", err)
	}
	if err := full.ReleaseStrict(deadline, lease); !errors.Is(err, ratelimit.ErrDeadline) {
		t.Fatalf("deadline release = %v", err)
	}
	if err := store.CheckStrict(deadline); !errors.Is(err, ratelimit.ErrDeadline) {
		t.Fatalf("deadline check = %v", err)
	}
}

func TestStrictValkeyConstructorOwnership(t *testing.T) {
	t.Parallel()

	options := Options{Prefix: "strict", Timeout: time.Second}
	callerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	infoCalls, configCalls := 0, 0
	var infoCtx, configCtx context.Context
	native := &nativeExecutor{
		info: func(ctx context.Context) (string, error) {
			infoCalls++
			infoCtx = ctx
			return "valkey_version:9.0.0", nil
		},
		config: func(ctx context.Context) (map[string]string, error) {
			configCalls++
			configCtx = ctx
			return map[string]string{"maxmemory-policy": "noeviction"}, nil
		},
	}
	store, err := newStore(native, options)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := openStrictChecked(callerCtx, func() (*Store, error) { return store, nil })
	if err != nil || opened != store || infoCalls != 1 || configCalls != 1 || infoCtx != callerCtx || configCtx != callerCtx {
		t.Fatalf("successful open = %+v, %v, info=%d, config=%d, contexts=%v/%v", opened, err, infoCalls, configCalls, infoCtx == callerCtx, configCtx == callerCtx)
	}

	infoCalls, configCalls = 0, 0
	native.info = func(context.Context) (string, error) { infoCalls++; return "", errors.New("offline") }
	if failed, openErr := openStrictChecked(callerCtx, func() (*Store, error) { return store, nil }); failed != nil || !errors.Is(openErr, ratelimit.ErrUnavailable) || infoCalls != 1 || configCalls != 0 {
		t.Fatalf("failed open = %+v, %v, info=%d, config=%d", failed, openErr, infoCalls, configCalls)
	}

	panicValue := &struct{ owner string }{owner: "caller"}
	native.info = func(context.Context) (string, error) { panic(panicValue) }
	func() {
		defer func() {
			if recovered := recover(); recovered != panicValue {
				t.Fatalf("open panic = %v", recovered)
			}
		}()
		_, _ = openStrictChecked(callerCtx, func() (*Store, error) { return store, nil })
	}()
}

func TestStrictValkeyReplyCategoriesAndCorruption(t *testing.T) {
	for _, test := range []struct {
		input error
		want  error
	}{
		{nil, nil},
		{ratelimit.ErrRejected, ratelimit.ErrRejected},
		{ratelimit.ErrOverflow, ratelimit.ErrOverflow},
		{ratelimit.ErrCorrupt, ratelimit.ErrCorrupt},
		{ratelimit.ErrLeaseNotFound, ratelimit.ErrLeaseNotFound},
		{ratelimit.ErrLeaseNotOwned, ratelimit.ErrLeaseNotOwned},
		{errors.New("unexpected"), ratelimit.ErrCorrupt},
	} {
		if got := strictValkeyCategory(test.input); !errors.Is(got, test.want) || test.want == nil && got != nil {
			t.Fatalf("strictValkeyCategory(%v) = %v", test.input, got)
		}
	}

	decisionReplies := []struct {
		reply []string
		want  error
	}{
		{reply: []string{"-1", "0", "0", "0", "0", "overflow"}, want: ratelimit.ErrOverflow},
		{reply: []string{"-1", "0", "0", "0", "0", "corrupt"}, want: ratelimit.ErrCorrupt},
		{reply: []string{"1", "0", "1", "1", "0", "allowed"}, want: ratelimit.ErrOutcomeUnknown},
		{reply: []string{"1", "0", "1", "1", "0", "allowed", "bad-effective"}, want: ratelimit.ErrOutcomeUnknown},
		{reply: []string{"x", "0", "1", "1", "0", "allowed", "1"}, want: ratelimit.ErrOutcomeUnknown},
	}
	for _, test := range decisionReplies {
		decision, err := decodeStrictDecisionReply(test.reply)
		if decision != (ratelimit.Decision{}) || !errors.Is(err, test.want) || test.want != ratelimit.ErrOutcomeUnknown && errors.Is(err, ratelimit.ErrOutcomeUnknown) { //nolint:errorlint // Known reply categories must remain distinct from unknown.
			t.Fatalf("decodeStrictDecisionReply(%q) = %+v, %v; want %v", test.reply, decision, err, test.want)
		}
	}

	request := edgeLeaseRequest(t)
	rejectedReply := []string{"0", "0", "2", "2000000", "1000000", "limited", "0", "1000000"}
	lease, decision, err := decodeStrictLeaseReply(rejectedReply, request)
	if lease != (ratelimit.Lease{}) || !errors.Is(err, ratelimit.ErrRejected) || decision.Allowed || decision.Reason != ratelimit.ReasonLimited {
		t.Fatalf("decodeStrictLeaseReply(rejected) = %+v, %+v, %v", lease, decision, err)
	}
	leaseReplies := []struct {
		reply []string
		want  error
	}{
		{reply: []string{"-1", "0", "0", "0", "0", "overflow", "0"}, want: ratelimit.ErrOverflow},
		{reply: []string{"-1", "0", "0", "0", "0", "not_found", "0"}, want: ratelimit.ErrLeaseNotFound},
		{reply: []string{"-1", "0", "0", "0", "0", "not_owned", "0"}, want: ratelimit.ErrLeaseNotOwned},
		{reply: []string{"-1", "0", "0", "0", "0", "corrupt", "0"}, want: ratelimit.ErrCorrupt},
		{reply: []string{"-1", "0", "0", "0", "0", "unknown", "0"}, want: ratelimit.ErrOutcomeUnknown},
		{reply: []string{"1", "0", "1", "2", "0", "allowed", "2"}, want: ratelimit.ErrOutcomeUnknown},
		{reply: []string{"1"}, want: ratelimit.ErrOutcomeUnknown},
		{reply: []string{"1", "0", "1", "2", "0", "allowed", "2", "bad-effective"}, want: ratelimit.ErrOutcomeUnknown},
		{reply: []string{"x", "0", "1", "2", "0", "allowed", "2", "1"}, want: ratelimit.ErrOutcomeUnknown},
		{reply: []string{"1", "0", "1", "500000", "0", "allowed", "500000", "1000000"}, want: ratelimit.ErrOutcomeUnknown},
	}
	for _, test := range leaseReplies {
		lease, decision, err := decodeStrictLeaseReply(test.reply, request)
		if lease != (ratelimit.Lease{}) || decision != (ratelimit.Decision{}) || !errors.Is(err, test.want) || test.want != ratelimit.ErrOutcomeUnknown && errors.Is(err, ratelimit.ErrOutcomeUnknown) { //nolint:errorlint // Known reply categories must remain distinct from unknown.
			t.Fatalf("decodeStrictLeaseReply(%q) = %+v, %+v, %v; want %v", test.reply, lease, decision, err, test.want)
		}
	}
}
