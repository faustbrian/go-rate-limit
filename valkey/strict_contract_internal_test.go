package valkey

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
	valkeygo "github.com/valkey-io/valkey-go"
)

type nilContext struct{}

func (*nilContext) Deadline() (time.Time, bool) { panic("called") }
func (*nilContext) Done() <-chan struct{}       { panic("called") }
func (*nilContext) Err() error                  { panic("called") }
func (*nilContext) Value(any) any               { panic("called") }

type clientWrapper struct{ valkeygo.Client }

func TestStrictValkeyRejectsAuthorityRelativeInvalidTimesAsUnknown(t *testing.T) {
	for _, test := range []struct {
		name      string
		clock     ClockPolicy
		effective time.Time
	}{
		{name: "client", clock: ClientClock, effective: time.Unix(300, 0)},
		{name: "server", clock: ServerClock, effective: time.Unix(100, 0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := valkeyRequest(t)
			request.Now = time.Unix(300, 0)
			staleReset := test.effective.Add(-time.Microsecond).UnixMicro()
			store, _ := newStore(&fakeExecutor{reply: []string{"1", "7", "10", fmt.Sprint(staleReset), "0", "allowed", fmt.Sprint(test.effective.UnixMicro())}}, Options{Prefix: "strict", Timeout: time.Second, Clock: test.clock})
			if decision, err := store.AdmitStrict(context.Background(), request); decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrOutcomeUnknown) {
				t.Fatalf("stale admit = %+v, %v", decision, err)
			}

			leaseRequest := edgeLeaseRequest(t)
			leaseRequest.Request.Now = time.Unix(300, 0)
			nonfuture := test.effective.UnixMicro()
			executor := &fullExecutor{acquireReply: []string{"1", "0", "1", fmt.Sprint(nonfuture), "0", "allowed", fmt.Sprint(nonfuture), fmt.Sprint(nonfuture)}}
			store, _ = newStore(executor, Options{Prefix: "strict", Timeout: time.Second, Clock: test.clock})
			if lease, decision, err := store.AcquireStrict(context.Background(), leaseRequest); lease != (ratelimit.Lease{}) || decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrOutcomeUnknown) {
				t.Fatalf("nonfuture acquire = %+v, %+v, %v", lease, decision, err)
			}
		})
	}
}

func TestStrictValkeyServerClockAcceptsResultsBeforeCallerTime(t *testing.T) {
	callerNow := time.Unix(300, 0)
	serverNow := time.Unix(100, 0)
	reset := time.Unix(200, 0)
	request := valkeyRequest(t)
	request.Now = callerNow

	for _, test := range []struct {
		name  string
		reply []string
		err   error
	}{
		{name: "allowed", reply: []string{"1", "7", "10", fmt.Sprint(reset.UnixMicro()), "0", "allowed", fmt.Sprint(serverNow.UnixMicro())}},
		{name: "rejected", reply: []string{"0", "0", "10", fmt.Sprint(reset.UnixMicro()), "100000000", "limited", fmt.Sprint(serverNow.UnixMicro())}, err: ratelimit.ErrRejected},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, _ := newStore(&fakeExecutor{reply: test.reply}, Options{Prefix: "strict", Timeout: time.Second, Clock: ServerClock})
			observed := make(chan ratelimit.Observation, 1)
			service, err := ratelimit.NewStrictService(store, ratelimit.ObserveFunc(func(observation ratelimit.Observation) { observed <- observation }))
			if err != nil {
				t.Fatal(err)
			}
			decision, err := service.Admit(context.Background(), request)
			if err != test.err || errors.Is(err, ratelimit.ErrOutcomeUnknown) || !decision.Reset.Equal(reset) { //nolint:errorlint // Known outcomes preserve exact identity.
				t.Fatalf("Admit() = %+v, %v", decision, err)
			}
			observation := <-observed
			if observation.Decision != decision || observation.Err != err { //nolint:errorlint // Observer receives the exact returned error object.
				t.Fatalf("observation = %+v/%v", observation.Decision, observation.Err)
			}
		})
	}

	leaseRequest := edgeLeaseRequest(t)
	leaseRequest.Request.Now = callerNow
	executor := &fullExecutor{acquireReply: []string{"1", "1", "2", fmt.Sprint(reset.UnixMicro()), "0", "allowed", fmt.Sprint(reset.UnixMicro()), fmt.Sprint(serverNow.UnixMicro())}}
	store, _ := newStore(executor, Options{Prefix: "strict", Timeout: time.Second, Clock: ServerClock})
	observed := make(chan ratelimit.Observation, 1)
	service, _ := ratelimit.NewStrictService(store, ratelimit.ObserveFunc(func(observation ratelimit.Observation) { observed <- observation }))
	lease, decision, err := service.Acquire(context.Background(), leaseRequest)
	if err != nil || errors.Is(err, ratelimit.ErrOutcomeUnknown) || !decision.Reset.Equal(reset) || !lease.ExpiresAt.Equal(reset) {
		t.Fatalf("Acquire() = %+v, %+v, %v", lease, decision, err)
	}
	observation := <-observed
	if observation.Decision != decision || observation.Err != nil {
		t.Fatalf("observation = %+v/%v", observation.Decision, observation.Err)
	}
}

func TestAdmitStrictTreatsTransportFailureAfterDispatchAsUnknown(t *testing.T) {
	store, err := newStore(&fakeExecutor{err: errors.New("connection lost secret=value")}, Options{
		Prefix: "strict", Timeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, callErr := store.AdmitStrict(context.Background(), valkeyRequest(t))
	if !errors.Is(callErr, ratelimit.ErrOutcomeUnknown) {
		t.Fatalf("AdmitStrict() error = %v, want outcome unknown", callErr)
	}
	if errors.Is(callErr, ratelimit.ErrUnavailable) {
		t.Fatalf("post-dispatch error was incorrectly classified unavailable: %v", callErr)
	}
}

func TestStrictValkeyConstructionAndValidation(t *testing.T) {
	options := Options{Prefix: "strict", Timeout: time.Second}
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
	if store, err := NewStrict(nil, options); store != nil || err == nil || err.Error() != "invalid rate limit policy: Valkey client is required" {
		t.Fatalf("nil=%v,%v", store, err)
	}
	var typedNil *clientWrapper
	if store, err := NewStrict(typedNil, options); store != nil || err == nil || err.Error() != "invalid rate limit policy: Valkey client is required" {
		t.Fatalf("typed nil=%v,%v", store, err)
	}
	if store, err := NewStrict(clientWrapper{}, options); err != nil || store == nil {
		t.Fatalf("value=%v,%v", store, err)
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
	store, _ := newStore(&fakeExecutor{}, options)
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
	if err := store.CheckStrict(context.Background()); !errors.Is(err, ratelimit.ErrUnsupported) {
		t.Fatalf("check=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.AdmitStrict(ctx, valkeyRequest(t)); !errors.Is(err, ratelimit.ErrCanceled) {
		t.Fatalf("canceled=%v", err)
	}
}

func TestStrictValkeyKnownRepliesAndLeaseLifecycle(t *testing.T) {
	admitExecutor := &fakeExecutor{reply: []string{"1", "7", "10", "11000000", "0", "allowed", "10000000"}}
	store, _ := newStore(admitExecutor, Options{Prefix: "strict", Timeout: time.Second})
	decision, err := store.AdmitStrict(context.Background(), valkeyRequest(t))
	if err != nil || !decision.Allowed {
		t.Fatalf("admit=%+v,%v", decision, err)
	}
	admitExecutor.reply = []string{"broken"}
	if _, err := store.AdmitStrict(context.Background(), valkeyRequest(t)); !errors.Is(err, ratelimit.ErrOutcomeUnknown) {
		t.Fatalf("corrupt=%v", err)
	}
	request := edgeLeaseRequest(t)
	executor := &fullExecutor{acquireReply: []string{"1", "0", "1", "101000000", "0", "allowed", "101000000", "100000000"}, releaseReply: []string{"ok"}}
	store, _ = newStore(executor, Options{Prefix: "strict", Timeout: time.Second})
	lease, decision, err := store.AcquireStrict(context.Background(), request)
	if err != nil || !decision.Allowed {
		t.Fatalf("acquire=%+v,%+v,%v", lease, decision, err)
	}
	if err := store.ReleaseStrict(context.Background(), lease); err != nil {
		t.Fatalf("release=%v", err)
	}
	if len(executor.releaseArgs) != 6 || executor.releaseArgs[5] != "strict" {
		t.Fatalf("strict release args=%q", executor.releaseArgs)
	}
	for _, test := range []struct {
		reply string
		want  error
	}{{"not_found", ratelimit.ErrLeaseNotFound}, {"not_owned", ratelimit.ErrLeaseNotOwned}, {"other", ratelimit.ErrOutcomeUnknown}} {
		executor.releaseReply = []string{test.reply}
		if err := store.ReleaseStrict(context.Background(), lease); !errors.Is(err, test.want) {
			t.Fatalf("release(%s)=%v", test.reply, err)
		}
	}
	executor.releaseReply = []string{}
	if err := store.ReleaseStrict(context.Background(), lease); !errors.Is(err, ratelimit.ErrOutcomeUnknown) {
		t.Fatalf("short=%v", err)
	}
	executor.leaseErr = errors.New("lost")
	if _, _, err := store.AcquireStrict(context.Background(), request); !errors.Is(err, ratelimit.ErrOutcomeUnknown) {
		t.Fatalf("acquire lost=%v", err)
	}
	if err := store.ReleaseStrict(context.Background(), lease); !errors.Is(err, ratelimit.ErrOutcomeUnknown) {
		t.Fatalf("release lost=%v", err)
	}
}

func TestStrictValkeyCheck(t *testing.T) {
	native := &nativeExecutor{info: func(context.Context) (string, error) { return "valkey_version:9.0.0", nil }, config: func(context.Context) (map[string]string, error) {
		return map[string]string{"maxmemory-policy": "noeviction"}, nil
	}}
	store, _ := newStore(native, Options{Prefix: "strict", Timeout: time.Second})
	if err := store.CheckStrict(context.Background()); err != nil {
		t.Fatalf("check=%v", err)
	}
	native.info = func(context.Context) (string, error) { return "", errors.New("info") }
	if err := store.CheckStrict(context.Background()); err != ratelimit.ErrUnavailable { //nolint:errorlint // Unsafe INFO failures normalize to the exact sentinel.
		t.Fatalf("info=%v", err)
	}
	native.info = func(context.Context) (string, error) { return "valkey_version:8.0.0", nil }
	if err := store.CheckStrict(context.Background()); err != ratelimit.ErrUnavailable { //nolint:errorlint // Unsupported versions return the exact stable sentinel.
		t.Fatalf("version=%v", err)
	}
	native.info = func(context.Context) (string, error) { return "valkey_version:9.0.0", nil }
	native.config = func(context.Context) (map[string]string, error) { return nil, errors.New("config") }
	if err := store.CheckStrict(context.Background()); err != ratelimit.ErrUnavailable { //nolint:errorlint // Unsafe CONFIG failures normalize to the exact sentinel.
		t.Fatalf("config=%v", err)
	}
	native.config = func(context.Context) (map[string]string, error) {
		return map[string]string{"maxmemory-policy": "allkeys-lru"}, nil
	}
	if err := store.CheckStrict(context.Background()); err != ratelimit.ErrUnavailable { //nolint:errorlint // Unsafe eviction policy returns the exact stable sentinel.
		t.Fatalf("policy=%v", err)
	}
}
