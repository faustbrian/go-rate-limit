package ratelimithttp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
)

type strictBackend struct {
	decision ratelimit.Decision
	err      error
	raw      bool
}

type hostileBackendError struct{}

func (hostileBackendError) Error() string { panic("hostile Error invoked") }
func (hostileBackendError) Is(error) bool { panic("hostile Is invoked") }
func (hostileBackendError) As(any) bool   { panic("hostile As invoked") }

func (*strictBackend) Name() string { return "test" }
func (*strictBackend) Admit(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
	panic("legacy")
}
func (backend *strictBackend) AdmitStrict(_ context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
	decision := backend.decision
	if decision == (ratelimit.Decision{}) && backend.err == nil && !backend.raw {
		decision = ratelimit.Decision{Allowed: true, Limit: request.Policy.Limit(), Remaining: request.Policy.Limit() - request.Cost, Reset: request.Now.Add(time.Second), Reason: ratelimit.ReasonAllowed}
	}
	return decision, backend.err
}

type panicErrorOnly struct{}

func (panicErrorOnly) Error() string { panic("hostile Error invoked") }

type panicIsOnly struct{}

func (panicIsOnly) Error() string { return "hostile-is" }
func (panicIsOnly) Is(error) bool { panic("hostile Is invoked") }

type panicAsOnly struct{}

func (panicAsOnly) Error() string { return "hostile-as" }
func (panicAsOnly) As(any) bool   { panic("hostile As invoked") }

type panicResponseWriter struct{}

func (panicResponseWriter) Header() http.Header       { panic("writer panic") }
func (panicResponseWriter) Write([]byte) (int, error) { panic("writer panic") }
func (panicResponseWriter) WriteHeader(int)           { panic("writer panic") }

func httpPolicy(t *testing.T) ratelimit.Policy {
	t.Helper()
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{ID: "http", Revision: "v1", Algorithm: ratelimit.TokenBucket, Capacity: 2, Period: time.Second, MaxCost: 2})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}
func httpService(t *testing.T, backend *strictBackend) *ratelimit.StrictService {
	t.Helper()
	service, err := ratelimit.NewStrictService(backend)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func TestNewAndMiddlewareOutcomes(t *testing.T) {
	policy := httpPolicy(t)
	service := httpService(t, &strictBackend{})
	if _, err := New(Options{}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("empty error=%v", err)
	}
	if _, err := New(Options{Service: service}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("policy error=%v", err)
	}
	if _, err := New(Options{Service: service, Policy: policy, ClientIP: ClientIPOptions{TrustedProxies: []netip.Prefix{{}}}}); !errors.Is(err, ErrInvalidClientIP) {
		t.Fatalf("proxy error=%v", err)
	}
	defaultMiddleware, _ := New(Options{Service: service, Policy: policy})
	if handler, err := defaultMiddleware.Wrap(nil); handler != nil || !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("nil handler=%v,%v", handler, err)
	}
	defaultHandler, _ := defaultMiddleware.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	badPeer := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	badPeer.RemoteAddr = "bad"
	badResponse := httptest.NewRecorder()
	defaultHandler.ServeHTTP(badResponse, badPeer)
	if badResponse.Code != http.StatusBadRequest {
		t.Fatalf("bad peer status=%d", badResponse.Code)
	}
	key, _ := ratelimit.NewKey(ratelimit.KeySpec{Namespace: "http", Version: "v1", Subject: ratelimit.Subject{Kind: "test", Value: "x"}, Hash: true})
	tests := []struct {
		name    string
		backend *strictBackend
		key     func(*http.Request) (ratelimit.Key, error)
		cost    func(*http.Request) (uint64, error)
		want    int
		called  bool
	}{
		{name: "success", backend: &strictBackend{}, want: 200, called: true},
		{name: "key", backend: &strictBackend{}, key: func(*http.Request) (ratelimit.Key, error) { return ratelimit.Key{}, errors.New("key") }, want: 400},
		{name: "cost", backend: &strictBackend{}, key: func(*http.Request) (ratelimit.Key, error) { return key, nil }, cost: func(*http.Request) (uint64, error) { return 0, errors.New("cost") }, want: 400},
		{name: "rejected", backend: &strictBackend{decision: ratelimit.Decision{Allowed: false, Limit: 2, Remaining: 0, Reset: time.Unix(101, 0), RetryAfter: time.Second, Reason: ratelimit.ReasonLimited}, err: ratelimit.ErrRejected}, key: func(*http.Request) (ratelimit.Key, error) { return key, nil }, want: 429},
		{name: "unavailable", backend: &strictBackend{err: ratelimit.ErrUnavailable}, key: func(*http.Request) (ratelimit.Key, error) { return key, nil }, want: 503},
		{name: "huge hostile", backend: &strictBackend{err: errors.New(strings.Repeat("hostile-marker", 1_048_576/len("hostile-marker")+1)[:1_048_576])}, key: func(*http.Request) (ratelimit.Key, error) { return key, nil }, want: 503},
		{name: "wrapped hostile", backend: &strictBackend{err: fmt.Errorf("hostile-marker: %w", ratelimit.ErrUnavailable)}, key: func(*http.Request) (ratelimit.Key, error) { return key, nil }, want: 503},
		{name: "joined hostile", backend: &strictBackend{err: errors.Join(ratelimit.ErrUnavailable, ratelimit.ErrCorrupt)}, key: func(*http.Request) (ratelimit.Key, error) { return key, nil }, want: 503},
		{name: "hostile", backend: &strictBackend{err: hostileBackendError{}}, key: func(*http.Request) (ratelimit.Key, error) { return key, nil }, want: 503},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			called := false
			middleware, err := New(Options{Service: httpService(t, test.backend), Policy: policy, Now: func() time.Time { return time.Unix(100, 0) }, Key: test.key, Cost: test.cost})
			if err != nil {
				t.Fatal(err)
			}
			handler, err := middleware.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
			request.RemoteAddr = "192.0.2.1:123"
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.want || called != test.called {
				t.Fatalf("status=%d called=%v", response.Code, called)
			}
			if test.name == "rejected" && response.Header().Get("Retry-After") != "1" {
				t.Fatalf("retry=%q", response.Header().Get("Retry-After"))
			}
			if test.name == "success" && response.Header().Get("Retry-After") != "" {
				t.Fatalf("unexpected retry=%q", response.Header().Get("Retry-After"))
			}
			if test.want == http.StatusServiceUnavailable &&
				(len(response.Body.String()) > 64 || strings.Contains(response.Body.String(), "hostile-marker")) {
				t.Fatalf("unavailable response disclosed or exceeded its bound: %q", response.Body.String())
			}
		})
	}
	if nilInterface(1) {
		t.Fatal("integer reported nil")
	}
}

func TestMiddlewareRejectsEveryUnsafeAndContradictoryAdmission(t *testing.T) {
	policy := httpPolicy(t)
	key, _ := ratelimit.NewKey(ratelimit.KeySpec{Namespace: "http", Version: "v1", Subject: ratelimit.Subject{Kind: "test", Value: "x"}, Hash: true})
	reset := time.Unix(101, 0)
	allowed := ratelimit.Decision{Allowed: true, Limit: 2, Remaining: 1, Reset: reset, Reason: ratelimit.ReasonAllowed}
	rejected := ratelimit.Decision{Limit: 2, Remaining: 0, Reset: reset, RetryAfter: time.Second, Reason: ratelimit.ReasonLimited}
	backendError := func(category error) error {
		value, err := ratelimit.NewBackendError(category)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	var nilBackendError *ratelimit.BackendError
	tests := []struct {
		name     string
		decision ratelimit.Decision
		err      error
	}{
		{name: "raw canceled", err: context.Canceled},
		{name: "raw deadline", err: context.DeadlineExceeded},
		{name: "nil backend error", err: nilBackendError},
		{name: "lease not found", err: ratelimit.ErrLeaseNotFound},
		{name: "lease not found wrapper", err: backendError(ratelimit.ErrLeaseNotFound)},
		{name: "lease not owned", err: ratelimit.ErrLeaseNotOwned},
		{name: "lease not owned wrapper", err: backendError(ratelimit.ErrLeaseNotOwned)},
		{name: "panic Error", err: panicErrorOnly{}},
		{name: "panic Is", err: panicIsOnly{}},
		{name: "panic As", err: panicAsOnly{}},
		{name: "zero success"},
		{name: "denied without error", decision: rejected},
		{name: "allowed rejection", decision: allowed, err: ratelimit.ErrRejected},
		{name: "wrong rejection reason", decision: func() ratelimit.Decision { value := rejected; value.Reason = ratelimit.ReasonAllowed; return value }(), err: ratelimit.ErrRejected},
		{name: "wrong rejection limit", decision: func() ratelimit.Decision { value := rejected; value.Limit = 3; return value }(), err: ratelimit.ErrRejected},
		{name: "wrong rejection remaining", decision: func() ratelimit.Decision { value := rejected; value.Remaining = 1; return value }(), err: ratelimit.ErrRejected},
		{name: "missing rejection reset", decision: func() ratelimit.Decision { value := rejected; value.Reset = time.Time{}; return value }(), err: ratelimit.ErrRejected},
		{name: "negative rejection retry", decision: func() ratelimit.Decision { value := rejected; value.RetryAfter = -time.Second; return value }(), err: ratelimit.ErrRejected},
		{name: "nonzero terminal", decision: allowed, err: ratelimit.ErrUnavailable},
	}
	for _, category := range []error{ratelimit.ErrRejected, ratelimit.ErrUnavailable, ratelimit.ErrOverflow, ratelimit.ErrCorrupt, ratelimit.ErrUnsupported, ratelimit.ErrLeaseNotFound, ratelimit.ErrLeaseNotOwned} {
		tests = append(tests, struct {
			name     string
			decision ratelimit.Decision
			err      error
		}{name: "wrapped " + category.Error(), err: fmt.Errorf("outer: %w", category)})
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			called := false
			middleware, err := New(Options{Service: httpService(t, &strictBackend{decision: test.decision, err: test.err, raw: true}), Policy: policy, Now: func() time.Time { return time.Unix(100, 0) }, Key: func(*http.Request) (ratelimit.Key, error) { return key, nil }})
			if err != nil {
				t.Fatal(err)
			}
			handler, err := middleware.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
			if err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil))
			if response.Code != http.StatusServiceUnavailable || called || response.Body.String() != "rate limit unavailable\n" {
				t.Fatalf("response=%d/%q called=%v", response.Code, response.Body.String(), called)
			}
		})
	}
}

func TestMiddlewareCallbackLifecycle(t *testing.T) {
	policy := httpPolicy(t)
	service := httpService(t, &strictBackend{})
	key, _ := ratelimit.NewKey(ratelimit.KeySpec{Namespace: "http", Version: "v1", Subject: ratelimit.Subject{Kind: "test", Value: "x"}, Hash: true})
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	var order []string
	var middleware *Middleware
	middleware, _ = New(Options{Service: service, Policy: policy,
		Key: func(received *http.Request) (ratelimit.Key, error) {
			if received != request {
				t.Fatal("key request identity changed")
			}
			order = append(order, "key")
			if _, err := middleware.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})); err != nil {
				t.Fatal(err)
			}
			return key, nil
		},
		Cost: func(received *http.Request) (uint64, error) {
			if received != request {
				t.Fatal("cost request identity changed")
			}
			order = append(order, "cost")
			return 1, nil
		},
		Now: func() time.Time { order = append(order, "now"); return time.Unix(100, 0) },
	})
	handler, _ := middleware.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { order = append(order, "handler") }))
	handler.ServeHTTP(httptest.NewRecorder(), request)
	if strings.Join(order, ",") != "key,cost,now,handler" {
		t.Fatalf("order=%v", order)
	}

	blocked := make(chan struct{})
	entered := make(chan struct{})
	blocking, _ := New(Options{Service: service, Policy: policy, Key: func(*http.Request) (ratelimit.Key, error) { close(entered); <-blocked; return key, nil }})
	blockingHandler, _ := blocking.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	done := make(chan struct{})
	go func() { blockingHandler.ServeHTTP(httptest.NewRecorder(), request); close(done) }()
	<-entered
	select {
	case <-done:
		t.Fatal("callback did not block caller")
	default:
	}
	close(blocked)
	<-done

	panicking, _ := New(Options{Service: service, Policy: policy, Key: func(*http.Request) (ratelimit.Key, error) { panic("key panic") }})
	panickingHandler, _ := panicking.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	func() {
		defer func() {
			if recover() != "key panic" {
				t.Fatal("callback panic did not propagate")
			}
		}()
		panickingHandler.ServeHTTP(httptest.NewRecorder(), request)
	}()
	panickingDownstream, _ := middleware.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("handler panic") }))
	func() {
		defer func() {
			if recover() != "handler panic" {
				t.Fatal("downstream panic did not propagate")
			}
		}()
		panickingDownstream.ServeHTTP(httptest.NewRecorder(), request)
	}()
	func() {
		defer func() {
			if recover() != "writer panic" {
				t.Fatal("response writer panic did not propagate")
			}
		}()
		handler.ServeHTTP(panicResponseWriter{}, request)
	}()

	var keys atomic.Int64
	var handled atomic.Int64
	concurrent, _ := New(Options{Service: service, Policy: policy, Key: func(*http.Request) (ratelimit.Key, error) { keys.Add(1); return key, nil }})
	concurrentHandler, _ := concurrent.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { handled.Add(1) }))
	var group sync.WaitGroup
	for range 16 {
		group.Add(1)
		go func() {
			defer group.Done()
			concurrentHandler.ServeHTTP(httptest.NewRecorder(), request.Clone(context.Background()))
		}()
	}
	group.Wait()
	if keys.Load() != 16 || handled.Load() != 16 {
		t.Fatalf("concurrent calls=%d/%d", keys.Load(), handled.Load())
	}
}

func TestMiddlewarePreservesAuthorityNeutralClockSkew(t *testing.T) {
	policy := httpPolicy(t)
	key, _ := ratelimit.NewKey(ratelimit.KeySpec{Namespace: "http", Version: "v1", Subject: ratelimit.Subject{Kind: "test", Value: "x"}, Hash: true})
	callerNow := time.Unix(300, 0)
	backendReset := time.Unix(200, 0)
	for _, test := range []struct {
		name     string
		decision ratelimit.Decision
		err      error
		status   int
		called   bool
	}{
		{
			name: "allowed",
			decision: ratelimit.Decision{
				Allowed: true, Limit: policy.Limit(), Remaining: policy.Limit() - 1,
				Reset: backendReset, Reason: ratelimit.ReasonAllowed,
			},
			status: http.StatusOK, called: true,
		},
		{
			name: "rejected",
			decision: ratelimit.Decision{
				Limit: policy.Limit(), Reset: backendReset,
				RetryAfter: time.Second, Reason: ratelimit.ReasonLimited,
			},
			err: ratelimit.ErrRejected, status: http.StatusTooManyRequests,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			middleware, err := New(Options{
				Service: httpService(t, &strictBackend{decision: test.decision, err: test.err}),
				Policy:  policy, Now: func() time.Time { return callerNow },
				Key: func(*http.Request) (ratelimit.Key, error) { return key, nil },
			})
			if err != nil {
				t.Fatal(err)
			}
			called := false
			handler, err := middleware.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.status || called != test.called {
				t.Fatalf("status=%d called=%v", response.Code, called)
			}
		})
	}
}

func TestMiddlewareUsesConfiguredClockForResetHeader(t *testing.T) {
	policy := httpPolicy(t)
	key, _ := ratelimit.NewKey(ratelimit.KeySpec{Namespace: "http", Version: "v1", Subject: ratelimit.Subject{Kind: "test", Value: "x"}, Hash: true})
	now := time.Unix(300, 0)
	decision := ratelimit.Decision{Allowed: true, Limit: policy.Limit(), Remaining: policy.Limit() - 1, Reset: now.Add(3 * time.Second), Reason: ratelimit.ReasonAllowed}
	middleware, err := New(Options{
		Service: httpService(t, &strictBackend{decision: decision}), Policy: policy,
		Now: func() time.Time { return now }, Key: func(*http.Request) (ratelimit.Key, error) { return key, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := middleware.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil))
	if reset := response.Header().Get("RateLimit-Reset"); reset != "3" {
		t.Fatalf("reset=%q", reset)
	}
}

func TestClientIPBoundaries(t *testing.T) {
	tooMany := make([]netip.Prefix, MaxTrustedProxies+1)
	if _, err := NewClientIPExtractor(ClientIPOptions{TrustedProxies: tooMany}); !errors.Is(err, ErrInvalidClientIP) {
		t.Fatalf("too many=%v", err)
	}
	trusted := netip.MustParsePrefix("10.0.0.0/8")
	maximum := make([]netip.Prefix, MaxTrustedProxies)
	for index := range maximum {
		maximum[index] = trusted
	}
	if _, err := NewClientIPExtractor(ClientIPOptions{TrustedProxies: maximum}); err != nil {
		t.Fatalf("maximum trusted proxies error=%v", err)
	}
	extractor, err := NewClientIPExtractor(ClientIPOptions{TrustedProxies: []netip.Prefix{trusted}})
	if err != nil {
		t.Fatal(err)
	}
	requestWithoutForwardedHeader := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	requestWithoutForwardedHeader.RemoteAddr = "10.0.0.1:80"
	if address, clientIPErr := extractor.ClientIP(requestWithoutForwardedHeader); clientIPErr != nil || address.String() != "10.0.0.1" {
		t.Fatalf("trusted without forwarded header=%v,%v", address, clientIPErr)
	}
	cases := []struct {
		name, remote, forwarded string
		want                    string
		wantErr                 string
	}{
		{name: "malformed remote", remote: "bad", wantErr: "invalid client IP: malformed remote address"},
		{name: "untrusted ignores forwarded", remote: "192.0.2.1", forwarded: "bad", want: "192.0.2.1"},
		{name: "trusted no header", remote: "10.0.0.1:80", want: "10.0.0.1"},
		{name: "chain", remote: "10.0.0.1:80", forwarded: "192.0.2.2, 10.0.0.2", want: "192.0.2.2"},
		{name: "all trusted", remote: "10.0.0.1:80", forwarded: "10.0.0.2,10.0.0.3", want: "10.0.0.2"},
		{name: "malformed hop", remote: "10.0.0.1:80", forwarded: "bad", wantErr: "invalid client IP: malformed forwarded hop"},
		{name: "maximum hops", remote: "10.0.0.1:80", forwarded: strings.TrimSuffix(strings.Repeat("10.0.0.2,", maxForwardedHops), ","), want: "10.0.0.2"},
		{name: "too many hops", remote: "10.0.0.1:80", forwarded: strings.Repeat("10.0.0.2,", maxForwardedHops) + "10.0.0.2", wantErr: "invalid client IP: too many forwarded hops"},
		{name: "maximum bytes", remote: "10.0.0.1:80", forwarded: strings.Repeat("1", maxForwardedBytes), wantErr: "invalid client IP: malformed forwarded hop"},
		{name: "too large", remote: "10.0.0.1:80", forwarded: strings.Repeat("1", maxForwardedBytes+1), wantErr: "invalid client IP: forwarded chain too large"},
	}
	for _, test := range cases {
		request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
		request.RemoteAddr = test.remote
		request.Header.Set("X-Forwarded-For", test.forwarded)
		address, err := extractor.ClientIP(request)
		if (err != nil && err.Error() != test.wantErr) || (err == nil && test.wantErr != "") || (err == nil && address.String() != test.want) {
			t.Fatalf("%s=%v,%v", test.name, address, err)
		}
	}
}

func TestClientIPRetainsReleasedFirstForwardedHeaderValue(t *testing.T) {
	t.Parallel()

	trusted := netip.MustParsePrefix("10.0.0.0/8")
	extractor, err := NewClientIPExtractor(ClientIPOptions{TrustedProxies: []netip.Prefix{trusted}})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "http://example.test", nil)
	request.RemoteAddr = "10.0.0.1:80"
	request.Header["X-Forwarded-For"] = []string{"198.51.100.1", "203.0.113.2"}
	if address, err := extractor.ClientIP(request); err != nil || address.String() != "198.51.100.1" {
		t.Fatalf("ClientIP(multiple lines) = %s, %v", address, err)
	}
}
