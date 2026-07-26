package ratelimithttp_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
	"github.com/faustbrian/golib/pkg/rate-limit/ratelimithttp"
)

type backend struct {
	decision ratelimit.Decision
	err      error
	request  ratelimit.Request
}

func (backend *backend) Name() string { return "http-test" }
func (backend *backend) Admit(_ context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
	backend.request = request
	return backend.decision, backend.err
}

func TestMiddlewareSetsHeadersAndUsesTrustedClientIP(t *testing.T) {
	t.Parallel()

	now := time.Unix(100, 0)
	implementation := &backend{decision: ratelimit.Decision{
		Allowed: true, Limit: 10, Remaining: 7,
		Reset: now.Add(30 * time.Second), Reason: ratelimit.ReasonAllowed,
	}}
	service, err := ratelimit.NewService(implementation)
	if err != nil {
		t.Fatal(err)
	}
	proxy := netip.MustParsePrefix("10.0.0.0/8")
	middleware, err := ratelimithttp.New(ratelimithttp.Options{
		Service: service, Policy: fixedPolicy(t), Now: func() time.Time { return now },
		ClientIP: ratelimithttp.ClientIPOptions{TrustedProxies: []netip.Prefix{proxy}},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler := middleware(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	request.RemoteAddr = "10.0.0.2:1234"
	request.Header.Set("X-Forwarded-For", "198.51.100.4, 10.0.0.1")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent ||
		response.Header().Get("RateLimit-Limit") != "10" ||
		response.Header().Get("RateLimit-Remaining") != "7" ||
		response.Header().Get("RateLimit-Reset") != "30" {
		t.Fatalf("response = %d, %v", response.Code, response.Header())
	}
	if implementation.request.Key.String() == "" ||
		implementation.request.Key.String() == "198.51.100.4" {
		t.Fatalf("key = %q", implementation.request.Key.String())
	}
}

func TestMiddlewareRejectsWithoutDisclosingPolicy(t *testing.T) {
	t.Parallel()

	now := time.Unix(100, 0)
	implementation := &backend{decision: ratelimit.Decision{
		Allowed: false, Limit: 10, Remaining: 0,
		Reset: now.Add(1500 * time.Millisecond), RetryAfter: 1500 * time.Millisecond,
		Reason: ratelimit.ReasonLimited,
	}, err: ratelimit.ErrRejected}
	service, err := ratelimit.NewService(implementation)
	if err != nil {
		t.Fatal(err)
	}
	middleware, err := ratelimithttp.New(ratelimithttp.Options{
		Service: service, Policy: fixedPolicy(t), Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	request.RemoteAddr = "192.0.2.1:1234"
	middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("next handler called")
	})).ServeHTTP(response, request)

	if response.Code != http.StatusTooManyRequests ||
		response.Header().Get("Retry-After") != "2" ||
		response.Body.String() != "rate limit exceeded\n" {
		t.Fatalf("response = %d, %v, %q", response.Code, response.Header(), response.Body.String())
	}
}

func TestClientIPRejectsMalformedTrustedProxyChain(t *testing.T) {
	t.Parallel()

	extractor, err := ratelimithttp.NewClientIPExtractor(ratelimithttp.ClientIPOptions{
		TrustedProxies: []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	request.RemoteAddr = "10.0.0.2:1234"
	request.Header.Set("X-Forwarded-For", "not-an-ip")
	if _, err := extractor.ClientIP(request); !errors.Is(err, ratelimithttp.ErrInvalidClientIP) {
		t.Fatalf("ClientIP() error = %v", err)
	}
}

func fixedPolicy(t *testing.T) ratelimit.Policy {
	t.Helper()
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "secret-login-policy", Revision: "v1", Algorithm: ratelimit.FixedWindow,
		Capacity: 10, Period: time.Minute, MaxCost: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}
