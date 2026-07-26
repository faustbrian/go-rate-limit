package ratelimithttp

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
)

type edgeBackend struct{ err error }

func (*edgeBackend) Name() string { return "edge" }
func (backend *edgeBackend) Admit(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
	return ratelimit.Decision{}, backend.err
}

func TestClientIPEdgeCases(t *testing.T) {
	t.Parallel()

	if _, err := NewClientIPExtractor(ClientIPOptions{
		TrustedProxies: []netip.Prefix{{}},
	}); !errors.Is(err, ErrInvalidClientIP) {
		t.Fatalf("invalid prefix error = %v", err)
	}
	manyProxies := make([]netip.Prefix, 65)
	for index := range manyProxies {
		manyProxies[index] = netip.MustParsePrefix("10.0.0.0/8")
	}
	if _, err := NewClientIPExtractor(ClientIPOptions{
		TrustedProxies: manyProxies,
	}); !errors.Is(err, ErrInvalidClientIP) {
		t.Fatalf("too many trusted proxies error = %v", err)
	}
	extractor, err := NewClientIPExtractor(ClientIPOptions{
		TrustedProxies: []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	request.RemoteAddr = "bad"
	if _, err := extractor.ClientIP(request); !errors.Is(err, ErrInvalidClientIP) {
		t.Fatalf("bad peer error = %v", err)
	}
	request.RemoteAddr = "192.0.2.1:80"
	request.Header.Set("X-Forwarded-For", "bad")
	if got, err := extractor.ClientIP(request); err != nil || got.String() != "192.0.2.1" {
		t.Fatalf("untrusted ClientIP() = %v, %v", got, err)
	}
	request.RemoteAddr = "10.0.0.1:80"
	request.Header.Del("X-Forwarded-For")
	if got, err := extractor.ClientIP(request); err != nil || got.String() != "10.0.0.1" {
		t.Fatalf("empty chain ClientIP() = %v, %v", got, err)
	}
	request.Header.Set("X-Forwarded-For", strings.Repeat("1", maxForwardedBytes+1))
	if _, err := extractor.ClientIP(request); err == nil {
		t.Fatal("oversized chain error = nil")
	}
	request.Header.Set("X-Forwarded-For", strings.Repeat("10.0.0.1,", maxForwardedHops)+"10.0.0.1")
	if _, err := extractor.ClientIP(request); err == nil {
		t.Fatal("long chain error = nil")
	}
	request.Header.Set("X-Forwarded-For", "10.0.0.2, 10.0.0.3")
	if got, err := extractor.ClientIP(request); err != nil || got.String() != "10.0.0.2" {
		t.Fatalf("trusted chain ClientIP() = %v, %v", got, err)
	}
	if got, err := parseRemoteAddr("2001:db8::1"); err != nil || got.String() != "2001:db8::1" {
		t.Fatalf("plain parseRemoteAddr() = %v, %v", got, err)
	}
}

func TestMiddlewareConfigurationAndFailureEdges(t *testing.T) {
	t.Parallel()

	if _, err := New(Options{}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("New(empty) error = %v", err)
	}
	service, err := ratelimit.NewService(&edgeBackend{})
	if err != nil {
		t.Fatal(err)
	}
	policy := edgePolicy(t)
	if _, err := New(Options{
		Service: service, Policy: policy,
		ClientIP: ClientIPOptions{TrustedProxies: []netip.Prefix{{}}},
	}); !errors.Is(err, ErrInvalidClientIP) {
		t.Fatalf("New(invalid proxy) error = %v", err)
	}
	defaultMiddleware, err := New(Options{Service: service, Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	badRemote := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	badRemote.RemoteAddr = "bad"
	badResponse := httptest.NewRecorder()
	defaultMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("next called")
	})).ServeHTTP(badResponse, badRemote)
	if badResponse.Code != http.StatusBadRequest {
		t.Fatalf("bad remote status = %d", badResponse.Code)
	}
	tests := []struct {
		name    string
		options Options
		status  int
	}{
		{name: "key", options: Options{
			Service: service, Policy: policy,
			Key: func(*http.Request) (ratelimit.Key, error) { return ratelimit.Key{}, errors.New("key") },
		}, status: http.StatusBadRequest},
		{name: "cost", options: Options{
			Service: service, Policy: policy,
			Key:  func(*http.Request) (ratelimit.Key, error) { return edgeKey(t), nil },
			Cost: func(*http.Request) (uint64, error) { return 0, errors.New("cost") },
		}, status: http.StatusBadRequest},
	}
	for _, test := range tests {
		middleware, err := New(test.options)
		if err != nil {
			t.Fatal(err)
		}
		response := httptest.NewRecorder()
		middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			t.Fatal("next called")
		})).ServeHTTP(response, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil))
		if response.Code != test.status {
			t.Fatalf("%s status = %d", test.name, response.Code)
		}
	}
	unavailable := &edgeBackend{err: ratelimit.ErrUnavailable}
	service, _ = ratelimit.NewService(unavailable)
	middleware, err := New(Options{
		Service: service, Policy: policy, Key: func(*http.Request) (ratelimit.Key, error) {
			return edgeKey(t), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("next called")
	})).ServeHTTP(response, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("unavailable status = %d", response.Code)
	}
	if ceilSeconds(-time.Second) != 0 {
		t.Fatal("negative reset was not clamped")
	}
}

func edgePolicy(t *testing.T) ratelimit.Policy {
	t.Helper()
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "edge", Revision: "v1", Algorithm: ratelimit.FixedWindow,
		Capacity: 1, Period: time.Second, MaxCost: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func edgeKey(t *testing.T) ratelimit.Key {
	t.Helper()
	key, err := ratelimit.NewKey(ratelimit.KeySpec{
		Namespace: "test", Version: "v1",
		Subject: ratelimit.Subject{Kind: "case", Value: "edge"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return key
}
