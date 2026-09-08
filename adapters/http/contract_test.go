package ratelimithttp_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
	ratelimithttp "github.com/faustbrian/go-rate-limit/adapters/http"
)

type backend struct{}

func (backend) Name() string { return "test" }
func (backend) Admit(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
	panic("legacy dispatch")
}
func (backend) AdmitStrict(_ context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
	return ratelimit.Decision{Allowed: true, Limit: request.Policy.Limit(), Remaining: request.Policy.Limit() - request.Cost, Reset: request.Now.Add(time.Second), Reason: ratelimit.ReasonAllowed}, nil
}

type nilHandler struct{}

type contextKey struct{}

func (*nilHandler) ServeHTTP(http.ResponseWriter, *http.Request) { panic("must not be called") }

func TestWrapRejectsNilAndTypedNilHandlers(t *testing.T) {
	for _, middleware := range []*ratelimithttp.Middleware{nil, {}} {
		if handler, err := middleware.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})); handler != nil ||
			!errors.Is(err, ratelimit.ErrInvalidPolicy) || err.Error() != "invalid rate limit policy: middleware is nil or uninitialized" {
			t.Fatalf("nil/zero middleware = %v, %v", handler, err)
		}
	}
	service, _ := ratelimit.NewStrictService(backend{})
	policy, _ := ratelimit.NewPolicy(ratelimit.PolicySpec{ID: "http", Revision: "v1", Algorithm: ratelimit.TokenBucket, Capacity: 1, Period: time.Second, MaxCost: 1})
	middleware, err := ratelimithttp.New(ratelimithttp.Options{Service: service, Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	if handler, err := middleware.Wrap(nil); handler != nil || !errors.Is(err, ratelimit.ErrInvalidPolicy) || err.Error() != "invalid rate limit policy: handler is required" {
		t.Fatalf("literal nil handler = %v, %v", handler, err)
	}
	var next *nilHandler
	if handler, err := middleware.Wrap(next); handler != nil || !errors.Is(err, ratelimit.ErrInvalidPolicy) || err.Error() != "invalid rate limit policy: handler is required" {
		t.Fatalf("typed nil handler = %v, %v", handler, err)
	}
	key := contextKey{}
	request := httptest.NewRequestWithContext(context.WithValue(context.Background(), key, "value"), http.MethodGet, "http://example.test", nil)
	request.RemoteAddr = "192.0.2.1:1234"
	called := false
	wrapped, err := middleware.Wrap(http.HandlerFunc(func(writer http.ResponseWriter, received *http.Request) {
		called = true
		if received != request || received.Context().Value(key) != "value" {
			t.Fatal("request or context identity changed")
		}
		writer.WriteHeader(http.StatusCreated)
	}))
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	wrapped.ServeHTTP(response, request)
	if !called || response.Code != http.StatusCreated {
		t.Fatalf("successful transfer called=%v status=%d", called, response.Code)
	}
}

func TestNilExtractorIsTotal(t *testing.T) {
	var extractor *ratelimithttp.ClientIPExtractor
	if _, err := extractor.ClientIP(nil); !errors.Is(err, ratelimithttp.ErrInvalidClientIP) {
		t.Fatalf("ClientIP() error = %v", err)
	}
}

func TestConstructionAndExtractorValidationPrecedence(t *testing.T) {
	if _, err := ratelimithttp.New(ratelimithttp.Options{}); err == nil || err.Error() != "invalid rate limit policy: strict service is required" {
		t.Fatalf("zero options error = %v", err)
	}
	service, _ := ratelimit.NewStrictService(backend{})
	if _, err := ratelimithttp.New(ratelimithttp.Options{Service: service}); err == nil || err.Error() != "invalid rate limit policy: policy is required" {
		t.Fatalf("zero policy error = %v", err)
	}
	extractor, err := ratelimithttp.NewClientIPExtractor(ratelimithttp.ClientIPOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := extractor.ClientIP(nil); err == nil || err.Error() != "invalid client IP: request is required" {
		t.Fatalf("nil request error = %v", err)
	}
}
