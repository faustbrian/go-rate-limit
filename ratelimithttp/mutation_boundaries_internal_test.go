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

func TestClientIPConfigurationMaximumIsInclusive(t *testing.T) {
	t.Parallel()

	trusted := make([]netip.Prefix, MaxTrustedProxies)
	for index := range trusted {
		trusted[index] = netip.MustParsePrefix("10.0.0.0/8")
	}
	if _, err := NewClientIPExtractor(ClientIPOptions{TrustedProxies: trusted}); err != nil {
		t.Fatalf("NewClientIPExtractor(maximum) error = %v", err)
	}
}

func TestForwardedChainMaximumsAreInclusive(t *testing.T) {
	t.Parallel()

	extractor, err := NewClientIPExtractor(ClientIPOptions{
		TrustedProxies: []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	request.RemoteAddr = "10.0.0.1:80"
	parts := make([]string, maxForwardedHops)
	for index := range parts {
		parts[index] = "10.0.0.2"
	}
	parts[0] = "192.0.2.1"
	request.Header.Set("X-Forwarded-For", strings.Join(parts, ","))
	if got, err := extractor.ClientIP(request); err != nil || got.String() != "192.0.2.1" {
		t.Fatalf("ClientIP(maximum hops) = %s, %v", got, err)
	}
	request.Header.Set("X-Forwarded-For", "192.0.2.1"+strings.Repeat(" ", maxForwardedBytes-len("192.0.2.1")))
	if got, err := extractor.ClientIP(request); err != nil || got.String() != "192.0.2.1" {
		t.Fatalf("ClientIP(maximum bytes) = %s, %v", got, err)
	}
}

func TestClientIPExaminesTheFirstForwardedAddress(t *testing.T) {
	t.Parallel()

	extractor, err := NewClientIPExtractor(ClientIPOptions{
		TrustedProxies: []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	request.RemoteAddr = "10.0.0.1:80"
	request.Header.Set("X-Forwarded-For", "192.0.2.1, 10.0.0.2")
	if got, err := extractor.ClientIP(request); err != nil || got.String() != "192.0.2.1" {
		t.Fatalf("ClientIP() = %s, %v", got, err)
	}
}

func TestMiddlewareRequiresServiceAndPolicyIndependently(t *testing.T) {
	t.Parallel()

	service, err := ratelimit.NewService(&edgeBackend{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(Options{Service: service}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("New(missing policy) error = %v", err)
	}
	if _, err := New(Options{Policy: edgePolicy(t)}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("New(missing service) error = %v", err)
	}
}

func TestHeaderDurationBoundaries(t *testing.T) {
	t.Parallel()

	now := time.Unix(1, 0)
	for _, test := range []struct {
		name       string
		retryAfter time.Duration
		want       string
	}{
		{name: "zero", retryAfter: 0, want: ""},
		{name: "positive", retryAfter: time.Nanosecond, want: "1"},
	} {
		header := make(http.Header)
		writeHeaders(header, ratelimit.Decision{Reset: now, RetryAfter: test.retryAfter}, now)
		if got := header.Get("Retry-After"); got != test.want {
			t.Fatalf("%s Retry-After = %q", test.name, got)
		}
	}
	if ceilSeconds(0) != 0 || ceilSeconds(time.Nanosecond) != 1 {
		t.Fatal("ceilSeconds boundary diverged")
	}
}
