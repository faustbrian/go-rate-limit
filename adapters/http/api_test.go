//nolint:staticcheck // Explicit types compile-check the complete public API signatures.
package ratelimithttp_test

import (
	"net/http"
	"net/netip"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
	ratelimithttp "github.com/faustbrian/go-rate-limit/adapters/http"
)

func TestPublicAPIExists(t *testing.T) {
	t.Parallel()

	var _ = ratelimithttp.MaxTrustedProxies
	var _ error = ratelimithttp.ErrInvalidClientIP
	var _ = ratelimithttp.ClientIPOptions{TrustedProxies: []netip.Prefix{}}
	var extractor *ratelimithttp.ClientIPExtractor
	var _ func(*http.Request) (netip.Addr, error) = extractor.ClientIP
	var _ func(ratelimithttp.ClientIPOptions) (*ratelimithttp.ClientIPExtractor, error) = ratelimithttp.NewClientIPExtractor
	var _ = ratelimithttp.Options{
		Service: (*ratelimit.StrictService)(nil), Policy: ratelimit.Policy{},
		Now: func() time.Time { return time.Time{} }, Cost: func(*http.Request) (uint64, error) { return 0, nil },
		Key: func(*http.Request) (ratelimit.Key, error) { return ratelimit.Key{}, nil }, ClientIP: ratelimithttp.ClientIPOptions{},
	}
	var middleware *ratelimithttp.Middleware
	var _ func(http.Handler) (http.Handler, error) = middleware.Wrap
	var _ func(ratelimithttp.Options) (*ratelimithttp.Middleware, error) = ratelimithttp.New
}
