package ratelimithttp

import (
	"context"
	"net/http"
	"net/netip"
	"testing"
)

func FuzzTrustedProxyChainNeverPanics(f *testing.F) {
	f.Add("198.51.100.1, 10.0.0.1")
	f.Add("not-an-ip")
	extractor, err := NewClientIPExtractor(ClientIPOptions{
		TrustedProxies: []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")},
	})
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, forwarded string) {
		request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://example.test", nil)
		if err != nil {
			t.Fatal(err)
		}
		request.RemoteAddr = "10.0.0.2:1234"
		request.Header.Set("X-Forwarded-For", forwarded)
		_, _ = extractor.ClientIP(request)
	})
}
