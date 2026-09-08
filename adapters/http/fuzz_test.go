package ratelimithttp

import (
	"context"
	"net/http"
	"net/netip"
	"testing"
)

func FuzzClientIPNeverPanics(f *testing.F) {
	f.Add("10.0.0.2:1234", "198.51.100.1, 10.0.0.1")
	f.Add("not-an-ip", "not-an-ip")
	extractor, err := NewClientIPExtractor(ClientIPOptions{
		TrustedProxies: []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")},
	})
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, remote, forwarded string) {
		request, requestErr := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://example.test", nil)
		if requestErr != nil {
			t.Fatal(requestErr)
		}
		request.RemoteAddr = remote
		request.Header.Set("X-Forwarded-For", forwarded)
		address, clientErr := extractor.ClientIP(request)
		if clientErr == nil && !address.IsValid() {
			t.Fatal("ClientIP returned an invalid address without an error")
		}
	})
}
