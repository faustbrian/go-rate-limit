package ratelimithttp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit/v3"
)

type recordingBackend struct {
	reject  bool
	request ratelimit.Request
}

func (*recordingBackend) Name() string { return "test" }
func (*recordingBackend) Admit(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
	panic("legacy")
}
func (backend *recordingBackend) AdmitStrict(_ context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
	backend.request = request
	if backend.reject {
		return ratelimit.Decision{
			Allowed: false, Limit: request.Policy.Limit(), Remaining: 0,
			Reset: request.Now.Add(1500 * time.Millisecond), RetryAfter: 1500 * time.Millisecond,
			Reason: ratelimit.ReasonLimited,
		}, ratelimit.ErrRejected
	}
	return ratelimit.Decision{
		Allowed: true, Limit: request.Policy.Limit(), Remaining: request.Policy.Limit() - request.Cost,
		Reset: request.Now.Add(29200 * time.Millisecond), Reason: ratelimit.ReasonAllowed,
	}, nil
}

func serveRecorded(t *testing.T, backend *recordingBackend, request *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	service, err := ratelimit.NewStrictService(backend)
	if err != nil {
		t.Fatal(err)
	}
	middleware, err := New(Options{
		Service: service, Policy: httpPolicy(t), Now: func() time.Time { return time.Unix(100, 0) },
		ClientIP: ClientIPOptions{TrustedProxies: []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := middleware.Wrap(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	}))
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func forwardedRequest() *http.Request {
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	request.RemoteAddr = "10.0.0.2:1234"
	request.Header.Set("X-Forwarded-For", "198.51.100.4, 10.0.0.1")
	return request
}

func TestAllowedResponseHeadersAndDefaultKey(t *testing.T) {
	backend := &recordingBackend{}
	response := serveRecorded(t, backend, forwardedRequest())
	if response.Code != http.StatusNoContent ||
		response.Header().Get("RateLimit-Limit") != "2" ||
		response.Header().Get("RateLimit-Remaining") != "1" ||
		response.Header().Get("RateLimit-Reset") != "30" ||
		response.Header().Get("Retry-After") != "" {
		t.Fatalf("response = %d, %v", response.Code, response.Header())
	}
	// The default key is persisted by backends; this value was produced by the
	// published v2.1.0 module for the same trusted-chain client address.
	const want = "http:v1:ip:b5eb0beede6787db695abd9f411deb01931a66a4f79e4b1df30c24f6b70a7afa"
	if got := backend.request.Key.String(); got != want || backend.request.Key.SubjectKind() != "ip" {
		t.Fatalf("default key = %q (%s), want %q", got, backend.request.Key.SubjectKind(), want)
	}
}

func TestRejectedResponseRoundsRetryAndHidesPolicy(t *testing.T) {
	response := serveRecorded(t, &recordingBackend{reject: true}, forwardedRequest())
	if response.Code != http.StatusTooManyRequests ||
		response.Header().Get("Retry-After") != "2" ||
		response.Header().Get("RateLimit-Reset") != "2" ||
		response.Header().Get("RateLimit-Remaining") != "0" ||
		response.Body.String() != "rate limit exceeded\n" {
		t.Fatalf("response = %d, %v, %q", response.Code, response.Header(), response.Body.String())
	}
}

func TestWriteHeadersClampsPastReset(t *testing.T) {
	now := time.Unix(100, 0)
	header := http.Header{}
	writeHeaders(header, ratelimit.Decision{Limit: 10, Remaining: 7, Reset: now.Add(-100 * time.Second)}, now)
	if header.Get("RateLimit-Reset") != "0" || header.Get("RateLimit-Limit") != "10" ||
		header.Get("RateLimit-Remaining") != "7" || header.Get("Retry-After") != "" {
		t.Fatalf("headers = %v", header)
	}
}
