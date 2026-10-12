// Package ratelimithttp provides strict inbound HTTP rate-limit middleware at
// github.com/faustbrian/go-rate-limit/v2/adapters/http.
//
// The middleware is synchronous and borrows callbacks, service and handler.
// Callbacks may block or panic and must support concurrent requests. Proxy trust
// is configured by network prefix; forwarded headers are trusted only from a
// trusted direct peer, not authenticated by this package. Admission does not own
// request-body buffering, outbound pacing, retries or application shutdown.
package ratelimithttp
