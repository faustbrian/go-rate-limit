// Package ratelimithttp provides net/http inbound admission middleware and
// strict trusted-proxy client IP extraction.
//
// This compatibility path uses Service rather than StrictService and preserves
// its unchecked downstream-handler and receiver behavior. The canonical adapter
// adds validated middleware construction; neither path owns application retries.
//
// Deprecated: use github.com/faustbrian/go-rate-limit/v2/adapters/http. This package remains supported through the documented compatibility interval.
package ratelimithttp
