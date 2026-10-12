// Package ratelimitqueue provides queue admission middleware that returns a
// typed deferral without acknowledging, sleeping, or changing retry ownership.
//
// This compatibility path uses Service, borrows handlers/callbacks and preserves
// legacy nil-function behavior. Message fields are not generally size-validated;
// only the selected key is bounded. The canonical adapter adds strict admission
// and checked handler construction, not durable queue ownership.
//
// Deprecated: use github.com/faustbrian/go-rate-limit/v2/adapters/queue. This package remains supported through the documented compatibility interval.
package ratelimitqueue
