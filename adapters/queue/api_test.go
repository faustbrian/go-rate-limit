//nolint:staticcheck // Explicit types compile-check the complete public API signatures.
package ratelimitqueue_test

import (
	"context"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
	ratelimitqueue "github.com/faustbrian/go-rate-limit/adapters/queue"
)

func TestPublicAPIExists(t *testing.T) {
	t.Parallel()

	var _ = ratelimitqueue.Message{ID: "id", Queue: "queue", Tenant: "tenant", Principal: "principal", Attempt: 1}
	var _ ratelimitqueue.Handler = ratelimitqueue.HandlerFunc(nil)
	var _ func(context.Context, ratelimitqueue.Message) error = ratelimitqueue.HandlerFunc(nil).Handle
	var _ ratelimitqueue.SubjectFunc = ratelimitqueue.ByPrincipal()
	var _ = ratelimitqueue.Options{
		Service: (*ratelimit.StrictService)(nil), Policy: ratelimit.Policy{}, Subject: ratelimitqueue.SubjectFunc(nil),
		Cost: func(ratelimitqueue.Message) (uint64, error) { return 0, nil }, Now: func() time.Time { return time.Time{} },
	}
	var deferred = &ratelimitqueue.Deferred{RetryAfter: time.Second}
	var _ error = deferred
	var _ func() string = deferred.Error
	var _ func() error = deferred.Unwrap
	var middleware *ratelimitqueue.Middleware
	var _ func(ratelimitqueue.Handler) (ratelimitqueue.Handler, error) = middleware.Wrap
	var _ func(ratelimitqueue.Options) (*ratelimitqueue.Middleware, error) = ratelimitqueue.New
	var _ func() ratelimitqueue.SubjectFunc = ratelimitqueue.ByQueueAndTenant
	var _ func() ratelimitqueue.SubjectFunc = ratelimitqueue.ByPrincipal
}
