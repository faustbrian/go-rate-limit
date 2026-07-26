package ratelimitqueue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
)

// Message contains bounded queue admission inputs, not a durable payload.
type Message struct {
	// ID identifies the durable message.
	ID string
	// Queue identifies the source queue.
	Queue string
	// Tenant is an optional tenant identity.
	Tenant string
	// Principal is an optional authenticated principal identity.
	Principal string
	// Attempt reports the durable system's current delivery attempt.
	Attempt uint64
}

// Handler processes an admitted queue message.
type Handler interface {
	// Handle processes message without changing acknowledgement ownership.
	Handle(context.Context, Message) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(context.Context, Message) error

// Handle calls function with ctx and message.
func (function HandlerFunc) Handle(ctx context.Context, message Message) error {
	return function(ctx, message)
}

// SubjectFunc derives a typed subject from queue metadata.
type SubjectFunc func(Message) (ratelimit.Subject, error)

// Options configures queue admission.
type Options struct {
	// Service makes admission decisions.
	Service *ratelimit.Service
	// Policy contains immutable admission semantics.
	Policy ratelimit.Policy
	// Subject derives queue, tenant, principal, or custom identity.
	Subject SubjectFunc
	// Cost derives message weight; nil returns one.
	Cost func(Message) (uint64, error)
	// Now supplies explicit UTC time; nil uses time.Now.
	Now func() time.Time
}

// Middleware wraps a Handler with admission that preserves durable semantics.
type Middleware func(Handler) Handler

// Deferred indicates the durable queue should retry after RetryAfter.
type Deferred struct {
	// RetryAfter is the backend decision's minimum suggested delay.
	RetryAfter time.Duration
	cause      error
}

// Error returns a stable message that does not disclose policy internals.
func (deferred *Deferred) Error() string {
	return "rate-limited queue admission deferred"
}

// Unwrap returns ratelimit.ErrRejected.
func (deferred *Deferred) Unwrap() error {
	return deferred.cause
}

// New validates options and returns queue admission middleware.
func New(options Options) (Middleware, error) {
	if options.Service == nil || options.Policy.ID() == "" || options.Subject == nil {
		return nil, fmt.Errorf("%w: service, policy, and subject are required", ratelimit.ErrInvalidPolicy)
	}
	if options.Cost == nil {
		options.Cost = func(Message) (uint64, error) { return 1, nil }
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return func(next Handler) Handler {
		return HandlerFunc(func(ctx context.Context, message Message) error {
			subject, err := options.Subject(message)
			if err != nil {
				return err
			}
			key, err := ratelimit.NewKey(ratelimit.KeySpec{
				Namespace: "queue", Version: "v1", Subject: subject, Hash: true,
			})
			if err != nil {
				return err
			}
			cost, err := options.Cost(message)
			if err != nil {
				return err
			}
			decision, err := options.Service.Admit(ctx, ratelimit.Request{
				Policy: options.Policy, Key: key, Cost: cost, Now: options.Now().UTC(),
			})
			if errors.Is(err, ratelimit.ErrRejected) {
				return &Deferred{RetryAfter: decision.RetryAfter, cause: err}
			}
			if err != nil {
				return err
			}
			return next.Handle(ctx, message)
		})
	}, nil
}

// ByQueueAndTenant derives an unambiguous composite subject.
func ByQueueAndTenant() SubjectFunc {
	return func(message Message) (ratelimit.Subject, error) {
		if message.Queue == "" || message.Tenant == "" {
			return ratelimit.Subject{}, fmt.Errorf("%w: queue and tenant are required", ratelimit.ErrInvalidKey)
		}
		value := strconv.Itoa(len(message.Queue)) + ":" + message.Queue + message.Tenant
		return ratelimit.Subject{Kind: "queue-tenant", Value: value}, nil
	}
}

// ByPrincipal derives a required principal subject.
func ByPrincipal() SubjectFunc {
	return func(message Message) (ratelimit.Subject, error) {
		if message.Principal == "" {
			return ratelimit.Subject{}, fmt.Errorf("%w: principal is required", ratelimit.ErrInvalidKey)
		}
		return ratelimit.Subject{Kind: "principal", Value: message.Principal}, nil
	}
}
