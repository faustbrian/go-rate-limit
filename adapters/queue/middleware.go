package ratelimitqueue

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
)

// Message contains bounded queue admission inputs, not a durable payload.
type Message struct {
	// ID is the durable message identifier.
	ID string
	// Queue is the bounded queue name.
	Queue string
	// Tenant is the bounded tenant identifier.
	Tenant string
	// Principal is the bounded authenticated subject.
	Principal string
	// Attempt is caller-owned delivery-attempt metadata.
	Attempt uint64
}

// Handler processes an admitted queue message without owning acknowledgement.
type Handler interface {
	Handle(context.Context, Message) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(context.Context, Message) error

// Handle invokes the function or rejects a nil function.
func (function HandlerFunc) Handle(ctx context.Context, message Message) error {
	if function == nil {
		return fmt.Errorf("%w: handler is required", ratelimit.ErrInvalidPolicy)
	}
	if nilInterface(ctx) {
		return fmt.Errorf("%w: context is required", ratelimit.ErrInvalidRequest)
	}
	return function(ctx, message)
}

// SubjectFunc derives a typed subject from queue metadata.
type SubjectFunc func(Message) (ratelimit.Subject, error)

// Options configures strict queue admission.
type Options struct {
	// Service performs strict admission.
	Service *ratelimit.StrictService
	// Policy is applied to every message.
	Policy ratelimit.Policy
	// Subject derives the required rate-limit subject.
	Subject SubjectFunc
	// Cost derives message cost; nil uses one.
	Cost func(Message) (uint64, error)
	// Now supplies request time; nil uses time.Now.
	Now func() time.Time
}

// Middleware holds validated immutable queue admission state.
type Middleware struct{ options Options }

// Deferred indicates the durable queue should retry after RetryAfter.
type Deferred struct {
	// RetryAfter is the bounded delay suggested by the decision.
	RetryAfter time.Duration
	cause      error
}

// Error returns the stable deferral message.
func (*Deferred) Error() string { return "rate-limited queue admission deferred" }

// Unwrap returns the normalized rejection cause.
func (deferred *Deferred) Unwrap() error {
	if deferred == nil || deferred.cause == nil {
		return ratelimit.ErrRejected
	}
	return deferred.cause
}

// New validates options without invoking a downstream handler.
func New(options Options) (*Middleware, error) {
	if options.Service == nil {
		return nil, fmt.Errorf("%w: strict service is required", ratelimit.ErrInvalidPolicy)
	}
	if options.Policy.ID() == "" {
		return nil, fmt.Errorf("%w: policy is required", ratelimit.ErrInvalidPolicy)
	}
	if options.Subject == nil {
		return nil, fmt.Errorf("%w: subject is required", ratelimit.ErrInvalidPolicy)
	}
	if options.Cost == nil {
		options.Cost = func(Message) (uint64, error) { return 1, nil }
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return &Middleware{options: options}, nil
}

// Wrap validates and wraps one downstream handler.
func (middleware *Middleware) Wrap(next Handler) (Handler, error) {
	if middleware == nil || middleware.options.Service == nil {
		return nil, fmt.Errorf("%w: middleware is nil or uninitialized", ratelimit.ErrInvalidPolicy)
	}
	if nilInterface(next) {
		return nil, fmt.Errorf("%w: handler is required", ratelimit.ErrInvalidPolicy)
	}
	options := middleware.options
	return HandlerFunc(func(ctx context.Context, message Message) error {
		if nilInterface(ctx) {
			return fmt.Errorf("%w: context is required", ratelimit.ErrInvalidRequest)
		}
		subject, err := options.Subject(message)
		if err != nil {
			return err
		}
		key, err := ratelimit.NewKey(ratelimit.KeySpec{Namespace: "queue", Version: "v1", Subject: subject, Hash: true})
		if err != nil {
			return err
		}
		cost, err := options.Cost(message)
		if err != nil {
			return err
		}
		decision, err := options.Service.Admit(ctx, ratelimit.Request{Policy: options.Policy, Key: key, Cost: cost, Now: options.Now().UTC()})
		if errors.Is(err, ratelimit.ErrRejected) {
			return &Deferred{RetryAfter: decision.RetryAfter, cause: err}
		}
		if err != nil {
			return err
		}
		return next.Handle(ctx, message)
	}), nil
}

// ByQueueAndTenant derives an unambiguous composite subject.
func ByQueueAndTenant() SubjectFunc {
	return func(message Message) (ratelimit.Subject, error) {
		if message.Queue == "" || message.Tenant == "" {
			return ratelimit.Subject{}, fmt.Errorf("%w: queue and tenant are required", ratelimit.ErrInvalidKey)
		}
		return ratelimit.Subject{Kind: "queue-tenant", Value: strconv.Itoa(len(message.Queue)) + ":" + message.Queue + message.Tenant}, nil
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

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
