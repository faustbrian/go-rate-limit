package ratelimitrpc

import (
	"context"
	"errors"
	"fmt"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
)

const (
	// CodeRateLimited is returned when a rule rejects admission.
	CodeRateLimited = -32029
	// CodeRateUnavailable is returned when a rule cannot decide safely.
	CodeRateUnavailable = -32030
	// MaxRules bounds work and state fan-out for one call.
	MaxRules = 16
)

// Call contains bounded JSON-RPC admission inputs.
type Call struct {
	// ID is copied to the response without interpretation.
	ID any
	// Method is the requested JSON-RPC method.
	Method string
	// Principal is an optional authenticated principal identity.
	Principal string
	// Tenant is an optional tenant identity.
	Tenant string
}

// Error is a transport-safe JSON-RPC admission error.
type Error struct {
	// Code is CodeRateLimited or CodeRateUnavailable.
	Code int
	// Message omits policy and identity internals.
	Message string
	// RetryAfter is populated only for capacity rejection.
	RetryAfter time.Duration
}

// Response contains a JSON-RPC result or admission error.
type Response struct {
	// ID corresponds to Call.ID.
	ID any
	// Result is the application result.
	Result any
	// Error is non-nil when admission or handling failed at protocol level.
	Error *Error
}

// Handler processes an admitted JSON-RPC call.
type Handler interface {
	// Handle processes call and returns its response.
	Handle(context.Context, Call) (Response, error)
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(context.Context, Call) (Response, error)

// Handle calls function with ctx and call.
func (function HandlerFunc) Handle(ctx context.Context, call Call) (Response, error) {
	return function(ctx, call)
}

// SubjectFunc derives a typed subject without creating a persisted key.
type SubjectFunc func(Call) (ratelimit.Subject, error)

// Rule applies one policy to one derived subject and weighted cost.
type Rule struct {
	// Policy contains immutable admission semantics.
	Policy ratelimit.Policy
	// Subject derives global, principal, method, tenant, or custom identity.
	Subject SubjectFunc
	// Cost derives operation weight; nil returns one.
	Cost func(Call) (uint64, error)
}

// Options configures ordered JSON-RPC admission rules.
type Options struct {
	// Service makes admission decisions.
	Service *ratelimit.Service
	// Rules run in order and are bounded by MaxRules.
	Rules []Rule
	// Now supplies explicit UTC time; nil uses time.Now.
	Now func() time.Time
}

// Middleware wraps a JSON-RPC Handler with ordered admission rules.
type Middleware func(Handler) Handler

// New validates and copies options before constructing middleware.
func New(options Options) (Middleware, error) {
	if options.Service == nil || len(options.Rules) == 0 || len(options.Rules) > MaxRules {
		return nil, fmt.Errorf("%w: service and bounded rules are required", ratelimit.ErrInvalidPolicy)
	}
	rules := append([]Rule(nil), options.Rules...)
	for index := range rules {
		if rules[index].Policy.ID() == "" || rules[index].Subject == nil {
			return nil, fmt.Errorf("%w: rule %d is incomplete", ratelimit.ErrInvalidPolicy, index)
		}
		if rules[index].Cost == nil {
			rules[index].Cost = func(Call) (uint64, error) { return 1, nil }
		}
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return func(next Handler) Handler {
		return HandlerFunc(func(ctx context.Context, call Call) (Response, error) {
			for _, rule := range rules {
				subject, err := rule.Subject(call)
				if err != nil {
					return Response{ID: call.ID, Error: &Error{
						Code: CodeRateUnavailable, Message: "invalid rate limit subject",
					}}, err
				}
				key, err := ratelimit.NewKey(ratelimit.KeySpec{
					Namespace: "rpc", Version: "v1", Subject: subject, Hash: true,
				})
				if err != nil {
					return Response{ID: call.ID, Error: &Error{
						Code: CodeRateUnavailable, Message: "invalid rate limit subject",
					}}, err
				}
				cost, err := rule.Cost(call)
				if err != nil {
					return Response{ID: call.ID, Error: &Error{
						Code: CodeRateUnavailable, Message: "invalid rate limit cost",
					}}, err
				}
				decision, err := options.Service.Admit(ctx, ratelimit.Request{
					Policy: rule.Policy, Key: key, Cost: cost, Now: options.Now().UTC(),
				})
				if errors.Is(err, ratelimit.ErrRejected) {
					return Response{ID: call.ID, Error: &Error{
						Code: CodeRateLimited, Message: "rate limit exceeded",
						RetryAfter: decision.RetryAfter,
					}}, err
				}
				if err != nil {
					return Response{ID: call.ID, Error: &Error{
						Code: CodeRateUnavailable, Message: "rate limit unavailable",
					}}, err
				}
			}
			return next.Handle(ctx, call)
		})
	}, nil
}

// Global returns a subject function using a required shared identity.
func Global(value string) SubjectFunc {
	return func(Call) (ratelimit.Subject, error) {
		return requiredSubject("global", value)
	}
}

// Principal returns a subject function using Call.Principal.
func Principal() SubjectFunc {
	return func(call Call) (ratelimit.Subject, error) {
		return requiredSubject("principal", call.Principal)
	}
}

// Method returns a subject function using Call.Method.
func Method() SubjectFunc {
	return func(call Call) (ratelimit.Subject, error) {
		return requiredSubject("method", call.Method)
	}
}

// Tenant returns a subject function using Call.Tenant.
func Tenant() SubjectFunc {
	return func(call Call) (ratelimit.Subject, error) {
		return requiredSubject("tenant", call.Tenant)
	}
}

func requiredSubject(kind, value string) (ratelimit.Subject, error) {
	if value == "" {
		return ratelimit.Subject{}, fmt.Errorf("%w: missing %s", ratelimit.ErrInvalidKey, kind)
	}
	return ratelimit.Subject{Kind: kind, Value: value}, nil
}
