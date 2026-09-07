package ratelimithttp

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"reflect"
	"strconv"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
)

// Options configures strict inbound HTTP admission.
type Options struct {
	// Service performs strict admission.
	Service *ratelimit.StrictService
	// Policy is applied to every request.
	Policy ratelimit.Policy
	// Now supplies request time; nil uses time.Now.
	Now func() time.Time
	// Cost derives request cost; nil uses one.
	Cost func(*http.Request) (uint64, error)
	// Key derives the admission key; nil uses the validated client IP.
	Key func(*http.Request) (ratelimit.Key, error)
	// ClientIP configures trusted proxy handling for the default key function.
	ClientIP ClientIPOptions
}

// Middleware holds validated immutable HTTP admission state.
type Middleware struct{ options Options }

// New validates options without invoking a downstream handler.
func New(options Options) (*Middleware, error) {
	if options.Service == nil {
		return nil, fmt.Errorf("%w: strict service is required", ratelimit.ErrInvalidPolicy)
	}
	if options.Policy.ID() == "" {
		return nil, fmt.Errorf("%w: policy is required", ratelimit.ErrInvalidPolicy)
	}
	extractor, err := NewClientIPExtractor(options.ClientIP)
	if err != nil {
		return nil, err
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.Cost == nil {
		options.Cost = func(*http.Request) (uint64, error) { return 1, nil }
	}
	if options.Key == nil {
		options.Key = func(request *http.Request) (ratelimit.Key, error) {
			address, err := extractor.ClientIP(request)
			if err != nil {
				return ratelimit.Key{}, err
			}
			return ratelimit.NewKey(ratelimit.KeySpec{Namespace: "http", Version: "v1", Subject: ratelimit.Subject{Kind: "ip", Value: address.String()}, Hash: true})
		}
	}
	return &Middleware{options: options}, nil
}

// Wrap validates and wraps one downstream handler.
func (middleware *Middleware) Wrap(next http.Handler) (http.Handler, error) {
	if middleware == nil || middleware.options.Service == nil {
		return nil, fmt.Errorf("%w: middleware is nil or uninitialized", ratelimit.ErrInvalidPolicy)
	}
	if nilInterface(next) {
		return nil, fmt.Errorf("%w: handler is required", ratelimit.ErrInvalidPolicy)
	}
	options := middleware.options
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		key, err := options.Key(request)
		if err != nil {
			http.Error(writer, "invalid rate limit subject", http.StatusBadRequest)
			return
		}
		cost, err := options.Cost(request)
		if err != nil {
			http.Error(writer, "invalid rate limit cost", http.StatusBadRequest)
			return
		}
		now := options.Now().UTC()
		decision, err := options.Service.Admit(request.Context(), ratelimit.Request{Policy: options.Policy, Key: key, Cost: cost, Now: now})
		writeHeaders(writer.Header(), decision, now)
		if errors.Is(err, ratelimit.ErrRejected) {
			http.Error(writer, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		if err != nil {
			http.Error(writer, "rate limit unavailable", http.StatusServiceUnavailable)
			return
		}
		next.ServeHTTP(writer, request)
	}), nil
}

func writeHeaders(header http.Header, decision ratelimit.Decision, now time.Time) {
	header.Set("RateLimit-Limit", strconv.FormatUint(decision.Limit, 10))
	header.Set("RateLimit-Remaining", strconv.FormatUint(decision.Remaining, 10))
	header.Set("RateLimit-Reset", strconv.FormatInt(int64(math.Ceil(max(decision.Reset.Sub(now), 0).Seconds())), 10))
	if decision.RetryAfter > 0 {
		header.Set("Retry-After", strconv.FormatInt(int64(math.Ceil(decision.RetryAfter.Seconds())), 10))
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
