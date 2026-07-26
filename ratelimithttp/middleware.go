package ratelimithttp

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
)

// Options configures inbound HTTP admission.
type Options struct {
	// Service makes admission decisions.
	Service *ratelimit.Service
	// Policy applies to each request.
	Policy ratelimit.Policy
	// Now supplies explicit UTC time; nil uses time.Now.
	Now func() time.Time
	// Cost derives bounded request weight; nil returns one.
	Cost func(*http.Request) (uint64, error)
	// Key derives a bounded subject key; nil uses the hashed client IP.
	Key func(*http.Request) (ratelimit.Key, error)
	// ClientIP configures trusted proxies for the default Key function.
	ClientIP ClientIPOptions
}

// Middleware wraps an HTTP handler with inbound admission.
type Middleware func(http.Handler) http.Handler

// New validates options and returns HTTP admission middleware.
func New(options Options) (Middleware, error) {
	if options.Service == nil || options.Policy.ID() == "" {
		return nil, fmt.Errorf("%w: service and policy are required", ratelimit.ErrInvalidPolicy)
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
			return ratelimit.NewKey(ratelimit.KeySpec{
				Namespace: "http", Version: "v1",
				Subject: ratelimit.Subject{Kind: "ip", Value: address.String()},
				Hash:    true,
			})
		}
	}
	return func(next http.Handler) http.Handler {
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
			decision, err := options.Service.Admit(request.Context(), ratelimit.Request{
				Policy: options.Policy, Key: key, Cost: cost, Now: now,
			})
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
		})
	}, nil
}

func writeHeaders(header http.Header, decision ratelimit.Decision, now time.Time) {
	header.Set("RateLimit-Limit", strconv.FormatUint(decision.Limit, 10))
	header.Set("RateLimit-Remaining", strconv.FormatUint(decision.Remaining, 10))
	reset := ceilSeconds(decision.Reset.Sub(now))
	header.Set("RateLimit-Reset", strconv.FormatInt(reset, 10))
	if decision.RetryAfter > 0 {
		header.Set("Retry-After", strconv.FormatInt(ceilSeconds(decision.RetryAfter), 10))
	}
}

func ceilSeconds(duration time.Duration) int64 {
	if duration <= 0 {
		return 0
	}
	return int64(math.Ceil(duration.Seconds()))
}
