package postgres

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/bits"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
)

const (
	stateSchema      = 1
	stateSegments    = 16
	microsecondNanos = int64(time.Microsecond)
)

type persistedState struct {
	Schema         int                             `json:"schema"`
	PolicyID       string                          `json:"policy_id"`
	Revision       string                          `json:"revision"`
	Algorithm      ratelimit.Algorithm             `json:"algorithm"`
	Tokens         uint64                          `json:"tokens"`
	Remainder      uint64                          `json:"remainder"`
	LastMicros     int64                           `json:"last_micros"`
	ObservedMicros int64                           `json:"observed_micros"`
	Window         int64                           `json:"window"`
	Used           uint64                          `json:"used"`
	Segments       [stateSegments]persistedSegment `json:"segments"`
	Leases         map[string]persistedLease       `json:"leases,omitempty"`
}

type persistedSegment struct {
	Index int64  `json:"index"`
	Used  uint64 `json:"used"`
}

type persistedLease struct {
	Cost          uint64 `json:"cost"`
	ExpiresMicros int64  `json:"expires_micros"`
}

func mutateLease(current *persistedState, request ratelimit.LeaseRequest, digest string) (*persistedState, ratelimit.Lease, ratelimit.Decision, error) {
	if current == nil {
		current = &persistedState{
			Schema: stateSchema, PolicyID: request.Request.Policy.ID(),
			Revision:  request.Request.Policy.Revision(),
			Algorithm: ratelimit.Concurrency,
			Leases:    make(map[string]persistedLease),
		}
	} else if current.Schema != stateSchema ||
		current.PolicyID != request.Request.Policy.ID() ||
		current.Algorithm != ratelimit.Concurrency {
		return nil, ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrCorrupt
	}
	if current.Leases == nil {
		current.Leases = make(map[string]persistedLease)
	}
	current.Revision = request.Request.Policy.Revision()
	now := request.Request.Now.UnixMicro()
	if now < current.ObservedMicros {
		now = current.ObservedMicros
		request.Request.Now = time.UnixMicro(now)
	}
	current.ObservedMicros = now
	var used uint64
	var earliest int64
	for key, lease := range current.Leases {
		if lease.ExpiresMicros <= now {
			delete(current.Leases, key)
			continue
		}
		if lease.Cost == 0 ||
			lease.Cost > ratelimit.MaxConcurrencyLeases-min(used, uint64(ratelimit.MaxConcurrencyLeases)) {
			return nil, ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrCorrupt
		}
		used += lease.Cost
		if earliest == 0 || lease.ExpiresMicros < earliest {
			earliest = lease.ExpiresMicros
		}
	}
	if existing, ok := current.Leases[digest]; ok {
		if existing.Cost != request.Request.Cost {
			return current, ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrLeaseNotOwned
		}
		lease := postgresLease(request, time.UnixMicro(existing.ExpiresMicros))
		return current, lease, ratelimit.Decision{
			Allowed: true, Limit: request.Request.Policy.Limit(),
			Remaining: request.Request.Policy.Limit() - min(used, request.Request.Policy.Limit()),
			Reset:     lease.ExpiresAt, Reason: ratelimit.ReasonAllowed,
		}, nil
	}
	remaining := request.Request.Policy.Limit() - min(used, request.Request.Policy.Limit())
	if request.Request.Cost > remaining {
		reset := time.UnixMicro(earliest)
		return current, ratelimit.Lease{}, ratelimit.Decision{
			Allowed: false, Limit: request.Request.Policy.Limit(),
			Remaining: remaining, Reset: reset,
			RetryAfter: max(reset.Sub(request.Request.Now), time.Duration(0)),
			Reason:     ratelimit.ReasonLimited,
		}, ratelimit.ErrRejected
	}
	expiresAt := request.Request.Now.Add(request.Request.Policy.LeaseDuration())
	current.Leases[digest] = persistedLease{
		Cost: request.Request.Cost, ExpiresMicros: expiresAt.UnixMicro(),
	}
	lease := postgresLease(request, expiresAt)
	return current, lease, ratelimit.Decision{
		Allowed: true, Limit: request.Request.Policy.Limit(),
		Remaining: remaining - request.Request.Cost,
		Reset:     expiresAt, Reason: ratelimit.ReasonAllowed,
	}, nil
}

func postgresLease(request ratelimit.LeaseRequest, expiresAt time.Time) ratelimit.Lease {
	return ratelimit.Lease{
		ID: request.LeaseID, Key: request.Request.Key,
		PolicyID:       request.Request.Policy.ID(),
		PolicyRevision: request.Request.Policy.Revision(),
		Cost:           request.Request.Cost, ExpiresAt: expiresAt, Backend: "postgres",
	}
}

func mutateState(current *persistedState, request ratelimit.Request) (*persistedState, ratelimit.Decision, error) {
	if current == nil {
		current = &persistedState{
			Schema: stateSchema, PolicyID: request.Policy.ID(),
			Revision: request.Policy.Revision(), Algorithm: request.Policy.Algorithm(),
			Tokens: request.Policy.Limit(), LastMicros: request.Now.UnixMicro(),
			ObservedMicros: request.Now.UnixMicro(),
		}
	} else if current.Schema != stateSchema || current.PolicyID != request.Policy.ID() ||
		current.Algorithm != request.Policy.Algorithm() {
		return nil, ratelimit.Decision{}, ratelimit.ErrCorrupt
	}
	if request.Now.UnixMicro() < current.ObservedMicros {
		request.Now = time.UnixMicro(current.ObservedMicros)
	}
	current.ObservedMicros = request.Now.UnixMicro()
	current.Revision = request.Policy.Revision()
	switch request.Policy.Algorithm() { //nolint:exhaustive // validated policy; concurrency rejected by Store
	case ratelimit.TokenBucket:
		decision, err := mutateToken(current, request)
		return current, decision, err
	case ratelimit.FixedWindow:
		decision, err := mutateFixed(current, request)
		return current, decision, err
	}
	decision, err := mutateSliding(current, request)
	return current, decision, err
}

func mutateToken(current *persistedState, request ratelimit.Request) (ratelimit.Decision, error) {
	now := request.Now.UnixMicro()
	period := uint64(request.Policy.Period().Microseconds())
	if now > current.LastMicros && current.Tokens < request.Policy.Limit() {
		elapsed := uint64(now - current.LastMicros)
		high, low := bits.Mul64(elapsed, request.Policy.Capacity())
		low, carry := bits.Add64(low, current.Remainder, 0)
		high += carry
		if high >= period {
			current.Tokens, current.Remainder = request.Policy.Limit(), 0
		} else {
			added, remainder := bits.Div64(high, low, period)
			if added >= request.Policy.Limit()-current.Tokens {
				current.Tokens, current.Remainder = request.Policy.Limit(), 0
			} else {
				current.Tokens += added
				current.Remainder = remainder
			}
		}
	}
	if now > current.LastMicros {
		current.LastMicros = now
	}
	limit := request.Policy.Limit()
	if current.Tokens < request.Cost {
		retry := tokenDuration(request.Cost-current.Tokens, current.Remainder, request.Policy)
		reset := tokenDuration(limit-current.Tokens, current.Remainder, request.Policy)
		return ratelimit.Decision{
			Allowed: false, Limit: limit, Remaining: current.Tokens,
			Reset: request.Now.Add(reset), RetryAfter: retry,
			Reason: ratelimit.ReasonLimited,
		}, ratelimit.ErrRejected
	}
	current.Tokens -= request.Cost
	reset := tokenDuration(limit-current.Tokens, current.Remainder, request.Policy)
	return ratelimit.Decision{
		Allowed: true, Limit: limit, Remaining: current.Tokens,
		Reset: request.Now.Add(reset), Reason: ratelimit.ReasonAllowed,
	}, nil
}

func tokenDuration(tokens, remainder uint64, policy ratelimit.Policy) time.Duration {
	if tokens == 0 {
		return 0
	}
	period := uint64(policy.Period().Microseconds())
	high, low := bits.Mul64(tokens, period)
	low, borrow := bits.Sub64(low, remainder, 0)
	high, _ = bits.Sub64(high, 0, borrow)
	if high >= policy.Capacity() {
		return time.Duration(math.MaxInt64)
	}
	micros, rest := bits.Div64(high, low, policy.Capacity())
	if rest != 0 {
		micros++
	}
	if micros > uint64(math.MaxInt64/microsecondNanos) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(micros * uint64(microsecondNanos))
}

func mutateFixed(current *persistedState, request ratelimit.Request) (ratelimit.Decision, error) {
	period := request.Policy.Period().Microseconds()
	window := floor(request.Now.UnixMicro(), period)
	if current.Window != window {
		current.Window, current.Used = window, 0
	}
	return consume(current, request, time.UnixMicro(window+period))
}

func mutateSliding(current *persistedState, request ratelimit.Request) (ratelimit.Decision, error) {
	period := request.Policy.Period().Microseconds()
	width := (period + stateSegments - 1) / stateSegments
	index := floor(request.Now.UnixMicro(), width) / width
	oldest := floor(request.Now.Add(-request.Policy.Period()).UnixMicro(), width) / width
	current.Used = 0
	earliest := int64(math.MaxInt64)
	for slot := range current.Segments {
		if current.Segments[slot].Index <= oldest {
			current.Segments[slot] = persistedSegment{}
			continue
		}
		current.Used += current.Segments[slot].Used
		if current.Segments[slot].Used > 0 && current.Segments[slot].Index < earliest {
			earliest = current.Segments[slot].Index
		}
	}
	slot := positiveRemainder(index, stateSegments)
	if current.Segments[slot].Index != index {
		current.Segments[slot] = persistedSegment{Index: index}
	}
	reset := request.Now.Add(request.Policy.Period())
	if earliest != math.MaxInt64 {
		reset = time.UnixMicro((earliest + 1) * width).Add(request.Policy.Period())
	}
	decision, err := consume(current, request, reset)
	if err == nil {
		current.Segments[slot].Used += request.Cost
	}
	return decision, err
}

func consume(current *persistedState, request ratelimit.Request, reset time.Time) (ratelimit.Decision, error) {
	limit := request.Policy.Limit()
	used := min(current.Used, limit)
	remaining := limit - used
	if request.Cost > remaining {
		retry := reset.Sub(request.Now)
		if retry < 0 {
			retry = 0
		}
		return ratelimit.Decision{
			Allowed: false, Limit: limit, Remaining: remaining,
			Reset: reset, RetryAfter: retry, Reason: ratelimit.ReasonLimited,
		}, ratelimit.ErrRejected
	}
	current.Used += request.Cost
	return ratelimit.Decision{
		Allowed: true, Limit: limit, Remaining: limit - current.Used,
		Reset: reset, Reason: ratelimit.ReasonAllowed,
	}, nil
}

func encodeState(state *persistedState) []byte {
	encoded, _ := json.Marshal(state)
	return encoded
}

func decodeState(encoded []byte) (*persistedState, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var state persistedState
	if err := decoder.Decode(&state); err != nil {
		return nil, fmt.Errorf("%w: decode state: %w", ratelimit.ErrCorrupt, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("%w: trailing state data", ratelimit.ErrCorrupt)
	}
	if state.Schema != stateSchema || state.PolicyID == "" || state.Algorithm == "" {
		return nil, fmt.Errorf("%w: invalid state identity", ratelimit.ErrCorrupt)
	}
	return &state, nil
}

func floor(value, size int64) int64 {
	quotient := value / size
	if value < 0 && value%size != 0 {
		quotient--
	}
	return quotient * size
}

func positiveRemainder(value int64, modulus int) int {
	result := value % int64(modulus)
	if result < 0 {
		result += int64(modulus)
	}
	return int(result)
}
