package postgres

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/bits"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
)

const (
	stateSchema      = 1
	stateSegments    = 16
	microsecondNanos = int64(time.Microsecond)
	maxExactMicros   = int64(9_007_199_254_740_991)
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
	PeriodMicros   int64                           `json:"-"`
	Carried        bool                            `json:"-"`
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
	return mutateLeaseMode(current, request, digest, true)
}

func mutateLeaseLegacy(current *persistedState, request ratelimit.LeaseRequest, digest string) (*persistedState, ratelimit.Lease, ratelimit.Decision, error) {
	return mutateLeaseMode(current, request, digest, false)
}

func mutateLeaseMode(current *persistedState, request ratelimit.LeaseRequest, digest string, strict bool) (*persistedState, ratelimit.Lease, ratelimit.Decision, error) {
	if current == nil {
		current = &persistedState{
			Schema: stateSchema, PolicyID: request.Request.Policy.ID(),
			Revision:  request.Request.Policy.Revision(),
			Algorithm: ratelimit.Concurrency,
			Leases:    make(map[string]persistedLease),
		}
	} else {
		if current.Schema != stateSchema {
			return nil, ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrCorrupt
		}
		if current.PolicyID != request.Request.Policy.ID() {
			return nil, ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrCorrupt
		}
		if current.Algorithm != ratelimit.Concurrency {
			return nil, ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrCorrupt
		}
		if strict {
			if err := validateConcurrencyState(current, request.Request.Policy.ID()); err != nil {
				return nil, ratelimit.Lease{}, ratelimit.Decision{}, err
			}
		}
	}
	if current.Leases == nil {
		current.Leases = make(map[string]persistedLease)
	}
	now := request.Request.Now.UnixMicro()
	requestedNow := now
	now = max(now, current.ObservedMicros)
	if now != requestedNow {
		request.Request.Now = time.UnixMicro(now).UTC()
	}
	if strict {
		if err := validateServerClockRange(request.Request.Now, request.Request.Policy.LeaseDuration()); err != nil {
			return nil, ratelimit.Lease{}, ratelimit.Decision{}, err
		}
	}
	current.Revision = request.Request.Policy.Revision()
	current.ObservedMicros = now
	var used uint64
	earliest := int64(math.MaxInt64)
	for key, lease := range current.Leases {
		if lease.ExpiresMicros <= now {
			delete(current.Leases, key)
		} else {
			if lease.Cost == 0 {
				return nil, ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrCorrupt
			}
			if lease.Cost > ratelimit.MaxConcurrencyLeases {
				return nil, ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrCorrupt
			}
			if used > ratelimit.MaxConcurrencyLeases-lease.Cost {
				return nil, ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrCorrupt
			}
			used += lease.Cost
			earliest = min(earliest, lease.ExpiresMicros)
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
	return mutateStateMode(current, request, true)
}

func mutateStateLegacy(current *persistedState, request ratelimit.Request) (*persistedState, ratelimit.Decision, error) {
	return mutateStateMode(current, request, false)
}

func mutateStateMode(current *persistedState, request ratelimit.Request, strict bool) (*persistedState, ratelimit.Decision, error) {
	if strict && request.Policy.Algorithm() == ratelimit.FixedWindow &&
		!validPersistedMicros(floor(request.Now.UnixMicro(), request.Policy.Period().Microseconds())) {
		return nil, ratelimit.Decision{}, ratelimit.ErrOverflow
	}
	if current == nil {
		current = &persistedState{
			Schema: stateSchema, PolicyID: request.Policy.ID(),
			Revision: request.Policy.Revision(), Algorithm: request.Policy.Algorithm(),
			Tokens: request.Policy.Limit(), LastMicros: request.Now.UnixMicro(),
			ObservedMicros: request.Now.UnixMicro(),
		}
		if strict {
			recordWindowPeriod(current, request.Policy)
		}
	} else {
		if current.Schema != stateSchema {
			return nil, ratelimit.Decision{}, ratelimit.ErrCorrupt
		}
		if current.PolicyID != request.Policy.ID() {
			return nil, ratelimit.Decision{}, ratelimit.ErrCorrupt
		}
		if current.Algorithm != request.Policy.Algorithm() {
			return nil, ratelimit.Decision{}, ratelimit.ErrCorrupt
		}
		if strict {
			if !validPersistedMicros(current.ObservedMicros) {
				return nil, ratelimit.Decision{}, ratelimit.ErrCorrupt
			}
			switch current.Algorithm { //nolint:exhaustive // identity validation above restricts the value to the requested algorithm.
			case ratelimit.TokenBucket:
				if !validPersistedMicros(current.LastMicros) || current.Remainder > uint64(maxExactMicros) ||
					current.Revision == request.Policy.Revision() && current.Remainder >= uint64(request.Policy.Period().Microseconds()) {
					return nil, ratelimit.Decision{}, ratelimit.ErrCorrupt
				}
				if current.Revision == request.Policy.Revision() && current.Tokens > request.Policy.Limit() {
					return nil, ratelimit.Decision{}, ratelimit.ErrCorrupt
				}
			case ratelimit.FixedWindow:
				if !validPersistedMicros(current.Window) || current.Used > uint64(maxExactMicros) ||
					current.Revision == request.Policy.Revision() && current.Used > request.Policy.Limit() && !current.Carried {
					return nil, ratelimit.Decision{}, ratelimit.ErrCorrupt
				}
			case ratelimit.SlidingWindow:
				for _, segment := range current.Segments {
					if !validPersistedMicros(segment.Index) || segment.Used > uint64(maxExactMicros) {
						return nil, ratelimit.Decision{}, ratelimit.ErrCorrupt
					}
				}
			}
		}
	}
	requestedMicros := request.Now.UnixMicro()
	observedMicros := max(requestedMicros, current.ObservedMicros)
	if observedMicros != requestedMicros {
		request.Now = time.UnixMicro(observedMicros).UTC()
	}
	if strict {
		if err := validateServerClockRange(request.Now, request.Policy.Period()); err != nil {
			return nil, ratelimit.Decision{}, err
		}
		if usesWindowMetadata(request.Policy.Algorithm()) {
			if current.PeriodMicros != 0 && current.PeriodMicros != request.Policy.Period().Microseconds() {
				return nil, ratelimit.Decision{}, ratelimit.ErrCorrupt
			}
		}
		if request.Policy.Algorithm() == ratelimit.SlidingWindow {
			if err := validateSlidingPositions(current, request); err != nil {
				return nil, ratelimit.Decision{}, err
			}
			used, overflow := strictSlidingUsage(current, request)
			if overflow || current.Revision == request.Policy.Revision() && usageExceedsLimit(used, request.Policy.Limit()) && !current.Carried {
				return nil, ratelimit.Decision{}, ratelimit.ErrCorrupt
			}
		}
		if request.Policy.Algorithm() == ratelimit.TokenBucket && current.Revision != request.Policy.Revision() {
			current.Tokens = min(current.Tokens, request.Policy.Limit())
			current.Remainder = 0
			current.LastMicros = max(current.LastMicros, request.Now.UnixMicro())
		}
		recordWindowPeriod(current, request.Policy)
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
	if !strict {
		decision, err := mutateSlidingLegacy(current, request)
		return current, decision, err
	}
	decision, err := mutateSliding(current, request)
	return current, decision, err
}

func validPersistedMicros(value int64) bool {
	return value >= -maxExactMicros && value <= maxExactMicros
}

func usageExceedsLimit(used, limit uint64) bool {
	return used > limit
}

func usesWindowMetadata(algorithm ratelimit.Algorithm) bool {
	switch algorithm { //nolint:exhaustive // Only fixed and sliding windows use the compatibility metadata envelope.
	case ratelimit.FixedWindow, ratelimit.SlidingWindow:
		return true
	default:
		return false
	}
}

func recordWindowPeriod(state *persistedState, policy ratelimit.Policy) {
	switch policy.Algorithm() { //nolint:exhaustive // Only fixed and sliding windows persist a period.
	case ratelimit.FixedWindow, ratelimit.SlidingWindow:
		state.PeriodMicros = policy.Period().Microseconds()
	}
}

func strictSlidingUsage(current *persistedState, request ratelimit.Request) (uint64, bool) {
	period := request.Policy.Period().Microseconds()
	width := (period + stateSegments - 1) / stateSegments
	oldest := floor(request.Now.Add(-request.Policy.Period()).UnixMicro(), width) / width
	used := uint64(0)
	for _, segment := range current.Segments {
		if segment.Index <= oldest {
			continue
		}
		if segment.Used > uint64(maxExactMicros)-used {
			return 0, true
		}
		used += segment.Used
	}
	return used, false
}

func validateSlidingPositions(current *persistedState, request ratelimit.Request) error {
	period := request.Policy.Period().Microseconds()
	width := (period + stateSegments - 1) / stateSegments
	currentIndex := floor(request.Now.UnixMicro(), width) / width
	oldest := floor(request.Now.Add(-request.Policy.Period()).UnixMicro(), width) / width
	for slot, segment := range current.Segments {
		if segment.Used == 0 || segment.Index <= oldest {
			continue
		}
		if segment.Index > currentIndex || positiveRemainder(segment.Index, stateSegments) != slot {
			return ratelimit.ErrCorrupt
		}
	}
	return nil
}

func validateConcurrencyState(current *persistedState, policyID string) error {
	if current.Schema != stateSchema {
		return ratelimit.ErrCorrupt
	}
	if current.PolicyID != policyID {
		return ratelimit.ErrCorrupt
	}
	if current.Algorithm != ratelimit.Concurrency {
		return ratelimit.ErrCorrupt
	}
	if !validPersistedMicros(current.ObservedMicros) {
		return ratelimit.ErrCorrupt
	}
	var total uint64
	for digest, lease := range current.Leases {
		if !validLeaseDigest(digest) {
			return ratelimit.ErrCorrupt
		}
		if lease.Cost == 0 {
			return ratelimit.ErrCorrupt
		}
		if lease.Cost > ratelimit.MaxConcurrencyLeases {
			return ratelimit.ErrCorrupt
		}
		if lease.Cost > ratelimit.MaxConcurrencyLeases-total {
			return ratelimit.ErrCorrupt
		}
		if !validPersistedMicros(lease.ExpiresMicros) {
			return ratelimit.ErrCorrupt
		}
		total += lease.Cost
	}
	return nil
}

func validLeaseDigest(digest string) bool {
	if len(digest) != sha256.Size*2 {
		return false
	}
	for index := range len(digest) {
		if (digest[index] < '0' || digest[index] > '9') && (digest[index] < 'a' || digest[index] > 'f') {
			return false
		}
	}
	return true
}

func mutateToken(current *persistedState, request ratelimit.Request) (ratelimit.Decision, error) {
	now := request.Now.UnixMicro()
	period := uint64(request.Policy.Period().Microseconds())
	elapsedDuration := time.UnixMicro(now).Sub(time.UnixMicro(current.LastMicros))
	if elapsedDuration >= time.Microsecond {
		if current.Tokens < request.Policy.Limit() {
			elapsed := uint64(elapsedDuration.Microseconds())
			high, low := bits.Mul64(elapsed, request.Policy.Capacity())
			low, carry := bits.Add64(low, current.Remainder, 0)
			high, _ = bits.Add64(high, carry, 0)
			if high >= period {
				current.Tokens, current.Remainder = request.Policy.Limit(), 0
			} else {
				added, remainder := bits.Div64(high, low, period)
				gap := request.Policy.Limit() - current.Tokens
				if added >= gap {
					current.Tokens, current.Remainder = request.Policy.Limit(), 0
				} else {
					current.Tokens += added
					current.Remainder = remainder
				}
			}
		}
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
	decision, err := consume(current, request, time.UnixMicro(window+period))
	current.Carried = usageExceedsLimit(current.Used, request.Policy.Limit())
	return decision, err
}

func mutateSliding(current *persistedState, request ratelimit.Request) (ratelimit.Decision, error) {
	period := request.Policy.Period().Microseconds()
	width := (period + stateSegments - 1) / stateSegments
	index := floor(request.Now.UnixMicro(), width) / width
	oldest := floor(request.Now.Add(-request.Policy.Period()).UnixMicro(), width) / width
	used := uint64(0)
	earliest := int64(math.MaxInt64)
	for slot := range current.Segments {
		if current.Segments[slot].Index > oldest {
			used += current.Segments[slot].Used
			if current.Segments[slot].Used != 0 {
				earliest = min(earliest, current.Segments[slot].Index)
			}
		}
	}
	for slot := range current.Segments {
		if current.Segments[slot].Index <= oldest {
			current.Segments[slot] = persistedSegment{}
		}
	}
	current.Used = used
	current.Carried = usageExceedsLimit(used, request.Policy.Limit())
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

func mutateSlidingLegacy(current *persistedState, request ratelimit.Request) (ratelimit.Decision, error) {
	period := request.Policy.Period().Microseconds()
	width := (period + stateSegments - 1) / stateSegments
	index := floor(request.Now.UnixMicro(), width) / width
	oldest := floor(request.Now.Add(-request.Policy.Period()).UnixMicro(), width) / width
	current.Used = 0
	earliest := int64(math.MaxInt64)
	for slot := range current.Segments {
		if current.Segments[slot].Index <= oldest {
			current.Segments[slot] = persistedSegment{}
		} else {
			current.Used += current.Segments[slot].Used
			if current.Segments[slot].Used != 0 {
				earliest = min(earliest, current.Segments[slot].Index)
			}
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
		retry := max(reset.Sub(request.Now), time.Duration(0))
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
	wire := *state
	if usesWindowMetadata(state.Algorithm) {
		if state.PeriodMicros != 0 {
			wire.Remainder = uint64(state.PeriodMicros)
			wire.LastMicros = 0
			if state.Carried {
				wire.LastMicros = 1
			}
		}
	}
	encoded, _ := json.Marshal(&wire)
	return encoded
}

type persistedStateError struct{ cause error }

func (err *persistedStateError) Error() string { return err.cause.Error() }

func (err *persistedStateError) Unwrap() error { return err.cause }

func decodeState(encoded []byte) (*persistedState, error) { return decodeStateMode(encoded, true) }

func decodeStateLegacy(encoded []byte) (*persistedState, error) {
	return decodeStateMode(encoded, false)
}

func decodeStateMode(encoded []byte, strict bool) (*persistedState, error) {
	if strict {
		var shape struct {
			Segments []json.RawMessage `json:"segments"`
		}
		if err := json.Unmarshal(encoded, &shape); err != nil {
			return nil, stateDecodeError(strict, fmt.Errorf("%w: decode state: %w", ratelimit.ErrCorrupt, err))
		}
		if len(shape.Segments) != stateSegments {
			return nil, stateDecodeError(strict, fmt.Errorf("%w: invalid segment shape", ratelimit.ErrCorrupt))
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var state persistedState
	if err := decoder.Decode(&state); err != nil {
		return nil, stateDecodeError(strict, fmt.Errorf("%w: decode state: %w", ratelimit.ErrCorrupt, err))
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, stateDecodeError(strict, fmt.Errorf("%w: trailing state data", ratelimit.ErrCorrupt))
	}
	if state.Schema != stateSchema {
		return nil, stateDecodeError(strict, fmt.Errorf("%w: invalid state identity", ratelimit.ErrCorrupt))
	}
	if state.PolicyID == "" {
		return nil, stateDecodeError(strict, fmt.Errorf("%w: invalid state identity", ratelimit.ErrCorrupt))
	}
	if state.Algorithm == "" {
		return nil, stateDecodeError(strict, fmt.Errorf("%w: invalid state identity", ratelimit.ErrCorrupt))
	}
	if strict && usesWindowMetadata(state.Algorithm) {
		if state.Remainder != 0 {
			if state.Remainder > uint64(maxExactMicros) || (state.LastMicros != 0 && state.LastMicros != 1) {
				return nil, stateDecodeError(strict, fmt.Errorf("%w: invalid window metadata", ratelimit.ErrCorrupt))
			}
			state.PeriodMicros = int64(state.Remainder)
			state.Carried = state.LastMicros == 1
		}
	}
	return &state, nil
}

func stateDecodeError(strict bool, err error) error {
	if strict {
		return &persistedStateError{cause: err}
	}
	return err
}

func floor(value, size int64) int64 {
	quotient := value / size
	if value%size == 0 {
		return quotient * size
	}
	if value>>63 == -1 {
		quotient--
	}
	return quotient * size
}

func positiveRemainder(value int64, modulus int) int {
	result := value % int64(modulus)
	if result>>63 == -1 {
		result += int64(modulus)
	}
	return int(result)
}
