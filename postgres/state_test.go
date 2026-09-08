package postgres

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
)

func TestMutateStatePreservesWeightedTokenArithmetic(t *testing.T) {
	t.Parallel()

	request := postgresTokenRequest(t, time.Unix(10, 0), 3)
	state, decision, err := mutateState(nil, request)
	if err != nil || !decision.Allowed || decision.Remaining != 3 {
		t.Fatalf("first = %+v, %+v, %v", state, decision, err)
	}
	request.Now = request.Now.Add(250 * time.Millisecond)
	state, decision, err = mutateState(state, request)
	if err != nil || !decision.Allowed || decision.Remaining != 1 {
		t.Fatalf("second = %+v, %+v, %v", state, decision, err)
	}
	_, decision, err = mutateState(state, request)
	if !errors.Is(err, ratelimit.ErrRejected) ||
		decision.RetryAfter != 500*time.Millisecond {
		t.Fatalf("rejected = %+v, %v", decision, err)
	}
}

func TestDecodeStateRejectsForeignSchema(t *testing.T) {
	t.Parallel()

	if _, err := decodeState([]byte(`{"schema":2}`)); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("decodeState() error = %v", err)
	}
}

func TestDecodeStateRejectsNonExactSegmentShape(t *testing.T) {
	state := &persistedState{Schema: stateSchema, PolicyID: "shape", Revision: "v1", Algorithm: ratelimit.SlidingWindow}
	var document map[string]any
	if err := json.Unmarshal(encodeState(state), &document); err != nil {
		t.Fatal(err)
	}
	segments := document["segments"].([]any)
	document["segments"] = append(segments, map[string]any{"index": 1, "used": 1})
	encoded, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeState(encoded); !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("decodeState(extra segment) error = %v", err)
	}
	if legacy, err := decodeStateLegacy(encoded); err != nil || legacy == nil {
		t.Fatalf("decodeStateLegacy(extra segment) = %+v, %v", legacy, err)
	}
}

func TestDecodeStateRejectsMalformedDocumentsAndIdentity(t *testing.T) {
	valid := &persistedState{
		Schema: stateSchema, PolicyID: "identity", Revision: "v1", Algorithm: ratelimit.FixedWindow,
	}
	var unknown map[string]any
	if err := json.Unmarshal(encodeState(valid), &unknown); err != nil {
		t.Fatal(err)
	}
	unknown["unknown"] = true
	unknownDocument, err := json.Marshal(unknown)
	if err != nil {
		t.Fatal(err)
	}
	missingPolicy := *valid
	missingPolicy.PolicyID = ""
	missingAlgorithm := *valid
	missingAlgorithm.Algorithm = ""
	for _, test := range []struct {
		name     string
		document []byte
		want     string
	}{
		{name: "malformed JSON", document: []byte(`{`), want: "decode state"},
		{name: "unknown field", document: unknownDocument, want: "decode state"},
		{name: "trailing document", document: append(encodeState(valid), []byte(` {}`)...), want: "decode state"},
		{name: "missing policy", document: encodeState(&missingPolicy), want: "invalid state identity"},
		{name: "missing algorithm", document: encodeState(&missingAlgorithm), want: "invalid state identity"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := decodeState(test.document)
			if !errors.Is(err, ratelimit.ErrCorrupt) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("decodeState() error = %v", err)
			}
		})
	}
	if _, err := decodeStateLegacy(append(encodeState(valid), []byte(` {}`)...)); !errors.Is(err, ratelimit.ErrCorrupt) || !strings.Contains(err.Error(), "trailing state data") {
		t.Fatalf("decodeStateLegacy(trailing document) error = %v", err)
	}
}

func TestStrictWindowStateRemainsReadableAndRoundTripsThroughReleasedV1(t *testing.T) {
	type releasedV1State struct {
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
	for _, algorithm := range []ratelimit.Algorithm{ratelimit.FixedWindow, ratelimit.SlidingWindow} {
		t.Run(string(algorithm), func(t *testing.T) {
			strictState := &persistedState{
				Schema: stateSchema, PolicyID: "v1-readable", Revision: "v2", Algorithm: algorithm,
				Tokens: 5, ObservedMicros: 100_000_000, Window: 60_000_000, Used: 5,
				PeriodMicros: 60_000_000, Carried: true,
			}
			encoded := encodeState(strictState)
			decoder := json.NewDecoder(bytes.NewReader(encoded))
			decoder.DisallowUnknownFields()
			var released releasedV1State
			if err := decoder.Decode(&released); err != nil {
				t.Fatalf("released v1 decode: %v", err)
			}
			reencoded, err := json.Marshal(&released)
			if err != nil {
				t.Fatal(err)
			}
			roundTripped, err := decodeState(reencoded)
			if err != nil {
				t.Fatalf("strict decode after released v1 round trip: %v", err)
			}
			if roundTripped.PeriodMicros != strictState.PeriodMicros || !roundTripped.Carried {
				t.Fatalf("strict metadata after released v1 round trip = %+v", roundTripped)
			}
		})
	}

	base := releasedV1State{
		Schema: stateSchema, PolicyID: "v1-metadata-boundary", Revision: "v1", Algorithm: ratelimit.FixedWindow,
		Tokens: 1, ObservedMicros: 100, Segments: [stateSegments]persistedSegment{},
	}
	for _, test := range []struct {
		name      string
		remainder uint64
		last      int64
		wantError bool
	}{
		{name: "exact period boundary", remainder: uint64(maxExactMicros)},
		{name: "period above boundary", remainder: uint64(maxExactMicros) + 1, wantError: true},
		{name: "invalid carried marker", remainder: 1, last: 2, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			document := base
			document.Remainder = test.remainder
			document.LastMicros = test.last
			encoded, err := json.Marshal(&document)
			if err != nil {
				t.Fatal(err)
			}
			strict, strictErr := decodeState(encoded)
			if test.wantError {
				if strict != nil || !errors.Is(strictErr, ratelimit.ErrCorrupt) {
					t.Fatalf("decodeState() = %+v, %v", strict, strictErr)
				}
			} else if strictErr != nil || strict == nil || strict.PeriodMicros != int64(test.remainder) || strict.Carried {
				t.Fatalf("decodeState() = %+v, %v", strict, strictErr)
			}
			if legacy, legacyErr := decodeStateLegacy(encoded); legacyErr != nil || legacy == nil {
				t.Fatalf("decodeStateLegacy() = %+v, %v", legacy, legacyErr)
			}
		})
	}

	tokenState := &persistedState{
		Schema: stateSchema, PolicyID: "token-wire", Revision: "v1", Algorithm: ratelimit.TokenBucket,
		Tokens: 3, Remainder: 7, LastMicros: 11, ObservedMicros: 11,
		PeriodMicros: 60_000_000, Carried: true,
	}
	var tokenWire releasedV1State
	if err := json.Unmarshal(encodeState(tokenState), &tokenWire); err != nil {
		t.Fatal(err)
	}
	if tokenWire.Remainder != 7 || tokenWire.LastMicros != 11 {
		t.Fatalf("token wire fields were repurposed = %+v", tokenWire)
	}

	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "legacy-window-wire", Revision: "v1", Algorithm: ratelimit.FixedWindow,
		Capacity: 2, MaxCost: 2, Period: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := postgresRequest(t)
	request.Policy = policy
	request.Now = time.Unix(100, 0)
	legacyState, _, err := mutateStateLegacy(nil, request)
	if err != nil || legacyState == nil || legacyState.PeriodMicros != 0 || legacyState.Carried {
		t.Fatalf("legacy window state = %+v, %v", legacyState, err)
	}
	var legacyWire releasedV1State
	if err := json.Unmarshal(encodeState(legacyState), &legacyWire); err != nil {
		t.Fatal(err)
	}
	if legacyWire.Remainder != 0 || legacyWire.LastMicros != request.Now.UnixMicro() {
		t.Fatalf("legacy window wire fields = %+v", legacyWire)
	}
	strictState, decision, err := mutateState(legacyState, request)
	if err != nil || strictState == nil || !decision.Allowed || strictState.PeriodMicros != policy.Period().Microseconds() {
		t.Fatalf("strict adoption of legacy window = %+v, %+v, %v", strictState, decision, err)
	}
}

func TestMutateLeasePrunesExpiryAndIsIdempotent(t *testing.T) {
	t.Parallel()

	request := concurrencyLeaseRequest(t, time.Unix(20, 0), "job-1", 1)
	digestOne := strings.Repeat("a", 64)
	digestTwo := strings.Repeat("b", 64)
	state, lease, decision, err := mutateLease(nil, request, digestOne)
	if err != nil || !decision.Allowed || lease.ID != "job-1" ||
		decision.Remaining != 1 {
		t.Fatalf("first = %+v, %+v, %+v, %v", state, lease, decision, err)
	}
	_, same, decision, err := mutateLease(state, request, digestOne)
	if err != nil || same.ExpiresAt != lease.ExpiresAt || decision.Remaining != 1 {
		t.Fatalf("idempotent = %+v, %+v, %v", same, decision, err)
	}
	request.LeaseID = "job-2"
	request.Request.Cost = 2
	if _, _, _, err := mutateLease(state, request, digestTwo); !errors.Is(err, ratelimit.ErrRejected) {
		t.Fatalf("contended mutateLease() error = %v", err)
	}
	request.Request.Now = lease.ExpiresAt
	if _, _, decision, err := mutateLease(state, request, digestTwo); err != nil ||
		!decision.Allowed {
		t.Fatalf("expired mutateLease() = %+v, %v", decision, err)
	}
}

func TestStateClockRollbackIsClamped(t *testing.T) {
	t.Parallel()

	request := postgresRequest(t)
	request.Now = time.Unix(120, 0)
	request.Cost = 5
	state, _, err := mutateState(nil, request)
	if err != nil {
		t.Fatal(err)
	}
	request.Now = time.Unix(60, 0)
	request.Cost = 1
	if _, _, err := mutateState(state, request); !errors.Is(err, ratelimit.ErrRejected) {
		t.Fatalf("rollback mutateState() error = %v", err)
	}
}

func TestStrictFixedWindowRejectsUnrepresentablePersistedBoundary(t *testing.T) {
	request := postgresTokenRequest(t, time.UnixMicro(-9_007_199_254_740_991), 1)
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "fixed-boundary", Revision: "v1", Algorithm: ratelimit.FixedWindow,
		Capacity: 1, MaxCost: 1, Period: 9_007_199_254_740_990 * time.Microsecond,
		Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	request.Policy = policy
	state, decision, err := mutateState(nil, request)
	if state != nil || decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrOverflow) {
		t.Fatalf("mutateState() = %+v, %+v, %v", state, decision, err)
	}
}

func TestStrictRevisionCarryResetsOldTokenRemainderBeforeNewPeriodValidation(t *testing.T) {
	request := postgresTokenRequest(t, time.UnixMicro(100), 1)
	v1, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "revision-period", Revision: "v1", Algorithm: ratelimit.TokenBucket,
		Capacity: 1, MaxCost: 1, Period: 10 * time.Microsecond,
		Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	request.Policy = v1
	state, _, err := mutateState(nil, request)
	if err != nil {
		t.Fatal(err)
	}
	request.Now = time.UnixMicro(105)
	if _, _, err = mutateState(state, request); !errors.Is(err, ratelimit.ErrRejected) || state.Remainder != 5 {
		t.Fatalf("persist fractional remainder = %+v, %v", state, err)
	}
	v2, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "revision-period", Revision: "v2", Algorithm: ratelimit.TokenBucket,
		Capacity: 1, MaxCost: 1, Period: time.Microsecond,
		Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	request.Policy = v2
	next, decision, err := mutateState(state, request)
	if !errors.Is(err, ratelimit.ErrRejected) || errors.Is(err, ratelimit.ErrCorrupt) ||
		decision != (ratelimit.Decision{Limit: 1, Reset: time.UnixMicro(106), RetryAfter: time.Microsecond, Reason: ratelimit.ReasonLimited}) {
		t.Fatalf("revision carry = %+v, %+v, %v", next, decision, err)
	}
	if next == nil || next.Revision != "v2" || next.Tokens != 0 || next.Remainder != 0 || next.LastMicros != 105 || next.ObservedMicros != 105 {
		t.Fatalf("revision carry state = %+v", next)
	}
}

func TestStrictConcurrencyRejectsNonDigestLeaseIdentity(t *testing.T) {
	request := concurrencyLeaseRequest(t, time.UnixMicro(100), "legitimate", 1)
	state := &persistedState{
		Schema: stateSchema, PolicyID: request.Request.Policy.ID(), Revision: "v1",
		Algorithm: ratelimit.Concurrency, ObservedMicros: request.Request.Now.UnixMicro(),
		Leases: map[string]persistedLease{"x": {Cost: 1, ExpiresMicros: request.Request.Now.Add(time.Second).UnixMicro()}},
	}
	next, lease, decision, err := mutateLease(state, request, "legitimate-digest")
	if next != nil || lease != (ratelimit.Lease{}) || decision != (ratelimit.Decision{}) || !errors.Is(err, ratelimit.ErrCorrupt) {
		t.Fatalf("mutateLease() = %+v, %+v, %+v, %v", next, lease, decision, err)
	}
}

func postgresTokenRequest(t *testing.T, now time.Time, cost uint64) ratelimit.Request {
	t.Helper()
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "token", Revision: "v1", Algorithm: ratelimit.TokenBucket,
		Capacity: 4, Burst: 2, Period: time.Second, MaxCost: 6,
		Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	key, err := ratelimit.NewKey(ratelimit.KeySpec{
		Namespace: "test", Version: "v1",
		Subject: ratelimit.Subject{Kind: "principal", Value: "42"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return ratelimit.Request{Policy: policy, Key: key, Cost: cost, Now: now}
}

func concurrencyLeaseRequest(t *testing.T, now time.Time, id string, cost uint64) ratelimit.LeaseRequest {
	t.Helper()
	policy, err := ratelimit.NewPolicy(ratelimit.PolicySpec{
		ID: "workers", Revision: "v1", Algorithm: ratelimit.Concurrency,
		Capacity: 2, MaxCost: 2, Lease: time.Second,
		Consistency: ratelimit.ConsistencyStrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	key, err := ratelimit.NewKey(ratelimit.KeySpec{
		Namespace: "test", Version: "v1",
		Subject: ratelimit.Subject{Kind: "queue", Value: "jobs"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return ratelimit.LeaseRequest{
		Request: ratelimit.Request{Policy: policy, Key: key, Cost: cost, Now: now},
		LeaseID: id,
	}
}
