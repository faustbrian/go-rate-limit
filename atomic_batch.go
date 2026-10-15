package ratelimit

import (
	"context"
	"fmt"
	"io"
)

// MaxAtomicBatchSize bounds one native transaction's admission dimensions.
const MaxAtomicBatchSize = 16

// ReasonAtomicRejected means another bucket exhausted the atomic request.
// No capacity was debited, including this otherwise sufficient bucket.
const ReasonAtomicRejected Reason = "atomic_rejected"

// AtomicBatchRequest is one stable, caller-owned debit command. CommandID is
// 1..128 ASCII HTTP-token bytes, unique within the backend deployment; it is
// correlation, not a secret or authority. Retain it and the unchanged ordered
// Requests across unknown outcomes. Requests has 1..16 distinct policy/key
// identities, strong fail-closed token buckets and one clock observation.
// Concurrent caller mutation is forbidden. Native owners must atomically bind
// the command to complete policy/key/cost intent, retain terminal decisions,
// reject changed intent and never redebit a matching committed/rejected command.
// Now is a client-clock observation, not a retry freshness lease; database-clock
// owners must not reset original decision time or include retry time in intent.
type AtomicBatchRequest struct {
	// CommandID is copied deployment-wide correlation, not permission to debit.
	CommandID string
	// Requests is borrowed for the call; order is part of original intent.
	Requests []Request
}

// AtomicBatchDecision is the original command's indivisible admission result.
// Only nil error plus Allowed permits protected work. Decisions preserve input
// order; successful entries describe committed debits. Rejected entries are all
// non-allowed: exhausted buckets have ReasonLimited; otherwise sufficient ones
// have ReasonAtomicRejected and unchanged observed capacity. No bucket was
// debited on ErrRejected. A command-only result with an error grants no capacity
// and may require reconciliation; it is not a known rollback. Copies returned
// by the service own their decision slice, with no backend alias.
type AtomicBatchDecision struct {
	// CommandID identifies the unchanged caller command, including on unknown.
	CommandID string
	// Allowed grants admission only together with a nil error.
	Allowed bool
	// Decisions is an owned input-ordered copy of the original bucket outcomes.
	Decisions []Decision
}

// StrictAtomicBatchBackend optionally extends a strict backend with a real
// all-or-nothing transaction and original-command outcome owner. It must bound
// IO, serialize overlapping buckets without deadlock, use one authoritative
// clock, preserve earlier input intent and return original terminal decisions
// on replay even after refill. Never emulate this with per-item Admit calls.
// Unknown COMMIT acknowledgment returns zero decision plus UnknownOutcome;
// resubmitting the same command queries/reconciles its retained durable outcome
// without a second debit. Before-dispatch failure declarations have StrictBackend
// semantics; backend errors must contain no input or underlying diagnostics.
// Implementations must neither retain/mutate the borrowed input slice nor mutate
// returned slices after return. No resource ownership transfers through this port.
type StrictAtomicBatchBackend interface {
	StrictBackend
	// AdmitAtomicStrict owns atomic admission and original-command reconciliation.
	AdmitAtomicStrict(context.Context, AtomicBatchRequest) (AtomicBatchDecision, error)
}

// Validate checks complete bounded intrinsic input before backend dispatch.
// It authenticates neither command ownership nor a durable replay intent.
func (r AtomicBatchRequest) Validate() error {
	if !atomicCommandID(r.CommandID) || len(r.Requests) < 1 || len(r.Requests) > MaxAtomicBatchSize {
		return ErrInvalidRequest
	}
	seen := make(map[string]bool, len(r.Requests))
	for _, request := range r.Requests {
		if request.Validate() != nil || !request.Now.Equal(r.Requests[0].Now) {
			return ErrInvalidRequest
		}
		if request.Policy.Algorithm() != TokenBucket || request.Policy.Consistency() != ConsistencyStrong || request.Policy.FailureMode() != FailClosed {
			return ErrUnsupported
		}
		key := request.Policy.ID() + "\x00" + request.Key.String()
		if seen[key] {
			return ErrInvalidRequest
		}
		seen[key] = true
	}
	return nil
}

func atomicCommandID(id string) bool {
	if len(id) < 1 || len(id) > 128 {
		return false
	}
	for _, c := range []byte(id) {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '!', c == '#', c == '$', c == '%', c == '&', c == '\'', c == '*',
			c == '+', c == '-', c == '.', c == '^', c == '_', c == '`', c == '|', c == '~':
		default:
			return false
		}
	}
	return true
}

// AdmitAtomicBatch dispatches once to an explicit native atomic backend, never
// to the existing per-item Batch. Invalid/unsupported input and pre-cancellation
// cause no dispatch. Complete original commitment survives caller cancellation
// after dispatch; cancellation alone never proves rollback. Incomplete, partial,
// panicking or contradictory outputs become a redacted unknown outcome with
// original correlation only. Fail-open policies and fallback are unsupported.
// Native bounded IO and durable replay remain backend responsibilities. This
// boundary cannot certify an implementation's SQL atomicity merely from output.
func (s *StrictService) AdmitAtomicBatch(ctx context.Context, r AtomicBatchRequest) (out AtomicBatchDecision, err error) {
	if s == nil || s.backend == nil || s.name == "" {
		return out, ErrInvalidPolicy
	}
	if isNilInterface(ctx) {
		return out, ErrInvalidRequest
	}
	if err := r.Validate(); err != nil {
		return out, err
	}
	if err := strictCallerContext(ctx); err != nil {
		return out, err
	}
	backend, ok := s.backend.(StrictAtomicBatchBackend)
	if !ok {
		return out, ErrUnsupported
	}
	// The service owns this bounded copy; caller/back-end slice mutation cannot
	// replace the intent used for strict result validation after dispatch.
	r.Requests = append([]Request(nil), r.Requests...)
	validation := append([]Request(nil), r.Requests...)
	defer func() {
		if recover() != nil {
			out, err = AtomicBatchDecision{CommandID: r.CommandID}, UnknownOutcome(ctx.Err())
		}
	}()
	raw, rawErr := backend.AdmitAtomicStrict(ctx, r)
	decl, category := strictDeclaration(rawErr)
	if raw.CommandID == r.CommandID && len(raw.Decisions) == len(validation) {
		rejected := false
		valid := true
		for i, decision := range raw.Decisions {
			request := validation[i]
			switch {
			case raw.Allowed:
				valid = valid && completeAllowed(decision, request)
			case completeRejected(decision, request):
				rejected = true
			default:
				valid = valid && !decision.Allowed && decision.Reason == ReasonAtomicRejected && decision.Limit == request.Policy.Limit() && decision.Remaining >= request.Cost && decision.Remaining <= request.Policy.Limit() && !decision.Reset.IsZero() && decision.RetryAfter == 0
			}
		}
		if valid && (raw.Allowed && rawErr == nil || !raw.Allowed && rejected && decl == declarationSafe && directError(category, ErrRejected)) {
			raw.Decisions = append([]Decision(nil), raw.Decisions...)
			for i := range raw.Decisions {
				raw.Decisions[i] = s.stampDecision(raw.Decisions[i], validation[i])
			}
			return raw, rawErr
		}
	}
	if raw.CommandID == "" && !raw.Allowed && len(raw.Decisions) == 0 {
		if decl == declarationOutcome {
			return AtomicBatchDecision{CommandID: r.CommandID}, rawErr
		}
		if decl == declarationSafe && !directError(category, ErrRejected) && applicableCategory(category, strictMethodAdmit) {
			return AtomicBatchDecision{CommandID: r.CommandID}, rawErr
		}
	}
	return AtomicBatchDecision{CommandID: r.CommandID}, UnknownOutcome(ctx.Err())
}

// Format redacts bucket keys and command correlation for every verb.
func (AtomicBatchRequest) Format(s fmt.State, _ rune) {
	_, _ = io.WriteString(s, "ratelimit.AtomicBatchRequest(redacted)")
}

// MarshalJSON denies implicit export of key-bearing input.
func (AtomicBatchRequest) MarshalJSON() ([]byte, error) { return nil, ErrInvalidRequest }

// MarshalText denies implicit text/map-key input export.
func (AtomicBatchRequest) MarshalText() ([]byte, error) { return nil, ErrInvalidRequest }
