package ratelimit_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit/v3"
)

// Plan: one explicit backend owns all-or-nothing capacity and original-command
// replay. The existing per-item Batch remains unchanged. The strict boundary
// must refuse missing command identity, unsupported composition and partial or
// ambiguous positive output before any caller treats it as admission.
func TestAtomicBatchRequiresExplicitBackendAndOriginalCommand(t *testing.T) {
	b := &atomicBackend{}
	service, err := ratelimit.NewStrictService(b)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.AdmitAtomicBatch(context.Background(), ratelimit.AtomicBatchRequest{}); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatal("zero command admitted")
	}
	r := atomicRequest(t)
	for _, change := range []func(*ratelimit.AtomicBatchRequest){
		func(r *ratelimit.AtomicBatchRequest) { r.CommandID = "bad command" },
		func(r *ratelimit.AtomicBatchRequest) { r.CommandID = strings.Repeat("x", 129) },
		func(r *ratelimit.AtomicBatchRequest) { r.Requests = nil },
		func(r *ratelimit.AtomicBatchRequest) { r.Requests = []ratelimit.Request{r.Requests[0], r.Requests[0]} },
		func(r *ratelimit.AtomicBatchRequest) { r.Requests[1].Now = r.Requests[1].Now.Add(time.Second) },
		func(r *ratelimit.AtomicBatchRequest) { r.Requests[0].Cost = 0 },
	} {
		bad := r
		bad.Requests = append([]ratelimit.Request(nil), r.Requests...)
		change(&bad)
		if _, err := service.AdmitAtomicBatch(context.Background(), bad); !errors.Is(err, ratelimit.ErrInvalidRequest) {
			t.Fatal("invalid input dispatched", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.AdmitAtomicBatch(ctx, r); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation lost")
	}
	if b.calls != 0 {
		t.Fatal("invalid input reached backend")
	}
	legacy, err := ratelimit.NewStrictService(strictBackendFunc{name: "legacy", admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
		t.Fatal("legacy per-item backend dispatched")
		return ratelimit.Decision{}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.AdmitAtomicBatch(context.Background(), r); !errors.Is(err, ratelimit.ErrUnsupported) {
		t.Fatal("atomic fallback selected", err)
	}
	if _, err := service.AdmitAtomicBatch(missingAtomicContext(), r); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatal("nil context admitted")
	}
	if _, err := (*ratelimit.StrictService)(nil).AdmitAtomicBatch(context.Background(), r); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatal("nil service admitted")
	}
	p, err := ratelimit.NewPolicy(ratelimit.PolicySpec{ID: "fail-open", Revision: "1", Algorithm: ratelimit.TokenBucket, Capacity: 10, Period: time.Minute, Consistency: ratelimit.ConsistencyStrong, FailureMode: ratelimit.FailOpen})
	if err != nil {
		t.Fatal(err)
	}
	bad := r
	bad.Requests = append([]ratelimit.Request(nil), r.Requests...)
	bad.Requests[0].Policy = p
	if _, err := service.AdmitAtomicBatch(context.Background(), bad); !errors.Is(err, ratelimit.ErrUnsupported) {
		t.Fatal("fail-open atomic profile admitted", err)
	}
	if _, err := json.Marshal(r); !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatal("bucket-bearing input serialized")
	}
	if data, err := r.MarshalText(); len(data) != 0 || !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatal("bucket-bearing text export allowed")
	}
}

func missingAtomicContext() context.Context { return nil }

type atomicBackend struct {
	calls int
	run   func(context.Context, ratelimit.AtomicBatchRequest) (ratelimit.AtomicBatchDecision, error)
}

func (*atomicBackend) Name() string { return "atomic-fixture" }
func (*atomicBackend) Admit(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
	return ratelimit.Decision{}, ratelimit.ErrUnavailable
}
func (*atomicBackend) AdmitStrict(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
	return ratelimit.Decision{}, ratelimit.ErrUnavailable
}
func (b *atomicBackend) AdmitAtomicStrict(ctx context.Context, r ratelimit.AtomicBatchRequest) (ratelimit.AtomicBatchDecision, error) {
	b.calls++
	if b.run != nil {
		return b.run(ctx, r)
	}
	return ratelimit.AtomicBatchDecision{}, ratelimit.UnknownOutcome(nil)
}

func atomicRequest(t *testing.T) ratelimit.AtomicBatchRequest {
	t.Helper()
	p, err := ratelimit.NewPolicy(ratelimit.PolicySpec{ID: "atomic-policy", Revision: "1", Algorithm: ratelimit.TokenBucket, Capacity: 10, Period: 10 * time.Minute, Consistency: ratelimit.ConsistencyStrong})
	if err != nil {
		t.Fatal(err)
	}
	r := ratelimit.AtomicBatchRequest{CommandID: "original-command"}
	for _, subject := range []string{"private-ip", "private-user"} {
		key, err := ratelimit.NewKey(ratelimit.KeySpec{Namespace: "atomic-test", Version: "1", Subject: ratelimit.Subject{Kind: "dimension", Value: subject}, Hash: true})
		if err != nil {
			t.Fatal(err)
		}
		r.Requests = append(r.Requests, ratelimit.Request{Policy: p, Key: key, Cost: 1, Now: time.Unix(1800000000, 0)})
	}
	return r
}

func TestAtomicBatchConservesCompleteCommitAndRejectsPartialOutput(t *testing.T) {
	r := atomicRequest(t)
	allowed := ratelimit.AtomicBatchDecision{CommandID: r.CommandID, Allowed: true, Decisions: []ratelimit.Decision{{Allowed: true, Limit: 10, Remaining: 9, Reset: r.Requests[0].Now.Add(time.Minute), Reason: ratelimit.ReasonAllowed}, {Allowed: true, Limit: 10, Remaining: 9, Reset: r.Requests[0].Now.Add(time.Minute), Reason: ratelimit.ReasonAllowed}}}
	b := &atomicBackend{run: func(context.Context, ratelimit.AtomicBatchRequest) (ratelimit.AtomicBatchDecision, error) {
		return allowed, nil
	}}
	s, err := ratelimit.NewStrictService(b)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.AdmitAtomicBatch(context.Background(), r)
	if err != nil || !got.Allowed || got.CommandID != r.CommandID || len(got.Decisions) != 2 || got.Decisions[0].Backend != "atomic-fixture" || got.Decisions[0].PolicyRevision != "1" {
		t.Fatal("complete original debit lost", err)
	}
	got.Decisions[0].Remaining = 0
	if allowed.Decisions[0].Remaining != 9 {
		t.Fatal("backend result slice ownership escaped")
	}
	for _, mutate := range []func(*ratelimit.AtomicBatchDecision){
		func(d *ratelimit.AtomicBatchDecision) { d.CommandID = "foreign-command" },
		func(d *ratelimit.AtomicBatchDecision) { d.Decisions = d.Decisions[:1] },
		func(d *ratelimit.AtomicBatchDecision) { d.Decisions[1].Allowed = false },
		func(d *ratelimit.AtomicBatchDecision) { d.Decisions[1].Remaining = 10 },
	} {
		bad := allowed
		bad.Decisions = append([]ratelimit.Decision(nil), allowed.Decisions...)
		mutate(&bad)
		b.run = func(context.Context, ratelimit.AtomicBatchRequest) (ratelimit.AtomicBatchDecision, error) {
			return bad, nil
		}
		out, err := s.AdmitAtomicBatch(context.Background(), r)
		if !errors.Is(err, ratelimit.ErrOutcomeUnknown) || out.Allowed || len(out.Decisions) != 0 || out.CommandID != r.CommandID {
			t.Fatal("partial positive output admitted", err)
		}
	}
	if !strings.Contains(fmt.Sprintf("%#v", r), "redacted") {
		t.Fatal("input diagnostic leaked")
	}
	// Complete original commitment is not undone by a canceled caller after
	// dispatch. Input/result copies must also protect strict validation from a
	// backend that changes the borrowed dispatch slice before returning.
	ctx, cancel := context.WithCancel(context.Background())
	b.run = func(_ context.Context, input ratelimit.AtomicBatchRequest) (ratelimit.AtomicBatchDecision, error) {
		input.Requests[0].Cost = 10
		cancel()
		return allowed, nil
	}
	if got, err := s.AdmitAtomicBatch(ctx, r); err != nil || !got.Allowed || r.Requests[0].Cost != 1 {
		t.Fatal("known commit or caller input corrupted", err)
	}
}

func TestAtomicBatchRejectsWithoutPartialPermissionAndRedactsAmbiguity(t *testing.T) {
	r := atomicRequest(t)
	rejected := ratelimit.AtomicBatchDecision{CommandID: r.CommandID, Decisions: []ratelimit.Decision{{Limit: 10, Remaining: 10, Reset: r.Requests[0].Now.Add(time.Minute), Reason: ratelimit.ReasonAtomicRejected}, {Limit: 10, Reset: r.Requests[0].Now.Add(time.Minute), RetryAfter: time.Minute, Reason: ratelimit.ReasonLimited}}}
	b := &atomicBackend{run: func(context.Context, ratelimit.AtomicBatchRequest) (ratelimit.AtomicBatchDecision, error) {
		return rejected, ratelimit.ErrRejected
	}}
	s, err := ratelimit.NewStrictService(b)
	if err != nil {
		t.Fatal(err)
	}
	out, err := s.AdmitAtomicBatch(context.Background(), r)
	if !errors.Is(err, ratelimit.ErrRejected) || out.Allowed || len(out.Decisions) != 2 || out.Decisions[0].Allowed || out.Decisions[0].Remaining != 10 || out.Decisions[1].RetryAfter != time.Minute {
		t.Fatal("atomic rejection fabricated debit", err)
	}
	for _, mode := range []string{"panic", "raw-error", "unknown", "mixed"} {
		b.run = func(context.Context, ratelimit.AtomicBatchRequest) (ratelimit.AtomicBatchDecision, error) {
			switch mode {
			case "panic":
				panic("private-backend-detail")
			case "raw-error":
				return ratelimit.AtomicBatchDecision{}, errors.New("private-backend-detail")
			case "mixed":
				d := rejected
				d.Decisions = append([]ratelimit.Decision(nil), rejected.Decisions...)
				d.Decisions[0].Allowed = true
				return d, ratelimit.ErrRejected
			default:
				return ratelimit.AtomicBatchDecision{}, ratelimit.UnknownOutcome(context.DeadlineExceeded)
			}
		}
		out, err := s.AdmitAtomicBatch(context.Background(), r)
		if !errors.Is(err, ratelimit.ErrOutcomeUnknown) || out.Allowed || len(out.Decisions) != 0 || strings.Contains(err.Error(), "private-backend") {
			t.Fatal("ambiguous dispatch disclosed positive result", err)
		}
	}
}
