package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit/v3"
)

func TestAtomicPreDispatchAndReceiptCorruption(t *testing.T) {
	request := ratelimit.AtomicBatchRequest{CommandID: "command", Requests: []ratelimit.Request{atomicIntegrationRequest(t, "unit", 2)}}
	var empty *Store
	if out, err := empty.AdmitAtomicStrict(context.Background(), request); out.Allowed || !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("nil = %+v, %v", out, err)
	}
	// A real initialized strict boundary, but no transaction is needed for
	// invalid input or cancellation. Reaching the empty database would panic.
	store := &Store{executor: &nativeExecutor{}, options: Options{Timeout: time.Second}, rollbackTimeout: time.Second}
	if out, err := store.AdmitAtomicStrict(missingAtomicContext(), request); out.Allowed || !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("nil context = %+v, %v", out, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if out, err := store.AdmitAtomicStrict(ctx, request); out.Allowed || !errors.Is(err, ratelimit.ErrCanceled) {
		t.Fatalf("canceled = %+v, %v", out, err)
	}
	invalid := request
	invalid.CommandID = ""
	if out, err := store.AdmitAtomicStrict(context.Background(), invalid); out.Allowed || !errors.Is(err, ratelimit.ErrInvalidRequest) {
		t.Fatalf("invalid = %+v, %v", out, err)
	}
	good := ratelimit.AtomicBatchDecision{Allowed: true, Decisions: []ratelimit.Decision{{Allowed: true, Limit: 2, Remaining: 1, Reset: time.Unix(3700, 0), Reason: ratelimit.ReasonAllowed, Backend: "postgres", PolicyRevision: "v1"}}}
	encoded, _ := json.Marshal(good)
	if out, err := decodeAtomicReceipt(encoded, request); err != nil || !out.Allowed || out.CommandID != request.CommandID {
		t.Fatalf("valid = %+v, %v", out, err)
	}
	for _, test := range []struct {
		name   string
		modify func(*ratelimit.AtomicBatchDecision)
	}{
		{"raw-command", func(r *ratelimit.AtomicBatchDecision) { r.CommandID = "secret" }},
		{"partial", func(r *ratelimit.AtomicBatchDecision) { r.Decisions = nil }},
		{"mixed", func(r *ratelimit.AtomicBatchDecision) { r.Decisions[0].Allowed = false }},
		{"foreign-policy", func(r *ratelimit.AtomicBatchDecision) { r.Decisions[0].PolicyRevision = "v2" }},
		{"overflow", func(r *ratelimit.AtomicBatchDecision) { r.Decisions[0].Remaining = 3 }},
		{"no-debit", func(r *ratelimit.AtomicBatchDecision) { r.Decisions[0].Remaining = 2 }},
		{"no-time", func(r *ratelimit.AtomicBatchDecision) { r.Decisions[0].Reset = time.Time{} }},
		{"false-rejection", func(r *ratelimit.AtomicBatchDecision) {
			r.Allowed = false
			r.Decisions[0].Allowed = false
			r.Decisions[0].Reason = ratelimit.ReasonAtomicRejected
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := good
			r.Decisions = append([]ratelimit.Decision(nil), good.Decisions...)
			test.modify(&r)
			encoded, _ := json.Marshal(r)
			if out, err := decodeAtomicReceipt(encoded, request); out.Allowed || !errors.Is(err, ratelimit.ErrCorrupt) {
				t.Fatalf("corrupt = %+v, %v", out, err)
			}
		})
	}
	for _, data := range [][]byte{nil, []byte("{"), append(encoded, []byte("{}")...), make([]byte, maxAtomicReceiptBytes+1)} {
		if out, err := decodeAtomicReceipt(data, request); out.Allowed || !errors.Is(err, ratelimit.ErrCorrupt) {
			t.Fatalf("malformed = %+v, %v", out, err)
		}
	}
}

func missingAtomicContext() context.Context { return nil }

func TestAtomicIntentBindsCompletePolicyAndOrderNotRetryClock(t *testing.T) {
	request := ratelimit.AtomicBatchRequest{CommandID: "original", Requests: []ratelimit.Request{atomicIntegrationRequest(t, "a", 2), atomicIntegrationRequest(t, "b", 3)}}
	want := atomicIntent(request, ServerClock)
	request.CommandID = "other"
	for i := range request.Requests {
		request.Requests[i].Now = request.Requests[i].Now.Add(time.Hour)
	}
	if atomicIntent(request, ServerClock) != want {
		t.Fatal("retry time or correlation changed intent")
	}
	if atomicIntent(request, ClientClock) == want {
		t.Fatal("clock authority omitted")
	}
	request.Requests[0], request.Requests[1] = request.Requests[1], request.Requests[0]
	if atomicIntent(request, ServerClock) == want {
		t.Fatal("ordered intent omitted")
	}
	request.Requests[0], request.Requests[1] = request.Requests[1], request.Requests[0]
	request.Requests[0].Cost = 2
	if atomicIntent(request, ServerClock) == want {
		t.Fatal("cost omitted")
	}
}

func TestAtomicIntentPreservesRawKeyBytes(t *testing.T) {
	r := ratelimit.AtomicBatchRequest{CommandID: "raw-key", Requests: []ratelimit.Request{atomicIntegrationRequest(t, "raw-key-policy", 2)}}
	keys := make([]ratelimit.Key, 2)
	for i, value := range []byte{0xff, 0xfe} {
		var err error
		keys[i], err = ratelimit.NewKey(ratelimit.KeySpec{Namespace: "atomic", Version: "v1", Subject: ratelimit.Subject{Kind: "test", Value: string([]byte{value})}})
		if err != nil {
			t.Fatal("valid raw key rejected")
		}
	}
	r.Requests[0].Key = keys[0]
	first := atomicIntent(r, ServerClock)
	r.Requests[0].Key = keys[1]
	if first == atomicIntent(r, ServerClock) {
		t.Fatal("different raw key bytes collapsed into the same command intent")
	}
}

func FuzzDecodeAtomicReceiptNeverGrantsCorruptAdmission(f *testing.F) {
	request := ratelimit.AtomicBatchRequest{CommandID: "fuzz-command", Requests: []ratelimit.Request{atomicIntegrationRequest(f, "fuzz-policy", 2)}}
	good := ratelimit.AtomicBatchDecision{Allowed: true, Decisions: []ratelimit.Decision{{Allowed: true, Limit: 2, Remaining: 1, Reset: time.Unix(3700, 0), Reason: ratelimit.ReasonAllowed, Backend: "postgres", PolicyRevision: "v1"}}}
	encoded, err := json.Marshal(good)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(encoded)
	f.Add([]byte("{"))
	f.Add(append(append([]byte(nil), encoded...), []byte("{}")...))
	f.Add([]byte(nil))
	f.Add(make([]byte, maxAtomicReceiptBytes+1))
	f.Fuzz(func(t *testing.T, input []byte) {
		out, err := decodeAtomicReceipt(input, request)
		if err != nil && !errors.Is(err, ratelimit.ErrRejected) {
			if !errors.Is(err, ratelimit.ErrCorrupt) || out.Allowed || out.CommandID != "" || len(out.Decisions) != 0 {
				t.Fatal("corrupt receipt retained admission or correlation")
			}
			return
		}
		if out.CommandID != request.CommandID || len(out.Decisions) != 1 {
			t.Fatal("receipt lost original command or bucket")
		}
		d := out.Decisions[0]
		if out.Allowed {
			if err != nil || !d.Allowed || d.Remaining > 1 || d.Reason != ratelimit.ReasonAllowed {
				t.Fatal("contradictory admission")
			}
		} else if !errors.Is(err, ratelimit.ErrRejected) || d.Allowed || d.Reason != ratelimit.ReasonLimited {
			t.Fatal("rejection lacks exhausted bucket")
		}
	})
}
