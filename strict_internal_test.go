package ratelimit

import (
	"context"
	"errors"
	"testing"
)

func TestStrictOutcomePrivateZeroAndCategoryBranches(t *testing.T) {
	if CanceledOutcome().Error() != ErrCanceled.Error() || DeadlineOutcome().Error() != ErrDeadline.Error() {
		t.Fatal("outcome text changed")
	}
	var absent *strictOutcomeError
	if absent.Error() != "rate limit outcome error" || absent.Is(ErrCanceled) {
		t.Fatal("nil outcome is not total")
	}
	invalid := &strictOutcomeError{}
	if invalid.Error() != "rate limit outcome error" || invalid.Is(ErrCanceled) {
		t.Fatal("invalid outcome is not total")
	}
	deadline := DeadlineOutcome()
	if !errors.Is(deadline, ErrDeadline) || !errors.Is(deadline, context.DeadlineExceeded) || errors.Is(deadline, ErrCanceled) {
		t.Fatalf("deadline=%v", deadline)
	}
	unknownCanceled := UnknownOutcome(context.Canceled)
	if !errors.Is(unknownCanceled, ErrOutcomeUnknown) || !errors.Is(unknownCanceled, ErrCanceled) || !errors.Is(unknownCanceled, context.Canceled) || !errors.Is(unknownCanceled, ErrDeadline) {
		t.Fatalf("unknown canceled=%v", unknownCanceled)
	}
	unknownDeadline := UnknownOutcome(context.DeadlineExceeded)
	if !errors.Is(unknownDeadline, ErrOutcomeUnknown) || !errors.Is(unknownDeadline, ErrDeadline) || !errors.Is(unknownDeadline, context.DeadlineExceeded) {
		t.Fatalf("unknown deadline=%v", unknownDeadline)
	}
	unknown := UnknownOutcome(errors.New("other"))
	if !errors.Is(unknown, ErrOutcomeUnknown) || errors.Is(unknown, ErrCanceled) || errors.Is(unknown, ErrDeadline) {
		t.Fatalf("unknown=%v", unknown)
	}
}

func TestStrictPrivateDefaultBranches(t *testing.T) {
	if isNilInterface(1) {
		t.Fatal("integer reported nil")
	}
	if applicableCategory(ErrUnavailable, strictMethod(99)) {
		t.Fatal("unknown method applicable")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if strictCallerContext(ctx) == nil {
		t.Fatal("canceled context accepted")
	}
	if strictCallerContext(context.Background()) != nil {
		t.Fatal("active context rejected")
	}
	if contextReason(UnknownOutcome(nil)) != ReasonDeadline {
		t.Fatal("fallback reason changed")
	}
}
