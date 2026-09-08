package ratelimit_test

import (
	"context"
	"errors"
	"testing"

	ratelimit "github.com/faustbrian/go-rate-limit"
)

func TestStrictPublicAPIExists(t *testing.T) {
	t.Parallel()

	var _ ratelimit.StrictBackend = strictBackendFunc{}
	var _ ratelimit.StrictLeaseBackend = strictLeaseBackend{}
	var _ = ratelimit.NewStrictService
	var _ = ratelimit.CanceledOutcome
	var _ = ratelimit.DeadlineOutcome
	var _ = ratelimit.UnknownOutcome
	var _ = ratelimit.NewBackendError
	var _ = (*ratelimit.StrictService).Admit
	var _ = (*ratelimit.StrictService).Batch
	var _ = (*ratelimit.StrictService).Acquire
	var _ = (*ratelimit.StrictService).Release
	var _ = (*ratelimit.BackendError).Category
	var _ = (*ratelimit.BatchItemError).Index
	var _ = (*ratelimit.StrictBatchError).Items
	if ratelimit.MaxBackendNameBytes != 64 || ratelimit.MaxBackendErrorBytes != 32 ||
		ratelimit.MaxStrictBatchErrorBytes != 11007 {
		t.Fatal("strict public bounds differ from the frozen contract")
	}
	if ratelimit.ReasonCanceled != "canceled" || ratelimit.ReasonDeadline != "deadline" ||
		ratelimit.ReasonOutcomeUnknown != "outcome_unknown" {
		t.Fatal("strict public reasons differ from the frozen contract")
	}
}

func TestStrictServicePreservesDeclaredUnknownContextCategory(t *testing.T) {
	t.Parallel()

	service, err := ratelimit.NewStrictService(strictBackendFunc{
		name: "test-backend",
		admit: func(context.Context, ratelimit.Request) (ratelimit.Decision, error) {
			return ratelimit.Decision{}, ratelimit.UnknownOutcome(context.Canceled)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	decision, err := service.Admit(context.Background(), validRequest(t, ratelimit.FailOpen))
	if !errors.Is(err, ratelimit.ErrOutcomeUnknown) || !errors.Is(err, ratelimit.ErrCanceled) ||
		!errors.Is(err, context.Canceled) || !errors.Is(err, ratelimit.ErrDeadline) ||
		errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Admit() error matrix = %v", err)
	}
	if decision.Allowed || decision.Reason != ratelimit.ReasonOutcomeUnknown {
		t.Fatalf("Admit() decision = %+v", decision)
	}
}

type strictBackendFunc struct {
	name  string
	admit func(context.Context, ratelimit.Request) (ratelimit.Decision, error)
}

func (backend strictBackendFunc) Name() string { return backend.name }

func (backend strictBackendFunc) Admit(ctx context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
	return backend.admit(ctx, request)
}

func (backend strictBackendFunc) AdmitStrict(ctx context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
	return backend.admit(ctx, request)
}

type strictLeaseBackend struct{ strictBackendFunc }

func (strictLeaseBackend) AcquireStrict(context.Context, ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
	return ratelimit.Lease{}, ratelimit.Decision{}, errors.New("not implemented")
}

func (strictLeaseBackend) ReleaseStrict(context.Context, ratelimit.Lease) error {
	return errors.New("not implemented")
}
