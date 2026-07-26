package ratelimit

import (
	"context"
	"errors"
	"fmt"
)

// MaxBatchSize bounds memory, backend work, and observation fan-out per call.
const MaxBatchSize = 256

// Atomicity defines the guarantee requested for a batch.
type Atomicity string

const (
	// AtomicityPerItem evaluates each item independently and in input order.
	AtomicityPerItem Atomicity = "per_item"
	// AtomicityAllOrNothing requests a guarantee not provided by Service.Batch.
	AtomicityAllOrNothing Atomicity = "all_or_nothing"
)

// BatchRequest groups bounded admission attempts with explicit atomicity.
type BatchRequest struct {
	// Requests contains between one and MaxBatchSize attempts.
	Requests []Request
	// Atomicity must be AtomicityPerItem.
	Atomicity Atomicity
}

// BatchDecision preserves input order and documents actual atomicity.
type BatchDecision struct {
	// Decisions contains one result per input request.
	Decisions []Decision
	// Atomicity reports the guarantee applied to Decisions.
	Atomicity Atomicity
}

// Batch evaluates a bounded list with per-item atomicity and no hidden retries.
func (service *Service) Batch(ctx context.Context, batch BatchRequest) (BatchDecision, error) {
	if len(batch.Requests) == 0 || len(batch.Requests) > MaxBatchSize {
		return BatchDecision{}, fmt.Errorf("%w: batch size must be between 1 and %d", ErrInvalidRequest, MaxBatchSize)
	}
	if batch.Atomicity == AtomicityAllOrNothing {
		return BatchDecision{}, fmt.Errorf("%w: backend-independent batches are per-item", ErrUnsupported)
	}
	if batch.Atomicity != AtomicityPerItem {
		return BatchDecision{}, fmt.Errorf("%w: unknown batch atomicity", ErrInvalidRequest)
	}
	for _, request := range batch.Requests {
		if err := request.Validate(); err != nil {
			return BatchDecision{}, err
		}
	}
	result := BatchDecision{
		Decisions: make([]Decision, len(batch.Requests)),
		Atomicity: AtomicityPerItem,
	}
	var failures []error
	for index, request := range batch.Requests {
		decision, err := service.Admit(ctx, request)
		result.Decisions[index] = decision
		if err != nil {
			failures = append(failures, fmt.Errorf("item %d: %w", index, err))
		}
	}
	return result, errors.Join(failures...)
}
