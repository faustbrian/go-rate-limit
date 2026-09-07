package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

var (
	// ErrCanceled indicates that admission stopped before backend dispatch.
	ErrCanceled = errors.New("rate limit canceled")
	// ErrOutcomeUnknown indicates that backend dispatch occurred but its result is ambiguous.
	ErrOutcomeUnknown = errors.New("rate limit outcome unknown")
)

const (
	// MaxBackendNameBytes bounds backend identity retained by StrictService.
	MaxBackendNameBytes = 64
	// MaxBackendErrorBytes bounds normalized strict backend error strings.
	MaxBackendErrorBytes = 32
	// MaxStrictBatchErrorBytes bounds the largest possible strict batch error string.
	MaxStrictBatchErrorBytes = 11007
)

type strictOutcomeKind uint8

const (
	strictOutcomeCanceled strictOutcomeKind = iota + 1
	strictOutcomeDeadline
	strictOutcomeUnknown
)

type strictOutcomeError struct {
	kind    strictOutcomeKind
	context strictOutcomeKind
}

// CanceledOutcome constructs a bounded declaration that dispatch did not occur.
func CanceledOutcome() error { return &strictOutcomeError{kind: strictOutcomeCanceled} }

// DeadlineOutcome constructs a bounded deadline declaration that dispatch did not occur.
func DeadlineOutcome() error { return &strictOutcomeError{kind: strictOutcomeDeadline} }

// UnknownOutcome constructs a bounded declaration that dispatch may have occurred.
func UnknownOutcome(contextCategory error) error {
	contextKind := strictOutcomeKind(0)
	if directError(contextCategory, context.Canceled) {
		contextKind = strictOutcomeCanceled
	} else if directError(contextCategory, context.DeadlineExceeded) {
		contextKind = strictOutcomeDeadline
	}
	return &strictOutcomeError{kind: strictOutcomeUnknown, context: contextKind}
}

func (err *strictOutcomeError) Error() string {
	if err == nil {
		return "rate limit outcome error"
	}
	switch err.kind {
	case strictOutcomeCanceled:
		return ErrCanceled.Error()
	case strictOutcomeDeadline:
		return ErrDeadline.Error()
	case strictOutcomeUnknown:
		return ErrOutcomeUnknown.Error()
	default:
		return "rate limit outcome error"
	}
}

func (err *strictOutcomeError) Is(target error) bool {
	if err == nil {
		return false
	}
	switch err.kind {
	case strictOutcomeCanceled:
		return directError(target, ErrCanceled) || directError(target, context.Canceled) || directError(target, ErrDeadline)
	case strictOutcomeDeadline:
		return directError(target, ErrDeadline) || directError(target, context.DeadlineExceeded)
	case strictOutcomeUnknown:
		if directError(target, ErrOutcomeUnknown) {
			return true
		}
		if err.context == strictOutcomeCanceled {
			return directError(target, ErrCanceled) || directError(target, context.Canceled) || directError(target, ErrDeadline)
		}
		if err.context == strictOutcomeDeadline {
			return directError(target, ErrDeadline) || directError(target, context.DeadlineExceeded)
		}
	}
	return false
}

// BackendError carries one safe backend category across a strict service boundary.
type BackendError struct{ category error }

// NewBackendError validates and retains one direct safe backend category.
func NewBackendError(category error) (*BackendError, error) {
	if !safeBackendCategory(category) {
		return nil, fmt.Errorf("%w: backend error category is not safe", ErrInvalidPolicy)
	}
	return &BackendError{category: category}, nil
}

// Error returns only the retained stable category text.
func (err *BackendError) Error() string {
	if err == nil || err.category == nil {
		return "rate limit backend error"
	}
	return err.category.Error()
}

// Unwrap returns the retained stable category.
func (err *BackendError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.category
}

// Category returns the retained stable category.
func (err *BackendError) Category() error {
	if err == nil {
		return nil
	}
	return err.category
}

func safeBackendCategory(err error) bool {
	return directError(err, ErrRejected) || directError(err, ErrUnavailable) ||
		directError(err, ErrOverflow) || directError(err, ErrCorrupt) ||
		directError(err, ErrUnsupported) || directError(err, ErrLeaseNotFound) ||
		directError(err, ErrLeaseNotOwned)
}

func directError(err, target error) bool {
	if err == nil || target == nil {
		return err == nil && target == nil
	}
	typeOfErr := reflect.TypeOf(err)
	if !typeOfErr.Comparable() {
		return false
	}
	return err == target //nolint:errorlint // Strict declaration matching intentionally accepts only direct safe sentinels.
}

func isNilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

// BatchItemError identifies one failed zero-based strict batch item.
type BatchItemError struct {
	index int
	cause error
}

// Error returns a bounded indexed error string.
func (err *BatchItemError) Error() string {
	if err == nil || err.cause == nil {
		return "item error"
	}
	return fmt.Sprintf("item %d: %s", err.index, err.cause.Error())
}

// Unwrap returns the normalized item error.
func (err *BatchItemError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.cause
}

// Index returns the zero-based input index, or -1 for a nil or zero value.
func (err *BatchItemError) Index() int {
	if err == nil || err.cause == nil {
		return -1
	}
	return err.index
}

// StrictBatchError is the immutable machine-readable aggregate for a strict batch.
type StrictBatchError struct{ items []*BatchItemError }

// Error joins bounded item strings in input order.
func (err *StrictBatchError) Error() string {
	if err == nil || len(err.items) == 0 {
		return "batch error"
	}
	parts := make([]string, len(err.items))
	for index, item := range err.items {
		parts[index] = item.Error()
	}
	return strings.Join(parts, "\n")
}

// Unwrap returns a defensive ordered slice for errors.Is and errors.As.
func (err *StrictBatchError) Unwrap() []error {
	if err == nil || len(err.items) == 0 {
		return nil
	}
	result := make([]error, len(err.items))
	for index, item := range err.items {
		result[index] = item
	}
	return result
}

// Items returns a defensive ordered slice of immutable item errors.
func (err *StrictBatchError) Items() []*BatchItemError {
	if err == nil || len(err.items) == 0 {
		return nil
	}
	return append([]*BatchItemError(nil), err.items...)
}
