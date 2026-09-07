package ratelimitslog

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	ratelimit "github.com/faustbrian/go-rate-limit"
)

// Options configures structured observation logging.
type Options struct {
	// Logger receives bounded observation fields.
	Logger *slog.Logger
	// Level is used for each decision record.
	Level slog.Level
}

// Observer records bounded admission observations with slog.
type Observer struct {
	logger *slog.Logger
	level  slog.Level
}

// New validates options and constructs an Observer.
func New(options Options) (*Observer, error) {
	if options.Logger == nil {
		return nil, fmt.Errorf("%w: logger is required", ratelimit.ErrInvalidPolicy)
	}
	return &Observer{logger: options.Logger, level: options.Level}, nil
}

// Observe writes one bounded structured decision record.
func (observer *Observer) Observe(observation ratelimit.Observation) {
	if observer == nil || observer.logger == nil {
		return
	}
	observer.logger.LogAttrs(context.Background(), observer.level, "rate limit decision",
		slog.String("policy_id", observation.PolicyID), slog.String("policy_revision", observation.Decision.PolicyRevision),
		slog.String("subject_kind", observation.SubjectKind), slog.String("backend", observation.Decision.Backend),
		slog.String("reason", string(observation.Decision.Reason)), slog.Bool("allowed", observation.Decision.Allowed),
		slog.Int64("duration_ns", observation.Duration.Nanoseconds()), slog.String("error_kind", errorKind(observation.Err)))
}

func errorKind(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, ratelimit.ErrOutcomeUnknown):
		return "outcome_unknown"
	case errors.Is(err, ratelimit.ErrCanceled):
		return "canceled"
	case errors.Is(err, ratelimit.ErrDeadline):
		return "deadline"
	case errors.Is(err, ratelimit.ErrRejected):
		return "rejected"
	case errors.Is(err, ratelimit.ErrUnavailable):
		return "unavailable"
	case errors.Is(err, ratelimit.ErrOverflow):
		return "overflow"
	case errors.Is(err, ratelimit.ErrCorrupt):
		return "corrupt"
	case errors.Is(err, ratelimit.ErrUnsupported):
		return "unsupported"
	case errors.Is(err, ratelimit.ErrInvalidPolicy), errors.Is(err, ratelimit.ErrInvalidKey), errors.Is(err, ratelimit.ErrInvalidRequest):
		return "invalid"
	default:
		return "internal"
	}
}

var _ ratelimit.Observer = (*Observer)(nil)
