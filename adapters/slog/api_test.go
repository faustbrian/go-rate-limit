//nolint:staticcheck // Explicit types compile-check the complete public API signatures.
package ratelimitslog_test

import (
	"log/slog"
	"testing"

	ratelimit "github.com/faustbrian/go-rate-limit"
	ratelimitslog "github.com/faustbrian/go-rate-limit/adapters/slog"
)

func TestPublicAPIExists(t *testing.T) {
	t.Parallel()

	var _ = ratelimitslog.Options{Logger: (*slog.Logger)(nil), Level: slog.LevelInfo}
	var _ func(ratelimitslog.Options) (*ratelimitslog.Observer, error) = ratelimitslog.New
	var observer *ratelimitslog.Observer
	var _ func(ratelimit.Observation) = observer.Observe
	var _ ratelimit.Observer = observer
}
