package ratelimitlog_test

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
	"github.com/faustbrian/golib/pkg/rate-limit/ratelimitlog"
)

func TestObserverLogsOnlyBoundedDecisionMetadata(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	observer, err := ratelimitlog.New(ratelimitlog.Options{
		Logger: slog.New(slog.NewJSONHandler(&output, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	observer.Observe(ratelimit.Observation{
		PolicyID: "login", SubjectKind: "principal",
		Decision: ratelimit.Decision{
			Allowed: false, Backend: "valkey", Reason: ratelimit.ReasonLimited,
			PolicyRevision: "v2",
		},
		Err: ratelimit.ErrRejected, Duration: 2 * time.Millisecond,
	})
	logged := output.String()
	for _, fragment := range []string{
		`"msg":"rate limit decision"`,
		`"policy_id":"login"`,
		`"subject_kind":"principal"`,
		`"backend":"valkey"`,
		`"reason":"limited"`,
	} {
		if !strings.Contains(logged, fragment) {
			t.Fatalf("log %q missing %q", logged, fragment)
		}
	}
	if strings.Contains(logged, "credential") || strings.Contains(logged, "subject_value") {
		t.Fatalf("log leaked sensitive data: %q", logged)
	}
}
