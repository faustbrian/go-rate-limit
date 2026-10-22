package ratelimitslog

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit/v3"
)

func TestObserverWritesEveryBoundedAttributeAtConfiguredLevel(t *testing.T) {
	var output bytes.Buffer
	observer, err := New(Options{Logger: slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug})), Level: slog.LevelWarn})
	if err != nil {
		t.Fatal(err)
	}
	observer.Observe(ratelimit.Observation{
		PolicyID: "login", SubjectKind: "principal", Duration: 2 * time.Millisecond, Err: ratelimit.ErrRejected,
		Decision: ratelimit.Decision{Backend: "valkey", Reason: ratelimit.ReasonLimited, PolicyRevision: "v2"},
	})
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	delete(record, "time")
	want := map[string]any{
		"level": "WARN", "msg": "rate limit decision", "policy_id": "login", "policy_revision": "v2",
		"subject_kind": "principal", "backend": "valkey", "reason": "limited", "allowed": false,
		"duration_ns": float64(2_000_000), "error_kind": "rejected",
	}
	if len(record) != len(want) {
		t.Fatalf("record = %v, want %v", record, want)
	}
	for key, value := range want {
		if record[key] != value {
			t.Fatalf("record[%q] = %#v, want %#v (record %v)", key, record[key], value, record)
		}
	}
}
