package valkey

import "testing"

func FuzzDecodeDecisionNeverPanics(f *testing.F) {
	f.Add("1", "1", "2", "1000000", "0", "allowed")
	f.Fuzz(func(t *testing.T, a, b, c, d, e, reason string) {
		_, _ = decodeDecision([]string{a, b, c, d, e, reason})
	})
}
