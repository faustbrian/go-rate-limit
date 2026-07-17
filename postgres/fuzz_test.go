package postgres

import "testing"

func FuzzDecodeStateNeverPanics(f *testing.F) {
	f.Add([]byte(`{"schema":1,"policy_id":"x","algorithm":"fixed_window"}`))
	f.Add([]byte("not-json"))
	f.Fuzz(func(t *testing.T, encoded []byte) {
		_, _ = decodeState(encoded)
	})
}
