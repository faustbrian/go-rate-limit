package ratelimit_test

import (
	"testing"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
)

func FuzzNewKeyNeverLeaksHashedSubject(f *testing.F) {
	f.Add("http", "v1", "principal", "user-42")
	f.Add("", "", "", "")
	f.Fuzz(func(t *testing.T, namespace, version, kind, value string) {
		key, err := ratelimit.NewKey(ratelimit.KeySpec{
			Namespace: namespace, Version: version,
			Subject: ratelimit.Subject{Kind: kind, Value: value}, Hash: true,
		})
		if err == nil {
			if key.String() == "" || key.String() == value ||
				key.SubjectKind() != kind {
				t.Fatalf("unsafe key = %q for %q", key.String(), value)
			}
		}
	})
}
