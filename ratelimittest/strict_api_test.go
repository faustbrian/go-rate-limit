package ratelimittest_test

import (
	"testing"

	ratelimit "github.com/faustbrian/go-rate-limit"
	"github.com/faustbrian/go-rate-limit/ratelimittest"
)

func TestStrictPublicAPIExists(t *testing.T) {
	t.Parallel()

	var reference *ratelimittest.Reference
	var _ ratelimit.StrictBackend = reference
	var _ ratelimit.StrictLeaseBackend = reference
	var _ = reference.AdmitStrict
	var _ = reference.AcquireStrict
	var _ = reference.ReleaseStrict
}
