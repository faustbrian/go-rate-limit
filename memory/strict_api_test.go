package memory_test

import (
	"context"
	"testing"

	ratelimit "github.com/faustbrian/go-rate-limit"
	"github.com/faustbrian/go-rate-limit/memory"
)

func TestStrictPublicAPIExists(t *testing.T) {
	t.Parallel()

	var store *memory.Store
	var _ ratelimit.StrictBackend = store
	var _ ratelimit.StrictLeaseBackend = store
	var _ = store.AdmitStrict
	var _ = store.AcquireStrict
	var _ = store.ReleaseStrict
	var _ = context.Background
}
