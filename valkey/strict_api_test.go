package valkey_test

import (
	"testing"

	ratelimit "github.com/faustbrian/go-rate-limit"
	"github.com/faustbrian/go-rate-limit/valkey"
)

func TestStrictPublicAPIExists(t *testing.T) {
	t.Parallel()

	var store *valkey.Store
	var _ ratelimit.StrictBackend = store
	var _ ratelimit.StrictLeaseBackend = store
	var _ = valkey.NewStrict
	var _ = valkey.OpenStrict
	var _ = store.AdmitStrict
	var _ = store.AcquireStrict
	var _ = store.ReleaseStrict
	var _ = store.CheckStrict
}
