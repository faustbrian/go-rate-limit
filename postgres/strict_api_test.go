package postgres_test

import (
	"testing"

	ratelimit "github.com/faustbrian/go-rate-limit"
	"github.com/faustbrian/go-rate-limit/postgres"
)

func TestStrictPublicAPIExists(t *testing.T) {
	t.Parallel()

	var store *postgres.Store
	var _ ratelimit.StrictBackend = store
	var _ ratelimit.StrictLeaseBackend = store
	var _ = postgres.StrictOptions{}
	var _ = postgres.NewStrict
	var _ = postgres.OpenStrict
	var _ = store.AdmitStrict
	var _ = store.AcquireStrict
	var _ = store.ReleaseStrict
	var _ = store.CheckStrict
	var _ = store.CleanupStrict
}
