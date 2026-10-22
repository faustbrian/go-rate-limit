package ratelimitqueue

import (
	"context"
	"testing"
)

// The derived key is persisted by backends; this value was produced by the
// published v2.1.0 queue middleware for the same queue and tenant.
func TestQueueAndTenantKeyMatchesPublishedV2(t *testing.T) {
	backend := &strictBackend{}
	middleware, err := New(Options{Service: queueService(t, backend), Policy: queuePolicy(t), Subject: ByQueueAndTenant()})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := middleware.Wrap(HandlerFunc(func(context.Context, Message) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Handle(context.Background(), Message{Queue: "ab", Tenant: "c"}); err != nil {
		t.Fatal(err)
	}
	const want = "queue:v1:queue-tenant:1348fcf1dca2abcd87b84fdc716241b7a856384d29ae2b3c121646a29a482915"
	backend.mu.Lock()
	got := backend.request.Key.String()
	backend.mu.Unlock()
	if got != want {
		t.Fatalf("queue key = %q, want %q", got, want)
	}
}
