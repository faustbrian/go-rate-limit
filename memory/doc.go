// Package memory provides bounded, sharded, process-local admission state.
//
// State quotas are per shard. Cardinality pressure may evict rate counters,
// reopening their capacity, but cannot evict live leases. Sweep is caller-driven;
// Close marks availability without draining calls or erasing maps. Strict methods
// interrupt lock waits and recheck cancellation before mutation; legacy methods
// retain pre-lock-only cancellation checks. Policy consistency is not negotiated.
package memory
