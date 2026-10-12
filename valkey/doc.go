// Package valkey provides atomic distributed admission backed by native Valkey.
// Keys use one opaque hash tag, scripts are versioned, and state has bounded TTL.
//
// The client is borrowed. Startup checks observe Valkey 9+ and noeviction once;
// operators own ongoing configuration and replication durability. Lua executes
// on one authoritative key/slot; script atomic execution is not rollback after
// a runtime error. Strict transport/reply ambiguity never proves safe retry.
// Released methods retain legacy revision and error-normalization behavior.
package valkey
