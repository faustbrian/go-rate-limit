// Package postgres provides transactional admission backed by PostgreSQL and
// pgx. It is intended for coordination-heavy workloads, not as the default
// high-throughput backend.
//
// Pools and migration application are caller-owned. Startup checks establish
// table existence only. Strict operations declare commit/rollback ambiguity and
// use a detached rollback deadline starting when the transaction is acquired.
// Legacy methods retain released outcome and cleanup behavior. Deployment
// authority, replication durability and cleanup scheduling are outside the store.
package postgres
