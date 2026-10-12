// Package ratelimitrpc provides transport-neutral JSON-RPC admission
// middleware for global, principal, method, tenant, and custom subjects.
//
// Rules consume capacity independently in order; a later rejection does not
// roll back earlier rules. Call/response payloads are not encoded or deep-copied,
// and identities are not authenticated here. Borrowed callbacks and handlers
// run synchronously, may be concurrent, and retain legacy panic behavior.
package ratelimitrpc
