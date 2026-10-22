// Package ratelimitqueue provides strict queue admission middleware at
// github.com/faustbrian/go-rate-limit/v3/adapters/queue.
//
// Deferral is a signal, not scheduling or acknowledgement. Callbacks run
// synchronously in Subject/Cost/Now order and the downstream handler runs only
// after admission. Message identity does not establish deduplication or tenant
// authorization. Callers own borrowed collaborator lifetime and concurrency.
package ratelimitqueue
