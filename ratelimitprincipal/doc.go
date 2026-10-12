// Package ratelimitprincipal adapts authentication principals to hashed
// admission keys without depending on a concrete authentication package.
//
// Key retains its two Subject calls and nil-interface-only validation. KeyStrict
// checks typed nil and snapshots once; neither helper authenticates a principal
// or recovers its panic. SHA-256 derivation is not encryption or anonymity.
//
// Deprecated: use github.com/faustbrian/go-rate-limit/v2/adapters/authentication. This package remains supported through the documented compatibility interval.
package ratelimitprincipal
