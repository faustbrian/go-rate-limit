// Package ratelimitauthentication derives strict rate-limit keys at
// github.com/faustbrian/go-rate-limit/v3/adapters/authentication.
//
// Key snapshots Subject once after nil validation. It does not perform
// authentication, establish tenant membership or prevent guesses of low-entropy
// hashed identities. The caller owns principal lifetime and concurrent-use safety;
// Subject method panics propagate.
package ratelimitauthentication
