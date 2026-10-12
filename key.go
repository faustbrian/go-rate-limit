package ratelimit

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

const (
	// MaxSubjectBytes is the maximum unencoded subject length accepted by NewKey.
	MaxSubjectBytes = 256
	maxKeyPartBytes = 48
)

// Subject is a typed identity input used only to derive a bounded Key.
type Subject struct {
	// Kind is a syntactically bounded subject category; callers control cardinality.
	Kind string
	// Value is a 1..MaxSubjectBytes-byte identity input. Avoid raw secrets even
	// when hashing: deterministic hashes permit guesses of low-entropy values.
	Value string
}

// KeySpec describes a namespaced and versioned admission key.
type KeySpec struct {
	// Namespace isolates applications or package consumers.
	Namespace string
	// Version permits intentional key derivation changes.
	Version string
	// Subject supplies the typed identity.
	Subject Subject
	// Hash derives a SHA-256 digest from namespace, version, kind and value.
	// False persists the raw value; neither mode is encryption or authentication.
	Hash bool
}

// Key is an immutable, validated backend key. Its zero value is invalid.
// Subject kind is syntactically bounded, not verified to be low-cardinality.
type Key struct {
	value       string
	subjectKind string
}

// NewKey validates and derives a namespaced key from spec. Namespace, version
// and kind are 1..48 lowercase ASCII letters, digits, hyphens or underscores.
// Unhashed values may not contain braces, NUL, CR or LF. Hashing domain-separates
// the components with NUL bytes; callers own subject authenticity and privacy.
func NewKey(spec KeySpec) (Key, error) {
	if !validKeyPart(spec.Namespace) || !validKeyPart(spec.Version) ||
		!validKeyPart(spec.Subject.Kind) || spec.Subject.Value == "" ||
		len(spec.Subject.Value) > MaxSubjectBytes {
		return Key{}, fmt.Errorf("%w: invalid or oversized component", ErrInvalidKey)
	}
	value := spec.Subject.Value
	if spec.Hash {
		digest := sha256.Sum256([]byte(spec.Namespace + "\x00" + spec.Version + "\x00" + spec.Subject.Kind + "\x00" + value))
		value = hex.EncodeToString(digest[:])
	} else if strings.ContainsAny(value, "{}\x00\r\n") {
		return Key{}, fmt.Errorf("%w: unsafe subject characters", ErrInvalidKey)
	}
	return Key{
		value:       spec.Namespace + ":" + spec.Version + ":" + spec.Subject.Kind + ":" + value,
		subjectKind: spec.Subject.Kind,
	}, nil
}

func validKeyPart(value string) bool {
	if value == "" {
		return false
	}
	if len(value) > maxKeyPartBytes {
		return false
	}
	for _, char := range value {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '-' && char != '_' {
			return false
		}
	}
	return true
}

// String returns namespace:version:kind:value, or empty for a zero Key.
// With Hash false this includes the raw subject; do not log it as safe metadata.
func (k Key) String() string { return k.value }

// SubjectKind returns the syntactically bounded category, or empty for a zero Key.
func (k Key) SubjectKind() string { return k.subjectKind }
