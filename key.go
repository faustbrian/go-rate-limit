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
	// Kind is a bounded, low-cardinality subject category.
	Kind string
	// Value is the identity value and should normally be irreversibly hashed.
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
	// Hash irreversibly hashes Subject.Value before storage and observation.
	Hash bool
}

// Key is a validated, bounded backend key with a safe subject kind.
type Key struct {
	value       string
	subjectKind string
}

// NewKey validates and derives a namespaced key from spec.
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
	if value == "" || len(value) > maxKeyPartBytes {
		return false
	}
	for _, char := range value {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '-' && char != '_' {
			return false
		}
	}
	return true
}

// String returns the bounded persisted representation.
func (k Key) String() string { return k.value }

// SubjectKind returns the safe, low-cardinality subject category.
func (k Key) SubjectKind() string { return k.subjectKind }
