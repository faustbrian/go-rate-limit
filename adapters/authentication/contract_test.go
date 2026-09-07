package ratelimitauthentication_test

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
	ratelimitauthentication "github.com/faustbrian/go-rate-limit/adapters/authentication"
)

type principal struct {
	calls   int
	subject string
}
type valuePrincipal string

type changingPrincipal struct{ calls int }
type principalFunc func() string

func (callback principalFunc) Subject() string { return callback() }

func (principal *changingPrincipal) Subject() string {
	principal.calls++
	if principal.calls == 1 {
		return "first"
	}
	return "second"
}

func (principal valuePrincipal) Subject() string { return string(principal) }

func (principal *principal) Subject() string { principal.calls++; return principal.subject }
func TestKeySnapshotsSubjectOnceAndRejectsTypedNil(t *testing.T) {
	value := &principal{subject: "subject"}
	if _, err := ratelimitauthentication.Key(value); err != nil || value.calls != 1 {
		t.Fatalf("Key() calls = %d, error = %v", value.calls, err)
	}
	var absent *principal
	if _, err := ratelimitauthentication.Key(absent); !errors.Is(err, ratelimit.ErrInvalidPolicy) || err.Error() != "invalid rate limit policy: principal is required" {
		t.Fatalf("typed nil error = %v", err)
	}
	if _, err := ratelimitauthentication.Key(nil); !errors.Is(err, ratelimit.ErrInvalidPolicy) || err.Error() != "invalid rate limit policy: principal is required" {
		t.Fatalf("nil error = %v", err)
	}
	if _, err := ratelimitauthentication.Key(&principal{}); !errors.Is(err, ratelimit.ErrInvalidKey) {
		t.Fatalf("empty error = %v", err)
	}
	if _, err := ratelimitauthentication.Key(valuePrincipal("value")); err != nil {
		t.Fatalf("value principal error = %v", err)
	}
	changing := &changingPrincipal{}
	key, err := ratelimitauthentication.Key(changing)
	want, _ := ratelimit.NewKey(ratelimit.KeySpec{Namespace: "auth", Version: "v1", Subject: ratelimit.Subject{Kind: "principal", Value: "first"}, Hash: true})
	if err != nil || changing.calls != 1 || key != want {
		t.Fatalf("snapshot=%q want=%q calls=%d err=%v", key.String(), want.String(), changing.calls, err)
	}
	empty := &principal{}
	if _, err := ratelimitauthentication.Key(empty); !errors.Is(err, ratelimit.ErrInvalidKey) || empty.calls != 1 {
		t.Fatalf("empty calls=%d err=%v", empty.calls, err)
	}
}

func TestSubjectLifecycle(t *testing.T) {
	t.Run("blocks caller", func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = ratelimitauthentication.Key(principalFunc(func() string { close(entered); <-release; return "subject" }))
		}()
		awaitAuthenticationSignal(t, entered)
		select {
		case <-done:
			t.Fatal("Key returned before Subject completed")
		default:
		}
		close(release)
		awaitAuthenticationSignal(t, done)
	})

	t.Run("overlaps", func(t *testing.T) {
		entered := make(chan struct{}, 2)
		release := make(chan struct{})
		done := make(chan struct{}, 2)
		shared := principalFunc(func() string { entered <- struct{}{}; <-release; return "subject" })
		for range 2 {
			go func() { _, _ = ratelimitauthentication.Key(shared); done <- struct{}{} }()
		}
		awaitAuthenticationSignal(t, entered)
		awaitAuthenticationSignal(t, entered)
		close(release)
		awaitAuthenticationSignal(t, done)
		awaitAuthenticationSignal(t, done)
	})

	t.Run("reentry", func(t *testing.T) {
		var entered atomic.Bool
		callback := principalFunc(func() string {
			if entered.CompareAndSwap(false, true) {
				if _, err := ratelimitauthentication.Key(valuePrincipal("inner")); err != nil {
					t.Fatalf("reentrant Key: %v", err)
				}
			}
			return "outer"
		})
		if _, err := ratelimitauthentication.Key(callback); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("panic propagates", func(t *testing.T) {
		panicValue := &struct{ owner string }{"caller"}
		defer func() {
			if recovered := recover(); recovered != panicValue {
				t.Fatalf("panic = %v", recovered)
			}
		}()
		_, _ = ratelimitauthentication.Key(principalFunc(func() string { panic(panicValue) }))
	})

	t.Run("result retains snapshot only", func(t *testing.T) {
		principal := &principal{subject: "first"}
		key, err := ratelimitauthentication.Key(principal)
		if err != nil {
			t.Fatal(err)
		}
		principal.subject = "second"
		want, _ := ratelimit.NewKey(ratelimit.KeySpec{Namespace: "auth", Version: "v1", Subject: ratelimit.Subject{Kind: "principal", Value: "first"}, Hash: true})
		if key != want {
			t.Fatalf("snapshot changed: %q", key.String())
		}
	})
}

func awaitAuthenticationSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Subject")
	}
}
