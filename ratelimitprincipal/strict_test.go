package ratelimitprincipal_test

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
	"github.com/faustbrian/go-rate-limit/ratelimitprincipal"
)

type nilPrincipal struct{}

func (*nilPrincipal) Subject() string { panic("must not be called") }

type strictPrincipal struct {
	subject string
	calls   int
}

func (principal *strictPrincipal) Subject() string { principal.calls++; return principal.subject }

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

func TestKeyStrictRejectsTypedNil(t *testing.T) {
	var principal *nilPrincipal
	key, err := ratelimitprincipal.KeyStrict(principal)
	if key.String() != "" || !errors.Is(err, ratelimit.ErrInvalidPolicy) || err.Error() != "invalid rate limit policy: principal is required" {
		t.Fatalf("KeyStrict() = %v, %v", key, err)
	}
}

func TestKeyStrictSnapshotsAndValidatesSubject(t *testing.T) {
	principal := &strictPrincipal{subject: "user"}
	key, err := ratelimitprincipal.KeyStrict(principal)
	if err != nil || key.String() == "" || principal.calls != 1 {
		t.Fatalf("KeyStrict() = %q, %v, calls %d", key.String(), err, principal.calls)
	}
	if _, err := ratelimitprincipal.KeyStrict(nil); !errors.Is(err, ratelimit.ErrInvalidPolicy) || err.Error() != "invalid rate limit policy: principal is required" {
		t.Fatalf("nil error = %v", err)
	}
	if _, err := ratelimitprincipal.KeyStrict(&strictPrincipal{}); !errors.Is(err, ratelimit.ErrInvalidKey) {
		t.Fatalf("empty error = %v", err)
	}
	if _, err := ratelimitprincipal.KeyStrict(valuePrincipal("value")); err != nil {
		t.Fatalf("value error = %v", err)
	}
	changing := &changingPrincipal{}
	key, err = ratelimitprincipal.KeyStrict(changing)
	want, _ := ratelimit.NewKey(ratelimit.KeySpec{Namespace: "auth", Version: "v1", Subject: ratelimit.Subject{Kind: "principal", Value: "first"}, Hash: true})
	if err != nil || changing.calls != 1 || key != want {
		t.Fatalf("snapshot=%q want=%q calls=%d err=%v", key.String(), want.String(), changing.calls, err)
	}
	empty := &strictPrincipal{}
	if _, err := ratelimitprincipal.KeyStrict(empty); !errors.Is(err, ratelimit.ErrInvalidKey) || empty.calls != 1 {
		t.Fatalf("empty calls=%d err=%v", empty.calls, err)
	}
}

func TestKeyStrictSubjectLifecycle(t *testing.T) {
	t.Run("blocks caller", func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = ratelimitprincipal.KeyStrict(principalFunc(func() string { close(entered); <-release; return "subject" }))
		}()
		awaitPrincipalSignal(t, entered)
		select {
		case <-done:
			t.Fatal("KeyStrict returned before Subject completed")
		default:
		}
		close(release)
		awaitPrincipalSignal(t, done)
	})

	t.Run("overlaps", func(t *testing.T) {
		entered := make(chan struct{}, 2)
		release := make(chan struct{})
		done := make(chan struct{}, 2)
		shared := principalFunc(func() string { entered <- struct{}{}; <-release; return "subject" })
		for range 2 {
			go func() { _, _ = ratelimitprincipal.KeyStrict(shared); done <- struct{}{} }()
		}
		awaitPrincipalSignal(t, entered)
		awaitPrincipalSignal(t, entered)
		close(release)
		awaitPrincipalSignal(t, done)
		awaitPrincipalSignal(t, done)
	})

	t.Run("reentry", func(t *testing.T) {
		var entered atomic.Bool
		callback := principalFunc(func() string {
			if entered.CompareAndSwap(false, true) {
				if _, err := ratelimitprincipal.KeyStrict(valuePrincipal("inner")); err != nil {
					t.Fatalf("reentrant KeyStrict: %v", err)
				}
			}
			return "outer"
		})
		if _, err := ratelimitprincipal.KeyStrict(callback); err != nil {
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
		_, _ = ratelimitprincipal.KeyStrict(principalFunc(func() string { panic(panicValue) }))
	})

	t.Run("result retains snapshot only", func(t *testing.T) {
		principal := &strictPrincipal{subject: "first"}
		key, err := ratelimitprincipal.KeyStrict(principal)
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

func awaitPrincipalSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Subject")
	}
}
