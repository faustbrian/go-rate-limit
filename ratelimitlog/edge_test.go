package ratelimitlog

import (
	"errors"
	"testing"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
)

func TestObserverConfigurationAndErrorKinds(t *testing.T) {
	t.Parallel()

	if _, err := New(Options{}); !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("New(empty) error = %v", err)
	}
	tests := []struct {
		err  error
		want string
	}{
		{nil, "none"},
		{ratelimit.ErrRejected, "rejected"},
		{ratelimit.ErrDeadline, "deadline"},
		{ratelimit.ErrUnavailable, "unavailable"},
		{ratelimit.ErrOverflow, "overflow"},
		{ratelimit.ErrCorrupt, "corrupt"},
		{errors.New("other"), "internal"},
	}
	for _, test := range tests {
		if got := errorKind(test.err); got != test.want {
			t.Fatalf("errorKind(%v) = %q", test.err, got)
		}
	}
}
