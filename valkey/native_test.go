package valkey

import (
	"errors"
	"testing"
	"time"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
)

func TestNewRequiresNativeClient(t *testing.T) {
	t.Parallel()

	store, err := New(nil, Options{Prefix: "rl", Timeout: time.Second})
	if store != nil || !errors.Is(err, ratelimit.ErrInvalidPolicy) {
		t.Fatalf("New(nil) = %v, %v", store, err)
	}
}

func TestValkeyMajorParsesServerInfo(t *testing.T) {
	t.Parallel()

	major, err := valkeyMajor("# Server\r\nvalkey_version:9.1.2\r\n")
	if err != nil || major != 9 {
		t.Fatalf("valkeyMajor() = %d, %v", major, err)
	}
	if _, err := valkeyMajor("redis_version:7.0"); err == nil {
		t.Fatal("valkeyMajor() error = nil")
	}
}
