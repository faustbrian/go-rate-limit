package valkey

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	ratelimit "github.com/faustbrian/golib/pkg/rate-limit"
	valkeygo "github.com/valkey-io/valkey-go"
)

var (
	nativeAdmitScript   = valkeygo.NewLuaScript(admitScript)
	nativeAcquireScript = valkeygo.NewLuaScript(acquireLeaseScript)
	nativeReleaseScript = valkeygo.NewLuaScript(releaseLeaseScript)
)

type nativeExecutor struct {
	client valkeygo.Client
	info   func(context.Context) (string, error)
	config func(context.Context) (map[string]string, error)
}

// New constructs a Store without performing network startup checks.
func New(client valkeygo.Client, options Options) (*Store, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: native Valkey client is required", ratelimit.ErrInvalidPolicy)
	}
	executor := &nativeExecutor{client: client}
	executor.info = func(ctx context.Context) (string, error) {
		return client.Do(ctx, client.B().Info().Section("server").Build()).ToString()
	}
	executor.config = func(ctx context.Context) (map[string]string, error) {
		return client.Do(
			ctx,
			client.B().ConfigGet().Parameter("maxmemory-policy").Build(),
		).AsStrMap()
	}
	return newStore(executor, options)
}

// Open constructs a Store and verifies Valkey version and eviction policy.
func Open(ctx context.Context, client valkeygo.Client, options Options) (*Store, error) {
	store, err := New(client, options)
	if err != nil {
		return nil, err
	}
	if err := store.Check(ctx); err != nil {
		return nil, err
	}
	return store, nil
}

// Check requires Valkey 9 or newer and a noeviction memory policy.
func (store *Store) Check(ctx context.Context) error {
	native, ok := store.executor.(*nativeExecutor)
	if !ok {
		return fmt.Errorf("%w: startup check requires native client", ratelimit.ErrUnsupported)
	}
	info, err := native.info(ctx)
	if err != nil {
		return fmt.Errorf("%w: server info", ratelimit.ErrUnavailable)
	}
	major, err := valkeyMajor(info)
	if err != nil {
		return fmt.Errorf("%w: Valkey 9 or newer required", ratelimit.ErrUnavailable)
	}
	if major < 9 {
		return fmt.Errorf("%w: Valkey 9 or newer required", ratelimit.ErrUnavailable)
	}
	config, err := native.config(ctx)
	if err != nil {
		return fmt.Errorf("%w: eviction policy", ratelimit.ErrUnavailable)
	}
	if config["maxmemory-policy"] != "noeviction" {
		return fmt.Errorf("%w: maxmemory-policy must be noeviction", ratelimit.ErrUnavailable)
	}
	return nil
}

func (executor *nativeExecutor) exec(ctx context.Context, keys, args []string) ([]string, error) {
	return executeScript(ctx, executor.client, nativeAdmitScript, keys, args)
}

func (executor *nativeExecutor) acquire(ctx context.Context, keys, args []string) ([]string, error) {
	return executeScript(ctx, executor.client, nativeAcquireScript, keys, args)
}

func (executor *nativeExecutor) release(ctx context.Context, keys, args []string) ([]string, error) {
	return executeScript(ctx, executor.client, nativeReleaseScript, keys, args)
}

func executeScript(ctx context.Context, client valkeygo.Client, script *valkeygo.Lua, keys, args []string) ([]string, error) {
	messages, err := script.Exec(ctx, client, keys, args).ToArray()
	if err != nil {
		return nil, err
	}
	reply := make([]string, len(messages))
	for index := range messages {
		reply[index], err = messages[index].ToString()
		if err != nil {
			return nil, err
		}
	}
	return reply, nil
}

func valkeyMajor(info string) (int, error) {
	for _, line := range strings.Split(info, "\n") {
		value, found := strings.CutPrefix(strings.TrimSpace(line), "valkey_version:")
		if !found {
			continue
		}
		major, _, _ := strings.Cut(value, ".")
		return strconv.Atoi(major)
	}
	return 0, errors.New("valkey version missing")
}
