package valkey

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"

	ratelimit "github.com/faustbrian/go-rate-limit"
	valkeygo "github.com/valkey-io/valkey-go"
)

// NewStrict constructs a Store after panic-safe client validation.
func NewStrict(client valkeygo.Client, options Options) (*Store, error) {
	if strictNil(client) {
		return nil, fmt.Errorf("%w: Valkey client is required", ratelimit.ErrInvalidPolicy)
	}
	return New(client, options)
}

// OpenStrict constructs a Store and performs a strict startup check.
func OpenStrict(ctx context.Context, client valkeygo.Client, options Options) (*Store, error) {
	return openStrictChecked(ctx, func() (*Store, error) { return NewStrict(client, options) })
}

func openStrictChecked(ctx context.Context, construct func() (*Store, error)) (*Store, error) {
	if strictNil(ctx) {
		return nil, fmt.Errorf("%w: context is required", ratelimit.ErrInvalidRequest)
	}
	store, err := construct()
	if err != nil {
		return nil, err
	}
	if err := store.CheckStrict(ctx); err != nil {
		return nil, err
	}
	return store, nil
}

// AdmitStrict evaluates one request with explicit Lua dispatch outcomes.
func (store *Store) AdmitStrict(ctx context.Context, request ratelimit.Request) (ratelimit.Decision, error) {
	if err := store.strictReady(ctx); err != nil {
		return ratelimit.Decision{}, err
	}
	if err := request.Validate(); err != nil {
		return ratelimit.Decision{}, err
	}
	if err := strictContext(ctx); err != nil {
		return ratelimit.Decision{}, err
	}
	if request.Policy.Algorithm() == ratelimit.Concurrency {
		return ratelimit.Decision{}, ratelimit.ErrUnsupported
	}
	callCtx, cancel := context.WithTimeout(ctx, store.options.Timeout)
	defer cancel()
	args := store.args(request)
	args[10] = strconv.FormatInt(valkeyStateTTL(request.Policy.Period()).Milliseconds(), 10)
	args = append(args, "strict")
	reply, err := store.executor.exec(callCtx, []string{store.key(request)}, args)
	if err != nil {
		return ratelimit.Decision{}, ratelimit.UnknownOutcome(callCtx.Err())
	}
	return decodeStrictDecisionReply(reply)
}

// AcquireStrict obtains one lease with explicit Lua dispatch outcomes.
func (store *Store) AcquireStrict(ctx context.Context, request ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
	if err := store.strictReady(ctx); err != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, err
	}
	if err := request.Validate(); err != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, err
	}
	if err := strictContext(ctx); err != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, err
	}
	executor, ok := store.executor.(leaseExecutor)
	if !ok {
		return ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrUnsupported
	}
	callCtx, cancel := context.WithTimeout(ctx, store.options.Timeout)
	defer cancel()
	digest := sha256.Sum256([]byte(request.LeaseID))
	now := request.Request.Now.UnixMicro()
	serverClock := "0"
	if store.options.Clock == ServerClock {
		serverClock = "1"
	}
	ttl := valkeyStateTTL(request.Request.Policy.LeaseDuration())
	args := []string{
		"1", request.Request.Policy.ID(), request.Request.Policy.Revision(),
		strconv.FormatUint(request.Request.Policy.Limit(), 10),
		strconv.FormatUint(request.Request.Cost, 10), strconv.FormatInt(now, 10),
		strconv.FormatInt(request.Request.Policy.LeaseDuration().Microseconds(), 10),
		strconv.FormatInt(ttl.Milliseconds(), 10), serverClock, hex.EncodeToString(digest[:]), "strict",
	}
	reply, err := executor.acquire(callCtx, []string{store.key(request.Request)}, args)
	if err != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.UnknownOutcome(callCtx.Err())
	}
	return decodeStrictLeaseReply(reply, request)
}

// ReleaseStrict releases one lease with explicit Lua dispatch outcomes.
func (store *Store) ReleaseStrict(ctx context.Context, lease ratelimit.Lease) error {
	if err := store.strictReady(ctx); err != nil {
		return err
	}
	if lease.ID == "" || lease.PolicyID == "" || lease.Key.String() == "" || lease.Cost == 0 || lease.ExpiresAt.IsZero() {
		return ratelimit.ErrInvalidRequest
	}
	if err := strictContext(ctx); err != nil {
		return err
	}
	executor, ok := store.executor.(leaseExecutor)
	if !ok {
		return ratelimit.ErrUnsupported
	}
	callCtx, cancel := context.WithTimeout(ctx, store.options.Timeout)
	defer cancel()
	digest := sha256.Sum256([]byte(lease.ID))
	requestKey := sha256.Sum256([]byte(lease.PolicyID + "\x00" + lease.Key.String()))
	key := store.options.Prefix + ":{" + hex.EncodeToString(requestKey[:]) + "}"
	reply, err := executor.release(callCtx, []string{key}, []string{
		"1", lease.PolicyID, hex.EncodeToString(digest[:]),
		strconv.FormatUint(lease.Cost, 10), strconv.FormatInt(lease.ExpiresAt.UnixMicro(), 10), "strict",
	})
	if err != nil {
		return ratelimit.UnknownOutcome(callCtx.Err())
	}
	if len(reply) != 1 {
		return ratelimit.UnknownOutcome(nil)
	}
	switch reply[0] {
	case "ok":
		return nil
	case "not_found":
		return ratelimit.ErrLeaseNotFound
	case "not_owned":
		return ratelimit.ErrLeaseNotOwned
	case "corrupt":
		return ratelimit.ErrCorrupt
	default:
		return ratelimit.UnknownOutcome(nil)
	}
}

// CheckStrict verifies Valkey version and eviction policy without mutation.
func (store *Store) CheckStrict(ctx context.Context) error {
	if err := store.strictReady(ctx); err != nil {
		return err
	}
	if err := strictContext(ctx); err != nil {
		return err
	}
	native, ok := store.executor.(*nativeExecutor)
	if !ok {
		return ratelimit.ErrUnsupported
	}
	info, err := native.info(ctx)
	if err != nil {
		return strictValkeyPredispatchCategory(ctx, err)
	}
	major, err := valkeyMajor(info)
	if err != nil || major < 9 {
		return ratelimit.ErrUnavailable
	}
	config, err := native.config(ctx)
	if err != nil {
		return strictValkeyPredispatchCategory(ctx, err)
	}
	if config["maxmemory-policy"] != "noeviction" {
		return ratelimit.ErrUnavailable
	}
	return nil
}

func strictValkeyPredispatchCategory(ctx context.Context, err error) error {
	if err := strictContext(ctx); err != nil {
		return err
	}
	if err == context.Canceled { //nolint:errorlint // Strict dispatch accepts only the direct cancellation sentinel.
		return ratelimit.CanceledOutcome()
	}
	if err == context.DeadlineExceeded { //nolint:errorlint // Strict dispatch accepts only the direct deadline sentinel.
		return ratelimit.DeadlineOutcome()
	}
	if backendErr, ok := err.(*ratelimit.BackendError); ok { //nolint:errorlint // Strict dispatch accepts only a direct declared BackendError.
		if backendErr == nil {
			return ratelimit.ErrUnavailable
		}
		if backendErr.Category() == nil {
			return ratelimit.ErrUnavailable
		}
		return backendErr
	}
	if err == ratelimit.ErrRejected { //nolint:errorlint // Strict dispatch accepts only the direct rejection sentinel.
		return err
	}
	if err == ratelimit.ErrOverflow || err == ratelimit.ErrCorrupt || //nolint:errorlint // Strict dispatch accepts only direct safe sentinels.
		err == ratelimit.ErrUnsupported || err == ratelimit.ErrLeaseNotFound || err == ratelimit.ErrLeaseNotOwned { //nolint:errorlint // Strict dispatch accepts only direct safe sentinels.
		return err
	}
	return ratelimit.ErrUnavailable
}

func (store *Store) strictReady(ctx context.Context) error {
	if store == nil || store.executor == nil {
		return fmt.Errorf("%w: backend is nil or uninitialized", ratelimit.ErrInvalidPolicy)
	}
	if strictNil(ctx) {
		return fmt.Errorf("%w: context is required", ratelimit.ErrInvalidRequest)
	}
	return nil
}

func strictContext(ctx context.Context) error {
	if ctx.Err() == context.Canceled {
		return ratelimit.CanceledOutcome()
	}
	if ctx.Err() == context.DeadlineExceeded {
		return ratelimit.DeadlineOutcome()
	}
	return nil
}

func strictValkeyCategory(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, ratelimit.ErrRejected):
		return ratelimit.ErrRejected
	case errors.Is(err, ratelimit.ErrOverflow):
		return ratelimit.ErrOverflow
	case errors.Is(err, ratelimit.ErrCorrupt):
		return ratelimit.ErrCorrupt
	case errors.Is(err, ratelimit.ErrLeaseNotFound):
		return ratelimit.ErrLeaseNotFound
	case errors.Is(err, ratelimit.ErrLeaseNotOwned):
		return ratelimit.ErrLeaseNotOwned
	default:
		return ratelimit.ErrCorrupt
	}
}

func decodeStrictDecisionReply(reply []string) (ratelimit.Decision, error) {
	if len(reply) == 6 && reply[0] == "-1" {
		decision, err := decodeDecision(reply)
		return decision, strictValkeyCategory(err)
	}
	if len(reply) != 7 {
		return ratelimit.Decision{}, ratelimit.UnknownOutcome(nil)
	}
	effectiveMicros, err := strconv.ParseInt(reply[6], 10, 64)
	if err != nil {
		return ratelimit.Decision{}, ratelimit.UnknownOutcome(nil)
	}
	decision, resultErr := decodeDecision(reply[:6])
	if resultErr != nil && !errors.Is(resultErr, ratelimit.ErrRejected) {
		return ratelimit.Decision{}, ratelimit.UnknownOutcome(nil)
	}
	if !decision.Reset.IsZero() && decision.Reset.Before(time.UnixMicro(effectiveMicros)) {
		return ratelimit.Decision{}, ratelimit.UnknownOutcome(nil)
	}
	return decision, strictValkeyCategory(resultErr)
}

func decodeStrictLeaseReply(reply []string, request ratelimit.LeaseRequest) (ratelimit.Lease, ratelimit.Decision, error) {
	if len(reply) == 7 && reply[0] == "-1" {
		switch reply[5] {
		case "overflow":
			return ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrOverflow
		case "not_found":
			return ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrLeaseNotFound
		case "not_owned":
			return ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrLeaseNotOwned
		case "corrupt":
			return ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.ErrCorrupt
		default:
			return ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.UnknownOutcome(nil)
		}
	}
	if len(reply) != 8 {
		return ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.UnknownOutcome(nil)
	}
	effectiveMicros, err := strconv.ParseInt(reply[7], 10, 64)
	if err != nil {
		return ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.UnknownOutcome(nil)
	}
	lease, decision, resultErr := decodeLeaseReply(reply[:7], request)
	if resultErr != nil && !errors.Is(resultErr, ratelimit.ErrRejected) {
		return ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.UnknownOutcome(nil)
	}
	effective := time.UnixMicro(effectiveMicros)
	if !decision.Reset.IsZero() && decision.Reset.Before(effective) {
		return ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.UnknownOutcome(nil)
	}
	if errors.Is(resultErr, ratelimit.ErrRejected) {
		return ratelimit.Lease{}, decision, ratelimit.ErrRejected
	}
	if !lease.ExpiresAt.IsZero() && (!lease.ExpiresAt.After(effective) || !lease.ExpiresAt.Equal(decision.Reset)) {
		return ratelimit.Lease{}, ratelimit.Decision{}, ratelimit.UnknownOutcome(nil)
	}
	return lease, decision, strictValkeyCategory(resultErr)
}

func strictNil(value any) bool {
	if value == nil {
		return true
	}
	current := reflect.ValueOf(value)
	switch current.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return current.IsNil()
	default:
		return false
	}
}

func valkeyStateTTL(duration time.Duration) time.Duration {
	if duration > time.Duration(math.MaxInt64)/2 {
		return time.Duration(math.MaxInt64)
	}
	return max(duration*2, time.Second)
}
