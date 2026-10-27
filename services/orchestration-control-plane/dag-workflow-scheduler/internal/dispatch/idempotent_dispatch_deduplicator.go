package dispatch

import (
	"context"
	"sync"
	"time"
)

// DefaultDispatchIdempotencyTTL bounds how long a dispatch idempotency key
// (see dispatch_idempotency_key.go) is remembered before it is eligible to
// be forgotten. It is sized well past this platform's longest documented
// single-node dispatch retry window (dispatchActivityRetryPolicy in
// internal/workflow/dag_pipeline_workflow.go caps at 5 attempts with a
// 1-minute max backoff, i.e. low single-digit minutes worst case) so a
// legitimate retry can never outlive the record meant to deduplicate it.
const DefaultDispatchIdempotencyTTL = 24 * time.Hour

// DispatchDeduplicationStore is the outbound port
// RedpandaExecutionFleetDispatcher depends on to guarantee that dispatching
// the same idempotency key (see DeriveDispatchIdempotencyKey) more than once
// only ever produces the node-dispatch message onto Redpanda a single time.
// Modeling it as an interface -- rather than reaching for a concrete Redis
// client directly -- lets internal/dispatch's own tests exercise the real
// dedup logic without a broker or a Redis instance, and lets a production
// deployment swap in a shared, multi-replica-safe store without touching
// execution_fleet_dispatcher.go.
type DispatchDeduplicationStore interface {
	// MarkDispatchedIfAbsent atomically records idempotencyKey as dispatched
	// unless it is already recorded (and not yet expired), returning
	// recordedNow=true only for the call that performed that recording.
	// Every subsequent call with the same key before ttl elapses must
	// observe recordedNow=false, so callers know to suppress the
	// corresponding side effect (the actual Redpanda produce) as a
	// duplicate. The semantics deliberately mirror Redis's `SET key val NX
	// PX ttl`: "set the key only if it does not already exist, expiring
	// after ttl", which is exactly what a production RedisDispatchDeduplicationStore
	// adapter implementing this same port would do with a single round trip
	// (see the doc comment on InMemoryDispatchDeduplicationStore below).
	MarkDispatchedIfAbsent(ctx context.Context, idempotencyKey string, ttl time.Duration) (recordedNow bool, err error)
}

// InMemoryDispatchDeduplicationStore is a DispatchDeduplicationStore backed
// by a process-local map instead of a shared cache. It is deliberately
// shaped like a Redis-backed implementation would be -- a single
// check-and-set style operation keyed by an opaque string, with per-key TTL
// expiry -- so that swapping in a real
// `RedisDispatchDeduplicationStore` (using e.g. go-redis's
// `client.SetNX(ctx, key, "1", ttl).Result()`, which is a single atomic
// Redis command providing the same "first caller wins" guarantee) is a
// drop-in replacement that requires no change to
// RedpandaExecutionFleetDispatcher.
//
// This in-memory adapter is correct for a single scheduler replica (and
// therefore sufficient for local dev and for this package's tests) but does
// NOT provide the cross-replica dedup guarantee a horizontally scaled
// dag-workflow-scheduler deployment needs in production -- see
// docs/architecture-decision-records/0011-cross-cutting-robustness-hardening.md
// and the "Idempotent dispatch" section of README.md for the operational
// note that production deployments must configure a
// RedisDispatchDeduplicationStore instead.
type InMemoryDispatchDeduplicationStore struct {
	mu deduplicationStoreMutex
	// recordedUntil maps an idempotency key to the wall-clock time its
	// record expires. A key present in this map with an expiry in the
	// future is "already dispatched"; a key absent, or present but expired,
	// is treated as never dispatched.
	recordedUntil map[string]time.Time
	// now is overridable so tests can exercise TTL expiry deterministically
	// without sleeping; defaults to time.Now.
	now func() time.Time
}

// deduplicationStoreMutex is a tiny named type alias purely so the field
// declaration above documents itself at the call site (`mu
// deduplicationStoreMutex` reads clearly as "guards the store") without
// resorting to a bare, uncommented sync.Mutex field.
type deduplicationStoreMutex = sync.Mutex

// NewInMemoryDispatchDeduplicationStore constructs an empty, ready-to-use
// InMemoryDispatchDeduplicationStore.
func NewInMemoryDispatchDeduplicationStore() *InMemoryDispatchDeduplicationStore {
	return &InMemoryDispatchDeduplicationStore{
		recordedUntil: make(map[string]time.Time),
		now:           time.Now,
	}
}

// MarkDispatchedIfAbsent implements DispatchDeduplicationStore. See the
// interface doc comment for the exact contract.
func (s *InMemoryDispatchDeduplicationStore) MarkDispatchedIfAbsent(_ context.Context, idempotencyKey string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	s.evictExpiredLocked(now)

	if expiresAt, alreadyRecorded := s.recordedUntil[idempotencyKey]; alreadyRecorded && now.Before(expiresAt) {
		return false, nil
	}

	s.recordedUntil[idempotencyKey] = now.Add(ttl)
	return true, nil
}

// evictExpiredLocked drops expired records so a long-lived scheduler process
// doesn't grow this map unboundedly across many pipeline runs. Callers must
// hold s.mu. This linear sweep is intentionally simple: dispatch volume for
// a single in-process (i.e. non-Redis, non-production) store is bounded by
// what one scheduler replica dispatches, which is orders of magnitude below
// where this would become a hot path.
func (s *InMemoryDispatchDeduplicationStore) evictExpiredLocked(now time.Time) {
	for key, expiresAt := range s.recordedUntil {
		if !now.Before(expiresAt) {
			delete(s.recordedUntil, key)
		}
	}
}
