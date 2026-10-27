// Package safety implements the chaos-ebpf-sidecar's global, incident-response-grade stop
// mechanism: a single circuit breaker that, once tripped, immediately halts and rolls back
// every active fault injection this sidecar instance is running and refuses to start any new
// one until an operator explicitly resets it.
//
// This is deliberately a different safety layer from the per-experiment blast-radius caps in
// internal/chaos (BlastRadiusLimit) and the per-experiment steady-state auto-abort in
// internal/hypothesis (HypothesisGuardedExperimentRunner): those two stop *one* experiment
// because that experiment's own data says to. GlobalChaosCircuitBreaker exists for the case
// where a human (or an automated incident-response system) needs to say "stop literally
// everything this sidecar is doing, right now, for a reason this sidecar cannot itself
// observe" — a real production incident unrelated to (or made worse by) whatever chaos
// experiment happens to be running. See chaos-ebpf-sidecar/SCALING.md's "Global chaos circuit
// breaker" section for how this fits the platform's blast-radius-of-last-resort story, and
// docs/architecture-decision-records/0007-hypothesis-guarded-chaos-and-global-kill-switch.md
// for why this exists as a distinct mechanism from the per-experiment safety controls.
package safety

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// KillSwitchMessage models the payload of a Redpanda "chaos.kill-switch.v1" topic message — the
// out-of-band, emergency-stop counterpart to the control plane's normal
// "dag.node.dispatch.v1" dispatch topic described in ARCHITECTURE.md's "Communication
// contracts" section. Any consumer of that topic (an incident-response tool, an on-call
// engineer's `kubectl exec`-triggered CLI, a future dedicated control-plane "panic button" UI
// action) produces one of these per emergency stop.
//
// NOTE: the real Redpanda consumer for chaos.kill-switch.v1 is not wired up in this starter
// repo, mirroring every other "seam documented, real I/O deferred" boundary in this service
// (see e.g. chaos.KernelNetworkFaultApplier's NOTE comment, or main.go's NOTE-documented
// not-yet-implemented dag.node.dispatch.v1 consumer loop). Production wiring subscribes a
// dedicated consumer group to chaos.kill-switch.v1 and calls
// GlobalChaosCircuitBreaker.Trip(ctx, msg) for every message received — deliberately a
// *separate* consumer group from the dag.node.dispatch.v1 one, so a kill-switch message is
// never stuck behind backlog on the normal dispatch topic.
type KillSwitchMessage struct {
	// Reason is a short human-readable explanation of why the kill switch was triggered, shown
	// in every subsequent Check() error until Reset.
	Reason string
	// TrippedBy identifies who or what issued the trip (an operator identity, an incident ID,
	// or an automated incident-response system's name) for audit purposes.
	TrippedBy string
	// TrippedAt is when the trip was issued, per the message producer's clock — recorded
	// as-is rather than re-stamped with this process's own clock, so multiple sidecar
	// instances consuming the same message agree on when the incident started.
	TrippedAt time.Time
}

func (m KillSwitchMessage) validate() error {
	if m.Reason == "" {
		return fmt.Errorf("kill switch message: Reason is required")
	}
	if m.TrippedBy == "" {
		return fmt.Errorf("kill switch message: TrippedBy is required")
	}
	if m.TrippedAt.IsZero() {
		return fmt.Errorf("kill switch message: TrippedAt is required")
	}
	return nil
}

// ActiveFaultRollback undoes one currently-active fault injection. Implementations are the
// small closures registered by internal/chaos's injectors (see RegisterActiveFault) — typically
// a direct call back into that injector's own Revert/EndSkew method for one specific target.
type ActiveFaultRollback func(ctx context.Context) error

// GlobalChaosCircuitBreaker is the safety mechanism described at the package level: tripping it
// blocks all new fault injection (via Check) and best-effort rolls back every fault currently
// registered via RegisterActiveFault. It is safe for concurrent use.
type GlobalChaosCircuitBreaker struct {
	mu            sync.RWMutex
	tripped       bool
	trippedReason string
	trippedBy     string
	trippedAt     time.Time
	activeFaults  map[string]ActiveFaultRollback
}

// NewGlobalChaosCircuitBreaker constructs an untripped breaker with no registered active
// faults. Most production code should use Global() (the process-wide singleton) rather than
// constructing its own instance directly, so every injector in this sidecar process shares one
// kill switch; this constructor exists mainly for tests that want an isolated breaker.
func NewGlobalChaosCircuitBreaker() *GlobalChaosCircuitBreaker {
	return &GlobalChaosCircuitBreaker{activeFaults: make(map[string]ActiveFaultRollback)}
}

var (
	globalOnce     sync.Once
	globalInstance *GlobalChaosCircuitBreaker
	globalMu       sync.RWMutex
)

// Global returns the process-wide GlobalChaosCircuitBreaker singleton, lazily constructing it
// on first use. Every chaos primitive in internal/chaos consults this instance (see e.g.
// NetworkDegradationInjector.Apply's entry-point check) so a single kill-switch trip reaches
// every fault-injection code path in this sidecar instance without each caller needing to be
// individually wired with a breaker reference.
func Global() *GlobalChaosCircuitBreaker {
	globalOnce.Do(func() {
		globalMu.Lock()
		globalInstance = NewGlobalChaosCircuitBreaker()
		globalMu.Unlock()
	})
	globalMu.RLock()
	defer globalMu.RUnlock()
	return globalInstance
}

// ReplaceGlobal swaps the process-wide singleton returned by Global, returning a restore func
// that puts the previous instance back. Exists so tests can exercise trip/reset behavior
// against a fresh, isolated breaker without cross-test contamination via shared global state —
// production code should never call this outside of test setup.
func ReplaceGlobal(b *GlobalChaosCircuitBreaker) (restore func()) {
	// Ensure the lazy-init path in Global() never fires after this point; if Global() was never
	// called yet, this also performs the first-ever initialization (with an instance we
	// immediately overwrite below), which is harmless.
	globalOnce.Do(func() {})

	globalMu.Lock()
	previous := globalInstance
	globalInstance = b
	globalMu.Unlock()

	return func() {
		globalMu.Lock()
		globalInstance = previous
		globalMu.Unlock()
	}
}

// Check returns an error if the breaker is currently tripped, and nil otherwise. Every existing
// injector's Inject/Start-equivalent method (NetworkDegradationInjector.Apply,
// NodeClockSkewInjector.BeginSkew, BlastRadiusControlledPodKiller.KillMatching*) calls this
// first and refuses to proceed if it returns an error — this is what "blocks new injections
// until manually reset" means in practice. Check performs no side effects.
func (b *GlobalChaosCircuitBreaker) Check(ctx context.Context) error {
	_ = ctx // accepted for symmetry with every other seam in this codebase that takes ctx first
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.tripped {
		return fmt.Errorf(
			"global chaos circuit breaker: tripped at %s by %q (reason: %q); refusing new fault injection until an operator calls Reset",
			b.trippedAt.Format(time.RFC3339), b.trippedBy, b.trippedReason,
		)
	}
	return nil
}

// IsTripped reports the current trip state as a plain boolean, for callers that just need a
// status flag (health/readiness reporting, telemetry) rather than Check's formatted error.
func (b *GlobalChaosCircuitBreaker) IsTripped() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.tripped
}

// RegisterActiveFault records rollback as the way to undo one currently-active fault injection,
// keyed by id. Callers should use a domain-prefixed, target-derived key (e.g.
// "network-degradation:"+targetKey(target) or "clock-skew:"+targetKey(target)) so keys never
// collide across injector types even when they happen to share the same underlying target. The
// returned unregister func MUST be called once the fault is reverted through its normal path
// (Revert/EndSkew), so a later Trip does not attempt to roll back a fault that is already
// clean — rolling back an already-clean target is safe in every injector's Revert/EndSkew
// implementation, but skipping the redundant call keeps Trip's fault count accurate.
func (b *GlobalChaosCircuitBreaker) RegisterActiveFault(id string, rollback ActiveFaultRollback) (unregister func()) {
	b.mu.Lock()
	b.activeFaults[id] = rollback
	b.mu.Unlock()

	return func() {
		b.mu.Lock()
		delete(b.activeFaults, id)
		b.mu.Unlock()
	}
}

// ActiveFaultCount reports how many faults are currently registered as roll-back-able,
// primarily for tests and status/telemetry reporting.
func (b *GlobalChaosCircuitBreaker) ActiveFaultCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.activeFaults)
}

// Trip immediately blocks all new fault injection (every subsequent Check call fails until
// Reset) and then rolls back every fault registered via RegisterActiveFault, best-effort: a
// rollback failure for one fault does not stop the others from being attempted. Returns a
// combined error describing any rollback failures, but the breaker is tripped — and new
// injection blocked — regardless of whether every rollback succeeded, since "some faults could
// not be cleanly reverted" must never be mistaken for "it is safe to keep injecting".
func (b *GlobalChaosCircuitBreaker) Trip(ctx context.Context, msg KillSwitchMessage) error {
	if err := msg.validate(); err != nil {
		return fmt.Errorf("global chaos circuit breaker: refusing to trip on invalid kill-switch message: %w", err)
	}

	b.mu.Lock()
	b.tripped = true
	b.trippedReason = msg.Reason
	b.trippedBy = msg.TrippedBy
	b.trippedAt = msg.TrippedAt
	faultsToRollBack := make(map[string]ActiveFaultRollback, len(b.activeFaults))
	for id, rollback := range b.activeFaults {
		faultsToRollBack[id] = rollback
	}
	b.mu.Unlock()

	var failures []string
	for id, rollback := range faultsToRollBack {
		if err := rollback(ctx); err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", id, err))
			continue
		}
		b.mu.Lock()
		delete(b.activeFaults, id)
		b.mu.Unlock()
	}

	if len(failures) > 0 {
		return fmt.Errorf(
			"global chaos circuit breaker: tripped (reason: %q), but %d of %d active fault rollback(s) failed: %v",
			msg.Reason, len(failures), len(faultsToRollBack), failures,
		)
	}
	return nil
}

// Reset manually clears the tripped state, re-enabling new fault injection. There is
// deliberately no automatic reset path (a timer, a health-check recovering) — a global
// kill-switch trip means a human (or an equally deliberate automated incident-response action)
// decided this sidecar should stop, and only an equally deliberate action should decide it may
// resume.
func (b *GlobalChaosCircuitBreaker) Reset(resetBy string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.tripped = false
	b.trippedReason = ""
	b.trippedBy = ""
	b.trippedAt = time.Time{}

	// NOTE: production wiring should append resetBy plus a timestamp to an audit log / emit an
	// OTLP chaos-event here so "who reset the kill switch and when" is durably recorded
	// alongside the trip itself. Not implemented in this starter repo, matching the established
	// pattern of documenting-not-implementing telemetry emission points elsewhere in this
	// service (see e.g. NetworkDegradationInjector.autoRevertAfter's NOTE comment).
	_ = resetBy
}
