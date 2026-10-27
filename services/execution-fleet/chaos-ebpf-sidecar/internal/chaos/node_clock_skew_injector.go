package chaos

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/distributed-testing-benchmarking-tool/chaos-ebpf-sidecar/internal/safety"
)

// SkewSpec describes a target clock offset and how gradually to reach it, used for
// time-travel testing of consensus protocols (Raft/Paxos leader-election timeouts, log
// term comparisons) and expiry-sensitive logic (JWT/TLS cert validity windows).
type SkewSpec struct {
	// OffsetDuration is the target steady-state offset from real time. Positive moves the
	// simulated clock into the future, negative into the past.
	OffsetDuration time.Duration
	// RampDuration is how long it takes to linearly ramp from zero offset to OffsetDuration.
	// Zero means "apply the full offset instantly" (a step function), which is the more
	// dangerous option since instantaneous large jumps are more likely to trip watchdogs or
	// crash-loop a consensus node rather than exercise its skew-tolerance logic.
	RampDuration time.Duration
}

// Validate checks that spec describes a boundable, sane skew schedule.
func (s SkewSpec) Validate() error {
	if s.OffsetDuration == 0 {
		return fmt.Errorf("skew spec: OffsetDuration must be non-zero; a zero offset is not a skew")
	}
	if s.RampDuration < 0 {
		return fmt.Errorf("skew spec: RampDuration must not be negative")
	}
	return nil
}

// NodeClockManipulator is the seam between this package's skew scheduling/ramp math and the
// actual privileged mechanism that makes a node's observable clock disagree with real time.
// Method names are exported so a real privileged implementation can be wired in from a
// separate package (e.g. cmd/chaos-ebpf-sidecar) rather than being forced into package chaos.
//
// NOTE: There is no safe, general way to change a Linux kernel's view of time for one
// workload without affecting the whole node (CLOCK_REALTIME is host-wide), so a real
// implementation has to pick one of:
//  1. A privileged sidecar container that runs `date -s "<skewed time>"` / manipulates
//     `/proc/sys/kernel/...`-adjacent chrony `offset` commands — affects the ENTIRE node,
//     acceptable only for genuinely single-tenant chaos test nodes.
//  2. A per-container time-namespace (Linux `CLONE_NEWTIME`, kernel 5.6+) applied only to the
//     target pod's namespace via a small privileged helper (requires CAP_SYS_TIME plus
//     namespace-entry tooling) — the safer, workload-scoped option.
//  3. An in-process LD_PRELOAD/eBPF uprobe that intercepts clock_gettime(2)/gettimeofday(2)
//     calls made by the target process and returns a skewed value, leaving the real host
//     clock untouched — highest isolation, most implementation effort.
//
// None of these are wired up here; SetNodeClockOffset should be treated as "describes intent"
// until one of the above is implemented.
type NodeClockManipulator interface {
	// SetNodeClockOffset applies (or updates) the effective clock offset for target.
	SetNodeClockOffset(ctx context.Context, target NetworkTarget, offset time.Duration) error
	// ClearNodeClockOffset restores target's clock to tracking real time with zero offset.
	ClearNodeClockOffset(ctx context.Context, target NetworkTarget) error
}

// noopNodeClockManipulator is the default backend: it records intent but never touches a real
// clock. Used for local dev, tests, and dry-run mode.
type noopNodeClockManipulator struct{}

func (noopNodeClockManipulator) SetNodeClockOffset(ctx context.Context, target NetworkTarget, offset time.Duration) error {
	return nil
}

func (noopNodeClockManipulator) ClearNodeClockOffset(ctx context.Context, target NetworkTarget) error {
	return nil
}

// activeSkew is the in-memory record of one target's in-progress or completed skew ramp,
// enabling both live "what's the effective simulated time right now" queries and clean
// reversion on shutdown.
type activeSkew struct {
	target    NetworkTarget
	spec      SkewSpec
	startedAt time.Time
	// unregisterFromBreaker deregisters this fault's rollback callback from the global chaos
	// circuit breaker's active-fault set. See activeDegradation.unregisterFromBreaker in
	// network_degradation_injector.go for the identical pattern.
	unregisterFromBreaker func()
}

// currentOffset returns the offset that should be in effect at wallClockNow, linearly ramping
// from zero at a.startedAt to a.spec.OffsetDuration at a.startedAt+a.spec.RampDuration, then
// holding steady at OffsetDuration thereafter.
func (a activeSkew) currentOffset(wallClockNow time.Time) time.Duration {
	if a.spec.RampDuration <= 0 {
		return a.spec.OffsetDuration
	}
	elapsed := wallClockNow.Sub(a.startedAt)
	if elapsed <= 0 {
		return 0
	}
	if elapsed >= a.spec.RampDuration {
		return a.spec.OffsetDuration
	}
	fraction := float64(elapsed) / float64(a.spec.RampDuration)
	return time.Duration(float64(a.spec.OffsetDuration) * fraction)
}

// NodeClockSkewInjector schedules a ramped clock-offset injection against a NetworkTarget
// (reused here purely as a node/pod identity struct — no networking fields are used) and
// exposes the effective simulated time for a given real-time instant, for both driving the
// privileged clock-manipulation seam and for test assertions that don't require one.
type NodeClockSkewInjector struct {
	clock NodeClockManipulator

	mu     sync.Mutex
	active map[string]*activeSkew
}

// NewNodeClockSkewInjector constructs an injector. A nil backend selects the safe no-op
// backend, matching the dry-run-by-default posture of the other chaos primitives.
func NewNodeClockSkewInjector(clock NodeClockManipulator) *NodeClockSkewInjector {
	if clock == nil {
		clock = noopNodeClockManipulator{}
	}
	return &NodeClockSkewInjector{clock: clock, active: make(map[string]*activeSkew)}
}

// BeginSkew validates spec and starts (or replaces) a ramped clock skew against target. The
// ramp itself is computed lazily by EffectiveSimulatedTime/CurrentOffset rather than by a
// background goroutine, since the true source of truth for "what time does the node think it
// is" must ultimately be re-derived from wall-clock time on every read to survive process
// restarts.
func (n *NodeClockSkewInjector) BeginSkew(ctx context.Context, target NetworkTarget, spec SkewSpec) error {
	// Global kill-switch check: this is the entry point for this injector's Start-equivalent
	// action. A tripped breaker refuses every new skew, independent of everything below.
	if err := safety.Global().Check(ctx); err != nil {
		return fmt.Errorf("begin clock skew: %w", err)
	}

	if err := target.validate(); err != nil {
		return fmt.Errorf("begin clock skew: %w", err)
	}
	if err := spec.Validate(); err != nil {
		return fmt.Errorf("begin clock skew: %w", err)
	}

	startedAt := time.Now()
	if err := n.clock.SetNodeClockOffset(ctx, target, spec.OffsetDuration); err != nil {
		return fmt.Errorf("begin clock skew: %w", err)
	}

	key := targetKey(target)
	record := &activeSkew{target: target, spec: spec, startedAt: startedAt}
	record.unregisterFromBreaker = safety.Global().RegisterActiveFault(
		"clock-skew:"+key,
		func(rollbackCtx context.Context) error { return n.EndSkew(rollbackCtx, target) },
	)

	n.mu.Lock()
	n.active[key] = record
	n.mu.Unlock()
	return nil
}

// EndSkew reverts target to zero offset and drops its bookkeeping record. Ending a target with
// no active skew is a no-op, matching NetworkDegradationInjector.Revert's shutdown-safe
// semantics.
func (n *NodeClockSkewInjector) EndSkew(ctx context.Context, target NetworkTarget) error {
	key := targetKey(target)

	n.mu.Lock()
	record, exists := n.active[key]
	if exists {
		delete(n.active, key)
	}
	n.mu.Unlock()

	if !exists {
		return nil
	}
	if record.unregisterFromBreaker != nil {
		record.unregisterFromBreaker()
	}
	if err := n.clock.ClearNodeClockOffset(ctx, target); err != nil {
		return fmt.Errorf("end clock skew for %s: %w", key, err)
	}
	return nil
}

// CurrentOffset returns the offset that would be in effect right now for target, or false if
// target has no active skew. Useful for telemetry/status reporting without needing a
// wall-clock instant supplied by the caller.
func (n *NodeClockSkewInjector) CurrentOffset(target NetworkTarget) (time.Duration, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	record, exists := n.active[targetKey(target)]
	if !exists {
		return 0, false
	}
	return record.currentOffset(time.Now()), true
}

// EffectiveSimulatedTime returns what target's clock should currently read: realNow plus
// whatever offset the active ramp has reached by realNow. If target has no active skew, it
// returns realNow unchanged.
func (n *NodeClockSkewInjector) EffectiveSimulatedTime(target NetworkTarget, realNow time.Time) time.Time {
	n.mu.Lock()
	record, exists := n.active[targetKey(target)]
	n.mu.Unlock()

	if !exists {
		return realNow
	}
	return realNow.Add(record.currentOffset(realNow))
}

// ActiveTargets returns the NetworkTargets that currently have a clock skew applied. Used by
// graceful-shutdown and crash-recovery reconciliation, mirroring
// NetworkDegradationInjector.ActiveTargets.
func (n *NodeClockSkewInjector) ActiveTargets() []NetworkTarget {
	n.mu.Lock()
	defer n.mu.Unlock()

	targets := make([]NetworkTarget, 0, len(n.active))
	for _, record := range n.active {
		targets = append(targets, record.target)
	}
	return targets
}
