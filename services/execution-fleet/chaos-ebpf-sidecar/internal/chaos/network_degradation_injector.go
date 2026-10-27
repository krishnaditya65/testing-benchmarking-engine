// Package chaos implements privileged, node-local fault-injection primitives for the
// chaos-ebpf-sidecar agent. Every type in this package is DANGEROUS: correctly used it
// degrades or kills real workloads on purpose. Callers must enforce dry-run/safety gating
// (see cmd/chaos-ebpf-sidecar/main.go) before invoking any Apply/Kill/Skew method for real.
package chaos

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/distributed-testing-benchmarking-tool/chaos-ebpf-sidecar/internal/safety"
)

// NetworkTarget identifies the network scope a degradation profile is applied to: a single
// pod's veth/eth interface as seen from the host network namespace, or a raw interface name
// for node-wide injection (e.g. testing a node's uplink rather than one workload).
type NetworkTarget struct {
	// NodeName is the Kubernetes node this sidecar instance is running on (DaemonSet identity).
	NodeName string
	// PodUID, when set, scopes the injection to one pod's network namespace.
	PodUID string
	// NetworkInterfaceName is the host-visible interface (e.g. "veth1a2b3c" or "eth0") that
	// the tc qdisc / eBPF TC hook will be attached to.
	NetworkInterfaceName string
}

func (t NetworkTarget) validate() error {
	if t.NodeName == "" {
		return fmt.Errorf("network target: NodeName is required")
	}
	if t.NetworkInterfaceName == "" {
		return fmt.Errorf("network target: NetworkInterfaceName is required")
	}
	return nil
}

// DegradationProfile models the parameters of a tc-netem-style network fault: added latency
// with jitter, random packet loss, and probabilistic injected TCP RSTs (simulating a flaky
// upstream or a mid-flight connection reset).
type DegradationProfile struct {
	// LatencyMs is the base one-way latency added to matched traffic, in milliseconds.
	LatencyMs uint32
	// JitterMs is the +/- variance applied around LatencyMs per packet (tc netem "delay X Yms").
	JitterMs uint32
	// PacketLossPercent is the probability, 0.0-100.0, that a given packet is dropped.
	PacketLossPercent float64
	// ResetProbability is the probability, 0.0-1.0, that an open TCP connection matched by this
	// profile receives an injected RST per evaluation tick (see resetEvaluationInterval).
	ResetProbability float64
	// Duration bounds how long the degradation stays active before auto-reverting. Zero means
	// "apply until explicitly reverted" and is intentionally the more dangerous default, so
	// callers should almost always set a non-zero Duration.
	Duration time.Duration
}

// Validate checks that a DegradationProfile describes a physically sane, boundable fault. It
// is called before scheduling to prevent parameter mistakes (e.g. 150% packet loss) from ever
// reaching the kernel/eBPF seam.
func (p DegradationProfile) Validate() error {
	if p.JitterMs > p.LatencyMs && p.LatencyMs != 0 {
		// Jitter larger than base latency is legal in tc netem (it can floor at 0), but for a
		// deliberate chaos experiment it usually indicates a mistyped profile.
		return fmt.Errorf("degradation profile: JitterMs (%dms) exceeds LatencyMs (%dms); reduce jitter or raise latency", p.JitterMs, p.LatencyMs)
	}
	if p.PacketLossPercent < 0 || p.PacketLossPercent > 100 {
		return fmt.Errorf("degradation profile: PacketLossPercent must be within [0,100], got %f", p.PacketLossPercent)
	}
	if p.ResetProbability < 0 || p.ResetProbability > 1 {
		return fmt.Errorf("degradation profile: ResetProbability must be within [0,1], got %f", p.ResetProbability)
	}
	if p.LatencyMs == 0 && p.PacketLossPercent == 0 && p.ResetProbability == 0 {
		return fmt.Errorf("degradation profile: at least one of LatencyMs, PacketLossPercent, ResetProbability must be non-zero")
	}
	if p.Duration < 0 {
		return fmt.Errorf("degradation profile: Duration must not be negative")
	}
	return nil
}

// KernelNetworkFaultApplier is the seam between this package's validated, scheduled
// degradation logic and the actual privileged kernel call that makes traffic misbehave.
// Method names are exported so that a real privileged implementation can live in a separate
// package (e.g. wired from cmd/chaos-ebpf-sidecar) rather than being forced into package chaos.
//
// NOTE: A real implementation shells out to `tc qdisc add dev <iface> root netem delay
// <LatencyMs>ms <JitterMs>ms loss <PacketLossPercent>%` (and a companion eBPF TC classifier
// program, loaded via internal/ebpf.EbpfProgramLoader, for the TCP-reset-injection path,
// since netem alone cannot targeted-RST a live connection). Both require CAP_NET_ADMIN and
// CAP_BPF in the sidecar's container, a hostNetwork/hostPID-adjacent pod spec, and running in
// the correct network namespace for NetworkTarget.PodUID via `ip netns exec` or
// setns(2)/nsenter equivalents. None of that privileged wiring exists in this starter repo.
type KernelNetworkFaultApplier interface {
	// ApplyNetem installs the qdisc-based delay/jitter/loss discipline on the target interface.
	ApplyNetem(ctx context.Context, target NetworkTarget, profile DegradationProfile) error
	// ApplyTCPResetInjection attaches/updates the eBPF TC program responsible for probabilistic
	// RST injection on matched connections.
	ApplyTCPResetInjection(ctx context.Context, target NetworkTarget, resetProbability float64) error
	// RevertNetworkFault removes any netem qdisc and detaches the reset-injection program from
	// target.
	RevertNetworkFault(ctx context.Context, target NetworkTarget) error
}

// noopKernelFaultApplier is the default KernelNetworkFaultApplier used when no privileged
// backend has been wired in (e.g. local dev, unit tests, `--dry-run`). It records what it
// would have done instead of touching the kernel.
type noopKernelFaultApplier struct{}

func (noopKernelFaultApplier) ApplyNetem(ctx context.Context, target NetworkTarget, profile DegradationProfile) error {
	return nil
}

func (noopKernelFaultApplier) ApplyTCPResetInjection(ctx context.Context, target NetworkTarget, resetProbability float64) error {
	return nil
}

func (noopKernelFaultApplier) RevertNetworkFault(ctx context.Context, target NetworkTarget) error {
	return nil
}

// activeDegradation is the in-memory bookkeeping record for one currently-applied fault, kept
// so that (a) Revert can undo it and (b) main.go's crash-recovery / SIGTERM path can revert
// every still-active fault without an external source of truth.
type activeDegradation struct {
	target    NetworkTarget
	profile   DegradationProfile
	appliedAt time.Time
	cancel    context.CancelFunc
	// unregisterFromBreaker deregisters this fault's rollback callback from the global chaos
	// circuit breaker's active-fault set (see safety.GlobalChaosCircuitBreaker.RegisterActiveFault).
	// Called from Revert so a normal, already-clean revert doesn't leave a stale rollback
	// registered for the breaker to redundantly (though safely) invoke on a future Trip.
	unregisterFromBreaker func()
}

// NetworkDegradationInjector schedules and applies tc-netem-style network faults (latency,
// jitter, packet loss, TCP resets) against a NetworkTarget. It is safe for concurrent use.
type NetworkDegradationInjector struct {
	kernel KernelNetworkFaultApplier

	mu     sync.Mutex
	active map[string]*activeDegradation // keyed by targetKey(target)

	// randSource drives ResetProbability evaluation; overridable in tests for determinism.
	randSource *rand.Rand
}

// NewNetworkDegradationInjector constructs an injector. Passing a nil kernel backend selects
// the safe no-op backend (equivalent to always-dry-run for the kernel seam specifically);
// production wiring must supply a real KernelNetworkFaultApplier.
func NewNetworkDegradationInjector(kernel KernelNetworkFaultApplier) *NetworkDegradationInjector {
	if kernel == nil {
		kernel = noopKernelFaultApplier{}
	}
	return &NetworkDegradationInjector{
		kernel:     kernel,
		active:     make(map[string]*activeDegradation),
		randSource: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func targetKey(t NetworkTarget) string {
	return t.NodeName + "/" + t.PodUID + "/" + t.NetworkInterfaceName
}

// Apply validates profile, installs the degradation via the kernel seam, and schedules an
// automatic revert after profile.Duration (if non-zero). It refuses to double-apply to the
// same target without an explicit Revert first, since stacking netem qdiscs silently produces
// confusing composite behavior.
func (n *NetworkDegradationInjector) Apply(ctx context.Context, target NetworkTarget, profile DegradationProfile) error {
	// Global kill-switch check: this is the entry point for this injector's Start-equivalent
	// action. A tripped breaker refuses every new degradation, independent of everything below.
	if err := safety.Global().Check(ctx); err != nil {
		return fmt.Errorf("apply network degradation: %w", err)
	}

	if err := target.validate(); err != nil {
		return fmt.Errorf("apply network degradation: %w", err)
	}
	if err := profile.Validate(); err != nil {
		return fmt.Errorf("apply network degradation: %w", err)
	}

	key := targetKey(target)

	n.mu.Lock()
	if _, exists := n.active[key]; exists {
		n.mu.Unlock()
		return fmt.Errorf("apply network degradation: target %s already has an active degradation; call Revert first", key)
	}
	n.mu.Unlock()

	if err := n.kernel.ApplyNetem(ctx, target, profile); err != nil {
		return fmt.Errorf("apply network degradation: netem application failed: %w", err)
	}
	if profile.ResetProbability > 0 {
		if err := n.kernel.ApplyTCPResetInjection(ctx, target, profile.ResetProbability); err != nil {
			// Best-effort cleanup of the netem side so we don't leave a half-applied fault.
			_ = n.kernel.RevertNetworkFault(ctx, target)
			return fmt.Errorf("apply network degradation: reset injection failed: %w", err)
		}
	}

	revertCtx, cancel := context.WithCancel(context.Background())
	record := &activeDegradation{target: target, profile: profile, appliedAt: time.Now(), cancel: cancel}
	record.unregisterFromBreaker = safety.Global().RegisterActiveFault(
		"network-degradation:"+key,
		func(rollbackCtx context.Context) error { return n.Revert(rollbackCtx, target) },
	)

	n.mu.Lock()
	n.active[key] = record
	n.mu.Unlock()

	if profile.Duration > 0 {
		go n.autoRevertAfter(revertCtx, target, profile.Duration)
	}
	return nil
}

func (n *NetworkDegradationInjector) autoRevertAfter(ctx context.Context, target NetworkTarget, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		// Best-effort: scheduled revert. Errors are swallowed here because there is no caller
		// left to return them to; a real implementation should route this through the
		// injector's OTLP telemetry/chaos-event emission instead of discarding it.
		_ = n.Revert(context.Background(), target)
	case <-ctx.Done():
		// Revert was already called manually; nothing to do.
	}
}

// Revert removes an active degradation from target, both in the kernel seam and from the
// in-memory active set. Reverting a target with no active degradation is a no-op, not an
// error, so shutdown/reconciliation code can call it unconditionally.
func (n *NetworkDegradationInjector) Revert(ctx context.Context, target NetworkTarget) error {
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
	record.cancel()
	if record.unregisterFromBreaker != nil {
		record.unregisterFromBreaker()
	}

	if err := n.kernel.RevertNetworkFault(ctx, target); err != nil {
		return fmt.Errorf("revert network degradation for %s: %w", key, err)
	}
	return nil
}

// ActiveTargets returns the NetworkTargets that currently have a degradation applied. Used by
// graceful-shutdown and crash-recovery reconciliation to revert every outstanding fault.
func (n *NetworkDegradationInjector) ActiveTargets() []NetworkTarget {
	n.mu.Lock()
	defer n.mu.Unlock()

	targets := make([]NetworkTarget, 0, len(n.active))
	for _, record := range n.active {
		targets = append(targets, record.target)
	}
	return targets
}

// ShouldInjectReset evaluates whether, for a given profile, this tick should inject a TCP
// reset. It is exposed so a real eBPF program's userspace control loop can poll it, or so
// tests can exercise the probability logic deterministically by supplying a seeded injector.
func (n *NetworkDegradationInjector) ShouldInjectReset(profile DegradationProfile) bool {
	if profile.ResetProbability <= 0 {
		return false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.randSource.Float64() < profile.ResetProbability
}
