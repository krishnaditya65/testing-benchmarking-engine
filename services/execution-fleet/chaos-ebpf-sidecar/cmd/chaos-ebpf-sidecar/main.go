// Command chaos-ebpf-sidecar runs the privileged, node-local eBPF chaos-engineering agent.
//
// DANGER: this process is designed to degrade networking, kill pods, and skew clocks on
// purpose. It refuses to take any destructive action unless started with
// --i-understand-this-is-destructive (or CHAOS_SIDECAR_ACKNOWLEDGE_DESTRUCTIVE=true); without
// that flag it starts in dry-run mode, where every chaos primitive is wired to its safe no-op
// kernel/K8s backend and only logs what it would have done.
//
// It takes orders from the orchestration control plane over Redpanda's dag.node.dispatch.v1
// topic (see shared-libraries/protocol-contracts/proto/dag_dispatch.proto) and emits chaos
// events / telemetry outward via OTLP to the OpenTelemetry Collector DaemonSet. It never calls
// other execution-fleet workers directly.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/distributed-testing-benchmarking-tool/chaos-ebpf-sidecar/internal/chaos"
	"github.com/distributed-testing-benchmarking-tool/chaos-ebpf-sidecar/internal/ebpf"
)

// config holds every operator-tunable setting for this process, populated from flags with
// environment-variable fallbacks so it runs the same way under `go run` locally and as a
// Kubernetes DaemonSet container (where env vars, not flags, are the idiomatic way to
// configure a container image built once and deployed everywhere).
type config struct {
	// nodeName identifies which Kubernetes node this DaemonSet pod instance owns. Required:
	// every fault-injection record is scoped to this node.
	nodeName string
	// redpandaBrokers is the comma-separated bootstrap broker list for consuming
	// dag.node.dispatch.v1 orders from the control plane.
	redpandaBrokers string
	// otelCollectorEndpoint is the OTLP gRPC endpoint of the node-local OTel Collector
	// DaemonSet, per ARCHITECTURE.md's telemetry contract.
	otelCollectorEndpoint string
	// acknowledgeDestructive gates whether any chaos primitive is wired to a real (as opposed
	// to no-op) backend. This is the single safety interlock for the whole process.
	acknowledgeDestructive bool
	// stateFilePath is where currently-active fault-injection records are persisted, so a
	// crash-and-restart can detect and revert orphaned faults. See SCALING.md.
	stateFilePath string
}

func loadConfigFromFlags() config {
	var cfg config

	flag.StringVar(&cfg.nodeName, "node-name", envOrDefault("NODE_NAME", ""), "Kubernetes node this sidecar instance owns (required)")
	flag.StringVar(&cfg.redpandaBrokers, "redpanda-brokers", envOrDefault("REDPANDA_BROKERS", "localhost:9092"), "comma-separated Redpanda bootstrap brokers")
	flag.StringVar(&cfg.otelCollectorEndpoint, "otel-collector-endpoint", envOrDefault("OTEL_COLLECTOR_ENDPOINT", "localhost:4317"), "OTLP gRPC endpoint of the local OTel Collector")
	flag.BoolVar(&cfg.acknowledgeDestructive, "i-understand-this-is-destructive", envOrDefaultBool("CHAOS_SIDECAR_ACKNOWLEDGE_DESTRUCTIVE", false), "required to enable real (non-dry-run) fault injection; omit to run in safe dry-run mode")
	flag.StringVar(&cfg.stateFilePath, "state-file", envOrDefault("CHAOS_SIDECAR_STATE_FILE", "/var/run/chaos-ebpf-sidecar/active-injections.json"), "path used to persist active-injection records for crash recovery")
	flag.Parse()

	return cfg
}

func envOrDefault(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func envOrDefaultBool(key string, fallback bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	parsed, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return parsed
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := loadConfigFromFlags()

	if cfg.nodeName == "" {
		logger.Error("startup refused: --node-name (or NODE_NAME env var) is required; this sidecar must know which node it owns before it can safely scope any fault")
		os.Exit(1)
	}

	if cfg.acknowledgeDestructive {
		logger.Warn("DESTRUCTIVE MODE ENABLED: this process will apply real network degradation, kill real pods, and skew real clocks on this node",
			"node_name", cfg.nodeName)
	} else {
		logger.Info("starting in DRY-RUN mode: no destructive action will be taken; pass --i-understand-this-is-destructive to enable real fault injection",
			"node_name", cfg.nodeName)
	}

	// NOTE: real wiring replaces these three nil/simulated backends with privileged
	// implementations satisfying chaos.KernelNetworkFaultApplier, chaos.PodKillTarget, and
	// chaos.NodeClockManipulator respectively — see the NOTE comments on each seam in
	// internal/chaos/*.go for exactly what that requires (CAP_NET_ADMIN/CAP_BPF, a real
	// client-go clientset, and a node/pod time-namespace mechanism). Until
	// acknowledgeDestructive is set AND those backends are wired, every chaos primitive here
	// safely no-ops.
	networkInjector := chaos.NewNetworkDegradationInjector(selectKernelNetworkBackend(cfg))
	clockInjector := chaos.NewNodeClockSkewInjector(selectClockBackend(cfg))
	podKiller, err := chaos.NewBlastRadiusControlledPodKiller(selectPodKillBackend(cfg), chaos.BlastRadiusLimit{
		// Conservative defaults for a starter deployment: never kill more than a fifth of the
		// matched population, and never more than 5 pods, in a single action. Real deployments
		// should source these from the DAG dispatch payload / control-plane experiment config
		// rather than hardcoding them here.
		MaxBlastRadiusPercent: 20,
		MaxBlastRadiusCount:   5,
	})
	if err != nil {
		logger.Error("startup refused: failed to construct blast-radius-controlled pod killer", "error", err)
		os.Exit(1)
	}

	programLoader := ebpf.EbpfProgramLoader(ebpf.NewSimulatedProgramLoader())
	// NOTE: production wiring swaps the line above for a constructed
	// ebpf.CiliumEbpfProgramLoader once that type has a real implementation (compiled BPF
	// object files + cilium/ebpf loading, see its doc comment).

	// NOTE: podKiller, programLoader, and clockInjector are wired here so their construction
	// (and the safety checks inside NewBlastRadiusControlledPodKiller) run at startup, but the
	// actual dispatch of work to them happens in the not-yet-implemented Redpanda consumer loop
	// below; reference them so a future consumer can be added without a top-level rewrite.
	logger.Debug("chaos primitives wired",
		"network_injector", networkInjector != nil,
		"clock_injector", clockInjector != nil,
		"pod_killer", podKiller != nil,
		"program_loader", programLoader != nil,
	)

	logger.Info("chaos-ebpf-sidecar started",
		"node_name", cfg.nodeName,
		"redpanda_brokers", cfg.redpandaBrokers,
		"otel_collector_endpoint", cfg.otelCollectorEndpoint,
		"dry_run", !cfg.acknowledgeDestructive,
		"state_file", cfg.stateFilePath,
	)

	// NOTE: production wiring subscribes here to the dag.node.dispatch.v1 Redpanda topic
	// (payload shape: shared-libraries/protocol-contracts/proto/dag_dispatch.proto) and, for
	// each chaos-kind dispatch message addressed to cfg.nodeName, calls into networkInjector /
	// podKiller / clockInjector, persists the resulting active-injection record to
	// cfg.stateFilePath (see SCALING.md's crash-recovery section), and emits an OTLP chaos
	// event confirming the action. That consumer loop is not implemented in this starter repo.

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	<-ctx.Done()
	logger.Info("shutdown signal received; reverting all active fault injections before exit")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	revertAllActiveFaultInjections(shutdownCtx, logger, networkInjector, clockInjector)
	logger.Info("chaos-ebpf-sidecar shutdown complete")
}

// revertAllActiveFaultInjections walks every currently-active network degradation and clock
// skew and reverts it, so a graceful SIGTERM never leaves a node in a permanently degraded
// state. Pod kills are not reverted here since killing a pod is not an undoable action; the
// blast-radius cap is the safety control for that primitive, not shutdown reconciliation.
func revertAllActiveFaultInjections(ctx context.Context, logger *slog.Logger, networkInjector *chaos.NetworkDegradationInjector, clockInjector *chaos.NodeClockSkewInjector) {
	for _, target := range networkInjector.ActiveTargets() {
		if err := networkInjector.Revert(ctx, target); err != nil {
			logger.Error("failed to revert network degradation during shutdown", "target", fmt.Sprintf("%+v", target), "error", err)
			continue
		}
		logger.Info("reverted network degradation during shutdown", "target", fmt.Sprintf("%+v", target))
	}

	for _, target := range clockInjector.ActiveTargets() {
		if err := clockInjector.EndSkew(ctx, target); err != nil {
			logger.Error("failed to revert clock skew during shutdown", "target", fmt.Sprintf("%+v", target), "error", err)
			continue
		}
		logger.Info("reverted clock skew during shutdown", "target", fmt.Sprintf("%+v", target))
	}
}

// selectKernelNetworkBackend returns nil (selecting NetworkDegradationInjector's built-in
// no-op backend) unless cfg explicitly acknowledges destructive mode AND a real backend has
// been wired in below.
//
// NOTE: the `if cfg.acknowledgeDestructive` branch is intentionally left as a NOTE rather than
// a real construction, because no privileged tc/eBPF backend is implemented in this starter
// repo yet (see chaos.KernelNetworkFaultApplier's doc comment for what that requires).
// Wiring it here, once it exists, looks like:
//
//	if cfg.acknowledgeDestructive {
//	    return tcnetem.NewShellBackend() // or similar, from a new privileged-only package
//	}
func selectKernelNetworkBackend(cfg config) chaos.KernelNetworkFaultApplier {
	return nil
}

// selectClockBackend mirrors selectKernelNetworkBackend for chaos.NodeClockManipulator: nil
// until a real node-clock-manipulation backend exists (see that interface's NOTE comment for
// the three viable real implementations and their tradeoffs).
func selectClockBackend(cfg config) chaos.NodeClockManipulator {
	return nil
}

// selectPodKillBackend mirrors the above for chaos.PodKillTarget: nil until this process is
// wired with a real k8s.io/client-go clientset (in-cluster config + RBAC permission to delete
// pods). NewBlastRadiusControlledPodKiller requires a non-nil killer, so in the absence of a
// real backend this returns a killer whose KillPod always fails loudly rather than silently
// pretending to succeed — unlike the network/clock seams, "silently no-op a pod kill" would be
// a misleading signal to whatever called KillMatching expecting it to have taken effect.
func selectPodKillBackend(cfg config) chaos.PodKillTarget {
	return refusingPodKillBackend{}
}

// refusingPodKillBackend is a chaos.PodKillTarget that always refuses, used as the default
// until a real k8s.io/client-go-backed implementation is wired in. This is deliberately louder
// than a no-op: callers of BlastRadiusControlledPodKiller.KillMatching should see an explicit
// "not wired up" error, not silent success, if they invoke it before real client-go wiring
// exists.
type refusingPodKillBackend struct{}

func (refusingPodKillBackend) KillPod(ctx context.Context, pod chaos.PodRef) error {
	return fmt.Errorf("pod kill backend not wired: no k8s.io/client-go clientset configured for this sidecar instance (pod %s/%s)", pod.Namespace, pod.Name)
}
