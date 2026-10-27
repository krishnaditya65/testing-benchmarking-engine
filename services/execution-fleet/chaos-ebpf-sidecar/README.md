# chaos-ebpf-sidecar

Privileged, node-local Go agent for eBPF-based chaos engineering inside target Kubernetes
clusters. Part of the **Execution Fleet** (`services/execution-fleet/`) per
[ARCHITECTURE.md](../../../ARCHITECTURE.md): it takes orders from the orchestration control
plane over Redpanda's `dag.node.dispatch.v1` topic and emits telemetry/chaos events outward via
OTLP to the OpenTelemetry Collector DaemonSet. **It never calls other execution-fleet workers
directly** — no synchronous RPC to peers, matching the platform-wide worker isolation rule.

## This service is privileged and dangerous

Every other execution-fleet worker (`protocol-load-worker-go`, `protocol-load-worker-rust`,
`dag-ipld-benchmark-worker-rust`, `wasm-payload-sandbox-runtime`) is a stateless process that
only generates traffic or runs sandboxed user code — losing one costs nothing but a retried
test. `chaos-ebpf-sidecar` is different: it runs with elevated Linux capabilities
(`CAP_NET_ADMIN`, `CAP_BPF`, and depending on the clock-skew mechanism chosen,
`CAP_SYS_TIME`/`CAP_SYS_ADMIN`) and a Kubernetes RBAC grant to delete pods, and it exists
specifically to make real workloads misbehave on purpose (degraded networking, killed pods,
skewed clocks). A bug here does not fail a test — it can take down real infrastructure. Treat
every exported method on the types in `internal/chaos/` as load-bearing and dangerous.

### Safety posture

- **Dry-run by default.** `cmd/chaos-ebpf-sidecar/main.go` refuses to wire any chaos primitive
  to a real backend unless started with `--i-understand-this-is-destructive` (or
  `CHAOS_SIDECAR_ACKNOWLEDGE_DESTRUCTIVE=true`). Without it, every primitive runs against its
  safe no-op/simulated backend and only logs intent.
- **Blast-radius caps enforced before action.** `BlastRadiusControlledPodKiller` computes the
  safe kill set and refuses the *entire* request — killing nothing — if it would exceed the
  configured `MaxBlastRadiusPercent` / `MaxBlastRadiusCount`. The cap is evaluated before any
  `KillPod` call is issued, not after.
- **Graceful revert on shutdown.** `main.go`'s SIGTERM/SIGINT handler walks every currently
  active network degradation and clock skew and reverts it before the process exits, so a
  routine pod restart of the sidecar itself never leaves a node permanently degraded. Pod kills
  are not "reverted" (a killed pod cannot be un-killed) — the blast-radius cap is the control
  for that primitive, not shutdown cleanup.
- **Crash-mid-test recovery.** See [SCALING.md](./SCALING.md) for how this service reconciles
  and reverts orphaned fault injections after an unclean restart.
- **Steady-state hypothesis auto-abort.** `internal/hypothesis.HypothesisGuardedExperimentRunner`
  wraps any injector so it verifies a `SteadyStateHypothesis` holds before injecting, keeps
  polling it during injection, and automatically aborts and rolls back the fault the instant the
  hypothesis is violated — see "Hypothesis-guarded chaos scheduling" below.
- **Global kill switch.** `internal/safety.GlobalChaosCircuitBreaker` is checked at the entry
  point of every injector's Start-equivalent method; once tripped, it blocks all new injection
  and rolls back every active fault in this sidecar instance, independent of and in addition to
  the blast-radius cap and the hypothesis auto-abort above — see "Global chaos circuit breaker"
  below and [SCALING.md](./SCALING.md).

## Feature manifest → code map

| Feature-manifest bullet | Implemented in | Notes |
|---|---|---|
| "eBPF kernel-level latency capture" | *(not this service)* | This service focuses on **fault injection**, not passive latency measurement. Kernel-level latency capture lives in `protocol-load-worker-rust`'s `ebpf_latency_probe` (see `services/execution-fleet/protocol-load-worker-rust/src/telemetry/mod.rs`); `internal/ebpf/ebpf_program_loader.go` here provides the shared attach/detach/stats lifecycle contract that a future latency-capture eBPF program in either service could reuse. |
| "network degradation simulation (latency/jitter/packet loss/TCP resets)" | `internal/chaos/network_degradation_injector.go` | `NetworkDegradationInjector.Apply` validates and schedules a `DegradationProfile` (latency, jitter, packet-loss %, TCP-reset probability), modeled on `tc netem` parameters. The actual kernel call is behind the `KernelNetworkFaultApplier` seam — see its doc comment for the real `tc`/eBPF TC-hook wiring required. |
| "blast-radius controlled fault injection (kill pods, spike CPU/memory)" | `internal/chaos/blast_radius_controlled_pod_killer.go` | `BlastRadiusControlledPodKiller` implements the pod-kill half: label-selector scoping (`LabelSelector.Matches`) plus a hard cap enforced before any kill (`PlanKillSet` / `KillMatching`). **CPU/memory spike injection is not implemented in this starter repo** — it is a documented near-term extension point: a `CPUMemorySpikeInjector` would follow the same shape as `NetworkDegradationInjector` (a validated spec + a privileged seam, likely a container-level `cgroup` CPU-quota/memory-limit tightening or a stress-process exec'd inside the target pod's namespace via a debug-container equivalent). |
| "time-travel/clock-skew testing" | `internal/chaos/node_clock_skew_injector.go` | `NodeClockSkewInjector` computes and ramps a `SkewSpec{OffsetDuration, RampDuration}` and exposes `EffectiveSimulatedTime` for a given real-time instant, for testing Raft/Paxos consensus timeout behavior and JWT/TLS expiry logic under clock disagreement. The actual node-clock manipulation is behind the `NodeClockManipulator` seam — see its doc comment for the three viable real mechanisms and their isolation tradeoffs. |
| "automated chaos experiment scheduling with steady-state hypothesis + auto-abort" | `internal/hypothesis/` | `SteadyStateHypothesis` (`steady_state_hypothesis.go`) declares a measurable health statement (metric query + threshold + comparison) evaluated through the `MetricQueryPort` seam. `HypothesisGuardedExperimentRunner` (`hypothesis_guarded_experiment_runner.go`) wraps any existing injector — via `NetworkDegradationFaultInjection`, `ClockSkewFaultInjection`, `PodKillFaultInjection` adapters — verifying the hypothesis before injecting, polling it during injection, and auto-aborting/rolling back on violation. `ChaosExperimentSchedule` (`chaos_experiment_schedule.go`) lets an experiment be scheduled one-shot or on a recurring cadence via a caller-driven `ChaosExperimentScheduler.Tick`, instead of only manually triggered. See ADR [0007](../../../docs/architecture-decision-records/0007-hypothesis-guarded-chaos-and-global-kill-switch.md). |
| "dependency-aware fault targeting" | `internal/targeting/` | `ServiceDependencyGraphClient` (`service_dependency_graph_client.go`) fetches a `ServiceDependencyGraph` derived from `telemetry-query-api`'s cross-service-trace-correlation read (a stub `HTTPServiceDependencyGraphClient` adapter is provided). `DependencyAwarePodSelectionStrategy` (`dependency_aware_pod_selector.go`) implements the new `chaos.PodSelectionStrategy` interface, restricting eligible pods to services with at least `MinDirectDependents` distinct upstream callers, so a blast-radius-capped pod kill guarantees it exercises a real failover path instead of an arbitrary label match. Wired into `BlastRadiusControlledPodKiller` as an optional strategy alongside the pre-existing (and still default) `LabelSelectorPodSelectionStrategy`. |
| "global chaos circuit breaker / kill switch" | `internal/safety/global_chaos_circuit_breaker.go` | `GlobalChaosCircuitBreaker` — see "Global chaos circuit breaker" below and [SCALING.md](./SCALING.md). |

## Package layout

```
cmd/chaos-ebpf-sidecar/main.go        entrypoint: config, safety gating, wiring, graceful shutdown
internal/chaos/
  network_degradation_injector.go     latency/jitter/packet-loss/TCP-reset scheduling + Apply/Revert
  blast_radius_controlled_pod_killer.go  label-selector scoped, blast-radius-capped pod kills
  node_clock_skew_injector.go         ramped clock-offset scheduling + effective-time queries
internal/ebpf/
  ebpf_program_loader.go              Load/Attach/Detach/Stats lifecycle contract + SimulatedProgramLoader
internal/hypothesis/
  steady_state_hypothesis.go              SteadyStateHypothesis + MetricQueryPort seam
  hypothesis_guarded_experiment_runner.go FaultInjection adapters + guarded verify/poll/auto-abort runner
  chaos_experiment_schedule.go            one-shot/recurring ChaosExperimentSchedule + ChaosExperimentScheduler
internal/targeting/
  service_dependency_graph_client.go  ServiceDependencyGraph model + ServiceDependencyGraphClient seam
  dependency_aware_pod_selector.go    DependencyAwarePodSelectionStrategy (chaos.PodSelectionStrategy)
internal/safety/
  global_chaos_circuit_breaker.go     GlobalChaosCircuitBreaker — process-wide kill switch
```

## Hypothesis-guarded chaos scheduling

`internal/hypothesis` answers "was it actually safe to run this experiment, the whole time it
ran?" — not just "did the blast-radius cap allow it to start?". A `SteadyStateHypothesis` is a
declarative health statement (metric query, threshold, comparison operator — e.g.
"checkout-service p99 stays under 300ms"), evaluated through the `MetricQueryPort` interface so
this package never depends on `telemetry-query-api`'s transport directly (a stub HTTP adapter,
`HTTPTelemetryQueryAPIMetricQueryPort`, is provided as the intended real-wiring point).

`HypothesisGuardedExperimentRunner.RunGuarded` owns the full experiment lifecycle: it verifies
the hypothesis before calling the wrapped injector's start method, polls the hypothesis on a
fixed interval while the fault is active, and — the moment the hypothesis is violated, or
becomes unevaluable (treated the same as a violation, fail-safe) — aborts and rolls back the
fault automatically, using a context detached from the caller's so a canceled caller can never
block cleanup. `ChaosExperimentSchedule` then lets a hypothesis-guarded experiment be scheduled
(one-shot at a specific time, or recurring on an interval with an optional weekday/business-hours
eligibility window) via a caller-driven `ChaosExperimentScheduler.Tick`, rather than only ever
being triggered manually.

## Dependency-aware fault targeting

`internal/targeting` answers "which pod should we actually kill to test a real failover path?"
`ServiceDependencyGraphClient` derives a `ServiceDependencyGraph` from `telemetry-query-api`'s
cross-service-trace-correlation read — parent/child spans that cross a service boundary within a
correlated trace are direct evidence of a caller→callee edge. `DependencyAwarePodSelectionStrategy`
implements `chaos.PodSelectionStrategy` (see below) against that graph, restricting eligible pods
to services with at least `MinDirectDependents` distinct upstream callers.

`blast_radius_controlled_pod_killer.go` now defines `PodSelectionStrategy` as the seam between
"which candidates are eligible" and the blast-radius cap enforced afterward.
`LabelSelectorPodSelectionStrategy` reproduces the original label-selector-only behavior exactly
and remains the default — `PlanKillSet`/`KillMatching`'s pre-existing signatures and behavior are
unchanged. `PlanKillSetWithStrategy`/`KillMatchingWithStrategy` are the generalized entry points
either strategy funnels through.

## Global chaos circuit breaker

`internal/safety.GlobalChaosCircuitBreaker` is a distinct, incident-response-grade safety layer
from the per-experiment blast-radius cap and hypothesis auto-abort above: it is checked at the
entry point of every injector's Start-equivalent method (`NetworkDegradationInjector.Apply`,
`NodeClockSkewInjector.BeginSkew`, `BlastRadiusControlledPodKiller.KillMatchingWithStrategy`),
modeled as reacting to a Redpanda `chaos.kill-switch.v1` topic message (a separate,
out-of-band topic from the control plane's normal `dag.node.dispatch.v1` dispatch). Tripping it
immediately blocks all new injection and rolls back **every** active fault this sidecar instance
is running, and only an explicit `Reset` re-enables injection — there is no automatic reset. See
[SCALING.md](./SCALING.md) for why this is the platform's blast-radius-of-last-resort, and ADR
[0007](../../../docs/architecture-decision-records/0007-hypothesis-guarded-chaos-and-global-kill-switch.md)
for the full design rationale behind all three additions in this section.

Every privileged kernel/Kubernetes call is behind a narrow interface (`KernelNetworkFaultApplier`,
`PodKillTarget`, `NodeClockManipulator`, `EbpfProgramLoader`) with a `// NOTE:` doc comment on
the interface explaining exactly what real, privileged wiring it needs. None of those real
backends are implemented in this starter repo — only safe no-op/simulated defaults are, so the
service is safe to `go run` locally without a live cluster or root.

## Local development

```bash
# Dry-run (default): logs intent, touches nothing.
go run ./cmd/chaos-ebpf-sidecar --node-name=local-dev-node

# Would enable real fault injection IF a privileged backend were wired in (see main.go's
# selectKernelNetworkBackend / selectClockBackend / selectPodKillBackend for the wiring points):
go run ./cmd/chaos-ebpf-sidecar --node-name=local-dev-node --i-understand-this-is-destructive
```

`go.mod` lists real dependency lines (`github.com/cilium/ebpf`, `k8s.io/client-go`, the OTel
SDK/OTLP exporter, gRPC) for the privileged wiring described above, but this repo has no
network access to run `go mod tidy`/`go build` against them — that must be done once real
backend implementations are added.
