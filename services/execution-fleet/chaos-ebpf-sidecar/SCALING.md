# Scaling & failure characteristics

`chaos-ebpf-sidecar` has a fundamentally different scaling model from the rest of the
execution fleet, because its job requires privileged, node-local kernel access rather than
horizontally-partitionable request/traffic volume.

## Deployment model: DaemonSet, not replica count

`protocol-load-worker-go`, `protocol-load-worker-rust`, and `dag-ipld-benchmark-worker-rust`
are stateless Deployments scaled by Kubernetes HPA against CPU/queue-depth — more replicas
means more load-generation throughput, and any replica can pick up any dispatched work.

`chaos-ebpf-sidecar` cannot work that way: a `tc netem` qdisc, an attached eBPF TC program, and
a node clock offset are properties of *one specific node's kernel*, not a fungible unit of
compute. This service is deployed as a **Kubernetes DaemonSet** — exactly one pod per node in
scope for chaos experiments — so that:

- Every node has local `KernelNetworkFaultApplier`/`NodeClockManipulator` access without a
  network hop to reach the right node.
- "Scaling" this service means adding more nodes to the DaemonSet's node selector, not
  increasing a replica count against the same nodes.
- A control-plane-issued chaos action targeting node X is routed (via the `dag.node.dispatch.v1`
  payload's node-selection field) to the one sidecar instance actually running on node X.

## State held

Unlike the stateless workers, this service holds real in-memory state that matters for
correctness, not just performance:

- `NetworkDegradationInjector.active` — every network target currently under an applied
  `DegradationProfile`, keyed by `targetKey(target)` (`NodeName/PodUID/NetworkInterfaceName`).
- `NodeClockSkewInjector.active` — every target currently under a ramped `SkewSpec`, plus the
  wall-clock instant the ramp started (`activeSkew.startedAt`), from which the current
  effective offset is *recomputed* on every read rather than cached — the ramp math must
  survive the injector's own restart re-deriving from persisted state (see below), not just an
  in-process timer.

Both `ActiveTargets()` accessors exist specifically so shutdown and reconciliation code (in
`cmd/chaos-ebpf-sidecar/main.go`) can enumerate "everything I currently have applied" without a
separate source of truth.

## Crash-mid-test recovery

If this process dies uncleanly (OOM-killed, node kernel panic, `kubectl delete pod
--force`) while a fault is active, the in-memory `active` maps above are lost — but the actual
kernel-level qdisc, eBPF attachment, or clock offset **is not**, because those live in the
kernel/K8s API, not in this process's memory. An unclean crash therefore risks leaving an
*orphaned* fault: a degraded network or skewed clock with no live process tracking it, active
until someone notices.

The reconciliation approach for a real (non-starter) deployment:

1. **Persist on apply, not just in memory.** Every successful `NetworkDegradationInjector.Apply`
   / `NodeClockSkewInjector.BeginSkew` call must also append/update a record in the local state
   file (`--state-file`, default `/var/run/chaos-ebpf-sidecar/active-injections.json`, backed by
   a `hostPath` or `emptyDir` volume that survives container restart but not node loss) *before*
   returning success to the caller — mirroring the applied-target key, profile/spec, and
   applied-at timestamp already held in memory.
2. **On startup, before accepting any new dispatch**, read the state file. For every record
   found: assume it is still active in the kernel (crashes happen mid-apply, never mid-clean-up),
   and immediately call the corresponding `Revert`/`EndSkew` through the real privileged backend,
   then remove the record. This makes "was there an orphaned fault" moot — every restart reverts
   everything it find on disk, which is always safe (reverting an already-clean target is a
   no-op both in this codebase's `Revert`/`EndSkew` methods and in real `tc`/clock tooling).
3. **Reconcile against the control plane as a second check.** Because the state file itself
   could be lost (node disk pressure evicting the volume, a fresh node), the sidecar should also
   report its currently-active-injection set back to the orchestration control plane on an OTLP
   chaos-event heartbeat; the control plane, which durably tracks which chaos actions it
   dispatched to which node (in Temporal, per `dag-workflow-scheduler`), becomes a second source
   of truth a sidecar can reconcile against if its local state file is unavailable.
4. **On clean shutdown** (the SIGTERM path already implemented in `main.go`), revert everything
   *and* clear the state file, so a normal rolling restart of the DaemonSet doesn't trigger
   step 2's revert-everything-found-on-disk logic unnecessarily.

This starter repo implements the in-memory `active` maps and the graceful-shutdown revert path;
the state-file persistence and startup reconciliation described above are the next piece of
real implementation work before this service is safe to run against a real cluster.

## Global chaos circuit breaker: blast-radius-of-last-resort

Every safety mechanism described above (blast-radius caps, graceful-shutdown revert,
crash-mid-test recovery) operates *per experiment* or *per process lifecycle event*. None of
them answer "stop literally everything this sidecar instance is doing, right now, because of an
incident this sidecar cannot itself observe from its own experiment data." That is what
`internal/safety.GlobalChaosCircuitBreaker` exists for, and it changes this service's scaling
and failure-domain story in one specific way worth calling out explicitly:

- **It is process-local, matching the DaemonSet-per-node deployment model above.** `safety.Global()`
  is a singleton *within one sidecar process* (one node), not a cluster-wide breaker — tripping
  the breaker on node A's sidecar instance does not, by itself, stop node B's. A cluster-wide
  kill switch is achieved by every sidecar instance's Redpanda consumer subscribing to the same
  `chaos.kill-switch.v1` topic and independently calling `Trip` on its own local
  `safety.Global()` instance when a kill-switch message arrives — the fan-out is Redpanda's job
  (every DaemonSet pod in the same consumer group... more precisely, every pod as its *own*
  consumer group, since every node's sidecar must independently see and act on the same
  message), not this package's.
- **It is the correct blast-radius-of-last-resort precisely because it does not depend on any
  per-experiment logic being correct.** `BlastRadiusLimit` caps can still be misconfigured or
  bypassed by a bug in a caller; `HypothesisGuardedExperimentRunner`'s auto-abort depends on
  `telemetry-query-api` being reachable and the configured `SteadyStateHypothesis` actually
  being the right health signal for the target. The global circuit breaker depends on neither —
  it is a flat "no" enforced at the same three entry points every fault must pass through
  regardless of what experiment, hypothesis, or targeting strategy requested them.
- **Tripping it is O(active faults on this node), not O(cluster size).** `Trip` iterates only
  the faults registered via `RegisterActiveFault` on the local instance — a bounded, small set
  (this service's blast-radius caps already keep any one experiment's active fault count small;
  see "Resource profile" below) — so a trip resolves in the time it takes to call
  `Revert`/`EndSkew` on each one, not a cluster-wide fan-out wait.
- **Resetting it is a deliberately manual, per-node action.** There is no timer-based or
  health-check-based automatic reset (see `GlobalChaosCircuitBreaker.Reset`'s doc comment) — an
  operator (or an equally deliberate automated incident-response system) must explicitly decide
  each affected node's sidecar is safe to resume, mirroring the deliberateness already required
  to *start* destructive mode via `--i-understand-this-is-destructive`.

## Resource profile

- **Mostly idle.** Between dispatched chaos actions, this process does essentially nothing:
  no polling loops beyond the Redpanda consumer, no busy-waiting. CPU/memory footprint at rest
  should be a small, flat baseline (goroutine scheduler, gRPC/OTLP client idle connections).
- **Hypothesis polling adds one bounded goroutine per in-flight guarded experiment.** Each
  `HypothesisGuardedExperimentRunner.RunGuarded` call blocks its caller's goroutine for at most
  `experimentDuration`, issuing one `MetricQueryPort.QueryMetric` HTTP round-trip to
  `telemetry-query-api` per `pollInterval` tick — negligible relative to the kernel-level work
  the wrapped injector itself does, and bounded by the same "chaos experiments don't overlap
  within one sidecar instance" rule `ChaosExperimentScheduler.Tick` enforces by running due
  experiments sequentially rather than concurrently.
- **`ServiceDependencyGraphClient.FetchDependencyGraph` is a bursty, off-hot-path cost.**
  Building a dependency graph issues one HTTP request per seed trace ID to
  `telemetry-query-api`; callers should fetch it once per chaos-experiment dispatch (or on a
  slow refresh cadence) and reuse the resulting `ServiceDependencyGraph` across
  `DependencyAwarePodSelectionStrategy.SelectCandidates` calls, not refetch it per pod-kill
  decision.
- **Spikes during injection application.** Attaching an eBPF program, running `tc qdisc`
  commands, or issuing pod-delete calls are all short, bursty operations — brief CPU spikes,
  not sustained load.
- **Must respect strict resource limits itself.** Because this pod runs privileged and
  node-local, alongside real workloads on the same node, it must set conservative Kubernetes
  `resources.limits` (CPU and memory) so a bug in the sidecar (e.g. a runaway reconciliation
  loop) cannot itself starve the real workloads it is supposed to be testing against. A
  privileged process consuming unbounded node resources is its own outage, independent of any
  chaos it was asked to inject.
