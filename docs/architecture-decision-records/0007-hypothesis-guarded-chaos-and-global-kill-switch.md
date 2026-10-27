# 0007. Hypothesis-guarded chaos experiments and a global kill switch

- Status: Accepted
- Date: 2026-08-11
- Owners: chaos-ebpf-sidecar maintainers

## Context

`chaos-ebpf-sidecar`'s existing safety model, before this ADR, is entirely *local* to one
experiment and evaluated only *before* an action is taken:
`BlastRadiusControlledPodKiller` refuses to kill more than a capped percentage/count of pods,
`NetworkDegradationInjector.Apply` and `NodeClockSkewInjector.BeginSkew` validate their
parameters, and every fault carries either an explicit `Duration` or an explicit `Revert`/
`EndSkew` call. None of that answers two operational questions the platform needs answered as
it runs real chaos experiments against real services:

1. **"Is the target actually healthy, both before and *while* we're breaking it?"** A capped
   blast radius still permits killing/degrading pods of a service that was already unhealthy
   before the experiment started, or that becomes unhealthy for reasons unrelated to the
   injected fault partway through — nothing today re-checks target health *during* an
   in-progress, duration-bound degradation and reacts to it. Chaos-engineering practice calls
   this a "steady-state hypothesis": define what healthy looks like, verify it holds before
   perturbing the system, and abort if it stops holding.
2. **"How do we stop everything, right now, for a reason this sidecar cannot itself observe?"**
   A real production incident — whether or not it was caused by the chaos experiment running at
   the time — needs a stop mechanism that is faster and blunter than "wait for each active
   fault's `Duration` to elapse" or "an operator finds and calls `Revert` on each active target
   individually across however many sidecar instances are running." Today there is no
   single action that halts every active fault this *one sidecar instance* is running and
   refuses new ones, independent of any per-experiment logic.

Both gaps also block two features on the manifest that depend on them: automated (rather than
purely manually-triggered) chaos experiment scheduling only makes sense to ship once experiments
can safely run unattended (requiring #1), and dependency-aware fault targeting (choosing WHICH
pod to kill based on real upstream/downstream trace-derived dependency edges, rather than a
static label selector alone) is a targeting-precision improvement that is independent of, but
shipped alongside, both safety mechanisms in this same pass.

## Decision

Add two new, independent safety layers on top of (not replacing) the existing blast-radius caps
and dry-run gating, plus dependency-aware targeting as a third, unrelated improvement:

### 1. Steady-state hypothesis + auto-abort (`internal/hypothesis`)

`SteadyStateHypothesis` (`steady_state_hypothesis.go`) is a declarative, measurable health
statement — a metric query, a threshold, and a comparison operator (e.g. "checkout-service p99
stays under 300ms") — evaluated through a `MetricQueryPort` interface so this package never
depends on `telemetry-query-api`'s transport directly (a stub HTTP adapter is provided;
`internal/targeting`'s dependency-graph client follows the identical seam-decoupling pattern).

`HypothesisGuardedExperimentRunner` (`hypothesis_guarded_experiment_runner.go`) wraps any
existing chaos primitive — via small `FaultInjection` adapters for
`NetworkDegradationInjector`, `NodeClockSkewInjector`, and
`BlastRadiusControlledPodKiller` — so that it (a) verifies the hypothesis holds *before*
injecting, refusing to start otherwise; (b) polls the hypothesis metric on a fixed interval
*during* injection; and (c) the moment the hypothesis is violated (or becomes unevaluable — a
fail-safe treated the same as a violation), automatically aborts and rolls back the fault,
detached from the caller's own context so a canceled caller can never prevent cleanup.

`ChaosExperimentSchedule` (`chaos_experiment_schedule.go`) lets a hypothesis-guarded experiment
be scheduled — one-shot at a specific time, or recurring on an interval with an optional
weekday/hour eligibility window — rather than only manually triggered, via a caller-driven
`ChaosExperimentScheduler.Tick` (no internal goroutine/ticker, matching this service's existing
pattern of leaving the real periodic/consumer driver as an explicit wiring point in `main.go`).

### 2. Global chaos circuit breaker (`internal/safety`)

`GlobalChaosCircuitBreaker` is a process-wide, injectable-singleton-style safety mechanism
(`safety.Global()`, swappable in tests via `ReplaceGlobal`) that every existing injector's
Start-equivalent entry point (`NetworkDegradationInjector.Apply`,
`NodeClockSkewInjector.BeginSkew`, `BlastRadiusControlledPodKiller.KillMatchingWithStrategy`)
checks first, before doing anything else. Tripping it (`Trip`, modeled as the handler for a
Redpanda `chaos.kill-switch.v1` topic message — a separate, out-of-band topic from the control
plane's normal `dag.node.dispatch.v1` dispatch, per `ARCHITECTURE.md`'s "Communication
contracts") immediately blocks all new injection and rolls back every fault currently
registered via `RegisterActiveFault` — every revertible fault registers its rollback closure at
apply-time and deregisters it at normal revert-time. There is no automatic reset; only an
explicit `Reset` call re-enables injection.

This is deliberately independent of (and additive to) `HypothesisGuardedExperimentRunner`'s
auto-abort: a hypothesis violation means one experiment's own target looks unhealthy; a
kill-switch trip means an operator decided this whole sidecar instance should stop, for a
reason the sidecar cannot itself observe from any single experiment's metrics.

### 3. Dependency-aware fault targeting (`internal/targeting`)

`ServiceDependencyGraphClient` fetches a `ServiceDependencyGraph` derived from
`telemetry-query-api`'s cross-service-trace-correlation read (correlating parent/child spans
across a sample of trace IDs into caller→callee edges), through the same port/stub-adapter
pattern as `MetricQueryPort`. `DependencyAwarePodSelectionStrategy` implements a new
`chaos.PodSelectionStrategy` interface, added to `blast_radius_controlled_pod_killer.go` with
the pre-existing label-selector behavior kept as the default implementation
(`LabelSelectorPodSelectionStrategy`), so `BlastRadiusControlledPodKiller`'s original
`PlanKillSet`/`KillMatching` signatures and behavior are unchanged. The dependency-aware
strategy restricts eligible pods to services with at least `MinDirectDependents` distinct
upstream callers, guaranteeing a pod kill exercises a real failover path instead of an
arbitrary label match.

## Consequences

**Positive:**

- Chaos experiments can now run unattended (scheduled) without trading away safety — the
  auto-abort mechanism is what makes "unattended" acceptable for a service this dangerous (see
  chaos-ebpf-sidecar/README.md's "This service is privileged and dangerous").
- The global circuit breaker gives incident response a single, sidecar-instance-wide stop action
  that is strictly faster and more certain than waiting out per-fault durations or hunting down
  every active target — documented in SCALING.md as the platform's blast-radius-of-last-resort.
- Dependency-aware targeting makes blast-radius-capped pod kills more *meaningful*, not just
  safer: a capped kill of pods nothing depends on tests nothing useful.
- All three additions are purely additive — every existing exported method's signature and
  documented behavior (`Apply`, `BeginSkew`, `PlanKillSet`, `KillMatching`) is unchanged; the
  new safety checks are inserted at existing entry points, not new required parameters.

**Negative / trade-offs:**

- Two independent auto-stop mechanisms (per-experiment hypothesis auto-abort, global kill
  switch) plus the pre-existing blast-radius cap means three places that can refuse or halt an
  action for three different reasons — an operator debugging "why didn't my experiment run"
  needs to check all three, not one.
- `HypothesisGuardedExperimentRunner` and `ServiceDependencyGraphClient` both depend on
  `telemetry-query-api` being reachable and healthy; a hypothesis-guarded or dependency-aware
  experiment cannot run at all during a `telemetry-query-api` outage, even though the
  now-unobservable target might otherwise be perfectly safe to test (this package's fail-safe
  bias treats "cannot verify" as "assume violated," which is the deliberately conservative
  choice, but it does mean a telemetry outage silently blocks chaos testing rather than only
  blocking dashboards).
- `ChaosExperimentSchedule`'s recurring-schedule support is a minimal interval-plus-eligibility-
  window model, not a full cron-expression parser (no new dependency could be added in this
  offline environment — see chaos-ebpf-sidecar/go.mod's NOTE) — sufficiently expressive for the
  common "every N minutes, business-hours only" case, but not for arbitrary cron syntax.
- The global circuit breaker's real Redpanda `chaos.kill-switch.v1` consumer, and
  `HTTPServiceDependencyGraphClient`/`HTTPTelemetryQueryAPIMetricQueryPort`'s exact request/
  response contracts against `telemetry-query-api`, remain documented seams rather than wired
  I/O in this pass, consistent with every other privileged/external boundary in this service
  (`KernelNetworkFaultApplier`, `NodeClockManipulator`, `PodKillTarget`).
