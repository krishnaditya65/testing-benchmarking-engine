# redpanda-ingestion-pipeline

Topic and message schema definitions, plus a backpressure-aware producer, for every Redpanda
topic in the telemetry-lake ingestion path and the `dag.node.dispatch.v1` control-plane bus.

## Implements

- Feature manifest: telemetry ingestion pipeline schema/backpressure requirements — "ingestion
  sheds load gracefully during a 10M-RPS spike rather than collapsing ClickHouse."
- ARCHITECTURE.md communication contracts for `dag.node.dispatch.v1` and `telemetry.raw.v1`.

## Contents

- `src/topic-definitions.ts` — the four topics this platform uses (`telemetry.raw.v1`,
  `dag.node.dispatch.v1`, `chaos.fault-injection-event.v1`, `telemetry.backpressure.v1`), their
  TypeScript message interfaces (`TelemetryRawEvent`, `DagNodeDispatchCommand`,
  `ChaosFaultInjectionEvent`, `TelemetryBackpressureSignal`), and the partition-key strategy for
  each — read the comments at the bottom of that file before writing a new producer.
- `src/backpressure-aware-producer.ts` — `BackpressureAwareProducer`, a circuit-breaker +
  bounded-queue wrapper around a kafkajs-style producer.
- `src/backpressure-signal-publisher.ts` — `BackpressureSignalPublisher`, which observes a
  `BackpressureAwareProducer`'s circuit-breaker transitions and publishes them onto
  `telemetry.backpressure.v1` — see "Upstream backpressure feedback loop" below.

## Upstream backpressure feedback loop

`BackpressureAwareProducer` sheds load locally once its circuit breaker trips, but that only
ever protects this ingestion path in isolation — nothing told `dag-workflow-scheduler` (the
component actually deciding how much new load-test traffic, and therefore how much new
telemetry, to generate next) that the telemetry lake was under pressure. `BackpressureSignalPublisher`
closes that loop:

```
BackpressureAwareProducer trips its circuit breaker (telemetry lake under pressure)
  -> BackpressureSignalPublisher publishes telemetry.backpressure.v1
    -> TelemetryBackpressureThrottle (dag-workflow-scheduler, internal/dispatch/telemetry_backpressure_throttle.go)
       observes the signal
      -> the scheduler's dispatch loop pauses/slows new node dispatch until CLOSED
```

Wire it in by passing `onCircuitStateChange` to the observed `BackpressureAwareProducer`:

```ts
const signalPublisher = new BackpressureSignalPublisher(signalKafkaProducer, {
  sourceService: "redpanda-ingestion-pipeline",
});
await signalPublisher.connect();

let observedProducer!: BackpressureAwareProducer;
observedProducer = new BackpressureAwareProducer(rawKafkaProducer, {
  onCircuitStateChange: (previousState, nextState) =>
    signalPublisher.publishStateChange(observedProducer, previousState, nextState).catch(() => {
      /* see backpressure-signal-publisher.ts's doc comment on error handling */
    }),
});
```

Deliberately published on a *separate* Kafka producer from the one being observed, so the signal
reporting the outage is never itself at risk of being shed by that same outage. See ADR
[0011](../../../docs/architecture-decision-records/0011-cross-cutting-robustness-hardening.md)
for the full rationale.

## Why a circuit breaker in front of Kafka, not just Kafka's own retry/backoff

kafkajs's own retry logic assumes transient failures resolve quickly and will happily keep an
unbounded number of promises in flight while retrying. That is fine at normal load. Under a
10M-RPS load-test spike, the correct behavior is different: once Redpanda or the downstream
ClickHouse Kafka-engine consumer group is falling behind, we want to **shed** new telemetry at
the producer boundary — cheaply and immediately — rather than let producers pile up in-memory
retries until the process OOMs or exerts backpressure that stalls the load generation the
platform exists to run.

`BackpressureAwareProducer` adds two independent shedding mechanisms on top of the wrapped
producer:

1. **Bounded in-flight queue** (`maxQueuedMessages`) — sends are shed immediately once the
   number of in-flight messages exceeds this cap, regardless of circuit state.
2. **Circuit breaker** (`CLOSED -> OPEN -> HALF_OPEN -> CLOSED`) — after `failureThreshold`
   consecutive failed/timed-out sends, the breaker trips OPEN and every send is shed for
   `openStateCooldownMs` without even attempting the network call. After the cooldown, a
   `HALF_OPEN` probe determines whether to fully re-close or re-open.

Every `send()` / `sendBatch()` call returns a `SendOutcome` discriminated union
(`SENT | SHED_QUEUE_FULL | SHED_CIRCUIT_OPEN | FAILED`) instead of throwing, so hot-path
callers (the OTel Collector's exporter shim, or a worker's direct telemetry emitter) can make a
local decision — drop, locally re-sample, or log-and-continue — without exception-handling
overhead on the common path.

## Partition-key strategy summary

| Topic | Key | Reason |
|---|---|---|
| `telemetry.raw.v1` | `pipelineRunId:dagNodeId` | keeps one DAG node's full telemetry ordered on one partition |
| `dag.node.dispatch.v1` | `pipelineRunId` | preserves dispatch ordering within a run |
| `chaos.fault-injection-event.v1` | `pipelineRunId:faultId` | preserves one fault's lifecycle ordering while spreading load across concurrent faults |

Full rationale for each is documented as comments in `src/topic-definitions.ts`.

## Relationship to protocol-contracts

`DagNodeDispatchCommand` here is the TypeScript mirror of
`shared-libraries/protocol-contracts/proto/dag_dispatch.proto`. The proto file is the
authoritative wire contract (used by Go/Rust consumers); this interface exists so TypeScript
producers/consumers (control plane, telemetry-query-api) get compile-time safety without a
protoc step in every dev loop. Keep the two in sync — see that package's README for the
compatibility-review process required before changing either.
