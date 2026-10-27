/**
 * topic-definitions.ts
 *
 * Canonical schema + partition-key documentation for every Redpanda topic this platform
 * produces to or consumes from. This is documentation-as-code: the interfaces here are the
 * wire contract until services generate/consume the equivalent Protobuf messages in
 * `services/shared-libraries/protocol-contracts/proto/`. Where a topic has a .proto
 * counterpart, the TS interface below MUST stay field-for-field compatible with it — CI does
 * not currently enforce this automatically, so treat any drift as a bug.
 *
 * Topics defined here:
 *  - telemetry.raw.v1              (OTel Collector -> ClickHouse Kafka-engine table)
 *  - dag.node.dispatch.v1          (dag-workflow-scheduler -> execution-fleet workers)
 *  - chaos.fault-injection-event.v1 (chaos-ebpf-sidecar -> telemetry lake + scheduler)
 *  - telemetry.backpressure.v1     (backpressure-aware-producer -> dag-workflow-scheduler)
 */
import type { CircuitBreakerState } from "./backpressure-aware-producer";

/** Every topic name used across the platform, kept as a const enum so producers/consumers
 * can never typo a topic string. */
export const RedpandaTopic = {
  TELEMETRY_RAW_V1: "telemetry.raw.v1",
  DAG_NODE_DISPATCH_V1: "dag.node.dispatch.v1",
  CHAOS_FAULT_INJECTION_EVENT_V1: "chaos.fault-injection-event.v1",
  TELEMETRY_BACKPRESSURE_V1: "telemetry.backpressure.v1",
} as const;

export type RedpandaTopicName = (typeof RedpandaTopic)[keyof typeof RedpandaTopic];

// ---------------------------------------------------------------------------------------------
// telemetry.raw.v1
// ---------------------------------------------------------------------------------------------

/** The three OTel signal kinds multiplexed onto one topic (see otel-collector-config for why
 * they share a topic but use distinct Kafka exporters). */
export type TelemetrySignalKind = "trace" | "metric" | "log";

/**
 * TelemetryRawEvent
 *
 * Envelope for a single OTLP-derived record published to `telemetry.raw.v1` by the OTel
 * Collector's kafka exporters. `payload` carries the OTLP protobuf bytes as base64 in this
 * TS-documented form; the real wire format is `otlp_proto` binary (see collector-config.yaml
 * `encoding: otlp_proto`) — this interface documents the logical shape ClickHouse's
 * Kafka-engine ingestion table (002_kafka_engine_ingestion_table.sql) expects after its
 * JSONEachRow-compatible projection.
 */
export interface TelemetryRawEvent {
  /** ULID assigned at collector export time; primary de-dup key downstream. */
  eventId: string;
  signalKind: TelemetrySignalKind;
  /** Service that originated the signal, e.g. "protocol-load-worker-go". */
  serviceName: string;
  /** W3C trace-context trace id, hex-encoded. Absent for some log records. */
  traceId?: string;
  spanId?: string;
  /** Pipeline run this telemetry belongs to, correlates back to dag-workflow-scheduler state. */
  pipelineRunId: string;
  dagNodeId?: string;
  /** Unix epoch nanoseconds — matches ClickHouse DateTime64(9) columns. */
  timestampUnixNano: string;
  /** For traces: span duration; for metrics representing latency, the observed value. */
  durationMs?: number;
  /** OTel status code string ("OK" | "ERROR" | "UNSET") when signalKind === "trace". */
  statusCode?: "OK" | "ERROR" | "UNSET";
  /** Free-form OTel resource + span/log attributes, flattened to string values for
   * ClickHouse Map(String, String) ingestion. */
  attributes: Record<string, string>;
  /** Raw OTLP protobuf payload, base64-encoded. */
  payload: string;
}

// ---------------------------------------------------------------------------------------------
// dag.node.dispatch.v1
// ---------------------------------------------------------------------------------------------

export type DagNodeKind =
  | "API_REQUEST"
  | "LOAD_TEST"
  | "DATA_SEED"
  | "ASSERTION"
  | "CHAOS_INJECTION"
  | "ENVIRONMENT_TEARDOWN";

/**
 * DagNodeDispatchCommand
 *
 * Published by `dag-workflow-scheduler` (control plane) and consumed by whichever
 * execution-fleet worker pool owns `nodeKind`. This is the TS mirror of
 * `protocol-contracts/proto/dag_dispatch.proto`'s `DagNodeDispatchCommand` message — see that
 * proto file for the authoritative field numbering used on the wire.
 */
export interface DagNodeDispatchCommand {
  pipelineRunId: string;
  dagNodeId: string;
  nodeKind: DagNodeKind;
  /** Node ids that must have reached a terminal state before this dispatch was issued. */
  upstreamNodeIds: string[];
  /** Opaque, node-kind-specific configuration (request spec, load profile, fault spec, ...);
   * decoded against the matching PipelineNode variant from `shared-libraries/dag-domain-model`. */
  nodeConfigJson: string;
  /** Monotonically increasing per pipeline-run, used for idempotent worker-side dedup. */
  dispatchSequence: number;
  dispatchedAtUnixNano: string;
  /** Soft deadline; workers should abandon and report timeout past this. */
  deadlineUnixNano: string;
}

// ---------------------------------------------------------------------------------------------
// chaos.fault-injection-event.v1
// ---------------------------------------------------------------------------------------------

export type ChaosFaultKind =
  | "NETWORK_LATENCY"
  | "PACKET_LOSS"
  | "CPU_STARVATION"
  | "MEMORY_PRESSURE"
  | "PROCESS_KILL"
  | "DISK_IO_THROTTLE";

export type ChaosFaultLifecycleState = "SCHEDULED" | "ACTIVE" | "RECOVERED" | "ABORTED";

/**
 * ChaosFaultInjectionEvent
 *
 * Published by `chaos-ebpf-sidecar` any time a fault's lifecycle state changes. Consumed both
 * by the telemetry lake (to correlate latency/error spikes against a fault's active window in
 * `AbCanaryDifferentialBenchmarkComparator`) and by `dag-workflow-scheduler` (to know when a
 * CHAOS_INJECTION dag node can be marked complete).
 */
export interface ChaosFaultInjectionEvent {
  pipelineRunId: string;
  dagNodeId: string;
  faultId: string;
  faultKind: ChaosFaultKind;
  lifecycleState: ChaosFaultLifecycleState;
  /** Kernel-level target: pod/container/interface the eBPF program attached to. */
  targetIdentifier: string;
  /** Fault-kind-specific magnitude, e.g. { "latencyMs": 250 } or { "lossPercentage": 5 }. */
  parameters: Record<string, number>;
  occurredAtUnixNano: string;
}

// ---------------------------------------------------------------------------------------------
// telemetry.backpressure.v1
// ---------------------------------------------------------------------------------------------

/**
 * TelemetryBackpressureSignal
 *
 * Published by `BackpressureSignalPublisher` (backpressure-signal-publisher.ts) whenever the
 * `BackpressureAwareProducer` it observes changes circuit-breaker state, and consumed by
 * `dag-workflow-scheduler`'s `TelemetryBackpressureThrottle`
 * (`internal/dispatch/telemetry_backpressure_throttle.go`) so DAG node dispatch actually slows
 * down while the telemetry lake is shedding load, instead of the circuit breaker only ever
 * protecting the telemetry lake in isolation while the control plane keeps dispatching new
 * LOAD_STEP nodes that generate proportionally more telemetry. See both services' READMEs for
 * the full signal path (producer trips breaker -> publishes this signal -> scheduler throttles).
 *
 * Field shape is intentionally close to `CircuitBreakerState` and
 * `BackpressureAwareProducerOptions` in `backpressure-aware-producer.ts` — this is a thin
 * broadcast of that producer's own internal state, not a new domain concept.
 */
export interface TelemetryBackpressureSignal {
  /** ULID assigned at publish time, primarily for log/trace correlation across the two services. */
  signalId: string;
  /** The service that owns the tripped producer, e.g. "redpanda-ingestion-pipeline". Consumers
   * with more than one upstream backpressure source can use this to attribute the signal. */
  sourceService: string;
  /** Mirrors BackpressureAwareProducer.getCircuitState() at signal-publish time. */
  circuitState: CircuitBreakerState;
  /** Snapshot of the producer's in-flight queue depth when the signal was published — gives the
   * scheduler-side consumer a magnitude, not just a boolean, for logging/alerting. */
  queuedMessageCount: number;
  /** Snapshot of BackpressureAwareProducer's consecutive-failure counter at signal time. */
  consecutiveFailureCount: number;
  /** How long (milliseconds) the publisher expects its own circuit to remain OPEN
   * (BackpressureAwareProducerOptions.openStateCooldownMs at signal time). Advisory: lets a
   * consumer self-clear its throttle after roughly this long even if it misses the follow-up
   * CLOSED signal (e.g. due to a consumer-group rebalance), so a dropped message degrades to
   * "throttle eventually lifts" rather than "throttle stuck forever". */
  recommendedThrottleMs: number;
  occurredAtUnixNano: string;
}

// ---------------------------------------------------------------------------------------------
// Partition-key strategy
// ---------------------------------------------------------------------------------------------
//
// Every producer in this pipeline (see backpressure-aware-producer.ts) MUST key messages as
// documented below. Getting this wrong silently breaks ordering guarantees consumers rely on.
//
// telemetry.raw.v1
//   Key: `${pipelineRunId}:${dagNodeId ?? "unscoped"}`
//   Why: co-locates every span/metric/log for one DAG node's execution on the same partition,
//   so ClickHouse's Kafka-engine consumer (and any downstream trace-reassembly job) sees them
//   in produce order without cross-partition merge-sorting. Keying by traceId alone would be
//   more evenly distributed but would scatter a pipeline run's telemetry across partitions,
//   making "give me everything for this run" queries fan out unnecessarily.
//
// dag.node.dispatch.v1
//   Key: `${pipelineRunId}`
//   Why: all dispatch commands for one pipeline run must be strictly ordered relative to each
//   other (a downstream node must never be observed by a worker before its upstream nodes'
//   dispatch), so they share a partition. Cross-run parallelism still scales because distinct
//   pipeline runs land on distinct partitions.
//
// chaos.fault-injection-event.v1
//   Key: `${pipelineRunId}:${faultId}`
//   Why: a single fault's SCHEDULED -> ACTIVE -> RECOVERED/ABORTED lifecycle must be observed
//   in order by any consumer reconstructing "was this fault active at time T"; keying at
//   fault granularity (rather than just pipelineRunId) spreads load better across partitions
//   when many faults run concurrently within one chaos scenario.
//
// telemetry.backpressure.v1
//   Key: `${sourceService}`
//   Why: this is a low-volume, state-transition signal (published on circuit-breaker state
//   changes, not per-message), so throughput-driven partition spreading doesn't matter. Keying
//   by sourceService instead guarantees that one producer's own sequence of state transitions
//   (e.g. CLOSED -> OPEN -> HALF_OPEN -> CLOSED) is observed in order by the scheduler-side
//   consumer, which matters because TelemetryBackpressureThrottle trusts the most recent signal
//   it has seen — an out-of-order OPEN arriving after a CLOSED would incorrectly re-throttle
//   dispatch.
