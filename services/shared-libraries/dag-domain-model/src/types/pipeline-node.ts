/**
 * pipeline-node.ts
 *
 * Canonical discriminated union of every DAG node kind the platform supports. This is the
 * single source of truth — `workflow-canvas-frontend` renders these, `pipeline-api-gateway`
 * persists/validates these, `dag-workflow-scheduler` dispatches these (via
 * `DagNodeDispatchCommand.nodeConfigJson`, see `shared-libraries/protocol-contracts`), and
 * execution-fleet workers execute these. Never redeclare these shapes elsewhere — import them.
 */

export type PipelineNodeKind =
  | "API_REQUEST"
  | "LOAD_TEST"
  | "DATA_SEED"
  | "ASSERTION"
  | "CHAOS_INJECTION"
  | "ENVIRONMENT_TEARDOWN";

/** Fields every node kind shares, regardless of what it does when dispatched. */
export interface PipelineNodeBase {
  nodeId: string;
  /** Human-authored label shown on the workflow-canvas-frontend DAG builder. */
  displayName: string;
  /** Position on the visual canvas; purely presentational, ignored by the scheduler. */
  canvasPosition: { x: number; y: number };
}

export type HttpProtocol = "REST" | "GRPC" | "WEBSOCKET" | "GRAPHQL" | "MQTT";

/** A single functional/API call step — e.g. "POST /orders then assert 201". */
export interface ApiRequestNode extends PipelineNodeBase {
  kind: "API_REQUEST";
  protocol: HttpProtocol;
  targetUrl: string;
  method: string;
  headers: Record<string, string>;
  /** JSON-encoded request body template; may reference upstream node outputs via
   * `{{nodeId.field}}` interpolation, resolved by the worker at dispatch time. */
  bodyTemplateJson?: string;
  timeoutMs: number;
}

export type LoadProfileShape = "CONSTANT" | "RAMP_UP" | "SPIKE" | "STEP";

/** Generates sustained/ramping/spiking traffic against a target, executed by
 * `protocol-load-worker-go` or `protocol-load-worker-rust` depending on `protocol`. */
export interface LoadTestNode extends PipelineNodeBase {
  kind: "LOAD_TEST";
  protocol: HttpProtocol | "TCP" | "QUIC";
  targetUrl: string;
  loadProfile: LoadProfileShape;
  targetRequestsPerSecond: number;
  durationSeconds: number;
  /** Number of concurrent worker replicas to fan this load test across. */
  workerReplicaCount: number;
}

export type DataSeedStrategy = "SYNTHETIC_GENERATION" | "FIXTURE_REPLAY" | "PII_MASKED_SNAPSHOT";

/** Populates or resets test data before downstream nodes run, executed by
 * `test-data-factory-service`. */
export interface DataSeedNode extends PipelineNodeBase {
  kind: "DATA_SEED";
  strategy: DataSeedStrategy;
  targetSchema: string;
  rowCount: number;
  /** Required when strategy === 'FIXTURE_REPLAY'; a stored fixture identifier. */
  fixtureId?: string;
}

export type AssertionOperator =
  | "EQUALS"
  | "LESS_THAN"
  | "GREATER_THAN"
  | "LESS_THAN_OR_EQUAL"
  | "GREATER_THAN_OR_EQUAL"
  | "MATCHES_REGEX";

/** A pass/fail quality gate — e.g. "p99 latency of upstream LOAD_TEST node < 250ms". Powers
 * both functional assertions and A/B canary regression gates (see
 * `telemetry-query-api`'s `AbCanaryDifferentialBenchmarkComparator`). */
export interface AssertionNode extends PipelineNodeBase {
  kind: "ASSERTION";
  /** Node id whose output/telemetry this assertion evaluates. */
  subjectNodeId: string;
  metricPath: string; // e.g. "latency.p99Ms", "errorRate", "canaryComparison.overallVerdict"
  operator: AssertionOperator;
  expectedValue: string | number;
  /** If true, a failed assertion halts the pipeline run; if false, it's recorded but non-blocking. */
  isBlocking: boolean;
}

export type ChaosFaultKind =
  | "NETWORK_LATENCY"
  | "PACKET_LOSS"
  | "CPU_STARVATION"
  | "MEMORY_PRESSURE"
  | "PROCESS_KILL"
  | "DISK_IO_THROTTLE";

/** Triggers `chaos-ebpf-sidecar` to inject a fault against a target for the node's active
 * window. Mirrors `ChaosFaultInjectionEvent` in `redpanda-ingestion-pipeline`. */
export interface ChaosInjectionNode extends PipelineNodeBase {
  kind: "CHAOS_INJECTION";
  faultKind: ChaosFaultKind;
  targetSelector: string; // label selector or pod/container identifier
  parameters: Record<string, number>;
  durationSeconds: number;
}

/** Tears down/deprovisions an ephemeral test environment via
 * `environment-lifecycle-manager` (Terraform/Crossplane). Typically the terminal node in a
 * pipeline that provisions its own environment. */
export interface EnvironmentTeardownNode extends PipelineNodeBase {
  kind: "ENVIRONMENT_TEARDOWN";
  environmentId: string;
  /** If false, teardown failures are logged but do not fail the overall pipeline run — useful
   * so a flaky teardown doesn't mask an otherwise-successful test result. */
  isBlocking: boolean;
}

/** The discriminated union every consumer should switch over on `.kind`. */
export type PipelineNode =
  | ApiRequestNode
  | LoadTestNode
  | DataSeedNode
  | AssertionNode
  | ChaosInjectionNode
  | EnvironmentTeardownNode;
