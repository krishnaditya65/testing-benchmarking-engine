/**
 * canary-differential-report-client.port.ts
 *
 * Hexagonal port for fetching an A/B canary comparison report from the sibling
 * `telemetry-lake/telemetry-query-api` service's `AbCanaryDifferentialBenchmarkComparator`
 * (`src/analytics/ab-canary-differential-benchmark-comparator.ts`). Same pattern as
 * `pipeline-api-gateway`'s `TelemetryQueryApiClientPort` in `src/quality-gate/telemetry-query-api-client.port.ts`
 * — a thin, typed HTTP client behind an interface so `canary-rollout-decision-engine.ts` depends
 * on this port, never on `axios`/fetch details directly.
 *
 * The request/response shapes below are copies of `telemetry-query-api`'s
 * `CanaryComparisonRequest`/`CanaryComparisonResult`/`MetricDelta`, kept field-for-field
 * identical deliberately (this service reads that file before defining these — see this
 * package's README). They are necessarily a separate declaration, not an import: the two
 * services are independently deployable processes communicating over HTTP, so there is no
 * shared-language type boundary to import across (unlike `@shared-libraries/dag-domain-model`,
 * which both `pipeline-api-gateway` and `workflow-canvas-frontend` link into the same process
 * boundary). If `telemetry-query-api`'s comparator output shape changes, this port's types must
 * be updated to match — that drift risk is the trade-off of an HTTP boundary instead of a
 * shared TS package, and is exactly the kind of contract change `contract-healing-service`
 * (this service's sibling under `services/ai-assisted-qa/`) exists to catch on the pipeline
 * data-extraction side; this port is hand-synced instead since it's service-to-service, not
 * pipeline-to-backend.
 */

export interface CanaryComparisonTimeWindow {
  start: string; // ISO-8601, over the wire (telemetry-query-api's TimeWindow uses Date objects in-process)
  end: string;
}

export interface CanaryComparisonRequest {
  serviceName: string;
  controlVersionDagNodeId: string;
  canaryVersionDagNodeId: string;
  window: CanaryComparisonTimeWindow;
  regressionThresholdFraction: number;
}

export interface CanaryMetricDelta {
  controlValue: number;
  canaryValue: number;
  relativeDeltaFraction: number;
  isRegression: boolean;
}

export interface CanaryComparisonResult {
  serviceName: string;
  controlVersionDagNodeId: string;
  canaryVersionDagNodeId: string;
  window: CanaryComparisonTimeWindow;
  p50: CanaryMetricDelta;
  p99: CanaryMetricDelta;
  errorRate: CanaryMetricDelta;
  overallVerdict: "PASS" | "REGRESSED";
}

/**
 * CanaryDifferentialReportClientPort
 *
 * `canary-rollout-decision-engine.ts`'s sole dependency for fetching comparison data — never
 * called directly by anything above the decision engine, so the HTTP boundary stays in one
 * place.
 */
export interface CanaryDifferentialReportClientPort {
  fetchComparisonReport(request: CanaryComparisonRequest): Promise<CanaryComparisonResult>;
}
