/**
 * SLA thresholds configured on a pipeline (or a specific run trigger) that
 * automated quality gates evaluate against telemetry gathered for the run.
 * Any field left undefined is not evaluated (no threshold configured for
 * that dimension).
 */
export interface QualityGateSlaThresholds {
  p99LatencyMs?: number;
  errorRatePercent?: number;
  minThroughputRps?: number;
}

/** Aggregated telemetry for a single pipeline run, as reported by telemetry-query-api. */
export interface RunTelemetrySummary {
  runId: string;
  p99LatencyMs: number;
  errorRatePercent: number;
  throughputRps: number;
}

/** Result of evaluating one threshold field against the observed telemetry. */
export interface QualityGateThresholdResult {
  field: keyof QualityGateSlaThresholds;
  thresholdValue: number;
  observedValue: number;
  passed: boolean;
}

/** Overall result of a quality-gate evaluation for a run. */
export interface QualityGateEvaluationResult {
  runId: string;
  passed: boolean;
  thresholdResults: QualityGateThresholdResult[];
}
