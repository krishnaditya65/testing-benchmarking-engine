/**
 * ab-canary-differential-benchmark-comparator.ts
 *
 * Computes P50/P99/error-rate deltas between a baseline ("control") API version and a canary
 * version, over the same wall-clock window, so a DAG pipeline's ASSERTION nodes (or a human
 * reviewing a canary rollout) can answer "is the canary meaningfully worse than control" with a
 * number instead of eyeballing two dashboards.
 *
 * Deliberately independent of StatisticalBaselineDeviationDetector: baseline-deviation
 * detection answers "does this version look different from its own history"; this comparator
 * answers "do these two versions, sampled concurrently, differ from each other" — the two
 * questions need different statistics (this one is a paired two-sample comparison, not
 * historical isolation-forest scoring), though both consume the same repository.
 */
import type {
  ClickHouseQueryRepository,
  ErrorRateRollupPoint,
  LatencyPercentile,
  PercentileLatencyPoint,
  TimeWindow,
} from "../repository/click-house-query-repository";

export interface CanaryComparisonRequest {
  serviceName: string;
  controlVersionDagNodeId: string;
  canaryVersionDagNodeId: string;
  window: TimeWindow;
  /** Delta beyond which a metric is flagged as regressed, expressed as a fraction (0.1 = 10%
   * worse than control). Tunable per pipeline via ASSERTION node config. */
  regressionThresholdFraction: number;
}

export interface MetricDelta {
  controlValue: number;
  canaryValue: number;
  /** (canary - control) / control; positive means canary is worse for latency/error metrics. */
  relativeDeltaFraction: number;
  isRegression: boolean;
}

export interface CanaryComparisonResult {
  serviceName: string;
  controlVersionDagNodeId: string;
  canaryVersionDagNodeId: string;
  window: TimeWindow;
  p50: MetricDelta;
  p99: MetricDelta;
  errorRate: MetricDelta;
  /** True if any of p50/p99/errorRate crossed the regression threshold. */
  overallVerdict: "PASS" | "REGRESSED";
}

const COMPARED_PERCENTILES: LatencyPercentile[] = [50, 99];

/**
 * AbCanaryDifferentialBenchmarkComparator
 *
 * Pulls percentile-latency and error-rate rollups for both the control and canary DAG nodes
 * (each an ApiRequestNode/LoadTestNode variant targeting the same logical service but a
 * different deployed version — see `shared-libraries/dag-domain-model`) over the same time
 * window, and computes relative deltas.
 */
export class AbCanaryDifferentialBenchmarkComparator {
  constructor(private readonly repository: ClickHouseQueryRepository) {}

  async compare(request: CanaryComparisonRequest): Promise<CanaryComparisonResult> {
    const [controlLatencies, canaryLatencies, controlErrors, canaryErrors] = await Promise.all([
      this.fetchPercentiles(request.serviceName, request.controlVersionDagNodeId, request.window),
      this.fetchPercentiles(request.serviceName, request.canaryVersionDagNodeId, request.window),
      this.repository.getErrorRateRollup(request.serviceName, request.window, request.controlVersionDagNodeId),
      this.repository.getErrorRateRollup(request.serviceName, request.window, request.canaryVersionDagNodeId),
    ]);

    const p50 = this.buildDelta(
      averageLatency(controlLatencies, 50),
      averageLatency(canaryLatencies, 50),
      request.regressionThresholdFraction,
    );
    const p99 = this.buildDelta(
      averageLatency(controlLatencies, 99),
      averageLatency(canaryLatencies, 99),
      request.regressionThresholdFraction,
    );
    const errorRate = this.buildDelta(
      averageErrorRate(controlErrors),
      averageErrorRate(canaryErrors),
      request.regressionThresholdFraction,
    );

    return {
      serviceName: request.serviceName,
      controlVersionDagNodeId: request.controlVersionDagNodeId,
      canaryVersionDagNodeId: request.canaryVersionDagNodeId,
      window: request.window,
      p50,
      p99,
      errorRate,
      overallVerdict: p50.isRegression || p99.isRegression || errorRate.isRegression ? "REGRESSED" : "PASS",
    };
  }

  private async fetchPercentiles(
    serviceName: string,
    dagNodeId: string,
    window: TimeWindow,
  ): Promise<Map<LatencyPercentile, PercentileLatencyPoint[]>> {
    const entries = await Promise.all(
      COMPARED_PERCENTILES.map(
        async (percentile) =>
          [percentile, await this.repository.getPercentileLatency(serviceName, percentile, window, dagNodeId)] as const,
      ),
    );
    return new Map(entries);
  }

  private buildDelta(controlValue: number, canaryValue: number, thresholdFraction: number): MetricDelta {
    const relativeDeltaFraction = controlValue === 0 ? (canaryValue === 0 ? 0 : 1) : (canaryValue - controlValue) / controlValue;
    return {
      controlValue,
      canaryValue,
      relativeDeltaFraction,
      isRegression: relativeDeltaFraction > thresholdFraction,
    };
  }
}

function averageLatency(byPercentile: Map<LatencyPercentile, PercentileLatencyPoint[]>, percentile: LatencyPercentile): number {
  const points = byPercentile.get(percentile) ?? [];
  if (points.length === 0) return 0;
  return points.reduce((sum, p) => sum + p.latencyMs, 0) / points.length;
}

function averageErrorRate(points: ErrorRateRollupPoint[]): number {
  const totalCount = points.reduce((sum, p) => sum + p.totalCount, 0);
  const errorCount = points.reduce((sum, p) => sum + p.errorCount, 0);
  return totalCount === 0 ? 0 : errorCount / totalCount;
}
