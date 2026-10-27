/**
 * telemetry-query-controller.ts
 *
 * HTTP surface for the telemetry-query-api. Thin by design — request parsing/validation only,
 * all real logic lives in ClickHouseQueryRepository / the analytics classes.
 */
import { Controller, Get, Query } from "@nestjs/common";
import {
  ClickHouseQueryRepository,
  type LatencyPercentile,
} from "../repository/click-house-query-repository";
import { AbCanaryDifferentialBenchmarkComparator } from "../analytics/ab-canary-differential-benchmark-comparator";
import {
  StatisticalBaselineDeviationDetector,
  type BaselineFeatureVector,
} from "../analytics/statistical-baseline-deviation-detector";

@Controller("telemetry")
export class TelemetryQueryController {
  constructor(
    private readonly repository: ClickHouseQueryRepository,
    private readonly canaryComparator: AbCanaryDifferentialBenchmarkComparator,
  ) {}

  @Get("latency-percentile")
  async getPercentileLatency(
    @Query("service") service: string,
    @Query("percentile") percentile: string,
    @Query("windowStart") windowStart: string,
    @Query("windowEnd") windowEnd: string,
    @Query("dagNodeId") dagNodeId?: string,
  ) {
    return this.repository.getPercentileLatency(
      service,
      Number(percentile) as LatencyPercentile,
      { start: new Date(windowStart), end: new Date(windowEnd) },
      dagNodeId,
    );
  }

  @Get("error-rate")
  async getErrorRateRollup(
    @Query("service") service: string,
    @Query("windowStart") windowStart: string,
    @Query("windowEnd") windowEnd: string,
    @Query("dagNodeId") dagNodeId?: string,
  ) {
    return this.repository.getErrorRateRollup(
      service,
      { start: new Date(windowStart), end: new Date(windowEnd) },
      dagNodeId,
    );
  }

  @Get("trace-correlation")
  async getCrossServiceTraceCorrelation(@Query("traceId") traceId: string) {
    return this.repository.getCrossServiceTraceCorrelation(traceId);
  }

  @Get("canary-comparison")
  async compareCanary(
    @Query("service") service: string,
    @Query("controlDagNodeId") controlVersionDagNodeId: string,
    @Query("canaryDagNodeId") canaryVersionDagNodeId: string,
    @Query("windowStart") windowStart: string,
    @Query("windowEnd") windowEnd: string,
    @Query("regressionThreshold") regressionThreshold?: string,
  ) {
    return this.canaryComparator.compare({
      serviceName: service,
      controlVersionDagNodeId,
      canaryVersionDagNodeId,
      window: { start: new Date(windowStart), end: new Date(windowEnd) },
      regressionThresholdFraction: regressionThreshold ? Number(regressionThreshold) : 0.1,
    });
  }

  @Get("baseline-deviation")
  async getBaselineDeviation(
    @Query("service") service: string,
    @Query("baselineStart") baselineStart: string,
    @Query("baselineEnd") baselineEnd: string,
    @Query("evaluationStart") evaluationStart: string,
    @Query("evaluationEnd") evaluationEnd: string,
  ) {
    const detector = new StatisticalBaselineDeviationDetector();

    const [baselineLatency, baselineErrors] = await Promise.all([
      this.repository.getLatencySeriesForBaseline(service, {
        start: new Date(baselineStart),
        end: new Date(baselineEnd),
      }),
      this.repository.getErrorRateRollup(service, { start: new Date(baselineStart), end: new Date(baselineEnd) }),
    ]);
    const baselineFeatures = mergeFeatureVectors(baselineLatency, baselineErrors);
    detector.fit(baselineFeatures);

    const [evalLatency, evalErrors] = await Promise.all([
      this.repository.getLatencySeriesForBaseline(service, {
        start: new Date(evaluationStart),
        end: new Date(evaluationEnd),
      }),
      this.repository.getErrorRateRollup(service, { start: new Date(evaluationStart), end: new Date(evaluationEnd) }),
    ]);
    const evalFeatures = mergeFeatureVectors(evalLatency, evalErrors);

    return detector.score(evalFeatures);
  }
}

function mergeFeatureVectors(
  latency: Array<{ bucketMinute: Date; p50: number; p99: number }>,
  errors: Array<{ bucketMinute: Date; errorRate: number }>,
): BaselineFeatureVector[] {
  const errorByBucket = new Map(errors.map((e) => [e.bucketMinute.toISOString(), e.errorRate]));
  return latency.map((l) => ({
    bucketMinute: l.bucketMinute,
    p50LatencyMs: l.p50,
    p99LatencyMs: l.p99,
    errorRate: errorByBucket.get(l.bucketMinute.toISOString()) ?? 0,
  }));
}
