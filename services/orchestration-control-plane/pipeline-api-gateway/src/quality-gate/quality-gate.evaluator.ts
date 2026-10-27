import { Injectable } from '@nestjs/common';
import {
  QualityGateEvaluationResult,
  QualityGateSlaThresholds,
  QualityGateThresholdResult,
  RunTelemetrySummary,
} from './quality-gate-sla-threshold.types';

/**
 * Pure comparison logic for automated quality gates: given the thresholds
 * configured for a pipeline and the telemetry observed for a run, decides
 * per-field pass/fail and an overall verdict. Contains no I/O, making it
 * trivially unit-testable independent of telemetry-query-api or Postgres.
 */
@Injectable()
export class QualityGateEvaluator {
  evaluate(
    thresholds: QualityGateSlaThresholds,
    telemetrySummary: RunTelemetrySummary,
  ): QualityGateEvaluationResult {
    const thresholdResults: QualityGateThresholdResult[] = [];

    if (thresholds.p99LatencyMs !== undefined) {
      thresholdResults.push({
        field: 'p99LatencyMs',
        thresholdValue: thresholds.p99LatencyMs,
        observedValue: telemetrySummary.p99LatencyMs,
        // Lower is better: fail if observed latency exceeds the threshold.
        passed: telemetrySummary.p99LatencyMs <= thresholds.p99LatencyMs,
      });
    }

    if (thresholds.errorRatePercent !== undefined) {
      thresholdResults.push({
        field: 'errorRatePercent',
        thresholdValue: thresholds.errorRatePercent,
        observedValue: telemetrySummary.errorRatePercent,
        // Lower is better: fail if observed error rate exceeds the threshold.
        passed: telemetrySummary.errorRatePercent <= thresholds.errorRatePercent,
      });
    }

    if (thresholds.minThroughputRps !== undefined) {
      thresholdResults.push({
        field: 'minThroughputRps',
        thresholdValue: thresholds.minThroughputRps,
        observedValue: telemetrySummary.throughputRps,
        // Higher is better: fail if observed throughput falls below the minimum.
        passed: telemetrySummary.throughputRps >= thresholds.minThroughputRps,
      });
    }

    return {
      runId: telemetrySummary.runId,
      passed: thresholdResults.every((result) => result.passed),
      thresholdResults,
    };
  }
}
