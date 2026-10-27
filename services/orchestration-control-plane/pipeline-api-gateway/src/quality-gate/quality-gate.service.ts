import { Inject, Injectable, Logger } from '@nestjs/common';
import { PipelineRunService } from '../pipeline-run/pipeline-run.service';
import { QualityGateEvaluationResult, QualityGateSlaThresholds } from './quality-gate-sla-threshold.types';
import { QualityGateEvaluator } from './quality-gate.evaluator';
import { TELEMETRY_QUERY_API_CLIENT, TelemetryQueryApiClientPort } from './telemetry-query-api-client.port';

/**
 * Orchestrates the automated-quality-gates feature end to end: fetch
 * telemetry for a completed/completing run from telemetry-query-api,
 * evaluate it against configured SLA thresholds, and fail the run in the
 * control plane if any threshold is violated.
 */
@Injectable()
export class QualityGateService {
  private readonly logger = new Logger(QualityGateService.name);

  constructor(
    @Inject(TELEMETRY_QUERY_API_CLIENT)
    private readonly telemetryQueryApiClient: TelemetryQueryApiClientPort,
    private readonly qualityGateEvaluator: QualityGateEvaluator,
    private readonly pipelineRunService: PipelineRunService,
  ) {}

  /**
   * Evaluates quality gates for `runId` against `thresholds`. If any
   * threshold fails, the run is transitioned to FAILED via
   * PipelineRunService (which also republishes the status over the live
   * subscription feed).
   */
  async evaluateAndEnforce(
    runId: string,
    thresholds: QualityGateSlaThresholds,
  ): Promise<QualityGateEvaluationResult> {
    const telemetrySummary = await this.telemetryQueryApiClient.fetchRunTelemetrySummary(runId);
    const evaluationResult = this.qualityGateEvaluator.evaluate(thresholds, telemetrySummary);

    if (!evaluationResult.passed) {
      const failedFields = evaluationResult.thresholdResults
        .filter((result) => !result.passed)
        .map((result) => `${result.field} (observed ${result.observedValue}, threshold ${result.thresholdValue})`)
        .join(', ');

      this.logger.warn(`Quality gate failed for run ${runId}: ${failedFields}`);
      await this.pipelineRunService.markRunFailed(runId, `Quality gate SLA violation: ${failedFields}`);
    }

    return evaluationResult;
  }
}
