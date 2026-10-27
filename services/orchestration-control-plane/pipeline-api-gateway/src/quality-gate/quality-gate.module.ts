import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { PipelineRunModule } from '../pipeline-run/pipeline-run.module';
import { QualityGateEvaluator } from './quality-gate.evaluator';
import { QualityGateService } from './quality-gate.service';
import { TELEMETRY_QUERY_API_CLIENT } from './telemetry-query-api-client.port';
import { TelemetryQueryApiHttpClientAdapter } from './telemetry-query-api-http-client.adapter';

/**
 * Wires the automated quality-gates feature: telemetry-query-api client,
 * pure evaluator, and the orchestrating service that can fail a run via
 * PipelineRunModule's exported PipelineRunService.
 */
@Module({
  imports: [ConfigModule, PipelineRunModule],
  providers: [
    QualityGateEvaluator,
    QualityGateService,
    { provide: TELEMETRY_QUERY_API_CLIENT, useClass: TelemetryQueryApiHttpClientAdapter },
  ],
  exports: [QualityGateService, QualityGateEvaluator],
})
export class QualityGateModule {}
