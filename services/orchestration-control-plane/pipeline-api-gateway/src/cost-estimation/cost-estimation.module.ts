import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { COST_ESTIMATOR_CLIENT } from './cost-estimator-client.port';
import { CostEstimatorHttpClientAdapter } from './cost-estimator-http-client.adapter';

/**
 * Wires the CostEstimatorClientPort implementation (an HTTP adapter to the
 * sibling `run-cost-estimator` service). Consumed by
 * pipeline-run.module.ts so PipelineRunService can fetch a projected
 * cost/carbon estimate before triggering a run.
 */
@Module({
  imports: [ConfigModule],
  providers: [{ provide: COST_ESTIMATOR_CLIENT, useClass: CostEstimatorHttpClientAdapter }],
  exports: [COST_ESTIMATOR_CLIENT],
})
export class CostEstimationModule {}
