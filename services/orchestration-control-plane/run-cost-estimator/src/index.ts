import type { PipelineDefinition } from '@shared-libraries/dag-domain-model';
import { CloudProvider, lookupInstancePricing } from './estimation/cloud-instance-pricing-catalog';
import { estimateCarbonFootprint } from './estimation/carbon-footprint-estimator';
import { NodeCostLineItem, PipelineCostEstimate, PipelineCostEstimator } from './estimation/pipeline-cost-estimator';

export type { CloudProvider, InstancePricingEntry, WorkerInstanceType } from './estimation/cloud-instance-pricing-catalog';
export { CLOUD_INSTANCE_PRICING_CATALOG, lookupInstancePricing } from './estimation/cloud-instance-pricing-catalog';
export type { CarbonFootprintEstimate } from './estimation/carbon-footprint-estimator';
export { estimateCarbonFootprint } from './estimation/carbon-footprint-estimator';
export type { NodeCostLineItem, PipelineCostEstimate, PipelineCostEstimatorOptions } from './estimation/pipeline-cost-estimator';
export { PipelineCostEstimator } from './estimation/pipeline-cost-estimator';

/** Full estimateCost() response: dollar cost plus its accompanying rough carbon estimate,
 * mirrored per-node so a UI (or pipeline-api-gateway's cost-estimator-client.port.ts caller)
 * can render either a total or a per-node breakdown. */
export interface RunCostAndCarbonEstimate extends PipelineCostEstimate {
  estimatedKgCo2e: number;
  estimatedKwh: number;
}

export interface EstimateCostOptions {
  cloudProvider: CloudProvider;
}

/**
 * estimateCost is the single entry point this service exists to expose:
 * given a PipelineDefinition, return its projected dollar cost and rough
 * CO2 footprint. This is what pipeline-api-gateway's
 * `src/cost-estimation/cost-estimator-client.port.ts` calls over HTTP (via
 * `src/http/cost-estimate-http-server.ts` in this service) so
 * workflow-canvas-frontend can show a projected cost before the user clicks
 * "Run" — see pipeline-run.service.ts's triggerRun call site.
 */
export function estimateCost(
  pipelineDefinition: PipelineDefinition,
  options: EstimateCostOptions = { cloudProvider: 'AWS' },
): RunCostAndCarbonEstimate {
  const costEstimator = new PipelineCostEstimator();
  const costEstimate = costEstimator.estimate(pipelineDefinition, { cloudProvider: options.cloudProvider });

  const totalCarbon = sumCarbonAcrossLineItems(costEstimate.lineItems, options.cloudProvider);

  return {
    ...costEstimate,
    estimatedKgCo2e: totalCarbon.estimatedKgCo2e,
    estimatedKwh: totalCarbon.estimatedKwh,
  };
}

/** Carbon is estimated per line item (since different nodes may run on different instance
 * types with different vCPU counts) and summed, rather than approximated off a single
 * blended average. */
function sumCarbonAcrossLineItems(
  lineItems: NodeCostLineItem[],
  cloudProvider: CloudProvider,
): { estimatedKgCo2e: number; estimatedKwh: number } {
  let estimatedKgCo2e = 0;
  let estimatedKwh = 0;

  for (const lineItem of lineItems) {
    const pricing = lookupInstancePricing(cloudProvider, lineItem.instanceType);
    const carbon = estimateCarbonFootprint({
      totalWorkerHours: lineItem.workerHours,
      vCpuCountPerWorker: pricing.vCpuCount,
    });
    estimatedKgCo2e += carbon.estimatedKgCo2e;
    estimatedKwh += carbon.estimatedKwh;
  }

  return {
    estimatedKgCo2e: Math.round(estimatedKgCo2e * 1000) / 1000,
    estimatedKwh: Math.round(estimatedKwh * 1000) / 1000,
  };
}
