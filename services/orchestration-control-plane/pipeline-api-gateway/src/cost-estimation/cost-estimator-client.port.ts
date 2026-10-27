import { PipelineDag } from '../test-definition/dag-domain-types';

/** DI token for the active CostEstimatorClientPort implementation. */
export const COST_ESTIMATOR_CLIENT = Symbol('COST_ESTIMATOR_CLIENT');

/** Which cloud provider to price the estimate against — see the sibling
 * run-cost-estimator service's cloud-instance-pricing-catalog.ts for the supported set. */
export type EstimateCloudProvider = 'AWS' | 'GCP';

/** Per-LOAD_TEST-node cost line item, mirrored from run-cost-estimator's NodeCostLineItem. */
export interface RunCostLineItem {
  nodeId: string;
  displayName: string;
  cloudProvider: EstimateCloudProvider;
  instanceType: string;
  workerHours: number;
  onDemandHourlyUsd: number;
  costUsd: number;
}

/** Projected cost/carbon for a pipeline run, as returned by run-cost-estimator's estimateCost(). */
export interface RunCostAndCarbonEstimate {
  pipelineId: string;
  cloudProvider: EstimateCloudProvider;
  totalCostUsd: number;
  totalWorkerHours: number;
  lineItems: RunCostLineItem[];
  estimatedKgCo2e: number;
  estimatedKwh: number;
}

/**
 * Port (hexagonal-architecture boundary) to the sibling `run-cost-estimator`
 * service, following the same shape as `quality-gate`'s
 * `TelemetryQueryApiClientPort` and `pipeline-run`'s
 * `DagWorkflowSchedulerClientPort`: `pipeline-run.service.ts` and any
 * canvas-facing resolver depend only on this interface, so the HTTP
 * adapter backing it can change (or be mocked in tests) without touching
 * call sites.
 */
export interface CostEstimatorClientPort {
  /**
   * Requests a projected cost/carbon estimate for dag before a run is
   * triggered. pipeline-run.service.ts calls this so
   * workflow-canvas-frontend can show projected cost before the user
   * clicks "Run" (surfaced via PipelineRunEntity.estimatedCostUsd /
   * estimatedCarbonKgCo2e — see pipeline-run.entity.ts).
   */
  estimateRunCost(pipelineId: string, dag: PipelineDag, cloudProvider: EstimateCloudProvider): Promise<RunCostAndCarbonEstimate>;
}
