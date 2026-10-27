import type { LoadTestNode, PipelineDefinition } from '@shared-libraries/dag-domain-model';
import {
  CloudProvider,
  InstancePricingEntry,
  WorkerInstanceType,
  lookupInstancePricing,
} from './cloud-instance-pricing-catalog';

/** Per-LoadTestNode line item in a PipelineCostEstimate, so a UI can show which node
 * in the DAG canvas is driving the total (e.g. a 50k-VU load test dwarfing everything else). */
export interface NodeCostLineItem {
  nodeId: string;
  displayName: string;
  cloudProvider: CloudProvider;
  instanceType: WorkerInstanceType;
  /** workerReplicaCount x durationSeconds/3600 — how much compute this node actually consumes. */
  workerHours: number;
  onDemandHourlyUsd: number;
  costUsd: number;
}

export interface PipelineCostEstimate {
  pipelineId: string;
  cloudProvider: CloudProvider;
  totalCostUsd: number;
  totalWorkerHours: number;
  lineItems: NodeCostLineItem[];
}

export interface PipelineCostEstimatorOptions {
  /** Which cloud provider the estimate should be priced against. LoadTestNode itself carries
   * no provider field (see shared-libraries/dag-domain-model/src/types/pipeline-node.ts) since
   * that's an execution-environment concern, not a DAG-authoring one — the caller (here,
   * ultimately pipeline-api-gateway's cost-estimator-client.port.ts) supplies it, typically
   * from the target environment's configured provider. */
  cloudProvider: CloudProvider;
}

/**
 * Estimates the dollar cost of a pipeline run from its LOAD_TEST nodes'
 * worker-hours (workerReplicaCount x durationSeconds) x cloud-instance
 * pricing. Every other PipelineNode kind (API_REQUEST, DATA_SEED,
 * ASSERTION, CHAOS_INJECTION, ENVIRONMENT_TEARDOWN) is treated as
 * effectively free relative to sustained load generation — a reasonable
 * approximation for a *pre-run budgeting signal* (see the carbon estimator
 * NOTE for the same caveat), not a hard accounting guarantee.
 */
export class PipelineCostEstimator {
  estimate(pipelineDefinition: PipelineDefinition, options: PipelineCostEstimatorOptions): PipelineCostEstimate {
    const loadTestNodes = pipelineDefinition.nodes.filter(
      (node): node is LoadTestNode => node.kind === 'LOAD_TEST',
    );

    const lineItems = loadTestNodes.map((node) => this.estimateLoadTestNode(node, options.cloudProvider));

    return {
      pipelineId: pipelineDefinition.pipelineId,
      cloudProvider: options.cloudProvider,
      totalCostUsd: roundToCents(lineItems.reduce((sum, item) => sum + item.costUsd, 0)),
      totalWorkerHours: lineItems.reduce((sum, item) => sum + item.workerHours, 0),
      lineItems,
    };
  }

  private estimateLoadTestNode(node: LoadTestNode, cloudProvider: CloudProvider): NodeCostLineItem {
    const instanceType = instanceTypeForLoadProtocol(node.protocol);
    const pricing: InstancePricingEntry = lookupInstancePricing(cloudProvider, instanceType);

    const workerHours = node.workerReplicaCount * (node.durationSeconds / 3600);
    const costUsd = roundToCents(workerHours * pricing.onDemandHourlyUsd);

    return {
      nodeId: node.nodeId,
      displayName: node.displayName,
      cloudProvider,
      instanceType,
      workerHours,
      onDemandHourlyUsd: pricing.onDemandHourlyUsd,
      costUsd,
    };
  }
}

/**
 * Maps a LoadTestNode's protocol to the worker/instance family that
 * executes it, per ADR 0002's split ("raw TCP/QUIC saturation... dominated
 * by per-connection... overhead" needs protocol-load-worker-rust on
 * compute-optimized instances; REST/gRPC/WebSocket/GraphQL run on
 * protocol-load-worker-go's general-purpose instances).
 */
function instanceTypeForLoadProtocol(protocol: LoadTestNode['protocol']): WorkerInstanceType {
  return protocol === 'TCP' || protocol === 'QUIC' ? 'COMPUTE_OPTIMIZED_LARGE' : 'GENERAL_PURPOSE_MEDIUM';
}

function roundToCents(value: number): number {
  return Math.round(value * 100) / 100;
}
