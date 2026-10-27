import { Injectable, Logger, ServiceUnavailableException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import axios, { AxiosInstance } from 'axios';
import { PipelineDag, PipelineNode } from '../test-definition/dag-domain-types';
import {
  CostEstimatorClientPort,
  EstimateCloudProvider,
  RunCostAndCarbonEstimate,
} from './cost-estimator-client.port';

/**
 * Real HTTP adapter to the sibling `run-cost-estimator` service, following
 * the same shape as `quality-gate`'s `TelemetryQueryApiHttpClientAdapter`:
 * calls `POST /cost-estimate` with a PipelineDefinition-shaped body and
 * returns the parsed RunCostAndCarbonEstimate.
 */
@Injectable()
export class CostEstimatorHttpClientAdapter implements CostEstimatorClientPort {
  private readonly logger = new Logger(CostEstimatorHttpClientAdapter.name);
  private readonly httpClient: AxiosInstance;

  constructor(private readonly configService: ConfigService) {
    this.httpClient = axios.create({
      baseURL: this.configService.get<string>('RUN_COST_ESTIMATOR_BASE_URL', 'http://run-cost-estimator:8095'),
      timeout: this.configService.get<number>('RUN_COST_ESTIMATOR_TIMEOUT_MS', 5000),
    });
  }

  async estimateRunCost(
    pipelineId: string,
    dag: PipelineDag,
    cloudProvider: EstimateCloudProvider,
  ): Promise<RunCostAndCarbonEstimate> {
    try {
      const response = await this.httpClient.post<RunCostAndCarbonEstimate>('/cost-estimate', {
        pipelineDefinition: toRunCostEstimatorPipelineDefinition(pipelineId, dag),
        cloudProvider,
      });
      return response.data;
    } catch (error) {
      this.logger.error(
        `run-cost-estimator unavailable or rejected estimate request for pipeline ${pipelineId}: ${this.describeError(error)}`,
      );
      throw new ServiceUnavailableException(
        `run-cost-estimator is unavailable: ${this.describeError(error)}`,
      );
    }
  }

  private describeError(error: unknown): string {
    if (axios.isAxiosError(error)) {
      return `${error.response?.status ?? 'no-response'} ${JSON.stringify(error.response?.data ?? error.message)}`;
    }
    return error instanceof Error ? error.message : String(error);
  }
}

/**
 * Adapts this gateway's local, loosely-typed PipelineDag (see
 * dag-domain-types.ts's header comment on why it's redeclared locally
 * rather than importing `@shared-libraries/dag-domain-model` directly) into
 * the shape run-cost-estimator's `estimateCost` expects: a `kind`-discriminated
 * node union with LOAD_TEST-specific fields (`workerReplicaCount`,
 * `durationSeconds`, `protocol`) at the top level rather than nested inside
 * a generic `config` bag. Those fields are expected to already be present
 * inside a `LoadTest`-step node's `config` (the canvas writes them there
 * when authoring a LoadTest step), so this is a reshape, not a data
 * fabrication — but it does mean a malformed LoadTest node's config
 * silently produces a $0 line item on the run-cost-estimator side rather
 * than a validation error here; a stricter version of this adapter would
 * validate config shape per stepType before sending.
 */
function toRunCostEstimatorPipelineDefinition(pipelineId: string, dag: PipelineDag) {
  return {
    pipelineId,
    name: pipelineId,
    version: 0,
    nodes: dag.nodes.map((node) => toRunCostEstimatorNode(node)),
    edges: dag.edges.map((edge) => ({ fromNodeId: edge.fromNodeId, toNodeId: edge.toNodeId })),
    createdAtIso: new Date().toISOString(),
    updatedAtIso: new Date().toISOString(),
    ownerId: 'unknown',
  };
}

function toRunCostEstimatorNode(node: PipelineNode) {
  if (node.stepType !== 'LoadTest') {
    // run-cost-estimator only prices LOAD_TEST nodes; every other kind is passed through
    // with a kind it will simply filter out, rather than omitted, so line-item ordering
    // in a future richer estimate stays aligned with the DAG's own node ordering.
    return { nodeId: node.id, displayName: node.name, kind: 'API_REQUEST' as const };
  }

  return {
    nodeId: node.id,
    displayName: node.name,
    kind: 'LOAD_TEST' as const,
    protocol: node.config.protocol,
    targetUrl: node.config.targetUrl,
    loadProfile: node.config.loadProfile,
    targetRequestsPerSecond: node.config.targetRequestsPerSecond,
    durationSeconds: node.config.durationSeconds,
    workerReplicaCount: node.config.workerReplicaCount,
  };
}
