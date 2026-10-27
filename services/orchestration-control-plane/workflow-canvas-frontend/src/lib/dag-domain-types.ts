// NOTE: In the full monorepo these types are imported from
// @distributed-testing/dag-domain-model (services/shared-libraries/dag-domain-model)
// instead of being redefined here. That package does not exist yet, so this
// service defines its own local copies to stay unblocked.

import type { Node, Edge } from 'reactflow';

import type { ApiRequestNodeData } from '@/features/dag-canvas/node-types/ApiRequestNode';
import type { LoadTestNodeData } from '@/features/dag-canvas/node-types/LoadTestNode';
import type { DataSeedNodeData } from '@/features/dag-canvas/node-types/DataSeedNode';
import type { AssertionNodeData } from '@/features/dag-canvas/node-types/AssertionNode';
import type { ChaosInjectionNodeData } from '@/features/dag-canvas/node-types/ChaosInjectionNode';
import type { EnvironmentTeardownNodeData } from '@/features/dag-canvas/node-types/EnvironmentTeardownNode';
import type { PipelineEdgeData } from '@/features/dag-canvas/dag-edge-types';

/**
 * Discriminant for every step type this control plane can schedule. Each
 * value maps 1:1 to a custom React Flow node component under
 * src/features/dag-canvas/node-types/.
 */
export type PipelineNodeKind =
  | 'apiRequest'
  | 'loadTest'
  | 'dataSeed'
  | 'assertion'
  | 'chaosInjection'
  | 'environmentTeardown';

/** Union of every step's typed `data` payload, keyed by PipelineNodeKind. */
export type PipelineNodeDataByKind = {
  apiRequest: ApiRequestNodeData;
  loadTest: LoadTestNodeData;
  dataSeed: DataSeedNodeData;
  assertion: AssertionNodeData;
  chaosInjection: ChaosInjectionNodeData;
  environmentTeardown: EnvironmentTeardownNodeData;
};

/** A React Flow node specialized with one of the pipeline step data shapes. */
export type PipelineNode<TKind extends PipelineNodeKind = PipelineNodeKind> = Node<
  PipelineNodeDataByKind[TKind],
  TKind
>;

/** A React Flow edge carrying this platform's conditional-branching metadata. */
export type PipelineEdge = Edge<PipelineEdgeData>;

/**
 * The full authorable definition of a DAG test workflow, as persisted by
 * pipeline-api-gateway. This is the shape sent/received over GraphQL when
 * loading or saving a pipeline in the canvas.
 */
export interface PipelineDefinition {
  pipelineId: string;
  displayName: string;
  description?: string;
  nodes: PipelineNode[];
  edges: PipelineEdge[];
  updatedAt: string;
}

/** Lightweight summary used by list views (landing page, pipeline picker). */
export interface PipelineSummary {
  pipelineId: string;
  displayName: string;
  nodeCount: number;
  updatedAt: string;
}

export type {
  ApiRequestNodeData,
  LoadTestNodeData,
  DataSeedNodeData,
  AssertionNodeData,
  ChaosInjectionNodeData,
  EnvironmentTeardownNodeData,
};
