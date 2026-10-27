import type { NodeTypes } from 'reactflow';

import ApiRequestNode from './ApiRequestNode';
import LoadTestNode from './LoadTestNode';
import DataSeedNode from './DataSeedNode';
import AssertionNode from './AssertionNode';
import ChaosInjectionNode from './ChaosInjectionNode';
import EnvironmentTeardownNode from './EnvironmentTeardownNode';

/**
 * Registry handed to React Flow's `nodeTypes` prop. Keys must match
 * PipelineNodeKind in src/lib/dag-domain-types.ts exactly.
 */
export const pipelineNodeTypeRegistry: NodeTypes = {
  apiRequest: ApiRequestNode,
  loadTest: LoadTestNode,
  dataSeed: DataSeedNode,
  assertion: AssertionNode,
  chaosInjection: ChaosInjectionNode,
  environmentTeardown: EnvironmentTeardownNode,
};

export {
  ApiRequestNode,
  LoadTestNode,
  DataSeedNode,
  AssertionNode,
  ChaosInjectionNode,
  EnvironmentTeardownNode,
};

export type { ApiRequestNodeData, ApiRequestHttpMethod } from './ApiRequestNode';
export type { LoadTestNodeData } from './LoadTestNode';
export type { DataSeedNodeData, DataSeedStrategy } from './DataSeedNode';
export type { AssertionNodeData, AssertionComparisonOperator } from './AssertionNode';
export type { ChaosInjectionNodeData, ChaosFaultType } from './ChaosInjectionNode';
export type { EnvironmentTeardownNodeData } from './EnvironmentTeardownNode';
