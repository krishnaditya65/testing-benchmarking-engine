/**
 * DAG domain types owned conceptually by the shared-libraries layer.
 *
 * In the full monorepo these would be imported from
 * `@distributed-testing/dag-domain-model` (services/shared-libraries/dag-domain-model),
 * which is not yet built. They are redefined locally here so
 * pipeline-api-gateway can ship independently; when the shared package
 * lands, this file should be deleted in favour of that import.
 */

/** The kinds of work a single DAG node can represent. */
export type PipelineStepType =
  | 'ApiRequest'
  | 'LoadTest'
  | 'DataSeed'
  | 'Assertion'
  | 'ChaosInjection'
  | 'EnvironmentTeardown';

/**
 * A single node in a pipeline's DAG. `config` is intentionally loosely
 * typed (Record<string, unknown>) since its shape is step-type specific
 * (e.g. an ApiRequest node's config differs entirely from a LoadTest
 * node's) and may contain secret-injection placeholders such as
 * `${{secret:vault:kv/data/pipelines#apiKey}}` resolved by
 * secret-injection at execution time.
 */
export interface PipelineNode {
  id: string;
  stepType: PipelineStepType;
  name: string;
  config: Record<string, unknown>;
}

/** A directed edge between two node ids, describing execution order. */
export interface PipelineEdge {
  id: string;
  fromNodeId: string;
  toNodeId: string;
}

/** Full DAG shape persisted on a PipelineDefinition entity. */
export interface PipelineDag {
  nodes: PipelineNode[];
  edges: PipelineEdge[];
}
