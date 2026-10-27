/**
 * pipeline-definition.ts
 *
 * The top-level saved artifact: a named, versioned DAG of PipelineNodes connected by
 * PipelineEdges. This is what `pipeline-api-gateway` persists and what
 * `dag-workflow-scheduler` executes a run against.
 */
import type { PipelineNode } from "./pipeline-node";
import type { PipelineEdge } from "./pipeline-edge";

export interface PipelineDefinition {
  pipelineId: string;
  name: string;
  description?: string;
  /** Bumped on every save; `dag-workflow-scheduler` pins a run to the version active at
   * dispatch time so an in-flight run is never mutated out from under itself. */
  version: number;
  nodes: PipelineNode[];
  edges: PipelineEdge[];
  createdAtIso: string;
  updatedAtIso: string;
  /** RBAC owner, enforced by pipeline-api-gateway; carried here for display/audit purposes. */
  ownerId: string;
}
