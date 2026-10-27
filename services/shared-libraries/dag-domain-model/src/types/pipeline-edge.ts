/**
 * pipeline-edge.ts
 *
 * Edges connect PipelineNodes and optionally gate traversal on a predicate — e.g. only follow
 * an edge to a CHAOS_INJECTION node if the upstream ASSERTION node passed, or only proceed to
 * ENVIRONMENT_TEARDOWN regardless of upstream pass/fail (a "finally" edge).
 */

export type ConditionalBranchOperator =
  | "ALWAYS"
  | "ON_SUCCESS"
  | "ON_FAILURE"
  | "EXPRESSION";

/**
 * PipelineEdgePredicate
 *
 * Determines whether the scheduler should traverse this edge once its source node reaches a
 * terminal state.
 *  - ALWAYS: traverse regardless of outcome (e.g. teardown edges).
 *  - ON_SUCCESS / ON_FAILURE: traverse based on the source node's terminal status.
 *  - EXPRESSION: evaluate `expression` against the source node's output (e.g.
 *    "output.errorRate < 0.01"); `dag-workflow-scheduler` owns the expression evaluator, this
 *    package only carries the string.
 */
export interface PipelineEdgePredicate {
  operator: ConditionalBranchOperator;
  /** Required when operator === 'EXPRESSION'. */
  expression?: string;
}

export interface PipelineEdge {
  edgeId: string;
  sourceNodeId: string;
  targetNodeId: string;
  predicate: PipelineEdgePredicate;
}
