/**
 * dag-structural-diff.types.ts
 *
 * Shared *contract* for the "DAG definition versioning & structural diffing" feature: given
 * two versions of the same pipeline's DAG, report exactly which nodes/edges were added,
 * removed, or modified between them (the "someone silently removed the
 * consistency-assertion node" case).
 *
 * Both `dag-workflow-scheduler` (Go, see
 * `internal/diagnostics/dag_structural_diff_engine.go`) and `pipeline-api-gateway`
 * (TypeScript, see `src/test-definition/pipeline-definition-diff.service.ts`) implement this
 * feature's *evaluation* independently — each walks its own copy of the DAG snapshot and
 * produces its own verdict, matching this repo's existing precedent (ADR 0002) of
 * duplicating per-language logic atop a shared wire-level/type contract rather than sharing
 * executable code across a Go/TypeScript boundary. What *is* shared, and must never drift, is
 * the shape of the report itself: a UI or CI gate consuming either implementation's output
 * should not need to know which one produced it. That shared shape lives here.
 *
 * DiffableDagNode/DiffableDagEdge/DiffableDagSnapshot are intentionally a minimal structural
 * projection of a full PipelineNode/PipelineEdge/PipelineDefinition (see
 * `../types/pipeline-node.ts` etc.) rather than those types directly: the diff engine only
 * ever needs a node's identity, kind, and config to detect a change, and keeping the diff
 * contract decoupled from the full authoring-time node shape means callers on both sides of
 * the Go/TypeScript boundary can adapt whatever concrete DAG representation they hold (e.g.
 * pipeline-api-gateway's locally-redeclared `PipelineNode` in `dag-domain-types.ts`, which
 * differs field-for-field from this package's own `PipelineNode`) into a DiffableDagSnapshot
 * without those two shapes needing to match exactly.
 */

/** Minimal structural projection of a DAG node sufficient to diff it. */
export interface DiffableDagNode {
  nodeId: string;
  /** The node's kind/type/stepType — named generically here since each caller's own node
   * shape spells this differently (e.g. Go's `PipelineNodeType`, this package's
   * `PipelineNodeKind`, pipeline-api-gateway's local `PipelineStepType`). */
  kind: string;
  /** Free-form node configuration, compared key-by-key for DIFF_KIND_NODE_MODIFIED reporting. */
  config: Record<string, unknown>;
  /** Parent node ids, compared as an unordered set — reordering parents is not a structural change. */
  parentNodeIds: string[];
}

/** Minimal structural projection of a DAG edge sufficient to diff it. */
export interface DiffableDagEdge {
  fromNodeId: string;
  toNodeId: string;
  /** Optional edge condition/label (e.g. Go's `PipelineEdgeCondition`); when two callers'
   * DAG models don't carry edge conditions at all, leave undefined — the engine then treats
   * edges as identified purely by their endpoints. */
  condition?: string;
}

/** One version of one pipeline's DAG, ready to hand to a diff evaluator. */
export interface DiffableDagSnapshot {
  pipelineId: string;
  version: number;
  nodes: DiffableDagNode[];
  edges: DiffableDagEdge[];
}

export type DagStructuralChangeKind =
  | 'NODE_ADDED'
  | 'NODE_REMOVED'
  | 'NODE_MODIFIED'
  | 'EDGE_ADDED'
  | 'EDGE_REMOVED';

/** One added/removed/modified node between two DiffableDagSnapshots. */
export interface NodeStructuralChange {
  kind: DagStructuralChangeKind;
  nodeId: string;
  /** Populated for NODE_REMOVED/NODE_MODIFIED — the "before" state. */
  fromNode?: DiffableDagNode;
  /** Populated for NODE_ADDED/NODE_MODIFIED — the "after" state. */
  toNode?: DiffableDagNode;
  /** Which top-level `config` keys changed value; populated only for NODE_MODIFIED. */
  modifiedConfigFields: string[];
  /** Human-readable summary, e.g. `node "assert-p99-budget" (type ASSERTION) was removed`. */
  summary: string;
}

/**
 * One added/removed edge between two DiffableDagSnapshots. Edges are never reported as
 * "modified" — an edge with the same endpoints but a changed `condition` is a removal of the
 * old edge plus an addition of the new one, since the condition is intrinsic to what the edge
 * *means*, not incidental metadata layered on a stable identity.
 */
export interface EdgeStructuralChange {
  kind: Extract<DagStructuralChangeKind, 'EDGE_ADDED' | 'EDGE_REMOVED'>;
  edge: DiffableDagEdge;
  summary: string;
}

/** Full output of diffing two DiffableDagSnapshots of the same pipeline. */
export interface DagStructuralDiffReport {
  pipelineId: string;
  fromVersion: number;
  toVersion: number;
  /** True when both snapshots' structural content hashes identically — a fast-path signal
   * that nothing changed, independent of whatever is (or isn't) enumerated below. */
  identicalContent: boolean;
  nodeChanges: NodeStructuralChange[];
  edgeChanges: EdgeStructuralChange[];
}
