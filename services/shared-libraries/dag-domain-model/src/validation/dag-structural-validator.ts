/**
 * dag-structural-validator.ts
 *
 * Structural validation that must pass before `pipeline-api-gateway` persists a
 * PipelineDefinition and before `dag-workflow-scheduler` accepts it for execution. Checks:
 *  1. Acyclicity — a cycle would mean the scheduler can never find a valid topological
 *     dispatch order.
 *  2. Reachability — every node must be reachable from at least one root (a node with no
 *     incoming edges); unreachable nodes are almost always an authoring mistake (an
 *     accidentally disconnected branch) and would silently never execute.
 *  3. Referential integrity — every edge must reference node ids that actually exist in the
 *     definition, checked first since acyclicity/reachability are meaningless over dangling
 *     references.
 *
 * This class does not mutate its input and has no I/O — it's pure so both the frontend (for
 * instant canvas-side validation feedback) and the backend (as a save-time gate) can run the
 * identical check and never disagree about what "valid" means.
 */
import type { PipelineDefinition } from "../types/pipeline-definition";

export type DagStructuralIssueKind =
  | "DANGLING_EDGE_REFERENCE"
  | "CYCLE_DETECTED"
  | "UNREACHABLE_NODE"
  | "NO_ROOT_NODE"
  | "DUPLICATE_NODE_ID";

export interface DagStructuralIssue {
  kind: DagStructuralIssueKind;
  message: string;
  /** Node/edge ids implicated in this issue, for highlighting on the canvas. */
  nodeIds: string[];
  edgeIds: string[];
}

export interface DagStructuralValidationResult {
  isValid: boolean;
  issues: DagStructuralIssue[];
}

export class DagStructuralValidator {
  /**
   * Runs every structural check against a PipelineDefinition. Cheap to call on every canvas
   * edit — all checks are O(V + E).
   */
  validate(definition: PipelineDefinition): DagStructuralValidationResult {
    const issues: DagStructuralIssue[] = [];

    const duplicateIssues = this.findDuplicateNodeIds(definition);
    issues.push(...duplicateIssues);

    const danglingEdgeIssues = this.findDanglingEdgeReferences(definition);
    issues.push(...danglingEdgeIssues);

    // Cycle/reachability checks assume referential integrity; skip them if edges reference
    // nonexistent nodes, since the adjacency list would be misleading otherwise.
    if (danglingEdgeIssues.length === 0) {
      issues.push(...this.findCycles(definition));
      issues.push(...this.findUnreachableNodes(definition));
    }

    return { isValid: issues.length === 0, issues };
  }

  private findDuplicateNodeIds(definition: PipelineDefinition): DagStructuralIssue[] {
    const seen = new Set<string>();
    const duplicates = new Set<string>();
    for (const node of definition.nodes) {
      if (seen.has(node.nodeId)) duplicates.add(node.nodeId);
      seen.add(node.nodeId);
    }
    if (duplicates.size === 0) return [];
    return [
      {
        kind: "DUPLICATE_NODE_ID",
        message: `Duplicate node id(s): ${Array.from(duplicates).join(", ")}`,
        nodeIds: Array.from(duplicates),
        edgeIds: [],
      },
    ];
  }

  private findDanglingEdgeReferences(definition: PipelineDefinition): DagStructuralIssue[] {
    const nodeIds = new Set(definition.nodes.map((n) => n.nodeId));
    const issues: DagStructuralIssue[] = [];
    for (const edge of definition.edges) {
      const missing: string[] = [];
      if (!nodeIds.has(edge.sourceNodeId)) missing.push(edge.sourceNodeId);
      if (!nodeIds.has(edge.targetNodeId)) missing.push(edge.targetNodeId);
      if (missing.length > 0) {
        issues.push({
          kind: "DANGLING_EDGE_REFERENCE",
          message: `Edge ${edge.edgeId} references nonexistent node id(s): ${missing.join(", ")}`,
          nodeIds: missing,
          edgeIds: [edge.edgeId],
        });
      }
    }
    return issues;
  }

  private buildAdjacency(definition: PipelineDefinition): Map<string, string[]> {
    const adjacency = new Map<string, string[]>(definition.nodes.map((n) => [n.nodeId, []]));
    for (const edge of definition.edges) {
      adjacency.get(edge.sourceNodeId)?.push(edge.targetNodeId);
    }
    return adjacency;
  }

  /** DFS-based cycle detection using the classic white/gray/black coloring, so it reports
   * every distinct cycle's participating nodes rather than stopping at the first one found. */
  private findCycles(definition: PipelineDefinition): DagStructuralIssue[] {
    const adjacency = this.buildAdjacency(definition);
    const color = new Map<string, "WHITE" | "GRAY" | "BLACK">(
      definition.nodes.map((n) => [n.nodeId, "WHITE"]),
    );
    const issues: DagStructuralIssue[] = [];
    const path: string[] = [];

    const visit = (nodeId: string): void => {
      color.set(nodeId, "GRAY");
      path.push(nodeId);

      for (const neighbor of adjacency.get(nodeId) ?? []) {
        const neighborColor = color.get(neighbor);
        if (neighborColor === "GRAY") {
          const cycleStart = path.indexOf(neighbor);
          const cycleNodes = path.slice(cycleStart);
          issues.push({
            kind: "CYCLE_DETECTED",
            message: `Cycle detected: ${cycleNodes.join(" -> ")} -> ${neighbor}`,
            nodeIds: cycleNodes,
            edgeIds: [],
          });
        } else if (neighborColor === "WHITE") {
          visit(neighbor);
        }
      }

      path.pop();
      color.set(nodeId, "BLACK");
    };

    for (const node of definition.nodes) {
      if (color.get(node.nodeId) === "WHITE") {
        visit(node.nodeId);
      }
    }

    return issues;
  }

  /** A node is a root if no edge targets it. Every node must be reachable from some root via
   * forward edge traversal; anything else is flagged as an orphaned/unreachable branch. */
  private findUnreachableNodes(definition: PipelineDefinition): DagStructuralIssue[] {
    if (definition.nodes.length === 0) return [];

    const targetedNodeIds = new Set(definition.edges.map((e) => e.targetNodeId));
    const roots = definition.nodes.filter((n) => !targetedNodeIds.has(n.nodeId));

    if (roots.length === 0) {
      // Every node has an incoming edge, which combined with a cycle-free graph (checked
      // separately) is only possible on an empty graph — but guard explicitly with a clear
      // message in case this runs standalone against a cyclic graph in the future.
      return [
        {
          kind: "NO_ROOT_NODE",
          message: "No root node found — every node has at least one incoming edge.",
          nodeIds: definition.nodes.map((n) => n.nodeId),
          edgeIds: [],
        },
      ];
    }

    const adjacency = this.buildAdjacency(definition);
    const reachable = new Set<string>();
    const queue = roots.map((r) => r.nodeId);
    while (queue.length > 0) {
      const current = queue.shift()!;
      if (reachable.has(current)) continue;
      reachable.add(current);
      for (const neighbor of adjacency.get(current) ?? []) {
        if (!reachable.has(neighbor)) queue.push(neighbor);
      }
    }

    const unreachable = definition.nodes.filter((n) => !reachable.has(n.nodeId));
    if (unreachable.length === 0) return [];

    return [
      {
        kind: "UNREACHABLE_NODE",
        message: `Node(s) unreachable from any root: ${unreachable.map((n) => n.nodeId).join(", ")}`,
        nodeIds: unreachable.map((n) => n.nodeId),
        edgeIds: [],
      },
    ];
  }
}
