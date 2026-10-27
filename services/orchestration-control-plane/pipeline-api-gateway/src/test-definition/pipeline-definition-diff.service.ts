import { Injectable, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import {
  DagStructuralDiffReport,
  DiffableDagEdge,
  DiffableDagNode,
  DiffableDagSnapshot,
  EdgeStructuralChange,
  NodeStructuralChange,
} from '@shared-libraries/dag-domain-model';
import { PipelineEdge, PipelineNode } from './dag-domain-types';
import { PipelineDefinitionVersionHistoryEntity } from './pipeline-definition-version-history.entity';

/**
 * Evaluates the "DAG definition versioning & structural diffing" feature on
 * the TypeScript side. This service's *evaluation logic* below is a
 * deliberate re-implementation of `dag-workflow-scheduler`'s
 * `internal/diagnostics/dag_structural_diff_engine.go` — the two are kept
 * behaviorally consistent by convention, not by sharing this code across the
 * Go/TypeScript boundary (see ADR 0002's precedent and the NOTE at the top
 * of that Go file). What genuinely *is* shared, and imported here rather
 * than redeclared, is the diff **contract** — `DagStructuralDiffReport` and
 * friends — from `@shared-libraries/dag-domain-model`'s
 * `src/diffing/dag-structural-diff.types.ts`. A UI (or another service)
 * consuming this service's output shares a schema with a hypothetical
 * future caller of the Go engine's output, even though no runtime code is
 * shared between them.
 */
@Injectable()
export class PipelineDefinitionDiffService {
  constructor(
    @InjectRepository(PipelineDefinitionVersionHistoryEntity)
    private readonly versionHistoryRepository: Repository<PipelineDefinitionVersionHistoryEntity>,
  ) {}

  /**
   * Persists a new immutable version snapshot for pipelineDefinitionId.
   * Called by TestDefinitionService on every create/update so
   * `pipelineDefinitionDiff` always has both endpoints of any version range
   * a caller might ask about.
   */
  async captureVersionSnapshot(
    pipelineDefinitionId: string,
    version: number,
    nodes: PipelineNode[],
    edges: PipelineEdge[],
  ): Promise<PipelineDefinitionVersionHistoryEntity> {
    const snapshot = this.versionHistoryRepository.create({
      pipelineDefinitionId,
      version,
      nodes,
      edges,
    });
    return this.versionHistoryRepository.save(snapshot);
  }

  /**
   * Computes the structural diff between two previously-captured versions of
   * the same pipeline definition. Throws NotFoundException if either version
   * was never captured (e.g. a typo'd version number from the caller).
   */
  async diffVersions(
    pipelineDefinitionId: string,
    fromVersion: number,
    toVersion: number,
  ): Promise<DagStructuralDiffReport> {
    const [fromEntity, toEntity] = await Promise.all([
      this.loadVersionOrThrow(pipelineDefinitionId, fromVersion),
      this.loadVersionOrThrow(pipelineDefinitionId, toVersion),
    ]);

    const fromSnapshot = toDiffableSnapshot(pipelineDefinitionId, fromVersion, fromEntity);
    const toSnapshot = toDiffableSnapshot(pipelineDefinitionId, toVersion, toEntity);

    return diffSnapshots(fromSnapshot, toSnapshot);
  }

  private async loadVersionOrThrow(
    pipelineDefinitionId: string,
    version: number,
  ): Promise<PipelineDefinitionVersionHistoryEntity> {
    const found = await this.versionHistoryRepository.findOne({
      where: { pipelineDefinitionId, version },
    });
    if (!found) {
      throw new NotFoundException(
        `No captured version ${version} found for pipeline definition ${pipelineDefinitionId}`,
      );
    }
    return found;
  }
}

/** Projects a captured version-history row into the shared diff contract's minimal shape. */
function toDiffableSnapshot(
  pipelineId: string,
  version: number,
  entity: PipelineDefinitionVersionHistoryEntity,
): DiffableDagSnapshot {
  return {
    pipelineId,
    version,
    nodes: entity.nodes.map(
      (node): DiffableDagNode => ({
        nodeId: node.id,
        kind: node.stepType,
        config: node.config,
        parentNodeIds: [], // local PipelineNode carries parentage via PipelineEdge, not on the node itself
      }),
    ),
    edges: entity.edges.map(
      (edge): DiffableDagEdge => ({
        fromNodeId: edge.fromNodeId,
        toNodeId: edge.toNodeId,
      }),
    ),
  };
}

/**
 * Pure diff evaluation, mirroring dag_structural_diff_engine.go's Diff/
 * diffNodes/diffEdges. Kept as free functions (rather than private methods)
 * so they can be unit tested independent of a Postgres-backed repository.
 */
export function diffSnapshots(from: DiffableDagSnapshot, to: DiffableDagSnapshot): DagStructuralDiffReport {
  const nodeChanges = diffNodes(from.nodes, to.nodes);
  const edgeChanges = diffEdges(from.edges, to.edges);

  return {
    pipelineId: from.pipelineId,
    fromVersion: from.version,
    toVersion: to.version,
    identicalContent: nodeChanges.length === 0 && edgeChanges.length === 0,
    nodeChanges,
    edgeChanges,
  };
}

function diffNodes(fromNodes: DiffableDagNode[], toNodes: DiffableDagNode[]): NodeStructuralChange[] {
  const fromById = new Map(fromNodes.map((node) => [node.nodeId, node]));
  const toById = new Map(toNodes.map((node) => [node.nodeId, node]));
  const changes: NodeStructuralChange[] = [];

  for (const [nodeId, fromNode] of fromById) {
    const toNode = toById.get(nodeId);
    if (!toNode) {
      changes.push({
        kind: 'NODE_REMOVED',
        nodeId,
        fromNode,
        modifiedConfigFields: [],
        summary: `node "${nodeId}" (kind ${fromNode.kind}) was removed`,
      });
      continue;
    }

    const modifiedConfigFields = diffConfigKeys(fromNode.config, toNode.config);
    const kindChanged = fromNode.kind !== toNode.kind;
    const parentsChanged = !unorderedStringArraysEqual(fromNode.parentNodeIds, toNode.parentNodeIds);

    if (modifiedConfigFields.length > 0 || kindChanged || parentsChanged) {
      changes.push({
        kind: 'NODE_MODIFIED',
        nodeId,
        fromNode,
        toNode,
        modifiedConfigFields,
        summary: summarizeNodeModification(nodeId, fromNode, toNode, modifiedConfigFields, kindChanged, parentsChanged),
      });
    }
  }

  for (const [nodeId, toNode] of toById) {
    if (fromById.has(nodeId)) continue;
    changes.push({
      kind: 'NODE_ADDED',
      nodeId,
      toNode,
      modifiedConfigFields: [],
      summary: `node "${nodeId}" (kind ${toNode.kind}) was added`,
    });
  }

  return changes;
}

function diffConfigKeys(fromConfig: Record<string, unknown>, toConfig: Record<string, unknown>): string[] {
  const changed = new Set<string>();

  for (const key of Object.keys(fromConfig)) {
    if (!(key in toConfig) || !valuesEqual(fromConfig[key], toConfig[key])) {
      changed.add(key);
    }
  }
  for (const key of Object.keys(toConfig)) {
    if (!(key in fromConfig)) {
      changed.add(key);
    }
  }

  return Array.from(changed);
}

/** Structural equality via JSON serialization — sufficient for config blobs that are
 * themselves JSON-serializable by construction (they're persisted as jsonb columns). */
function valuesEqual(a: unknown, b: unknown): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

function unorderedStringArraysEqual(a: string[], b: string[]): boolean {
  if (a.length !== b.length) return false;
  const sortedA = [...a].sort();
  const sortedB = [...b].sort();
  return sortedA.every((value, index) => value === sortedB[index]);
}

function summarizeNodeModification(
  nodeId: string,
  fromNode: DiffableDagNode,
  toNode: DiffableDagNode,
  modifiedConfigFields: string[],
  kindChanged: boolean,
  parentsChanged: boolean,
): string {
  if (kindChanged) {
    return `node "${nodeId}" changed kind from ${fromNode.kind} to ${toNode.kind}`;
  }
  if (parentsChanged) {
    return `node "${nodeId}"'s parent wiring changed from [${fromNode.parentNodeIds.join(', ')}] to [${toNode.parentNodeIds.join(', ')}]`;
  }
  return `node "${nodeId}"'s config field(s) changed: ${modifiedConfigFields.join(', ')}`;
}

function diffEdges(fromEdges: DiffableDagEdge[], toEdges: DiffableDagEdge[]): EdgeStructuralChange[] {
  const key = (edge: DiffableDagEdge) => `${edge.fromNodeId}->${edge.toNodeId}:${edge.condition ?? ''}`;
  const fromByKey = new Map(fromEdges.map((edge) => [key(edge), edge]));
  const toByKey = new Map(toEdges.map((edge) => [key(edge), edge]));
  const changes: EdgeStructuralChange[] = [];

  for (const [edgeKey, edge] of fromByKey) {
    if (!toByKey.has(edgeKey)) {
      changes.push({
        kind: 'EDGE_REMOVED',
        edge,
        summary: `edge ${edge.fromNodeId} -> ${edge.toNodeId} was removed`,
      });
    }
  }
  for (const [edgeKey, edge] of toByKey) {
    if (!fromByKey.has(edgeKey)) {
      changes.push({
        kind: 'EDGE_ADDED',
        edge,
        summary: `edge ${edge.fromNodeId} -> ${edge.toNodeId} was added`,
      });
    }
  }

  return changes;
}
