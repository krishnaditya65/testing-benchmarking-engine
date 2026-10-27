import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { CreatePipelineDefinitionInput } from './dto/create-pipeline-definition.input';
import { UpdatePipelineDefinitionInput } from './dto/update-pipeline-definition.input';
import { PipelineEdge, PipelineNode } from './dag-domain-types';
import { PipelineDefinitionDiffService } from './pipeline-definition-diff.service';
import { PipelineDefinitionEntity } from './pipeline-definition.entity';

/**
 * CRUD + DAG-shape validation for pipeline test definitions. This is the
 * only place in the gateway that mutates the `pipeline_definition` table.
 *
 * Also the only place that calls `PipelineDefinitionDiffService.captureVersionSnapshot`:
 * every create/update is the sole moment `PipelineDefinitionEntity.version`
 * changes, so capturing a version-history row right here (rather than, say,
 * a database trigger) keeps "when a new version exists" and "when its
 * snapshot was captured" atomically the same event.
 */
@Injectable()
export class TestDefinitionService {
  constructor(
    @InjectRepository(PipelineDefinitionEntity)
    private readonly pipelineDefinitionRepository: Repository<PipelineDefinitionEntity>,
    private readonly diffService: PipelineDefinitionDiffService,
  ) {}

  async create(input: CreatePipelineDefinitionInput): Promise<PipelineDefinitionEntity> {
    this.assertDagShapeIsValid(input.nodes, input.edges);

    const entity = this.pipelineDefinitionRepository.create({
      name: input.name,
      description: input.description,
      ownerId: input.ownerId,
      nodes: input.nodes,
      edges: input.edges,
    });

    const saved = await this.pipelineDefinitionRepository.save(entity);
    await this.diffService.captureVersionSnapshot(saved.id, saved.version, saved.nodes, saved.edges);
    return saved;
  }

  async findById(id: string): Promise<PipelineDefinitionEntity> {
    const found = await this.pipelineDefinitionRepository.findOne({ where: { id } });
    if (!found) {
      throw new NotFoundException(`Pipeline definition ${id} not found`);
    }
    return found;
  }

  async findAllForOwner(ownerId: string): Promise<PipelineDefinitionEntity[]> {
    return this.pipelineDefinitionRepository.find({
      where: { ownerId },
      order: { updatedAt: 'DESC' },
    });
  }

  async update(input: UpdatePipelineDefinitionInput): Promise<PipelineDefinitionEntity> {
    const existing = await this.findById(input.id);

    const nextNodes = input.nodes ?? existing.nodes;
    const nextEdges = input.edges ?? existing.edges;
    if (input.nodes || input.edges) {
      this.assertDagShapeIsValid(nextNodes, nextEdges);
    }

    existing.name = input.name ?? existing.name;
    existing.description = input.description ?? existing.description;
    existing.nodes = nextNodes;
    existing.edges = nextEdges;

    const saved = await this.pipelineDefinitionRepository.save(existing);
    await this.diffService.captureVersionSnapshot(saved.id, saved.version, saved.nodes, saved.edges);
    return saved;
  }

  async delete(id: string): Promise<boolean> {
    const result = await this.pipelineDefinitionRepository.delete({ id });
    if (!result.affected) {
      throw new NotFoundException(`Pipeline definition ${id} not found`);
    }
    return true;
  }

  /**
   * Automated DAG-shape validation, performed at write time so malformed
   * pipelines never reach dag-workflow-scheduler:
   *  - node ids must be unique
   *  - every edge must reference existing node ids on both ends
   *  - an edge may not connect a node to itself
   *  - the graph must not contain a cycle (a DAG, by definition, must not)
   */
  private assertDagShapeIsValid(nodes: PipelineNode[], edges: PipelineEdge[]): void {
    const nodeIds = new Set(nodes.map((node) => node.id));
    if (nodeIds.size !== nodes.length) {
      throw new BadRequestException('Pipeline definition contains duplicate node ids');
    }

    for (const edge of edges) {
      if (!nodeIds.has(edge.fromNodeId)) {
        throw new BadRequestException(
          `Edge "${edge.id}" references non-existent source node id "${edge.fromNodeId}"`,
        );
      }
      if (!nodeIds.has(edge.toNodeId)) {
        throw new BadRequestException(
          `Edge "${edge.id}" references non-existent target node id "${edge.toNodeId}"`,
        );
      }
      if (edge.fromNodeId === edge.toNodeId) {
        throw new BadRequestException(`Edge "${edge.id}" cannot connect node "${edge.fromNodeId}" to itself`);
      }
    }

    this.assertGraphIsAcyclic(nodeIds, edges);
  }

  private assertGraphIsAcyclic(nodeIds: Set<string>, edges: PipelineEdge[]): void {
    const adjacency = new Map<string, string[]>();
    for (const nodeId of nodeIds) {
      adjacency.set(nodeId, []);
    }
    for (const edge of edges) {
      adjacency.get(edge.fromNodeId)?.push(edge.toNodeId);
    }

    const visitState = new Map<string, 'visiting' | 'visited'>();

    const visit = (nodeId: string): void => {
      const state = visitState.get(nodeId);
      if (state === 'visited') {
        return;
      }
      if (state === 'visiting') {
        throw new BadRequestException(`Pipeline DAG contains a cycle through node "${nodeId}"`);
      }

      visitState.set(nodeId, 'visiting');
      for (const neighbourId of adjacency.get(nodeId) ?? []) {
        visit(neighbourId);
      }
      visitState.set(nodeId, 'visited');
    };

    for (const nodeId of nodeIds) {
      visit(nodeId);
    }
  }
}
