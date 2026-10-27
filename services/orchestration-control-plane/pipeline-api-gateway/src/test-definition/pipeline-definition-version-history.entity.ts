import { Field, ID, Int, ObjectType } from '@nestjs/graphql';
import { Column, CreateDateColumn, Entity, Index, PrimaryGeneratedColumn } from 'typeorm';
import { JsonScalar } from '../common/json.scalar';
import { PipelineEdge, PipelineNode } from './dag-domain-types';

/**
 * One immutable, point-in-time snapshot of a PipelineDefinition's DAG shape
 * (nodes + edges), captured every time TestDefinitionService persists a
 * create or update. This is what powers "DAG definition versioning &
 * structural diffing": PipelineDefinitionEntity only ever holds the
 * *current* nodes/edges (see pipeline-definition.entity.ts), so without a
 * separate append-only history there would be nothing to diff a definition
 * against — the whole point of this feature is catching the case where
 * someone silently removed, say, the consistency-assertion node between one
 * save and the next, which is precisely the comparison a single mutable row
 * cannot support.
 *
 * Deliberately a plain, denormalized snapshot (not a series of diffs stored
 * incrementally) — computing the diff between any two arbitrary versions is
 * then just "load both rows and diff them" (see
 * pipeline-definition-diff.service.ts), rather than needing to replay a
 * chain of incremental patches.
 */
@ObjectType('PipelineDefinitionVersionHistory')
@Entity({ name: 'pipeline_definition_version_history' })
@Index(['pipelineDefinitionId', 'version'], { unique: true })
export class PipelineDefinitionVersionHistoryEntity {
  @Field(() => ID)
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Field(() => ID)
  @Column({ type: 'uuid' })
  pipelineDefinitionId: string;

  /**
   * Mirrors PipelineDefinitionEntity.version (TypeORM's @VersionColumn) at
   * the moment this snapshot was captured — the identifier callers pass as
   * `fromVersion`/`toVersion` to the `pipelineDefinitionDiff` GraphQL query.
   */
  @Field(() => Int)
  @Column({ type: 'int' })
  version: number;

  @Field(() => JsonScalar)
  @Column({ type: 'jsonb', default: () => "'[]'" })
  nodes: PipelineNode[];

  @Field(() => JsonScalar)
  @Column({ type: 'jsonb', default: () => "'[]'" })
  edges: PipelineEdge[];

  @Field()
  @CreateDateColumn({ type: 'timestamptz' })
  capturedAt: Date;
}
