import { Field, ID, Int, ObjectType } from '@nestjs/graphql';
import {
  Column,
  CreateDateColumn,
  Entity,
  PrimaryGeneratedColumn,
  UpdateDateColumn,
  VersionColumn,
} from 'typeorm';
import { PipelineEdge, PipelineNode } from './dag-domain-types';
import { PipelineEdgeObjectType, PipelineNodeObjectType } from './pipeline-node.object-type';

/**
 * Persisted, versioned definition of a DAG test pipeline. Owned exclusively
 * by pipeline-api-gateway (the control plane) — dag-workflow-scheduler and
 * telemetry-query-api only ever receive a snapshot of this at run-trigger
 * time, they do not read this table directly.
 */
@ObjectType('PipelineDefinition')
@Entity({ name: 'pipeline_definition' })
export class PipelineDefinitionEntity {
  @Field(() => ID)
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Field()
  @Column({ type: 'varchar', length: 255 })
  name: string;

  @Field({ nullable: true })
  @Column({ type: 'text', nullable: true })
  description?: string;

  @Field()
  @Column({ type: 'uuid' })
  ownerId: string;

  /** DAG nodes, persisted as jsonb and exposed directly over GraphQL. */
  @Field(() => [PipelineNodeObjectType])
  @Column({ type: 'jsonb', default: () => "'[]'" })
  nodes: PipelineNode[];

  @Field(() => [PipelineEdgeObjectType])
  @Column({ type: 'jsonb', default: () => "'[]'" })
  edges: PipelineEdge[];

  @Field()
  @CreateDateColumn({ type: 'timestamptz' })
  createdAt: Date;

  @Field()
  @UpdateDateColumn({ type: 'timestamptz' })
  updatedAt: Date;

  /** Optimistic-concurrency version, bumped by TypeORM on every update. */
  @Field(() => Int)
  @VersionColumn()
  version: number;
}
