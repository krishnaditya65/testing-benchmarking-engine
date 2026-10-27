import { Field, ID, ObjectType } from '@nestjs/graphql';
import { Column, CreateDateColumn, Entity, PrimaryGeneratedColumn } from 'typeorm';
import { JsonScalar } from '../common/json.scalar';
import { PipelineRunStatus } from './pipeline-run-status.enum';

/** Per-node execution status, as reported back by dag-workflow-scheduler. */
export interface PipelineRunNodeStatus {
  nodeId: string;
  status: PipelineRunStatus;
  startedAt?: string;
  completedAt?: string;
  errorMessage?: string;
}

/**
 * Durable record of a single pipeline execution. This is the source of
 * truth for run state in the control plane — dag-workflow-scheduler is
 * stateless from this gateway's point of view; it only streams status
 * updates back in, which are folded into `nodeStatuses` here.
 */
@ObjectType('PipelineRun')
@Entity({ name: 'pipeline_run' })
export class PipelineRunEntity {
  @Field(() => ID)
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Field(() => ID)
  @Column({ type: 'uuid' })
  pipelineDefinitionId: string;

  @Field(() => PipelineRunStatus)
  @Column({ type: 'enum', enum: PipelineRunStatus, default: PipelineRunStatus.PENDING })
  status: PipelineRunStatus;

  @Field({ nullable: true })
  @Column({ type: 'timestamptz', nullable: true })
  startedAt?: Date;

  @Field({ nullable: true })
  @Column({ type: 'timestamptz', nullable: true })
  completedAt?: Date;

  /** Keyed by nodeId, kept as jsonb since node-status payload shape can evolve independently. */
  @Field(() => JsonScalar)
  @Column({ type: 'jsonb', default: () => "'[]'" })
  nodeStatuses: PipelineRunNodeStatus[];

  /**
   * Run id assigned by dag-workflow-scheduler once it accepts the trigger.
   * Needed to correlate inbound status webhooks/events and to issue cancel
   * requests. Nullable until the scheduler responds.
   */
  @Field({ nullable: true })
  @Column({ type: 'varchar', length: 255, nullable: true })
  schedulerRunId?: string;

  /**
   * Projected dollar cost/CO2 footprint fetched from run-cost-estimator
   * (see ../cost-estimation/cost-estimator-client.port.ts) at trigger time,
   * before dag-workflow-scheduler is called. Nullable because the estimate
   * call is best-effort — a run-cost-estimator outage should never block a
   * run from starting, only leave these fields unset (see
   * pipeline-run.service.ts's triggerRun).
   */
  @Field({ nullable: true })
  @Column({ type: 'numeric', nullable: true })
  estimatedCostUsd?: number;

  @Field({ nullable: true })
  @Column({ type: 'numeric', nullable: true })
  estimatedCarbonKgCo2e?: number;

  @Field()
  @CreateDateColumn({ type: 'timestamptz' })
  createdAt: Date;
}
