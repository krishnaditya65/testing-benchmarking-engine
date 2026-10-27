import { Field, ID, ObjectType, registerEnumType } from '@nestjs/graphql';
import { Column, CreateDateColumn, Entity, PrimaryGeneratedColumn } from 'typeorm';
import { JsonScalar } from '../common/json.scalar';
import { PolicyViolation } from '../policy-gate/policy-evaluation-result.types';

/** Lifecycle of a single human-in-the-loop approval request. */
export enum ApprovalRequestStatus {
  PENDING = 'PENDING',
  APPROVED = 'APPROVED',
  REJECTED = 'REJECTED',
}

registerEnumType(ApprovalRequestStatus, { name: 'ApprovalRequestStatus' });

/**
 * A durable record that a pipeline run was blocked by a REQUIRES_APPROVAL
 * policy violation (see ../policy-gate/policy-gate.service.ts) and is
 * waiting on a human's approve/reject decision before dag-workflow-scheduler
 * is ever asked to start it. This is the entity PolicyGateService creates
 * instead of hard-failing the run trigger when every violation's severity
 * is REQUIRES_APPROVAL rather than BLOCKING.
 */
@ObjectType('ApprovalRequest')
@Entity({ name: 'approval_request' })
export class ApprovalRequestEntity {
  @Field(() => ID)
  @PrimaryGeneratedColumn('uuid')
  id: string;

  /** The PipelineRunEntity this approval gates — see pipeline-run/pipeline-run.entity.ts.
   * The run itself is persisted with PipelineRunStatus.AWAITING_APPROVAL while this
   * request is PENDING (see pipeline-run.service.ts). */
  @Field(() => ID)
  @Column({ type: 'uuid' })
  pipelineRunId: string;

  @Field(() => ID)
  @Column({ type: 'uuid' })
  pipelineDefinitionId: string;

  @Field(() => ID)
  @Column({ type: 'uuid' })
  requestedByUserId: string;

  @Field(() => ApprovalRequestStatus)
  @Column({ type: 'enum', enum: ApprovalRequestStatus, default: ApprovalRequestStatus.PENDING })
  status: ApprovalRequestStatus;

  /** The exact REQUIRES_APPROVAL (and any co-occurring WARNING) violations that triggered
   * this request, preserved verbatim so a reviewer sees precisely what they're approving. */
  @Field(() => JsonScalar)
  @Column({ type: 'jsonb', default: () => "'[]'" })
  violations: PolicyViolation[];

  @Field(() => ID, { nullable: true })
  @Column({ type: 'uuid', nullable: true })
  resolvedByUserId?: string;

  @Field({ nullable: true })
  @Column({ type: 'text', nullable: true })
  resolutionNote?: string;

  @Field()
  @CreateDateColumn({ type: 'timestamptz' })
  createdAt: Date;

  /** Set explicitly by ApprovalWorkflowService.approve/reject — deliberately a plain
   * nullable column rather than @UpdateDateColumn, which would stamp a value on every
   * save (including the initial PENDING insert) instead of only on resolution. */
  @Field({ nullable: true })
  @Column({ type: 'timestamptz', nullable: true })
  resolvedAt?: Date;
}
