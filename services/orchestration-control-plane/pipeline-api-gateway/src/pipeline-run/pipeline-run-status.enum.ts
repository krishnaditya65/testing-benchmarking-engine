import { registerEnumType } from '@nestjs/graphql';

/** Lifecycle states of a triggered pipeline run. */
export enum PipelineRunStatus {
  PENDING = 'PENDING',
  /**
   * The run was blocked by a REQUIRES_APPROVAL policy-gate violation (see
   * ../policy-gate/policy-gate.service.ts) and is waiting on a human
   * approve/reject decision (see ../approval-workflow/) before
   * dag-workflow-scheduler is ever asked to start it.
   */
  AWAITING_APPROVAL = 'AWAITING_APPROVAL',
  RUNNING = 'RUNNING',
  PASSED = 'PASSED',
  FAILED = 'FAILED',
  CANCELLED = 'CANCELLED',
}

registerEnumType(PipelineRunStatus, { name: 'PipelineRunStatus' });
