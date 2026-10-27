import { Injectable, Logger } from '@nestjs/common';
import { OnEvent } from '@nestjs/event-emitter';
import { ApprovalRequestStatus } from '../approval-workflow/approval-request.entity';
import { RUN_APPROVAL_DECISION_EVENT, RunApprovalDecisionEvent } from '../approval-workflow/run-approval-decision.event';
import { PipelineRunService } from './pipeline-run.service';

/**
 * Subscribes to RunApprovalDecisionEvent (emitted by
 * ../approval-workflow/approval-workflow.service.ts's `resolve`) and turns
 * it into the actual run-resume/fail side effect. This listener — not a
 * direct method call from ApprovalWorkflowService — is what closes the loop
 * from "human approved/rejected a held run" back to "the run actually
 * resumes/fails", specifically *because* ApprovalWorkflowModule cannot
 * import PipelineRunModule without creating a module cycle (see
 * ../approval-workflow/run-approval-decision.event.ts's file header for the
 * full rationale). Living in PipelineRunModule (registered as a provider in
 * pipeline-run.module.ts) rather than in ApprovalWorkflowModule is what
 * makes this event-driven decoupling actually break the cycle: this module
 * depends on ApprovalWorkflowModule only for a plain TS type/event-name
 * import (no `@Module` import), while ApprovalWorkflowModule never needs to
 * know PipelineRunModule exists at all.
 */
@Injectable()
export class RunApprovalEventListener {
  private readonly logger = new Logger(RunApprovalEventListener.name);

  constructor(private readonly pipelineRunService: PipelineRunService) {}

  @OnEvent(RUN_APPROVAL_DECISION_EVENT)
  async handleRunApprovalDecision(event: RunApprovalDecisionEvent): Promise<void> {
    if (event.decision === ApprovalRequestStatus.APPROVED) {
      this.logger.log(
        `Approval request ${event.approvalRequestId} approved by user ${event.resolvedByUserId}; resuming pipeline run ${event.pipelineRunId}`,
      );
      await this.pipelineRunService.resumeHeldRun(event.pipelineRunId);
      return;
    }

    this.logger.log(
      `Approval request ${event.approvalRequestId} rejected by user ${event.resolvedByUserId}; failing pipeline run ${event.pipelineRunId}`,
    );
    const reason = event.resolutionNote
      ? `Rejected by approval request ${event.approvalRequestId}: ${event.resolutionNote}`
      : `Rejected by approval request ${event.approvalRequestId}`;
    await this.pipelineRunService.failHeldRun(event.pipelineRunId, reason);
  }
}
