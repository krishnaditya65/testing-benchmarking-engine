import { BadRequestException, Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { Repository } from 'typeorm';
import { PolicyViolation } from '../policy-gate/policy-evaluation-result.types';
import { ApprovalRequestEntity, ApprovalRequestStatus } from './approval-request.entity';
import { RUN_APPROVAL_DECISION_EVENT, RunApprovalDecisionEvent } from './run-approval-decision.event';

/**
 * Owns the human-in-the-loop approval lifecycle: PolicyGateService (see
 * ../policy-gate/policy-gate.service.ts) calls createApprovalRequest
 * instead of hard-failing a run trigger when every blocking violation's
 * severity is REQUIRES_APPROVAL; a human reviewer then calls approve/reject
 * via ApprovalWorkflowResolver's GraphQL mutations.
 *
 * Resuming (or failing) the gated pipeline run once a request is resolved
 * is deliberately *not* done via a direct call into PipelineRunService here
 * — that would require this module to depend on PipelineRunModule, which
 * would close a module import cycle, since PipelineRunModule already
 * depends on PolicyGateModule (which depends on this module) to gate run
 * triggers in the first place. Instead, `resolve` emits a
 * RunApprovalDecisionEvent on NestJS's built-in EventEmitter2
 * (`@nestjs/event-emitter`); PipelineRunModule's RunApprovalEventListener
 * (see ../pipeline-run/run-approval-event.listener.ts) subscribes to it and
 * actually resumes/fails the run. See run-approval-decision.event.ts's file
 * header for the full cycle-avoidance rationale.
 */
@Injectable()
export class ApprovalWorkflowService {
  private readonly logger = new Logger(ApprovalWorkflowService.name);

  constructor(
    @InjectRepository(ApprovalRequestEntity)
    private readonly approvalRequestRepository: Repository<ApprovalRequestEntity>,
    private readonly eventEmitter: EventEmitter2,
  ) {}

  async createApprovalRequest(
    pipelineRunId: string,
    pipelineDefinitionId: string,
    requestedByUserId: string,
    violations: PolicyViolation[],
  ): Promise<ApprovalRequestEntity> {
    const request = this.approvalRequestRepository.create({
      pipelineRunId,
      pipelineDefinitionId,
      requestedByUserId,
      violations,
      status: ApprovalRequestStatus.PENDING,
    });

    const saved = await this.approvalRequestRepository.save(request);
    this.logger.log(
      `Created approval request ${saved.id} for pipeline run ${pipelineRunId} (${violations.length} REQUIRES_APPROVAL violation(s))`,
    );
    return saved;
  }

  async findById(requestId: string): Promise<ApprovalRequestEntity> {
    const found = await this.approvalRequestRepository.findOne({ where: { id: requestId } });
    if (!found) {
      throw new NotFoundException(`Approval request ${requestId} not found`);
    }
    return found;
  }

  async findPendingForPipelineRun(pipelineRunId: string): Promise<ApprovalRequestEntity[]> {
    return this.approvalRequestRepository.find({
      where: { pipelineRunId, status: ApprovalRequestStatus.PENDING },
      order: { createdAt: 'DESC' },
    });
  }

  async approve(requestId: string, resolvedByUserId: string, resolutionNote?: string): Promise<ApprovalRequestEntity> {
    return this.resolve(requestId, ApprovalRequestStatus.APPROVED, resolvedByUserId, resolutionNote);
  }

  async reject(requestId: string, resolvedByUserId: string, resolutionNote?: string): Promise<ApprovalRequestEntity> {
    return this.resolve(requestId, ApprovalRequestStatus.REJECTED, resolvedByUserId, resolutionNote);
  }

  private async resolve(
    requestId: string,
    resolution: ApprovalRequestStatus.APPROVED | ApprovalRequestStatus.REJECTED,
    resolvedByUserId: string,
    resolutionNote?: string,
  ): Promise<ApprovalRequestEntity> {
    const request = await this.findById(requestId);

    if (request.status !== ApprovalRequestStatus.PENDING) {
      throw new BadRequestException(
        `Approval request ${requestId} was already resolved as ${request.status}; cannot resolve it again`,
      );
    }

    request.status = resolution;
    request.resolvedByUserId = resolvedByUserId;
    request.resolutionNote = resolutionNote;
    request.resolvedAt = new Date();

    const saved = await this.approvalRequestRepository.save(request);
    this.logger.log(`Approval request ${requestId} resolved as ${resolution} by user ${resolvedByUserId}`);

    this.eventEmitter.emit(
      RUN_APPROVAL_DECISION_EVENT,
      new RunApprovalDecisionEvent(saved.id, saved.pipelineRunId, resolution, resolvedByUserId, resolutionNote),
    );

    return saved;
  }
}
