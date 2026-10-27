import { EventEmitter2 } from '@nestjs/event-emitter';
import { Test } from '@nestjs/testing';
import { getRepositoryToken } from '@nestjs/typeorm';
import { ApprovalRequestEntity, ApprovalRequestStatus } from '../approval-workflow/approval-request.entity';
import { ApprovalWorkflowService } from '../approval-workflow/approval-workflow.service';
import { RUN_APPROVAL_DECISION_EVENT, RunApprovalDecisionEvent } from '../approval-workflow/run-approval-decision.event';

describe('ApprovalWorkflowService', () => {
  const pipelineRunId = 'pipeline-run-1';
  const requestId = 'approval-request-1';
  const resolvedByUserId = 'user-1';

  const buildPendingRequest = (): ApprovalRequestEntity =>
    ({
      id: requestId,
      pipelineRunId,
      pipelineDefinitionId: 'pipeline-def-1',
      requestedByUserId: 'requester-1',
      status: ApprovalRequestStatus.PENDING,
      violations: [],
      createdAt: new Date(),
    } as ApprovalRequestEntity);

  const buildService = async (initialRequest: ApprovalRequestEntity) => {
    const repository = {
      findOne: jest.fn().mockResolvedValue(initialRequest),
      save: jest.fn().mockImplementation((entity: ApprovalRequestEntity) => Promise.resolve(entity)),
    };
    const eventEmitter = { emit: jest.fn() };

    const moduleRef = await Test.createTestingModule({
      providers: [
        ApprovalWorkflowService,
        { provide: getRepositoryToken(ApprovalRequestEntity), useValue: repository },
        { provide: EventEmitter2, useValue: eventEmitter },
      ],
    }).compile();

    return {
      service: moduleRef.get(ApprovalWorkflowService),
      repository,
      eventEmitter,
    };
  };

  it('emits a RunApprovalDecisionEvent with decision APPROVED when approve() resolves', async () => {
    const { service, eventEmitter } = await buildService(buildPendingRequest());

    await service.approve(requestId, resolvedByUserId, 'looks safe');

    expect(eventEmitter.emit).toHaveBeenCalledTimes(1);
    const [eventName, event] = eventEmitter.emit.mock.calls[0] as [string, RunApprovalDecisionEvent];
    expect(eventName).toBe(RUN_APPROVAL_DECISION_EVENT);
    expect(event.approvalRequestId).toBe(requestId);
    expect(event.pipelineRunId).toBe(pipelineRunId);
    expect(event.decision).toBe(ApprovalRequestStatus.APPROVED);
    expect(event.resolvedByUserId).toBe(resolvedByUserId);
    expect(event.resolutionNote).toBe('looks safe');
  });

  it('emits a RunApprovalDecisionEvent with decision REJECTED when reject() resolves', async () => {
    const { service, eventEmitter } = await buildService(buildPendingRequest());

    await service.reject(requestId, resolvedByUserId, 'blast radius too large');

    expect(eventEmitter.emit).toHaveBeenCalledTimes(1);
    const [eventName, event] = eventEmitter.emit.mock.calls[0] as [string, RunApprovalDecisionEvent];
    expect(eventName).toBe(RUN_APPROVAL_DECISION_EVENT);
    expect(event.decision).toBe(ApprovalRequestStatus.REJECTED);
  });

  it('does not emit anything when resolving an already-resolved request throws', async () => {
    const alreadyResolved = { ...buildPendingRequest(), status: ApprovalRequestStatus.APPROVED };
    const { service, eventEmitter } = await buildService(alreadyResolved);

    await expect(service.approve(requestId, resolvedByUserId)).rejects.toThrow();
    expect(eventEmitter.emit).not.toHaveBeenCalled();
  });
});
