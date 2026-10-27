import { Test } from '@nestjs/testing';
import { ApprovalRequestStatus } from '../approval-workflow/approval-request.entity';
import { RunApprovalDecisionEvent } from '../approval-workflow/run-approval-decision.event';
import { PipelineRunEntity } from '../pipeline-run/pipeline-run.entity';
import { PipelineRunService } from '../pipeline-run/pipeline-run.service';
import { RunApprovalEventListener } from '../pipeline-run/run-approval-event.listener';

describe('RunApprovalEventListener', () => {
  const approvalRequestId = 'approval-request-1';
  const pipelineRunId = 'pipeline-run-1';
  const resolvedByUserId = 'user-1';

  const buildListener = async (pipelineRunService: Partial<PipelineRunService>) => {
    const moduleRef = await Test.createTestingModule({
      providers: [
        RunApprovalEventListener,
        { provide: PipelineRunService, useValue: pipelineRunService },
      ],
    }).compile();

    return moduleRef.get(RunApprovalEventListener);
  };

  it('resumes the held run when the decision is APPROVED', async () => {
    const resumeHeldRun = jest.fn().mockResolvedValue({ id: pipelineRunId } as PipelineRunEntity);
    const failHeldRun = jest.fn();
    const listener = await buildListener({ resumeHeldRun, failHeldRun });

    const event = new RunApprovalDecisionEvent(
      approvalRequestId,
      pipelineRunId,
      ApprovalRequestStatus.APPROVED,
      resolvedByUserId,
    );

    await listener.handleRunApprovalDecision(event);

    expect(resumeHeldRun).toHaveBeenCalledTimes(1);
    expect(resumeHeldRun).toHaveBeenCalledWith(pipelineRunId);
    expect(failHeldRun).not.toHaveBeenCalled();
  });

  it('fails the held run when the decision is REJECTED, propagating the resolution note into the failure reason', async () => {
    const resumeHeldRun = jest.fn();
    const failHeldRun = jest.fn().mockResolvedValue({ id: pipelineRunId } as PipelineRunEntity);
    const listener = await buildListener({ resumeHeldRun, failHeldRun });

    const event = new RunApprovalDecisionEvent(
      approvalRequestId,
      pipelineRunId,
      ApprovalRequestStatus.REJECTED,
      resolvedByUserId,
      'violates production safety policy',
    );

    await listener.handleRunApprovalDecision(event);

    expect(resumeHeldRun).not.toHaveBeenCalled();
    expect(failHeldRun).toHaveBeenCalledTimes(1);
    const [failedRunId, reason] = failHeldRun.mock.calls[0];
    expect(failedRunId).toBe(pipelineRunId);
    expect(reason).toContain(approvalRequestId);
    expect(reason).toContain('violates production safety policy');
  });

  it('fails the held run with a reason even when no resolution note was given', async () => {
    const resumeHeldRun = jest.fn();
    const failHeldRun = jest.fn().mockResolvedValue({ id: pipelineRunId } as PipelineRunEntity);
    const listener = await buildListener({ resumeHeldRun, failHeldRun });

    const event = new RunApprovalDecisionEvent(
      approvalRequestId,
      pipelineRunId,
      ApprovalRequestStatus.REJECTED,
      resolvedByUserId,
    );

    await listener.handleRunApprovalDecision(event);

    expect(failHeldRun).toHaveBeenCalledTimes(1);
    const [, reason] = failHeldRun.mock.calls[0];
    expect(reason).toContain(approvalRequestId);
  });
});
