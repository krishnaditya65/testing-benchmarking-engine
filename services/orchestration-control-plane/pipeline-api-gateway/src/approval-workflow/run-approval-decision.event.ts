/**
 * Domain event published on NestJS's built-in `EventEmitter2` (via
 * `@nestjs/event-emitter`) whenever a human resolves an ApprovalRequest.
 *
 * WHY AN EVENT INSTEAD OF A DIRECT SERVICE CALL: resuming (or failing) the
 * gated PipelineRunEntity once a request is approved/rejected requires
 * calling into PipelineRunModule. ApprovalWorkflowModule cannot import
 * PipelineRunModule directly — PipelineRunModule already depends on
 * PolicyGateModule (via triggerRun's policy-gate-before-dispatch check),
 * which itself depends on ApprovalWorkflowModule (so PolicyGateService can
 * create an approval request instead of hard-failing a run trigger). A
 * direct `ApprovalWorkflowModule -> PipelineRunModule` import would close
 * that into a cycle:
 *
 *   PipelineRunModule -> PolicyGateModule -> ApprovalWorkflowModule -> PipelineRunModule
 *
 * Publishing this event from ApprovalWorkflowService and having
 * PipelineRunModule's RunApprovalEventListener (see
 * ../pipeline-run/run-approval-event.listener.ts) subscribe to it keeps the
 * module graph acyclic: ApprovalWorkflowModule never needs to know
 * PipelineRunModule exists, it only ever emits onto the process-wide
 * EventEmitter2 bus that `EventEmitterModule.forRoot()` (registered in
 * app.module.ts) makes available everywhere without either module
 * importing the other.
 */
import { ApprovalRequestStatus } from './approval-request.entity';

export const RUN_APPROVAL_DECISION_EVENT = 'run-approval.decision';

/** Terminal resolutions only — RunApprovalDecisionEvent is never emitted for PENDING. */
export type RunApprovalDecision = ApprovalRequestStatus.APPROVED | ApprovalRequestStatus.REJECTED;

export class RunApprovalDecisionEvent {
  constructor(
    /** The ApprovalRequestEntity this decision resolves. */
    public readonly approvalRequestId: string,
    /** The PipelineRunEntity this approval request was gating. */
    public readonly pipelineRunId: string,
    public readonly decision: RunApprovalDecision,
    public readonly resolvedByUserId: string,
    public readonly resolutionNote?: string,
  ) {}
}
