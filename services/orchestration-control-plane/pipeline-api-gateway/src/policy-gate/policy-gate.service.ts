import { ForbiddenException, Inject, Injectable, Logger } from '@nestjs/common';
import { ApprovalWorkflowService } from '../approval-workflow/approval-workflow.service';
import { ApprovalRequestEntity } from '../approval-workflow/approval-request.entity';
import { DEFAULT_RUN_GATE_POLICY_BUNDLE, PolicyEvaluationInput, PolicyViolation } from './policy-evaluation-result.types';
import { POLICY_EVALUATION_PORT, PolicyEvaluationPort } from './policy-evaluation.port';

/**
 * Outcome of PolicyGateService.evaluateBeforeRun, returned to
 * pipeline-run.service.ts so it can decide how to persist the run's status:
 *  - "ALLOWED": no BLOCKING or REQUIRES_APPROVAL violations; proceed to
 *    trigger the run against dag-workflow-scheduler as normal.
 *  - "BLOCKED": at least one BLOCKING violation; the caller must not create
 *    (or must fail) the run — PolicyGateService itself throws
 *    ForbiddenException for this case rather than returning it, since there
 *    is nothing a caller could sensibly do except reject the trigger.
 *  - "AWAITING_APPROVAL": no BLOCKING violations, but at least one
 *    REQUIRES_APPROVAL violation; approvalRequest is populated and the
 *    caller should persist the run as PipelineRunStatus.AWAITING_APPROVAL
 *    instead of triggering it.
 */
export type PolicyGateOutcome =
  | { verdict: 'ALLOWED' }
  | { verdict: 'AWAITING_APPROVAL'; approvalRequest: ApprovalRequestEntity };

/**
 * Orchestrates the policy-as-code quality/security gate end to end,
 * following the same shape as quality-gate.service.ts: fetch an evaluation
 * from an injected port, interpret the result, and take the appropriate
 * control-plane action. Where quality-gate acts *after* a run completes
 * (failing it on SLA breach), PolicyGateService acts *before* a run is ever
 * triggered — see pipeline-run.service.ts's triggerRun, which calls
 * evaluateBeforeRun as its first step.
 */
@Injectable()
export class PolicyGateService {
  private readonly logger = new Logger(PolicyGateService.name);

  constructor(
    @Inject(POLICY_EVALUATION_PORT)
    private readonly policyEvaluationPort: PolicyEvaluationPort,
    private readonly approvalWorkflowService: ApprovalWorkflowService,
  ) {}

  /**
   * Evaluates the default run-gate policy bundle against a pipeline
   * definition about to be run. `pipelineRunId` must already exist (created
   * by pipeline-run.service.ts as PipelineRunStatus.PENDING before this is
   * called) since an AWAITING_APPROVAL outcome needs a run id to attach the
   * ApprovalRequest to.
   *
   * Throws ForbiddenException if any violation is BLOCKING severity —
   * callers should let that propagate as the run-trigger failure, matching
   * how quality-gate.evaluator.ts's failures surface today.
   */
  async evaluateBeforeRun(pipelineRunId: string, input: PolicyEvaluationInput): Promise<PolicyGateOutcome> {
    const result = await this.policyEvaluationPort.evaluate(input, DEFAULT_RUN_GATE_POLICY_BUNDLE);

    const blockingViolations = result.violations.filter((violation) => violation.severity === 'BLOCKING');
    if (blockingViolations.length > 0) {
      const summary = summarizeViolations(blockingViolations);
      this.logger.warn(`Pipeline definition ${input.pipelineDefinitionId} blocked by policy gate: ${summary}`);
      throw new ForbiddenException(`Run blocked by policy: ${summary}`);
    }

    const approvalViolations = result.violations.filter((violation) => violation.severity === 'REQUIRES_APPROVAL');
    if (approvalViolations.length > 0) {
      const summary = summarizeViolations(approvalViolations);
      this.logger.log(
        `Pipeline definition ${input.pipelineDefinitionId} requires human approval before run ${pipelineRunId} can start: ${summary}`,
      );
      const approvalRequest = await this.approvalWorkflowService.createApprovalRequest(
        pipelineRunId,
        input.pipelineDefinitionId,
        input.runContext.requestedByUserId,
        approvalViolations,
      );
      return { verdict: 'AWAITING_APPROVAL', approvalRequest };
    }

    // WARNING-severity violations (if any) are logged but never block or
    // route to approval — they exist purely for visibility.
    const warnings = result.violations.filter((violation) => violation.severity === 'WARNING');
    if (warnings.length > 0) {
      this.logger.warn(`Pipeline definition ${input.pipelineDefinitionId} has non-blocking policy warnings: ${summarizeViolations(warnings)}`);
    }

    return { verdict: 'ALLOWED' };
  }
}

function summarizeViolations(violations: PolicyViolation[]): string {
  return violations.map((violation) => `${violation.policyId}/${violation.ruleName}: ${violation.message}`).join('; ');
}
