import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { ApprovalWorkflowModule } from '../approval-workflow/approval-workflow.module';
import { OpaRegoPolicyEvaluatorAdapter } from './opa-rego-policy-evaluator.adapter';
import { POLICY_EVALUATION_PORT } from './policy-evaluation.port';
import { PolicyGateService } from './policy-gate.service';

/**
 * Wires the policy-as-code quality/security gate: the OPA sidecar HTTP
 * adapter behind PolicyEvaluationPort, and the orchestrating
 * PolicyGateService that creates an ApprovalWorkflowModule request (rather
 * than hard-failing) on REQUIRES_APPROVAL violations. Mirrors
 * quality-gate.module.ts's shape.
 */
@Module({
  imports: [ConfigModule, ApprovalWorkflowModule],
  providers: [PolicyGateService, { provide: POLICY_EVALUATION_PORT, useClass: OpaRegoPolicyEvaluatorAdapter }],
  exports: [PolicyGateService],
})
export class PolicyGateModule {}
