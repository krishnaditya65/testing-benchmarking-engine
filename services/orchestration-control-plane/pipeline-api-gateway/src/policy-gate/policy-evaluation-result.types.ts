import { PipelineDag } from '../test-definition/dag-domain-types';

/**
 * Severity of one policy violation, mirroring how the example Rego
 * policies (`example-policies/*.rego`) tag their `deny`/`require_approval`
 * rules. `BLOCKING` hard-fails the run trigger outright; `REQUIRES_APPROVAL`
 * routes the run into approval-workflow instead of failing it (see
 * PolicyGateService and src/approval-workflow/); `WARNING` is recorded but
 * never stops a run.
 */
export type PolicyViolationSeverity = 'BLOCKING' | 'REQUIRES_APPROVAL' | 'WARNING';

/** One rule violation returned by a policy bundle evaluation. */
export interface PolicyViolation {
  /** e.g. "no-chaos-injection-without-approval" — matches the .rego file's package/rule name. */
  policyId: string;
  ruleName: string;
  severity: PolicyViolationSeverity;
  message: string;
  /** Node id(s) implicated in the violation, if any, for canvas-side highlighting. */
  nodeIds: string[];
}

/** Identifies which set of Rego policies to evaluate a pipeline definition against. */
export interface PolicyBundle {
  bundleName: string;
  /** OPA package paths under services/orchestration-control-plane/pipeline-api-gateway/src/policy-gate/example-policies,
   * e.g. "testforge.policies.no_chaos_injection_without_approval". */
  policyPackages: string[];
}

/** Default bundle evaluated before every pipeline run trigger; see PolicyGateService. */
export const DEFAULT_RUN_GATE_POLICY_BUNDLE: PolicyBundle = {
  bundleName: 'default-run-gate',
  policyPackages: [
    'testforge.policies.no_chaos_injection_without_approval',
    'testforge.policies.production_namespace_requires_teardown_node',
  ],
};

/** Full result of evaluating a PolicyBundle against a PipelineDag. */
export interface PolicyEvaluationResult {
  /** True only when there are zero BLOCKING violations. REQUIRES_APPROVAL and WARNING
   * violations do not affect this flag — PolicyGateService decides what to do with those. */
  allowed: boolean;
  violations: PolicyViolation[];
}

/** Input handed to a PolicyEvaluationPort implementation. */
export interface PolicyEvaluationInput {
  pipelineDefinitionId: string;
  dag: PipelineDag;
  /** Free-form context a policy might condition on (e.g. target environment/namespace),
   * since that information does not live on PipelineDag itself. */
  runContext: {
    targetNamespace?: string;
    requestedByUserId: string;
  };
}
