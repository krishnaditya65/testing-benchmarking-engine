import { PolicyBundle, PolicyEvaluationInput, PolicyEvaluationResult } from './policy-evaluation-result.types';

/** DI token for the active PolicyEvaluationPort implementation. */
export const POLICY_EVALUATION_PORT = Symbol('POLICY_EVALUATION_PORT');

/**
 * Hexagonal-architecture port for evaluating a pipeline definition against a
 * policy-as-code bundle, mirroring quality-gate's `TelemetryQueryApiClientPort`
 * and secret-injection's `SecretProviderPort` patterns: PolicyGateService
 * depends only on this interface, so the evaluation backend (an OPA
 * sidecar today; conceivably Cerbos, Kyverno-in-CI, or a hand-rolled rules
 * engine later) can change without touching call sites.
 */
export interface PolicyEvaluationPort {
  /**
   * Evaluates policyBundle's rules against input.dag (and input.runContext).
   * Implementations should throw only for genuine infrastructure failure
   * (the evaluator being unreachable) — a policy simply having violations is
   * a normal, non-exceptional PolicyEvaluationResult.
   */
  evaluate(input: PolicyEvaluationInput, policyBundle: PolicyBundle): Promise<PolicyEvaluationResult>;
}
