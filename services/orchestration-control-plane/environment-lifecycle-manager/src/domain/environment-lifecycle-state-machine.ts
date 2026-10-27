/**
 * Core domain state machine for an ephemeral test environment's lifecycle.
 *
 * A test environment is provisioned (via Terraform or Crossplane, see
 * src/provisioning/), becomes READY for a DAG pipeline run to execute
 * against, passes through POLICY_VERIFYING while
 * services/distributed-systems-validation's service-mesh-policy-verifier and
 * consistency-verification-worker checks run against it (see
 * src/provisioning/policy-verification-gate.port.ts), transitions to
 * TEST_RUNNING while a run is in progress, and is eventually torn down. Any
 * state can fail out to DESTROYED via the failure path if the underlying
 * infrastructure operation errors; POLICY_VERIFYING additionally has its own
 * dedicated terminal-ish failure state, FAILED_POLICY_CHECK, for when the
 * environment itself provisioned fine but did not pass its pre-test policy
 * gate (see FAILED_POLICY_CHECK's own doc comment for why this is not simply
 * routed to DESTROYED).
 */

export type EnvironmentLifecycleState =
  | 'PROVISIONING'
  | 'READY'
  | 'POLICY_VERIFYING'
  | 'TEST_RUNNING'
  | 'TEARING_DOWN'
  | 'DESTROYED'
  | 'FAILED_POLICY_CHECK';

export interface EnvironmentLifecycleTransitionRecord {
  readonly fromState: EnvironmentLifecycleState;
  readonly toState: EnvironmentLifecycleState;
  readonly occurredAt: Date;
}

/**
 * Allowed forward transitions for a healthy environment, plus a failure
 * path that lets PROVISIONING, READY, and TEST_RUNNING fail straight to
 * DESTROYED (e.g. terraform apply errors, spot preemption with no
 * recoverable checkpoint, kubectl apply rejection).
 *
 * READY no longer transitions directly to TEST_RUNNING: every environment
 * must pass through POLICY_VERIFYING first, so the
 * services/distributed-systems-validation policy gate (service-mesh-policy-
 * verifier's mesh/drift checks, consistency-verification-worker's ACID/
 * eventual-consistency checks) always runs before a DAG pipeline run is
 * allowed to execute against the environment — there is intentionally no
 * bypass. POLICY_VERIFYING itself can fail straight to DESTROYED via the
 * same infra-error failure path as PROVISIONING/READY/TEST_RUNNING (e.g. the
 * gate's own call to policy-verification-gate.port.ts errored rather than
 * returning a verdict), or, if the gate returned a real failing verdict, to
 * FAILED_POLICY_CHECK — a dedicated terminal state distinct from DESTROYED
 * so "the environment was fine but the policy gate said no" remains
 * distinguishable in history/metrics from "the infrastructure itself broke."
 */
const ALLOWED_TRANSITIONS: Readonly<
  Record<EnvironmentLifecycleState, readonly EnvironmentLifecycleState[]>
> = {
  PROVISIONING: ['READY', 'DESTROYED'],
  READY: ['POLICY_VERIFYING', 'TEARING_DOWN', 'DESTROYED'],
  POLICY_VERIFYING: ['TEST_RUNNING', 'FAILED_POLICY_CHECK', 'DESTROYED'],
  TEST_RUNNING: ['READY', 'TEARING_DOWN', 'DESTROYED'],
  TEARING_DOWN: ['DESTROYED'],
  DESTROYED: [],
  FAILED_POLICY_CHECK: [],
};

export class InvalidEnvironmentLifecycleTransitionError extends Error {
  constructor(
    public readonly environmentId: string,
    public readonly fromState: EnvironmentLifecycleState,
    public readonly attemptedToState: EnvironmentLifecycleState,
  ) {
    super(
      `Environment "${environmentId}" cannot transition from ${fromState} to ${attemptedToState}. ` +
        `Allowed next states from ${fromState}: [${ALLOWED_TRANSITIONS[fromState].join(', ') || 'none (terminal state)'}].`,
    );
    this.name = 'InvalidEnvironmentLifecycleTransitionError';
    Object.setPrototypeOf(this, InvalidEnvironmentLifecycleTransitionError.prototype);
  }
}

export class EnvironmentLifecycleStateMachine {
  private currentState: EnvironmentLifecycleState;
  private readonly transitionHistory: EnvironmentLifecycleTransitionRecord[] = [];

  constructor(
    public readonly environmentId: string,
    initialState: EnvironmentLifecycleState = 'PROVISIONING',
  ) {
    this.currentState = initialState;
  }

  getCurrentState(): EnvironmentLifecycleState {
    return this.currentState;
  }

  getTransitionHistory(): readonly EnvironmentLifecycleTransitionRecord[] {
    return this.transitionHistory;
  }

  isTerminal(): boolean {
    return ALLOWED_TRANSITIONS[this.currentState].length === 0;
  }

  transitionTo(nextState: EnvironmentLifecycleState): void {
    const permittedNextStates = ALLOWED_TRANSITIONS[this.currentState];
    if (!permittedNextStates.includes(nextState)) {
      throw new InvalidEnvironmentLifecycleTransitionError(
        this.environmentId,
        this.currentState,
        nextState,
      );
    }

    const record: EnvironmentLifecycleTransitionRecord = {
      fromState: this.currentState,
      toState: nextState,
      occurredAt: new Date(),
    };
    this.transitionHistory.push(record);
    this.currentState = nextState;
  }
}
