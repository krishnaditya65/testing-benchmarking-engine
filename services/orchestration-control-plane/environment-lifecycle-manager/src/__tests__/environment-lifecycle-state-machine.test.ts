import {
  EnvironmentLifecycleStateMachine,
  InvalidEnvironmentLifecycleTransitionError,
} from '../domain/environment-lifecycle-state-machine';

describe('EnvironmentLifecycleStateMachine', () => {
  it('allows the happy-path lifecycle transitions, gated by POLICY_VERIFYING', () => {
    const stateMachine = new EnvironmentLifecycleStateMachine('env-1', 'PROVISIONING');

    stateMachine.transitionTo('READY');
    stateMachine.transitionTo('POLICY_VERIFYING');
    stateMachine.transitionTo('TEST_RUNNING');
    stateMachine.transitionTo('READY');
    stateMachine.transitionTo('TEARING_DOWN');
    stateMachine.transitionTo('DESTROYED');

    expect(stateMachine.getCurrentState()).toBe('DESTROYED');
    expect(stateMachine.getTransitionHistory()).toHaveLength(6);
    expect(stateMachine.isTerminal()).toBe(true);
  });

  it('allows the failure path from PROVISIONING straight to DESTROYED', () => {
    const stateMachine = new EnvironmentLifecycleStateMachine('env-2', 'PROVISIONING');
    stateMachine.transitionTo('DESTROYED');
    expect(stateMachine.getCurrentState()).toBe('DESTROYED');
  });

  it('rejects an illegal transition and records nothing', () => {
    const stateMachine = new EnvironmentLifecycleStateMachine('env-3', 'PROVISIONING');
    expect(() => stateMachine.transitionTo('TEST_RUNNING')).toThrow(
      InvalidEnvironmentLifecycleTransitionError,
    );
    expect(stateMachine.getTransitionHistory()).toHaveLength(0);
  });

  it('rejects any transition out of the terminal DESTROYED state', () => {
    const stateMachine = new EnvironmentLifecycleStateMachine('env-4', 'DESTROYED');
    expect(() => stateMachine.transitionTo('READY')).toThrow(
      InvalidEnvironmentLifecycleTransitionError,
    );
  });

  it('no longer allows READY to skip straight to TEST_RUNNING without POLICY_VERIFYING', () => {
    const stateMachine = new EnvironmentLifecycleStateMachine('env-policy-1', 'READY');
    expect(() => stateMachine.transitionTo('TEST_RUNNING')).toThrow(
      InvalidEnvironmentLifecycleTransitionError,
    );
  });

  it('transitions POLICY_VERIFYING to TEST_RUNNING when the policy gate passes', () => {
    const stateMachine = new EnvironmentLifecycleStateMachine('env-policy-2', 'READY');
    stateMachine.transitionTo('POLICY_VERIFYING');
    stateMachine.transitionTo('TEST_RUNNING');
    expect(stateMachine.getCurrentState()).toBe('TEST_RUNNING');
  });

  it('transitions POLICY_VERIFYING to the terminal FAILED_POLICY_CHECK state when the policy gate fails', () => {
    const stateMachine = new EnvironmentLifecycleStateMachine('env-policy-3', 'READY');
    stateMachine.transitionTo('POLICY_VERIFYING');
    stateMachine.transitionTo('FAILED_POLICY_CHECK');

    expect(stateMachine.getCurrentState()).toBe('FAILED_POLICY_CHECK');
    expect(stateMachine.isTerminal()).toBe(true);
  });

  it('rejects any transition out of the terminal FAILED_POLICY_CHECK state', () => {
    const stateMachine = new EnvironmentLifecycleStateMachine('env-policy-4', 'FAILED_POLICY_CHECK');
    expect(() => stateMachine.transitionTo('TEARING_DOWN')).toThrow(
      InvalidEnvironmentLifecycleTransitionError,
    );
  });

  it('allows POLICY_VERIFYING to fail straight to DESTROYED via the infra-error failure path', () => {
    const stateMachine = new EnvironmentLifecycleStateMachine('env-policy-5', 'READY');
    stateMachine.transitionTo('POLICY_VERIFYING');
    stateMachine.transitionTo('DESTROYED');
    expect(stateMachine.getCurrentState()).toBe('DESTROYED');
  });

  it('stamps each transition record with fromState, toState, and a timestamp', () => {
    const stateMachine = new EnvironmentLifecycleStateMachine('env-5', 'PROVISIONING');
    stateMachine.transitionTo('READY');
    const record = stateMachine.getTransitionHistory()[0];
    expect(record).toBeDefined();
    expect(record?.fromState).toBe('PROVISIONING');
    expect(record?.toState).toBe('READY');
    expect(record?.occurredAt).toBeInstanceOf(Date);
  });
});
