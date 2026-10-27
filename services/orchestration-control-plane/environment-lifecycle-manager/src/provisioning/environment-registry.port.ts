import { EnvironmentLifecycleStateMachine } from '../domain/environment-lifecycle-state-machine';

/**
 * Tracks the set of environments currently owned by this process, keyed by
 * environmentId. Kept behind an interface so the in-memory implementation
 * used by the starter repo can later be swapped for a shared/distributed
 * registry without touching EphemeralEnvironmentProvisioner.
 */
export interface EnvironmentRegistryPort {
  register(stateMachine: EnvironmentLifecycleStateMachine): void;
  get(environmentId: string): EnvironmentLifecycleStateMachine | undefined;
  remove(environmentId: string): void;
  listActiveEnvironmentIds(): string[];
}

export class InMemoryEnvironmentRegistry implements EnvironmentRegistryPort {
  private readonly stateMachinesByEnvironmentId = new Map<string, EnvironmentLifecycleStateMachine>();

  register(stateMachine: EnvironmentLifecycleStateMachine): void {
    this.stateMachinesByEnvironmentId.set(stateMachine.environmentId, stateMachine);
  }

  get(environmentId: string): EnvironmentLifecycleStateMachine | undefined {
    return this.stateMachinesByEnvironmentId.get(environmentId);
  }

  remove(environmentId: string): void {
    this.stateMachinesByEnvironmentId.delete(environmentId);
  }

  listActiveEnvironmentIds(): string[] {
    return Array.from(this.stateMachinesByEnvironmentId.keys());
  }
}
