import { v4 as uuidv4 } from 'uuid';
import { EnvironmentLifecycleStateMachine } from '../domain/environment-lifecycle-state-machine';
import { TerraformExecutionPort } from './terraform-execution.port';
import { CrossplaneCompositionPort } from './crossplane-composition.port';
import { EnvironmentRegistryPort } from './environment-registry.port';
import { PolicyVerificationGatePort } from './policy-verification-gate.port';

export type ProvisioningBackend = 'terraform' | 'crossplane';

export interface EphemeralEnvironmentRequest {
  readonly requestedBackend: ProvisioningBackend;
  readonly pipelineRunId: string;
  readonly terraform?: {
    readonly moduleSourcePath: string;
    readonly variables: Record<string, string | number | boolean>;
  };
  readonly crossplane?: {
    readonly compositionRef: string;
    readonly namespace: string;
    readonly parameters: Record<string, unknown>;
  };
}

export interface EphemeralEnvironmentHandle {
  readonly environmentId: string;
  readonly backend: ProvisioningBackend;
  readonly connectionOutputs: Record<string, string>;
  readonly state: ReturnType<EnvironmentLifecycleStateMachine['getCurrentState']>;
}

export class UnknownEnvironmentError extends Error {
  constructor(public readonly environmentId: string) {
    super(`No active environment found for environmentId "${environmentId}".`);
    this.name = 'UnknownEnvironmentError';
    Object.setPrototypeOf(this, UnknownEnvironmentError.prototype);
  }
}

/**
 * Thrown by beginTestExecution when the POLICY_VERIFYING gate returns a real failing verdict
 * (as opposed to the gate call itself erroring, which is a plain infra-error that drives the
 * environment to DESTROYED instead — see policy-verification-gate.port.ts's doc comment). The
 * environment is left in FAILED_POLICY_CHECK, not re-thrown as a generic Error, so callers can
 * distinguish "this environment is unusable for a structural/config reason" from other failure
 * kinds.
 */
export class PolicyVerificationFailedError extends Error {
  constructor(
    public readonly environmentId: string,
    public readonly verdict: import('./policy-verification-gate.port').PolicyVerificationVerdict,
  ) {
    const failingCheckNames = verdict.checks
      .filter((check) => !check.passed)
      .map((check) => check.checkName)
      .join(', ');
    super(
      `Environment "${environmentId}" failed its pre-test policy verification gate: [${failingCheckNames}]. ` +
        `See the environment's transition history for the FAILED_POLICY_CHECK record.`,
    );
    this.name = 'PolicyVerificationFailedError';
    Object.setPrototypeOf(this, PolicyVerificationFailedError.prototype);
  }
}

/**
 * Orchestrates the full lifecycle of an ephemeral test environment:
 * provisioning it through the requested infrastructure backend, tracking
 * its state via EnvironmentLifecycleStateMachine, handing it off for a
 * pipeline test run, and tearing it down afterward.
 */
export class EphemeralEnvironmentProvisioner {
  constructor(
    private readonly terraformExecutionPort: TerraformExecutionPort,
    private readonly crossplaneCompositionPort: CrossplaneCompositionPort,
    private readonly environmentRegistry: EnvironmentRegistryPort,
    private readonly policyVerificationGate: PolicyVerificationGatePort,
  ) {}

  async provisionEnvironment(
    request: EphemeralEnvironmentRequest,
  ): Promise<EphemeralEnvironmentHandle> {
    const environmentId = `env-${request.pipelineRunId}-${uuidv4()}`;
    const stateMachine = new EnvironmentLifecycleStateMachine(environmentId, 'PROVISIONING');
    this.environmentRegistry.register(stateMachine);

    try {
      const connectionOutputs = await this.dispatchProvisioning(environmentId, request);
      stateMachine.transitionTo('READY');
      return {
        environmentId,
        backend: request.requestedBackend,
        connectionOutputs,
        state: stateMachine.getCurrentState(),
      };
    } catch (provisioningError) {
      stateMachine.transitionTo('DESTROYED');
      throw provisioningError;
    }
  }

  /**
   * Drives environmentId from READY through POLICY_VERIFYING, calling out to
   * policyVerificationGate for a verdict, and lands on TEST_RUNNING (verdict passed) or
   * FAILED_POLICY_CHECK (verdict failed) — see environment-lifecycle-state-machine.ts and
   * policy-verification-gate.port.ts for why a direct READY -> TEST_RUNNING transition is no
   * longer legal. pipelineRunId/kubernetesNamespace are forwarded into the
   * PolicyVerificationRequest so the gate's mesh/drift/consistency checks are scoped to the
   * right namespace and attributable to the right pipeline run.
   *
   * If policyVerificationGate.verifyEnvironmentPolicies itself rejects (the underlying
   * service-mesh-policy-verifier / consistency-verification-worker call failed at the transport
   * level, not "checks ran and failed"), that is treated as an infra-error failure — the
   * environment transitions to DESTROYED, same as a Terraform apply failure — and the original
   * error is rethrown. If it resolves with a failing verdict, the environment transitions to
   * FAILED_POLICY_CHECK and PolicyVerificationFailedError is thrown instead.
   */
  async beginTestExecution(
    environmentId: string,
    pipelineRunId: string,
    kubernetesNamespace: string,
  ): Promise<void> {
    const stateMachine = this.requireStateMachine(environmentId);
    stateMachine.transitionTo('POLICY_VERIFYING');

    let verdict;
    try {
      verdict = await this.policyVerificationGate.verifyEnvironmentPolicies({
        environmentId,
        pipelineRunId,
        kubernetesNamespace,
      });
    } catch (policyGateError) {
      stateMachine.transitionTo('DESTROYED');
      throw policyGateError;
    }

    if (!verdict.passed) {
      stateMachine.transitionTo('FAILED_POLICY_CHECK');
      throw new PolicyVerificationFailedError(environmentId, verdict);
    }

    stateMachine.transitionTo('TEST_RUNNING');
  }

  async teardownEnvironment(environmentId: string, backend: ProvisioningBackend): Promise<void> {
    const stateMachine = this.requireStateMachine(environmentId);

    if (stateMachine.getCurrentState() !== 'TEARING_DOWN') {
      stateMachine.transitionTo('TEARING_DOWN');
    }

    if (backend === 'terraform') {
      await this.terraformExecutionPort.destroy(environmentId);
    } else {
      await this.crossplaneCompositionPort.deleteComposition(environmentId);
    }

    stateMachine.transitionTo('DESTROYED');
    this.environmentRegistry.remove(environmentId);
  }

  getEnvironmentState(
    environmentId: string,
  ): ReturnType<EnvironmentLifecycleStateMachine['getCurrentState']> {
    return this.requireStateMachine(environmentId).getCurrentState();
  }

  private async dispatchProvisioning(
    environmentId: string,
    request: EphemeralEnvironmentRequest,
  ): Promise<Record<string, string>> {
    if (request.requestedBackend === 'terraform') {
      if (!request.terraform) {
        throw new Error('Terraform provisioning backend requested but no terraform config supplied.');
      }
      const applyResult = await this.terraformExecutionPort.planAndApply({
        workspaceId: environmentId,
        moduleSourcePath: request.terraform.moduleSourcePath,
        variables: request.terraform.variables,
      });
      return applyResult.outputs;
    }

    if (!request.crossplane) {
      throw new Error('Crossplane provisioning backend requested but no crossplane config supplied.');
    }
    const applyResult = await this.crossplaneCompositionPort.applyComposition({
      claimName: environmentId,
      compositionRef: request.crossplane.compositionRef,
      namespace: request.crossplane.namespace,
      parameters: request.crossplane.parameters,
    });
    return applyResult.outputs;
  }

  private requireStateMachine(environmentId: string): EnvironmentLifecycleStateMachine {
    const stateMachine = this.environmentRegistry.get(environmentId);
    if (!stateMachine) {
      throw new UnknownEnvironmentError(environmentId);
    }
    return stateMachine;
  }
}
