import { Redis } from 'ioredis';
import {
  NodeChildProcessCommandRunner,
  TerraformCliExecutionAdapter,
} from './provisioning/terraform-cli-execution.adapter';
import { CrossplaneKubectlExecutionAdapter } from './provisioning/crossplane-kubectl-execution.adapter';
import { InMemoryEnvironmentRegistry } from './provisioning/environment-registry.port';
import { EphemeralEnvironmentProvisioner } from './provisioning/ephemeral-environment-provisioner';
import { NotWiredPolicyVerificationGateAdapter } from './provisioning/policy-verification-gate.port';
import { RedisTestStateCheckpointStore } from './spot-scheduling/test-state-checkpoint-store.port';
import { CostAwareSpotInstanceScheduler } from './spot-scheduling/cost-aware-spot-instance-scheduler';

export interface EnvironmentLifecycleManagerConfig {
  readonly redisConnectionUrl: string;
}

export interface EnvironmentLifecycleManager {
  readonly provisioner: EphemeralEnvironmentProvisioner;
  readonly spotScheduler: CostAwareSpotInstanceScheduler;
  readonly redisClient: Redis;
}

/**
 * Composition root: wires the real adapters together via constructor
 * injection. This function performs no side effects at module load time —
 * it must be explicitly called by an entry point (e.g. an HTTP server
 * bootstrap or a test) to actually connect to Redis and become usable.
 */
export function createEnvironmentLifecycleManager(
  config: EnvironmentLifecycleManagerConfig,
): EnvironmentLifecycleManager {
  const redisClient = new Redis(config.redisConnectionUrl);

  const commandRunner = new NodeChildProcessCommandRunner();
  const terraformExecutionPort = new TerraformCliExecutionAdapter(commandRunner);
  const crossplaneCompositionPort = new CrossplaneKubectlExecutionAdapter(commandRunner);
  const environmentRegistry = new InMemoryEnvironmentRegistry();
  const policyVerificationGate = new NotWiredPolicyVerificationGateAdapter();

  const provisioner = new EphemeralEnvironmentProvisioner(
    terraformExecutionPort,
    crossplaneCompositionPort,
    environmentRegistry,
    policyVerificationGate,
  );

  const checkpointStore = new RedisTestStateCheckpointStore(redisClient);
  const spotScheduler = new CostAwareSpotInstanceScheduler(checkpointStore);

  return { provisioner, spotScheduler, redisClient };
}
