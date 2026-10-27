import { Redis } from 'ioredis';

/**
 * Snapshot of an in-progress load-test phase, captured immediately before
 * requesting spot capacity so it can be resumed on another worker if the
 * spot instance running it is preempted (see ARCHITECTURE.md's "Durability
 * by design" section).
 */
export interface TestPhaseState {
  readonly environmentId: string;
  readonly phaseName: string;
  readonly completedRequestCount: number;
  readonly checkpointedAtIso: string;
  readonly customPayload: Record<string, unknown>;
}

export interface TestStateCheckpointStorePort {
  writeCheckpoint(
    environmentId: string,
    phaseName: string,
    phaseState: TestPhaseState,
    ttlSeconds: number,
  ): Promise<void>;
  readCheckpoint(environmentId: string, phaseName: string): Promise<TestPhaseState | null>;
}

function checkpointRedisKey(environmentId: string, phaseName: string): string {
  return `env-lifecycle:checkpoint:${environmentId}:${phaseName}`;
}

/**
 * Redis-backed checkpoint store, matching docker-compose's
 * `coordinator-state-redis` service. Worker state for a load-test phase is
 * checkpointed here before each spot capacity request so it survives spot
 * instance preemption.
 */
export class RedisTestStateCheckpointStore implements TestStateCheckpointStorePort {
  constructor(private readonly redisClient: Redis) {}

  async writeCheckpoint(
    environmentId: string,
    phaseName: string,
    phaseState: TestPhaseState,
    ttlSeconds: number,
  ): Promise<void> {
    const key = checkpointRedisKey(environmentId, phaseName);
    const serializedPhaseState = JSON.stringify(phaseState);
    await this.redisClient.set(key, serializedPhaseState, 'EX', ttlSeconds);
  }

  async readCheckpoint(environmentId: string, phaseName: string): Promise<TestPhaseState | null> {
    const key = checkpointRedisKey(environmentId, phaseName);
    const serializedPhaseState = await this.redisClient.get(key);
    if (serializedPhaseState === null) {
      return null;
    }
    return JSON.parse(serializedPhaseState) as TestPhaseState;
  }
}
