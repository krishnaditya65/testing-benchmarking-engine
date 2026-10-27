import {
  TestPhaseState,
  TestStateCheckpointStorePort,
} from './test-state-checkpoint-store.port';

/** Default checkpoint TTL: generously longer than any single load-phase is expected to run. */
const DEFAULT_CHECKPOINT_TTL_SECONDS = 3600;

export interface SpotInstanceCandidate {
  readonly instanceType: string;
  readonly pricePerHour: number;
  readonly interruptionRiskScore: number; // 0 (very unlikely to be reclaimed) - 1 (very likely)
}

export interface SpotCapacityRequest {
  readonly environmentId: string;
  readonly candidates: readonly SpotInstanceCandidate[];
  readonly maxPricePerHour: number;
  readonly maxAcceptableInterruptionRiskScore: number;
}

export interface SpotCapacityGrant {
  readonly environmentId: string;
  readonly grantedInstanceType: string;
  readonly pricePerHour: number;
  readonly interruptionRiskScore: number;
}

export class NoAcceptableSpotCandidateError extends Error {
  constructor(public readonly environmentId: string) {
    super(
      `No spot instance candidate for environment "${environmentId}" satisfied both the max price ` +
        `and max interruption risk constraints.`,
    );
    this.name = 'NoAcceptableSpotCandidateError';
    Object.setPrototypeOf(this, NoAcceptableSpotCandidateError.prototype);
  }
}

/**
 * Selects spot capacity for load-test execution workers with cost and
 * interruption-risk awareness, and checkpoints in-flight load-phase state
 * to Redis before every spot request so a preemption never loses test
 * progress (see ARCHITECTURE.md: "Load tests survive spot instance
 * preemption because worker state is checkpointed to Redis before each
 * load-phase transition").
 */
export class CostAwareSpotInstanceScheduler {
  constructor(private readonly checkpointStore: TestStateCheckpointStorePort) {}

  async checkpointBeforeSpotRequest(
    environmentId: string,
    testPhaseState: TestPhaseState,
    ttlSeconds: number = DEFAULT_CHECKPOINT_TTL_SECONDS,
  ): Promise<void> {
    await this.checkpointStore.writeCheckpoint(
      environmentId,
      testPhaseState.phaseName,
      testPhaseState,
      ttlSeconds,
    );
  }

  /**
   * Real cost-aware selection: filters candidates to those within budget
   * and below the interruption-risk threshold, then picks the cheapest of
   * the remaining candidates (ties broken by lowest interruption risk).
   */
  async requestSpotCapacity(request: SpotCapacityRequest): Promise<SpotCapacityGrant> {
    const acceptableCandidates = request.candidates.filter(
      (candidate) =>
        candidate.pricePerHour <= request.maxPricePerHour &&
        candidate.interruptionRiskScore <= request.maxAcceptableInterruptionRiskScore,
    );

    if (acceptableCandidates.length === 0) {
      throw new NoAcceptableSpotCandidateError(request.environmentId);
    }

    const bestCandidate = acceptableCandidates.reduce((currentBest, candidate) => {
      if (candidate.pricePerHour < currentBest.pricePerHour) {
        return candidate;
      }
      if (
        candidate.pricePerHour === currentBest.pricePerHour &&
        candidate.interruptionRiskScore < currentBest.interruptionRiskScore
      ) {
        return candidate;
      }
      return currentBest;
    });

    return {
      environmentId: request.environmentId,
      grantedInstanceType: bestCandidate.instanceType,
      pricePerHour: bestCandidate.pricePerHour,
      interruptionRiskScore: bestCandidate.interruptionRiskScore,
    };
  }

  async resumeFromCheckpointAfterPreemption(
    environmentId: string,
    phaseName: string,
  ): Promise<TestPhaseState | null> {
    return this.checkpointStore.readCheckpoint(environmentId, phaseName);
  }
}
