import { CostAwareSpotInstanceScheduler, NoAcceptableSpotCandidateError } from '../spot-scheduling/cost-aware-spot-instance-scheduler';
import { TestPhaseState, TestStateCheckpointStorePort } from '../spot-scheduling/test-state-checkpoint-store.port';

class FakeCheckpointStore implements TestStateCheckpointStorePort {
  public readonly writes: Array<{ environmentId: string; phaseName: string; ttlSeconds: number }> = [];
  private readonly checkpoints = new Map<string, TestPhaseState>();

  async writeCheckpoint(
    environmentId: string,
    phaseName: string,
    phaseState: TestPhaseState,
    ttlSeconds: number,
  ): Promise<void> {
    this.writes.push({ environmentId, phaseName, ttlSeconds });
    this.checkpoints.set(`${environmentId}:${phaseName}`, phaseState);
  }

  async readCheckpoint(environmentId: string, phaseName: string): Promise<TestPhaseState | null> {
    return this.checkpoints.get(`${environmentId}:${phaseName}`) ?? null;
  }
}

describe('CostAwareSpotInstanceScheduler', () => {
  it('picks the cheapest candidate that satisfies price and risk constraints', async () => {
    const scheduler = new CostAwareSpotInstanceScheduler(new FakeCheckpointStore());

    const grant = await scheduler.requestSpotCapacity({
      environmentId: 'env-1',
      maxPricePerHour: 1.0,
      maxAcceptableInterruptionRiskScore: 0.5,
      candidates: [
        { instanceType: 'c5.xlarge', pricePerHour: 0.4, interruptionRiskScore: 0.6 }, // too risky
        { instanceType: 'm5.large', pricePerHour: 0.9, interruptionRiskScore: 0.2 },
        { instanceType: 'm5.xlarge', pricePerHour: 0.85, interruptionRiskScore: 0.3 },
        { instanceType: 'r5.large', pricePerHour: 1.5, interruptionRiskScore: 0.1 }, // too expensive
      ],
    });

    expect(grant.grantedInstanceType).toBe('m5.xlarge');
  });

  it('throws when no candidate satisfies both constraints', async () => {
    const scheduler = new CostAwareSpotInstanceScheduler(new FakeCheckpointStore());
    await expect(
      scheduler.requestSpotCapacity({
        environmentId: 'env-2',
        maxPricePerHour: 0.1,
        maxAcceptableInterruptionRiskScore: 0.1,
        candidates: [{ instanceType: 'c5.xlarge', pricePerHour: 0.4, interruptionRiskScore: 0.6 }],
      }),
    ).rejects.toThrow(NoAcceptableSpotCandidateError);
  });

  it('checkpoints phase state before requesting spot capacity with a TTL', async () => {
    const checkpointStore = new FakeCheckpointStore();
    const scheduler = new CostAwareSpotInstanceScheduler(checkpointStore);

    await scheduler.checkpointBeforeSpotRequest('env-3', {
      environmentId: 'env-3',
      phaseName: 'ramp-up',
      completedRequestCount: 42,
      checkpointedAtIso: new Date().toISOString(),
      customPayload: {},
    });

    expect(checkpointStore.writes).toHaveLength(1);
    expect(checkpointStore.writes[0]?.ttlSeconds).toBeGreaterThan(0);

    const resumed = await scheduler.resumeFromCheckpointAfterPreemption('env-3', 'ramp-up');
    expect(resumed?.completedRequestCount).toBe(42);
  });
});
