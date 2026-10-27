import { Inject, Injectable, Logger, Optional } from '@nestjs/common';
import { SECRET_PROVIDER, SecretProviderPort } from './secret-provider.port';
import { SecretFreshnessTracker } from './secret-freshness-tracker';

/** DI token for the active RotationNotificationPort implementation, mirroring the
 * SECRET_PROVIDER token pattern in secret-provider.port.ts. Registered in
 * secret-injection.module.ts. */
export const ROTATION_NOTIFICATION_PORT = Symbol('ROTATION_NOTIFICATION_PORT');

/**
 * Pushed out via RotationNotificationPort whenever MidRunSecretRotationScheduler proactively
 * re-fetches a secret ahead of its predicted expiry.
 */
export interface RotatedSecretNotification {
  /** Same reference string the original ${{secret:...}}} placeholder resolved from — lets a
   * subscriber match this notification back to whichever injected config field(s) it belongs
   * to without the gateway needing to track that mapping itself. */
  secretReference: string;
  /** The freshly re-fetched plaintext value. */
  refreshedValue: string;
  rotatedAtEpochMillis: number;
}

/**
 * RotationNotificationPort
 *
 * Hexagonal outbound port for telling execution-fleet workers holding an injected secret (e.g.
 * an API key baked into a 6-hour load test's request headers) that a fresher value is
 * available, without SecretInjectionModule needing to know *how* that fan-out happens.
 *
 * INTERFACE ONLY in this hardening pass — see
 * docs/architecture-decision-records/0011-cross-cutting-robustness-hardening.md. A real
 * implementation would most plausibly publish onto a Redpanda topic (following this platform's
 * "control plane -> workers" communication contract in ARCHITECTURE.md, the same pattern
 * dag-workflow-scheduler uses for dag.node.dispatch.v1) keyed by pipelineRunId so a worker
 * already mid-run for that run can pick up the refreshed value; that concrete adapter is not
 * built here.
 */
export interface RotationNotificationPort {
  /** Delivers a single rotated-secret notification. Implementations should be safe to call
   * without a caller awaiting completion (MidRunSecretRotationScheduler does not block its
   * sweep loop on delivery), but must not throw synchronously for an already-rejected promise —
   * return a rejected Promise instead, matching every other async port in this module. */
  publishRotatedSecret(notification: RotatedSecretNotification): Promise<void>;
}

export interface MidRunSecretRotationSchedulerOptions {
  /** How often the proactive-rotation sweep runs. Defaults to a fraction of
   * DEFAULT_ROTATION_LEAD_TIME_MILLIS (secret-freshness-tracker.ts) so a secret becoming due
   * for rotation is very unlikely to sit unnoticed for the entire lead-time window before the
   * next sweep picks it up. */
  sweepIntervalMillis: number;
}

const DEFAULT_SWEEP_INTERVAL_MILLIS = 60 * 1000;

/**
 * MidRunSecretRotationScheduler
 *
 * Proactively re-fetches a secret from its underlying SecretProviderPort adapter before
 * SecretFreshnessTracker predicts it will expire, so a long-running load test doesn't fail
 * partway through because a token that was fine at injection time (e.g. hour 0) has since
 * expired (e.g. hour 2 of a 6-hour test) while the test itself is still running.
 *
 * Deliberately depends only on SecretProviderPort (never a concrete Vault/AWS adapter) and
 * RotationNotificationPort (never a concrete transport), mirroring the hexagonal shape
 * secret-injection.service.ts already establishes for this module.
 */
@Injectable()
export class MidRunSecretRotationScheduler {
  private readonly logger = new Logger(MidRunSecretRotationScheduler.name);
  private readonly options: MidRunSecretRotationSchedulerOptions;
  private sweepIntervalHandle: ReturnType<typeof setInterval> | undefined;

  constructor(
    @Inject(SECRET_PROVIDER) private readonly secretProvider: SecretProviderPort,
    private readonly freshnessTracker: SecretFreshnessTracker,
    @Inject(ROTATION_NOTIFICATION_PORT) private readonly rotationNotificationPort: RotationNotificationPort,
    @Optional() options: Partial<MidRunSecretRotationSchedulerOptions> = {},
  ) {
    this.options = { sweepIntervalMillis: DEFAULT_SWEEP_INTERVAL_MILLIS, ...options };
  }

  /** Starts the periodic sweep. Idempotent — calling start() while already running is a no-op
   * rather than leaking a second interval timer. */
  start(): void {
    if (this.sweepIntervalHandle) return;
    this.sweepIntervalHandle = setInterval(() => {
      void this.rotateDueSecrets();
    }, this.options.sweepIntervalMillis);
  }

  /** Stops the periodic sweep. Callers should invoke this during graceful shutdown so the
   * gateway process can exit without a dangling timer. */
  stop(): void {
    if (!this.sweepIntervalHandle) return;
    clearInterval(this.sweepIntervalHandle);
    this.sweepIntervalHandle = undefined;
  }

  /**
   * Re-fetches every secret reference SecretFreshnessTracker currently reports as due for
   * rotation, records the fresh issuance, and notifies RotationNotificationPort. Exposed as a
   * standalone method (rather than only running inside the setInterval callback) so it can be
   * invoked directly and awaited — both by tests and by any caller wanting a synchronous "check
   * now" rotation pass instead of waiting for the next scheduled sweep.
   *
   * A failure re-fetching or notifying about any single secret reference is logged and does not
   * abort the sweep for the remaining due references — one expired/unreachable secret shouldn't
   * block proactive rotation of every other secret in flight for the same (or another) run.
   */
  async rotateDueSecrets(nowEpochMillis: number = Date.now()): Promise<RotatedSecretNotification[]> {
    const dueSecretReferences = this.freshnessTracker.listSecretReferencesDueForRotation(nowEpochMillis);
    const rotated: RotatedSecretNotification[] = [];

    for (const secretReference of dueSecretReferences) {
      try {
        const refreshedValue = await this.secretProvider.resolveSecret(secretReference);
        this.freshnessTracker.recordIssued(secretReference, nowEpochMillis);

        const notification: RotatedSecretNotification = {
          secretReference,
          refreshedValue,
          rotatedAtEpochMillis: nowEpochMillis,
        };
        await this.rotationNotificationPort.publishRotatedSecret(notification);
        rotated.push(notification);
      } catch (error) {
        this.logger.error(
          `Mid-run rotation failed for secret reference "${secretReference}": ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      }
    }

    return rotated;
  }
}
