import { Injectable } from '@nestjs/common';

/**
 * A single injected secret's freshness bookkeeping: when it was fetched from the underlying
 * SecretProviderPort adapter, and when the tracker predicts it will expire.
 */
export interface SecretFreshnessRecord {
  /** The raw placeholder reference this record tracks, e.g.
   * `vault:kv/pipelines/load-test#apiKey` — same string SecretProviderPort.resolveSecret
   * receives, so it doubles as the natural lookup key. */
  secretReference: string;
  issuedAtEpochMillis: number;
  /** Predicted expiry. Neither VaultSecretProvider nor AwsSecretsManagerProvider currently
   * return a backend-reported TTL/lease duration (see NOTE in recordIssued), so this is
   * `issuedAtEpochMillis + assumedLifetimeMillis` — a conservative estimate, not a guarantee
   * from the secret backend itself. */
  expiresAtEpochMillis: number;
}

/** How long an injected secret is assumed to remain valid when the underlying
 * SecretProviderPort adapter doesn't report a real lease/TTL (true of both
 * VaultSecretProvider and AwsSecretsManagerProvider today — see resolveSecret in each). One
 * hour is deliberately conservative relative to typical Vault dynamic-secret leases (often
 * 30-60 minutes) so MidRunSecretRotationScheduler proactively refreshes well before a real
 * backend-side expiry could plausibly hit, at the cost of some unnecessary-but-harmless extra
 * re-fetches for secrets that actually live longer. */
export const DEFAULT_ASSUMED_SECRET_LIFETIME_MILLIS = 60 * 60 * 1000;

/** How long before predicted expiry a secret should be treated as due for proactive
 * re-fetch. Sized so a re-fetch attempt (and, if it fails, at least one retry via whatever
 * backoff MidRunSecretRotationScheduler's caller applies) can complete before the secret
 * actually goes stale mid-load-test. */
export const DEFAULT_ROTATION_LEAD_TIME_MILLIS = 5 * 60 * 1000;

/**
 * SecretFreshnessTracker
 *
 * Tracks issued-at/predicted-expires-at per injected secret reference, independent of which
 * SecretProviderPort adapter actually resolved it. Existing purely in-memory (per gateway
 * process) is intentional: freshness state is a cheap-to-recompute hint used to decide *when*
 * to re-fetch, not a source of truth for the secret's own validity — the backend (Vault/AWS
 * Secrets Manager) always remains authoritative on whether a value still works.
 *
 * This is what lets `MidRunSecretRotationScheduler` answer "which of the secrets injected into
 * this long-running load test are about to expire?" without every caller re-deriving expiry
 * math itself.
 */
@Injectable()
export class SecretFreshnessTracker {
  private readonly recordsBySecretReference = new Map<string, SecretFreshnessRecord>();

  constructor(private readonly assumedLifetimeMillis: number = DEFAULT_ASSUMED_SECRET_LIFETIME_MILLIS) {}

  /**
   * Records that `secretReference` was just (re-)issued at `issuedAtEpochMillis` (defaults to
   * now), predicting its expiry as `issuedAtEpochMillis + assumedLifetimeMillis`.
   *
   * NOTE: neither VaultSecretProvider nor AwsSecretsManagerProvider's resolveSecret() currently
   * surfaces a real lease duration (Vault's KV v2 response does carry `metadata` but not a
   * TTL for the read itself — Vault leases apply to *dynamic* secrets, not static KV reads;
   * AWS Secrets Manager's GetSecretValueCommand doesn't return an expiry either). If either
   * adapter is extended to report one, thread it through here as an explicit
   * `assumedLifetimeMillisOverride` argument rather than changing the class-wide default, so
   * different secret backends/paths can have different real lifetimes.
   */
  recordIssued(
    secretReference: string,
    issuedAtEpochMillis: number = Date.now(),
    assumedLifetimeMillisOverride?: number,
  ): SecretFreshnessRecord {
    const lifetimeMillis = assumedLifetimeMillisOverride ?? this.assumedLifetimeMillis;
    const record: SecretFreshnessRecord = {
      secretReference,
      issuedAtEpochMillis,
      expiresAtEpochMillis: issuedAtEpochMillis + lifetimeMillis,
    };
    this.recordsBySecretReference.set(secretReference, record);
    return record;
  }

  getRecord(secretReference: string): SecretFreshnessRecord | undefined {
    return this.recordsBySecretReference.get(secretReference);
  }

  /**
   * True once `nowEpochMillis` is within `DEFAULT_ROTATION_LEAD_TIME_MILLIS` of the tracked
   * secret's predicted expiry (or past it). A secret with no recorded issuance is reported as
   * due — an untracked secret must be treated as "refresh it before trusting it", not silently
   * skipped.
   */
  isDueForRotation(secretReference: string, nowEpochMillis: number = Date.now()): boolean {
    const record = this.recordsBySecretReference.get(secretReference);
    if (!record) return true;
    return nowEpochMillis >= record.expiresAtEpochMillis - DEFAULT_ROTATION_LEAD_TIME_MILLIS;
  }

  /**
   * Returns every tracked secret reference currently due for rotation per `isDueForRotation`.
   * This is the query `MidRunSecretRotationScheduler`'s periodic sweep calls to decide what to
   * re-fetch next.
   */
  listSecretReferencesDueForRotation(nowEpochMillis: number = Date.now()): string[] {
    const due: string[] = [];
    for (const [secretReference] of this.recordsBySecretReference) {
      if (this.isDueForRotation(secretReference, nowEpochMillis)) {
        due.push(secretReference);
      }
    }
    return due;
  }

  /** Stops tracking a secret reference entirely, e.g. once the pipeline run that injected it
   * has completed and the value no longer needs proactive rotation. */
  forget(secretReference: string): void {
    this.recordsBySecretReference.delete(secretReference);
  }
}
