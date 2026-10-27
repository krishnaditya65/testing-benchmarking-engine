import {
  DEFAULT_ASSUMED_SECRET_LIFETIME_MILLIS,
  DEFAULT_ROTATION_LEAD_TIME_MILLIS,
  SecretFreshnessTracker,
} from '../secret-injection/secret-freshness-tracker';

describe('SecretFreshnessTracker', () => {
  const secretReference = 'vault:kv/pipelines/load-test#apiKey';

  it('predicts expiry as issuedAt + assumed lifetime', () => {
    const tracker = new SecretFreshnessTracker();
    const issuedAt = 1_000_000;

    const record = tracker.recordIssued(secretReference, issuedAt);

    expect(record.issuedAtEpochMillis).toBe(issuedAt);
    expect(record.expiresAtEpochMillis).toBe(issuedAt + DEFAULT_ASSUMED_SECRET_LIFETIME_MILLIS);
    expect(tracker.getRecord(secretReference)).toEqual(record);
  });

  it('honors a per-call lifetime override without changing the tracker-wide default', () => {
    const tracker = new SecretFreshnessTracker();
    const issuedAt = 1_000_000;
    const shortLifetimeMillis = 10_000;

    const record = tracker.recordIssued(secretReference, issuedAt, shortLifetimeMillis);

    expect(record.expiresAtEpochMillis).toBe(issuedAt + shortLifetimeMillis);
  });

  it('reports a never-tracked secret reference as due for rotation', () => {
    const tracker = new SecretFreshnessTracker();
    expect(tracker.isDueForRotation('never-seen-reference')).toBe(true);
  });

  it('is NOT due for rotation comfortably before the lead-time window opens', () => {
    const tracker = new SecretFreshnessTracker();
    const issuedAt = 0;
    tracker.recordIssued(secretReference, issuedAt);

    // Well inside the assumed lifetime and outside the rotation lead-time window.
    const now = DEFAULT_ASSUMED_SECRET_LIFETIME_MILLIS - DEFAULT_ROTATION_LEAD_TIME_MILLIS - 60_000;

    expect(tracker.isDueForRotation(secretReference, now)).toBe(false);
  });

  it('becomes due for rotation once inside the lead-time window before predicted expiry', () => {
    const tracker = new SecretFreshnessTracker();
    const issuedAt = 0;
    tracker.recordIssued(secretReference, issuedAt);

    // Exactly at the boundary of the rotation lead-time window before predicted expiry.
    const atLeadTimeBoundary = DEFAULT_ASSUMED_SECRET_LIFETIME_MILLIS - DEFAULT_ROTATION_LEAD_TIME_MILLIS;

    expect(tracker.isDueForRotation(secretReference, atLeadTimeBoundary)).toBe(true);
  });

  it('remains due for rotation past predicted expiry', () => {
    const tracker = new SecretFreshnessTracker();
    const issuedAt = 0;
    tracker.recordIssued(secretReference, issuedAt);

    const wellPastExpiry = DEFAULT_ASSUMED_SECRET_LIFETIME_MILLIS + 60_000;

    expect(tracker.isDueForRotation(secretReference, wellPastExpiry)).toBe(true);
  });

  it('listSecretReferencesDueForRotation returns only the references currently due, mirroring a 6-hour load test with staggered secret issuance', () => {
    const tracker = new SecretFreshnessTracker();

    // Issued long enough ago that it is now due for rotation (simulates an hour-2 token in a
    // 6-hour test that was injected at hour 0).
    tracker.recordIssued('secret-issued-long-ago', 0);
    // Issued "just now" relative to `now`, so it is nowhere near its lead-time window.
    const now = DEFAULT_ASSUMED_SECRET_LIFETIME_MILLIS + 60_000;
    tracker.recordIssued('secret-issued-recently', now - 1_000);

    const due = tracker.listSecretReferencesDueForRotation(now);

    expect(due).toContain('secret-issued-long-ago');
    expect(due).not.toContain('secret-issued-recently');
  });

  it('forget() stops tracking a reference, which reports it as due again (untracked => must refresh before trusting)', () => {
    const tracker = new SecretFreshnessTracker();
    tracker.recordIssued(secretReference, Date.now());
    expect(tracker.isDueForRotation(secretReference)).toBe(false);

    tracker.forget(secretReference);

    expect(tracker.getRecord(secretReference)).toBeUndefined();
    expect(tracker.isDueForRotation(secretReference)).toBe(true);
  });
});
