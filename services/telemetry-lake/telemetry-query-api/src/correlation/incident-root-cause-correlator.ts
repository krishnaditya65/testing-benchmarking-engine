/**
 * incident-root-cause-correlator.ts
 *
 * The anomaly detectors in `../analytics/` answer "did this service's behavior change" — they
 * never explain *why*. This module is the next step in that investigation: given a detected
 * anomaly (a service + the timestamp it was flagged at), it asks `ClickHouseQueryRepository` for
 * every chaos-fault-injection, deploy, and config-drift event that happened near that timestamp
 * for that service, and ranks them as "likely cause" candidates.
 *
 * Ranking combines two signals:
 *
 * 1. **Temporal proximity** — an event that happened seconds before the anomaly is far more
 *    likely to be causal than one from 20 minutes earlier. Modeled as exponential decay with a
 *    half-life (`temporalDecayHalfLifeMs`): proximity score halves every half-life's worth of
 *    time-distance from the anomaly, in either direction (an event slightly *after* the anomaly
 *    timestamp is still considered, since ingestion/propagation delay means the recorded anomaly
 *    bucket and the recorded cause-event timestamp aren't perfectly synchronized).
 * 2. **Historical correlation strength** — not every event *kind* is equally likely to be
 *    causal just because it's nearby in time. An active chaos-fault injection overlapping an
 *    anomaly is, by the platform's own design (the fault was injected specifically to test
 *    blast radius), an extremely strong causal signal. A deploy immediately preceding a
 *    regression is a very common real-world root cause. A config-drift event is real signal but
 *    weaker on its own — plenty of config drift is benign. These are expressed as
 *    per-kind prior weights in `historicalCorrelationStrengthByKind`, multiplied into the
 *    proximity score to produce the final confidence.
 *
 * This is intentionally a transparent, tunable scoring function rather than a trained model —
 * consistent with this service's other analytics classes (`StatisticalBaselineDeviationDetector`,
 * `StreamingIsolationForestAnomalyDetector`), which are deterministic and explainable by design
 * so an on-call engineer can trust and audit *why* a candidate ranked where it did.
 */
import type {
  ChaosInjectionEventRecord,
  ClickHouseQueryRepository,
  ConfigDriftEventRecord,
  DeployEventRecord,
  TimeWindow,
} from "../repository/click-house-query-repository";

export interface DetectedAnomaly {
  serviceName: string;
  /** When the anomaly was flagged — typically an `AnomalyScore.bucketMinute` from either
   * `StatisticalBaselineDeviationDetector` or `StreamingIsolationForestAnomalyDetector`. */
  timestamp: Date;
}

export type RootCauseCandidateKind = "CHAOS_INJECTION" | "DEPLOY" | "CONFIG_DRIFT";

export interface RootCauseCandidate {
  kind: RootCauseCandidateKind;
  /** Human-readable summary for alerting/dashboards, e.g. "NETWORK_LATENCY fault (ACTIVE)
   * targeting checkout-api-7c9d" or "Deploy of checkout-api@2026.08.11-rc3". */
  description: string;
  eventTime: Date;
  /** Signed milliseconds from the candidate event to the anomaly (negative = event preceded the
   * anomaly, which is the common/expected case for a true root cause). */
  temporalDistanceMs: number;
  /** In [0, 1]; see module doc comment for how this is computed. Candidates are sorted
   * descending by this value. */
  confidenceScore: number;
  event: ChaosInjectionEventRecord | DeployEventRecord | ConfigDriftEventRecord;
}

export interface RootCauseCorrelationResult {
  anomaly: DetectedAnomaly;
  /** The wall-clock window actually queried around the anomaly (derived from
   * `RootCauseCorrelatorOptions`). */
  window: TimeWindow;
  /** Every candidate found, sorted descending by `confidenceScore`. Empty when no chaos/deploy/
   * config-drift event landed in the window — a legitimate, common result, not an error. */
  candidates: RootCauseCandidate[];
}

export interface RootCauseCorrelatorOptions {
  /** How far before the anomaly timestamp to look for candidate events. Generous by default
   * (15 minutes) because a root cause — especially a deploy — can precede its observable
   * latency/error impact by several minutes as traffic shifts onto the new version. */
  lookbackWindowMs: number;
  /** How far after the anomaly timestamp to still consider candidate events, to absorb
   * ingestion/aggregation lag between when an anomaly's underlying bucket occurred and when the
   * causal event's own telemetry was durably written. */
  lookaheadWindowMs: number;
  /** Half-life (milliseconds) for the temporal-proximity decay — see module doc comment. */
  temporalDecayHalfLifeMs: number;
  /** Per-kind prior weight in (0, 1], multiplied into the proximity score. See module doc
   * comment for the reasoning behind each default. */
  historicalCorrelationStrengthByKind: Record<RootCauseCandidateKind, number>;
}

export const DEFAULT_ROOT_CAUSE_CORRELATOR_OPTIONS: RootCauseCorrelatorOptions = {
  lookbackWindowMs: 15 * 60 * 1000,
  lookaheadWindowMs: 5 * 60 * 1000,
  temporalDecayHalfLifeMs: 5 * 60 * 1000,
  historicalCorrelationStrengthByKind: {
    CHAOS_INJECTION: 0.95,
    DEPLOY: 0.85,
    CONFIG_DRIFT: 0.6,
  },
};

/**
 * IncidentRootCauseCorrelator
 *
 * Reads exclusively through `ClickHouseQueryRepository` (`getRecentChaosInjectionEventsNear`,
 * `getRecentDeployEventsNear`, `getConfigDriftEventsNear`) — no direct ClickHouse access — so
 * this stays consistent with the rest of the service's "repository is the sole DB access point"
 * rule.
 */
export class IncidentRootCauseCorrelator {
  private readonly options: RootCauseCorrelatorOptions;

  constructor(
    private readonly repository: ClickHouseQueryRepository,
    options: Partial<RootCauseCorrelatorOptions> = {},
  ) {
    this.options = { ...DEFAULT_ROOT_CAUSE_CORRELATOR_OPTIONS, ...options };
  }

  async correlate(anomaly: DetectedAnomaly): Promise<RootCauseCorrelationResult> {
    const window: TimeWindow = {
      start: new Date(anomaly.timestamp.getTime() - this.options.lookbackWindowMs),
      end: new Date(anomaly.timestamp.getTime() + this.options.lookaheadWindowMs),
    };

    const [chaosEvents, deployEvents, configDriftEvents] = await Promise.all([
      this.repository.getRecentChaosInjectionEventsNear(anomaly.serviceName, window),
      this.repository.getRecentDeployEventsNear(anomaly.serviceName, window),
      this.repository.getConfigDriftEventsNear(anomaly.serviceName, window),
    ]);

    const candidates: RootCauseCandidate[] = [
      ...chaosEvents.map((event) =>
        this.buildCandidate(
          "CHAOS_INJECTION",
          event,
          event.eventTime,
          anomaly.timestamp,
          `${event.faultKind} fault (${event.lifecycleState}) targeting ${event.targetIdentifier}`,
        ),
      ),
      ...deployEvents.map((event) =>
        this.buildCandidate(
          "DEPLOY",
          event,
          event.eventTime,
          anomaly.timestamp,
          `Deploy of ${event.serviceName}@${event.deployedVersion}`,
        ),
      ),
      ...configDriftEvents.map((event) =>
        this.buildCandidate(
          "CONFIG_DRIFT",
          event,
          event.eventTime,
          anomaly.timestamp,
          `Config drift: ${event.configKey} changed from "${event.previousValue}" to "${event.newValue}"`,
        ),
      ),
    ];

    candidates.sort((a, b) => b.confidenceScore - a.confidenceScore);

    return { anomaly, window, candidates };
  }

  private buildCandidate(
    kind: RootCauseCandidateKind,
    event: ChaosInjectionEventRecord | DeployEventRecord | ConfigDriftEventRecord,
    eventTime: Date,
    anomalyTimestamp: Date,
    description: string,
  ): RootCauseCandidate {
    const temporalDistanceMs = eventTime.getTime() - anomalyTimestamp.getTime();
    const proximityScore = Math.pow(0.5, Math.abs(temporalDistanceMs) / this.options.temporalDecayHalfLifeMs);
    const priorWeight = this.options.historicalCorrelationStrengthByKind[kind];
    const confidenceScore = clamp01(proximityScore * priorWeight);
    return { kind, description, eventTime, temporalDistanceMs, confidenceScore, event };
  }
}

function clamp01(value: number): number {
  return Math.min(1, Math.max(0, value));
}
