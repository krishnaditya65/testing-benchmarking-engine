/**
 * click-house-query-repository.ts
 *
 * Sole point of contact between this service and ClickHouse. Every other module (analytics,
 * controllers) reads telemetry through this repository rather than issuing raw SQL, so query
 * shapes stay centralized and reviewable, and so the rollup tables defined in
 * `clickhouse-schema-migrations` have exactly one consumer to keep in sync with.
 */
import type { ClickHouseClient } from "@clickhouse/client";
import { createClient } from "@clickhouse/client";
import type { ClickHouseConnectionConfig } from "../config/click-house-connection-config";

export interface TimeWindow {
  start: Date;
  end: Date;
}

export type LatencyPercentile = 50 | 90 | 95 | 99 | 99.9;

export interface PercentileLatencyPoint {
  bucketMinute: Date;
  percentile: LatencyPercentile;
  latencyMs: number;
}

export interface ErrorRateRollupPoint {
  bucketMinute: Date;
  totalCount: number;
  errorCount: number;
  errorRate: number;
}

/**
 * Structured platform events (chaos fault lifecycle transitions, deploys, config drift)
 * currently ride the same `telemetry.raw.v1` -> `raw_telemetry_events` path as everything else,
 * landing as `signal_kind = 'log'` rows with an `event_type` attribute distinguishing them from
 * ordinary application logs — see `redpanda-ingestion-pipeline/src/topic-definitions.ts`'s
 * `ChaosFaultInjectionEvent` for the chaos case. This keeps `IncidentRootCauseCorrelator`
 * (src/correlation/) reading through the exact same raw table and repository every other query
 * in this service uses, rather than requiring a parallel event store.
 */
export interface ChaosInjectionEventRecord {
  eventId: string;
  serviceName: string;
  dagNodeId: string;
  faultId: string;
  faultKind: string;
  lifecycleState: string;
  targetIdentifier: string;
  eventTime: Date;
}

export interface DeployEventRecord {
  eventId: string;
  serviceName: string;
  dagNodeId: string;
  deployedVersion: string;
  eventTime: Date;
}

export interface ConfigDriftEventRecord {
  eventId: string;
  serviceName: string;
  dagNodeId: string;
  configKey: string;
  previousValue: string;
  newValue: string;
  eventTime: Date;
}

export interface CrossServiceTraceSpan {
  traceId: string;
  spanId: string;
  serviceName: string;
  dagNodeId: string;
  durationMs: number | null;
  statusCode: "OK" | "ERROR" | "UNSET";
  eventTime: Date;
}

const PERCENTILE_COLUMN_INDEX: Record<LatencyPercentile, number> = {
  50: 0,
  90: 1,
  95: 2,
  99: 3,
  99.9: 4,
};

/**
 * ClickHouseQueryRepository
 *
 * Thin, typed wrapper over `@clickhouse/client` scoped to the read patterns
 * `telemetry-query-api` needs. Every method issues parameterized queries (never string
 * interpolation of caller input) against the rollup/raw tables from
 * `clickhouse-schema-migrations`.
 */
export class ClickHouseQueryRepository {
  private readonly client: ClickHouseClient;

  constructor(config: ClickHouseConnectionConfig, client?: ClickHouseClient) {
    this.client =
      client ??
      createClient({
        url: config.host,
        database: config.database,
        username: config.username,
        password: config.password,
        max_open_connections: config.maxOpenConnections,
      });
  }

  /**
   * Reads a single percentile's latency series from the pre-aggregated
   * `latency_percentile_rollup` table (see clickhouse-schema-migrations/003), merging quantile
   * states across minute buckets within the requested window.
   */
  async getPercentileLatency(
    serviceName: string,
    percentile: LatencyPercentile,
    window: TimeWindow,
    dagNodeId?: string,
  ): Promise<PercentileLatencyPoint[]> {
    const columnIndex = PERCENTILE_COLUMN_INDEX[percentile];
    const result = await this.client.query({
      query: `
        SELECT
          bucket_minute,
          quantilesMerge(0.5, 0.9, 0.95, 0.99, 0.999)(duration_quantiles)[${columnIndex + 1}] AS latency_ms
        FROM latency_percentile_rollup
        WHERE service_name = {serviceName:String}
          AND bucket_minute >= {windowStart:DateTime}
          AND bucket_minute < {windowEnd:DateTime}
          AND ({dagNodeId:String} = '' OR dag_node_id = {dagNodeId:String})
        GROUP BY bucket_minute
        ORDER BY bucket_minute
      `,
      query_params: {
        serviceName,
        windowStart: toClickHouseDateTime(window.start),
        windowEnd: toClickHouseDateTime(window.end),
        dagNodeId: dagNodeId ?? "",
      },
      format: "JSONEachRow",
    });
    const rows = await result.json<{ bucket_minute: string; latency_ms: number }>();
    return rows.map((row) => ({
      bucketMinute: new Date(row.bucket_minute),
      percentile,
      latencyMs: row.latency_ms,
    }));
  }

  /**
   * Reads error-rate-over-time from the pre-aggregated `error_rate_rollup` table (see
   * clickhouse-schema-migrations/004).
   */
  async getErrorRateRollup(
    serviceName: string,
    window: TimeWindow,
    dagNodeId?: string,
  ): Promise<ErrorRateRollupPoint[]> {
    const result = await this.client.query({
      query: `
        SELECT
          bucket_minute,
          sum(total_count) AS total_count,
          sum(error_count) AS error_count
        FROM error_rate_rollup
        WHERE service_name = {serviceName:String}
          AND bucket_minute >= {windowStart:DateTime}
          AND bucket_minute < {windowEnd:DateTime}
          AND ({dagNodeId:String} = '' OR dag_node_id = {dagNodeId:String})
        GROUP BY bucket_minute
        ORDER BY bucket_minute
      `,
      query_params: {
        serviceName,
        windowStart: toClickHouseDateTime(window.start),
        windowEnd: toClickHouseDateTime(window.end),
        dagNodeId: dagNodeId ?? "",
      },
      format: "JSONEachRow",
    });
    const rows = await result.json<{ bucket_minute: string; total_count: string; error_count: string }>();
    return rows.map((row) => {
      const totalCount = Number(row.total_count);
      const errorCount = Number(row.error_count);
      return {
        bucketMinute: new Date(row.bucket_minute),
        totalCount,
        errorCount,
        errorRate: totalCount === 0 ? 0 : errorCount / totalCount,
      };
    });
  }

  /**
   * Walks the raw table (clickhouse-schema-migrations/001) to reconstruct every span that
   * shares a trace id, ordered by start time, regardless of which service emitted it. Used to
   * render the cross-service waterfall view and to let the anomaly detector pull the exact
   * spans behind a flagged deviation.
   */
  async getCrossServiceTraceCorrelation(traceId: string): Promise<CrossServiceTraceSpan[]> {
    const result = await this.client.query({
      query: `
        SELECT
          trace_id,
          span_id,
          service_name,
          dag_node_id,
          duration_ms,
          status_code,
          event_time
        FROM raw_telemetry_events
        WHERE signal_kind = 'trace' AND trace_id = {traceId:String}
        ORDER BY event_time ASC
      `,
      query_params: { traceId },
      format: "JSONEachRow",
    });
    const rows = await result.json<{
      trace_id: string;
      span_id: string;
      service_name: string;
      dag_node_id: string;
      duration_ms: number | null;
      status_code: "OK" | "ERROR" | "UNSET";
      event_time: string;
    }>();
    return rows.map((row) => ({
      traceId: row.trace_id,
      spanId: row.span_id,
      serviceName: row.service_name,
      dagNodeId: row.dag_node_id,
      durationMs: row.duration_ms,
      statusCode: row.status_code,
      eventTime: new Date(row.event_time),
    }));
  }

  /**
   * Raw per-minute latency samples for a service/window, used by
   * StatisticalBaselineDeviationDetector to build/score against a historical distribution
   * (needs the individual bucketed values, not a single merged percentile).
   */
  async getLatencySeriesForBaseline(
    serviceName: string,
    window: TimeWindow,
  ): Promise<Array<{ bucketMinute: Date; p50: number; p99: number }>> {
    const result = await this.client.query({
      query: `
        SELECT
          bucket_minute,
          quantilesMerge(0.5, 0.99)(duration_quantiles) AS quantiles
        FROM latency_percentile_rollup
        WHERE service_name = {serviceName:String}
          AND bucket_minute >= {windowStart:DateTime}
          AND bucket_minute < {windowEnd:DateTime}
        GROUP BY bucket_minute
        ORDER BY bucket_minute
      `,
      query_params: {
        serviceName,
        windowStart: toClickHouseDateTime(window.start),
        windowEnd: toClickHouseDateTime(window.end),
      },
      format: "JSONEachRow",
    });
    const rows = await result.json<{ bucket_minute: string; quantiles: [number, number] }>();
    return rows.map((row) => ({
      bucketMinute: new Date(row.bucket_minute),
      p50: row.quantiles[0],
      p99: row.quantiles[1],
    }));
  }

  /**
   * Chaos-fault lifecycle events (`chaos.fault-injection-event.v1`, see
   * `redpanda-ingestion-pipeline/src/topic-definitions.ts`) that landed in `raw_telemetry_events`
   * for the given service within `window`. Feeds `IncidentRootCauseCorrelator`'s
   * "was a chaos experiment active around this anomaly" candidate.
   */
  async getRecentChaosInjectionEventsNear(serviceName: string, window: TimeWindow): Promise<ChaosInjectionEventRecord[]> {
    const result = await this.client.query({
      query: `
        SELECT
          event_id,
          service_name,
          dag_node_id,
          attributes['fault_id'] AS fault_id,
          attributes['fault_kind'] AS fault_kind,
          attributes['lifecycle_state'] AS lifecycle_state,
          attributes['target_identifier'] AS target_identifier,
          event_time
        FROM raw_telemetry_events
        WHERE signal_kind = 'log'
          AND attributes['event_type'] = 'chaos_fault_injection'
          AND service_name = {serviceName:String}
          AND event_time >= {windowStart:DateTime}
          AND event_time < {windowEnd:DateTime}
        ORDER BY event_time ASC
      `,
      query_params: {
        serviceName,
        windowStart: toClickHouseDateTime(window.start),
        windowEnd: toClickHouseDateTime(window.end),
      },
      format: "JSONEachRow",
    });
    const rows = await result.json<{
      event_id: string;
      service_name: string;
      dag_node_id: string;
      fault_id: string;
      fault_kind: string;
      lifecycle_state: string;
      target_identifier: string;
      event_time: string;
    }>();
    return rows.map((row) => ({
      eventId: row.event_id,
      serviceName: row.service_name,
      dagNodeId: row.dag_node_id,
      faultId: row.fault_id,
      faultKind: row.fault_kind,
      lifecycleState: row.lifecycle_state,
      targetIdentifier: row.target_identifier,
      eventTime: new Date(row.event_time),
    }));
  }

  /**
   * Deploy events (`dag-workflow-scheduler` recording a new version rollout, logged as
   * `event_type = 'deployment'`) for the given service within `window`. Feeds
   * `IncidentRootCauseCorrelator`'s "did a deploy just precede this anomaly" candidate — one of
   * the most common real-world root causes of a latency/error regression.
   */
  async getRecentDeployEventsNear(serviceName: string, window: TimeWindow): Promise<DeployEventRecord[]> {
    const result = await this.client.query({
      query: `
        SELECT
          event_id,
          service_name,
          dag_node_id,
          attributes['deployed_version'] AS deployed_version,
          event_time
        FROM raw_telemetry_events
        WHERE signal_kind = 'log'
          AND attributes['event_type'] = 'deployment'
          AND service_name = {serviceName:String}
          AND event_time >= {windowStart:DateTime}
          AND event_time < {windowEnd:DateTime}
        ORDER BY event_time ASC
      `,
      query_params: {
        serviceName,
        windowStart: toClickHouseDateTime(window.start),
        windowEnd: toClickHouseDateTime(window.end),
      },
      format: "JSONEachRow",
    });
    const rows = await result.json<{
      event_id: string;
      service_name: string;
      dag_node_id: string;
      deployed_version: string;
      event_time: string;
    }>();
    return rows.map((row) => ({
      eventId: row.event_id,
      serviceName: row.service_name,
      dagNodeId: row.dag_node_id,
      deployedVersion: row.deployed_version,
      eventTime: new Date(row.event_time),
    }));
  }

  /**
   * Config-drift events (a service's live runtime config diverging from its declared/expected
   * config, logged as `event_type = 'config_drift'`) for the given service within `window`.
   * Feeds `IncidentRootCauseCorrelator`'s "did an undeclared config change happen nearby"
   * candidate — a lower-confidence but still common root cause (e.g. a feature flag flipped
   * outside the normal deploy path).
   */
  async getConfigDriftEventsNear(serviceName: string, window: TimeWindow): Promise<ConfigDriftEventRecord[]> {
    const result = await this.client.query({
      query: `
        SELECT
          event_id,
          service_name,
          dag_node_id,
          attributes['config_key'] AS config_key,
          attributes['previous_value'] AS previous_value,
          attributes['new_value'] AS new_value,
          event_time
        FROM raw_telemetry_events
        WHERE signal_kind = 'log'
          AND attributes['event_type'] = 'config_drift'
          AND service_name = {serviceName:String}
          AND event_time >= {windowStart:DateTime}
          AND event_time < {windowEnd:DateTime}
        ORDER BY event_time ASC
      `,
      query_params: {
        serviceName,
        windowStart: toClickHouseDateTime(window.start),
        windowEnd: toClickHouseDateTime(window.end),
      },
      format: "JSONEachRow",
    });
    const rows = await result.json<{
      event_id: string;
      service_name: string;
      dag_node_id: string;
      config_key: string;
      previous_value: string;
      new_value: string;
      event_time: string;
    }>();
    return rows.map((row) => ({
      eventId: row.event_id,
      serviceName: row.service_name,
      dagNodeId: row.dag_node_id,
      configKey: row.config_key,
      previousValue: row.previous_value,
      newValue: row.new_value,
      eventTime: new Date(row.event_time),
    }));
  }

  async close(): Promise<void> {
    await this.client.close();
  }
}

function toClickHouseDateTime(date: Date): string {
  return date.toISOString().replace("T", " ").replace("Z", "");
}
