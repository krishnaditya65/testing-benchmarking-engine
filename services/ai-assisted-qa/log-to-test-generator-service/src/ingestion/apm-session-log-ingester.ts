/**
 * apm-session-log-ingester.ts
 *
 * Reads structured APM/session-telemetry log entries that back "Log-to-Test Generation":
 * mining real production traffic (and crash sequences) so `frequent-traffic-path-miner.ts` and
 * `crash-sequence-extractor.ts` have something to mine. Modeled as reading from the
 * telemetry-lake's ClickHouse instance — the same physical store `telemetry-query-api` reads
 * from (`raw_telemetry_events`, see that service's `ClickHouseQueryRepository`) — but through
 * this service's own narrower port, since this service only needs request-path/method/status/
 * timing/session-id, not the full percentile/error-rate rollup surface telemetry-query-api
 * exposes.
 */

export interface ApmSessionLogEntry {
  sessionId: string;
  traceId: string;
  requestPath: string;
  httpMethod: string;
  statusCode: number;
  durationMs: number;
  /** Millisecond epoch timestamp; sequences are ordered by this field within a session. */
  occurredAtEpochMs: number;
  serviceName: string;
}

export interface ApmLogQueryWindow {
  startEpochMs: number;
  endEpochMs: number;
}

/**
 * ApmSessionLogSourcePort
 *
 * Hexagonal port `log-to-test-generator-service` mines against. Kept intentionally narrow (one
 * method, one filter shape) so an in-memory fixture adapter (used by
 * `frequent-traffic-path-miner.spec.ts`/`crash-sequence-extractor.spec.ts`) and a real
 * ClickHouse-backed adapter can both satisfy it without either implementation needing to know
 * about the other.
 */
export interface ApmSessionLogSourcePort {
  fetchSessionLogEntries(window: ApmLogQueryWindow, serviceName?: string): Promise<ApmSessionLogEntry[]>;
}

export interface ClickHouseApmLogClient {
  /** Narrow shape of the subset of `@clickhouse/client` this adapter needs — declared locally
   * rather than taking a dependency on `@clickhouse/client` directly, since this service (unlike
   * `telemetry-lake/telemetry-query-api`) does not otherwise need a live ClickHouse connection
   * to run its unit tests; a real deployment wires a real `@clickhouse/client` instance in here. */
  query(sql: string, params: Record<string, unknown>): Promise<{ json(): Promise<Record<string, unknown>[]> }>;
}

/**
 * ClickHouseApmSessionLogAdapter
 *
 * Reads `raw_telemetry_events` (the same table `telemetry-query-api`'s
 * `ClickHouseQueryRepository.getCrossServiceTraceCorrelation` reads, per
 * `clickhouse-schema-migrations` migration 001) filtered to HTTP request spans, projected down
 * to the fields path-mining needs. The query is stubbed as a template string here — this
 * service does not carry a live ClickHouse client dependency; wiring a real
 * `ClickHouseApmLogClient` (backed by `@clickhouse/client`, mirroring
 * `telemetry-query-api`'s `click-house-query-repository.ts`) is a deployment-time concern.
 */
export class ClickHouseApmSessionLogAdapter implements ApmSessionLogSourcePort {
  constructor(private readonly client: ClickHouseApmLogClient) {}

  async fetchSessionLogEntries(window: ApmLogQueryWindow, serviceName?: string): Promise<ApmSessionLogEntry[]> {
    const sql = `
      SELECT
        session_id      AS sessionId,
        trace_id        AS traceId,
        request_path    AS requestPath,
        http_method     AS httpMethod,
        status_code     AS statusCode,
        duration_ms     AS durationMs,
        toUnixTimestamp64Milli(event_time) AS occurredAtEpochMs,
        service_name    AS serviceName
      FROM raw_telemetry_events
      WHERE event_time >= fromUnixTimestamp64Milli({startEpochMs:Int64})
        AND event_time <  fromUnixTimestamp64Milli({endEpochMs:Int64})
        ${serviceName ? "AND service_name = {serviceName:String}" : ""}
        AND span_kind = 'SERVER'
      ORDER BY session_id, event_time ASC
    `;

    const resultSet = await this.client.query(sql, {
      startEpochMs: window.startEpochMs,
      endEpochMs: window.endEpochMs,
      ...(serviceName ? { serviceName } : {}),
    });
    const rows = await resultSet.json();
    return rows.map(toApmSessionLogEntry);
  }
}

function toApmSessionLogEntry(row: Record<string, unknown>): ApmSessionLogEntry {
  return {
    sessionId: String(row.sessionId),
    traceId: String(row.traceId),
    requestPath: String(row.requestPath),
    httpMethod: String(row.httpMethod),
    statusCode: Number(row.statusCode),
    durationMs: Number(row.durationMs),
    occurredAtEpochMs: Number(row.occurredAtEpochMs),
    serviceName: String(row.serviceName),
  };
}

/**
 * InMemoryApmSessionLogAdapter
 *
 * Test/fixture adapter satisfying the same port from a plain array — used by this service's own
 * unit tests (see `pathmining/*.spec.ts`) so path-mining logic can be tested without a
 * ClickHouse instance, the same way `dag-domain-model`'s `DagStructuralValidator` tests run
 * against plain in-memory `PipelineDefinition` fixtures rather than a live gateway.
 */
export class InMemoryApmSessionLogAdapter implements ApmSessionLogSourcePort {
  constructor(private readonly entries: ApmSessionLogEntry[]) {}

  async fetchSessionLogEntries(window: ApmLogQueryWindow, serviceName?: string): Promise<ApmSessionLogEntry[]> {
    return this.entries.filter(
      (entry) =>
        entry.occurredAtEpochMs >= window.startEpochMs &&
        entry.occurredAtEpochMs < window.endEpochMs &&
        (serviceName === undefined || entry.serviceName === serviceName),
    );
  }
}
