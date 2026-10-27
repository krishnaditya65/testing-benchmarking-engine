/**
 * click-house-connection-config.ts
 *
 * Central place resolving how this service connects to the telemetry-lake ClickHouse
 * instance. Kept deliberately tiny and dependency-free so it's trivial to override in tests.
 */
export interface ClickHouseConnectionConfig {
  host: string;
  database: string;
  username: string;
  password: string;
  /** Max simultaneous ClickHouse requests this service will issue; protects ClickHouse from
   * being overwhelmed by a dashboard fan-out of many concurrent panel queries. */
  maxOpenConnections: number;
}

export function loadClickHouseConnectionConfigFromEnv(): ClickHouseConnectionConfig {
  return {
    host: process.env.CLICKHOUSE_HTTP_URL ?? "http://localhost:8123",
    database: process.env.CLICKHOUSE_DATABASE ?? "telemetry_lake",
    username: process.env.CLICKHOUSE_USERNAME ?? "default",
    password: process.env.CLICKHOUSE_PASSWORD ?? "",
    maxOpenConnections: Number(process.env.CLICKHOUSE_MAX_OPEN_CONNECTIONS ?? 20),
  };
}
