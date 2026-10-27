import { RunTelemetrySummary } from './quality-gate-sla-threshold.types';

/** DI token for the active TelemetryQueryApiClientPort implementation. */
export const TELEMETRY_QUERY_API_CLIENT = Symbol('TELEMETRY_QUERY_API_CLIENT');

/**
 * Port to the sibling `telemetry-query-api` service (telemetry-lake layer).
 * pipeline-api-gateway reads run-level telemetry aggregates through this
 * port purely to evaluate quality gates — it never writes telemetry, and it
 * does not own or duplicate telemetry-query-api's storage.
 */
export interface TelemetryQueryApiClientPort {
  fetchRunTelemetrySummary(runId: string): Promise<RunTelemetrySummary>;
}
