/**
 * canary-differential-report-http-client.adapter.ts
 *
 * Real HTTP adapter for `CanaryDifferentialReportClientPort`, calling
 * `telemetry-lake/telemetry-query-api`'s HTTP surface (`telemetry-query-controller.ts`) the same
 * way `pipeline-api-gateway`'s `telemetry-query-api-http-client.adapter.ts` does for quality
 * gates. Kept dependency-free (plain `fetch`, available in Node >= 18/20 per this monorepo's
 * `engines.node` floor) rather than pulling in `axios`, since this adapter only ever makes one
 * kind of call.
 */
import type {
  CanaryComparisonRequest,
  CanaryComparisonResult,
  CanaryDifferentialReportClientPort,
} from "./canary-differential-report-client.port";

export interface CanaryDifferentialReportHttpClientConfig {
  /** Base URL of the telemetry-query-api instance, e.g. "http://telemetry-query-api:3000". */
  telemetryQueryApiBaseUrl: string;
  requestTimeoutMs?: number;
}

const DEFAULT_REQUEST_TIMEOUT_MS = 10_000;

export class CanaryDifferentialReportHttpClientAdapter implements CanaryDifferentialReportClientPort {
  constructor(private readonly config: CanaryDifferentialReportHttpClientConfig) {}

  async fetchComparisonReport(request: CanaryComparisonRequest): Promise<CanaryComparisonResult> {
    const controller = new AbortController();
    const timeout = setTimeout(
      () => controller.abort(),
      this.config.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS,
    );

    try {
      const response = await fetch(`${this.config.telemetryQueryApiBaseUrl.replace(/\/$/, "")}/canary-comparisons`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(request),
        signal: controller.signal,
      });

      if (!response.ok) {
        throw new Error(
          `telemetry-query-api canary comparison request failed: ${response.status} ${response.statusText}`,
        );
      }

      return (await response.json()) as CanaryComparisonResult;
    } finally {
      clearTimeout(timeout);
    }
  }
}
