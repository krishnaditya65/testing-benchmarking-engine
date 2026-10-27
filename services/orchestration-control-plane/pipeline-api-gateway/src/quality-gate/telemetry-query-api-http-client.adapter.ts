import { Injectable, Logger, ServiceUnavailableException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import axios, { AxiosInstance } from 'axios';
import { RunTelemetrySummary } from './quality-gate-sla-threshold.types';
import { TelemetryQueryApiClientPort } from './telemetry-query-api-client.port';

interface RunTelemetrySummaryResponseBody {
  runId: string;
  p99LatencyMs: number;
  errorRatePercent: number;
  throughputRps: number;
}

/**
 * Real HTTP adapter to the sibling `telemetry-query-api` service. Calls
 * `GET /runs/:runId/telemetry-summary`, which telemetry-query-api computes
 * from the raw metrics/traces it ingests from workers during a run.
 */
@Injectable()
export class TelemetryQueryApiHttpClientAdapter implements TelemetryQueryApiClientPort {
  private readonly logger = new Logger(TelemetryQueryApiHttpClientAdapter.name);
  private readonly httpClient: AxiosInstance;

  constructor(private readonly configService: ConfigService) {
    this.httpClient = axios.create({
      baseURL: this.configService.get<string>('TELEMETRY_QUERY_API_BASE_URL', 'http://telemetry-query-api:8090'),
      timeout: this.configService.get<number>('TELEMETRY_QUERY_API_TIMEOUT_MS', 5000),
    });
  }

  async fetchRunTelemetrySummary(runId: string): Promise<RunTelemetrySummary> {
    try {
      const response = await this.httpClient.get<RunTelemetrySummaryResponseBody>(
        `/runs/${runId}/telemetry-summary`,
      );
      return response.data;
    } catch (error) {
      this.logger.error(
        `Failed to fetch telemetry summary for run ${runId} from telemetry-query-api: ${this.describeError(error)}`,
      );
      throw new ServiceUnavailableException(
        `telemetry-query-api is unavailable or has no telemetry for run ${runId}`,
      );
    }
  }

  private describeError(error: unknown): string {
    if (axios.isAxiosError(error)) {
      return `${error.response?.status ?? 'no-response'} ${JSON.stringify(error.response?.data ?? error.message)}`;
    }
    return error instanceof Error ? error.message : String(error);
  }
}
