/**
 * telemetry-query-module.ts
 *
 * NestJS module wiring: constructs the ClickHouseQueryRepository from env-sourced connection
 * config and provides it (and the analytics classes that depend on it) to the controller.
 */
import { Module } from "@nestjs/common";
import { TelemetryQueryController } from "./controllers/telemetry-query-controller";
import { ClickHouseQueryRepository } from "./repository/click-house-query-repository";
import { AbCanaryDifferentialBenchmarkComparator } from "./analytics/ab-canary-differential-benchmark-comparator";
import { loadClickHouseConnectionConfigFromEnv } from "./config/click-house-connection-config";
import { RootCauseCorrelationController } from "./correlation/root-cause-correlation.controller";
import { IncidentRootCauseCorrelator } from "./correlation/incident-root-cause-correlator";

@Module({
  controllers: [TelemetryQueryController, RootCauseCorrelationController],
  providers: [
    {
      provide: ClickHouseQueryRepository,
      useFactory: () => new ClickHouseQueryRepository(loadClickHouseConnectionConfigFromEnv()),
    },
    {
      provide: AbCanaryDifferentialBenchmarkComparator,
      useFactory: (repository: ClickHouseQueryRepository) => new AbCanaryDifferentialBenchmarkComparator(repository),
      inject: [ClickHouseQueryRepository],
    },
    {
      provide: IncidentRootCauseCorrelator,
      useFactory: (repository: ClickHouseQueryRepository) => new IncidentRootCauseCorrelator(repository),
      inject: [ClickHouseQueryRepository],
    },
  ],
})
export class TelemetryQueryModule {}
