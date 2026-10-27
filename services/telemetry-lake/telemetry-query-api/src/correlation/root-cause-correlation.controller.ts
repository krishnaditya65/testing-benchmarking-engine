/**
 * root-cause-correlation.controller.ts
 *
 * HTTP surface for `IncidentRootCauseCorrelator`, following the same thin-controller convention
 * as `telemetry-query-controller.ts` — request parsing only, all logic lives in the correlator.
 * Kept as its own controller (rather than added to `TelemetryQueryController`) because root-cause
 * correlation is a distinct capability beyond the original "Read API" feature manifest (see this
 * package's README and ADR 0008), not another percentile/error-rate query shape.
 */
import { Controller, Get, Query } from "@nestjs/common";
import { IncidentRootCauseCorrelator } from "./incident-root-cause-correlator";

@Controller("telemetry/root-cause-correlation")
export class RootCauseCorrelationController {
  constructor(private readonly correlator: IncidentRootCauseCorrelator) {}

  @Get()
  async correlate(@Query("service") service: string, @Query("anomalyTimestamp") anomalyTimestamp: string) {
    return this.correlator.correlate({
      serviceName: service,
      timestamp: new Date(anomalyTimestamp),
    });
  }
}
