# observability-kit

Shared OTel initialization, structured logging, and correlation-id propagation for every
TypeScript service in the platform — so tracing setup, log shape, and request correlation are
identical across `pipeline-api-gateway`, `telemetry-query-api`, `test-data-factory-service`, and
`workflow-canvas-frontend`'s server-side code, instead of each reimplementing its own variant.

## Implements

- ARCHITECTURE.md: *"`observability-kit` — the observability kit every other service imports
  rather than reimplementing."*

## Contents

- `src/tracing/initialize-open-telemetry-instrumentation.ts` — `initializeOpenTelemetryInstrumentation(serviceName, options?)`.
  Bootstraps `@opentelemetry/sdk-node` with this platform's standard resource attributes and an
  OTLP gRPC exporter pointed at the node-local collector (see
  `services/telemetry-lake/otel-collector-config`), and starts it. Call once, first, in a
  service's entrypoint.
- `src/logging/structured-logger-factory.ts` — `createStructuredLogger(serviceName, options?)`.
  Returns a `StructuredLogger` (`debug`/`info`/`warn`/`error`/`child`) that emits one JSON
  object per line with a consistent shape (`timestampIso`, `level`, `service`, `message`,
  `correlationId`, plus caller-supplied fields).
- `src/correlation/correlation-id-propagation-middleware.ts` — Express middleware that reads or
  mints an `x-correlation-id` header per request, binds it to an `AsyncLocalStorage` context for
  the request's lifetime, and echoes it on the response. `getCurrentCorrelationId()` and
  `getOutboundCorrelationHeaders()` let any code (including the logger factory) pick it up
  without threading it through every function signature.

## Usage

```ts
import express from "express";
import {
  initializeOpenTelemetryInstrumentation,
  createStructuredLogger,
  correlationIdPropagationMiddleware,
  getCurrentCorrelationId,
} from "@shared-libraries/observability-kit";

// First, before other imports that should be auto-instrumented.
const sdk = initializeOpenTelemetryInstrumentation("pipeline-api-gateway");

const logger = createStructuredLogger("pipeline-api-gateway", {
  correlationIdProvider: getCurrentCorrelationId,
});

const app = express();
app.use(correlationIdPropagationMiddleware);
app.get("/health", (_req, res) => {
  logger.info("health check ok");
  res.sendStatus(200);
});

process.on("SIGTERM", () => sdk.shutdown().finally(() => process.exit(0)));
```

## Why correlation id and trace id are both tracked

A trace id (OTel) identifies one distributed trace and is only assigned once a span exists.
The correlation id here is minted at the very first HTTP entrypoint into the platform (or
propagated from an upstream service that already had one) and persists across every hop of a
logical operation, including hops that don't currently emit OTLP. Logs always carry the
correlation id; they carry a trace id only where one exists. Prefer trace id for "show me this
exact distributed trace" (via `telemetry-query-api`'s `getCrossServiceTraceCorrelation`) and
correlation id for "show me every log line related to this one user-facing operation."
