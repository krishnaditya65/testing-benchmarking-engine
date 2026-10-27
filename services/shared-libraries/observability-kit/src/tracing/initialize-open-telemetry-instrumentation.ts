/**
 * initialize-open-telemetry-instrumentation.ts
 *
 * Shared OTel SDK bootstrap every TypeScript service calls at process start so trace/metric
 * export configuration (endpoint, batching, resource attributes) is identical across services
 * instead of each one reimplementing its own SDK setup slightly differently. Non-TypeScript
 * services (Go/Rust workers) follow the equivalent pattern documented in this package's README
 * but cannot import this module directly.
 */
import { NodeSDK } from "@opentelemetry/sdk-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-grpc";
import { OTLPMetricExporter } from "@opentelemetry/exporter-metrics-otlp-grpc";
import { PeriodicExportingMetricReader } from "@opentelemetry/sdk-metrics";
import { Resource } from "@opentelemetry/resources";
import { SemanticResourceAttributes } from "@opentelemetry/semantic-conventions";
import { getNodeAutoInstrumentations } from "@opentelemetry/auto-instrumentations-node";

export interface OpenTelemetryInstrumentationOptions {
  /** Deployment environment tag ("local" | "staging" | "production", etc.); defaults to
   * process.env.DEPLOYMENT_ENVIRONMENT so it doesn't need to be threaded through every caller. */
  deploymentEnvironment?: string;
  /** OTLP gRPC endpoint of the node-local OTel Collector DaemonSet; defaults to the standard
   * collector port on localhost, which is correct for any pod colocated with the DaemonSet. */
  otlpEndpoint?: string;
  /** Extra static resource attributes merged in on top of the required service.name — e.g. a
   * worker might add pipeline-run-scoped attributes here. */
  additionalResourceAttributes?: Record<string, string>;
}

/**
 * Initializes the Node OpenTelemetry SDK for `serviceName` with this platform's standard
 * resource attributes, trace exporter, and metric reader, and starts it. Call this once, as
 * early as possible in a service's entrypoint (before other imports that should be
 * auto-instrumented).
 *
 * Returns the constructed SDK so the caller can register a graceful-shutdown hook
 * (`sdk.shutdown()`) — this function intentionally does not register process signal handlers
 * itself, since shutdown ordering relative to a service's other resources varies by service.
 */
export function initializeOpenTelemetryInstrumentation(
  serviceName: string,
  options: OpenTelemetryInstrumentationOptions = {},
): NodeSDK {
  const otlpEndpoint = options.otlpEndpoint ?? process.env.OTEL_EXPORTER_OTLP_ENDPOINT ?? "http://localhost:4317";
  const deploymentEnvironment =
    options.deploymentEnvironment ?? process.env.DEPLOYMENT_ENVIRONMENT ?? "local";

  const resource = new Resource({
    [SemanticResourceAttributes.SERVICE_NAME]: serviceName,
    [SemanticResourceAttributes.DEPLOYMENT_ENVIRONMENT]: deploymentEnvironment,
    ...options.additionalResourceAttributes,
  });

  const sdk = new NodeSDK({
    resource,
    traceExporter: new OTLPTraceExporter({ url: otlpEndpoint }),
    metricReader: new PeriodicExportingMetricReader({
      exporter: new OTLPMetricExporter({ url: otlpEndpoint }),
      // Aligned with the collector's own batch timeout (see
      // otel-collector-config/collector-config.yaml) so metrics don't sit buffered here
      // noticeably longer than they would downstream.
      exportIntervalMillis: 5_000,
    }),
    instrumentations: [
      getNodeAutoInstrumentations({
        // Filesystem instrumentation is extremely noisy and rarely actionable at this
        // platform's scale; every other default auto-instrumentation stays enabled.
        "@opentelemetry/instrumentation-fs": { enabled: false },
      }),
    ],
  });

  sdk.start();
  return sdk;
}
