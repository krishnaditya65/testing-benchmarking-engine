/**
 * structured-logger-factory.ts
 *
 * Every TypeScript service logs through a logger built by `createStructuredLogger()` so log
 * lines share one JSON shape platform-wide — this is what lets the telemetry lake (and any
 * human grepping raw pod logs) rely on a consistent set of fields (`service`, `level`,
 * `correlationId`, ...) instead of parsing per-service ad hoc formats.
 */

export type LogLevel = "debug" | "info" | "warn" | "error";

const LOG_LEVEL_SEVERITY: Record<LogLevel, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

/** The exact JSON shape every log line emitted through this factory takes. Keep this in sync
 * with any log-parsing dashboard/alert rule that greps on field names. */
export interface StructuredLogRecord {
  timestampIso: string;
  level: LogLevel;
  service: string;
  message: string;
  /** Propagated from correlation-id-propagation-middleware; absent for logs emitted outside
   * an inbound request/message context (e.g. startup logs). */
  correlationId?: string;
  /** Arbitrary structured fields the caller wants attached — merged at the top level so log
   * pipelines can query them directly rather than reaching into a nested `context` object. */
  [additionalField: string]: unknown;
}

export interface StructuredLoggerOptions {
  /** Minimum level that will actually be written; anything below is a no-op. Defaults to
   * "info" outside test environments so debug-level noise doesn't ship to production log
   * storage by default. */
  minimumLevel?: LogLevel;
  /** Injectable sink, primarily for tests; defaults to writing one JSON line to stdout, which
   * is what every container log collector in this platform expects. */
  sink?: (line: string) => void;
  /** Supplies the current correlation id at log time (typically backed by the AsyncLocalStorage
   * store set up in correlation-id-propagation-middleware), so callers don't have to thread it
   * through every log call manually. */
  correlationIdProvider?: () => string | undefined;
}

export interface StructuredLogger {
  debug(message: string, fields?: Record<string, unknown>): void;
  info(message: string, fields?: Record<string, unknown>): void;
  warn(message: string, fields?: Record<string, unknown>): void;
  error(message: string, fields?: Record<string, unknown>): void;
  /** Returns a child logger that merges `fields` into every log call automatically — useful
   * for e.g. binding `pipelineRunId`/`dagNodeId` once per worker task rather than repeating it. */
  child(fields: Record<string, unknown>): StructuredLogger;
}

/**
 * createStructuredLogger
 *
 * Builds a `StructuredLogger` bound to `serviceName`, emitting one-JSON-object-per-line to the
 * configured sink (stdout by default).
 */
export function createStructuredLogger(
  serviceName: string,
  options: StructuredLoggerOptions = {},
  boundFields: Record<string, unknown> = {},
): StructuredLogger {
  const minimumLevel = options.minimumLevel ?? "info";
  const sink = options.sink ?? ((line: string) => process.stdout.write(line + "\n"));

  function write(level: LogLevel, message: string, fields?: Record<string, unknown>): void {
    if (LOG_LEVEL_SEVERITY[level] < LOG_LEVEL_SEVERITY[minimumLevel]) return;

    const record: StructuredLogRecord = {
      timestampIso: new Date().toISOString(),
      level,
      service: serviceName,
      message,
      correlationId: options.correlationIdProvider?.(),
      ...boundFields,
      ...fields,
    };
    sink(JSON.stringify(record));
  }

  return {
    debug: (message, fields) => write("debug", message, fields),
    info: (message, fields) => write("info", message, fields),
    warn: (message, fields) => write("warn", message, fields),
    error: (message, fields) => write("error", message, fields),
    child: (fields) => createStructuredLogger(serviceName, options, { ...boundFields, ...fields }),
  };
}
