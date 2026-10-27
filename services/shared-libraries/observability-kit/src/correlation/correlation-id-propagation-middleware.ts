/**
 * correlation-id-propagation-middleware.ts
 *
 * Express-compatible middleware that reads (or mints) a correlation id per inbound request and
 * makes it available for the lifetime of that request via AsyncLocalStorage — without needing
 * to thread it through every function call manually. `structured-logger-factory`'s
 * `correlationIdProvider` option is designed to read from this store, and outbound HTTP calls
 * made during request handling should propagate the same id via `getOutboundCorrelationHeaders()`.
 *
 * "Correlation id" here is distinct from an OTel trace id: it's a platform-level identifier
 * that ties together an end-to-end pipeline-run request across services that don't all speak
 * OTLP directly (e.g. a request into `pipeline-api-gateway` triggering work eventually visible
 * as several separate traces once it reaches execution-fleet workers). Where a trace id is
 * already available, callers should include it as a field alongside, not instead of, this id.
 */
import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import type { NextFunction, Request, Response } from "express";

export const CORRELATION_ID_HEADER_NAME = "x-correlation-id";

interface CorrelationContext {
  correlationId: string;
}

const correlationStorage = new AsyncLocalStorage<CorrelationContext>();

/**
 * Reads the correlation id bound to the currently executing async context, if any. Intended
 * primary consumer: `structured-logger-factory`'s `correlationIdProvider` option.
 */
export function getCurrentCorrelationId(): string | undefined {
  return correlationStorage.getStore()?.correlationId;
}

/** Headers to attach to any outbound request made while handling the current inbound request,
 * so the correlation id survives the hop to the next service. */
export function getOutboundCorrelationHeaders(): Record<string, string> {
  const correlationId = getCurrentCorrelationId();
  return correlationId ? { [CORRELATION_ID_HEADER_NAME]: correlationId } : {};
}

/**
 * correlationIdPropagationMiddleware
 *
 * Express middleware: reads `x-correlation-id` from the inbound request if present (propagated
 * from an upstream service), otherwise mints a new UUIDv4; binds it to an AsyncLocalStorage
 * context for the duration of the request; echoes it back on the response so callers can
 * correlate their own logs against the response even if they didn't set the header themselves.
 */
export function correlationIdPropagationMiddleware(request: Request, response: Response, next: NextFunction): void {
  const incoming = request.header(CORRELATION_ID_HEADER_NAME);
  const correlationId = incoming && incoming.trim().length > 0 ? incoming : randomUUID();

  response.setHeader(CORRELATION_ID_HEADER_NAME, correlationId);

  correlationStorage.run({ correlationId }, () => {
    next();
  });
}
