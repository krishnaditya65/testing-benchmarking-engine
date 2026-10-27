import { describe, expect, it } from "vitest";
import { CrashSequenceExtractor } from "./crash-sequence-extractor";
import type { ApmSessionLogEntry } from "../ingestion/apm-session-log-ingester";

function entry(
  sessionId: string,
  requestPath: string,
  occurredAtEpochMs: number,
  statusCode: number,
  overrides: Partial<ApmSessionLogEntry> = {},
): ApmSessionLogEntry {
  return {
    sessionId,
    traceId: `trace-${sessionId}`,
    requestPath,
    httpMethod: "GET",
    statusCode,
    durationMs: 20,
    occurredAtEpochMs,
    serviceName: "checkout-service",
    ...overrides,
  };
}

describe("CrashSequenceExtractor", () => {
  it("extracts the requests immediately preceding a 5xx into one sequence", () => {
    const entries: ApmSessionLogEntry[] = [
      entry("s1", "/cart", 1000, 200),
      entry("s1", "/checkout", 2000, 200, { httpMethod: "POST" }),
      entry("s1", "/confirm", 3000, 500, { httpMethod: "POST" }),
      entry("s1", "/receipt", 4000, 200),
    ];

    const sequences = new CrashSequenceExtractor().extractCrashSequences(entries, { lookbackLength: 3 });

    expect(sequences).toHaveLength(1);
    expect(sequences[0].crashStatusCode).toBe(500);
    expect(sequences[0].steps.map((s) => s.requestPath)).toEqual(["/cart", "/checkout", "/confirm"]);
    expect(sequences[0].steps[sequences[0].steps.length - 1].statusCode).toBe(500);
  });

  it("truncates the lookback window gracefully when the crash is early in the session", () => {
    const entries: ApmSessionLogEntry[] = [
      entry("s2", "/checkout", 1000, 502, { httpMethod: "POST" }),
    ];

    const sequences = new CrashSequenceExtractor().extractCrashSequences(entries, { lookbackLength: 4 });

    expect(sequences).toHaveLength(1);
    expect(sequences[0].steps).toHaveLength(1);
    expect(sequences[0].crashStatusCode).toBe(502);
  });

  it("captures multiple crashes within the same session as separate sequences", () => {
    const entries: ApmSessionLogEntry[] = [
      entry("s3", "/a", 1000, 200),
      entry("s3", "/b", 2000, 500),
      entry("s3", "/c", 3000, 200),
      entry("s3", "/d", 4000, 503),
    ];

    const sequences = new CrashSequenceExtractor().extractCrashSequences(entries, { lookbackLength: 2 });

    expect(sequences).toHaveLength(2);
    expect(sequences[0].crashStatusCode).toBe(500);
    expect(sequences[1].crashStatusCode).toBe(503);
  });

  it("ignores 4xx client errors by default (only 5xx counts as a crash)", () => {
    const entries: ApmSessionLogEntry[] = [entry("s4", "/a", 1000, 404)];
    const sequences = new CrashSequenceExtractor().extractCrashSequences(entries);
    expect(sequences).toHaveLength(0);
  });
});
