import { describe, expect, it } from "vitest";
import { FrequentTrafficPathMiner } from "./frequent-traffic-path-miner";
import type { ApmSessionLogEntry } from "../ingestion/apm-session-log-ingester";

function entry(
  sessionId: string,
  requestPath: string,
  occurredAtEpochMs: number,
  overrides: Partial<ApmSessionLogEntry> = {},
): ApmSessionLogEntry {
  return {
    sessionId,
    traceId: `trace-${sessionId}-${occurredAtEpochMs}`,
    requestPath,
    httpMethod: "GET",
    statusCode: 200,
    durationMs: 42,
    occurredAtEpochMs,
    serviceName: "checkout-service",
    ...overrides,
  };
}

describe("FrequentTrafficPathMiner", () => {
  it("finds the top mined path matching the dominant checkout sequence across sessions", () => {
    // Five sessions walk /cart -> /checkout -> /confirm; two unrelated sessions do something else.
    const entries: ApmSessionLogEntry[] = [];
    for (let s = 0; s < 5; s++) {
      const sessionId = `checkout-session-${s}`;
      entries.push(
        entry(sessionId, "/cart", 1000, { httpMethod: "GET" }),
        entry(sessionId, "/checkout", 2000, { httpMethod: "POST" }),
        entry(sessionId, "/confirm", 3000, { httpMethod: "POST" }),
      );
    }
    entries.push(
      entry("browse-session-0", "/home", 1000, { httpMethod: "GET" }),
      entry("browse-session-0", "/search", 2000, { httpMethod: "GET" }),
      entry("browse-session-0", "/product", 3000, { httpMethod: "GET" }),
    );
    entries.push(
      entry("browse-session-1", "/home", 1000, { httpMethod: "GET" }),
      entry("browse-session-1", "/about", 2000, { httpMethod: "GET" }),
    );

    const mined = new FrequentTrafficPathMiner().mineFrequentPaths(entries, { sequenceLength: 3, topK: 3 });

    expect(mined.length).toBeGreaterThan(0);
    const top = mined[0];
    expect(top.occurrenceCount).toBe(5);
    expect(top.distinctSessionCount).toBe(5);
    expect(top.steps.map((s) => s.requestPath)).toEqual(["/cart", "/checkout", "/confirm"]);
  });

  it("orders each session's steps by timestamp, not log arrival order", () => {
    const entries: ApmSessionLogEntry[] = [
      entry("s1", "/checkout", 5000),
      entry("s1", "/cart", 1000),
      entry("s1", "/confirm", 9000),
      entry("s2", "/checkout", 5000),
      entry("s2", "/cart", 1000),
      entry("s2", "/confirm", 9000),
    ];

    const mined = new FrequentTrafficPathMiner().mineFrequentPaths(entries, { sequenceLength: 3, topK: 1 });

    expect(mined[0].steps.map((s) => s.requestPath)).toEqual(["/cart", "/checkout", "/confirm"]);
  });

  it("respects topK and returns fewer results when fewer distinct sequences exist", () => {
    const entries: ApmSessionLogEntry[] = [entry("s1", "/a", 1), entry("s1", "/b", 2)];
    const mined = new FrequentTrafficPathMiner().mineFrequentPaths(entries, { sequenceLength: 3, topK: 5 });
    expect(mined).toHaveLength(0); // session too short for a length-3 window
  });
});
