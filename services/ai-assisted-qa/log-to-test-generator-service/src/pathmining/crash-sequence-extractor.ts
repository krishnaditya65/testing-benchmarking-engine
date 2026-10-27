/**
 * crash-sequence-extractor.ts
 *
 * Isolates "what did the user do right before this broke" sequences: for every session, finds
 * each request whose `statusCode` is a 5xx server error (a "crash" from the caller's
 * perspective) and captures the `lookbackLength` requests immediately preceding it, including
 * the crash request itself as the final step. These are prime candidates for turning into a
 * regression-reproduction pipeline (see `traffic-path-to-dag-workflow-generator.ts`, which
 * appends an ASSERTION node expecting the crash status when converting one of these).
 */
import type { ApmSessionLogEntry } from "../ingestion/apm-session-log-ingester";

export interface CrashSequenceStep {
  httpMethod: string;
  requestPath: string;
  statusCode: number;
  durationMs: number;
}

export interface CrashSequence {
  sessionId: string;
  traceId: string;
  /** Ordered steps leading up to and including the crash; last element always has a 5xx
   * `statusCode`. */
  steps: CrashSequenceStep[];
  /** The crash step's status code, surfaced separately for convenience even though it's also
   * `steps[steps.length - 1].statusCode`. */
  crashStatusCode: number;
  crashOccurredAtEpochMs: number;
}

export interface CrashSequenceExtractorOptions {
  /** How many requests (including the crash request) each extracted sequence should contain.
   * Defaults to 4 — e.g. 3 legitimate requests then the one that 500'd. Sessions with fewer
   * preceding requests than this yield a shorter sequence rather than being dropped, so a crash
   * on a session's very first request is still captured. */
  lookbackLength?: number;
  /** Status codes at/above this value count as a crash. Defaults to 500 (any 5xx). */
  crashStatusCodeThreshold?: number;
}

const DEFAULT_LOOKBACK_LENGTH = 4;
const DEFAULT_CRASH_STATUS_CODE_THRESHOLD = 500;

/**
 * CrashSequenceExtractor
 *
 * Stateless. Complements `FrequentTrafficPathMiner`: that class finds the *common* case, this
 * class finds the *broken* case, and both feed the same
 * `traffic-path-to-dag-workflow-generator.ts` conversion step.
 */
export class CrashSequenceExtractor {
  extractCrashSequences(entries: ApmSessionLogEntry[], options: CrashSequenceExtractorOptions = {}): CrashSequence[] {
    const lookbackLength = options.lookbackLength ?? DEFAULT_LOOKBACK_LENGTH;
    const crashStatusCodeThreshold = options.crashStatusCodeThreshold ?? DEFAULT_CRASH_STATUS_CODE_THRESHOLD;

    const bySession = new Map<string, ApmSessionLogEntry[]>();
    for (const entry of entries) {
      const existing = bySession.get(entry.sessionId);
      if (existing) existing.push(entry);
      else bySession.set(entry.sessionId, [entry]);
    }

    const sequences: CrashSequence[] = [];
    for (const [sessionId, sessionEntries] of bySession) {
      const ordered = [...sessionEntries].sort((a, b) => a.occurredAtEpochMs - b.occurredAtEpochMs);

      for (let index = 0; index < ordered.length; index++) {
        const candidate = ordered[index];
        if (candidate.statusCode < crashStatusCodeThreshold) continue;

        const windowStart = Math.max(0, index - lookbackLength + 1);
        const windowEntries = ordered.slice(windowStart, index + 1);

        sequences.push({
          sessionId,
          traceId: candidate.traceId,
          steps: windowEntries.map(toStep),
          crashStatusCode: candidate.statusCode,
          crashOccurredAtEpochMs: candidate.occurredAtEpochMs,
        });
      }
    }

    return sequences.sort((a, b) => a.crashOccurredAtEpochMs - b.crashOccurredAtEpochMs);
  }
}

function toStep(entry: ApmSessionLogEntry): CrashSequenceStep {
  return {
    httpMethod: entry.httpMethod,
    requestPath: entry.requestPath,
    statusCode: entry.statusCode,
    durationMs: entry.durationMs,
  };
}
