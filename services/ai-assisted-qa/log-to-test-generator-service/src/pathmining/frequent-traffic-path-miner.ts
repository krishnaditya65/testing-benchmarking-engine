/**
 * frequent-traffic-path-miner.ts
 *
 * Real sequential pattern mining over production traffic: groups `ApmSessionLogEntry` rows by
 * `sessionId`, orders each session's requests chronologically, and counts how often each
 * length-N contiguous subsequence ("N-gram" of `{httpMethod, requestPath}` steps) occurs across
 * every session — a sliding-window n-gram frequency count, the acceptable "real" sequential
 * pattern mining approach called out in the manifest bullet. The top-K most frequent N-grams are
 * the platform's best guess at "the traffic paths worth load-testing because production traffic
 * actually walks them this way".
 */
import type { ApmSessionLogEntry } from "../ingestion/apm-session-log-ingester";

export interface TrafficPathStep {
  httpMethod: string;
  requestPath: string;
}

export interface MinedTrafficPath {
  steps: TrafficPathStep[];
  /** Number of distinct (session, starting-index) windows across all sessions that produced
   * this exact ordered step sequence. */
  occurrenceCount: number;
  /** Number of distinct sessions that exhibited this sequence at least once — distinguishes "one
   * session hammered the same sequence 500 times" (low session diversity) from "500 different
   * sessions each did this once" (high session diversity, likely a genuinely common user path). */
  distinctSessionCount: number;
  /** Up to 5 example session ids, for traceability/debugging back to the source logs. */
  exampleSessionIds: string[];
}

export interface FrequentTrafficPathMinerOptions {
  /** N-gram length — how many consecutive requests make up one candidate "traffic path".
   * Defaults to 3 (e.g. "GET /cart -> POST /cart/items -> POST /checkout"). */
  sequenceLength?: number;
  /** How many of the most frequent sequences to return. Defaults to 5. */
  topK?: number;
}

const DEFAULT_SEQUENCE_LENGTH = 3;
const DEFAULT_TOP_K = 5;
const MAX_EXAMPLE_SESSION_IDS = 5;

/**
 * FrequentTrafficPathMiner
 *
 * Stateless. `mineFrequentPaths` is the sole entry point:
 * `ApmSessionLogEntry[]` (already fetched via `ApmSessionLogSourcePort`, any window/service
 * filtering applied upstream) -> ranked `MinedTrafficPath[]`.
 */
export class FrequentTrafficPathMiner {
  mineFrequentPaths(entries: ApmSessionLogEntry[], options: FrequentTrafficPathMinerOptions = {}): MinedTrafficPath[] {
    const sequenceLength = options.sequenceLength ?? DEFAULT_SEQUENCE_LENGTH;
    const topK = options.topK ?? DEFAULT_TOP_K;

    const sessionSequences = groupIntoOrderedSessionSequences(entries);

    const countsByNgramKey = new Map<
      string,
      { steps: TrafficPathStep[]; occurrenceCount: number; sessionIds: Set<string> }
    >();

    for (const [sessionId, sequence] of sessionSequences) {
      if (sequence.length < sequenceLength) continue;
      for (let start = 0; start <= sequence.length - sequenceLength; start++) {
        const window = sequence.slice(start, start + sequenceLength);
        const key = ngramKey(window);
        const existing = countsByNgramKey.get(key);
        if (existing) {
          existing.occurrenceCount += 1;
          existing.sessionIds.add(sessionId);
        } else {
          countsByNgramKey.set(key, { steps: window, occurrenceCount: 1, sessionIds: new Set([sessionId]) });
        }
      }
    }

    return Array.from(countsByNgramKey.values())
      .map(
        (entry): MinedTrafficPath => ({
          steps: entry.steps,
          occurrenceCount: entry.occurrenceCount,
          distinctSessionCount: entry.sessionIds.size,
          exampleSessionIds: Array.from(entry.sessionIds).slice(0, MAX_EXAMPLE_SESSION_IDS),
        }),
      )
      .sort((a, b) => b.occurrenceCount - a.occurrenceCount || b.distinctSessionCount - a.distinctSessionCount)
      .slice(0, topK);
  }
}

/** Groups entries by session id and sorts each session's entries chronologically, returning
 * plain `TrafficPathStep` sequences (status/timing dropped — irrelevant to *which path* was
 * walked, only relevant to `crash-sequence-extractor.ts`). Exported for reuse there so both
 * miners agree on what "session order" means. */
export function groupIntoOrderedSessionSequences(
  entries: ApmSessionLogEntry[],
): Map<string, TrafficPathStep[]> {
  const bySession = new Map<string, ApmSessionLogEntry[]>();
  for (const entry of entries) {
    const existing = bySession.get(entry.sessionId);
    if (existing) existing.push(entry);
    else bySession.set(entry.sessionId, [entry]);
  }

  const result = new Map<string, TrafficPathStep[]>();
  for (const [sessionId, sessionEntries] of bySession) {
    const ordered = [...sessionEntries].sort((a, b) => a.occurredAtEpochMs - b.occurredAtEpochMs);
    result.set(
      sessionId,
      ordered.map((entry) => ({ httpMethod: entry.httpMethod, requestPath: entry.requestPath })),
    );
  }
  return result;
}

function ngramKey(steps: TrafficPathStep[]): string {
  return steps.map((step) => `${step.httpMethod} ${step.requestPath}`).join(" -> ");
}
