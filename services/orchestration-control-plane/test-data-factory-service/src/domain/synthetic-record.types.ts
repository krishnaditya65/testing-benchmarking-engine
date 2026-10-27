/**
 * A single generated record, keyed by field name. Deliberately loose
 * (`Record<string, unknown>`) because the concrete shape is driven entirely
 * by the `SyntheticEntitySchema` supplied at generation time, not by a
 * compile-time type per entity.
 */
export type SyntheticRecord = Record<string, unknown>;

/**
 * The output of generating one batch (or, for small entities, the entire
 * set) of records for a single entity.
 */
export interface SyntheticGenerationResult {
  readonly entityName: string;
  readonly records: SyntheticRecord[];
}
