import type { SyntheticEntitySchema } from '../domain/synthetic-schema.types';
import type {
  SyntheticGenerationResult,
  SyntheticRecord,
} from '../domain/synthetic-record.types';

/**
 * Port (hexagonal-architecture "driving" interface) for anything capable of
 * fabricating synthetic records for a single entity.
 *
 * `referencedRecordPools` is how referential consistency across entities is
 * achieved: the orchestrator generates parent entities first, then passes
 * their already-generated records in here (keyed by entity name) so that
 * child records can pick a foreign-key value that genuinely exists in the
 * parent set, instead of a random unrelated id.
 */
export interface SyntheticRecordGenerator {
  generate(
    schema: SyntheticEntitySchema,
    recordCount: number,
    referencedRecordPools: Map<string, SyntheticRecord[]>
  ): SyntheticGenerationResult;
}
