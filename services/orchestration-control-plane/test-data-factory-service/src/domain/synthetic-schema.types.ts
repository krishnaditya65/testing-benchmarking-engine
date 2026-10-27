/**
 * Domain types describing the shape of a synthetic entity (a table or a
 * Mongo collection) that this service knows how to generate fake, but
 * structurally-realistic, data against.
 *
 * These types are the contract a DAG pipeline's `DataSeedNode` step supplies
 * when it asks the factory to seed a target datastore before a downstream
 * benchmarking/load step runs against it.
 */

/**
 * The primitive kinds of values the record generator knows how to fabricate.
 * Kept intentionally small and explicit rather than "string | number | ..."
 * so every generator branch is exhaustively checked by the compiler.
 */
export type SyntheticFieldDataType =
  | 'string'
  | 'integer'
  | 'boolean'
  | 'timestamp'
  | 'uuid'
  | 'email'
  | 'currencyAmount';

/**
 * Describes a single field/column on a synthetic entity: what it's called,
 * what kind of value it holds, and whether it carries personally
 * identifiable information (in which case `PiiMaskingTransformer` will
 * mask/tokenize it before the record is considered safe to seed into a
 * shared/non-production datastore).
 */
export interface SyntheticFieldSpec {
  readonly fieldName: string;
  readonly dataType: SyntheticFieldDataType;
  readonly isPii?: boolean;
}

/**
 * Describes a foreign-key relationship from a field on this entity to a
 * field on another, previously-generated entity. `RelationalDatasetOrchestrator`
 * uses the full set of these across all schemas to compute a topological
 * generation order, and `RelationalSyntheticRecordGenerator` uses them at
 * generation time to pick a real, already-generated parent value so the
 * resulting dataset is referentially consistent.
 */
export interface SyntheticForeignKeySpec {
  readonly fieldName: string;
  readonly referencesEntityName: string;
  readonly referencesFieldName: string;
}

/**
 * The full description of one synthetic entity: its fields and, optionally,
 * the foreign keys that tie it to other entities in the same dataset.
 */
export interface SyntheticEntitySchema {
  readonly entityName: string;
  readonly fields: SyntheticFieldSpec[];
  readonly foreignKeys?: SyntheticForeignKeySpec[];
}
