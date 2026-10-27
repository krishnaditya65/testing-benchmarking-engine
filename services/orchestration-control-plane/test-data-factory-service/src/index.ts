import type { Pool } from 'pg';
import type { Db } from 'mongodb';
import type { SyntheticEntitySchema } from './domain/synthetic-schema.types';
import type { SyntheticGenerationResult } from './domain/synthetic-record.types';
import { RelationalSyntheticRecordGenerator, RelationalDatasetOrchestrator } from './generation/relational-synthetic-record-generator';
import { PiiMaskingTransformer } from './masking/pii-masking-transformer';
import { PostgresSeedingAdapter } from './seeding/postgres-seeding-adapter';
import { MongoSeedingAdapter } from './seeding/mongo-seeding-adapter';
import type { DatabaseSeedingAdapter, SeedingOutcome } from './seeding/database-seeding-adapter.port';

export type { SyntheticEntitySchema, SyntheticFieldSpec, SyntheticForeignKeySpec } from './domain/synthetic-schema.types';
export type { SyntheticRecord, SyntheticGenerationResult } from './domain/synthetic-record.types';
export type { SyntheticRecordGenerator } from './generation/synthetic-record-generator.port';
export { RelationalSyntheticRecordGenerator, RelationalDatasetOrchestrator, MissingReferencedRecordPoolError } from './generation/relational-synthetic-record-generator';
export { PiiMaskingTransformer } from './masking/pii-masking-transformer';
export type { PiiMaskingStrategy } from './masking/pii-masking-transformer';
export type { DatabaseSeedingAdapter, SeedingOutcome } from './seeding/database-seeding-adapter.port';
export { PostgresSeedingAdapter } from './seeding/postgres-seeding-adapter';
export { MongoSeedingAdapter } from './seeding/mongo-seeding-adapter';

/**
 * Which concrete `DatabaseSeedingAdapter` to wire up. Exactly one of
 * `pgPool` / `mongoDb` must be supplied to match.
 */
export type TestDataFactoryTargetDatastore =
  | { readonly kind: 'postgres'; readonly pgPool: Pool }
  | { readonly kind: 'mongodb'; readonly mongoDb: Db };

export interface TestDataFactoryOptions {
  readonly targetDatastore: TestDataFactoryTargetDatastore;
  /** Default per-batch row count used by `seedDataset` unless overridden per call. */
  readonly defaultBatchSize?: number;
}

/**
 * The assembled set of collaborators a `DataSeedNode` DAG step needs to
 * generate and seed a dataset. Everything here is composed once, in one
 * place (this factory function), from DI'd infrastructure handles --
 * nothing in `generation/` or `masking/` reaches out to a global/static
 * database handle.
 */
export interface TestDataFactory {
  readonly recordGenerator: RelationalSyntheticRecordGenerator;
  readonly datasetOrchestrator: RelationalDatasetOrchestrator;
  readonly piiMaskingTransformer: PiiMaskingTransformer;
  readonly seedingAdapter: DatabaseSeedingAdapter;

  /**
   * Generates and seeds an entire multi-entity dataset in dependency
   * (topological) order, batch by batch: each batch is generated, PII
   * fields are masked, the batch is seeded, and only then is the next
   * batch generated -- so peak memory is bounded by `batchSize`, not by the
   * total row counts requested.
   */
  seedDataset(
    schemas: SyntheticEntitySchema[],
    recordCountByEntityName: Map<string, number>,
    batchSize?: number
  ): Promise<SeedingOutcome[]>;
}

/**
 * Composition root: wires a `RelationalSyntheticRecordGenerator`, a
 * `RelationalDatasetOrchestrator`, a `PiiMaskingTransformer`, and the
 * `DatabaseSeedingAdapter` implied by `targetDatastore` into one
 * `TestDataFactory`. This is the single place in the service that decides
 * which concrete adapter class backs the `DatabaseSeedingAdapter` port --
 * everything upstream of it only ever depends on the port interface.
 */
export function createTestDataFactory(options: TestDataFactoryOptions): TestDataFactory {
  const recordGenerator = new RelationalSyntheticRecordGenerator();
  const datasetOrchestrator = new RelationalDatasetOrchestrator(
    recordGenerator,
    options.defaultBatchSize ?? 10_000
  );
  const piiMaskingTransformer = new PiiMaskingTransformer();
  const seedingAdapter = createSeedingAdapter(options.targetDatastore);

  return {
    recordGenerator,
    datasetOrchestrator,
    piiMaskingTransformer,
    seedingAdapter,

    async seedDataset(schemas, recordCountByEntityName, batchSize) {
      const schemaByEntityName = new Map(schemas.map((s) => [s.entityName, s]));
      const outcomes: SeedingOutcome[] = [];

      for (const batch of datasetOrchestrator.generateBatchesInDependencyOrder(
        schemas,
        recordCountByEntityName,
        batchSize
      )) {
        const schema = schemaByEntityName.get(batch.entityName) as SyntheticEntitySchema;
        const maskedBatch: SyntheticGenerationResult = {
          entityName: batch.entityName,
          records: batch.records.map((record) => piiMaskingTransformer.maskRecord(record, schema)),
        };
        outcomes.push(await seedingAdapter.seedRecords(maskedBatch.entityName, maskedBatch.records));
      }

      return outcomes;
    },
  };
}

function createSeedingAdapter(targetDatastore: TestDataFactoryTargetDatastore): DatabaseSeedingAdapter {
  switch (targetDatastore.kind) {
    case 'postgres':
      return new PostgresSeedingAdapter(targetDatastore.pgPool);
    case 'mongodb':
      return new MongoSeedingAdapter(targetDatastore.mongoDb);
    default: {
      const unreachable: never = targetDatastore;
      throw new Error(`Unsupported target datastore: ${String(unreachable)}`);
    }
  }
}
