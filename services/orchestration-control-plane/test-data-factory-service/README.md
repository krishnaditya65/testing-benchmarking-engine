# test-data-factory-service

Dynamic Test Data Management (TDM) service for the Orchestration Control
Plane. It generates referentially-consistent synthetic records and seeds
them into target databases as part of a DAG pipeline run.

## Where this fits

- The DAG pipeline step type that invokes this service is `DataSeedNode`,
  authored in the sibling `workflow-canvas-frontend` app.
- Execution of the `DataSeedNode` step (i.e. calling into this service at
  the right point in a DAG run) is scheduled by the sibling
  `dag-workflow-scheduler`.
- This service does not execute load itself -- it only produces/seeds the
  data that a later load/benchmark step in the same DAG run will exercise.

## Feature manifest -> code map

| Feature manifest bullet | Implementation |
| --- | --- |
| "Dynamic TDM" (the service as a whole) | Everything under `src/`, composed via `createTestDataFactory` in `src/index.ts` |
| Referentially-consistent synthetic data generation | `src/generation/relational-synthetic-record-generator.ts` -- `RelationalSyntheticRecordGenerator` (per-entity generation + FK resolution) and `RelationalDatasetOrchestrator` (topological generation order + batching) |
| PII masking / tokenization | `src/masking/pii-masking-transformer.ts` -- `PiiMaskingTransformer` |
| Seeding into target databases | `src/seeding/database-seeding-adapter.port.ts` (hexagonal port) with `PostgresSeedingAdapter` and `MongoSeedingAdapter` as concrete adapters |

## Design

This service follows a hexagonal (ports & adapters) shape:

- **Domain** (`src/domain/`): plain types describing entity schemas and
  generated records. No dependencies on faker, pg, or mongodb.
- **Generation** (`src/generation/`): `SyntheticRecordGenerator` is the
  port; `RelationalSyntheticRecordGenerator` is the faker-backed
  implementation. `RelationalDatasetOrchestrator` computes a topological
  generation order across entities from their declared
  `SyntheticForeignKeySpec`s (parents before children) and drives
  generation in configurable batches so that generating millions of rows
  for one entity doesn't require holding them all in memory at once.
- **Masking** (`src/masking/`): `PiiMaskingTransformer` walks a generated
  record's `isPii: true` fields and replaces them with either a
  deterministic SHA-256-derived token (`tokenize`, the default) or a fixed
  placeholder (`redact`), using Node's built-in `crypto` module.
- **Seeding** (`src/seeding/`): `DatabaseSeedingAdapter` is the port;
  `PostgresSeedingAdapter` (batched parameterized `INSERT`s over a
  DI'd `pg.Pool`) and `MongoSeedingAdapter` (batched `insertMany` over a
  DI'd `mongodb.Db`) are the concrete adapters.
- **Composition root** (`src/index.ts`): `createTestDataFactory(...)` is
  the one place that wires the generator, orchestrator, masking
  transformer, and the `DatabaseSeedingAdapter` implied by the caller's
  target datastore together, all via constructor injection of interfaces --
  no static/global database handles anywhere in `generation/` or
  `masking/`.

## Usage sketch

```ts
import { Pool } from 'pg';
import { createTestDataFactory } from './src/index';

const pgPool = new Pool({ connectionString: process.env.DATABASE_URL });

const factory = createTestDataFactory({
  targetDatastore: { kind: 'postgres', pgPool },
  defaultBatchSize: 10_000,
});

const customers = {
  entityName: 'customers',
  fields: [
    { fieldName: 'id', dataType: 'uuid' as const },
    { fieldName: 'fullName', dataType: 'string' as const },
    { fieldName: 'email', dataType: 'email' as const, isPii: true },
  ],
};

const orders = {
  entityName: 'orders',
  fields: [
    { fieldName: 'id', dataType: 'uuid' as const },
    { fieldName: 'customerId', dataType: 'uuid' as const },
    { fieldName: 'amount', dataType: 'currencyAmount' as const },
  ],
  foreignKeys: [
    { fieldName: 'customerId', referencesEntityName: 'customers', referencesFieldName: 'id' },
  ],
};

await factory.seedDataset(
  [customers, orders],
  new Map([
    ['customers', 50_000],
    ['orders', 200_000],
  ])
);
```

## Scripts

- `npm run build` -- compile TypeScript to `dist/`.
- `npm run dev` -- run `src/index.ts` with `tsx watch` for local iteration.
- `npm run lint` -- eslint over `src/`.
- `npm test` -- jest test suite.

See `SCALING.md` for how this service scales and its known starter-repo
tradeoffs.
