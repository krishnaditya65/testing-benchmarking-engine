import type { SyntheticRecord } from '../domain/synthetic-record.types';

/**
 * Summarizes the outcome of writing one batch of synthetic records into a
 * target datastore, so the caller (e.g. the DAG scheduler running a
 * `DataSeedNode` step) can log/aggregate seeding throughput.
 */
export interface SeedingOutcome {
  readonly entityName: string;
  readonly recordsInserted: number;
  readonly durationMs: number;
}

/**
 * Port (hexagonal-architecture "driven" interface) for writing generated
 * synthetic records into a concrete datastore. Concrete adapters
 * (`PostgresSeedingAdapter`, `MongoSeedingAdapter`, ...) are injected via
 * constructor DI rather than referenced statically, so the generation and
 * masking layers stay entirely datastore-agnostic.
 */
export interface DatabaseSeedingAdapter {
  seedRecords(entityName: string, records: SyntheticRecord[]): Promise<SeedingOutcome>;
}
