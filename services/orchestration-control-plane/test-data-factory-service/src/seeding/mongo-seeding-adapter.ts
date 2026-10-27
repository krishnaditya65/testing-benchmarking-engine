import type { Db, Document } from 'mongodb';
import type { SyntheticRecord } from '../domain/synthetic-record.types';
import type { DatabaseSeedingAdapter, SeedingOutcome } from './database-seeding-adapter.port';

/**
 * Seeds synthetic records into MongoDB via batched, unordered
 * `insertMany` calls.
 *
 * The `mongodb.Db` handle is constructor-injected -- this class never opens
 * or closes a connection itself, matching the same DI shape as
 * `PostgresSeedingAdapter`.
 */
export class MongoSeedingAdapter implements DatabaseSeedingAdapter {
  constructor(
    private readonly db: Db,
    /** Max documents per single insertMany call, keeping each write batch bounded. */
    private readonly insertBatchSize = 1_000
  ) {}

  async seedRecords(entityName: string, records: SyntheticRecord[]): Promise<SeedingOutcome> {
    const startedAtMs = Date.now();
    if (records.length === 0) {
      return { entityName, recordsInserted: 0, durationMs: Date.now() - startedAtMs };
    }

    const collection = this.db.collection(entityName);
    let recordsInserted = 0;

    for (let offset = 0; offset < records.length; offset += this.insertBatchSize) {
      const chunk = records.slice(offset, offset + this.insertBatchSize) as Document[];
      // `ordered: false` lets independent-batch inserts continue past a
      // single bad document instead of aborting the whole chunk, which
      // matters when seeding millions of synthetic rows.
      const result = await collection.insertMany(chunk, { ordered: false });
      recordsInserted += result.insertedCount;
    }

    return { entityName, recordsInserted, durationMs: Date.now() - startedAtMs };
  }
}
