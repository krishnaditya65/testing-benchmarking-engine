import type { Pool } from 'pg';
import type { SyntheticRecord } from '../domain/synthetic-record.types';
import type { DatabaseSeedingAdapter, SeedingOutcome } from './database-seeding-adapter.port';

/**
 * Seeds synthetic records into Postgres via batched, parameterized
 * `INSERT INTO ... VALUES ...` statements.
 *
 * The `pg.Pool` is constructor-injected (not opened/owned by this class),
 * so connection lifecycle and pooling policy stay the composition root's
 * responsibility -- this class only ever borrows a connection per query via
 * `pool.query`.
 */
export class PostgresSeedingAdapter implements DatabaseSeedingAdapter {
  constructor(
    private readonly pool: Pool,
    /** Max records per single INSERT statement, to avoid one gigantic multi-row statement. */
    private readonly insertBatchSize = 500
  ) {}

  async seedRecords(entityName: string, records: SyntheticRecord[]): Promise<SeedingOutcome> {
    const startedAtMs = Date.now();
    if (records.length === 0) {
      return { entityName, recordsInserted: 0, durationMs: Date.now() - startedAtMs };
    }

    const columnNames = Object.keys(records[0] as SyntheticRecord);
    let recordsInserted = 0;

    for (let offset = 0; offset < records.length; offset += this.insertBatchSize) {
      const chunk = records.slice(offset, offset + this.insertBatchSize);
      const { sql, parameters } = this.buildParameterizedInsertStatement(
        entityName,
        columnNames,
        chunk
      );
      await this.pool.query(sql, parameters);
      recordsInserted += chunk.length;
    }

    return { entityName, recordsInserted, durationMs: Date.now() - startedAtMs };
  }

  /**
   * Builds a single parameterized `INSERT INTO "entity" ("col1", "col2") VALUES
   * ($1, $2), ($3, $4), ...` statement for one chunk of records, so every
   * value -- including PII-masked tokens and generated strings -- goes over
   * the wire as a bound parameter rather than interpolated SQL text.
   */
  private buildParameterizedInsertStatement(
    entityName: string,
    columnNames: string[],
    records: SyntheticRecord[]
  ): { sql: string; parameters: unknown[] } {
    const quotedTable = this.quoteIdentifier(entityName);
    const quotedColumns = columnNames.map((c) => this.quoteIdentifier(c)).join(', ');

    const parameters: unknown[] = [];
    const valueRowPlaceholders: string[] = [];

    for (const record of records) {
      const rowPlaceholders: string[] = [];
      for (const column of columnNames) {
        parameters.push(record[column]);
        rowPlaceholders.push(`$${parameters.length}`);
      }
      valueRowPlaceholders.push(`(${rowPlaceholders.join(', ')})`);
    }

    const sql = `INSERT INTO ${quotedTable} (${quotedColumns}) VALUES ${valueRowPlaceholders.join(', ')}`;
    return { sql, parameters };
  }

  private quoteIdentifier(identifier: string): string {
    return `"${identifier.replace(/"/g, '""')}"`;
  }
}
