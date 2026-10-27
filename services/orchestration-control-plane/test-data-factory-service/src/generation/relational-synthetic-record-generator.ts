import { faker } from '@faker-js/faker';
import type {
  SyntheticEntitySchema,
  SyntheticFieldSpec,
  SyntheticForeignKeySpec,
} from '../domain/synthetic-schema.types';
import type {
  SyntheticGenerationResult,
  SyntheticRecord,
} from '../domain/synthetic-record.types';
import type { SyntheticRecordGenerator } from './synthetic-record-generator.port';

/**
 * Thrown when a foreign key on a schema references a parent entity whose
 * record pool wasn't supplied (or was empty) at generation time. In
 * practice this always means the caller generated entities out of
 * topological order -- e.g. generated `orders` before `customers` -- which
 * is exactly the class of bug `RelationalDatasetOrchestrator` exists to
 * prevent.
 */
export class MissingReferencedRecordPoolError extends Error {
  constructor(fk: SyntheticForeignKeySpec, owningEntityName: string) {
    super(
      `Cannot resolve foreign key "${owningEntityName}.${fk.fieldName}" -> ` +
        `"${fk.referencesEntityName}.${fk.referencesFieldName}": no records for ` +
        `"${fk.referencesEntityName}" were found in referencedRecordPools. This usually means ` +
        `entities are being generated out of topological order -- generate parent entities ` +
        `before entities that hold a foreign key to them.`
    );
    this.name = 'MissingReferencedRecordPoolError';
  }
}

/**
 * Generates synthetic records for a single `SyntheticEntitySchema`.
 *
 * Non-foreign-key fields are fabricated with `@faker-js/faker`, dispatched
 * first on `dataType` and, for free-form strings, further refined by a
 * heuristic scan of the field name (e.g. a `string` field named
 * "billingEmail" gets a real-looking email rather than lorem text).
 *
 * Foreign-key fields are resolved by sampling a real value out of the
 * matching parent pool in `referencedRecordPools`, which is what makes the
 * output referentially consistent rather than just independently random
 * per entity.
 */
export class RelationalSyntheticRecordGenerator implements SyntheticRecordGenerator {
  generate(
    schema: SyntheticEntitySchema,
    recordCount: number,
    referencedRecordPools: Map<string, SyntheticRecord[]>
  ): SyntheticGenerationResult {
    const foreignKeysByFieldName = new Map(
      (schema.foreignKeys ?? []).map((fk) => [fk.fieldName, fk])
    );

    const records: SyntheticRecord[] = [];
    for (let i = 0; i < recordCount; i++) {
      const record: SyntheticRecord = {};
      for (const field of schema.fields) {
        const fk = foreignKeysByFieldName.get(field.fieldName);
        record[field.fieldName] = fk
          ? this.resolveForeignKeyValue(fk, schema.entityName, referencedRecordPools)
          : this.generateFieldValue(field);
      }
      records.push(record);
    }

    return { entityName: schema.entityName, records };
  }

  /** Picks a real, already-generated value from the referenced parent pool. */
  private resolveForeignKeyValue(
    fk: SyntheticForeignKeySpec,
    owningEntityName: string,
    referencedRecordPools: Map<string, SyntheticRecord[]>
  ): unknown {
    const parentPool = referencedRecordPools.get(fk.referencesEntityName);
    if (!parentPool || parentPool.length === 0) {
      throw new MissingReferencedRecordPoolError(fk, owningEntityName);
    }
    const parentRecord = faker.helpers.arrayElement(parentPool);
    return parentRecord[fk.referencesFieldName];
  }

  /** Dispatches on `dataType`, with a field-name heuristic for free-form strings. */
  private generateFieldValue(field: SyntheticFieldSpec): unknown {
    switch (field.dataType) {
      case 'uuid':
        return faker.string.uuid();
      case 'integer':
        return faker.number.int({ min: 1, max: 1_000_000 });
      case 'boolean':
        return faker.datatype.boolean();
      case 'timestamp':
        return faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: new Date() });
      case 'email':
        return faker.internet.email().toLowerCase();
      case 'currencyAmount':
        return Number(faker.finance.amount({ min: 0, max: 100_000, dec: 2 }));
      case 'string':
        return this.generateHeuristicStringValue(field.fieldName);
      default: {
        // Exhaustiveness guard: if a new SyntheticFieldDataType is added
        // without updating this switch, this branch fails to compile.
        const unreachable: never = field.dataType;
        throw new Error(`Unsupported synthetic field data type: ${String(unreachable)}`);
      }
    }
  }

  /**
   * A free-form `string` field's real-world meaning lives in its name
   * (e.g. "firstName", "companyName", "streetAddress"), so before falling
   * back to generic lorem text we scan the field name for common
   * conventions and dispatch to the matching faker module.
   */
  private generateHeuristicStringValue(fieldName: string): string {
    const normalized = fieldName.toLowerCase();

    if (normalized.includes('email')) {
      return faker.internet.email().toLowerCase();
    }
    if (normalized.includes('firstname')) {
      return faker.person.firstName();
    }
    if (normalized.includes('lastname')) {
      return faker.person.lastName();
    }
    if (normalized.includes('fullname') || normalized === 'name') {
      return faker.person.fullName();
    }
    if (normalized.includes('company')) {
      return faker.company.name();
    }
    if (normalized.includes('phone')) {
      return faker.phone.number();
    }
    if (normalized.includes('street') || normalized.includes('address')) {
      return faker.location.streetAddress();
    }
    if (normalized.includes('city')) {
      return faker.location.city();
    }
    if (normalized.includes('state')) {
      return faker.location.state();
    }
    if (normalized.includes('country')) {
      return faker.location.country();
    }
    if (normalized.includes('zip') || normalized.includes('postalcode')) {
      return faker.location.zipCode();
    }
    if (normalized.includes('url') || normalized.includes('website')) {
      return faker.internet.url();
    }
    if (normalized.includes('username')) {
      return faker.internet.userName();
    }
    if (normalized.includes('description') || normalized.includes('notes')) {
      return faker.lorem.sentence();
    }
    if (normalized.includes('status')) {
      return faker.helpers.arrayElement(['PENDING', 'ACTIVE', 'COMPLETED', 'CANCELLED']);
    }
    if (normalized.includes('sku') || normalized.includes('code')) {
      return faker.string.alphanumeric({ length: 8 }).toUpperCase();
    }

    // Generic fallback for field names we don't have a specific heuristic for.
    return faker.lorem.words({ min: 1, max: 3 });
  }
}

/**
 * Sorts `SyntheticEntitySchema`s into a valid generation order (parents
 * before children) using a standard Kahn's-algorithm topological sort over
 * the foreign-key edges declared on each schema, then drives
 * `SyntheticRecordGenerator` across that order in fixed-size batches so
 * that generating "millions of rows" for a single entity never requires
 * holding the full record set for that entity in memory at once.
 *
 * NOTE (starter-repo tradeoff): the record *pool* used for foreign-key
 * resolution for a parent entity is still accumulated fully in memory
 * across that parent's batches (see `entityRecordPools`), because a child
 * batch may need to reference any parent record generated so far. For
 * genuinely "millions of rows" parent tables this pool would need to be
 * capped/sampled or backed by a lightweight external key store rather than
 * an in-process array -- left as a TODO, see SCALING.md.
 */
export class RelationalDatasetOrchestrator {
  constructor(
    private readonly generator: SyntheticRecordGenerator,
    private readonly defaultBatchSize = 10_000
  ) {}

  /**
   * Computes a topological generation order across every schema's foreign
   * keys. Throws if the schemas contain a foreign-key cycle, since no valid
   * generation order exists in that case.
   */
  computeTopologicalGenerationOrder(schemas: SyntheticEntitySchema[]): SyntheticEntitySchema[] {
    const schemaByName = new Map(schemas.map((s) => [s.entityName, s]));
    const inDegree = new Map<string, number>(schemas.map((s) => [s.entityName, 0]));
    const dependents = new Map<string, string[]>(schemas.map((s) => [s.entityName, []]));

    for (const schema of schemas) {
      for (const fk of schema.foreignKeys ?? []) {
        if (!schemaByName.has(fk.referencesEntityName)) {
          // A dangling FK reference (parent schema not included in this
          // batch) is treated as "already satisfied" rather than an error,
          // since the parent may have been generated in a prior job run.
          continue;
        }
        inDegree.set(schema.entityName, (inDegree.get(schema.entityName) ?? 0) + 1);
        dependents.get(fk.referencesEntityName)?.push(schema.entityName);
      }
    }

    const queue: string[] = schemas
      .map((s) => s.entityName)
      .filter((name) => (inDegree.get(name) ?? 0) === 0);
    const orderedNames: string[] = [];

    while (queue.length > 0) {
      const current = queue.shift() as string;
      orderedNames.push(current);
      for (const dependent of dependents.get(current) ?? []) {
        const remaining = (inDegree.get(dependent) ?? 0) - 1;
        inDegree.set(dependent, remaining);
        if (remaining === 0) {
          queue.push(dependent);
        }
      }
    }

    if (orderedNames.length !== schemas.length) {
      const unresolved = schemas
        .map((s) => s.entityName)
        .filter((name) => !orderedNames.includes(name));
      throw new Error(
        `Cannot compute a generation order: foreign-key cycle detected among entities ` +
          `[${unresolved.join(', ')}]`
      );
    }

    return orderedNames.map((name) => schemaByName.get(name) as SyntheticEntitySchema);
  }

  /**
   * Generates records for every schema in topological order, yielding one
   * `SyntheticGenerationResult` per batch per entity. Consumers (e.g. a
   * `DatabaseSeedingAdapter`) can seed each batch as it's produced instead
   * of waiting for the whole dataset, keeping peak memory bounded by
   * `batchSize` rather than by total record count.
   */
  *generateBatchesInDependencyOrder(
    schemas: SyntheticEntitySchema[],
    recordCountByEntityName: Map<string, number>,
    batchSize: number = this.defaultBatchSize
  ): Generator<SyntheticGenerationResult> {
    const generationOrder = this.computeTopologicalGenerationOrder(schemas);

    // Accumulated per-entity record pools, used as the FK-resolution source
    // for entities generated later in the order. See class-level TODO note
    // about the memory tradeoff this implies for very large parent tables.
    const entityRecordPools = new Map<string, SyntheticRecord[]>();

    for (const schema of generationOrder) {
      const totalRecordCount = recordCountByEntityName.get(schema.entityName) ?? 0;
      const pool: SyntheticRecord[] = [];
      entityRecordPools.set(schema.entityName, pool);

      let remaining = totalRecordCount;
      while (remaining > 0) {
        const countThisBatch = Math.min(batchSize, remaining);
        const result = this.generator.generate(schema, countThisBatch, entityRecordPools);
        pool.push(...result.records);
        remaining -= countThisBatch;
        yield result;
      }
    }
  }
}
