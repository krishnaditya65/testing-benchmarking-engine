import { createHash } from 'node:crypto';
import type { SyntheticEntitySchema, SyntheticFieldSpec } from '../domain/synthetic-schema.types';
import type { SyntheticRecord } from '../domain/synthetic-record.types';

/**
 * How a PII-flagged field should be transformed before it's safe to seed
 * into a shared/non-production datastore.
 *
 * - `tokenize`: deterministically derive a masked-but-shaped replacement
 *   value from the original (e.g. an email keeps its domain, everything
 *   else becomes a short hash-derived token). Deterministic so the same
 *   input always tokenizes the same way, which keeps referential joins on
 *   masked values stable across re-runs.
 * - `redact`: replace the value entirely with a fixed placeholder,
 *   discarding all shape/content.
 */
export type PiiMaskingStrategy = 'tokenize' | 'redact';

const DEFAULT_STRATEGY_BY_DATA_TYPE: Partial<Record<SyntheticFieldSpec['dataType'], PiiMaskingStrategy>> = {
  email: 'tokenize',
  string: 'tokenize',
  uuid: 'tokenize',
  integer: 'redact',
  boolean: 'redact',
  timestamp: 'redact',
  currencyAmount: 'redact',
};

const REDACTED_PLACEHOLDER = '[REDACTED]';

/**
 * Masks or tokenizes PII-flagged fields on a generated record, using
 * `node:crypto`'s SHA-256 to derive deterministic, irreversible tokens.
 *
 * Wired in ahead of a `DatabaseSeedingAdapter` in the composition root
 * (`src/index.ts`) so that no raw PII-shaped synthetic value (which, even
 * though fake, may still resemble real customer data closely enough to be
 * treated as sensitive by policy) reaches a seeded database un-masked.
 */
export class PiiMaskingTransformer {
  constructor(
    /** Overrides the default per-data-type strategy for specific field names. */
    private readonly strategyOverridesByFieldName: Map<string, PiiMaskingStrategy> = new Map()
  ) {}

  /**
   * Returns a new record with every field flagged `isPii: true` on the
   * schema replaced by its masked/tokenized value. Non-PII fields pass
   * through unchanged.
   */
  maskRecord(record: SyntheticRecord, schema: SyntheticEntitySchema): SyntheticRecord {
    const masked: SyntheticRecord = { ...record };

    for (const field of schema.fields) {
      if (!field.isPii) {
        continue;
      }
      const originalValue = record[field.fieldName];
      const strategy = this.resolveStrategy(field);
      masked[field.fieldName] = this.applyStrategy(strategy, field, originalValue);
    }

    return masked;
  }

  private resolveStrategy(field: SyntheticFieldSpec): PiiMaskingStrategy {
    return (
      this.strategyOverridesByFieldName.get(field.fieldName) ??
      DEFAULT_STRATEGY_BY_DATA_TYPE[field.dataType] ??
      'tokenize'
    );
  }

  private applyStrategy(
    strategy: PiiMaskingStrategy,
    field: SyntheticFieldSpec,
    value: unknown
  ): unknown {
    switch (strategy) {
      case 'redact':
        return REDACTED_PLACEHOLDER;
      case 'tokenize':
        return field.dataType === 'email'
          ? this.tokenizeEmail(value)
          : this.tokenizeGenericValue(value);
      default: {
        const unreachable: never = strategy;
        throw new Error(`Unsupported PII masking strategy: ${String(unreachable)}`);
      }
    }
  }

  /**
   * Tokenizes an email while preserving its domain shape, so masked data
   * still round-trips through validation like "is this a plausible email"
   * downstream: `user_<8-hex-char-hash>@example.invalid`.
   */
  private tokenizeEmail(value: unknown): string {
    const originalEmail = String(value ?? '');
    const token = this.hashToToken(originalEmail).slice(0, 8);
    return `user_${token}@example.invalid`;
  }

  /**
   * Irreversibly tokenizes a generic PII value into a short, stable,
   * non-reversible token derived from a SHA-256 digest of its string form.
   */
  private tokenizeGenericValue(value: unknown): string {
    const token = this.hashToToken(String(value ?? '')).slice(0, 12);
    return `tok_${token}`;
  }

  private hashToToken(input: string): string {
    return createHash('sha256').update(input).digest('hex');
  }
}
