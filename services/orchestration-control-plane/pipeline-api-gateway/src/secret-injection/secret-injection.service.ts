import { Inject, Injectable } from '@nestjs/common';
import { SECRET_PROVIDER, SecretProviderPort } from './secret-provider.port';
import { SecretFreshnessTracker } from './secret-freshness-tracker';

/**
 * Matches placeholders of the form `${{secret:<reference>}}` embedded
 * anywhere inside a string value of a pipeline node's config, e.g.
 * `${{secret:vault:kv/pipelines/load-test#apiKey}}` or
 * `${{secret:aws-secrets-manager:prod/api-keys#stripe}}`.
 */
const SECRET_PLACEHOLDER_PATTERN = /\$\{\{secret:([^}]+)\}\}/g;

/**
 * Facade over SecretProviderPort that walks a pipeline node's `config`
 * object (see PipelineNode.config in dag-domain-types.ts) and replaces any
 * `${{secret:...}}` placeholders with values resolved from the injected
 * secret backend. Depends only on SecretProviderPort — never a concrete
 * Vault/AWS adapter — so the backend is swappable via SECRET_PROVIDER_KIND.
 */
@Injectable()
export class SecretInjectionService {
  constructor(
    @Inject(SECRET_PROVIDER) private readonly secretProvider: SecretProviderPort,
    // Records issuance time for every secret reference resolved through this service, so
    // MidRunSecretRotationScheduler (mid-run-secret-rotation-scheduler.ts) knows which
    // injected secrets are approaching their predicted expiry during a long-running pipeline
    // run and can proactively re-fetch them — see that module's doc comment.
    private readonly freshnessTracker: SecretFreshnessTracker,
  ) {}

  /**
   * Returns a deep copy of `nodeConfig` with every secret placeholder
   * resolved. String values are scanned in-place (supporting placeholders
   * embedded inside a larger string, e.g. `"Bearer ${{secret:...}}"`);
   * arrays and nested objects are walked recursively.
   */
  async injectSecretsIntoNodeConfig(
    nodeConfig: Record<string, unknown>,
  ): Promise<Record<string, unknown>> {
    return this.resolveValue(nodeConfig) as Promise<Record<string, unknown>>;
  }

  private async resolveValue(value: unknown): Promise<unknown> {
    if (typeof value === 'string') {
      return this.resolveStringPlaceholders(value);
    }
    if (Array.isArray(value)) {
      return Promise.all(value.map((item) => this.resolveValue(item)));
    }
    if (value !== null && typeof value === 'object') {
      const entries = await Promise.all(
        Object.entries(value as Record<string, unknown>).map(async ([key, nestedValue]) => [
          key,
          await this.resolveValue(nestedValue),
        ]),
      );
      return Object.fromEntries(entries);
    }
    return value;
  }

  private async resolveStringPlaceholders(value: string): Promise<string> {
    const matches = [...value.matchAll(SECRET_PLACEHOLDER_PATTERN)];
    if (matches.length === 0) {
      return value;
    }

    let resolved = value;
    for (const match of matches) {
      const [placeholder, secretReference] = match;
      const trimmedReference = secretReference.trim();
      const secretValue = await this.secretProvider.resolveSecret(trimmedReference);
      // Every fresh resolution starts this reference's rotation clock: a node config resolved
      // now for a pipeline run that goes on to run for hours needs MidRunSecretRotationScheduler
      // to know "issued now" in order to predict when it next needs re-fetching.
      this.freshnessTracker.recordIssued(trimmedReference);
      resolved = resolved.replace(placeholder, secretValue);
    }
    return resolved;
  }
}
