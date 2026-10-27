/** DI token for the active SecretProviderPort implementation (chosen by SECRET_PROVIDER_KIND). */
export const SECRET_PROVIDER = Symbol('SECRET_PROVIDER');

/**
 * Hexagonal-architecture port for resolving a secret reference string
 * (e.g. `vault:kv/data/pipelines#apiKey` or an AWS Secrets Manager ARN)
 * into its plaintext value. Adapters implement this against a specific
 * backend; callers (secret-injection.service.ts) depend only on this
 * interface.
 */
export interface SecretProviderPort {
  /**
   * Resolves a single secret reference to its plaintext value.
   * Implementations should throw if the reference cannot be parsed or the
   * backend denies/can't find the secret — never return an empty string
   * silently, since that could inject a blank credential into a running
   * test.
   */
  resolveSecret(secretReference: string): Promise<string>;
}
