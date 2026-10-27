import { Injectable, InternalServerErrorException, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import axios from 'axios';
import { SecretProviderPort } from './secret-provider.port';

interface VaultKvV2ReadResponse {
  data: {
    // KV v2 wraps the actual secret payload one level deeper than KV v1.
    data: Record<string, string>;
    metadata: { version: number };
  };
}

/**
 * Resolves secrets from HashiCorp Vault's KV v2 secrets engine.
 *
 * Secret references are expected in the form
 * `vault:<mountPath>/<secretPath>#<fieldName>`, e.g.
 * `vault:kv/pipelines/load-test#apiKey`. This maps to a real KV v2 read at
 * `${VAULT_ADDR}/v1/<mountPath>/data/<secretPath>`, per Vault's KV v2 HTTP
 * API convention of inserting `data` after the mount path.
 */
@Injectable()
export class VaultSecretProvider implements SecretProviderPort {
  private readonly logger = new Logger(VaultSecretProvider.name);
  private readonly vaultAddress: string;
  private readonly vaultToken: string;

  constructor(private readonly configService: ConfigService) {
    this.vaultAddress = this.configService.get<string>('VAULT_ADDR', 'http://vault:8200');
    const token = this.configService.get<string>('VAULT_TOKEN');
    if (!token) {
      throw new Error('VAULT_TOKEN must be configured when SECRET_PROVIDER_KIND=vault');
    }
    this.vaultToken = token;
    // TODO: real deployments should use a short-lived token obtained via
    // AppRole/Kubernetes auth with periodic renewal, not a static VAULT_TOKEN.
  }

  async resolveSecret(secretReference: string): Promise<string> {
    const { mountPath, secretPath, fieldName } = this.parseVaultReference(secretReference);
    const kvV2ReadUrl = `${this.vaultAddress}/v1/${mountPath}/data/${secretPath}`;

    try {
      const response = await axios.get<VaultKvV2ReadResponse>(kvV2ReadUrl, {
        headers: { 'X-Vault-Token': this.vaultToken },
      });

      const secretValue = response.data.data.data[fieldName];
      if (secretValue === undefined) {
        throw new Error(`Field "${fieldName}" not present in Vault secret at ${mountPath}/${secretPath}`);
      }
      return secretValue;
    } catch (error) {
      this.logger.error(`Failed to read Vault secret "${secretReference}": ${this.describeError(error)}`);
      throw new InternalServerErrorException(`Unable to resolve secret reference "${secretReference}"`);
    }
  }

  private parseVaultReference(secretReference: string): {
    mountPath: string;
    secretPath: string;
    fieldName: string;
  } {
    // Expected shape: vault:<mount>/<path...>#<field>
    const match = /^vault:([^#]+)#(.+)$/.exec(secretReference);
    if (!match) {
      throw new Error(
        `Malformed Vault secret reference "${secretReference}", expected "vault:<mount>/<path>#<field>"`,
      );
    }
    const [, fullPath, fieldName] = match;
    const [mountPath, ...pathSegments] = fullPath.split('/');
    return { mountPath, secretPath: pathSegments.join('/'), fieldName };
  }

  private describeError(error: unknown): string {
    if (axios.isAxiosError(error)) {
      return `${error.response?.status ?? 'no-response'} ${JSON.stringify(error.response?.data ?? error.message)}`;
    }
    return error instanceof Error ? error.message : String(error);
  }
}
