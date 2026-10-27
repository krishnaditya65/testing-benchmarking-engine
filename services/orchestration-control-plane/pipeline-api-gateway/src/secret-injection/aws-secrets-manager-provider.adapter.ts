import { Injectable, InternalServerErrorException, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { GetSecretValueCommand, SecretsManagerClient } from '@aws-sdk/client-secrets-manager';
import { SecretProviderPort } from './secret-provider.port';

/**
 * Resolves secrets from AWS Secrets Manager.
 *
 * Secret references are expected in the form
 * `aws-secrets-manager:<secretId>#<jsonFieldName>` where `<secretId>` is
 * either a secret name or full ARN, and `<jsonFieldName>` selects a key out
 * of the secret's JSON SecretString (mirroring how AWS's own console/CLI
 * tooling treats structured secrets).
 */
@Injectable()
export class AwsSecretsManagerProvider implements SecretProviderPort {
  private readonly logger = new Logger(AwsSecretsManagerProvider.name);
  private readonly client: SecretsManagerClient;

  constructor(private readonly configService: ConfigService) {
    this.client = new SecretsManagerClient({
      region: this.configService.get<string>('AWS_REGION', 'us-east-1'),
    });
  }

  async resolveSecret(secretReference: string): Promise<string> {
    const { secretId, jsonFieldName } = this.parseAwsReference(secretReference);

    try {
      const response = await this.client.send(new GetSecretValueCommand({ SecretId: secretId }));
      if (!response.SecretString) {
        throw new Error(`Secret "${secretId}" has no SecretString value (binary secrets are unsupported)`);
      }

      if (!jsonFieldName) {
        return response.SecretString;
      }

      const parsed = JSON.parse(response.SecretString) as Record<string, string>;
      const value = parsed[jsonFieldName];
      if (value === undefined) {
        throw new Error(`Field "${jsonFieldName}" not present in secret "${secretId}"`);
      }
      return value;
    } catch (error) {
      this.logger.error(
        `Failed to read AWS Secrets Manager secret "${secretReference}": ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      throw new InternalServerErrorException(`Unable to resolve secret reference "${secretReference}"`);
    }
  }

  private parseAwsReference(secretReference: string): { secretId: string; jsonFieldName?: string } {
    // Expected shape: aws-secrets-manager:<secretIdOrArn>[#<jsonField>]
    const match = /^aws-secrets-manager:([^#]+)(?:#(.+))?$/.exec(secretReference);
    if (!match) {
      throw new Error(
        `Malformed AWS Secrets Manager reference "${secretReference}", expected "aws-secrets-manager:<secretId>[#<field>]"`,
      );
    }
    const [, secretId, jsonFieldName] = match;
    return { secretId, jsonFieldName };
  }
}
