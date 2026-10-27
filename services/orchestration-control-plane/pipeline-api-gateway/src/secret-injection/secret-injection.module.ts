import { Injectable, Logger, Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { AwsSecretsManagerProvider } from './aws-secrets-manager-provider.adapter';
import { SECRET_PROVIDER } from './secret-provider.port';
import { SecretInjectionService } from './secret-injection.service';
import { VaultSecretProvider } from './vault-secret-provider.adapter';
import { SecretFreshnessTracker } from './secret-freshness-tracker';
import {
  MidRunSecretRotationScheduler,
  ROTATION_NOTIFICATION_PORT,
  RotatedSecretNotification,
  RotationNotificationPort,
} from './mid-run-secret-rotation-scheduler';

/**
 * Logging-only RotationNotificationPort adapter used until a real transport (most plausibly a
 * Redpanda-backed adapter following this platform's "control plane -> workers" contract, see
 * the port's own doc comment) is built. Keeps MidRunSecretRotationScheduler fully wireable and
 * exercisable today without a fan-out mechanism existing yet — see
 * docs/architecture-decision-records/0011-cross-cutting-robustness-hardening.md.
 */
@Injectable()
class LoggingOnlyRotationNotificationPort implements RotationNotificationPort {
  private readonly logger = new Logger(LoggingOnlyRotationNotificationPort.name);

  async publishRotatedSecret(notification: RotatedSecretNotification): Promise<void> {
    this.logger.log(
      `Secret "${notification.secretReference}" rotated at ${new Date(notification.rotatedAtEpochMillis).toISOString()}; ` +
        'no RotationNotificationPort transport is configured yet, so no execution-fleet worker was notified (see mid-run-secret-rotation-scheduler.ts).',
    );
  }
}

/**
 * Registers the SecretProviderPort implementation selected by the
 * SECRET_PROVIDER_KIND env var ("vault" | "aws-secrets-manager"), keeping
 * SecretInjectionService and its callers entirely unaware of which backend
 * is active. Also registers the mid-run secret rotation collaborators
 * (SecretFreshnessTracker, MidRunSecretRotationScheduler, and a
 * placeholder RotationNotificationPort adapter) — see
 * mid-run-secret-rotation-scheduler.ts for why a 6-hour load test needs
 * these.
 */
@Module({
  imports: [ConfigModule],
  providers: [
    {
      provide: SECRET_PROVIDER,
      useFactory: (configService: ConfigService) => {
        const kind = configService.get<string>('SECRET_PROVIDER_KIND', 'vault');
        switch (kind) {
          case 'vault':
            return new VaultSecretProvider(configService);
          case 'aws-secrets-manager':
            return new AwsSecretsManagerProvider(configService);
          default:
            throw new Error(
              `Unsupported SECRET_PROVIDER_KIND "${kind}", expected "vault" or "aws-secrets-manager"`,
            );
        }
      },
      inject: [ConfigService],
    },
    SecretInjectionService,
    SecretFreshnessTracker,
    {
      provide: ROTATION_NOTIFICATION_PORT,
      useClass: LoggingOnlyRotationNotificationPort,
    },
    MidRunSecretRotationScheduler,
  ],
  exports: [SecretInjectionService, SecretFreshnessTracker, MidRunSecretRotationScheduler],
})
export class SecretInjectionModule {}
