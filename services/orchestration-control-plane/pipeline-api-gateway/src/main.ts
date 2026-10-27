import 'reflect-metadata';
import { ValidationPipe } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { NestFactory } from '@nestjs/core';
import {
  correlationIdPropagationMiddleware,
  createStructuredLogger,
  getCurrentCorrelationId,
  initializeOpenTelemetryInstrumentation,
} from '@shared-libraries/observability-kit';
import { AppModule } from './app.module';

// This platform's own control plane dogfoods the observability kit it ships to every other
// service (see ARCHITECTURE.md's shared-libraries description) rather than rolling its own
// bootstrap/logging conventions. `initializeOpenTelemetryInstrumentation` must run before
// `AppModule`'s providers (Postgres via TypeORM, HTTP clients via axios, etc.) are constructed
// so OTel's auto-instrumentation can actually patch those modules' exports on first `require`.
const openTelemetrySdk = initializeOpenTelemetryInstrumentation('pipeline-api-gateway');

// Every structured log line this entrypoint emits shares the platform-wide JSON shape
// (service, level, correlationId, ...) documented in structured-logger-factory.ts, instead of
// the ad hoc `console.log` this file used before this hardening pass.
const logger = createStructuredLogger('pipeline-api-gateway', {
  correlationIdProvider: getCurrentCorrelationId,
});

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule);
  const configService = app.get(ConfigService);

  // Binds/mints x-correlation-id for every inbound request before any resolver or guard runs,
  // so every log line and outbound call made while handling that request can be tied back to
  // it — see correlation-id-propagation-middleware.ts for why this is a platform-level id
  // distinct from an OTel trace id.
  app.use(correlationIdPropagationMiddleware);

  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      transform: true,
      forbidNonWhitelisted: true,
    }),
  );

  app.enableCors({
    origin: configService.get<string>('FRONTEND_ORIGIN', 'http://localhost:3000'),
    credentials: true,
  });

  const port = configService.get<number>('PORT', 4000);
  await app.listen(port);
  logger.info(`pipeline-api-gateway listening on port ${port}`, { port });

  const shutdownSignals = ['SIGTERM', 'SIGINT'] as const;
  for (const signal of shutdownSignals) {
    process.once(signal, () => {
      logger.info(`received ${signal}, shutting down OpenTelemetry SDK`, { signal });
      void openTelemetrySdk
        .shutdown()
        .catch((error: unknown) =>
          logger.error('error shutting down OpenTelemetry SDK', { error: String(error) }),
        );
    });
  }
}

bootstrap();
