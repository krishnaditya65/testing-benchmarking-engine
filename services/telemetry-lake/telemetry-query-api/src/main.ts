import "reflect-metadata";
import { NestFactory } from "@nestjs/core";
import { TelemetryQueryModule } from "./telemetry-query-module";

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(TelemetryQueryModule);
  const port = Number(process.env.PORT ?? 4500);
  await app.listen(port);
  // eslint-disable-next-line no-console
  console.log(`telemetry-query-api listening on :${port}`);
}

bootstrap();
