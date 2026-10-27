import { join } from 'path';
import { ApolloDriver, ApolloDriverConfig } from '@nestjs/apollo';
import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { EventEmitterModule } from '@nestjs/event-emitter';
import { GraphQLModule } from '@nestjs/graphql';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ApprovalWorkflowModule } from './approval-workflow/approval-workflow.module';
import { AuthModule } from './auth/auth.module';
import { CostEstimationModule } from './cost-estimation/cost-estimation.module';
import { PipelineRunModule } from './pipeline-run/pipeline-run.module';
import { PolicyGateModule } from './policy-gate/policy-gate.module';
import { QualityGateModule } from './quality-gate/quality-gate.module';
import { SecretInjectionModule } from './secret-injection/secret-injection.module';
import { TestDefinitionModule } from './test-definition/test-definition.module';

/**
 * Root module for pipeline-api-gateway. Wires:
 *  - TypeORM against the `control_plane` Postgres database (the
 *    `test-definition-postgres` service in docker-compose), owning
 *    pipeline_definition and pipeline_run.
 *  - GraphQL (code-first) with the subscriptions-transport-ws /
 *    graphql-ws transport enabled for pipelineRunStatusUpdated.
 *  - EventEmitter2 (`@nestjs/event-emitter`), registered globally via
 *    `forRoot()` so any module can inject `EventEmitter2` or use `@OnEvent`
 *    without needing an explicit module import. This is what lets
 *    ApprovalWorkflowService (approval-workflow.service.ts) notify
 *    PipelineRunModule's RunApprovalEventListener that a held run should
 *    resume/fail without ApprovalWorkflowModule ever importing
 *    PipelineRunModule directly — see
 *    approval-workflow/run-approval-decision.event.ts for why that import
 *    would otherwise create a module cycle.
 */
@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    EventEmitterModule.forRoot(),
    TypeOrmModule.forRootAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => ({
        type: 'postgres',
        host: configService.get<string>('CONTROL_PLANE_POSTGRES_HOST', 'test-definition-postgres'),
        port: configService.get<number>('CONTROL_PLANE_POSTGRES_PORT', 5432),
        username: configService.get<string>('CONTROL_PLANE_POSTGRES_USER', 'control_plane'),
        password: configService.get<string>('CONTROL_PLANE_POSTGRES_PASSWORD', 'control_plane'),
        database: configService.get<string>('CONTROL_PLANE_POSTGRES_DB', 'control_plane'),
        autoLoadEntities: true,
        // TODO: replace with real TypeORM migrations before production use;
        // synchronize is fine for local/starter development only.
        synchronize: configService.get<string>('NODE_ENV', 'development') !== 'production',
      }),
    }),
    GraphQLModule.forRootAsync<ApolloDriverConfig>({
      driver: ApolloDriver,
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => ({
        autoSchemaFile: join(process.cwd(), 'src/schema.gql'),
        sortSchema: true,
        playground: configService.get<string>('NODE_ENV', 'development') !== 'production',
        subscriptions: {
          'graphql-ws': true,
        },
        context: ({ req, res }: { req: unknown; res: unknown }) => ({ req, res }),
      }),
    }),
    AuthModule,
    TestDefinitionModule,
    PipelineRunModule,
    SecretInjectionModule,
    QualityGateModule,
    ApprovalWorkflowModule,
    PolicyGateModule,
    CostEstimationModule,
  ],
})
export class AppModule {}
