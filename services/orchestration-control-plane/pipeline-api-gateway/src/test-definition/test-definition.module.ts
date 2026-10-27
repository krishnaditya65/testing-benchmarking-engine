import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { AuthModule } from '../auth/auth.module';
import { PipelineDefinitionDiffResolver } from './pipeline-definition-diff.resolver';
import { PipelineDefinitionDiffService } from './pipeline-definition-diff.service';
import { PipelineDefinitionVersionHistoryEntity } from './pipeline-definition-version-history.entity';
import { PipelineDefinitionEntity } from './pipeline-definition.entity';
import { TestDefinitionResolver } from './test-definition.resolver';
import { TestDefinitionService } from './test-definition.service';

/**
 * Owns CRUD + validation for DAG pipeline test definitions
 * (`pipeline_definition` table), the append-only version history that
 * backs "DAG definition versioning & structural diffing"
 * (`pipeline_definition_version_history` table), and the
 * `pipelineDefinitionDiff` GraphQL query over that history. Exports
 * TestDefinitionService so pipeline-run can look up a definition's DAG
 * before triggering a run.
 */
@Module({
  imports: [
    TypeOrmModule.forFeature([PipelineDefinitionEntity, PipelineDefinitionVersionHistoryEntity]),
    AuthModule,
  ],
  providers: [
    TestDefinitionService,
    TestDefinitionResolver,
    PipelineDefinitionDiffService,
    PipelineDefinitionDiffResolver,
  ],
  exports: [TestDefinitionService, PipelineDefinitionDiffService],
})
export class TestDefinitionModule {}
