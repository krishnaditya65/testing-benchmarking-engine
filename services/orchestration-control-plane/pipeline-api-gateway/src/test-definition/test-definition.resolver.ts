import { UseGuards } from '@nestjs/common';
import { Args, ID, Mutation, Query, Resolver } from '@nestjs/graphql';
import { AuthGuard } from '@nestjs/passport';
import { CurrentUser } from '../auth/current-user.decorator';
import { AuthenticatedUser } from '../auth/authenticated-user';
import { RbacGuard } from '../auth/rbac.guard';
import { RequiredRoles } from '../auth/roles.decorator';
import { CreatePipelineDefinitionInput } from './dto/create-pipeline-definition.input';
import { UpdatePipelineDefinitionInput } from './dto/update-pipeline-definition.input';
import { PipelineDefinitionEntity } from './pipeline-definition.entity';
import { TestDefinitionService } from './test-definition.service';

@Resolver(() => PipelineDefinitionEntity)
@UseGuards(AuthGuard('jwt'), RbacGuard)
export class TestDefinitionResolver {
  constructor(private readonly testDefinitionService: TestDefinitionService) {}

  @Query(() => PipelineDefinitionEntity, { name: 'pipelineDefinition' })
  @RequiredRoles('pipeline-admin', 'pipeline-editor', 'pipeline-viewer', 'pipeline-run-operator')
  pipelineDefinition(@Args('id', { type: () => ID }) id: string): Promise<PipelineDefinitionEntity> {
    return this.testDefinitionService.findById(id);
  }

  @Query(() => [PipelineDefinitionEntity], { name: 'pipelineDefinitions' })
  @RequiredRoles('pipeline-admin', 'pipeline-editor', 'pipeline-viewer', 'pipeline-run-operator')
  pipelineDefinitions(
    @Args('ownerId', { type: () => ID }) ownerId: string,
  ): Promise<PipelineDefinitionEntity[]> {
    return this.testDefinitionService.findAllForOwner(ownerId);
  }

  @Mutation(() => PipelineDefinitionEntity, { name: 'createPipelineDefinition' })
  @RequiredRoles('pipeline-admin', 'pipeline-editor')
  createPipelineDefinition(
    @Args('input') input: CreatePipelineDefinitionInput,
    @CurrentUser() _user: AuthenticatedUser,
  ): Promise<PipelineDefinitionEntity> {
    return this.testDefinitionService.create(input);
  }

  @Mutation(() => PipelineDefinitionEntity, { name: 'updatePipelineDefinition' })
  @RequiredRoles('pipeline-admin', 'pipeline-editor')
  updatePipelineDefinition(
    @Args('input') input: UpdatePipelineDefinitionInput,
  ): Promise<PipelineDefinitionEntity> {
    return this.testDefinitionService.update(input);
  }

  @Mutation(() => Boolean, { name: 'deletePipelineDefinition' })
  @RequiredRoles('pipeline-admin')
  deletePipelineDefinition(@Args('id', { type: () => ID }) id: string): Promise<boolean> {
    return this.testDefinitionService.delete(id);
  }
}
