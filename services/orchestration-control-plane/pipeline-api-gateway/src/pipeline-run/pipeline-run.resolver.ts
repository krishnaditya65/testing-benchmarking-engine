import { Inject, UseGuards } from '@nestjs/common';
import { Args, ID, Mutation, Query, Resolver, Subscription } from '@nestjs/graphql';
import { AuthGuard } from '@nestjs/passport';
import { PubSub } from 'graphql-subscriptions';
import { AuthenticatedUser } from '../auth/authenticated-user';
import { CurrentUser } from '../auth/current-user.decorator';
import { RbacGuard } from '../auth/rbac.guard';
import { RequiredRoles } from '../auth/roles.decorator';
import { PipelineRunEntity } from './pipeline-run.entity';
import { PipelineRunService, PIPELINE_RUN_STATUS_UPDATED_EVENT } from './pipeline-run.service';
import { PIPELINE_RUN_PUB_SUB } from './pipeline-run.pubsub';

@Resolver(() => PipelineRunEntity)
@UseGuards(AuthGuard('jwt'), RbacGuard)
export class PipelineRunResolver {
  constructor(
    private readonly pipelineRunService: PipelineRunService,
    @Inject(PIPELINE_RUN_PUB_SUB) private readonly pubSub: PubSub,
  ) {}

  @Query(() => PipelineRunEntity, { name: 'pipelineRun' })
  @RequiredRoles('pipeline-admin', 'pipeline-editor', 'pipeline-viewer', 'pipeline-run-operator')
  pipelineRun(@Args('id', { type: () => ID }) id: string): Promise<PipelineRunEntity> {
    return this.pipelineRunService.getRunById(id);
  }

  @Mutation(() => PipelineRunEntity, { name: 'triggerPipelineRun' })
  @RequiredRoles('pipeline-admin', 'pipeline-editor', 'pipeline-run-operator')
  triggerPipelineRun(
    @Args('pipelineDefinitionId', { type: () => ID }) pipelineDefinitionId: string,
    @Args('targetNamespace', { nullable: true }) targetNamespace: string | undefined,
    @CurrentUser() user: AuthenticatedUser,
  ): Promise<PipelineRunEntity> {
    return this.pipelineRunService.triggerRun(pipelineDefinitionId, user.userId, targetNamespace);
  }

  @Mutation(() => PipelineRunEntity, { name: 'cancelPipelineRun' })
  @RequiredRoles('pipeline-admin', 'pipeline-editor', 'pipeline-run-operator')
  cancelPipelineRun(@Args('runId', { type: () => ID }) runId: string): Promise<PipelineRunEntity> {
    return this.pipelineRunService.cancelRun(runId);
  }

  /**
   * Live run-status feed consumed by the frontend over GraphQL subscriptions
   * (graphql-ws). Uses PubSub.asyncIterator over the shared event name and
   * filters server-side so a client subscribed to one runId never receives
   * another run's updates.
   */
  @Subscription(() => PipelineRunEntity, {
    name: 'pipelineRunStatusUpdated',
    filter: (payload: { pipelineRunStatusUpdated: PipelineRunEntity }, variables: { runId: string }) =>
      payload.pipelineRunStatusUpdated.id === variables.runId,
  })
  @RequiredRoles('pipeline-admin', 'pipeline-editor', 'pipeline-viewer', 'pipeline-run-operator')
  pipelineRunStatusUpdated(@Args('runId', { type: () => ID }) _runId: string) {
    return this.pubSub.asyncIterator(PIPELINE_RUN_STATUS_UPDATED_EVENT);
  }
}
