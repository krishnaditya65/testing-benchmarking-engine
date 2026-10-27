import { UseGuards } from '@nestjs/common';
import { Args, ID, Mutation, Query, Resolver } from '@nestjs/graphql';
import { AuthGuard } from '@nestjs/passport';
import { CurrentUser } from '../auth/current-user.decorator';
import { AuthenticatedUser } from '../auth/authenticated-user';
import { RbacGuard } from '../auth/rbac.guard';
import { RequiredRoles } from '../auth/roles.decorator';
import { ApprovalRequestEntity } from './approval-request.entity';
import { ApprovalWorkflowService } from './approval-workflow.service';

@Resolver(() => ApprovalRequestEntity)
@UseGuards(AuthGuard('jwt'), RbacGuard)
export class ApprovalWorkflowResolver {
  constructor(private readonly approvalWorkflowService: ApprovalWorkflowService) {}

  @Query(() => ApprovalRequestEntity, { name: 'approvalRequest' })
  @RequiredRoles('pipeline-admin', 'pipeline-editor', 'pipeline-viewer', 'pipeline-run-operator')
  approvalRequest(@Args('id', { type: () => ID }) id: string): Promise<ApprovalRequestEntity> {
    return this.approvalWorkflowService.findById(id);
  }

  @Query(() => [ApprovalRequestEntity], { name: 'pendingApprovalRequestsForRun' })
  @RequiredRoles('pipeline-admin', 'pipeline-editor', 'pipeline-viewer', 'pipeline-run-operator')
  pendingApprovalRequestsForRun(
    @Args('pipelineRunId', { type: () => ID }) pipelineRunId: string,
  ): Promise<ApprovalRequestEntity[]> {
    return this.approvalWorkflowService.findPendingForPipelineRun(pipelineRunId);
  }

  /**
   * Only pipeline-admin may approve — REQUIRES_APPROVAL violations exist
   * specifically to force a stronger sign-off than whatever role could
   * trigger the run in the first place (e.g. a pipeline-run-operator can
   * trigger a run, but cannot self-approve one their own trigger got
   * blocked on).
   */
  @Mutation(() => ApprovalRequestEntity, { name: 'approvePipelineRun' })
  @RequiredRoles('pipeline-admin')
  approvePipelineRun(
    @Args('requestId', { type: () => ID }) requestId: string,
    @Args('note', { nullable: true }) note: string | undefined,
    @CurrentUser() user: AuthenticatedUser,
  ): Promise<ApprovalRequestEntity> {
    return this.approvalWorkflowService.approve(requestId, user.userId, note);
  }

  @Mutation(() => ApprovalRequestEntity, { name: 'rejectPipelineRun' })
  @RequiredRoles('pipeline-admin')
  rejectPipelineRun(
    @Args('requestId', { type: () => ID }) requestId: string,
    @Args('note', { nullable: true }) note: string | undefined,
    @CurrentUser() user: AuthenticatedUser,
  ): Promise<ApprovalRequestEntity> {
    return this.approvalWorkflowService.reject(requestId, user.userId, note);
  }
}
