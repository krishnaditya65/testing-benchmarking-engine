import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { AuthModule } from '../auth/auth.module';
import { ApprovalRequestEntity } from './approval-request.entity';
import { ApprovalWorkflowResolver } from './approval-workflow.resolver';
import { ApprovalWorkflowService } from './approval-workflow.service';

/**
 * Owns the human-in-the-loop approval feature (`approval_request` table).
 * Exports ApprovalWorkflowService so PolicyGateModule can create an
 * approval request in place of hard-failing a run trigger on a
 * REQUIRES_APPROVAL policy violation. Deliberately has no dependency on
 * PipelineRunModule — see the NOTE in approval-workflow.service.ts for why.
 */
@Module({
  imports: [TypeOrmModule.forFeature([ApprovalRequestEntity]), AuthModule],
  providers: [ApprovalWorkflowService, ApprovalWorkflowResolver],
  exports: [ApprovalWorkflowService],
})
export class ApprovalWorkflowModule {}
