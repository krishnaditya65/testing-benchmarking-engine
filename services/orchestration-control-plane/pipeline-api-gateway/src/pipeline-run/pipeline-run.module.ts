import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { PubSub } from 'graphql-subscriptions';
import { AuthModule } from '../auth/auth.module';
import { CostEstimationModule } from '../cost-estimation/cost-estimation.module';
import { PolicyGateModule } from '../policy-gate/policy-gate.module';
import { TestDefinitionModule } from '../test-definition/test-definition.module';
import { DAG_WORKFLOW_SCHEDULER_CLIENT } from './dag-workflow-scheduler-client.port';
import { DagWorkflowSchedulerHttpClientAdapter } from './dag-workflow-scheduler-http-client.adapter';
import { PipelineRunEntity } from './pipeline-run.entity';
import { PIPELINE_RUN_PUB_SUB } from './pipeline-run.pubsub';
import { PipelineRunResolver } from './pipeline-run.resolver';
import { PipelineRunService } from './pipeline-run.service';
import { RunApprovalEventListener } from './run-approval-event.listener';

/**
 * Owns run lifecycle (`pipeline_run` table) and the outbound ports to
 * dag-workflow-scheduler, the policy-as-code gate, and the cost estimator.
 * Exports PipelineRunService so quality-gate can mark a run FAILED on SLA
 * breach. Imports PolicyGateModule (not the other way around) and
 * CostEstimationModule so PipelineRunService can gate/estimate a run before
 * ever calling the scheduler — see pipeline-run.service.ts's triggerRun.
 *
 * RunApprovalEventListener (registered here, not in ApprovalWorkflowModule)
 * is what resumes/fails a held run once a human resolves its
 * ApprovalRequest — it subscribes to RunApprovalDecisionEvent over
 * EventEmitter2 rather than this module importing ApprovalWorkflowModule
 * directly, which would create a cycle
 * (PipelineRunModule -> PolicyGateModule -> ApprovalWorkflowModule -> PipelineRunModule).
 * See ../approval-workflow/run-approval-decision.event.ts for the full
 * rationale. EventEmitter2 itself is available here without an explicit
 * import because EventEmitterModule.forRoot() (app.module.ts) registers it
 * as a global module.
 */
@Module({
  imports: [
    TypeOrmModule.forFeature([PipelineRunEntity]),
    TestDefinitionModule,
    AuthModule,
    PolicyGateModule,
    CostEstimationModule,
  ],
  providers: [
    PipelineRunService,
    PipelineRunResolver,
    RunApprovalEventListener,
    { provide: DAG_WORKFLOW_SCHEDULER_CLIENT, useClass: DagWorkflowSchedulerHttpClientAdapter },
    // In-memory PubSub — sufficient for a single replica / local dev only.
    // See pipeline-run.pubsub.ts and SCALING.md for the multi-replica caveat.
    { provide: PIPELINE_RUN_PUB_SUB, useValue: new PubSub() },
  ],
  exports: [PipelineRunService],
})
export class PipelineRunModule {}
