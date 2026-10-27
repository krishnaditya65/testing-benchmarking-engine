import { ForbiddenException, Inject, Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { PubSub } from 'graphql-subscriptions';
import { COST_ESTIMATOR_CLIENT, CostEstimatorClientPort } from '../cost-estimation/cost-estimator-client.port';
import { PolicyGateService } from '../policy-gate/policy-gate.service';
import { TestDefinitionService } from '../test-definition/test-definition.service';
import {
  DAG_WORKFLOW_SCHEDULER_CLIENT,
  DagWorkflowSchedulerClientPort,
} from './dag-workflow-scheduler-client.port';
import { PipelineRunEntity, PipelineRunNodeStatus } from './pipeline-run.entity';
import { PipelineRunStatus } from './pipeline-run-status.enum';
import { PIPELINE_RUN_PUB_SUB } from './pipeline-run.pubsub';

/** Published on PIPELINE_RUN_PUB_SUB whenever a run's status changes, filtered by runId in the resolver. */
export const PIPELINE_RUN_STATUS_UPDATED_EVENT = 'pipelineRunStatusUpdated';

@Injectable()
export class PipelineRunService {
  private readonly logger = new Logger(PipelineRunService.name);

  constructor(
    @InjectRepository(PipelineRunEntity)
    private readonly pipelineRunRepository: Repository<PipelineRunEntity>,
    private readonly testDefinitionService: TestDefinitionService,
    @Inject(DAG_WORKFLOW_SCHEDULER_CLIENT)
    private readonly schedulerClient: DagWorkflowSchedulerClientPort,
    private readonly policyGateService: PolicyGateService,
    @Inject(COST_ESTIMATOR_CLIENT)
    private readonly costEstimatorClient: CostEstimatorClientPort,
    @Inject(PIPELINE_RUN_PUB_SUB)
    private readonly pubSub: PubSub,
  ) {}

  /**
   * Persists a PENDING run row, evaluates the policy-as-code gate (see
   * ../policy-gate/policy-gate.service.ts) before allowing it to proceed,
   * fetches a best-effort cost/carbon estimate for visibility, and only
   * then asks dag-workflow-scheduler to start executing it.
   *
   * Three outcomes for the persisted run, mirroring PolicyGateOutcome:
   *  - policy gate BLOCKING violation: run is marked FAILED with the
   *    violation summary as the failure reason (never reaches the scheduler).
   *  - policy gate REQUIRES_APPROVAL violation: run is marked
   *    AWAITING_APPROVAL and an ApprovalRequest is created (see
   *    ../approval-workflow/); the scheduler is not called until a human
   *    approves. Approval/rejection is wired back to this run via
   *    RunApprovalEventListener calling resumeHeldRun/failHeldRun below, in
   *    reaction to an event ApprovalWorkflowService emits — see that
   *    listener's file header for why an event rather than a direct call
   *    here.
   *  - policy gate allowed: proceeds to the scheduler exactly as before this
   *    feature existed. If the scheduler call itself fails, the run is
   *    marked FAILED rather than left dangling in PENDING forever.
   */
  async triggerRun(
    pipelineDefinitionId: string,
    requestedByUserId: string,
    targetNamespace?: string,
  ): Promise<PipelineRunEntity> {
    const definition = await this.testDefinitionService.findById(pipelineDefinitionId);
    const dag = { nodes: definition.nodes, edges: definition.edges };

    let run = this.pipelineRunRepository.create({
      pipelineDefinitionId,
      status: PipelineRunStatus.PENDING,
      nodeStatuses: [],
    });
    run = await this.pipelineRunRepository.save(run);

    try {
      const policyOutcome = await this.policyGateService.evaluateBeforeRun(run.id, {
        pipelineDefinitionId,
        dag,
        runContext: { targetNamespace, requestedByUserId },
      });

      if (policyOutcome.verdict === 'AWAITING_APPROVAL') {
        run.status = PipelineRunStatus.AWAITING_APPROVAL;
        run = await this.pipelineRunRepository.save(run);
        await this.publishStatusUpdate(run);
        return run;
      }
    } catch (error) {
      if (error instanceof ForbiddenException) {
        this.logger.warn(`Pipeline run ${run.id} blocked by policy gate: ${error.message}`);
        run.status = PipelineRunStatus.FAILED;
        run.completedAt = new Date();
        run.nodeStatuses.push({
          nodeId: '__policy-gate__',
          status: PipelineRunStatus.FAILED,
          completedAt: new Date().toISOString(),
          errorMessage: error.message,
        });
        run = await this.pipelineRunRepository.save(run);
        await this.publishStatusUpdate(run);
        return run;
      }
      throw error;
    }

    await this.attachCostEstimate(run, dag);

    try {
      const handle = await this.schedulerClient.triggerPipelineRun(run.id, dag);

      run.schedulerRunId = handle.schedulerRunId;
      run.status = PipelineRunStatus.RUNNING;
      run.startedAt = new Date();
      run = await this.pipelineRunRepository.save(run);
    } catch (error) {
      this.logger.error(
        `Failed to trigger scheduler for pipeline run ${run.id}, marking FAILED: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      run.status = PipelineRunStatus.FAILED;
      run.completedAt = new Date();
      run = await this.pipelineRunRepository.save(run);
    }

    await this.publishStatusUpdate(run);
    return run;
  }

  /**
   * Best-effort: fetches a projected cost/carbon estimate from
   * run-cost-estimator (see ../cost-estimation/) and attaches it to run.
   * Failures here are logged and swallowed rather than propagated — an
   * unreachable cost estimator must never prevent a pipeline run from
   * starting, it only means the canvas won't have a projected-cost figure
   * to show for this run.
   */
  private async attachCostEstimate(run: PipelineRunEntity, dag: { nodes: unknown; edges: unknown }): Promise<void> {
    try {
      const estimate = await this.costEstimatorClient.estimateRunCost(
        run.pipelineDefinitionId,
        dag as Parameters<CostEstimatorClientPort['estimateRunCost']>[1],
        'AWS',
      );
      run.estimatedCostUsd = estimate.totalCostUsd;
      run.estimatedCarbonKgCo2e = estimate.estimatedKgCo2e;
      await this.pipelineRunRepository.save(run);
    } catch (error) {
      this.logger.warn(
        `Could not attach cost estimate to pipeline run ${run.id} (continuing without it): ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  }

  /**
   * Resumes a run that was held in AWAITING_APPROVAL once its ApprovalRequest
   * has been approved. Called by RunApprovalEventListener (see
   * ./run-approval-event.listener.ts) in reaction to a RunApprovalDecisionEvent
   * emitted by ApprovalWorkflowService — not called directly from
   * ApprovalWorkflowModule, to avoid a module import cycle (see that event's
   * file header for the full rationale).
   *
   * Re-runs the same "fetch definition, estimate cost, trigger scheduler"
   * sequence triggerRun uses for a fresh run, deliberately skipping
   * PolicyGateService.evaluateBeforeRun a second time — the human approval
   * that led here *is* this run's resolution of the policy gate's
   * REQUIRES_APPROVAL verdict, so re-evaluating would either be redundant
   * (nothing changed) or could contradict the human's decision (something
   * changed and now produces a new gate outcome), neither of which is
   * correct once a human has already explicitly signed off.
   */
  async resumeHeldRun(runId: string): Promise<PipelineRunEntity> {
    let run = await this.getRunById(runId);

    if (run.status !== PipelineRunStatus.AWAITING_APPROVAL) {
      this.logger.warn(
        `resumeHeldRun called for pipeline run ${runId} but its status is ${run.status}, not AWAITING_APPROVAL; ignoring`,
      );
      return run;
    }

    const definition = await this.testDefinitionService.findById(run.pipelineDefinitionId);
    const dag = { nodes: definition.nodes, edges: definition.edges };

    await this.attachCostEstimate(run, dag);

    try {
      const handle = await this.schedulerClient.triggerPipelineRun(run.id, dag);

      run.schedulerRunId = handle.schedulerRunId;
      run.status = PipelineRunStatus.RUNNING;
      run.startedAt = new Date();
      run = await this.pipelineRunRepository.save(run);
    } catch (error) {
      this.logger.error(
        `Failed to trigger scheduler while resuming approved pipeline run ${run.id}, marking FAILED: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      run.status = PipelineRunStatus.FAILED;
      run.completedAt = new Date();
      run = await this.pipelineRunRepository.save(run);
    }

    await this.publishStatusUpdate(run);
    return run;
  }

  /**
   * Transitions a run held in AWAITING_APPROVAL to FAILED once its
   * ApprovalRequest has been rejected. Called by RunApprovalEventListener —
   * see resumeHeldRun's doc comment for why this doesn't happen via a direct
   * ApprovalWorkflowModule -> PipelineRunModule call.
   */
  async failHeldRun(runId: string, reason: string): Promise<PipelineRunEntity> {
    let run = await this.getRunById(runId);

    if (run.status !== PipelineRunStatus.AWAITING_APPROVAL) {
      this.logger.warn(
        `failHeldRun called for pipeline run ${runId} but its status is ${run.status}, not AWAITING_APPROVAL; ignoring`,
      );
      return run;
    }

    run.status = PipelineRunStatus.FAILED;
    run.completedAt = new Date();
    run.nodeStatuses.push({
      nodeId: '__approval-workflow__',
      status: PipelineRunStatus.FAILED,
      completedAt: new Date().toISOString(),
      errorMessage: reason,
    });
    run = await this.pipelineRunRepository.save(run);
    await this.publishStatusUpdate(run);
    return run;
  }

  async cancelRun(runId: string): Promise<PipelineRunEntity> {
    const run = await this.getRunById(runId);
    if (run.schedulerRunId) {
      await this.schedulerClient.cancelPipelineRun(run.schedulerRunId);
    }
    run.status = PipelineRunStatus.CANCELLED;
    run.completedAt = new Date();
    const saved = await this.pipelineRunRepository.save(run);
    await this.publishStatusUpdate(saved);
    return saved;
  }

  async getRunById(runId: string): Promise<PipelineRunEntity> {
    const run = await this.pipelineRunRepository.findOne({ where: { id: runId } });
    if (!run) {
      throw new NotFoundException(`Pipeline run ${runId} not found`);
    }
    return run;
  }

  /**
   * Folds an incoming node status update (from a dag-workflow-scheduler
   * webhook/event, or from quality-gate marking a run FAILED) into the run's
   * persisted state, recomputes overall run status when all nodes have
   * settled, and republishes to GraphQL subscribers.
   */
  async recordNodeStatusUpdate(
    runId: string,
    nodeStatusUpdate: PipelineRunNodeStatus,
  ): Promise<PipelineRunEntity> {
    const run = await this.getRunById(runId);

    const existingIndex = run.nodeStatuses.findIndex((status) => status.nodeId === nodeStatusUpdate.nodeId);
    if (existingIndex >= 0) {
      run.nodeStatuses[existingIndex] = nodeStatusUpdate;
    } else {
      run.nodeStatuses.push(nodeStatusUpdate);
    }

    if (nodeStatusUpdate.status === PipelineRunStatus.FAILED) {
      run.status = PipelineRunStatus.FAILED;
      run.completedAt = new Date();
    } else if (run.status === PipelineRunStatus.RUNNING && this.allNodesPassed(run.nodeStatuses)) {
      run.status = PipelineRunStatus.PASSED;
      run.completedAt = new Date();
    }

    const saved = await this.pipelineRunRepository.save(run);
    await this.publishStatusUpdate(saved);
    return saved;
  }

  /** Used directly by quality-gate.service to fail a run on SLA breach. */
  async markRunFailed(runId: string, reason: string): Promise<PipelineRunEntity> {
    const run = await this.getRunById(runId);
    run.status = PipelineRunStatus.FAILED;
    run.completedAt = new Date();
    run.nodeStatuses.push({
      nodeId: '__quality-gate__',
      status: PipelineRunStatus.FAILED,
      completedAt: new Date().toISOString(),
      errorMessage: reason,
    });
    const saved = await this.pipelineRunRepository.save(run);
    await this.publishStatusUpdate(saved);
    return saved;
  }

  private allNodesPassed(nodeStatuses: PipelineRunNodeStatus[]): boolean {
    return nodeStatuses.length > 0 && nodeStatuses.every((status) => status.status === PipelineRunStatus.PASSED);
  }

  private async publishStatusUpdate(run: PipelineRunEntity): Promise<void> {
    await this.pubSub.publish(PIPELINE_RUN_STATUS_UPDATED_EVENT, {
      pipelineRunStatusUpdated: run,
    });
  }
}
