import { PipelineDag } from '../test-definition/dag-domain-types';

/**
 * DI token used to inject a DagWorkflowSchedulerClientPort implementation.
 * Kept separate from the interface itself since TS interfaces have no
 * runtime representation Nest can use as a provider token.
 */
export const DAG_WORKFLOW_SCHEDULER_CLIENT = Symbol('DAG_WORKFLOW_SCHEDULER_CLIENT');

/** Handle returned by dag-workflow-scheduler when a run is accepted. */
export interface PipelineRunHandle {
  /** Scheduler-assigned run id — the gateway persists this on PipelineRunEntity.id. */
  schedulerRunId: string;
  acceptedAt: Date;
}

/**
 * Port (hexagonal-architecture boundary) to the sibling `dag-workflow-scheduler`
 * Go service. Per ARCHITECTURE.md, actual node dispatch to workers happens
 * over the `dag.node.dispatch.v1` Redpanda topic, which is owned and
 * published by dag-workflow-scheduler itself — this gateway never publishes
 * to that topic directly. It only triggers/cancels runs via the scheduler's
 * REST control surface, modelled here as a port so the HTTP adapter can be
 * swapped (e.g. for a gRPC client) without touching pipeline-run.service.
 */
export interface DagWorkflowSchedulerClientPort {
  /**
   * Asks dag-workflow-scheduler to begin executing the given DAG. The
   * scheduler is responsible for topologically ordering nodes and
   * publishing dag.node.dispatch.v1 events as each node becomes runnable.
   */
  triggerPipelineRun(pipelineRunId: string, dag: PipelineDag): Promise<PipelineRunHandle>;

  /** Requests cancellation of an in-flight run. Best-effort: nodes already dispatched may still complete. */
  cancelPipelineRun(schedulerRunId: string): Promise<void>;
}
