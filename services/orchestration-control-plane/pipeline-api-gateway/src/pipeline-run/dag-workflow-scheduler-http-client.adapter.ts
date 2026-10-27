import { Injectable, Logger, ServiceUnavailableException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import axios, { AxiosInstance } from 'axios';
import { PipelineDag } from '../test-definition/dag-domain-types';
import { DagWorkflowSchedulerClientPort, PipelineRunHandle } from './dag-workflow-scheduler-client.port';

interface TriggerPipelineRunResponseBody {
  runId: string;
  acceptedAt: string;
}

/**
 * Real HTTP adapter for DagWorkflowSchedulerClientPort, talking to the
 * sibling `dag-workflow-scheduler` Go service's REST control surface. This
 * gateway does not publish to `dag.node.dispatch.v1` itself — that topic is
 * owned by dag-workflow-scheduler, which publishes to it once it accepts
 * the run triggered here.
 */
@Injectable()
export class DagWorkflowSchedulerHttpClientAdapter implements DagWorkflowSchedulerClientPort {
  private readonly logger = new Logger(DagWorkflowSchedulerHttpClientAdapter.name);
  private readonly httpClient: AxiosInstance;

  constructor(private readonly configService: ConfigService) {
    const baseURL = this.configService.get<string>(
      'DAG_WORKFLOW_SCHEDULER_BASE_URL',
      'http://dag-workflow-scheduler:8080',
    );
    this.httpClient = axios.create({
      baseURL,
      timeout: this.configService.get<number>('DAG_WORKFLOW_SCHEDULER_TIMEOUT_MS', 5000),
    });
  }

  async triggerPipelineRun(pipelineRunId: string, dag: PipelineDag): Promise<PipelineRunHandle> {
    try {
      const response = await this.httpClient.post<TriggerPipelineRunResponseBody>('/pipeline-runs', {
        controlPlaneRunId: pipelineRunId,
        nodes: dag.nodes,
        edges: dag.edges,
      });

      return {
        schedulerRunId: response.data.runId,
        acceptedAt: new Date(response.data.acceptedAt),
      };
    } catch (error) {
      this.logger.error(
        `dag-workflow-scheduler rejected trigger for pipeline run ${pipelineRunId}: ${this.describeError(error)}`,
      );
      throw new ServiceUnavailableException(
        `dag-workflow-scheduler is unavailable or rejected the run trigger: ${this.describeError(error)}`,
      );
    }
  }

  async cancelPipelineRun(schedulerRunId: string): Promise<void> {
    try {
      await this.httpClient.post(`/pipeline-runs/${schedulerRunId}/cancel`);
    } catch (error) {
      this.logger.error(
        `dag-workflow-scheduler failed to cancel run ${schedulerRunId}: ${this.describeError(error)}`,
      );
      throw new ServiceUnavailableException(
        `dag-workflow-scheduler is unavailable or rejected the cancel request: ${this.describeError(error)}`,
      );
    }
  }

  private describeError(error: unknown): string {
    if (axios.isAxiosError(error)) {
      return `${error.response?.status ?? 'no-response'} ${JSON.stringify(error.response?.data ?? error.message)}`;
    }
    return error instanceof Error ? error.message : String(error);
  }
}
