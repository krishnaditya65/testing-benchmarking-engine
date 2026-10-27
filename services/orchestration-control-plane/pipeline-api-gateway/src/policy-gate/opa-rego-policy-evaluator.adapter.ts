import { Injectable, Logger, ServiceUnavailableException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import axios, { AxiosInstance } from 'axios';
import { PolicyEvaluationPort } from './policy-evaluation.port';
import {
  PolicyBundle,
  PolicyEvaluationInput,
  PolicyEvaluationResult,
  PolicyViolation,
} from './policy-evaluation-result.types';

/**
 * Shape of one violation as emitted by the shared `violations` partial-set
 * rule that every .rego file under example-policies/ (all `package
 * testforge.policies`) contributes to. Kept structurally identical to
 * PolicyViolation minus `nodeIds` naming (Rego convention favors snake_case)
 * so the mapping in `evaluate` below is a straight field rename.
 */
interface OpaViolationDocument {
  policy_id: string;
  rule_name: string;
  severity: 'BLOCKING' | 'REQUIRES_APPROVAL' | 'WARNING';
  message: string;
  node_ids?: string[];
}

/** OPA's Data API wraps every query result in a `{ "result": ... }` envelope. */
interface OpaDataApiResponseBody {
  result?: OpaViolationDocument[];
}

/**
 * Real HTTP adapter to an OPA (Open Policy Agent) sidecar, implementing
 * PolicyEvaluationPort by calling OPA's Data API
 * (`POST /v1/data/<package-path>`) with the pipeline definition (plus run
 * context) as `input`, and reading back the `violations` partial-set every
 * example-policies/*.rego file contributes to under `package
 * testforge.policies`.
 *
 * NOTE: this adapter assumes a running OPA sidecar/service reachable at
 * OPA_SIDECAR_BASE_URL, loaded with the .rego bundle from
 * `example-policies/` (e.g. via `opa run --server --bundle
 * example-policies/` or a Kubernetes sidecar container with those files
 * mounted and `opa run --server --addr :8181 /policies`). This repo does
 * not run OPA itself — wiring that sidecar into
 * infrastructure/kubernetes/base/control-plane-services-deployment.yaml
 * and infrastructure/helm-charts/pipeline-api-gateway is required before
 * this adapter can succeed against anything other than a local `opa run`
 * process.
 */
@Injectable()
export class OpaRegoPolicyEvaluatorAdapter implements PolicyEvaluationPort {
  private readonly logger = new Logger(OpaRegoPolicyEvaluatorAdapter.name);
  private readonly httpClient: AxiosInstance;

  constructor(private readonly configService: ConfigService) {
    this.httpClient = axios.create({
      baseURL: this.configService.get<string>('OPA_SIDECAR_BASE_URL', 'http://localhost:8181'),
      timeout: this.configService.get<number>('OPA_SIDECAR_TIMEOUT_MS', 3000),
      headers: { 'Content-Type': 'application/json' },
    });
  }

  async evaluate(input: PolicyEvaluationInput, policyBundle: PolicyBundle): Promise<PolicyEvaluationResult> {
    try {
      // OPA's Data API addresses a rule by dotted package path turned into a
      // URL path: `package testforge.policies` + rule `violations` becomes
      // `/v1/data/testforge/policies/violations`. All policyPackages in the
      // bundle share that one package, so a single query evaluates every
      // loaded .rego file's contribution to the shared `violations` set —
      // policyBundle.policyPackages is carried through only for audit
      // logging here, not to select which rules run (OPA evaluates whatever
      // is loaded into the sidecar's bundle).
      const response = await this.httpClient.post<OpaDataApiResponseBody>(
        '/v1/data/testforge/policies/violations',
        {
          input: {
            pipelineDefinitionId: input.pipelineDefinitionId,
            nodes: input.dag.nodes,
            edges: input.dag.edges,
            runContext: input.runContext,
          },
        },
      );

      const violations = (response.data.result ?? []).map(toPolicyViolation);
      const hasBlockingViolation = violations.some((violation) => violation.severity === 'BLOCKING');

      this.logger.log(
        `Evaluated policy bundle "${policyBundle.bundleName}" for pipeline definition ${input.pipelineDefinitionId}: ` +
          `${violations.length} violation(s), allowed=${!hasBlockingViolation}`,
      );

      return { allowed: !hasBlockingViolation, violations };
    } catch (error) {
      this.logger.error(
        `OPA sidecar unreachable or rejected evaluation for pipeline definition ${input.pipelineDefinitionId}: ${this.describeError(error)}`,
      );
      throw new ServiceUnavailableException(
        `Policy evaluation is unavailable (OPA sidecar unreachable): ${this.describeError(error)}`,
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

function toPolicyViolation(doc: OpaViolationDocument): PolicyViolation {
  return {
    policyId: doc.policy_id,
    ruleName: doc.rule_name,
    severity: doc.severity,
    message: doc.message,
    nodeIds: doc.node_ids ?? [],
  };
}
