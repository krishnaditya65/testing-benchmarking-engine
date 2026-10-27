/**
 * Port for provisioning ephemeral test environments backed by Terraform.
 * Adapters (see terraform-cli-execution.adapter.ts) implement this against
 * a concrete Terraform execution mechanism (local CLI, remote runner, ...).
 */

export interface TerraformModuleConfig {
  readonly workspaceId: string;
  readonly moduleSourcePath: string;
  readonly variables: Record<string, string | number | boolean>;
}

export interface TerraformApplyResult {
  readonly workspaceId: string;
  readonly outputs: Record<string, string>;
  readonly applyDurationMs: number;
}

export interface TerraformExecutionPort {
  planAndApply(moduleConfig: TerraformModuleConfig): Promise<TerraformApplyResult>;
  destroy(workspaceId: string): Promise<void>;
}
