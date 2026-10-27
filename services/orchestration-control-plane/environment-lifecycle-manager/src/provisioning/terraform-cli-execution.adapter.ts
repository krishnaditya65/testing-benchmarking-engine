import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import {
  TerraformApplyResult,
  TerraformExecutionPort,
  TerraformModuleConfig,
} from './terraform-execution.port';
import { CommandRunnerPort, CommandRunnerResult } from './command-runner.port';

const execFileAsync = promisify(execFile);

export class TerraformExecutionFailedError extends Error {
  constructor(
    public readonly workspaceId: string,
    public readonly operation: 'apply' | 'destroy',
    public readonly exitCode: number,
    public readonly stderr: string,
  ) {
    super(
      `terraform ${operation} failed for workspace "${workspaceId}" with exit code ${exitCode}: ${stderr.trim()}`,
    );
    this.name = 'TerraformExecutionFailedError';
    Object.setPrototypeOf(this, TerraformExecutionFailedError.prototype);
  }
}

/**
 * Executes `terraform` as a real CLI process, delegated through an injected
 * CommandRunnerPort so this adapter is unit-testable with a fake runner and
 * never spawns a process merely by being imported or constructed.
 */
export class TerraformCliExecutionAdapter implements TerraformExecutionPort {
  constructor(private readonly commandRunner: CommandRunnerPort) {}

  async planAndApply(moduleConfig: TerraformModuleConfig): Promise<TerraformApplyResult> {
    const applyArgs = this.buildApplyArgs(moduleConfig);
    const startedAtMs = Date.now();
    const applyResult = await this.commandRunner.run('terraform', applyArgs);
    const applyDurationMs = Date.now() - startedAtMs;

    if (applyResult.exitCode !== 0) {
      throw new TerraformExecutionFailedError(
        moduleConfig.workspaceId,
        'apply',
        applyResult.exitCode,
        applyResult.stderr,
      );
    }

    const outputArgs = this.buildOutputArgs(moduleConfig.moduleSourcePath);
    const outputResult = await this.commandRunner.run('terraform', outputArgs);
    if (outputResult.exitCode !== 0) {
      throw new TerraformExecutionFailedError(
        moduleConfig.workspaceId,
        'apply',
        outputResult.exitCode,
        outputResult.stderr,
      );
    }

    return {
      workspaceId: moduleConfig.workspaceId,
      outputs: this.parseTerraformOutputJson(outputResult),
      applyDurationMs,
    };
  }

  async destroy(workspaceId: string): Promise<void> {
    const destroyArgs = ['-chdir=' + workspaceId, 'destroy', '-auto-approve', '-input=false'];
    const destroyResult = await this.commandRunner.run('terraform', destroyArgs);
    if (destroyResult.exitCode !== 0) {
      throw new TerraformExecutionFailedError(
        workspaceId,
        'destroy',
        destroyResult.exitCode,
        destroyResult.stderr,
      );
    }
  }

  private buildApplyArgs(moduleConfig: TerraformModuleConfig): string[] {
    const variableArgs = Object.entries(moduleConfig.variables).map(
      ([variableName, variableValue]) => `-var=${variableName}=${variableValue}`,
    );
    return [
      '-chdir=' + moduleConfig.moduleSourcePath,
      'apply',
      '-auto-approve',
      '-input=false',
      ...variableArgs,
    ];
  }

  private buildOutputArgs(moduleSourcePath: string): string[] {
    return ['-chdir=' + moduleSourcePath, 'output', '-json'];
  }

  /**
   * Parses the JSON shape produced by `terraform output -json`, which maps
   * each output name to an object carrying (at minimum) a `value` field:
   *   { "vpc_id": { "sensitive": false, "type": "string", "value": "vpc-123" }, ... }
   */
  private parseTerraformOutputJson(outputResult: CommandRunnerResult): Record<string, string> {
    let parsed: unknown;
    try {
      parsed = JSON.parse(outputResult.stdout);
    } catch (parseError) {
      throw new Error(
        `Failed to parse "terraform output -json" stdout as JSON: ${(parseError as Error).message}`,
      );
    }

    if (typeof parsed !== 'object' || parsed === null) {
      return {};
    }

    const outputs: Record<string, string> = {};
    for (const [outputName, outputEntry] of Object.entries(parsed as Record<string, unknown>)) {
      if (
        typeof outputEntry === 'object' &&
        outputEntry !== null &&
        'value' in outputEntry
      ) {
        const rawValue = (outputEntry as { value: unknown }).value;
        outputs[outputName] = typeof rawValue === 'string' ? rawValue : JSON.stringify(rawValue);
      }
    }
    return outputs;
  }
}

/**
 * Real process-spawning implementation of CommandRunnerPort using Node's
 * child_process.execFile. This is the only place in the service that
 * actually spawns an OS process for Terraform; it is never invoked at
 * import time, only when explicitly constructed and used by a caller.
 */
export class NodeChildProcessCommandRunner implements CommandRunnerPort {
  async run(command: string, args: string[]): Promise<CommandRunnerResult> {
    try {
      const { stdout, stderr } = await execFileAsync(command, args, {
        maxBuffer: 1024 * 1024 * 32,
      });
      return { stdout, stderr, exitCode: 0 };
    } catch (error) {
      const execError = error as { stdout?: string; stderr?: string; code?: number };
      return {
        stdout: execError.stdout ?? '',
        stderr: execError.stderr ?? (error as Error).message,
        exitCode: typeof execError.code === 'number' ? execError.code : 1,
      };
    }
  }
}
