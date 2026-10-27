import {
  CrossplaneApplyResult,
  CrossplaneCompositionConfig,
  CrossplaneCompositionPort,
} from './crossplane-composition.port';
import { CommandRunnerPort } from './command-runner.port';

export class CrossplaneExecutionFailedError extends Error {
  constructor(
    public readonly claimName: string,
    public readonly operation: 'apply' | 'delete',
    public readonly exitCode: number,
    public readonly stderr: string,
  ) {
    super(
      `kubectl ${operation} failed for Crossplane claim "${claimName}" with exit code ${exitCode}: ${stderr.trim()}`,
    );
    this.name = 'CrossplaneExecutionFailedError';
    Object.setPrototypeOf(this, CrossplaneExecutionFailedError.prototype);
  }
}

interface CrossplaneClaimManifest {
  readonly apiVersion: string;
  readonly kind: string;
  readonly metadata: {
    readonly name: string;
    readonly namespace: string;
  };
  readonly spec: {
    readonly compositionRef: { readonly name: string };
    readonly parameters: Record<string, unknown>;
  };
}

/**
 * Executes `kubectl` as a real CLI process to apply/delete a Crossplane
 * composite resource claim, delegated through an injected CommandRunnerPort
 * (shared with TerraformCliExecutionAdapter) so this adapter never spawns a
 * process merely by being imported or constructed.
 */
export class CrossplaneKubectlExecutionAdapter implements CrossplaneCompositionPort {
  constructor(private readonly commandRunner: CommandRunnerPort) {}

  async applyComposition(
    compositionConfig: CrossplaneCompositionConfig,
  ): Promise<CrossplaneApplyResult> {
    const manifest = this.buildClaimManifest(compositionConfig);
    const manifestJson = JSON.stringify(manifest);

    const startedAtMs = Date.now();
    const applyResult = await this.commandRunner.run('kubectl', [
      'apply',
      '-f',
      '-',
      '--filename-stdin-content=' + manifestJson,
    ]);
    const applyDurationMs = Date.now() - startedAtMs;

    if (applyResult.exitCode !== 0) {
      throw new CrossplaneExecutionFailedError(
        compositionConfig.claimName,
        'apply',
        applyResult.exitCode,
        applyResult.stderr,
      );
    }

    const getResult = await this.commandRunner.run('kubectl', [
      'get',
      manifest.kind,
      compositionConfig.claimName,
      '-n',
      compositionConfig.namespace,
      '-o',
      'json',
    ]);
    if (getResult.exitCode !== 0) {
      throw new CrossplaneExecutionFailedError(
        compositionConfig.claimName,
        'apply',
        getResult.exitCode,
        getResult.stderr,
      );
    }

    return {
      claimName: compositionConfig.claimName,
      outputs: this.parseClaimStatusOutputs(getResult.stdout),
      applyDurationMs,
    };
  }

  async deleteComposition(claimName: string): Promise<void> {
    const deleteResult = await this.commandRunner.run('kubectl', [
      'delete',
      'compositeresourceclaim',
      claimName,
      '--ignore-not-found=true',
    ]);
    if (deleteResult.exitCode !== 0) {
      throw new CrossplaneExecutionFailedError(
        claimName,
        'delete',
        deleteResult.exitCode,
        deleteResult.stderr,
      );
    }
  }

  /**
   * Builds an in-memory Kubernetes-shaped manifest for the Crossplane claim.
   * This is what would be piped into `kubectl apply -f -` in a real
   * environment; here it is serialized to JSON and handed to the injected
   * command runner.
   */
  private buildClaimManifest(
    compositionConfig: CrossplaneCompositionConfig,
  ): CrossplaneClaimManifest {
    return {
      apiVersion: 'testenv.orchestration.internal/v1alpha1',
      kind: 'EphemeralTestEnvironmentClaim',
      metadata: {
        name: compositionConfig.claimName,
        namespace: compositionConfig.namespace,
      },
      spec: {
        compositionRef: { name: compositionConfig.compositionRef },
        parameters: compositionConfig.parameters,
      },
    };
  }

  /**
   * Parses `kubectl get <kind> <name> -o json` stdout, pulling flat string
   * values out of `status.atProvider` (the conventional location Crossplane
   * managed resources publish connection/output details under).
   */
  private parseClaimStatusOutputs(stdout: string): Record<string, string> {
    let parsed: unknown;
    try {
      parsed = JSON.parse(stdout);
    } catch (parseError) {
      throw new Error(
        `Failed to parse "kubectl get -o json" stdout as JSON: ${(parseError as Error).message}`,
      );
    }

    const outputs: Record<string, string> = {};
    const status = (parsed as { status?: { atProvider?: Record<string, unknown> } })?.status;
    const atProvider = status?.atProvider ?? {};
    for (const [key, value] of Object.entries(atProvider)) {
      outputs[key] = typeof value === 'string' ? value : JSON.stringify(value);
    }
    return outputs;
  }
}
