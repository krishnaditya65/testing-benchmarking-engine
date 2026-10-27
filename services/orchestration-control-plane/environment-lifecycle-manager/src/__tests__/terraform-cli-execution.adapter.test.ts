import {
  TerraformCliExecutionAdapter,
  TerraformExecutionFailedError,
} from '../provisioning/terraform-cli-execution.adapter';
import { CommandRunnerPort, CommandRunnerResult } from '../provisioning/command-runner.port';

class FakeCommandRunner implements CommandRunnerPort {
  public readonly recordedCalls: Array<{ command: string; args: string[] }> = [];
  constructor(private readonly responses: CommandRunnerResult[]) {}

  async run(command: string, args: string[]): Promise<CommandRunnerResult> {
    this.recordedCalls.push({ command, args });
    const next = this.responses.shift();
    if (!next) {
      throw new Error('FakeCommandRunner ran out of queued responses');
    }
    return next;
  }
}

describe('TerraformCliExecutionAdapter', () => {
  it('builds a correct apply argv and parses terraform output -json', async () => {
    const fakeRunner = new FakeCommandRunner([
      { stdout: '', stderr: '', exitCode: 0 },
      {
        stdout: JSON.stringify({
          load_balancer_url: { sensitive: false, type: 'string', value: 'https://lb.internal' },
        }),
        stderr: '',
        exitCode: 0,
      },
    ]);
    const adapter = new TerraformCliExecutionAdapter(fakeRunner);

    const result = await adapter.planAndApply({
      workspaceId: 'env-abc',
      moduleSourcePath: './modules/ephemeral-env',
      variables: { region: 'us-east-1', node_count: 3 },
    });

    expect(fakeRunner.recordedCalls[0]?.args).toEqual([
      '-chdir=./modules/ephemeral-env',
      'apply',
      '-auto-approve',
      '-input=false',
      '-var=region=us-east-1',
      '-var=node_count=3',
    ]);
    expect(result.outputs.load_balancer_url).toBe('https://lb.internal');
    expect(result.workspaceId).toBe('env-abc');
  });

  it('throws TerraformExecutionFailedError on non-zero apply exit code', async () => {
    const fakeRunner = new FakeCommandRunner([
      { stdout: '', stderr: 'apply failed: quota exceeded', exitCode: 1 },
    ]);
    const adapter = new TerraformCliExecutionAdapter(fakeRunner);

    await expect(
      adapter.planAndApply({
        workspaceId: 'env-xyz',
        moduleSourcePath: './modules/ephemeral-env',
        variables: {},
      }),
    ).rejects.toThrow(TerraformExecutionFailedError);
  });

  it('builds a correct destroy argv', async () => {
    const fakeRunner = new FakeCommandRunner([{ stdout: '', stderr: '', exitCode: 0 }]);
    const adapter = new TerraformCliExecutionAdapter(fakeRunner);

    await adapter.destroy('env-abc');

    expect(fakeRunner.recordedCalls[0]).toEqual({
      command: 'terraform',
      args: ['-chdir=env-abc', 'destroy', '-auto-approve', '-input=false'],
    });
  });
});
