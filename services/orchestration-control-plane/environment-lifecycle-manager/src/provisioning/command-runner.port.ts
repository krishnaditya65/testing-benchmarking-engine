/**
 * Shared abstraction over spawning an external CLI process (terraform,
 * kubectl, ...). Both provisioning adapters depend on this port instead of
 * spawning processes themselves, which keeps them unit-testable with a
 * fake/mock runner and keeps process-spawning code in exactly one place.
 */
export interface CommandRunnerResult {
  readonly stdout: string;
  readonly stderr: string;
  readonly exitCode: number;
}

export interface CommandRunnerPort {
  run(command: string, args: string[]): Promise<CommandRunnerResult>;
}
