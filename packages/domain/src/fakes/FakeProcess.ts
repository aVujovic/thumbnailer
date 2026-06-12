import type { IProcess, StartThumbnailProcess } from '../process.js';

/** How a single fake process should behave. */
export interface FakeProcessBehavior {
  /** Exit code resolved by waitForCompletion. Default 0 (success). */
  exitCode?: number;
  /** stderr returned after completion. */
  stderr?: string;
  /**
   * If true, the process never finishes on its own — waitForCompletion stays
   * pending until kill() is called (simulates an ffmpeg hang). On kill it
   * resolves with `killExitCode`.
   */
  hang?: boolean;
  /** Exit code reported after a kill on a hung process. Default 137 (SIGKILL). */
  killExitCode?: number;
  /** Delay (ms) before a non-hanging process completes. Default 0. */
  durationMs?: number;
}

/** A single programmable fake process. */
export class FakeProcess implements IProcess {
  killed = false;
  private settle?: (code: number) => void;
  private readonly behavior: FakeProcessBehavior;
  private completion: Promise<number>;

  constructor(behavior: FakeProcessBehavior = {}) {
    this.behavior = behavior;
    this.completion = new Promise<number>((resolve) => {
      this.settle = resolve;
      if (behavior.hang) return; // wait for kill()
      const code = behavior.exitCode ?? 0;
      if (behavior.durationMs) setTimeout(() => resolve(code), behavior.durationMs);
      else resolve(code);
    });
  }

  kill(): void {
    this.killed = true;
    // A hung process resolves on kill; a finished one is a no-op.
    this.settle?.(this.behavior.killExitCode ?? 137);
  }

  waitForCompletion(): Promise<number> {
    return this.completion;
  }

  getStderr(): string {
    return this.behavior.stderr ?? '';
  }
}

/**
 * Builds a StartThumbnailProcess fake. Pass a behavior or a function that
 * derives behavior from the call (e.g. fail the first N calls to test retry).
 * Records every (videoPath, outputPath) invocation in `calls`.
 */
export function makeFakeThumbnailProcess(
  behavior:
    | FakeProcessBehavior
    | ((call: { videoPath: string; outputPath: string; attempt: number }) => FakeProcessBehavior)
    | { throwOnLaunch: Error } = {},
): StartThumbnailProcess & { calls: Array<{ videoPath: string; outputPath: string }>; processes: FakeProcess[] } {
  const calls: Array<{ videoPath: string; outputPath: string }> = [];
  const processes: FakeProcess[] = [];

  const start = ((videoPath: string, outputPath: string): IProcess => {
    const attempt = calls.length;
    calls.push({ videoPath, outputPath });

    if (typeof behavior === 'object' && 'throwOnLaunch' in behavior) {
      throw behavior.throwOnLaunch; // synchronous launch failure (e.g. too many FDs)
    }

    const resolved =
      typeof behavior === 'function' ? behavior({ videoPath, outputPath, attempt }) : behavior;
    const proc = new FakeProcess(resolved);
    processes.push(proc);
    return proc;
  }) as StartThumbnailProcess & { calls: typeof calls; processes: FakeProcess[] };

  start.calls = calls;
  start.processes = processes;
  return start;
}

export default FakeProcess;
