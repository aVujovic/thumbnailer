import type { ILogger, StartThumbnailProcess } from '@thumbnailer/domain';
import type { VideoJob } from '@thumbnailer/contracts';
import type { IThumbnailStore } from './ThumbnailStore.js';
import { thumbnailOutputPath } from './outputPath.js';

interface GeneratorConfig {
  outputRoot: string;
  thumbnailFormat: string;
  timeoutMs: number;
  maxRetries: number;
  force: boolean;
  backoffBaseMs: number;
  backoffFactor: number;
}

/** Outcome of generating one thumbnail — returned, not thrown, so the consumer
 *  can decide commit vs. redeliver and log a structured result. */
export type GenerateResult =
  | { status: 'generated'; outputPath: string; attempts: number }
  | { status: 'skipped'; outputPath: string; reason: 'exists' }
  | { status: 'failed'; outputPath: string; attempts: number; reason: string; stderr?: string };

/** Injectable timer so timeout/backoff are deterministic in tests. */
export interface Timer {
  sleep(ms: number): Promise<void>;
  /** Reject after `ms`; used to race against the process. */
  timeout(ms: number): { promise: Promise<never>; cancel: () => void };
}

export const realTimer: Timer = {
  sleep: (ms) => new Promise((r) => setTimeout(r, ms)),
  timeout: (ms) => {
    let handle: ReturnType<typeof setTimeout>;
    const promise = new Promise<never>((_, reject) => {
      handle = setTimeout(() => reject(new TimeoutError(ms)), ms);
    });
    return { promise, cancel: () => clearTimeout(handle) };
  },
};

class TimeoutError extends Error {
  constructor(ms: number) {
    super(`thumbnail process timed out after ${ms}ms`);
    this.name = 'TimeoutError';
  }
}

/**
 * Generates a thumbnail for a single video, resiliently. This is the heart of
 * the generator and where the interesting decisions live (kept out of `core`):
 *
 *  1. **Idempotency** — if the thumbnail already exists, skip (unless `force`).
 *  2. **Launch guard** — `startThumbnailProcess` is wrapped in try/catch; a
 *     synchronous launch failure (e.g. too many FDs) is a normal failure, not a
 *     crash.
 *  3. **Timeout → kill → await grace** — the process is raced against a timer;
 *     on timeout we kill() AND await waitForCompletion() so we never leak a
 *     zombie.
 *  4. **Retry with backoff** — up to `maxRetries` blind retries with exponential
 *     backoff, catching transient hiccups (documented trade-off: a genuinely
 *     broken file burns all attempts).
 *
 * Returns a structured result; never throws for an expected failure.
 */
export class ThumbnailGenerator {
  private readonly start: StartThumbnailProcess;
  private readonly store: IThumbnailStore;
  private readonly logger: ILogger;
  private readonly config: GeneratorConfig;
  private readonly timer: Timer;

  constructor(
    deps: {
      startThumbnailProcess: StartThumbnailProcess;
      thumbnailStore: IThumbnailStore;
      logger: ILogger;
      config: GeneratorConfig;
    },
    timer: Timer = realTimer,
  ) {
    this.start = deps.startThumbnailProcess;
    this.store = deps.thumbnailStore;
    this.logger = deps.logger;
    this.config = deps.config;
    this.timer = timer;
  }

  async generate(job: VideoJob): Promise<GenerateResult> {
    const outputPath = thumbnailOutputPath(
      this.config.outputRoot,
      job.path,
      this.config.thumbnailFormat,
      job.tenantId,
    );

    if (!this.config.force && this.store.exists(outputPath)) {
      this.logger.debug({ path: job.path, outputPath }, 'thumbnail exists — skipping');
      return { status: 'skipped', outputPath, reason: 'exists' };
    }

    this.store.ensureDir(outputPath);

    let lastErr = 'unknown error';
    let lastStderr: string | undefined;
    const totalAttempts = this.config.maxRetries + 1;

    for (let attempt = 1; attempt <= totalAttempts; attempt++) {
      const res = await this.runOnce(job.path, outputPath);
      if (res.ok) {
        this.logger.info({ path: job.path, outputPath, attempt }, '🖼️  thumbnail generated');
        return { status: 'generated', outputPath, attempts: attempt };
      }

      lastErr = res.error;
      lastStderr = res.stderr;
      this.logger.warn(
        { path: job.path, attempt, totalAttempts, err: res.error },
        'thumbnail attempt failed',
      );

      if (attempt < totalAttempts) {
        await this.timer.sleep(this.backoffMs(attempt));
      }
    }

    this.logger.error(
      { path: job.path, outputPath, attempts: totalAttempts, err: lastErr, stderr: lastStderr },
      'thumbnail generation failed (exhausted retries)',
    );
    return {
      status: 'failed',
      outputPath,
      attempts: totalAttempts,
      reason: lastErr,
      stderr: lastStderr,
    };
  }

  /** One attempt: launch, race against timeout, classify the outcome. */
  private async runOnce(
    videoPath: string,
    outputPath: string,
  ): Promise<{ ok: true } | { ok: false; error: string; stderr?: string }> {
    let proc;
    try {
      proc = this.start(videoPath, outputPath);
    } catch (err) {
      // Synchronous launch failure (e.g. EMFILE / too many processes).
      return { ok: false, error: `launch failed: ${errMsg(err)}` };
    }

    const t = this.timer.timeout(this.config.timeoutMs);
    try {
      const code = await Promise.race([proc.waitForCompletion(), t.promise]);
      if (code === 0) return { ok: true };
      return { ok: false, error: `exited with code ${code}`, stderr: proc.getStderr() };
    } catch (err) {
      if (isTimeout(err)) {
        // Kill, then AWAIT completion so the process is reaped (no zombie).
        proc.kill();
        await proc.waitForCompletion().catch(() => undefined);
        return { ok: false, error: errMsg(err), stderr: proc.getStderr() };
      }
      return { ok: false, error: errMsg(err), stderr: proc.getStderr?.() };
    } finally {
      t.cancel();
    }
  }

  /** Exponential backoff: base, base*factor, base*factor², ... per attempt. */
  private backoffMs(attempt: number): number {
    return Math.round(this.config.backoffBaseMs * this.config.backoffFactor ** (attempt - 1));
  }
}

/** A timeout is identified by name, so any Timer implementation's timeout
 *  rejection is recognised (not just our own TimeoutError instance). */
function isTimeout(err: unknown): boolean {
  return err instanceof Error && err.name === 'TimeoutError';
}

function errMsg(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

export { TimeoutError };
export default ThumbnailGenerator;
