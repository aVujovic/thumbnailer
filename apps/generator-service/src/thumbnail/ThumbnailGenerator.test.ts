import { describe, it, expect, vi } from 'vitest';
import { FakeLogger, makeFakeThumbnailProcess } from '@thumbnailer/domain/fakes';
import type { VideoJob } from '@thumbnailer/contracts';
import { ThumbnailGenerator, type Timer } from './ThumbnailGenerator.js';
import type { IThumbnailStore } from './ThumbnailStore.js';

const job: VideoJob = { path: '/data/clip.mp4', size: 100, discoveredAt: 1 };

const config = {
  outputRoot: '/out',
  thumbnailFormat: 'jpg',
  timeoutMs: 1000,
  maxRetries: 2,
  force: false,
  backoffBaseMs: 100,
  backoffFactor: 2,
};

/** Store fake: nothing exists by default; records ensureDir calls. */
function fakeStore(exists = false): IThumbnailStore & { dirs: string[] } {
  const dirs: string[] = [];
  return {
    exists: () => exists,
    ensureDir: (p: string) => void dirs.push(p),
    dirs,
  };
}

/**
 * Fake timer: sleep is instant (backoff doesn't slow tests); timeout fires on
 * the next microtask, so a hanging process always loses the race to it.
 */
const fastTimer: Timer = {
  sleep: () => Promise.resolve(),
  timeout: () => {
    let cancelled = false;
    const promise = new Promise<never>((_, reject) => {
      queueMicrotask(() => {
        if (!cancelled) reject(Object.assign(new Error('timed out'), { name: 'TimeoutError' }));
      });
    });
    return { promise, cancel: () => void (cancelled = true) };
  },
};

/** A timer whose timeout never fires — lets a fast process win the race. */
const noTimeout: Timer = {
  sleep: () => Promise.resolve(),
  timeout: () => ({ promise: new Promise<never>(() => {}), cancel: () => {} }),
};

describe('ThumbnailGenerator', () => {
  it('generates a thumbnail on the first successful attempt', async () => {
    const start = makeFakeThumbnailProcess({ exitCode: 0 });
    const store = fakeStore(false);
    const gen = new ThumbnailGenerator(
      { startThumbnailProcess: start, thumbnailStore: store, logger: new FakeLogger(), config },
      noTimeout,
    );

    const res = await gen.generate(job);

    expect(res).toEqual({ status: 'generated', outputPath: '/out/data/clip.mp4.jpg', attempts: 1 });
    expect(start.calls).toHaveLength(1);
    expect(start.calls[0]).toEqual({ videoPath: '/data/clip.mp4', outputPath: '/out/data/clip.mp4.jpg' });
    expect(store.dirs).toEqual(['/out/data/clip.mp4.jpg']); // ensureDir called
  });

  it('skips when the thumbnail already exists (idempotent)', async () => {
    const start = makeFakeThumbnailProcess({ exitCode: 0 });
    const gen = new ThumbnailGenerator(
      { startThumbnailProcess: start, thumbnailStore: fakeStore(true), logger: new FakeLogger(), config },
      noTimeout,
    );

    const res = await gen.generate(job);

    expect(res).toEqual({ status: 'skipped', outputPath: '/out/data/clip.mp4.jpg', reason: 'exists' });
    expect(start.calls).toHaveLength(0); // never launched ffmpeg
  });

  it('regenerates an existing thumbnail when force is set', async () => {
    const start = makeFakeThumbnailProcess({ exitCode: 0 });
    const gen = new ThumbnailGenerator(
      {
        startThumbnailProcess: start,
        thumbnailStore: fakeStore(true),
        logger: new FakeLogger(),
        config: { ...config, force: true },
      },
      noTimeout,
    );

    const res = await gen.generate(job);
    expect(res.status).toBe('generated');
    expect(start.calls).toHaveLength(1);
  });

  it('retries on a non-zero exit and succeeds within the retry budget', async () => {
    // Fail attempt 0, succeed attempt 1.
    const start = makeFakeThumbnailProcess(({ attempt }) =>
      attempt === 0 ? { exitCode: 1, stderr: 'boom' } : { exitCode: 0 },
    );
    const gen = new ThumbnailGenerator(
      { startThumbnailProcess: start, thumbnailStore: fakeStore(false), logger: new FakeLogger(), config },
      fastTimer,
    );

    const res = await gen.generate(job);
    expect(res).toMatchObject({ status: 'generated', attempts: 2 });
    expect(start.calls).toHaveLength(2);
  });

  it('fails after exhausting retries, surfacing the exit code and stderr', async () => {
    const start = makeFakeThumbnailProcess({ exitCode: 1, stderr: 'ffmpeg: invalid data' });
    const gen = new ThumbnailGenerator(
      { startThumbnailProcess: start, thumbnailStore: fakeStore(false), logger: new FakeLogger(), config },
      fastTimer,
    );

    const res = await gen.generate(job);
    expect(res.status).toBe('failed');
    if (res.status === 'failed') {
      expect(res.attempts).toBe(3); // maxRetries(2) + 1
      expect(res.reason).toContain('code 1');
      expect(res.stderr).toBe('ffmpeg: invalid data');
    }
    expect(start.calls).toHaveLength(3);
  });

  it('kills a hung process on timeout and awaits it (no zombie)', async () => {
    const start = makeFakeThumbnailProcess({ hang: true });
    const gen = new ThumbnailGenerator(
      {
        startThumbnailProcess: start,
        thumbnailStore: fakeStore(false),
        logger: new FakeLogger(),
        config: { ...config, maxRetries: 0 },
      },
      fastTimer,
    );

    const res = await gen.generate(job);
    expect(res.status).toBe('failed');
    expect(start.processes[0]!.killed).toBe(true); // process was killed
  });

  it('treats a synchronous launch failure as a normal failure (not a crash)', async () => {
    const start = makeFakeThumbnailProcess({ throwOnLaunch: new Error('EMFILE: too many open files') });
    const gen = new ThumbnailGenerator(
      {
        startThumbnailProcess: start,
        thumbnailStore: fakeStore(false),
        logger: new FakeLogger(),
        config: { ...config, maxRetries: 0 },
      },
      fastTimer,
    );

    const res = await gen.generate(job);
    expect(res.status).toBe('failed');
    if (res.status === 'failed') expect(res.reason).toContain('launch failed');
  });
});
