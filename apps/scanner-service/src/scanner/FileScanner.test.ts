import { describe, it, expect } from 'vitest';
import { FakeFileSystem, FakeLogger, type FakeEntry } from '@thumbnailer/domain/fakes';
import { FileScanner } from './FileScanner.js';
import { VideoDetector } from './videoDetector.js';

const detector = new VideoDetector({ config: { videoExtensions: ['mp4', 'mov'] } });

function scanner(
  root: Record<string, FakeEntry>,
  over?: { maxDepth?: number; progressEvery?: number; yieldEvery?: number },
) {
  const logger = new FakeLogger();
  const fs = new FakeFileSystem(root);
  const fileScanner = new FileScanner({
    fs,
    logger,
    videoDetector: detector,
    config: {
      scanRoot: '',
      maxDepth: over?.maxDepth ?? 64,
      progressEvery: over?.progressEvery ?? 0,
      yieldEvery: over?.yieldEvery ?? 0,
    },
  });
  return { fileScanner, logger };
}

async function collect(s: FileScanner): Promise<string[]> {
  const out: string[] = [];
  for await (const v of s.walk()) out.push(v.path);
  return out.sort();
}

describe('FileScanner', () => {
  it('finds videos across a nested tree, skipping non-videos and directories', async () => {
    const { fileScanner } = scanner({
      a: {
        children: {
          'movie.mp4': { size: 10 },
          'notes.txt': { size: 10 },
          sub: { children: { 'clip.mov': { size: 10 }, 'pic.jpg': { size: 10 } } },
        },
      },
      'top.mp4': { size: 10 },
    });

    expect(await collect(fileScanner)).toEqual(['/a/movie.mp4', '/a/sub/clip.mov', '/top.mp4']);
  });

  it('handles a deep tree iteratively without stack overflow', async () => {
    // Build a 5000-deep chain with a video at the bottom.
    let node: FakeEntry = { children: { 'deep.mp4': { size: 1 } } };
    for (let i = 0; i < 5000; i++) node = { children: { d: node } };
    const { fileScanner } = scanner(node.children!, { maxDepth: 10_000 });

    const found = await collect(fileScanner);
    expect(found).toHaveLength(1);
    expect(found[0]).toContain('deep.mp4');
  });

  it('guards against cycles with the depth limit (does not hang)', async () => {
    // `loop` lists a link back to itself — an infinite cycle without a guard.
    const { fileScanner, logger } = scanner(
      { loop: { children: { 'v.mp4': { size: 1 } }, links: ['/loop'] } },
      { maxDepth: 5 },
    );

    const found = await collect(fileScanner);
    expect(found).toContain('/loop/v.mp4');
    expect(logger.lines.some((l) => l.message?.includes('max depth'))).toBe(true);
  });

  it('skips a directory whose listFiles throws (permission denied) and continues', async () => {
    const { fileScanner, logger } = scanner({
      ok: { children: { 'a.mp4': { size: 1 } } },
      denied: { children: {}, listError: new Error('EACCES: permission denied') },
    });

    expect(await collect(fileScanner)).toEqual(['/ok/a.mp4']);
    expect(logger.lines.some((l) => l.message?.includes('listFiles failed'))).toBe(true);
  });

  it('skips a file that vanished between list and stat (TOCTOU) and continues', async () => {
    const { fileScanner, logger } = scanner({
      'gone.mp4': { openError: new Error('ENOENT: vanished') },
      'here.mp4': { size: 1 },
    });

    expect(await collect(fileScanner)).toEqual(['/here.mp4']);
    expect(logger.lines.some((l) => l.message?.includes('openFile failed'))).toBe(true);
  });

  it('yields nothing for an empty tree', async () => {
    const { fileScanner } = scanner({});
    expect(await collect(fileScanner)).toEqual([]);
  });

  it('emits a progress heartbeat every N scanned entries', async () => {
    const children: Record<string, FakeEntry> = {};
    for (let i = 0; i < 5; i++) children[`f${i}.txt`] = { size: 1 };
    const { fileScanner, logger } = scanner(children, { progressEvery: 2 });

    await collect(fileScanner);
    const beats = logger.lines.filter((l) => l.message === 'scanning…');
    expect(beats.length).toBe(2); // at 2 and 4 of 5 entries
  });
});
