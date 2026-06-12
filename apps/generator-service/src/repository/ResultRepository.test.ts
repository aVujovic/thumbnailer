import { describe, it, expect, afterEach } from 'vitest';
import { readFileSync, rmSync, mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { ThumbnailResult } from '@thumbnailer/contracts';
import { JsonlResultRepository } from './ResultRepository.js';

const dirs: string[] = [];
function tmpPath(name: string): string {
  const dir = mkdtempSync(join(tmpdir(), 'results-'));
  dirs.push(dir);
  return join(dir, name);
}

afterEach(() => {
  for (const d of dirs.splice(0)) rmSync(d, { recursive: true, force: true });
});

function result(over: Partial<ThumbnailResult>): ThumbnailResult {
  return {
    videoPath: '/v.mp4',
    outputPath: '/out/v.mp4.jpg',
    size: 10,
    discoveredAt: 1,
    format: 'jpg',
    status: 'generated',
    attempts: 1,
    processedAt: 2,
    synced: false,
    ...over,
  };
}

describe('JsonlResultRepository', () => {
  it('appends one JSON line per result, creating the dir', async () => {
    const path = tmpPath('nested/results.jsonl'); // dir does not exist yet
    const repo = new JsonlResultRepository({ config: { resultsPath: path } });

    await repo.save(result({ videoPath: '/a.mp4' }));
    await repo.save(result({ videoPath: '/b.mp4', status: 'failed', error: 'boom' }));

    const lines = readFileSync(path, 'utf-8').trim().split('\n');
    expect(lines).toHaveLength(2);
    expect(JSON.parse(lines[0]!)).toMatchObject({ videoPath: '/a.mp4', status: 'generated' });
    expect(JSON.parse(lines[1]!)).toMatchObject({ videoPath: '/b.mp4', status: 'failed', error: 'boom' });
  });

  it('appends to an existing file across instances (durable log)', async () => {
    const path = tmpPath('results.jsonl');
    await new JsonlResultRepository({ config: { resultsPath: path } }).save(result({ videoPath: '/1' }));
    // a fresh instance (e.g. after restart) keeps appending, not truncating
    await new JsonlResultRepository({ config: { resultsPath: path } }).save(result({ videoPath: '/2' }));

    const lines = readFileSync(path, 'utf-8').trim().split('\n');
    expect(lines).toHaveLength(2);
  });

  it('upserts by videoPath — reprocessing replaces the row, never duplicates', async () => {
    const path = tmpPath('videoDb.jsonl');
    const repo = new JsonlResultRepository({ config: { resultsPath: path } });
    await repo.save(result({ videoPath: '/v.mp4', attempts: 1 }));
    await repo.save(result({ videoPath: '/v.mp4', attempts: 3 })); // same key, new outcome

    const lines = readFileSync(path, 'utf-8').trim().split('\n');
    expect(lines).toHaveLength(1);
    expect(JSON.parse(lines[0]!)).toMatchObject({ videoPath: '/v.mp4', attempts: 3 });
  });

  it('markSynced flips the matched row to synced:true with a syncedAt', async () => {
    const path = tmpPath('videoDb.jsonl');
    const repo = new JsonlResultRepository({ config: { resultsPath: path } });
    await repo.save(result({ videoPath: '/a.mp4' }));
    await repo.save(result({ videoPath: '/b.mp4' }));

    const updated = await repo.markSynced('/b.mp4', 999);
    expect(updated).toMatchObject({ videoPath: '/b.mp4', synced: true, syncedAt: 999 });

    const rows = readFileSync(path, 'utf-8').trim().split('\n').map((l) => JSON.parse(l));
    expect(rows.find((r) => r.videoPath === '/a.mp4').synced).toBe(false); // untouched
    expect(rows.find((r) => r.videoPath === '/b.mp4').synced).toBe(true);
  });

  it('markSynced returns null when no row matches', async () => {
    const path = tmpPath('videoDb.jsonl');
    const repo = new JsonlResultRepository({ config: { resultsPath: path } });
    await repo.save(result({ videoPath: '/a.mp4' }));
    expect(await repo.markSynced('/missing.mp4', 1)).toBeNull();
  });
});
