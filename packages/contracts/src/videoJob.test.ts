import { describe, it, expect } from 'vitest';
import { serializeVideoJob, parseVideoJob, type VideoJob } from './videoJob.js';

const job: VideoJob = { path: '/data/clip.mp4', size: 4096, discoveredAt: 1700000000000 };

describe('videoJob', () => {
  it('round-trips through serialize -> parse', () => {
    const buf = serializeVideoJob(job);
    expect(Buffer.isBuffer(buf)).toBe(true);
    expect(parseVideoJob(JSON.parse(buf.toString('utf-8')))).toEqual(job);
  });

  it('accepts a valid object', () => {
    expect(parseVideoJob(job)).toEqual(job);
  });

  it('rejects malformed input (returns null, does not throw)', () => {
    expect(parseVideoJob(null)).toBeNull();
    expect(parseVideoJob({})).toBeNull();
    expect(parseVideoJob({ path: '', size: 1, discoveredAt: 1 })).toBeNull(); // empty path
    expect(parseVideoJob({ path: '/a', size: -1, discoveredAt: 1 })).toBeNull(); // negative size
    expect(parseVideoJob({ path: '/a', size: 1.5, discoveredAt: 1 })).toBeNull(); // non-int size
    expect(parseVideoJob({ path: '/a', size: 1 })).toBeNull(); // missing discoveredAt
    expect(parseVideoJob('not-an-object')).toBeNull();
  });

  it('strips unknown fields to the schema shape', () => {
    const parsed = parseVideoJob({ ...job, extra: 'ignored' });
    expect(parsed).toEqual(job);
    expect(parsed).not.toHaveProperty('extra');
  });
});
