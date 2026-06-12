import { describe, it, expect } from 'vitest';
import { thumbnailOutputPath } from './outputPath.js';

describe('thumbnailOutputPath', () => {
  it('mirrors the source tree under the output root and appends .jpg', () => {
    expect(thumbnailOutputPath('/out', '/data/movies/clip.mp4')).toBe(
      '/out/data/movies/clip.mp4.jpg',
    );
  });

  it('keeps the extension so different videos with the same stem do not collide', () => {
    expect(thumbnailOutputPath('/out', '/a/clip.mp4')).toBe('/out/a/clip.mp4.jpg');
    expect(thumbnailOutputPath('/out', '/a/clip.mov')).toBe('/out/a/clip.mov.jpg');
  });

  it('handles a relative output root', () => {
    expect(thumbnailOutputPath('./output', '/data/x.mp4')).toBe('output/data/x.mp4.jpg');
  });

  it('strips path-traversal segments so output stays inside the root', () => {
    expect(thumbnailOutputPath('/out', '/../../etc/passwd.mp4')).toBe('/out/etc/passwd.mp4.jpg');
    expect(thumbnailOutputPath('/out', 'a/../../b/v.mp4')).toBe('/out/b/v.mp4.jpg');
  });

  it('nests under a per-tenant subdirectory when a tenantId is given', () => {
    expect(thumbnailOutputPath('/out', '/data/clip.mp4', 'jpg', 'tenant-1')).toBe(
      '/out/tenant-1/data/clip.mp4.jpg',
    );
  });

  it('keeps the tenant segment from escaping the root', () => {
    expect(thumbnailOutputPath('/out', '/v.mp4', 'jpg', '../evil')).toBe('/out/evil/v.mp4.jpg');
  });
});
