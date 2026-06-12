import { posix } from 'node:path';

/** Strip leading slashes and any `..`/`.` segments so a path can't escape its root. */
function safeRelative(p: string): string {
  return posix
    .normalize(p)
    .split('/')
    .filter((seg) => seg && seg !== '..' && seg !== '.')
    .join('/');
}

/**
 * Maps a source video path to its thumbnail output path, mirroring the source
 * tree under `outputRoot` (optionally under a per-tenant subdirectory). Keeps the
 * original file name (extension included) and appends `.{format}`, so two
 * different videos can never collide:
 *
 *   outputRoot=/out, video=/data/movies/clip.mp4, format=jpg
 *     ->  /out/data/movies/clip.mp4.jpg
 *   ... with tenantId=tenant-1
 *     ->  /out/tenant-1/data/movies/clip.mp4.jpg
 *
 * The video path and tenant are normalized so neither can escape `outputRoot`
 * (path traversal guard).
 */
export function thumbnailOutputPath(
  outputRoot: string,
  videoPath: string,
  format = 'jpg',
  tenantId?: string,
): string {
  const ext = format.toLowerCase().replace(/^\./, '');
  const tenantSeg = tenantId ? safeRelative(tenantId) : '';
  return posix.join(outputRoot, tenantSeg, `${safeRelative(videoPath)}.${ext}`);
}
