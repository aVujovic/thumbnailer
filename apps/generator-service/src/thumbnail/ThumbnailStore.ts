import { existsSync, mkdirSync } from 'node:fs';
import { dirname } from 'node:path';

/**
 * Where thumbnails live. Abstracted so the idempotency check and directory
 * preparation don't hard-code the local disk — a remote store (S3, etc.) can
 * implement the same interface later (see PLAN, out-of-scope).
 */
export interface IThumbnailStore {
  /** True if a thumbnail already exists at `outputPath` (idempotency). */
  exists(outputPath: string): boolean;
  /** Ensure the parent directory of `outputPath` exists before ffmpeg writes. */
  ensureDir(outputPath: string): void;
}

/** Local-disk implementation backed by node:fs. */
export class LocalThumbnailStore implements IThumbnailStore {
  exists(outputPath: string): boolean {
    return existsSync(outputPath);
  }

  ensureDir(outputPath: string): void {
    mkdirSync(dirname(outputPath), { recursive: true });
  }
}

export default LocalThumbnailStore;
