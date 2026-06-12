import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';
import {
  parseResultLine,
  serializeResultLine,
  type ThumbnailResult,
} from '@thumbnailer/contracts';

/**
 * Read/update access to the shared "video DB" — the same JSONL file the
 * generator writes (a Postgres/MySQL table in production). The sync-service only
 * ever UPDATEs the `synced` flag, so this exposes just that operation, behind an
 * interface so the consumer is testable without touching the filesystem.
 */
export interface IVideoDb {
  /**
   * Flip a video's row to synced. Returns the updated row, or null if no row
   * matched the path (e.g. a ready-event arrived before its row was written, or
   * for a video that isn't in this DB).
   */
  markSynced(videoPath: string, syncedAt: number): Promise<ThumbnailResult | null>;
}

/**
 * JSONL-backed implementation: read-modify-write the file, replacing the matched
 * row. This is the file-DB equivalent of `UPDATE videos SET synced=true WHERE
 * path=?`. A production DB repo swaps the body for a real UPDATE behind the same
 * interface.
 */
export class JsonlVideoDb implements IVideoDb {
  private readonly path: string;

  constructor(deps: { config: { videoDbPath: string } }) {
    this.path = deps.config.videoDbPath;
  }

  private readAll(): ThumbnailResult[] {
    if (!existsSync(this.path)) return [];
    return readFileSync(this.path, 'utf-8')
      .split('\n')
      .map(parseResultLine)
      .filter((r): r is ThumbnailResult => r !== null);
  }

  async markSynced(videoPath: string, syncedAt: number): Promise<ThumbnailResult | null> {
    const rows = this.readAll();
    const idx = rows.findIndex((r) => r.videoPath === videoPath);
    if (idx === -1) return null;
    const updated: ThumbnailResult = { ...rows[idx]!, synced: true, syncedAt };
    rows[idx] = updated;
    mkdirSync(dirname(this.path), { recursive: true });
    writeFileSync(this.path, rows.map(serializeResultLine).join(''));
    return updated;
  }
}

export default JsonlVideoDb;
