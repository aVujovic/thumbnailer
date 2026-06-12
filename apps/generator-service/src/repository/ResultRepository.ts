import { appendFileSync, mkdirSync, readFileSync, writeFileSync, existsSync } from 'node:fs';
import { dirname } from 'node:path';
import {
  parseResultLine,
  serializeResultLine,
  type ThumbnailResult,
} from '@thumbnailer/contracts';

/**
 * The "video DB" — one row per video, keyed by videoPath. In production this is
 * a Postgres/MySQL table; here it's `videoDb.jsonl` (JSONL, one row per line).
 * The generator INSERTs rows (`save`); the sync-service UPDATEs the `synced`
 * flag (`markSynced`). Abstracted so neither service cares it's a file.
 */
export interface IResultRepository {
  /** Insert (or upsert by videoPath) a processed-video row. */
  save(result: ThumbnailResult): Promise<void>;
  /**
   * Mark a video's thumbnail as synced. Returns the updated row, or null if no
   * row matched (e.g. event for a video not in the DB). Used by sync-service.
   */
  markSynced(videoPath: string, syncedAt: number): Promise<ThumbnailResult | null>;
}

/**
 * JSONL-backed video DB: one JSON row per line. `save` appends (fast, atomic,
 * many writers never clobber each other); `markSynced` is a read-modify-write
 * that rewrites the file with the matched row updated — the file-DB equivalent
 * of an UPDATE. A production DB repo would swap append+rewrite for INSERT/UPDATE
 * behind the same interface.
 *
 * On `save` we also collapse duplicate rows for the same videoPath (last wins),
 * so reprocessing a video doesn't leave stale rows — keeping it a true keyed
 * store rather than an append log.
 */
export class JsonlResultRepository implements IResultRepository {
  private readonly path: string;
  private dirEnsured = false;

  constructor(deps: { config: { resultsPath: string } }) {
    this.path = deps.config.resultsPath;
  }

  private ensureDir(): void {
    if (this.dirEnsured) return;
    mkdirSync(dirname(this.path), { recursive: true });
    this.dirEnsured = true;
  }

  private readAll(): ThumbnailResult[] {
    if (!existsSync(this.path)) return [];
    return readFileSync(this.path, 'utf-8')
      .split('\n')
      .map(parseResultLine)
      .filter((r): r is ThumbnailResult => r !== null);
  }

  private writeAll(rows: ThumbnailResult[]): void {
    this.ensureDir();
    writeFileSync(this.path, rows.map(serializeResultLine).join(''));
  }

  async save(result: ThumbnailResult): Promise<void> {
    this.ensureDir();
    const existing = this.readAll();
    const had = existing.some((r) => r.videoPath === result.videoPath);
    if (!had) {
      // Fast path: brand-new video → atomic append, no rewrite.
      appendFileSync(this.path, serializeResultLine(result));
      return;
    }
    // Reprocessed video → upsert by replacing the prior row (last write wins).
    this.writeAll(
      existing.map((r) => (r.videoPath === result.videoPath ? result : r)),
    );
  }

  async markSynced(videoPath: string, syncedAt: number): Promise<ThumbnailResult | null> {
    const rows = this.readAll();
    const idx = rows.findIndex((r) => r.videoPath === videoPath);
    if (idx === -1) return null;
    const updated: ThumbnailResult = { ...rows[idx]!, synced: true, syncedAt };
    rows[idx] = updated;
    this.writeAll(rows);
    return updated;
  }
}

export default JsonlResultRepository;
