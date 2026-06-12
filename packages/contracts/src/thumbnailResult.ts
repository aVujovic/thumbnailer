import { z } from 'zod';

/**
 * The status of one video — a row in the "video DB". In production this is a row
 * in a Postgres/MySQL table; here it's one line in `videoDb.jsonl` (JSONL). The
 * shape is the same either way, which is why it lives in contracts (the "table
 * schema"). The generator INSERTs a row per processed video; the sync-service
 * later UPDATEs that row's `synced` flag to true once the thumbnail is synced.
 */
export const ThumbnailResultSchema = z.object({
  /** The scan this result belongs to (if the job carried one). */
  scanId: z.string().min(1).optional(),
  /** The tenant this result belongs to (if the job carried one). */
  tenantId: z.string().min(1).optional(),
  /** Source video path. */
  videoPath: z.string().min(1),
  /** Where the thumbnail was (or would be) written. */
  outputPath: z.string().min(1),
  /** Video size in bytes (from the scan). */
  size: z.number().int().nonnegative(),
  /** Epoch millis the scanner discovered the file. */
  discoveredAt: z.number().int().nonnegative(),
  /** Thumbnail image format (jpg, png, ...). */
  format: z.string().min(1),
  /** Terminal outcome of generation. */
  status: z.enum(['generated', 'skipped', 'failed']),
  /** Attempts made (1 for skip; 1..maxRetries+1 otherwise). */
  attempts: z.number().int().positive(),
  /** Failure reason — present only when status is 'failed'. */
  error: z.string().optional(),
  /** Epoch millis the result was recorded. */
  processedAt: z.number().int().nonnegative(),
  /**
   * Whether the thumbnail has been synced to its destination by the
   * sync-service. The generator INSERTs rows with `synced: false`; the
   * sync-service flips it to true. Defaults to false so older rows read sanely.
   */
  synced: z.boolean().default(false),
  /** Epoch millis the sync-service synced it — present once `synced` is true. */
  syncedAt: z.number().int().nonnegative().optional(),
});

export type ThumbnailResult = z.infer<typeof ThumbnailResultSchema>;

/** Serialize one result as a single JSONL line (newline included). */
export function serializeResultLine(result: ThumbnailResult): string {
  return `${JSON.stringify(result)}\n`;
}

/** Parse one JSONL line back into a result row (null on malformed/invalid). */
export function parseResultLine(line: string): ThumbnailResult | null {
  const trimmed = line.trim();
  if (!trimmed) return null;
  try {
    const parsed = ThumbnailResultSchema.safeParse(JSON.parse(trimmed));
    return parsed.success ? parsed.data : null;
  } catch {
    return null;
  }
}
