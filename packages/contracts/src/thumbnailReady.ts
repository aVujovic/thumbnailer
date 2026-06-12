import { z } from 'zod';

/**
 * Emitted by the generator on the `thumbnail-ready` topic once a thumbnail has
 * been successfully generated — a fact saying "this video's thumbnail exists and
 * is ready to be synced elsewhere". The sync-service consumes it, replicates the
 * file (here: just logs it), and flips the video's `synced` flag in the video DB.
 *
 * Decoupling: the generator doesn't know a sync-service exists; it just announces
 * the fact. Any number of downstream consumers (sync, search indexer, CDN
 * warmer, ...) can react without the generator changing.
 */
export const ThumbnailReadySchema = z.object({
  /** Source video path — the primary key joining this event to the video DB. */
  videoPath: z.string().min(1),
  /** Where the thumbnail was written (what would be synced). */
  outputPath: z.string().min(1),
  /** Image format (jpg, png, ...). */
  format: z.string().min(1),
  /** The tenant this thumbnail belongs to (if the job carried one). */
  tenantId: z.string().min(1).optional(),
  /** The scan this thumbnail belongs to (if the job carried one). */
  scanId: z.string().min(1).optional(),
  /** Epoch millis the generator finished generating. */
  generatedAt: z.number().int().nonnegative(),
});

export type ThumbnailReady = z.infer<typeof ThumbnailReadySchema>;

export function serializeThumbnailReady(event: ThumbnailReady): Buffer {
  return Buffer.from(JSON.stringify(event), 'utf-8');
}

export function parseThumbnailReady(raw: unknown): ThumbnailReady | null {
  const result = ThumbnailReadySchema.safeParse(raw);
  return result.success ? result.data : null;
}
