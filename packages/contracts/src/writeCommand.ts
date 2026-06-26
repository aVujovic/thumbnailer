import { z } from 'zod';
import { ThumbnailResultSchema } from './thumbnailResult.js';

/**
 * A database mutation, announced on the `db-flush` topic instead of being
 * applied inline. Generator/sync no longer write the DB directly — they emit a
 * WriteCommand and a single db-flush-service applies it in batches (async
 * write-behind). See DB-FLUSH-DESIGN.md.
 *
 * Every command is KEYED BY `key` (the videoPath / primary key) when produced,
 * so all mutations for one row land on one partition → applied in produce order
 * (upsert before mark-synced) and idempotently (an UPSERT replaces, never dupes).
 */
export const WriteCommandSchema = z.discriminatedUnion('op', [
  z.object({
    op: z.literal('upsert'),
    /** Primary key of the affected row (videoPath). The partition key. */
    key: z.string().min(1),
    /** The full row to insert-or-replace. */
    row: ThumbnailResultSchema,
  }),
  z.object({
    op: z.literal('mark-synced'),
    /** Primary key of the affected row (videoPath). The partition key. */
    key: z.string().min(1),
    /** Epoch millis the sync completed. */
    syncedAt: z.number().int().nonnegative(),
  }),
]);

export type WriteCommand = z.infer<typeof WriteCommandSchema>;

/** Builders — the one place a WriteCommand object is shaped. */
export const writeCommands = {
  upsert: (row: z.input<typeof ThumbnailResultSchema>): WriteCommand => ({
    op: 'upsert',
    key: row.videoPath,
    row: ThumbnailResultSchema.parse(row),
  }),
  markSynced: (videoPath: string, syncedAt: number): WriteCommand => ({
    op: 'mark-synced',
    key: videoPath,
    syncedAt,
  }),
} as const;

export function serializeWriteCommand(cmd: WriteCommand): Buffer {
  return Buffer.from(JSON.stringify(cmd), 'utf-8');
}

/** Parse + validate; null on malformed input (poison-message safe). */
export function parseWriteCommand(raw: unknown): WriteCommand | null {
  const result = WriteCommandSchema.safeParse(raw);
  return result.success ? result.data : null;
}
