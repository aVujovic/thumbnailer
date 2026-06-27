import type { Db } from '@thumbnailer/core';
import type { ThumbnailResult } from '@thumbnailer/contracts';

/**
 * One write operation against the `videos` table, returned (not awaited) so the
 * caller can batch several into a single transaction. Opaque to the sink — it
 * only knows "a thing I can run in a transaction", not that it's a Prisma promise.
 */
export type VideoWriteOp = ReturnType<Db['video']['upsert']> | ReturnType<Db['video']['updateMany']>;

/**
 * All contact with the video DB lives here. The sink decides WHICH operation a
 * WriteCommand maps to; this repository owns HOW that operation talks to the DB
 * (the Prisma calls, the column/BigInt mapping, the transaction). Behind an
 * interface so the sink is testable with a fake repo, and so the store is
 * swappable (a different DB = a different repository, same interface).
 */
export interface IVideoRepository {
  /** Build an idempotent upsert (insert-or-replace by videoPath). Not awaited. */
  upsertOp(row: ThumbnailResult): VideoWriteOp;
  /** Build an idempotent "mark synced" update keyed by videoPath. Not awaited. */
  markSyncedOp(videoPath: string, syncedAt: number): VideoWriteOp;
  /** Run a set of ops atomically (all-or-nothing). */
  transaction(ops: VideoWriteOp[]): Promise<void>;
  /** Release the connection on shutdown. */
  close(): Promise<void>;
}

/** Prisma/Postgres implementation. The only file that imports Prisma column shapes. */
export class PrismaVideoRepository implements IVideoRepository {
  private readonly db: Db;

  constructor(deps: { db: Db }) {
    this.db = deps.db;
  }

  upsertOp(row: ThumbnailResult): VideoWriteOp {
    const data = {
      videoPath: row.videoPath,
      scanId: row.scanId ?? null,
      tenantId: row.tenantId ?? null,
      outputPath: row.outputPath,
      size: BigInt(row.size),
      discoveredAt: BigInt(row.discoveredAt),
      format: row.format,
      status: row.status,
      attempts: row.attempts,
      error: row.error ?? null,
      processedAt: BigInt(row.processedAt),
      synced: row.synced,
      syncedAt: row.syncedAt !== undefined ? BigInt(row.syncedAt) : null,
    };
    return this.db.video.upsert({
      where: { videoPath: row.videoPath },
      create: data,
      update: data,
    });
  }

  markSyncedOp(videoPath: string, syncedAt: number): VideoWriteOp {
    // updateMany (not update) so a not-yet-present row affects 0 rows silently
    // instead of throwing P2025 — same-key ordering means the upsert normally
    // lands first, but this avoids a redelivery loop if a mark-synced ever
    // arrives ahead of its row.
    return this.db.video.updateMany({
      where: { videoPath },
      data: { synced: true, syncedAt: BigInt(syncedAt) },
    });
  }

  async transaction(ops: VideoWriteOp[]): Promise<void> {
    if (ops.length === 0) return;
    await this.db.$transaction(ops);
  }

  async close(): Promise<void> {
    await this.db.$disconnect();
  }
}

export default PrismaVideoRepository;
