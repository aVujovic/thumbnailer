import { describe, it, expect, vi } from 'vitest';
import type { ThumbnailResult } from '@thumbnailer/contracts';
import type { Db } from '@thumbnailer/core';
import { PrismaVideoRepository } from './VideoRepository.js';

const row: ThumbnailResult = {
  videoPath: '/data/clip.mp4',
  outputPath: '/out/clip.mp4.jpg',
  scanId: 'scan-1',
  tenantId: 'tenant-1',
  size: 100,
  discoveredAt: 1,
  format: 'jpg',
  status: 'generated',
  attempts: 1,
  processedAt: 2,
  synced: false,
};

/** Fake Prisma client — records the calls. */
function fakeDb() {
  const upsert = vi.fn((args: unknown) => ({ kind: 'upsert', args }));
  const updateMany = vi.fn((args: unknown) => ({ kind: 'updateMany', args }));
  const $transaction = vi.fn(async (ops: unknown[]) => ops);
  const $disconnect = vi.fn(async () => undefined);
  const db = { video: { upsert, updateMany }, $transaction, $disconnect } as unknown as Db;
  return { db, upsert, updateMany, $transaction, $disconnect };
}

describe('PrismaVideoRepository', () => {
  it('upsertOp maps a row to video.upsert keyed by videoPath, BigInt-coercing numbers', () => {
    const { db, upsert } = fakeDb();
    new PrismaVideoRepository({ db }).upsertOp(row);

    const arg = upsert.mock.calls[0]![0] as {
      where: { videoPath: string };
      create: { size: bigint; processedAt: bigint; synced: boolean; syncedAt: bigint | null };
    };
    expect(arg.where).toEqual({ videoPath: '/data/clip.mp4' });
    expect(arg.create.size).toBe(100n);
    expect(arg.create.processedAt).toBe(2n);
    expect(arg.create.synced).toBe(false);
    expect(arg.create.syncedAt).toBeNull();
  });

  it('markSyncedOp maps to video.updateMany (no throw on missing row)', () => {
    const { db, updateMany } = fakeDb();
    new PrismaVideoRepository({ db }).markSyncedOp('/data/clip.mp4', 999);

    expect(updateMany.mock.calls[0]![0]).toEqual({
      where: { videoPath: '/data/clip.mp4' },
      data: { synced: true, syncedAt: 999n },
    });
  });

  it('transaction runs the ops in one $transaction', async () => {
    const { db, $transaction } = fakeDb();
    const repo = new PrismaVideoRepository({ db });
    await repo.transaction([repo.upsertOp(row), repo.markSyncedOp('/data/clip.mp4', 5)]);

    expect($transaction).toHaveBeenCalledOnce();
    expect($transaction.mock.calls[0]![0]).toHaveLength(2);
  });

  it('transaction is a no-op for an empty batch', async () => {
    const { db, $transaction } = fakeDb();
    await new PrismaVideoRepository({ db }).transaction([]);
    expect($transaction).not.toHaveBeenCalled();
  });

  it('close() disconnects the client', async () => {
    const { db, $disconnect } = fakeDb();
    await new PrismaVideoRepository({ db }).close();
    expect($disconnect).toHaveBeenCalledOnce();
  });
});
