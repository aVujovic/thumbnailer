import { describe, it, expect, vi } from 'vitest';
import { writeCommands, type ThumbnailResult } from '@thumbnailer/contracts';
import { DbSink } from './DbSink.js';
import type { IVideoRepository, VideoWriteOp } from '../repository/VideoRepository.js';

const row: ThumbnailResult = {
  videoPath: '/data/clip.mp4',
  outputPath: '/out/clip.mp4.jpg',
  size: 100,
  discoveredAt: 1,
  format: 'jpg',
  status: 'generated',
  attempts: 1,
  processedAt: 2,
  synced: false,
};

/** Fake repo — records which op-builders were called + the batch handed to transaction. */
function fakeRepo() {
  const upsertOp = vi.fn((r: ThumbnailResult) => ({ op: 'upsert', key: r.videoPath }) as unknown as VideoWriteOp);
  const markSyncedOp = vi.fn((k: string) => ({ op: 'markSynced', key: k }) as unknown as VideoWriteOp);
  const transaction = vi.fn(async (_ops: VideoWriteOp[]) => undefined);
  const close = vi.fn(async () => undefined);
  const repo: IVideoRepository = { upsertOp, markSyncedOp, transaction, close };
  return { repo, upsertOp, markSyncedOp, transaction, close };
}

describe('DbSink (command routing)', () => {
  it('does nothing on an empty batch (no transaction)', async () => {
    const { repo, transaction } = fakeRepo();
    await new DbSink({ videoRepository: repo }).applyBatch([]);
    expect(transaction).not.toHaveBeenCalled();
  });

  it('routes an upsert command to repo.upsertOp', async () => {
    const { repo, upsertOp, markSyncedOp } = fakeRepo();
    await new DbSink({ videoRepository: repo }).applyBatch([writeCommands.upsert(row)]);
    expect(upsertOp).toHaveBeenCalledOnce();
    expect(markSyncedOp).not.toHaveBeenCalled();
  });

  it('routes a mark-synced command to repo.markSyncedOp with key + syncedAt', async () => {
    const { repo, markSyncedOp } = fakeRepo();
    await new DbSink({ videoRepository: repo }).applyBatch([
      writeCommands.markSynced('/data/clip.mp4', 999),
    ]);
    expect(markSyncedOp).toHaveBeenCalledWith('/data/clip.mp4', 999);
  });

  it('funnels a mixed batch into ONE transaction (atomic)', async () => {
    const { repo, upsertOp, markSyncedOp, transaction } = fakeRepo();
    await new DbSink({ videoRepository: repo }).applyBatch([
      writeCommands.upsert(row),
      writeCommands.markSynced('/data/clip.mp4', 5),
    ]);
    expect(upsertOp).toHaveBeenCalledOnce();
    expect(markSyncedOp).toHaveBeenCalledOnce();
    expect(transaction).toHaveBeenCalledOnce();
    expect(transaction.mock.calls[0]![0]).toHaveLength(2); // both ops, one tx
  });

  it('propagates a transaction failure (so the consumer does not commit)', async () => {
    const { repo, transaction } = fakeRepo();
    (transaction as ReturnType<typeof vi.fn>).mockRejectedValueOnce(new Error('deadlock'));
    await expect(
      new DbSink({ videoRepository: repo }).applyBatch([writeCommands.upsert(row)]),
    ).rejects.toThrow('deadlock');
  });

  it('close() delegates to the repository', async () => {
    const { repo, close } = fakeRepo();
    await new DbSink({ videoRepository: repo }).close();
    expect(close).toHaveBeenCalledOnce();
  });
});
