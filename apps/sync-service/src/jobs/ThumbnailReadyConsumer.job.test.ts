import { describe, it, expect, vi } from 'vitest';
import { FakeLogger } from '@thumbnailer/domain/fakes';
import {
  serializeThumbnailReady,
  type ThumbnailReady,
  type ThumbnailResult,
} from '@thumbnailer/contracts';
import { KafkaUtils, type KafkaConnection } from '@thumbnailer/core';
import type { EachMessagePayload } from 'kafkajs';
import { ThumbnailReadyConsumer } from './ThumbnailReadyConsumer.job.js';
import type { ISyncTarget } from '../sync/SyncTarget.js';
import type { IVideoDb } from '../repository/VideoDb.js';

const event: ThumbnailReady = {
  videoPath: '/data/clip.mp4',
  outputPath: '/out/clip.mp4.jpg',
  format: 'jpg',
  generatedAt: 1,
};

const row = (over: Partial<ThumbnailResult> = {}): ThumbnailResult => ({
  videoPath: '/data/clip.mp4',
  outputPath: '/out/clip.mp4.jpg',
  size: 1,
  discoveredAt: 1,
  format: 'jpg',
  status: 'generated',
  attempts: 1,
  processedAt: 2,
  synced: true,
  syncedAt: 999,
  ...over,
});

/** Drive the consumer far enough to capture its eachMessage handler. */
async function harness(opts: { syncThrows?: boolean; markReturns?: ThumbnailResult | null } = {}) {
  const commitOffsets = vi.fn().mockResolvedValue(undefined);
  let handler!: (p: EachMessagePayload) => Promise<void>;
  const consumer = {
    subscribe: vi.fn().mockResolvedValue(undefined),
    run: vi.fn(async (o: { eachMessage: (p: EachMessagePayload) => Promise<void> }) => {
      handler = o.eachMessage;
    }),
    commitOffsets,
  };
  const kafka = { consumer } as unknown as KafkaConnection;
  const logger = new FakeLogger();

  const sync = vi.fn(async () => {
    if (opts.syncThrows) throw new Error('rsync failed');
  });
  const syncTarget: ISyncTarget = { sync };

  const markSynced = vi.fn(async () =>
    opts.markReturns === undefined ? row() : opts.markReturns,
  );
  const videoDb: IVideoDb = { markSynced };

  const job = new ThumbnailReadyConsumer(
    {
      logger,
      config: {
        readyTopic: 'thumbnail-ready',
        fromBeginning: true,
        videoDbPath: '/db.json',
        kafka: { consumer: { groupId: 'g' } },
      } as never,
      kafka,
      kafkaUtils: new KafkaUtils({ logger }),
      syncTarget,
      videoDb,
    },
    () => 999,
  );

  await job.start();
  return { handler, commitOffsets, sync, markSynced, logger };
}

function payload(value: Buffer | null, offset = '5'): EachMessagePayload {
  return {
    topic: 'thumbnail-ready',
    partition: 0,
    message: { value, offset },
  } as unknown as EachMessagePayload;
}

describe('ThumbnailReadyConsumer', () => {
  it('syncs the thumbnail, marks the row synced, then commits offset+1', async () => {
    const { handler, commitOffsets, sync, markSynced } = await harness();

    await handler(payload(serializeThumbnailReady(event), '5'));

    expect(sync).toHaveBeenCalledOnce();
    expect(markSynced).toHaveBeenCalledWith('/data/clip.mp4', 999);
    expect(commitOffsets).toHaveBeenCalledWith([
      { topic: 'thumbnail-ready', partition: 0, offset: '6' },
    ]);
  });

  it('tolerates a commit failure — logs and does NOT throw out of eachMessage', async () => {
    const { handler, commitOffsets, markSynced, logger } = await harness();
    commitOffsets.mockRejectedValueOnce(new Error('rebalance: partition revoked'));

    await expect(handler(payload(serializeThumbnailReady(event)))).resolves.toBeUndefined();

    expect(markSynced).toHaveBeenCalledOnce(); // work was done
    expect(logger.lines.some((l) => l.message?.includes('offset commit failed'))).toBe(true);
  });

  it('does NOT commit when the sync fails (crash → redeliver)', async () => {
    const { handler, commitOffsets, markSynced } = await harness({ syncThrows: true });

    await expect(handler(payload(serializeThumbnailReady(event)))).rejects.toThrow('rsync failed');
    expect(markSynced).not.toHaveBeenCalled(); // never reached the DB update
    expect(commitOffsets).not.toHaveBeenCalled();
  });

  it('still commits (terminal) when no DB row matches, but warns', async () => {
    const { handler, commitOffsets, logger } = await harness({ markReturns: null });

    await handler(payload(serializeThumbnailReady(event)));

    expect(commitOffsets).toHaveBeenCalledOnce();
    expect(logger.lines.some((l) => l.message?.includes('no video-DB row'))).toBe(true);
  });

  it('skips and commits a poison message without syncing', async () => {
    const { handler, commitOffsets, sync } = await harness();

    await handler(payload(Buffer.from('not json')));

    expect(sync).not.toHaveBeenCalled();
    expect(commitOffsets).toHaveBeenCalledOnce();
  });
});
