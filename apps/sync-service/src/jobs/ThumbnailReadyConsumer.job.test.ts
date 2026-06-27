import { describe, it, expect, vi } from 'vitest';
import { FakeLogger } from '@thumbnailer/domain/fakes';
import {
  parseWriteCommand,
  serializeThumbnailReady,
  type ThumbnailReady,
} from '@thumbnailer/contracts';
import { KafkaUtils, type KafkaConnection } from '@thumbnailer/core';
import type { EachMessagePayload } from 'kafkajs';
import { ThumbnailReadyConsumer } from './ThumbnailReadyConsumer.job.js';
import type { ISyncTarget } from '../sync/SyncTarget.js';

const event: ThumbnailReady = {
  videoPath: '/data/clip.mp4',
  outputPath: '/out/clip.mp4.jpg',
  format: 'jpg',
  generatedAt: 1,
};

/** Drive the consumer far enough to capture its eachMessage handler. */
async function harness(opts: { syncThrows?: boolean } = {}) {
  const commitOffsets = vi.fn().mockResolvedValue(undefined);
  let handler!: (p: EachMessagePayload) => Promise<void>;
  const consumer = {
    subscribe: vi.fn().mockResolvedValue(undefined),
    run: vi.fn(async (o: { eachMessage: (p: EachMessagePayload) => Promise<void> }) => {
      handler = o.eachMessage;
    }),
    commitOffsets,
  };
  const send = vi.fn().mockResolvedValue(undefined);
  const kafka = { consumer, producer: { send } } as unknown as KafkaConnection;
  const logger = new FakeLogger();

  const sync = vi.fn(async () => {
    if (opts.syncThrows) throw new Error('rsync failed');
  });
  const syncTarget: ISyncTarget = { sync };

  const job = new ThumbnailReadyConsumer(
    {
      logger,
      config: {
        readyTopic: 'thumbnail-ready',
        flushTopic: 'db-flush',
        fromBeginning: true,
        kafka: { consumer: { groupId: 'g' } },
      } as never,
      kafka,
      kafkaUtils: new KafkaUtils({ logger }),
      syncTarget,
    },
    () => 999,
  );

  await job.start();

  // mark-synced WriteCommands emitted onto db-flush.
  const flushed = () =>
    send.mock.calls
      .map((c) => c[0] as { topic: string; messages: { value: Buffer }[] })
      .filter((arg) => arg.topic === 'db-flush')
      .map((arg) => parseWriteCommand(JSON.parse(arg.messages[0]!.value.toString())));

  return { handler, commitOffsets, sync, send, flushed, logger };
}

function payload(value: Buffer | null, offset = '5'): EachMessagePayload {
  return {
    topic: 'thumbnail-ready',
    partition: 0,
    message: { value, offset },
  } as unknown as EachMessagePayload;
}

describe('ThumbnailReadyConsumer', () => {
  it('syncs the thumbnail, emits a mark-synced WriteCommand (keyed by videoPath), then commits offset+1', async () => {
    const { handler, commitOffsets, sync, send, flushed } = await harness();

    await handler(payload(serializeThumbnailReady(event), '5'));

    expect(sync).toHaveBeenCalledOnce();
    // emitted onto db-flush, keyed by videoPath
    const arg = send.mock.calls[0]![0] as { topic: string; messages: { key: string }[] };
    expect(arg.topic).toBe('db-flush');
    expect(arg.messages[0]!.key).toBe('/data/clip.mp4');
    expect(flushed()[0]).toEqual({ op: 'mark-synced', key: '/data/clip.mp4', syncedAt: 999 });
    expect(commitOffsets).toHaveBeenCalledWith([
      { topic: 'thumbnail-ready', partition: 0, offset: '6' },
    ]);
  });

  it('tolerates a commit failure — logs and does NOT throw out of eachMessage', async () => {
    const { handler, commitOffsets, flushed, logger } = await harness();
    commitOffsets.mockRejectedValueOnce(new Error('rebalance: partition revoked'));

    await expect(handler(payload(serializeThumbnailReady(event)))).resolves.toBeUndefined();

    expect(flushed()).toHaveLength(1); // work was done (mark-synced emitted)
    expect(logger.lines.some((l) => l.message?.includes('offset commit failed'))).toBe(true);
  });

  it('does NOT commit when the sync fails (crash → redeliver)', async () => {
    const { handler, commitOffsets, send } = await harness({ syncThrows: true });

    await expect(handler(payload(serializeThumbnailReady(event)))).rejects.toThrow('rsync failed');
    expect(send).not.toHaveBeenCalled(); // never reached the emit
    expect(commitOffsets).not.toHaveBeenCalled();
  });

  it('does NOT commit when the mark-synced emit fails (crash → redeliver, sync not lost)', async () => {
    const { handler, commitOffsets, send } = await harness();
    send.mockRejectedValueOnce(new Error('broker down'));

    await expect(handler(payload(serializeThumbnailReady(event)))).rejects.toThrow('broker down');
    expect(commitOffsets).not.toHaveBeenCalled();
  });

  it('skips and commits a poison message without syncing', async () => {
    const { handler, commitOffsets, sync, send } = await harness();

    await handler(payload(Buffer.from('not json')));

    expect(sync).not.toHaveBeenCalled();
    expect(send).not.toHaveBeenCalled();
    expect(commitOffsets).toHaveBeenCalledOnce();
  });
});
