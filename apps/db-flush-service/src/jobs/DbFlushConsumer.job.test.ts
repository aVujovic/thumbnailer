import { describe, it, expect, vi } from 'vitest';
import { FakeLogger } from '@thumbnailer/domain/fakes';
import { serializeWriteCommand, writeCommands, type WriteCommand } from '@thumbnailer/contracts';
import { KafkaUtils, type KafkaConnection } from '@thumbnailer/core';
import type { EachBatchPayload } from 'kafkajs';
import { DbFlushConsumer } from './DbFlushConsumer.job.js';
import type { IDbSink } from '../sink/DbSink.js';

/** Drive the consumer to capture its eachBatch handler. */
async function harness(batchMaxSize: number) {
  let handler!: (p: EachBatchPayload) => Promise<void>;
  const consumer = {
    subscribe: vi.fn().mockResolvedValue(undefined),
    run: vi.fn(async (o: { eachBatch: (p: EachBatchPayload) => Promise<void> }) => {
      handler = o.eachBatch;
    }),
  };
  const kafka = { consumer } as unknown as KafkaConnection;
  const logger = new FakeLogger();

  // Record each flushed sub-batch's size.
  const flushes: number[] = [];
  const applyBatch = vi.fn(async (cmds: WriteCommand[]) => void flushes.push(cmds.length));
  const dbSink: IDbSink = { applyBatch, close: vi.fn(async () => undefined) };

  const job = new DbFlushConsumer({
    logger,
    config: {
      flushTopic: 'db-flush',
      fromBeginning: true,
      batchMaxSize,
      kafka: { consumer: { groupId: 'g' } },
    } as never,
    kafka,
    kafkaUtils: new KafkaUtils({ logger }),
    dbSink,
  });
  await job.start();
  return { handler, flushes, applyBatch };
}

/** Build an eachBatch payload from a list of message values, with spies. */
function batchPayload(values: (Buffer | null)[], startOffset = 0): EachBatchPayload & {
  _resolved: string[];
} {
  const resolved: string[] = [];
  const messages = values.map((value, i) => ({ value, offset: String(startOffset + i) }));
  return {
    batch: { topic: 'db-flush', partition: 0, messages },
    resolveOffset: (o: string) => resolved.push(o),
    commitOffsetsIfNecessary: vi.fn(async () => undefined),
    heartbeat: vi.fn(async () => undefined),
    isRunning: () => true,
    isStale: () => false,
    _resolved: resolved,
  } as unknown as EachBatchPayload & { _resolved: string[] };
}

const upsert = (path: string) =>
  serializeWriteCommand(
    writeCommands.upsert({
      videoPath: path,
      outputPath: `${path}.jpg`,
      size: 1,
      discoveredAt: 1,
      format: 'jpg',
      status: 'generated',
      attempts: 1,
      processedAt: 1,
      synced: false,
    }),
  );

describe('DbFlushConsumer batching', () => {
  it('splits a fetched batch into sub-batches of at most batchMaxSize', async () => {
    const { handler, flushes } = await harness(2);
    const p = batchPayload([upsert('/a'), upsert('/b'), upsert('/c'), upsert('/d'), upsert('/e')]);

    await handler(p);

    // 5 messages, maxSize 2 → flushes of 2, 2, 1
    expect(flushes).toEqual([2, 2, 1]);
  });

  it('resolves the last offset of each flushed sub-batch (write-ahead)', async () => {
    const { handler } = await harness(2);
    const p = batchPayload([upsert('/a'), upsert('/b'), upsert('/c'), upsert('/d'), upsert('/e')]);

    await handler(p);

    // sub-batch [0,1] → resolve "1"; [2,3] → "3"; [4] → "4"
    expect(p._resolved).toEqual(['1', '3', '4']);
  });

  it('flushes the whole fetched batch at once when it fits under the size cap', async () => {
    const { handler, flushes } = await harness(500);
    await handler(batchPayload([upsert('/a'), upsert('/b'), upsert('/c')]));
    expect(flushes).toEqual([3]);
  });

  it('skips poison messages but still advances past them', async () => {
    const { handler, flushes } = await harness(500);
    const p = batchPayload([upsert('/a'), Buffer.from('not json'), upsert('/c')]);

    await handler(p);

    expect(flushes).toEqual([2]); // only the 2 valid commands applied
    expect(p._resolved).toEqual(['2']); // but offset advanced past the poison at index 1
  });

  it('does not resolve an offset when the flush fails (crash → redeliver)', async () => {
    const { handler, applyBatch } = await harness(500);
    applyBatch.mockRejectedValueOnce(new Error('deadlock'));
    const p = batchPayload([upsert('/a')]);

    await expect(handler(p)).rejects.toThrow('deadlock');
    expect(p._resolved).toEqual([]); // offset stays → batch redelivers
  });
});
