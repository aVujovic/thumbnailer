import { describe, it, expect, vi } from 'vitest';
import { FakeLogger } from '@thumbnailer/domain/fakes';
import {
  serializeScanCommand,
  serializeVideoJob,
  type ThumbnailResult,
  type VideoJob,
} from '@thumbnailer/contracts';
import { KafkaUtils, type KafkaConnection } from '@thumbnailer/core';
import type { EachMessagePayload } from 'kafkajs';
import { VideoJobConsumer } from './VideoJobConsumer.job.js';
import type { ThumbnailGenerator, GenerateResult } from '../thumbnail/ThumbnailGenerator.js';
import type { IResultRepository } from '../repository/ResultRepository.js';

const job: VideoJob = { path: '/data/clip.mp4', size: 100, discoveredAt: 1 };

/**
 * Drives a VideoJobConsumer far enough to capture the eachMessage handler it
 * registers, so we can feed it messages and assert commit behaviour — no real
 * Kafka. Returns the handler plus spies on the underlying consumer.
 */
async function harness(generateResult: GenerateResult | Error) {
  const commitOffsets = vi.fn().mockResolvedValue(undefined);
  let handler!: (p: EachMessagePayload) => Promise<void>;

  const pause = vi.fn();
  const resume = vi.fn();
  const consumer = {
    subscribe: vi.fn().mockResolvedValue(undefined),
    stop: vi.fn().mockResolvedValue(undefined),
    run: vi.fn(async (opts: { eachMessage: (p: EachMessagePayload) => Promise<void> }) => {
      handler = opts.eachMessage;
    }),
    pause,
    resume,
    commitOffsets,
  };

  const send = vi.fn().mockResolvedValue(undefined);
  const producer = { send };
  const kafka = { consumer, producer } as unknown as KafkaConnection;
  const logger = new FakeLogger();

  const generator = {
    generate: vi.fn(async () => {
      if (generateResult instanceof Error) throw generateResult;
      return generateResult;
    }),
  } as unknown as ThumbnailGenerator;

  const saved: ThumbnailResult[] = [];
  const resultRepository: IResultRepository = {
    save: vi.fn(async (r: ThumbnailResult) => void saved.push(r)),
    markSynced: vi.fn(async () => null),
  };

  const consumerJob = new VideoJobConsumer(
    {
      logger,
      config: {
        topic: 'video-jobs',
        commandsTopic: 'scan-commands',
        readyTopic: 'thumbnail-ready',
        outputRoot: '/out',
        thumbnailFormat: 'jpg',
        kafka: { consumer: { groupId: 'g' } },
      } as never,
      kafka,
      kafkaUtils: new KafkaUtils({ logger }),
      thumbnailGenerator: generator,
      resultRepository,
    },
    () => 999, // fixed clock for processedAt
  );

  await consumerJob.start();
  return { handler, commitOffsets, generator, logger, saved, pause, resume, send };
}

function payload(value: Buffer | null, offset = '5'): EachMessagePayload {
  return {
    topic: 'video-jobs',
    partition: 0,
    message: { value, offset },
  } as unknown as EachMessagePayload;
}

function commandPayload(value: Buffer): EachMessagePayload {
  return {
    topic: 'scan-commands',
    partition: 0,
    message: { value, offset: '0' },
  } as unknown as EachMessagePayload;
}

describe('VideoJobConsumer commit semantics', () => {
  it('tolerates a commit failure — logs and does NOT throw out of eachMessage', async () => {
    const { handler, commitOffsets, logger, saved } = await harness({
      status: 'generated',
      outputPath: '/out/data/clip.mp4.jpg',
      attempts: 1,
    });
    commitOffsets.mockRejectedValueOnce(new Error('rebalance: partition revoked'));

    // A throw here would kill the consumer loop — it must not propagate.
    await expect(handler(payload(serializeVideoJob(job)))).resolves.toBeUndefined();

    expect(saved).toHaveLength(1); // work was done
    expect(logger.lines.some((l) => l.message?.includes('offset commit failed'))).toBe(true);
  });

  it('commits offset+1 after a successful generation (write-ahead)', async () => {
    const { handler, commitOffsets, generator } = await harness({
      status: 'generated',
      outputPath: '/out/data/clip.mp4.jpg',
      attempts: 1,
    });

    await handler(payload(serializeVideoJob(job), '5'));

    expect(generator.generate).toHaveBeenCalledOnce();
    expect(commitOffsets).toHaveBeenCalledWith([
      { topic: 'video-jobs', partition: 0, offset: '6' }, // offset + 1
    ]);
  });

  it('commits when generation is skipped (idempotent no-op)', async () => {
    const { handler, commitOffsets } = await harness({
      status: 'skipped',
      outputPath: '/out/data/clip.mp4.jpg',
      reason: 'exists',
    });

    await handler(payload(serializeVideoJob(job)));
    expect(commitOffsets).toHaveBeenCalledOnce();
  });

  it('commits after an exhausted failure to avoid a redelivery loop', async () => {
    const { handler, commitOffsets, logger } = await harness({
      status: 'failed',
      outputPath: '/out/data/clip.mp4.jpg',
      attempts: 3,
      reason: 'exited with code 1',
    });

    await handler(payload(serializeVideoJob(job)));

    expect(commitOffsets).toHaveBeenCalledOnce();
    expect(logger.lines.some((l) => l.message?.includes('after retries'))).toBe(true);
  });

  it('skips and commits a poison message without invoking the generator', async () => {
    const { handler, commitOffsets, generator } = await harness({
      status: 'generated',
      outputPath: '/x',
      attempts: 1,
    });

    await handler(payload(Buffer.from('not json'))); // poison

    expect(generator.generate).not.toHaveBeenCalled();
    expect(commitOffsets).toHaveBeenCalledOnce(); // committed so we don't re-read it
  });

  it('does NOT commit when generation throws (crash → redeliver)', async () => {
    const { handler, commitOffsets } = await harness(new Error('unexpected crash'));

    await expect(handler(payload(serializeVideoJob(job)))).rejects.toThrow('unexpected crash');
    expect(commitOffsets).not.toHaveBeenCalled(); // offset stays → message redelivered
  });
});

describe('VideoJobConsumer result recording', () => {
  it('records a full result row on success', async () => {
    const { handler, saved } = await harness({
      status: 'generated',
      outputPath: '/out/data/clip.mp4.jpg',
      attempts: 2,
    });

    await handler(payload(serializeVideoJob(job)));

    expect(saved).toHaveLength(1);
    expect(saved[0]).toEqual({
      videoPath: '/data/clip.mp4',
      outputPath: '/out/data/clip.mp4.jpg',
      size: 100,
      discoveredAt: 1,
      format: 'jpg',
      status: 'generated',
      attempts: 2,
      processedAt: 999,
      synced: false,
    });
  });

  it('records a skipped result with attempts=1 and no error', async () => {
    const { handler, saved } = await harness({
      status: 'skipped',
      outputPath: '/out/data/clip.mp4.jpg',
      reason: 'exists',
    });

    await handler(payload(serializeVideoJob(job)));

    expect(saved[0]).toMatchObject({ status: 'skipped', attempts: 1 });
    expect(saved[0]).not.toHaveProperty('error');
  });

  it('records a failed result including the error reason', async () => {
    const { handler, saved } = await harness({
      status: 'failed',
      outputPath: '/out/data/clip.mp4.jpg',
      attempts: 3,
      reason: 'exited with code 234',
    });

    await handler(payload(serializeVideoJob(job)));

    expect(saved[0]).toMatchObject({
      status: 'failed',
      attempts: 3,
      error: 'exited with code 234',
    });
  });

  it('does not record a result for a poison message', async () => {
    const { handler, saved } = await harness({ status: 'generated', outputPath: '/x', attempts: 1 });
    await handler(payload(Buffer.from('not json')));
    expect(saved).toHaveLength(0);
  });

  it('carries scanId from the job onto the result', async () => {
    const { handler, saved } = await harness({
      status: 'generated',
      outputPath: '/out/x.jpg',
      attempts: 1,
    });
    await handler(payload(serializeVideoJob({ ...job, scanId: 'scan-9' })));
    expect(saved[0]).toMatchObject({ scanId: 'scan-9' });
  });

  it('carries tenantId from the job onto the result', async () => {
    const { handler, saved } = await harness({
      status: 'generated',
      outputPath: '/out/tenant-1/x.jpg',
      attempts: 1,
    });
    await handler(payload(serializeVideoJob({ ...job, tenantId: 'tenant-1' })));
    expect(saved[0]).toMatchObject({ tenantId: 'tenant-1' });
  });
});

describe('VideoJobConsumer thumbnail-ready emission', () => {
  it('emits a ThumbnailReady event (keyed by videoPath) after a generated thumbnail', async () => {
    const { handler, send } = await harness({
      status: 'generated',
      outputPath: '/out/data/clip.mp4.jpg',
      attempts: 1,
    });

    await handler(payload(serializeVideoJob({ ...job, tenantId: 'tenant-1', scanId: 'scan-9' })));

    expect(send).toHaveBeenCalledOnce();
    const arg = send.mock.calls[0]![0] as { topic: string; messages: { key: string; value: Buffer }[] };
    expect(arg.topic).toBe('thumbnail-ready');
    expect(arg.messages[0]!.key).toBe('/data/clip.mp4');
    expect(JSON.parse(arg.messages[0]!.value.toString())).toMatchObject({
      videoPath: '/data/clip.mp4',
      outputPath: '/out/data/clip.mp4.jpg',
      format: 'jpg',
      tenantId: 'tenant-1',
      scanId: 'scan-9',
      generatedAt: 999,
    });
  });

  it('emits ThumbnailReady on a skipped thumbnail (the file already exists)', async () => {
    const { handler, send } = await harness({
      status: 'skipped',
      outputPath: '/out/data/clip.mp4.jpg',
      reason: 'exists',
    });
    await handler(payload(serializeVideoJob(job)));
    expect(send).toHaveBeenCalledOnce();
  });

  it('does NOT emit ThumbnailReady on a failed job (nothing was produced)', async () => {
    const { handler, send } = await harness({
      status: 'failed',
      outputPath: '/out/data/clip.mp4.jpg',
      attempts: 3,
      reason: 'boom',
    });
    await handler(payload(serializeVideoJob(job)));
    expect(send).not.toHaveBeenCalled();
  });

  it('still commits even if announcing thumbnail-ready throws (best-effort)', async () => {
    const { handler, send, commitOffsets } = await harness({
      status: 'generated',
      outputPath: '/x.jpg',
      attempts: 1,
    });
    send.mockRejectedValueOnce(new Error('broker down'));

    await handler(payload(serializeVideoJob(job)));
    expect(commitOffsets).toHaveBeenCalledOnce(); // announce failure didn't block the commit
  });
});

describe('VideoJobConsumer pause/resume', () => {
  it('pauses video-jobs on PauseGenerator and does not touch the generator', async () => {
    const { handler, pause, resume, generator } = await harness({
      status: 'generated',
      outputPath: '/x',
      attempts: 1,
    });

    await handler(commandPayload(serializeScanCommand({ type: 'PauseGenerator' })));

    expect(pause).toHaveBeenCalledWith([{ topic: 'video-jobs' }]);
    expect(resume).not.toHaveBeenCalled();
    expect(generator.generate).not.toHaveBeenCalled(); // a command is not a job
  });

  it('resumes video-jobs on ResumeGenerator', async () => {
    const { handler, pause, resume } = await harness({
      status: 'generated',
      outputPath: '/x',
      attempts: 1,
    });

    await handler(commandPayload(serializeScanCommand({ type: 'ResumeGenerator' })));

    expect(resume).toHaveBeenCalledWith([{ topic: 'video-jobs' }]);
    expect(pause).not.toHaveBeenCalled();
  });

  it('ignores scanner-only commands (StartScan/StopScan)', async () => {
    const { handler, pause, resume } = await harness({
      status: 'generated',
      outputPath: '/x',
      attempts: 1,
    });

    await handler(commandPayload(serializeScanCommand({ type: 'StopScan' })));
    await handler(commandPayload(serializeScanCommand({ type: 'StartScan', scanId: 'a', scanRoot: '/r' })));

    expect(pause).not.toHaveBeenCalled();
    expect(resume).not.toHaveBeenCalled();
  });
});
