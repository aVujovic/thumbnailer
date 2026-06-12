import { describe, it, expect, vi } from 'vitest';
import { parseVideoJob } from '@thumbnailer/contracts';
import { FakeLogger } from '@thumbnailer/domain/fakes';
import type { FileInfo } from '@thumbnailer/domain';
import type { KafkaConnection } from '@thumbnailer/core';
import { JobProducer } from './JobProducer.js';

function fakeKafka() {
  const send = vi.fn().mockResolvedValue(undefined);
  const kafka = { producer: { send } } as unknown as KafkaConnection;
  return { kafka, send };
}

const info: FileInfo = { name: 'clip.mp4', isDirectory: false, size: 4096, permissions: 0o644 };

describe('JobProducer', () => {
  it('produces a valid VideoJob keyed by path', async () => {
    const { kafka, send } = fakeKafka();
    const producer = new JobProducer(
      { kafka, logger: new FakeLogger(), config: { topic: 'video-jobs' } },
      () => 1000,
    );

    await producer.produce('/data/clip.mp4', info);

    expect(send).toHaveBeenCalledOnce();
    const arg = send.mock.calls[0]![0];
    expect(arg.topic).toBe('video-jobs');
    expect(arg.messages[0].key).toBe('/data/clip.mp4');

    const decoded = parseVideoJob(JSON.parse(arg.messages[0].value.toString('utf-8')));
    expect(decoded).toEqual({ path: '/data/clip.mp4', size: 4096, discoveredAt: 1000 });
  });

  it('propagates a producer send failure', async () => {
    const { kafka, send } = fakeKafka();
    send.mockRejectedValueOnce(new Error('broker down'));
    const producer = new JobProducer({
      kafka,
      logger: new FakeLogger(),
      config: { topic: 'video-jobs' },
    });

    await expect(producer.produce('/data/clip.mp4', info)).rejects.toThrow('broker down');
  });
});
