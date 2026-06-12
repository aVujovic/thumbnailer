import { describe, it, expect, vi } from 'vitest';
import type { Consumer, KafkaMessage } from 'kafkajs';
import { FakeLogger } from '@thumbnailer/domain/fakes';
import { KafkaUtils } from './kafkaUtils.provider.js';

function msg(value: string | null): KafkaMessage {
  return { value: value === null ? null : Buffer.from(value, 'utf-8') } as KafkaMessage;
}

function utils() {
  const logger = new FakeLogger();
  return { ku: new KafkaUtils({ logger }), logger };
}

describe('KafkaUtils.parseJson', () => {
  it('parses a valid JSON body', () => {
    const { ku } = utils();
    expect(ku.parseJson(msg('{"a":1}'))).toEqual({ a: 1 });
  });

  it('returns null for an empty body without logging', () => {
    const { ku, logger } = utils();
    expect(ku.parseJson(msg(null))).toBeNull();
    expect(ku.parseJson(msg(''))).toBeNull();
    expect(logger.countAt('warn')).toBe(0);
  });

  it('returns null and warns once on a poison (non-JSON) body', () => {
    const { ku, logger } = utils();
    expect(ku.parseJson(msg('not json'), { topic: 't', partition: 0 })).toBeNull();
    expect(logger.countAt('warn')).toBe(1);
    expect(logger.lines[0]?.message).toContain('non-JSON');
  });
});

describe('KafkaUtils.trackOffset', () => {
  it('keeps the latest offset per (topic, partition)', () => {
    const { ku } = utils();
    const pending = new Map<string, string>();
    ku.trackOffset(pending, 'video-jobs', 0, '5');
    ku.trackOffset(pending, 'video-jobs', 0, '6'); // overwrites
    ku.trackOffset(pending, 'video-jobs', 1, '2');
    expect(pending.get('video-jobs:0')).toBe('6');
    expect(pending.get('video-jobs:1')).toBe('2');
  });
});

describe('KafkaUtils.commitPending', () => {
  it('commits offset+1 per partition and clears the map', async () => {
    const { ku } = utils();
    const commitOffsets = vi.fn().mockResolvedValue(undefined);
    const consumer = { commitOffsets } as unknown as Consumer;

    const pending = new Map([
      ['video-jobs:0', '6'],
      ['video-jobs:1', '2'],
    ]);

    const committed = await ku.commitPending(consumer, pending);

    expect(commitOffsets).toHaveBeenCalledOnce();
    expect(committed).toEqual([
      { topic: 'video-jobs', partition: 0, offset: '7' },
      { topic: 'video-jobs', partition: 1, offset: '3' },
    ]);
    expect(pending.size).toBe(0); // cleared
  });

  it('is a no-op on an empty map', async () => {
    const { ku } = utils();
    const commitOffsets = vi.fn();
    const consumer = { commitOffsets } as unknown as Consumer;

    expect(await ku.commitPending(consumer, new Map())).toBeNull();
    expect(commitOffsets).not.toHaveBeenCalled();
  });
});
