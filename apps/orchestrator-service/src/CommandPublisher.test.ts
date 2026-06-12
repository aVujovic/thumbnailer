import { describe, it, expect, vi } from 'vitest';
import { FakeLogger } from '@thumbnailer/domain/fakes';
import { scanCommands, parseScanCommand } from '@thumbnailer/contracts';
import type { KafkaConnection } from '@thumbnailer/core';
import { CommandPublisher } from './CommandPublisher.js';

/** Build a publisher over a fake producer; return it plus the send spy. */
function harness() {
  const send = vi.fn().mockResolvedValue(undefined);
  const kafka = { producer: { send } } as unknown as KafkaConnection;
  const publisher = new CommandPublisher({
    kafka,
    logger: new FakeLogger(),
    config: { commandsTopic: 'scan-commands' },
  });
  return { publisher, send };
}

/** Extract the key + parsed value of the single message sent. */
function sent(send: ReturnType<typeof vi.fn>) {
  const arg = send.mock.calls[0]![0] as {
    topic: string;
    messages: { key: string; value: Buffer }[];
  };
  const msg = arg.messages[0]!;
  return { topic: arg.topic, key: msg.key, command: parseScanCommand(JSON.parse(msg.value.toString())) };
}

describe('CommandPublisher partition keying', () => {
  it('keys StartScan by tenantId (routes a tenant to its owning scanner)', async () => {
    const { publisher, send } = harness();
    await publisher.publish(scanCommands.startScan('s-1', '/data', 'tenant-1'));
    const { topic, key, command } = sent(send);
    expect(topic).toBe('scan-commands');
    expect(key).toBe('tenant-1');
    expect(command).toMatchObject({ type: 'StartScan', tenantId: 'tenant-1' });
  });

  it('keys StopScan by tenantId so it lands on the same partition as its start', async () => {
    const { publisher, send } = harness();
    await publisher.publish(scanCommands.stopScan('s-1', 'tenant-1'));
    expect(sent(send).key).toBe('tenant-1');
  });

  it('falls back to scanId when there is no tenant', async () => {
    const { publisher, send } = harness();
    await publisher.publish(scanCommands.stopScan('s-1'));
    expect(sent(send).key).toBe('s-1');
  });

  it('falls back to the command type for generator commands (no tenant, no scanId)', async () => {
    const { publisher, send } = harness();
    await publisher.publish(scanCommands.pauseGenerator());
    expect(sent(send).key).toBe('PauseGenerator');
  });
});
