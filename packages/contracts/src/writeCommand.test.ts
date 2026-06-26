import { describe, it, expect } from 'vitest';
import {
  parseWriteCommand,
  serializeWriteCommand,
  writeCommands,
  type WriteCommand,
} from './writeCommand.js';
import type { ThumbnailResult } from './thumbnailResult.js';

const row: ThumbnailResult = {
  videoPath: '/v.mp4',
  outputPath: '/out/v.mp4.jpg',
  size: 10,
  discoveredAt: 1,
  format: 'jpg',
  status: 'generated',
  attempts: 1,
  processedAt: 2,
  synced: false,
};

describe('writeCommand', () => {
  it('builds an upsert keyed by videoPath', () => {
    const cmd = writeCommands.upsert(row);
    expect(cmd).toMatchObject({ op: 'upsert', key: '/v.mp4' });
    expect((cmd as Extract<WriteCommand, { op: 'upsert' }>).row.videoPath).toBe('/v.mp4');
  });

  it('builds a mark-synced keyed by videoPath', () => {
    expect(writeCommands.markSynced('/v.mp4', 999)).toEqual({
      op: 'mark-synced',
      key: '/v.mp4',
      syncedAt: 999,
    });
  });

  it('round-trips both ops through serialize/parse', () => {
    for (const cmd of [writeCommands.upsert(row), writeCommands.markSynced('/v.mp4', 5)]) {
      const parsed = parseWriteCommand(JSON.parse(serializeWriteCommand(cmd).toString('utf-8')));
      expect(parsed).toEqual(cmd);
    }
  });

  it('rejects malformed / unknown commands (poison-safe)', () => {
    expect(parseWriteCommand({ op: 'delete', key: 'x' })).toBeNull();
    expect(parseWriteCommand({ op: 'upsert' })).toBeNull(); // missing key/row
    expect(parseWriteCommand({ op: 'mark-synced', key: '' })).toBeNull();
    expect(parseWriteCommand(null)).toBeNull();
  });
});
