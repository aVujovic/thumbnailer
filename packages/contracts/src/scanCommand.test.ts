import { describe, it, expect } from 'vitest';
import {
  parseScanCommand,
  scanCommands,
  serializeScanCommand,
  type ScanCommand,
} from './scanCommand.js';

describe('scanCommand', () => {
  it('round-trips each command type', () => {
    const cmds: ScanCommand[] = [
      { type: 'StartScan', scanId: 'a', scanRoot: '/x' },
      { type: 'StartScan', scanId: 'a', scanRoot: '/x', tenantId: 't1' },
      { type: 'StopScan', scanId: 'a' },
      { type: 'StopScan', scanId: 'a', tenantId: 't1' },
      { type: 'StopScan' },
      { type: 'PauseGenerator' },
      { type: 'ResumeGenerator' },
    ];
    for (const c of cmds) {
      const parsed = parseScanCommand(JSON.parse(serializeScanCommand(c).toString('utf-8')));
      expect(parsed).toEqual(c);
    }
  });

  it('stopScan builder carries tenantId for partition routing', () => {
    expect(scanCommands.stopScan('s-1', 'tenant-1')).toEqual({
      type: 'StopScan',
      scanId: 's-1',
      tenantId: 'tenant-1',
    });
    expect(scanCommands.stopScan(undefined, 'tenant-1')).toEqual({
      type: 'StopScan',
      tenantId: 'tenant-1',
    });
    expect(scanCommands.stopScan()).toEqual({ type: 'StopScan' });
  });

  it('rejects unknown / malformed commands', () => {
    expect(parseScanCommand({ type: 'Nope' })).toBeNull();
    expect(parseScanCommand({ type: 'StartScan' })).toBeNull(); // missing scanId/scanRoot
    expect(parseScanCommand({ type: 'StartScan', scanId: '', scanRoot: '/x' })).toBeNull();
    expect(parseScanCommand(null)).toBeNull();
  });
});
