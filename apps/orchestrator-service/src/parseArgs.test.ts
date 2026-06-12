import { describe, it, expect, vi } from 'vitest';
import { parseArgs } from './parseArgs.js';

const id = () => 'generated-id';
// Fake tenant resolver: tenant-1 → a known path, anything else throws.
const resolveTenant = vi.fn((t: string) => {
  if (t === 'tenant-1') return '/resolved/downloads';
  throw new Error(`unknown tenant '${t}'`);
});

describe('parseArgs', () => {
  it('builds StartScan from --tenant, resolving the path and carrying tenantId', () => {
    expect(parseArgs(['start', '--tenant', 'tenant-1', '--id', 'abc'], id, resolveTenant)).toEqual({
      type: 'StartScan',
      scanId: 'abc',
      scanRoot: '/resolved/downloads',
      tenantId: 'tenant-1',
    });
  });

  it('generates a scanId when none is given', () => {
    const cmd = parseArgs(['start', '--tenant', 'tenant-1'], id, resolveTenant);
    expect(cmd).toMatchObject({ type: 'StartScan', scanId: 'generated-id', tenantId: 'tenant-1' });
  });

  it('propagates an unknown-tenant error', () => {
    expect(() => parseArgs(['start', '--tenant', 'nope'], id, resolveTenant)).toThrow(/unknown tenant/);
  });

  it('requires --tenant for start (no raw paths)', () => {
    expect(() => parseArgs(['start'], id, resolveTenant)).toThrow(/--tenant <id>/);
    expect(() => parseArgs(['start', '--root', '/x'], id, resolveTenant)).toThrow(/--tenant <id>/);
  });

  it('builds StopScan with and without an id', () => {
    expect(parseArgs(['stop', '--id', 'abc'], id, resolveTenant)).toEqual({
      type: 'StopScan',
      scanId: 'abc',
    });
    expect(parseArgs(['stop'], id, resolveTenant)).toEqual({ type: 'StopScan' });
  });

  it('carries --tenant on stop (routing key, not resolved to a path)', () => {
    resolveTenant.mockClear();
    expect(parseArgs(['stop', '--tenant', 'tenant-1'], id, resolveTenant)).toEqual({
      type: 'StopScan',
      tenantId: 'tenant-1',
    });
    expect(parseArgs(['stop', '--id', 'abc', '--tenant', 'tenant-1'], id, resolveTenant)).toEqual({
      type: 'StopScan',
      scanId: 'abc',
      tenantId: 'tenant-1',
    });
    // stop must NOT resolve the tenant to a path (unlike start)
    expect(resolveTenant).not.toHaveBeenCalled();
  });

  it('builds Pause/Resume', () => {
    expect(parseArgs(['pause'], id, resolveTenant)).toEqual({ type: 'PauseGenerator' });
    expect(parseArgs(['resume'], id, resolveTenant)).toEqual({ type: 'ResumeGenerator' });
  });

  it('rejects unknown commands', () => {
    expect(() => parseArgs(['frobnicate'], id, resolveTenant)).toThrow(/unknown command/);
  });
});
