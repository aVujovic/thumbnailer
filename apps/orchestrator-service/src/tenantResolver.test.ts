import { describe, it, expect, afterEach } from 'vitest';
import { writeFileSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir, homedir } from 'node:os';
import { join } from 'node:path';
import { TenantResolver } from './tenantResolver.js';

const dirs: string[] = [];
function tmpFile(content: string): string {
  const dir = mkdtempSync(join(tmpdir(), 'tenants-'));
  dirs.push(dir);
  const path = join(dir, 'tenants.json');
  writeFileSync(path, content);
  return path;
}
afterEach(() => {
  for (const d of dirs.splice(0)) rmSync(d, { recursive: true, force: true });
});

describe('TenantResolver', () => {
  it('resolves a known tenant to an absolute path, expanding ~', () => {
    const r = new TenantResolver({
      'tenant-1': { scanRoot: '~/Downloads' },
      'tenant-2': { scanRoot: '/data/desktop' },
    });
    expect(r.resolveScanRoot('tenant-1')).toBe(`${homedir()}/Downloads`);
    expect(r.resolveScanRoot('tenant-2')).toBe('/data/desktop');
  });

  it('throws on an unknown tenant, listing the known ones', () => {
    const r = new TenantResolver({ 'tenant-1': { scanRoot: '/x' } });
    expect(() => r.resolveScanRoot('nope')).toThrow(/unknown tenant 'nope'.*tenant-1/);
  });

  it('loads + validates a tenants.json file', () => {
    const path = tmpFile('{"a":{"scanRoot":"/a"},"b":{"scanRoot":"/b"}}');
    const r = TenantResolver.fromFile(path);
    expect(r.tenantIds().sort()).toEqual(['a', 'b']);
    expect(r.resolveScanRoot('a')).toBe('/a');
  });

  it('rejects a malformed tenants.json', () => {
    const path = tmpFile('{"a":{"wrongKey":"/a"}}');
    expect(() => TenantResolver.fromFile(path)).toThrow(/invalid tenants file/);
  });

  it('errors clearly when the file is missing', () => {
    expect(() => TenantResolver.fromFile('/no/such/tenants.json')).toThrow(/failed to read/);
  });
});
