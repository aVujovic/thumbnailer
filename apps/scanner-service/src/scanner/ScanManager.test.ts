import { describe, it, expect, vi } from 'vitest';
import { FakeLogger } from '@thumbnailer/domain/fakes';
import type { FileInfo } from '@thumbnailer/domain';
import type { ScanEvent } from '@thumbnailer/contracts';
import { ScanManager } from './ScanManager.js';
import type { FileScanner } from './FileScanner.js';
import type { JobProducer } from '../producer/JobProducer.js';
import type { EventProducer } from '../producer/EventProducer.js';

const info: FileInfo = { name: 'v.mp4', isDirectory: false, size: 1, permissions: 0o644 };

/** FileScanner stand-in. Yields `count` videos per root; optionally pauses on
 *  each yield so a test can cancel mid-walk. */
function fakeScanner(count: number, onYield?: () => void): FileScanner {
  return {
    async *walk(scanRoot: string) {
      for (let i = 0; i < count; i++) {
        onYield?.();
        yield { path: `${scanRoot}/v${i}.mp4`, info };
      }
    },
  } as unknown as FileScanner;
}

function harness(scanner: FileScanner) {
  const logger = new FakeLogger();
  const produced: Array<{ path: string; scanId?: string; tenantId?: string }> = [];
  const jobProducer = {
    produce: vi.fn(async (path: string, _info: FileInfo, ctx?: { scanId?: string; tenantId?: string }) =>
      void produced.push({ path, scanId: ctx?.scanId, tenantId: ctx?.tenantId }),
    ),
  } as unknown as JobProducer;
  const events: ScanEvent[] = [];
  const eventProducer = {
    emit: vi.fn(async (e: ScanEvent) => void events.push(e)),
  } as unknown as EventProducer;

  let clock = 0;
  const mgr = new ScanManager({ logger, fileScanner: scanner, jobProducer, eventProducer }, () => ++clock);
  return { mgr, produced, events, logger };
}

const flush = () => new Promise((r) => setTimeout(r, 0));

describe('ScanManager', () => {
  it('runs a scan: emits Started+Completed and produces jobs with scanId', async () => {
    const { mgr, produced, events } = harness(fakeScanner(3));

    mgr.enqueue({ scanId: 'scan-A', scanRoot: '/root' });
    await flush();

    expect(produced.map((p) => p.path)).toEqual(['/root/v0.mp4', '/root/v1.mp4', '/root/v2.mp4']);
    expect(produced.every((p) => p.scanId === 'scan-A')).toBe(true);
    expect(events.map((e) => e.type)).toEqual(['ScanStarted', 'ScanCompleted']);
    expect(events[1]).toMatchObject({ type: 'ScanCompleted', scanId: 'scan-A', produced: 3 });
  });

  it('propagates tenantId onto produced jobs and emitted events', async () => {
    const { mgr, produced, events } = harness(fakeScanner(2));

    mgr.enqueue({ scanId: 'scan-T', scanRoot: '/root', tenantId: 'tenant-1' });
    await flush();

    expect(produced.every((p) => p.tenantId === 'tenant-1')).toBe(true);
    expect(events[0]).toMatchObject({ type: 'ScanStarted', tenantId: 'tenant-1' });
    expect(events[1]).toMatchObject({ type: 'ScanCompleted', tenantId: 'tenant-1' });
  });

  it('runs queued scans one at a time, in FIFO order', async () => {
    const { mgr, events } = harness(fakeScanner(1));

    mgr.enqueue({ scanId: 'A', scanRoot: '/a' });
    mgr.enqueue({ scanId: 'B', scanRoot: '/b' });
    await flush();

    // Started/Completed pairs, A fully before B.
    expect(events.map((e) => `${e.type}:${e.scanId}`)).toEqual([
      'ScanStarted:A',
      'ScanCompleted:A',
      'ScanStarted:B',
      'ScanCompleted:B',
    ]);
  });

  it('stop(scanId) cancels the running scan and emits ScanCancelled', async () => {
    let mgrRef: ScanManager;
    // cancel the running scan after its first produced video
    const scanner = fakeScanner(10, () => {
      // stop on the 2nd yield onward
      mgrRef?.stop('A');
    });
    const h = harness(scanner);
    mgrRef = h.mgr;

    h.mgr.enqueue({ scanId: 'A', scanRoot: '/a' });
    await flush();

    const last = h.events.at(-1)!;
    expect(last.type).toBe('ScanCancelled');
    expect(last).toMatchObject({ scanId: 'A' });
    expect(h.produced.length).toBeLessThan(10); // stopped early
  });

  it('stop() with no id clears the queue and cancels the current scan', async () => {
    let mgrRef: ScanManager;
    const scanner = fakeScanner(10, () => mgrRef?.stop());
    const h = harness(scanner);
    mgrRef = h.mgr;

    h.mgr.enqueue({ scanId: 'A', scanRoot: '/a' });
    h.mgr.enqueue({ scanId: 'B', scanRoot: '/b' });
    await flush();

    // B was dropped from the queue; only A ran (and got cancelled)
    const scanIds = new Set(h.events.map((e) => e.scanId));
    expect(scanIds.has('B')).toBe(false);
    expect(h.events.some((e) => e.type === 'ScanCancelled' && e.scanId === 'A')).toBe(true);
  });

  it('stop(scanId) drops a not-yet-running queued scan', async () => {
    // slow first scan so B is still queued when we drop it
    let resolveFirst!: () => void;
    const gate = new Promise<void>((r) => (resolveFirst = r));
    const scanner = {
      async *walk(scanRoot: string) {
        if (scanRoot === '/a') await gate; // hold A until released
        yield { path: `${scanRoot}/v.mp4`, info };
      },
    } as unknown as FileScanner;
    const h = harness(scanner);

    h.mgr.enqueue({ scanId: 'A', scanRoot: '/a' });
    h.mgr.enqueue({ scanId: 'B', scanRoot: '/b' });
    h.mgr.stop('B'); // drop B while A is still gated
    resolveFirst();
    await flush();
    await flush();

    const scanIds = h.events.map((e) => e.scanId);
    expect(scanIds).toContain('A');
    expect(scanIds).not.toContain('B'); // never started
  });
});
