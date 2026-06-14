import { describe, it, expect, vi, afterEach } from 'vitest';
import { FakeLogger } from '@thumbnailer/domain/fakes';
import { Signals } from './signals.provider.js';

/**
 * terminate() ends with setTimeout(() => process.exit(...), graceMs). We stub
 * process.exit so the test process survives, and use graceMs:0 + fake timers to
 * flush that final tick deterministically.
 */
function makeSignals() {
  const logger = new FakeLogger();
  const signals = new Signals({ logger, config: { shutdownGraceMs: 0 } });
  return { signals, logger };
}

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
});

describe('Signals teardown', () => {
  it('runs handlers SEQUENTIALLY in registration order (not concurrently)', async () => {
    const exit = vi.spyOn(process, 'exit').mockImplementation(() => undefined as never);
    const { signals } = makeSignals();
    const order: string[] = [];

    // Each handler records its start, awaits a tick, then records its end.
    // If they ran concurrently the starts would interleave (A-start, B-start, …);
    // sequential execution yields strict A-start,A-end,B-start,B-end,…
    const mk = (name: string) => async () => {
      order.push(`${name}-start`);
      await new Promise((r) => setTimeout(r, 0));
      order.push(`${name}-end`);
    };
    signals.onTerm(mk('A'));
    signals.onTerm(mk('B'));
    signals.onTerm(mk('C'));

    vi.useFakeTimers();
    const done = signals.terminate(0);
    await vi.runAllTimersAsync();
    await done;

    expect(order).toEqual([
      'A-start', 'A-end',
      'B-start', 'B-end',
      'C-start', 'C-end',
    ]);
    expect(exit).toHaveBeenCalled();
  });

  it('a failing handler is logged but does not block the remaining handlers', async () => {
    vi.spyOn(process, 'exit').mockImplementation(() => undefined as never);
    const { signals, logger } = makeSignals();
    const ran: string[] = [];

    signals.onTerm(() => void ran.push('first'));
    signals.onTerm(() => {
      throw new Error('teardown boom');
    });
    signals.onTerm(() => void ran.push('third'));

    vi.useFakeTimers();
    const done = signals.terminate(0);
    await vi.runAllTimersAsync();
    await done;

    expect(ran).toEqual(['first', 'third']); // third still ran despite the throw
    expect(logger.lines.some((l) => l.message?.includes('Teardown handler failed'))).toBe(true);
  });

  it('is idempotent — a second terminate() is ignored', async () => {
    const exit = vi.spyOn(process, 'exit').mockImplementation(() => undefined as never);
    const { signals } = makeSignals();
    const handler = vi.fn();
    signals.onTerm(handler);

    vi.useFakeTimers();
    const first = signals.terminate(0);
    await vi.runAllTimersAsync();
    await first;
    await signals.terminate(0); // ignored

    expect(handler).toHaveBeenCalledOnce();
    expect(exit).toHaveBeenCalledOnce();
  });
});
