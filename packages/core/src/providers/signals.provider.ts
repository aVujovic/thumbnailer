import type { ILogger } from '@thumbnailer/domain';

type TermHandler = () => Promise<void> | void;

/**
 * Centralised process-signal handling.
 *
 * Any provider or service registers a teardown callback via `onTerm()`.
 * On SIGTERM / SIGINT every handler runs in registration order, then the
 * process exits. One registry, one ordered teardown, one exit — instead of
 * ad-hoc `process.on('SIGTERM', ...)` scattered across the codebase.
 */
export class Signals {
  private terminating = false;
  private handlers: TermHandler[] = [];
  private logger: ILogger;

  /** Ms to let logs flush before process.exit after teardown. */
  private readonly graceMs: number;

  constructor({ logger, config }: { logger: ILogger; config?: { shutdownGraceMs?: number } }) {
    this.logger = logger;
    this.graceMs = config?.shutdownGraceMs ?? 300;

    const shutdown = (signal: NodeJS.Signals) => () => {
      void this.terminate(0, signal);
    };
    process.once('SIGTERM', shutdown('SIGTERM'));
    process.once('SIGINT', shutdown('SIGINT'));
  }

  /**
   * Register a teardown callback. Called once on termination, in registration
   * order, before the process exits. A throwing/rejecting handler is logged
   * but does not block the remaining handlers.
   */
  onTerm(handler: TermHandler): void {
    if (typeof handler !== 'function') {
      throw new Error('Signals.onTerm: handler must be a function');
    }
    if (this.handlers.includes(handler)) return;
    this.handlers.push(handler);
  }

  /**
   * Run every registered handler, then exit. Idempotent — repeated signals are
   * ignored while a termination is already in progress.
   */
  async terminate(code = 0, signal?: NodeJS.Signals): Promise<void> {
    if (this.terminating) return;
    this.terminating = true;

    this.logger.info(
      `👋 Shutting down${signal ? ` (${signal})` : ''} — running ${this.handlers.length} teardown handler(s)`,
    );

    // Run handlers SEQUENTIALLY in registration order. Teardown has ordering
    // dependencies (e.g. stop the consumer / drain in-flight work BEFORE
    // disconnecting the producer it writes to), so a parallel allSettled would
    // race them — a producer could be disconnected while an in-flight message is
    // still trying to produce. A failing handler is logged but never blocks the
    // rest, so one bad teardown can't strand the others.
    let failures = 0;
    for (const handler of this.handlers) {
      try {
        await handler();
      } catch (err) {
        failures++;
        this.logger.warn({ err: String(err) }, 'Teardown handler failed');
      }
    }

    this.logger.info('✅ Shutdown complete');

    // Give logs a tick to flush, then exit unconditionally — an explicit
    // shutdown must terminate even if a setInterval / undrained client is
    // still keeping the event loop alive.
    setTimeout(() => process.exit(code || (failures ? 1 : 0)), this.graceMs);
  }
}

export default Signals;
