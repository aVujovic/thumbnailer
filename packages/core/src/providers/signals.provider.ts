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

    const results = await Promise.allSettled(this.handlers.map((h) => h()));
    let failures = 0;
    for (const r of results) {
      if (r.status === 'rejected') {
        failures++;
        this.logger.warn({ err: String(r.reason) }, 'Teardown handler failed');
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
