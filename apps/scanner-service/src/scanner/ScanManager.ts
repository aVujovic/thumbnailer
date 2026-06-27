import type { ILogger } from '@thumbnailer/domain';
import type { FileScanner } from './FileScanner.js';
import type { JobProducer } from '../producer/JobProducer.js';
import type { EventProducer } from '../producer/EventProducer.js';

interface QueuedScan {
  scanId: string;
  scanRoot: string;
  tenantId?: string;
}

/**
 * Runs scans one at a time, driven by commands. A StartScan is appended to a
 * FIFO queue; the manager drains the queue sequentially so two scans never run
 * concurrently (bounded ffmpeg/producer load downstream). A StopScan cancels
 * the running scan and, when no scanId is given, also clears the queue.
 *
 * Pure orchestration — no Kafka wiring here (that's the consumer job), so this
 * is unit-testable with a fake FileScanner + stub producers. The walk itself is
 * cancellable via a per-scan `cancelled` flag checked each iteration (same
 * mechanism as the SIGINT cancellation).
 */
export class ScanManager {
  private readonly logger: ILogger;
  private readonly fileScanner: FileScanner;
  private readonly jobProducer: JobProducer;
  private readonly eventProducer: EventProducer;
  private readonly now: () => number;

  private readonly queue: QueuedScan[] = [];
  private running: QueuedScan | null = null;
  private cancelled = false;
  private draining = false;

  constructor(
    deps: {
      logger: ILogger;
      fileScanner: FileScanner;
      jobProducer: JobProducer;
      eventProducer: EventProducer;
    },
    now: () => number = Date.now,
  ) {
    this.logger = deps.logger;
    this.fileScanner = deps.fileScanner;
    this.jobProducer = deps.jobProducer;
    this.eventProducer = deps.eventProducer;
    this.now = now;
  }

  /** Queue a scan and kick the drain loop if idle. */
  enqueue(scan: QueuedScan): void {
    this.queue.push(scan);
    this.logger.info({ ...scan, queued: this.queue.length }, 'scan queued');
    void this.drain();
  }

  /**
   * Stop a scan. With a scanId: cancel it if running, else drop it from the
   * queue. Without: cancel whatever is running AND clear the whole queue.
   */
  stop(scanId?: string): void {
    if (scanId === undefined) {
      this.queue.length = 0;
      if (this.running) this.cancelled = true;
      this.logger.info('stop: cancelling current scan and clearing queue');
      return;
    }
    if (this.running?.scanId === scanId) {
      this.cancelled = true;
      this.logger.info({ scanId }, 'stop: cancelling running scan');
    } else {
      const before = this.queue.length;
      const remaining = this.queue.filter((s) => s.scanId !== scanId);
      this.queue.length = 0;
      this.queue.push(...remaining);
      if (this.queue.length < before) this.logger.info({ scanId }, 'stop: dropped queued scan');
    }
  }

  /** Drain the queue sequentially. Re-entrant-safe via the `draining` guard. */
  private async drain(): Promise<void> {
    if (this.draining) return;
    this.draining = true;
    try {
      while (this.queue.length > 0) {
        const scan = this.queue.shift()!;
        this.running = scan;
        this.cancelled = false;
        try {
          await this.runScan(scan);
        } catch (err) {
          // runScan is written not to throw, but guard anyway: one scan's
          // failure must never abandon the rest of the queue or leak `running`.
          this.logger.error(
            { scanId: scan.scanId, err: errMsg(err) },
            'unexpected error draining scan — continuing with queue',
          );
        } finally {
          this.running = null;
        }
      }
    } finally {
      this.draining = false;
    }
  }

  private async runScan(scan: QueuedScan): Promise<void> {
    const { scanId, scanRoot, tenantId } = scan;
    const tenant = tenantId ? { tenantId } : {};
    let produced = 0;
    try {
      await this.eventProducer.emit({
        type: 'ScanStarted',
        scanId,
        scanRoot,
        ...tenant,
        startedAt: this.now(),
      });
      this.logger.info({ scanId, scanRoot, tenantId }, 'scan started');

      for await (const { path, info } of this.fileScanner.walk(scanRoot)) {
        if (this.cancelled) break;
        await this.jobProducer.produce(path, info, { scanId, tenantId });
        produced++;
      }
    } catch (err) {
      // A produce/emit failure (e.g. the broker went away mid-walk) must NOT
      // crash the daemon — it would take down the whole scanner and the queue
      // with it. If we were cancelled, fall through to emit ScanCancelled (the
      // error is just the in-flight produce losing the broker as we stop).
      // Otherwise emit a terminal ScanFailed so the orchestrator still sees an
      // end state, then return; the drain loop moves on to the next scan.
      if (!this.cancelled) {
        const reason = errMsg(err);
        this.logger.error({ scanId, produced, err: reason }, 'scan failed');
        await this.eventProducer
          .emit({ type: 'ScanFailed', scanId, ...tenant, produced, reason, failedAt: this.now() })
          .catch((emitErr) =>
            // If even the failure event can't be published (broker still down),
            // there's nothing more to do but log — never rethrow out of runScan.
            this.logger.error(
              { scanId, err: errMsg(emitErr) },
              'failed to emit ScanFailed (broker unreachable?)',
            ),
          );
        return;
      }
    }

    // Terminal event is best-effort: if the broker is unreachable here too, log
    // rather than throw — a throw would propagate to drain() and kill the daemon.
    try {
      if (this.cancelled) {
        await this.eventProducer.emit({
          type: 'ScanCancelled',
          scanId,
          ...tenant,
          produced,
          cancelledAt: this.now(),
        });
        this.logger.info({ scanId, produced }, 'scan cancelled');
      } else {
        await this.eventProducer.emit({
          type: 'ScanCompleted',
          scanId,
          ...tenant,
          produced,
          completedAt: this.now(),
        });
        this.logger.info({ scanId, produced }, 'scan complete');
      }
    } catch (err) {
      this.logger.error(
        { scanId, produced, err: errMsg(err) },
        'failed to emit terminal scan event (broker unreachable?)',
      );
    }
  }

  /** True while a scan is in progress (for shutdown / status). */
  get isRunning(): boolean {
    return this.running !== null;
  }
}

function errMsg(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

export default ScanManager;
