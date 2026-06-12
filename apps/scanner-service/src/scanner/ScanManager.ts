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
        await this.runScan(scan);
        this.running = null;
      }
    } finally {
      this.draining = false;
    }
  }

  private async runScan(scan: QueuedScan): Promise<void> {
    const { scanId, scanRoot, tenantId } = scan;
    const tenant = tenantId ? { tenantId } : {};
    await this.eventProducer.emit({
      type: 'ScanStarted',
      scanId,
      scanRoot,
      ...tenant,
      startedAt: this.now(),
    });
    this.logger.info({ scanId, scanRoot, tenantId }, 'scan started');

    let produced = 0;
    for await (const { path, info } of this.fileScanner.walk(scanRoot)) {
      if (this.cancelled) break;
      try {
        await this.jobProducer.produce(path, info, { scanId, tenantId });
        produced++;
      } catch (err) {
        if (this.cancelled) break;
        throw err;
      }
    }

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
  }

  /** True while a scan is in progress (for shutdown / status). */
  get isRunning(): boolean {
    return this.running !== null;
  }
}

export default ScanManager;
