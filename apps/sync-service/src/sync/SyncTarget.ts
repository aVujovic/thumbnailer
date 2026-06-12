import type { ILogger } from '@thumbnailer/domain';
import type { ThumbnailReady } from '@thumbnailer/contracts';

/**
 * Where a generated thumbnail gets replicated to. Abstracted so the consumer
 * doesn't care HOW the sync happens — in production this is rsync / scp / an S3
 * PutObject to a server that serves the previews; here it just logs. Swapping
 * the implementation never touches the consumer.
 */
export interface ISyncTarget {
  /** Replicate one ready thumbnail to the destination. */
  sync(event: ThumbnailReady): Promise<void>;
}

interface SyncConfig {
  syncDelayMs: number;
}

/**
 * Log-only sync target: the assessment doesn't ship a real destination server,
 * so this stands in for one — it logs "video X synced" and optionally sleeps to
 * simulate transfer latency, making the daemon's work visible. The shape is
 * exactly what a real target would expose, so production swaps the body, not the
 * interface.
 */
export class LoggingSyncTarget implements ISyncTarget {
  private readonly logger: ILogger;
  private readonly delayMs: number;

  constructor(deps: { logger: ILogger; config: SyncConfig }) {
    this.logger = deps.logger;
    this.delayMs = deps.config.syncDelayMs;
  }

  async sync(event: ThumbnailReady): Promise<void> {
    if (this.delayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, this.delayMs));
    }
    this.logger.info(
      {
        videoPath: event.videoPath,
        thumbnail: event.outputPath,
        ...(event.tenantId ? { tenantId: event.tenantId } : {}),
      },
      `🔄 video synced — ${event.outputPath}`,
    );
  }
}

export default LoggingSyncTarget;
