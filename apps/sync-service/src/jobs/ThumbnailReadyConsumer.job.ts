import { parseThumbnailReady, type ThumbnailReady } from '@thumbnailer/contracts';
import type { ILogger } from '@thumbnailer/domain';
import type { KafkaConnection, KafkaUtils } from '@thumbnailer/core';
import type { SyncConfig } from '../config.schema.js';
import type { ISyncTarget } from '../sync/SyncTarget.js';
import type { IVideoDb } from '../repository/VideoDb.js';

/**
 * Long-running consumer for the thumbnail-ready topic. For each ready thumbnail
 * it syncs the file to its destination, then marks the video's row synced in the
 * video DB — using the same write-ahead commit pattern as the generator: the
 * offset is committed only AFTER the sync + DB update succeed, so a crash
 * mid-sync redelivers the event (sync is idempotent — re-marking a row synced is
 * a no-op).
 */
export class ThumbnailReadyConsumer {
  private readonly logger: ILogger;
  private readonly config: SyncConfig;
  private readonly kafka: KafkaConnection;
  private readonly kafkaUtils: KafkaUtils;
  private readonly target: ISyncTarget;
  private readonly db: IVideoDb;
  private readonly now: () => number;

  private readonly pendingOffsets = new Map<string, string>();
  private running = false;

  constructor(
    deps: {
      logger: ILogger;
      config: SyncConfig;
      kafka: KafkaConnection;
      kafkaUtils: KafkaUtils;
      syncTarget: ISyncTarget;
      videoDb: IVideoDb;
    },
    now: () => number = Date.now,
  ) {
    this.logger = deps.logger;
    this.config = deps.config;
    this.kafka = deps.kafka;
    this.kafkaUtils = deps.kafkaUtils;
    this.target = deps.syncTarget;
    this.db = deps.videoDb;
    this.now = now;
  }

  async start(): Promise<void> {
    if (this.running) return;
    const consumer = this.kafka.consumer!;

    await consumer.subscribe({
      topic: this.config.readyTopic,
      fromBeginning: this.config.fromBeginning,
    });

    this.logger.info(
      {
        topic: this.config.readyTopic,
        groupId: this.config.kafka.consumer.groupId,
        videoDb: this.config.videoDbPath,
      },
      'sync-service started — waiting for ready thumbnails',
    );

    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }) => {
        const offset = message.offset;
        this.kafkaUtils.trackOffset(this.pendingOffsets, topic, partition, offset);

        const event = parseThumbnailReady(
          this.kafkaUtils.parseJson(message, { topic, partition, offset }),
        );
        if (!event) {
          // poison message — already warned; commit so we don't re-read it.
          await this.commitSafely(consumer);
          return;
        }

        await this.handle(event);

        // Write-ahead commit: only AFTER the sync + DB update succeed.
        await this.commitSafely(consumer);
      },
    });

    this.running = true;
  }

  /**
   * Commit pending offsets, tolerating a commit failure. A failed commit (e.g. a
   * rebalance revoked the partition, or a transient broker error) must NOT throw
   * out of eachMessage — that would kill the consumer loop. `commitPending` only
   * clears its map AFTER a successful commitOffsets, so on failure the offsets
   * stay pending and the next message's commit retries them. Worst case the
   * thumbnail is re-synced (idempotent: re-marking synced is a no-op), never lost.
   */
  private async commitSafely(consumer: NonNullable<KafkaConnection['consumer']>): Promise<void> {
    try {
      await this.kafkaUtils.commitPending(consumer, this.pendingOffsets);
    } catch (err) {
      this.logger.warn(
        { err: err instanceof Error ? err.message : String(err) },
        'offset commit failed — will retry on next message (work already done)',
      );
    }
  }

  /**
   * Sync one ready thumbnail and flip its row to synced in the video DB. A
   * missing row (event arrived before the generator's write, or for a video not
   * in this DB) is logged but still treated as terminal — the file was synced;
   * there's just no row to update, and redelivering wouldn't create one.
   */
  async handle(event: ThumbnailReady): Promise<void> {
    await this.target.sync(event);
    const updated = await this.db.markSynced(event.videoPath, this.now());
    if (!updated) {
      this.logger.warn(
        { videoPath: event.videoPath },
        'synced, but no video-DB row to mark (event arrived before its row?)',
      );
    }
  }
}

export default ThumbnailReadyConsumer;
