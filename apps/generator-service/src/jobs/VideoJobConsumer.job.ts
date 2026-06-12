import {
  parseScanCommand,
  parseVideoJob,
  SCAN_COMMAND,
  serializeThumbnailReady,
  type ThumbnailReady,
  type ThumbnailResult,
  type VideoJob,
} from '@thumbnailer/contracts';
import type { ILogger } from '@thumbnailer/domain';
import type { KafkaConnection, KafkaUtils } from '@thumbnailer/core';
import type { GeneratorConfig } from '../config.schema.js';
import type { ThumbnailGenerator } from '../thumbnail/ThumbnailGenerator.js';
import type { IResultRepository } from '../repository/ResultRepository.js';

/**
 * Long-running consumer. Subscribes to the video-jobs topic and processes one
 * job at a time with the write-ahead commit pattern: the offset is committed
 * only AFTER the work reaches a terminal state, so a crash mid-work redelivers
 * the message.
 *
 * Collaborators arrive via constructor injection, so the job is testable with a
 * fake consumer + fake generator, no real Kafka.
 */
export class VideoJobConsumer {
  private readonly logger: ILogger;
  private readonly config: GeneratorConfig;
  private readonly kafka: KafkaConnection;
  private readonly kafkaUtils: KafkaUtils;
  private readonly generator: ThumbnailGenerator;
  private readonly results: IResultRepository;
  private readonly now: () => number;

  private readonly pendingOffsets = new Map<string, string>();
  private running = false;

  constructor(
    deps: {
      logger: ILogger;
      config: GeneratorConfig;
      kafka: KafkaConnection;
      kafkaUtils: KafkaUtils;
      thumbnailGenerator: ThumbnailGenerator;
      resultRepository: IResultRepository;
    },
    now: () => number = Date.now,
  ) {
    this.logger = deps.logger;
    this.config = deps.config;
    this.kafka = deps.kafka;
    this.kafkaUtils = deps.kafkaUtils;
    this.generator = deps.thumbnailGenerator;
    this.results = deps.resultRepository;
    this.now = now;
  }

  async start(): Promise<void> {
    if (this.running) return;
    const consumer = this.kafka.consumer!;

    // Two topics on one consumer: video-jobs (work) and scan-commands (control,
    // for Pause/Resume). Only the video-jobs topic is ever paused.
    await consumer.subscribe({
      topic: this.config.topic,
      fromBeginning: this.config.fromBeginning,
    });
    await consumer.subscribe({ topic: this.config.commandsTopic, fromBeginning: false });

    this.logger.info(
      {
        topic: this.config.topic,
        commandsTopic: this.config.commandsTopic,
        groupId: this.config.kafka.consumer.groupId,
        outputRoot: this.config.outputRoot,
      },
      'generator-service started — waiting for jobs',
    );

    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }) => {
        // Control commands (Pause/Resume) arrive on the commands topic.
        if (topic === this.config.commandsTopic) {
          this.handleCommand(message);
          return; // commands auto-commit (control signals, not work)
        }

        const offset = message.offset;
        this.kafkaUtils.trackOffset(this.pendingOffsets, topic, partition, offset);

        const job = parseVideoJob(
          this.kafkaUtils.parseJson(message, { topic, partition, offset }),
        );
        if (!job) {
          // poison message — already warned; commit so we don't re-read it.
          await this.kafkaUtils.commitPending(consumer, this.pendingOffsets);
          return;
        }

        await this.process(job, { partition, offset });

        // Write-ahead commit: only AFTER process() reaches a terminal state
        // (generated / skipped / failed-after-retries). A crash before this
        // line never commits, so the message is redelivered.
        await this.kafkaUtils.commitPending(consumer, this.pendingOffsets);
      },
    });

    this.running = true;
  }

  /**
   * Process a single job: generate its thumbnail, record the outcome to the
   * video DB (synced:false on insert), and — when a thumbnail was actually
   * produced — announce a ThumbnailReady event so the sync-service can pick it
   * up. All three outcomes (generated / skipped / failed) are terminal and lead
   * to a commit by the caller — retries already happened inside the generator,
   * so redelivering a permanently-broken video would just loop.
   */
  async process(job: VideoJob, ctx: { partition: number; offset: string }): Promise<void> {
    const result = await this.generator.generate(job);

    const record: ThumbnailResult = {
      ...(job.scanId ? { scanId: job.scanId } : {}),
      ...(job.tenantId ? { tenantId: job.tenantId } : {}),
      videoPath: job.path,
      outputPath: result.outputPath,
      size: job.size,
      discoveredAt: job.discoveredAt,
      format: this.config.thumbnailFormat,
      status: result.status,
      attempts: result.status === 'skipped' ? 1 : result.attempts,
      ...(result.status === 'failed' ? { error: result.reason } : {}),
      processedAt: this.now(),
      synced: false,
    };
    await this.results.save(record);

    // A thumbnail now exists on disk (freshly generated or already there) →
    // announce it for downstream sync. 'failed' produced nothing, so it isn't
    // ready; emitting it would have the sync-service chase a missing file.
    if (result.status === 'generated' || result.status === 'skipped') {
      await this.emitReady(record);
    }

    if (result.status === 'failed') {
      this.logger.error(
        { path: job.path, ...ctx, reason: result.reason },
        'job failed after retries — committing to avoid a redelivery loop',
      );
    }
  }

  /**
   * Publish a ThumbnailReady event. Keyed by videoPath so a video's ready-events
   * keep partition order and shard the same way jobs do — the sync-service
   * scales identically to the generator. Best-effort: a failed announce is
   * logged but never blocks the commit (the row is already in the DB; the
   * thumbnail can be reconciled later).
   */
  private async emitReady(record: ThumbnailResult): Promise<void> {
    const event: ThumbnailReady = {
      videoPath: record.videoPath,
      outputPath: record.outputPath,
      format: record.format,
      ...(record.tenantId ? { tenantId: record.tenantId } : {}),
      ...(record.scanId ? { scanId: record.scanId } : {}),
      generatedAt: record.processedAt,
    };
    try {
      await this.kafka.producer!.send({
        topic: this.config.readyTopic,
        messages: [{ key: event.videoPath, value: serializeThumbnailReady(event) }],
      });
    } catch (err) {
      this.logger.warn(
        { videoPath: record.videoPath, err: err instanceof Error ? err.message : String(err) },
        'failed to announce thumbnail-ready (will not block commit)',
      );
    }
  }

  /**
   * Handle a control command on the commands topic. PauseGenerator pauses
   * fetching from the video-jobs topic (the process stays alive and keeps
   * consuming commands); ResumeGenerator unpauses it. Other command types
   * (StartScan/StopScan) are for the scanner — ignored here.
   */
  private handleCommand(message: { value: Buffer | null }): void {
    const command = parseScanCommand(this.kafkaUtils.parseJson(message as never));
    if (!command) return;

    const consumer = this.kafka.consumer!;
    if (command.type === SCAN_COMMAND.PauseGenerator) {
      consumer.pause([{ topic: this.config.topic }]);
      this.logger.info('⏸️  generator paused (video-jobs)');
    } else if (command.type === SCAN_COMMAND.ResumeGenerator) {
      consumer.resume([{ topic: this.config.topic }]);
      this.logger.info('▶️  generator resumed (video-jobs)');
    }
  }
}

export default VideoJobConsumer;
