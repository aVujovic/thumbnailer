import { serializeVideoJob, type VideoJob } from '@thumbnailer/contracts';
import type { FileInfo, ILogger } from '@thumbnailer/domain';
import type { KafkaConnection } from '@thumbnailer/core';

interface ProducerConfig {
  topic: string;
}

/**
 * Maps a discovered video to a VideoJob and produces it onto the topic.
 *
 * The file path is the message key, so Kafka shards by `murmur2(path) % partitions`.
 * That gives, for free: an even spread across partitions/generators (paths are
 * diverse), idempotency (the same video always hashes to the same partition →
 * the same generator, which can skip an already-generated thumbnail), and
 * per-path ordering. (Keying by e.g. tenantId would funnel a whole tenant to one
 * partition — the path is the right shard key.)
 *
 * Thin and injectable: the caller doesn't know about Kafka, and this class
 * doesn't know about walking — single responsibility.
 */
export class JobProducer {
  private readonly kafka: KafkaConnection;
  private readonly logger: ILogger;
  private readonly config: ProducerConfig;
  private readonly now: () => number;

  /**
   * @param deps  Injected by awilix (kafka, logger, config).
   * @param now   Injectable clock for deterministic tests; not from the cradle,
   *              so awilix never tries to resolve it. Defaults to Date.now.
   */
  constructor(
    deps: { kafka: KafkaConnection; logger: ILogger; config: ProducerConfig },
    now: () => number = Date.now,
  ) {
    this.kafka = deps.kafka;
    this.logger = deps.logger;
    this.config = deps.config;
    this.now = now;
  }

  async produce(
    path: string,
    info: FileInfo,
    ctx: { scanId?: string; tenantId?: string } = {},
  ): Promise<void> {
    const job: VideoJob = {
      path,
      size: info.size,
      discoveredAt: this.now(),
      ...(ctx.scanId ? { scanId: ctx.scanId } : {}),
      ...(ctx.tenantId ? { tenantId: ctx.tenantId } : {}),
    };
    await this.kafka.producer!.send({
      topic: this.config.topic,
      messages: [{ key: job.path, value: serializeVideoJob(job) }],
    });
    this.logger.debug({ job }, 'produced video job');
  }
}

export default JobProducer;
