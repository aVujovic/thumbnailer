import type { ILogger } from '@thumbnailer/domain';
import type { Consumer, KafkaMessage } from 'kafkajs';

export type CommittablePosition = { topic: string; partition: number; offset: string };

/**
 * Shared toolbox for Kafka consumers. 
 *
 *   1. Decode message.value (utf-8) + JSON.parse with a warn-and-skip fallback
 *      so a single poison message never crashes the partition.
 *   2. Track per-(topic, partition) pending offsets.
 *   3. Commit `offset + 1` AFTER the work succeeds (autoCommit: false,
 *      write-ahead pattern) so a crash mid-work redelivers the message.
 */
export class KafkaUtils {
  private readonly logger: ILogger;

  constructor(deps: { logger: ILogger }) {
    this.logger = deps.logger;
  }

  /**
   * Decode + parse a Kafka message body. Returns null on empty or non-JSON
   * payloads, after a single `warn` with a short sample of the offender.
   */
  parseJson<T>(message: KafkaMessage, ctx?: Record<string, unknown>): T | null {
    const raw = message.value?.toString('utf-8') ?? '';
    if (!raw) return null;
    try {
      return JSON.parse(raw) as T;
    } catch (err) {
      this.logger.warn(
        { ...ctx, err: (err as Error).message, sample: raw.slice(0, 120) },
        'Skipping non-JSON message',
      );
      return null;
    }
  }

  /** Record the latest seen offset for (topic, partition). */
  trackOffset(
    pending: Map<string, string>,
    topic: string,
    partition: number,
    offset: string,
  ): void {
    pending.set(`${topic}:${partition}`, offset);
  }

  /**
   * Commit `offset + 1` per (topic, partition) so the next consumer cycle
   * resumes past the last processed message. Clears `pending` in place.
   * No-op (returns null) when the map is empty.
   */
  async commitPending(
    consumer: Consumer,
    pending: Map<string, string>,
    label = 'offsets',
  ): Promise<CommittablePosition[] | null> {
    if (pending.size === 0) return null;
    const toCommit: CommittablePosition[] = [];
    for (const [tp, off] of pending.entries()) {
      const [topic, p] = tp.split(':');
      toCommit.push({
        topic: topic!,
        partition: Number(p),
        offset: (BigInt(off) + 1n).toString(),
      });
    }
    await consumer.commitOffsets(toCommit);
    pending.clear();
    this.logger.info({ committed: toCommit, label }, '✅ Kafka offsets committed');
    return toCommit;
  }
}

export default KafkaUtils;
