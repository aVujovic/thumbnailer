import { parseWriteCommand, type WriteCommand } from '@thumbnailer/contracts';
import type { ILogger } from '@thumbnailer/domain';
import type { KafkaConnection, KafkaUtils } from '@thumbnailer/core';
import type { DbFlushConfig } from '../config.schema.js';
import type { IDbSink } from '../sink/DbSink.js';

/**
 * Long-running consumer for the `db-flush` topic. It batches WriteCommands and
 * applies them to the sink in one transaction, then commits — the write-behind
 * worker that replaces every service's inline DB write.
 *
 * Batching uses kafkajs `eachBatch`: kafkajs hands us a fetched batch of messages
 * per partition. We split it into sub-batches of at most `BATCH_MAX_SIZE` and
 * apply each in one DB transaction, resolving its offset only AFTER the flush
 * succeeds. With autoCommit:false the offset advances only after `applyBatch`
 * succeeds — a crash mid-flush redelivers, and because the sink is idempotent
 * (UPSERT by video_path) redelivery is safe.
 *
 * Two micro-batch knobs:
 *  - `BATCH_MAX_SIZE` — max messages per DB transaction (caps transaction size /
 *    statement count, so one huge fetch can't become one giant transaction).
 *  - `BATCH_MAX_WAIT_MS` — the consumer's fetch wait (`maxWaitTimeInMs`, wired in
 *    the kafka provider): a quiet topic flushes after this, a busy one fills the
 *    fetch sooner. Together: the classic size-OR-time window.
 */
export class DbFlushConsumer {
  private readonly logger: ILogger;
  private readonly config: DbFlushConfig;
  private readonly kafka: KafkaConnection;
  private readonly kafkaUtils: KafkaUtils;
  private readonly sink: IDbSink;

  private running = false;

  constructor(deps: {
    logger: ILogger;
    config: DbFlushConfig;
    kafka: KafkaConnection;
    kafkaUtils: KafkaUtils;
    dbSink: IDbSink;
  }) {
    this.logger = deps.logger;
    this.config = deps.config;
    this.kafka = deps.kafka;
    this.kafkaUtils = deps.kafkaUtils;
    this.sink = deps.dbSink;
  }

  async start(): Promise<void> {
    if (this.running) return;
    const consumer = this.kafka.consumer!;

    await consumer.subscribe({
      topic: this.config.flushTopic,
      fromBeginning: this.config.fromBeginning,
    });

    this.logger.info(
      {
        topic: this.config.flushTopic,
        groupId: this.config.kafka.consumer.groupId,
        batchMaxSize: this.config.batchMaxSize,
        batchMaxWaitMs: this.config.batchMaxWaitMs,
      },
      'db-flush-service started — waiting for write commands',
    );

    await consumer.run({
      autoCommit: false,
      // eachBatch hands us a whole fetched batch per partition — exactly the
      // unit we want to apply transactionally and commit once.
      eachBatch: async ({ batch, resolveOffset, commitOffsetsIfNecessary, heartbeat, isRunning, isStale }) => {
        const maxSize = this.config.batchMaxSize;
        // Accumulate the current sub-batch; flush when it reaches maxSize.
        let pending: WriteCommand[] = [];
        let pendingLastOffset: string | undefined;

        // Flush the accumulated sub-batch in one transaction, then resolve its
        // offset (write-ahead: offset advances only AFTER the flush succeeds).
        const flush = async () => {
          if (pending.length > 0) {
            // Throws on failure → offset NOT resolved → the sub-batch redelivers.
            await this.sink.applyBatch(pending);
            this.logger.info(
              { applied: pending.length, partition: batch.partition },
              '💾 flushed batch to DB',
            );
          }
          if (pendingLastOffset !== undefined) {
            resolveOffset(pendingLastOffset);
            await commitOffsetsIfNecessary();
          }
          pending = [];
          pendingLastOffset = undefined;
        };

        for (const message of batch.messages) {
          if (!isRunning() || isStale()) break; // rebalance/stop — drop, will redeliver
          const cmd = parseWriteCommand(
            this.kafkaUtils.parseJson(message, {
              topic: batch.topic,
              partition: batch.partition,
              offset: message.offset,
            }),
          );
          // Poison message: skip it but still advance past it so we don't loop.
          if (cmd) pending.push(cmd);
          pendingLastOffset = message.offset;
          await heartbeat();

          // Size threshold reached → flush this sub-batch (caps transaction size).
          if (pending.length >= maxSize) await flush();
        }

        // Flush the tail (the partial sub-batch the size threshold didn't trigger).
        await flush();
      },
    });

    this.running = true;
  }
}

export default DbFlushConsumer;
