import type { ILogger } from '@thumbnailer/domain';
import type { KafkaConnection } from './kafka.provider.js';
import type { Signals } from './signals.provider.js';

/**
 * Service lifecycle for a headless worker (no HTTP server).
 *
 * `start()` connects the wired Kafka clients and registers their teardown with
 * the central Signals registry, so SIGINT/SIGTERM drains them in order. Domain
 * services register their own teardown (stop consume, kill in-flight, drain)
 * via `signals.onTerm()` from their own start path.
 */
export class Commands {
  private readonly logger: ILogger;
  private readonly kafka: KafkaConnection;
  private readonly signals: Signals;

  constructor({
    logger,
    kafka,
    signals,
  }: {
    logger: ILogger;
    kafka: KafkaConnection;
    signals: Signals;
  }) {
    this.logger = logger;
    this.kafka = kafka;
    this.signals = signals;
  }

  /** Connect the pre-wired producer/consumer and register their teardown. */
  async start(): Promise<void> {
    const { producer, consumer } = this.kafka;

    if (producer) {
      await producer.connect();
      this.logger.info('Kafka producer connected');
      this.signals.onTerm(async () => {
        await producer.disconnect();
        this.logger.info('🔌 Kafka producer disconnected');
      });
    }

    if (consumer) {
      await consumer.connect();
      this.logger.info('Kafka consumer connected');
      this.signals.onTerm(async () => {
        await consumer.disconnect();
        this.logger.info('🔌 Kafka consumer disconnected');
      });
    }
  }

  /** Trigger a graceful shutdown programmatically (e.g. after a one-shot run). */
  async stop(code = 0): Promise<void> {
    await this.signals.terminate(code);
  }
}

export default Commands;
