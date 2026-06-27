import { serializeScanEvent, type ScanEvent } from '@thumbnailer/contracts';
import type { ILogger } from '@thumbnailer/domain';
import type { KafkaConnection } from '@thumbnailer/core';

interface EventProducerConfig {
  eventsTopic: string;
}

/**
 * Emits scan lifecycle events (ScanStarted / ScanCompleted / ScanCancelled /
 * ScanFailed) onto the events topic, for the orchestrator / observers to react to.
 */
export class EventProducer {
  private readonly kafka: KafkaConnection;
  private readonly logger: ILogger;
  private readonly config: EventProducerConfig;

  constructor(deps: { kafka: KafkaConnection; logger: ILogger; config: EventProducerConfig }) {
    this.kafka = deps.kafka;
    this.logger = deps.logger;
    this.config = deps.config;
  }

  async emit(event: ScanEvent): Promise<void> {
    await this.kafka.producer!.send({
      topic: this.config.eventsTopic,
      messages: [{ key: event.scanId, value: serializeScanEvent(event) }],
    });
    this.logger.info({ event }, '📢 emitted scan event');
  }
}

export default EventProducer;
