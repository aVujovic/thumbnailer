import {
  serializeScanCommand,
  type ScanCommand,
} from '@thumbnailer/contracts';
import type { ILogger } from '@thumbnailer/domain';
import type { KafkaConnection } from '@thumbnailer/core';

interface PublisherConfig {
  commandsTopic: string;
}

/**
 * Publishes a single ScanCommand onto the commands topic. The CLI builds the
 * command from its args and hands it here; this class only knows how to send.
 * Injectable so the build-the-command logic can be unit-tested with a stub
 * producer, no real Kafka.
 */
export class CommandPublisher {
  private readonly kafka: KafkaConnection;
  private readonly logger: ILogger;
  private readonly config: PublisherConfig;

  constructor(deps: { kafka: KafkaConnection; logger: ILogger; config: PublisherConfig }) {
    this.kafka = deps.kafka;
    this.logger = deps.logger;
    this.config = deps.config;
  }

  async publish(command: ScanCommand): Promise<void> {
    // Partition key, in priority order:
    //   tenantId  → routes a tenant's StartScan/StopScan to the SAME partition,
    //               i.e. the one scanner instance that owns that tenant (this is
    //               how scanners shard work across partitions, like the generator).
    //   scanId    → fallback to keep a scan's commands partition-ordered.
    //   type      → generator commands (Pause/Resume) have neither; key by type.
    const key =
      'tenantId' in command && command.tenantId
        ? command.tenantId
        : 'scanId' in command && command.scanId
          ? command.scanId
          : command.type;
    await this.kafka.producer!.send({
      topic: this.config.commandsTopic,
      messages: [{ key, value: serializeScanCommand(command) }],
    });
    this.logger.info({ command }, '📤 published scan command');
  }
}

export default CommandPublisher;
