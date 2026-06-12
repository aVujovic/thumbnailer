import { parseScanCommand, SCAN_COMMAND } from '@thumbnailer/contracts';
import type { ILogger } from '@thumbnailer/domain';
import type { KafkaConnection, KafkaUtils, Signals } from '@thumbnailer/core';
import type { ScannerConfig } from '../config.schema.js';
import type { ScanManager } from '../scanner/ScanManager.js';

/**
 * The scanner daemon's entry point: consumes the commands topic and drives the
 * ScanManager. StartScan -> enqueue; StopScan -> cancel/clear. Generator-only
 * commands (Pause/Resume) are ignored here.
 *
 * Commands are auto-committed: they're fire-and-forget control signals, not
 * work units, and a scan's progress is tracked by its own events — there's no
 * value in redelivering a StartScan after a crash (the orchestrator can re-issue
 * it). This is intentionally different from the video-jobs write-ahead path.
 */
export class CommandConsumer {
  private readonly logger: ILogger;
  private readonly config: ScannerConfig;
  private readonly kafka: KafkaConnection;
  private readonly kafkaUtils: KafkaUtils;
  private readonly signals: Signals;
  private readonly scanManager: ScanManager;

  private running = false;

  constructor(deps: {
    logger: ILogger;
    config: ScannerConfig;
    kafka: KafkaConnection;
    kafkaUtils: KafkaUtils;
    signals: Signals;
    scanManager: ScanManager;
  }) {
    this.logger = deps.logger;
    this.config = deps.config;
    this.kafka = deps.kafka;
    this.kafkaUtils = deps.kafkaUtils;
    this.signals = deps.signals;
    this.scanManager = deps.scanManager;
  }

  async start(): Promise<void> {
    if (this.running) return;
    const consumer = this.kafka.consumer!;

    await consumer.subscribe({ topic: this.config.commandsTopic, fromBeginning: false });

    this.signals.onTerm(async () => {
      this.scanManager.stop(); // cancel current scan + clear queue
      await consumer.stop();
      this.logger.info('🛑 command consumer stopped');
    });

    this.logger.info(
      { commandsTopic: this.config.commandsTopic, groupId: this.config.kafka.consumer.groupId },
      'scanner-service started — waiting for commands',
    );

    await consumer.run({
      autoCommit: true,
      eachMessage: async ({ topic, partition, message }) => {
        const command = parseScanCommand(
          this.kafkaUtils.parseJson(message, { topic, partition, offset: message.offset }),
        );
        if (!command) return; // poison — already warned

        this.handle(command);
      },
    });

    this.running = true;
  }

  private handle(command: ReturnType<typeof parseScanCommand>): void {
    switch (command!.type) {
      case SCAN_COMMAND.StartScan:
        this.scanManager.enqueue({
          scanId: command!.scanId,
          scanRoot: command!.scanRoot,
          tenantId: command!.tenantId,
        });
        break;
      case SCAN_COMMAND.StopScan:
        this.scanManager.stop(command!.scanId);
        break;
      // PauseGenerator / ResumeGenerator are for the generator — ignore here.
      default:
        break;
    }
  }
}

export default CommandConsumer;
