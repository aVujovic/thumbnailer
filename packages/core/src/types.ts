import type { AwilixContainer } from 'awilix';
import type { ILogger } from '@thumbnailer/domain';
import type { KafkaConnection } from './providers/kafka.provider.js';
import type { KafkaUtils } from './providers/kafkaUtils.provider.js';
import type { Signals } from './providers/signals.provider.js';
import type { Commands } from './providers/commands.provider.js';

/**
 * The infrastructure cradle every service gets from `createService`. Service
 * containers extend this with their own domain registrations, so a service's
 * full cradle type is `CoreCradle & { ...domain }`.
 */
export interface CoreCradle {
  config: Record<string, unknown>;
  logger: ILogger;
  kafka: KafkaConnection;
  kafkaUtils: KafkaUtils;
  signals: Signals;
  commands: Commands;
}

export type CoreContainer = AwilixContainer<CoreCradle>;
