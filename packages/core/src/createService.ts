import { asClass, asFunction, type AwilixContainer } from 'awilix';
import type { z } from 'zod';

import loggerProvider from './providers/logger.provider.js';
import kafkaProvider from './providers/kafka.provider.js';
import { makeConfigProvider } from './providers/config.provider.js';
import { KafkaUtils } from './providers/kafkaUtils.provider.js';
import { Signals } from './providers/signals.provider.js';
import { Commands } from './providers/commands.provider.js';

/**
 * Registers every cross-cutting infrastructure provider into the given awilix
 * container in one call.
 *
 * After this returns, the cradle exposes: config, logger, kafka, kafkaUtils,
 * signals, commands. Each service then registers only its own domain classes,
 * which receive all of the above through constructor injection.
 *
 * @param container  The service's awilix container (composition root).
 * @param configSchema  Service-specific zod schema validated against env.
 */
export function createService<S extends z.ZodTypeAny>({
  container,
  configSchema,
}: {
  container: AwilixContainer;
  configSchema: S;
}): AwilixContainer {
  container.register({
    config: asFunction(makeConfigProvider(configSchema)).singleton(),
    logger: asFunction(loggerProvider).singleton(),
    signals: asClass(Signals).singleton(),
    kafka: asFunction(kafkaProvider).singleton(),
    kafkaUtils: asClass(KafkaUtils).singleton(),
    commands: asClass(Commands).singleton(),
  });

  return container;
}
