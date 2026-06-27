import { createService, dbProvider } from '@thumbnailer/core';
import { asClass, asFunction } from 'awilix';

import container from './container.js';
import { ConfigSchema } from './config.schema.js';
import PrismaVideoRepository from './repository/VideoRepository.js';
import DbSink from './sink/DbSink.js';
import DbFlushConsumer from './jobs/DbFlushConsumer.job.js';

/**
 * Entry point / composition root for db-flush-service. The sole writer to the
 * video DB: it consumes WriteCommands off the `db-flush` topic, the sink routes
 * each to a video-repository operation, and the repository persists them to
 * Postgres (via Prisma) in batches. Generator/sync no longer touch the DB.
 */
const main = async (): Promise<void> => {
  createService({ container, configSchema: ConfigSchema });

  container.register({
    // The shared Prisma client, built by core's db provider from config.
    db: asFunction(dbProvider).singleton(),
    // Repository owns DB contact; sink owns command routing.
    videoRepository: asClass(PrismaVideoRepository).singleton(),
    dbSink: asClass(DbSink).singleton(),
    dbFlushConsumer: asClass(DbFlushConsumer).singleton(),
  });

  const { commands, signals, dbSink, dbFlushConsumer } = container.cradle;
  // Release the Prisma connection on shutdown.
  signals.onTerm(async () => {
    await dbSink.close();
  });

  await commands.start(); // connect consumer + register Kafka teardown
  await dbFlushConsumer.start(); // subscribe + run (blocks as a daemon)
};

void main();
