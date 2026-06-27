import { createService } from '@thumbnailer/core';
import { asClass } from 'awilix';

import container from './container.js';
import { ConfigSchema } from './config.schema.js';
import LoggingSyncTarget from './sync/SyncTarget.js';
import ThumbnailReadyConsumer from './jobs/ThumbnailReadyConsumer.job.js';

/**
 * Entry point / composition root for sync-service. No business logic here —
 * wires infrastructure (core) and the service's own classes, then starts them.
 * The consumer is a long-running daemon; shutdown is handled by core's signals
 * registry. It sits idle until the generator announces a ready thumbnail.
 */
const main = async (): Promise<void> => {
  createService({ container, configSchema: ConfigSchema });

  container.register({
    // Log-only in this assessment; rsync/scp/S3 in production (same interface).
    syncTarget: asClass(LoggingSyncTarget).singleton(),
    // No DB repo here anymore — sync emits a mark-synced WriteCommand onto the
    // db-flush topic; the db-flush-service persists it.
    thumbnailReadyConsumer: asClass(ThumbnailReadyConsumer).singleton(),
  });

  const { commands, thumbnailReadyConsumer } = container.cradle;
  await commands.start(); // connect consumer + register Kafka teardown
  await thumbnailReadyConsumer.start(); // subscribe + run (blocks as a daemon)
};

void main();