import { createService } from '@thumbnailer/core';
import { asClass, asFunction } from 'awilix';

import container from './container.js';
import { ConfigSchema, type GeneratorConfig } from './config.schema.js';
import { makeStartThumbnailProcess } from './ffmpeg/startThumbnailProcess.js';
import LocalThumbnailStore from './thumbnail/ThumbnailStore.js';
import ThumbnailGenerator from './thumbnail/ThumbnailGenerator.js';
import VideoJobConsumer from './jobs/VideoJobConsumer.job.js';

/**
 * Entry point / composition root. No business logic here — wires infrastructure
 * (core) and the service's own classes, then starts them. The consumer is a
 * long-running daemon; shutdown is handled by core's signals registry.
 */
const main = async (): Promise<void> => {
  createService({ container, configSchema: ConfigSchema });

  container.register({
    // The production startThumbnailProcess is assumed; this adapter runs real
    // ffmpeg if present, else writes a placeholder. Built from env-driven config.
    startThumbnailProcess: asFunction(({ config }: { config: GeneratorConfig }) =>
      makeStartThumbnailProcess({
        ffmpegPath: config.ffmpegPath,
        thumbnailTimestamp: config.thumbnailTimestamp,
        killSignal: config.killSignal,
      }),
    ).singleton(),
    thumbnailStore: asClass(LocalThumbnailStore).singleton(),
    thumbnailGenerator: asClass(ThumbnailGenerator).singleton(),
    // No DB repo here anymore — the generator emits an upsert WriteCommand onto
    // the db-flush topic; the db-flush-service persists it.
    videoJobConsumer: asClass(VideoJobConsumer).singleton(),
  });

  const { commands, videoJobConsumer } = container.cradle;
  await commands.start(); // connect consumer + register Kafka teardown
  await videoJobConsumer.start(); // subscribe + run (blocks as a daemon)
};

void main();
