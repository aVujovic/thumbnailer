import { createContainer, type AwilixContainer } from 'awilix';
import type { CoreCradle } from '@thumbnailer/core';
import type { StartThumbnailProcess } from '@thumbnailer/domain';
import type { GeneratorConfig } from './config.schema.js';
import type { IThumbnailStore } from './thumbnail/ThumbnailStore.js';
import type { ThumbnailGenerator } from './thumbnail/ThumbnailGenerator.js';
import type VideoJobConsumer from './jobs/VideoJobConsumer.job.js';

/** Full DI cradle for generator-service: core infra + this service's classes. */
export interface GeneratorCradle extends CoreCradle {
  config: GeneratorConfig;
  startThumbnailProcess: StartThumbnailProcess;
  thumbnailStore: IThumbnailStore;
  thumbnailGenerator: ThumbnailGenerator;
  videoJobConsumer: VideoJobConsumer;
}

/** Composition root for generator-service. Populated in service.ts. */
const container: AwilixContainer<GeneratorCradle> = createContainer<GeneratorCradle>();

export default container;
