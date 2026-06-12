import { createContainer, type AwilixContainer } from 'awilix';
import type { CoreCradle } from '@thumbnailer/core';
import type { SyncConfig } from './config.schema.js';
import type { ISyncTarget } from './sync/SyncTarget.js';
import type { IVideoDb } from './repository/VideoDb.js';
import type ThumbnailReadyConsumer from './jobs/ThumbnailReadyConsumer.job.js';

/** Full DI cradle for sync-service: core infra + this service's classes. */
export interface SyncCradle extends CoreCradle {
  config: SyncConfig;
  syncTarget: ISyncTarget;
  videoDb: IVideoDb;
  thumbnailReadyConsumer: ThumbnailReadyConsumer;
}

/** Composition root for sync-service. Populated in service.ts. */
const container: AwilixContainer<SyncCradle> = createContainer<SyncCradle>();

export default container;
