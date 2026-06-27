import { createContainer, type AwilixContainer } from 'awilix';
import type { CoreCradle, Db } from '@thumbnailer/core';
import type { DbFlushConfig } from './config.schema.js';
import type { IVideoRepository } from './repository/VideoRepository.js';
import type { IDbSink } from './sink/DbSink.js';
import type DbFlushConsumer from './jobs/DbFlushConsumer.job.js';

/** Full DI cradle for db-flush-service: core infra + this service's classes. */
export interface DbFlushCradle extends CoreCradle {
  config: DbFlushConfig;
  db: Db;
  videoRepository: IVideoRepository;
  dbSink: IDbSink;
  dbFlushConsumer: DbFlushConsumer;
}

/** Composition root for db-flush-service. Populated in service.ts. */
const container: AwilixContainer<DbFlushCradle> = createContainer<DbFlushCradle>();

export default container;
