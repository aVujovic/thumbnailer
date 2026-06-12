import { createContainer, type AwilixContainer } from 'awilix';
import type { CoreCradle } from '@thumbnailer/core';
import type { IFileSystem } from '@thumbnailer/domain';
import type { ScannerConfig } from './config.schema.js';
import type { FileScanner } from './scanner/FileScanner.js';
import type { VideoDetector } from './scanner/videoDetector.js';
import type { ScanManager } from './scanner/ScanManager.js';
import type { JobProducer } from './producer/JobProducer.js';
import type { EventProducer } from './producer/EventProducer.js';
import type CommandConsumer from './jobs/CommandConsumer.job.js';

/** Full DI cradle for scanner-service: core infra + this service's classes. */
export interface ScannerCradle extends CoreCradle {
  config: ScannerConfig;
  fs: IFileSystem;
  videoDetector: VideoDetector;
  fileScanner: FileScanner;
  jobProducer: JobProducer;
  eventProducer: EventProducer;
  scanManager: ScanManager;
  commandConsumer: CommandConsumer;
}

/** Composition root for scanner-service. Populated in service.ts. */
const container: AwilixContainer<ScannerCradle> = createContainer<ScannerCradle>();

export default container;
