import { createService } from '@thumbnailer/core';
import { asClass } from 'awilix';

import container from './container.js';
import { ConfigSchema } from './config.schema.js';
import LocalFileSystem from './fs/LocalFileSystem.js';
import VideoDetector from './scanner/videoDetector.js';
import FileScanner from './scanner/FileScanner.js';
import ScanManager from './scanner/ScanManager.js';
import JobProducer from './producer/JobProducer.js';
import EventProducer from './producer/EventProducer.js';
import CommandConsumer from './jobs/CommandConsumer.job.js';

/**
 * Entry point / composition root. The scanner is now a long-lived daemon: it
 * consumes scan-commands and drives the ScanManager, producing video-jobs and
 * scan-events. No business logic here — only wiring + start.
 */
const main = async (): Promise<void> => {
  createService({ container, configSchema: ConfigSchema });

  container.register({
    // The production IFileSystem is assumed to exist; LocalFileSystem is the
    // local stand-in so the demo runs against real files.
    fs: asClass(LocalFileSystem).singleton(),
    videoDetector: asClass(VideoDetector).singleton(),
    fileScanner: asClass(FileScanner).singleton(),
    jobProducer: asClass(JobProducer).singleton(),
    eventProducer: asClass(EventProducer).singleton(),
    scanManager: asClass(ScanManager).singleton(),
    commandConsumer: asClass(CommandConsumer).singleton(),
  });

  const { commands, commandConsumer } = container.cradle;
  await commands.start(); // connect producer + consumer, register teardown
  await commandConsumer.start(); // subscribe to scan-commands + run (daemon)
};

void main();
