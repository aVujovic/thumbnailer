import { createContainer, type AwilixContainer } from 'awilix';
import type { CoreCradle } from '@thumbnailer/core';
import type { OrchestratorConfig } from './config.schema.js';
import type CommandPublisher from './CommandPublisher.js';

export interface OrchestratorCradle extends CoreCradle {
  config: OrchestratorConfig;
  commandPublisher: CommandPublisher;
}

const container: AwilixContainer<OrchestratorCradle> =
  createContainer<OrchestratorCradle>();

export default container;
