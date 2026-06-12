import { randomUUID } from 'node:crypto';
import { createService } from '@thumbnailer/core';
import { asClass } from 'awilix';

import container from './container.js';
import { ConfigSchema, type OrchestratorConfig } from './config.schema.js';
import CommandPublisher from './CommandPublisher.js';
import { parseArgs } from './parseArgs.js';
import { TenantResolver } from './tenantResolver.js';

/**
 * One-shot CLI: resolve the tenant, parse the sub-command, connect the producer,
 * publish one ScanCommand, and exit. The scanner/generator daemons react to it.
 * Every scan is tenant-scoped — the path comes from the tenant catalog, never
 * from the CLI.
 *
 *   pnpm scan:start --tenant tenant-1   -> StartScan (path from tenants.json)
 *   pnpm scan:stop  [--id <scanId>]     -> StopScan
 *   pnpm gen:pause                       -> PauseGenerator
 *   pnpm gen:resume                      -> ResumeGenerator
 */
const main = async (): Promise<void> => {
  createService({ container, configSchema: ConfigSchema });
  container.register({ commandPublisher: asClass(CommandPublisher).singleton() });

  const config = container.cradle.config as OrchestratorConfig;
  // Lazily load the tenant catalog only when a --tenant is actually resolved,
  // so --root usage works even without a tenants.json present.
  let resolver: TenantResolver | undefined;
  const resolveTenant = (tenantId: string): string => {
    resolver ??= TenantResolver.fromFile(config.tenantsFile);
    return resolver.resolveScanRoot(tenantId);
  };

  const command = parseArgs(process.argv.slice(2), randomUUID, resolveTenant);

  const { commands, commandPublisher } = container.cradle;
  await commands.start(); // connect producer + register teardown
  await commandPublisher.publish(command);
  await commands.stop(0); // flush + graceful exit
};

main().catch((err: unknown) => {
  console.error(`orchestrator: ${err instanceof Error ? err.message : String(err)}`);
  process.exit(1);
});
