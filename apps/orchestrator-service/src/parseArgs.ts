import { scanCommands, type ScanCommand } from '@thumbnailer/contracts';

function flag(args: string[], name: string): string | undefined {
  const i = args.indexOf(`--${name}`);
  return i !== -1 ? args[i + 1] : undefined;
}

/** Resolves a tenantId to its scan root (injected so parseArgs stays pure/testable). */
export type ResolveTenant = (tenantId: string) => string;

/**
 * Turn CLI args into a ScanCommand. Pure (no I/O) — tenant resolution is
 * injected via `resolveTenant`, scanId generation via `newId`.
 *
 * Every scan is tenant-scoped: the orchestrator only accepts a `--tenant <id>`
 * and resolves the path from its catalog. Raw paths are not accepted — the
 * scan root is a tenant configuration detail, never a CLI argument.
 *
 * Usage:
 *   start  --tenant <id> [--id <scanId>]   StartScan (path resolved from catalog)
 *   stop   [--tenant <id>] [--id <scanId>] StopScan (--tenant routes to the owning scanner)
 *   pause                                  PauseGenerator
 *   resume                                 ResumeGenerator
 */
export function parseArgs(
  argv: string[],
  newId: () => string,
  resolveTenant: ResolveTenant,
): ScanCommand {
  const [sub, ...rest] = argv;

  switch (sub) {
    case 'start': {
      const tenant = flag(rest, 'tenant');
      if (!tenant) throw new Error('start requires --tenant <id>');
      const scanId = flag(rest, 'id') ?? newId();
      // Tenant-driven: the orchestrator resolves the path from its catalog.
      return scanCommands.startScan(scanId, resolveTenant(tenant), tenant);
    }
    case 'stop':
      // --tenant is optional here, and only routes the command to the scanner
      // that owns the tenant (the partition key); it is NOT resolved to a path.
      return scanCommands.stopScan(flag(rest, 'id'), flag(rest, 'tenant'));
    case 'pause':
      return scanCommands.pauseGenerator();
    case 'resume':
      return scanCommands.resumeGenerator();
    default:
      throw new Error(
        `unknown command '${sub ?? ''}'. Use: ` +
          `start --tenant <id> | stop [--id <id>] | pause | resume`,
      );
  }
}
