import { z } from 'zod';

/**
 * Command type names — the single source of truth for the `type` discriminant.
 * Used by the schema, the builders, and every consumer's switch/if, so a
 * command name is never a bare string literal anywhere.
 */
export const SCAN_COMMAND = {
  StartScan: 'StartScan',
  StopScan: 'StopScan',
  PauseGenerator: 'PauseGenerator',
  ResumeGenerator: 'ResumeGenerator',
} as const;

export type ScanCommandType = (typeof SCAN_COMMAND)[keyof typeof SCAN_COMMAND];

/**
 * Commands sent by the orchestrator on the `scan-commands` topic. Consumed by
 * the scanner (StartScan / StopScan) and the generator (PauseGenerator /
 * ResumeGenerator). A discriminated union on `type` so each consumer handles
 * only the commands it cares about and ignores the rest.
 */
export const ScanCommandSchema = z.discriminatedUnion('type', [
  z.object({
    type: z.literal(SCAN_COMMAND.StartScan),
    /** Correlation id for this scan; flows onto every VideoJob/result. */
    scanId: z.string().min(1),
    /** Directory to scan (recursively). */
    scanRoot: z.string().min(1),
    /** Tenant this scan runs for; flows onto every VideoJob/result. */
    tenantId: z.string().min(1).optional(),
  }),
  z.object({
    type: z.literal(SCAN_COMMAND.StopScan),
    /** Stop a specific scan, or omit to stop whatever is running + clear the queue. */
    scanId: z.string().min(1).optional(),
    /**
     * Tenant whose scan to stop. Used as the partition key so the StopScan lands
     * on the same partition (same scanner instance) as that tenant's StartScan —
     * essential once scanners are sharded by tenant across partitions.
     */
    tenantId: z.string().min(1).optional(),
  }),
  z.object({ type: z.literal(SCAN_COMMAND.PauseGenerator) }),
  z.object({ type: z.literal(SCAN_COMMAND.ResumeGenerator) }),
]);

export type ScanCommand = z.infer<typeof ScanCommandSchema>;
export type StartScanCommand = Extract<ScanCommand, { type: 'StartScan' }>;

/**
 * Builders for each command — the one place a ScanCommand object is shaped.
 * Producers (the orchestrator) call these instead of hand-writing `{ type, … }`.
 */
export const scanCommands = {
  startScan: (scanId: string, scanRoot: string, tenantId?: string): ScanCommand => ({
    type: SCAN_COMMAND.StartScan,
    scanId,
    scanRoot,
    ...(tenantId ? { tenantId } : {}),
  }),
  stopScan: (scanId?: string, tenantId?: string): ScanCommand => ({
    type: SCAN_COMMAND.StopScan,
    ...(scanId ? { scanId } : {}),
    ...(tenantId ? { tenantId } : {}),
  }),
  pauseGenerator: (): ScanCommand => ({ type: SCAN_COMMAND.PauseGenerator }),
  resumeGenerator: (): ScanCommand => ({ type: SCAN_COMMAND.ResumeGenerator }),
} as const;

export function serializeScanCommand(cmd: ScanCommand): Buffer {
  return Buffer.from(JSON.stringify(cmd), 'utf-8');
}

/** Parse + validate a command; null on malformed input (poison-message safe). */
export function parseScanCommand(raw: unknown): ScanCommand | null {
  const result = ScanCommandSchema.safeParse(raw);
  return result.success ? result.data : null;
}
