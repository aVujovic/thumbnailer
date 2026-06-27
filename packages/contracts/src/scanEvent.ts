import { z } from 'zod';

/**
 * Events emitted by the scanner on the `scan-events` topic — facts about what
 * happened, consumed by the orchestrator / dashboards. Distinct from commands
 * (imperatives): an event reports the past, a command requests the future.
 */
export const ScanEventSchema = z.discriminatedUnion('type', [
  z.object({
    type: z.literal('ScanStarted'),
    scanId: z.string().min(1),
    scanRoot: z.string().min(1),
    tenantId: z.string().min(1).optional(),
    startedAt: z.number().int().nonnegative(),
  }),
  z.object({
    type: z.literal('ScanCompleted'),
    scanId: z.string().min(1),
    tenantId: z.string().min(1).optional(),
    /** Number of VideoJobs produced. */
    produced: z.number().int().nonnegative(),
    completedAt: z.number().int().nonnegative(),
  }),
  z.object({
    type: z.literal('ScanCancelled'),
    scanId: z.string().min(1),
    tenantId: z.string().min(1).optional(),
    /** Jobs produced before cancellation. */
    produced: z.number().int().nonnegative(),
    cancelledAt: z.number().int().nonnegative(),
  }),
  z.object({
    type: z.literal('ScanFailed'),
    scanId: z.string().min(1),
    tenantId: z.string().min(1).optional(),
    /** Jobs produced before the failure. */
    produced: z.number().int().nonnegative(),
    /** Why the scan aborted (e.g. a broker/producer error mid-walk). */
    reason: z.string().min(1),
    failedAt: z.number().int().nonnegative(),
  }),
]);

export type ScanEvent = z.infer<typeof ScanEventSchema>;

export function serializeScanEvent(event: ScanEvent): Buffer {
  return Buffer.from(JSON.stringify(event), 'utf-8');
}

export function parseScanEvent(raw: unknown): ScanEvent | null {
  const result = ScanEventSchema.safeParse(raw);
  return result.success ? result.data : null;
}
