import { z } from 'zod';

/**
 * The unit of work passed from scanner to generator over Kafka/Redpanda.
 *
 * Kept deliberately small: the path is the source of truth, plus the metadata
 * the scanner already had in hand (cheap to include, useful downstream for
 * logging / future filtering). The generator re-derives the output path.
 */
export const VideoJobSchema = z.object({
  /** Absolute path of the discovered video file in the scanned file system. */
  path: z.string().min(1),
  /** File size in bytes, as reported by the scanner. */
  size: z.number().int().nonnegative(),
  /** Epoch millis when the scanner discovered the file. */
  discoveredAt: z.number().int().nonnegative(),
  /** The scan this job belongs to — correlates jobs/results back to a scan. */
  scanId: z.string().min(1).optional(),
  /** The tenant this job belongs to — drives per-tenant output + tracking. */
  tenantId: z.string().min(1).optional(),
});

export type VideoJob = z.infer<typeof VideoJobSchema>;

/** Serialize a job to the Buffer Kafka expects as a message value. */
export function serializeVideoJob(job: VideoJob): Buffer {
  return Buffer.from(JSON.stringify(job), 'utf-8');
}

/**
 * Parse + validate an incoming job. Returns null on malformed input so the
 * consumer can skip poison messages instead of crashing the partition.
 */
export function parseVideoJob(raw: unknown): VideoJob | null {
  const result = VideoJobSchema.safeParse(raw);
  return result.success ? result.data : null;
}
