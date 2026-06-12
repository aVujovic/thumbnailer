import { z } from 'zod';
import { booleanish, resolvePath } from '@thumbnailer/core';

/**
 * Sync-service config, validated against process.env at startup. A consumer-only
 * daemon: it reads the thumbnail-ready topic and updates the shared video DB.
 */
export const ConfigSchema = z
  .object({
    KAFKA_BROKERS: z.string().min(1),
    SYNC_GROUP_ID: z.string().min(1).default('thumbnail-syncers'),
    KAFKA_CLIENT_ID: z.string().min(1).default('sync-service'),
    // Shorter than kafkajs' 30s default so a dead/ghost consumer is evicted
    // quickly and the group rebalances fast on (re)start.
    KAFKA_SESSION_TIMEOUT_MS: z.coerce.number().int().positive().default(10_000),
    KAFKA_FROM_BEGINNING: booleanish.default('true'),
    TOPIC_THUMBNAIL_READY: z.string().min(1).default('thumbnail-ready'),

    // The shared "video DB" — same file the generator writes. Lives at the repo
    // root beside tenants.json. The sync-service flips each row's `synced` flag.
    VIDEO_DB_PATH: z.string().min(1).default('../../videoDb.jsonl'),
    // Simulated per-sync latency (ms) so the daemon visibly does work; the real
    // sync (rsync/S3/scp) would take this place. 0 = no delay.
    SYNC_DELAY_MS: z.coerce.number().int().nonnegative().default(0),

    SHUTDOWN_GRACE_MS: z.coerce.number().int().nonnegative().default(300),
    LOG_LEVEL: z.string().default('info'),
    LOG_PRETTY: booleanish.optional(),
    NODE_ENV: z.string().default('development'),
  })
  .transform((env) => ({
    name: 'sync-service',
    env: env.NODE_ENV,
    logLevel: env.LOG_LEVEL,
    logPretty: env.LOG_PRETTY,
    shutdownGraceMs: env.SHUTDOWN_GRACE_MS,

    readyTopic: env.TOPIC_THUMBNAIL_READY,
    fromBeginning: env.KAFKA_FROM_BEGINNING,
    videoDbPath: resolvePath(env.VIDEO_DB_PATH),
    syncDelayMs: env.SYNC_DELAY_MS,

    kafka: {
      brokers: env.KAFKA_BROKERS.split(',').map((b) => b.trim()),
      clientId: env.KAFKA_CLIENT_ID,
      mode: 'consumer' as const,
      consumer: {
        groupId: env.SYNC_GROUP_ID,
        sessionTimeoutMs: env.KAFKA_SESSION_TIMEOUT_MS,
      },
    },
  }));

export type SyncConfig = z.infer<typeof ConfigSchema>;
