import { z } from 'zod';
import { booleanish } from '@thumbnailer/core';

/**
 * db-flush-service config. A consumer-only daemon: it reads WriteCommands off the
 * `db-flush` topic and applies them to Postgres in batches.
 */
export const ConfigSchema = z
  .object({
    KAFKA_BROKERS: z.string().min(1),
    DB_FLUSH_GROUP_ID: z.string().min(1).default('db-flushers'),
    KAFKA_CLIENT_ID: z.string().min(1).default('db-flush-service'),
    KAFKA_SESSION_TIMEOUT_MS: z.coerce.number().int().positive().default(10_000),
    KAFKA_FROM_BEGINNING: booleanish.default('true'),
    TOPIC_DB_FLUSH: z.string().min(1).default('db-flush'),

    // Postgres connection (the "video DB" in production form).
    DATABASE_URL: z
      .string()
      .min(1)
      .default('postgres://thumbnailer:thumbnailer@localhost:5433/thumbnailer'),

    // Batch tuning: flush when EITHER threshold trips first. This is the
    // write-behind micro-batch — a bigger batch amortises the round-trip but
    // raises the visibility delay.
    BATCH_MAX_SIZE: z.coerce.number().int().positive().default(500),
    BATCH_MAX_WAIT_MS: z.coerce.number().int().positive().default(1_000),

    SHUTDOWN_GRACE_MS: z.coerce.number().int().nonnegative().default(300),
    LOG_LEVEL: z.string().default('info'),
    LOG_PRETTY: booleanish.optional(),
    NODE_ENV: z.string().default('development'),
  })
  .transform((env) => ({
    name: 'db-flush-service',
    env: env.NODE_ENV,
    logLevel: env.LOG_LEVEL,
    logPretty: env.LOG_PRETTY,
    shutdownGraceMs: env.SHUTDOWN_GRACE_MS,

    flushTopic: env.TOPIC_DB_FLUSH,
    fromBeginning: env.KAFKA_FROM_BEGINNING,
    databaseUrl: env.DATABASE_URL,
    batchMaxSize: env.BATCH_MAX_SIZE,
    batchMaxWaitMs: env.BATCH_MAX_WAIT_MS,

    kafka: {
      brokers: env.KAFKA_BROKERS.split(',').map((b) => b.trim()),
      clientId: env.KAFKA_CLIENT_ID,
      mode: 'consumer' as const,
      consumer: {
        groupId: env.DB_FLUSH_GROUP_ID,
        sessionTimeoutMs: env.KAFKA_SESSION_TIMEOUT_MS,
        // Time-bound for micro-batching: how long the consumer waits to fill a
        // fetch before returning a partial batch (quiet topic → flush promptly).
        maxWaitTimeMs: env.BATCH_MAX_WAIT_MS,
      },
    },
  }));

export type DbFlushConfig = z.infer<typeof ConfigSchema>;
