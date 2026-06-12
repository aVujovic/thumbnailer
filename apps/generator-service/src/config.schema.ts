import { z } from 'zod';
import { booleanish, resolvePath } from '@thumbnailer/core';

/**
 * Generator config, validated against process.env at startup. The `kafka`
 * block (mode: consumer + groupId) matches what core's kafka provider expects.
 */
export const ConfigSchema = z
  .object({
    KAFKA_BROKERS: z.string().min(1),
    GENERATOR_GROUP_ID: z.string().min(1).default('thumbnail-generators'),
    KAFKA_CLIENT_ID: z.string().min(1).default('generator-service'),
    // Shorter than kafkajs' 30s default so a dead/ghost consumer is evicted
    // quickly and the group rebalances fast on (re)start.
    KAFKA_SESSION_TIMEOUT_MS: z.coerce.number().int().positive().default(10_000),
    KAFKA_FROM_BEGINNING: booleanish.default('true'),
    TOPIC_VIDEO_JOBS: z.string().min(1).default('video-jobs'),
    TOPIC_SCAN_COMMANDS: z.string().min(1).default('scan-commands'),

    OUTPUT_ROOT: z.string().min(1).default('./output'),
    THUMBNAIL_FORMAT: z.string().min(1).default('jpg'),
    // The "video DB" path (JSONL). Lives at the repo root beside tenants.json so
    // the generator (writer) and sync-service (updater) share one store.
    VIDEO_DB_PATH: z.string().min(1).default('../../videoDb.jsonl'),
    TOPIC_THUMBNAIL_READY: z.string().min(1).default('thumbnail-ready'),

    GENERATE_TIMEOUT_MS: z.coerce.number().int().positive().default(120_000),
    GENERATE_MAX_RETRIES: z.coerce.number().int().nonnegative().default(2),
    GENERATE_FORCE: booleanish.default('false'),
    BACKOFF_BASE_MS: z.coerce.number().int().positive().default(100),
    BACKOFF_FACTOR: z.coerce.number().positive().default(2),

    FFMPEG_PATH: z.string().min(1).default('ffmpeg'),
    THUMBNAIL_TIMESTAMP: z.string().min(1).default('00:00:01'),
    KILL_SIGNAL: z
      .enum(['SIGKILL', 'SIGTERM', 'SIGINT'])
      .default('SIGKILL'),

    SHUTDOWN_GRACE_MS: z.coerce.number().int().nonnegative().default(300),
    LOG_LEVEL: z.string().default('info'),
    LOG_PRETTY: booleanish.optional(),
    NODE_ENV: z.string().default('development'),
  })
  .transform((env) => ({
    name: 'generator-service',
    env: env.NODE_ENV,
    logLevel: env.LOG_LEVEL,
    logPretty: env.LOG_PRETTY,
    shutdownGraceMs: env.SHUTDOWN_GRACE_MS,
    topic: env.TOPIC_VIDEO_JOBS,
    commandsTopic: env.TOPIC_SCAN_COMMANDS,
    fromBeginning: env.KAFKA_FROM_BEGINNING,

    // thumbnail output
    outputRoot: resolvePath(env.OUTPUT_ROOT),
    thumbnailFormat: env.THUMBNAIL_FORMAT.toLowerCase().replace(/^\./, ''),
    resultsPath: resolvePath(env.VIDEO_DB_PATH),
    readyTopic: env.TOPIC_THUMBNAIL_READY,

    // generation behaviour
    timeoutMs: env.GENERATE_TIMEOUT_MS,
    maxRetries: env.GENERATE_MAX_RETRIES,
    force: env.GENERATE_FORCE,
    backoffBaseMs: env.BACKOFF_BASE_MS,
    backoffFactor: env.BACKOFF_FACTOR,

    // ffmpeg adapter
    ffmpegPath: env.FFMPEG_PATH,
    thumbnailTimestamp: env.THUMBNAIL_TIMESTAMP,
    killSignal: env.KILL_SIGNAL,

    kafka: {
      brokers: env.KAFKA_BROKERS.split(',').map((b) => b.trim()),
      clientId: env.KAFKA_CLIENT_ID,
      // Consumes video-jobs + scan-commands, produces thumbnail-ready events.
      mode: 'both' as const,
      consumer: {
        groupId: env.GENERATOR_GROUP_ID,
        sessionTimeoutMs: env.KAFKA_SESSION_TIMEOUT_MS,
      },
    },
  }));

export type GeneratorConfig = z.infer<typeof ConfigSchema>;
