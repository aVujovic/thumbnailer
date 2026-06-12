import { z } from 'zod';
import { booleanish, resolvePath } from '@thumbnailer/core';

/**
 * Scanner config, validated against process.env at startup by core's config
 * provider. The `kafka` block matches the shape core's kafka provider expects.
 */
export const ConfigSchema = z
  .object({
    KAFKA_BROKERS: z.string().min(1),
    TOPIC_VIDEO_JOBS: z.string().min(1).default('video-jobs'),
    TOPIC_SCAN_COMMANDS: z.string().min(1).default('scan-commands'),
    TOPIC_SCAN_EVENTS: z.string().min(1).default('scan-events'),
    SCANNER_GROUP_ID: z.string().min(1).default('scanners'),
    // Shorter than kafkajs' 30s default so a dead/ghost consumer is evicted
    // quickly — otherwise a new scanner waits out the timeout before the
    // rebalance completes and it can fetch commands.
    KAFKA_SESSION_TIMEOUT_MS: z.coerce.number().int().positive().default(10_000),
    // Fallback scan root — used only if a StartScan command omits one is not
    // expected; the orchestrator supplies scanRoot per scan.
    SCAN_ROOT: z.string().min(1).default('/data'),
    SCAN_MAX_DEPTH: z.coerce.number().int().positive().default(64),
    // Emit an info-level progress line every N entries scanned (0 = off).
    SCAN_PROGRESS_EVERY: z.coerce.number().int().nonnegative().default(1000),
    // Yield the event loop every N entries so the Kafka heartbeat keeps firing
    // during a large scan (sync fs would otherwise hog the loop). 0 = never.
    SCAN_YIELD_EVERY: z.coerce.number().int().nonnegative().default(500),
    VIDEO_EXTENSIONS: z
      .string()
      .default('mp4,mov,mkv,avi,webm,m4v,mpg,mpeg,wmv,flv'),
    KAFKA_CLIENT_ID: z.string().min(1).default('scanner-service'),
    LOG_LEVEL: z.string().default('info'),
    LOG_PRETTY: booleanish.optional(),
    NODE_ENV: z.string().default('development'),
  })
  .transform((env) => ({
    name: 'scanner-service',
    env: env.NODE_ENV,
    logLevel: env.LOG_LEVEL,
    logPretty: env.LOG_PRETTY,
    scanRoot: resolvePath(env.SCAN_ROOT),
    maxDepth: env.SCAN_MAX_DEPTH,
    progressEvery: env.SCAN_PROGRESS_EVERY,
    yieldEvery: env.SCAN_YIELD_EVERY,
    videoExtensions: env.VIDEO_EXTENSIONS.split(',')
      .map((e) => e.trim().toLowerCase().replace(/^\./, ''))
      .filter(Boolean),
    topic: env.TOPIC_VIDEO_JOBS,
    commandsTopic: env.TOPIC_SCAN_COMMANDS,
    eventsTopic: env.TOPIC_SCAN_EVENTS,
    kafka: {
      brokers: env.KAFKA_BROKERS.split(',').map((b) => b.trim()),
      clientId: env.KAFKA_CLIENT_ID,
      // Daemon: consumes commands, produces video-jobs + scan-events.
      mode: 'both' as const,
      consumer: {
        groupId: env.SCANNER_GROUP_ID,
        sessionTimeoutMs: env.KAFKA_SESSION_TIMEOUT_MS,
      },
    },
  }));

export type ScannerConfig = z.infer<typeof ConfigSchema>;
