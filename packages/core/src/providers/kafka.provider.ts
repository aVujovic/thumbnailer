import { createRequire } from 'node:module';
import type { Kafka, Producer, Consumer } from 'kafkajs';
import type { ILogger } from '@thumbnailer/domain';

// ESM-safe `require` so kafkajs is only loaded when this provider runs.
const require = createRequire(import.meta.url);

export interface KafkaConnection {
  /** The shared Kafka client. Use it to create extra producers / consumers
   *  beyond the auto-wired ones. */
  kafka: Kafka;
  /** Pre-connected producer. Present when `mode` is `'producer'` or `'both'`. */
  producer?: Producer;
  /** Pre-connected consumer, joined to `consumer.groupId`. Present when `mode`
   *  is `'consumer'` or `'both'`. Caller must `subscribe()` then `run()`. */
  consumer?: Consumer;
}

export interface KafkaConnectorConfig {
  /** Broker host:port list (Kafka or Redpanda — wire-compatible). */
  brokers: string[];
  /** Logical client identifier sent to the broker. Default: `config.name`. */
  clientId?: string;
  /** Which clients to wire up. Default: `'producer'`. */
  mode?: 'producer' | 'consumer' | 'both';
  /** Required when `mode` includes consumer. */
  consumer?: {
    groupId: string;
    sessionTimeoutMs?: number;
  };
  /** Producer tunables forwarded verbatim to `kafka.producer({...})`. */
  producer?: {
    allowAutoTopicCreation?: boolean;
    idempotent?: boolean;
  };
  connectionTimeoutMs?: number;
  requestTimeoutMs?: number;
}

interface ProviderDeps {
  logger: ILogger;
  config: { name?: string; kafka?: KafkaConnectorConfig };
}

const kafkaProvider = ({ logger, config }: ProviderDeps): KafkaConnection => {
  const cfg = config?.kafka;
  if (!cfg?.brokers || cfg.brokers.length === 0) {
    throw new Error('config.kafka.brokers is required (non-empty list)');
  }

  const mode = cfg.mode ?? 'producer';
  if ((mode === 'consumer' || mode === 'both') && !cfg.consumer?.groupId) {
    throw new Error(`config.kafka.consumer.groupId is required when mode="${mode}"`);
  }

  const { Kafka } = require('kafkajs') as typeof import('kafkajs');

  const kafka = new Kafka({
    clientId: cfg.clientId ?? config?.name ?? 'thumbnailer-service',
    brokers: cfg.brokers,
    ...(cfg.connectionTimeoutMs !== undefined
      ? { connectionTimeout: cfg.connectionTimeoutMs }
      : {}),
    ...(cfg.requestTimeoutMs !== undefined ? { requestTimeout: cfg.requestTimeoutMs } : {}),
    // kafkajs writes to its own logger — silence it; we surface relevant
    // events through our pino logger below.
    logCreator: () => () => {},
  });

  const connection: KafkaConnection = { kafka };

  if (mode === 'producer' || mode === 'both') {
    const producer = kafka.producer({
      ...(cfg.producer?.allowAutoTopicCreation !== undefined
        ? { allowAutoTopicCreation: cfg.producer.allowAutoTopicCreation }
        : {}),
      ...(cfg.producer?.idempotent !== undefined
        ? { idempotent: cfg.producer.idempotent }
        : {}),
    });
    connection.producer = producer;
  }

  if (mode === 'consumer' || mode === 'both') {
    const consumer = kafka.consumer({
      groupId: cfg.consumer!.groupId,
      ...(cfg.consumer!.sessionTimeoutMs !== undefined
        ? { sessionTimeout: cfg.consumer!.sessionTimeoutMs }
        : {}),
    });
    connection.consumer = consumer;
  }

  return connection;
};

export default kafkaProvider;
