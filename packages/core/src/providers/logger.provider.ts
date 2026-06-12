import pino, { type Logger as PinoLogger } from 'pino';
import type { ILogger } from '@thumbnailer/domain';

/**
 * Builds the root logger for the service.
 *
 * - Production (`env: 'production'`): JSON, one log per line, no pretty printing.
 * - Anything else: human-readable via `pino-pretty` with colour + timestamp.
 *
 * Returns the pino instance directly — pino implements the ILogger contract.
 */
const loggerProvider = ({ config }: { config: AppConfig }): ILogger => {
  const isProd = config?.env === 'production';
  const level = config?.logLevel ?? 'info';
  // Pretty printing defaults to "on outside production"; LOG_PRETTY overrides
  // either way (e.g. force JSON in dev, or pretty in a non-prod container).
  const pretty = config?.logPretty ?? !isProd;

  const base: PinoLogger = pino(
    {
      level,
      base: config?.name ? { service: config.name } : null,
      timestamp: isProd ? pino.stdTimeFunctions.epochTime : pino.stdTimeFunctions.isoTime,
    },
    pretty
      ? pino.transport({
          target: 'pino-pretty',
          options: {
            colorize: true,
            translateTime: 'HH:MM:ss.l',
            ignore: 'pid,hostname',
          },
        })
      : pino.destination(1),
  );

  return base as unknown as ILogger;
};

/** Minimal slice of the service config the logger needs. */
interface AppConfig {
  env?: string;
  logLevel?: string;
  logPretty?: boolean;
  name?: string;
}

export default loggerProvider;
