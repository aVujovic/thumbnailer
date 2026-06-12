/**
 * Structured logger contract. The first argument may be a plain message string
 * or an object of fields followed by an optional message.
 *
 *   logger.info('scan started');
 *   logger.info({ path }, 'video found');
 *   logger.error({ err }, 'thumbnail failed');
 */
export interface ILogger {
  info(message: string): void;
  info(fields: Record<string, unknown>, message?: string): void;
  error(message: string): void;
  error(fields: Record<string, unknown>, message?: string): void;
  warn(message: string): void;
  warn(fields: Record<string, unknown>, message?: string): void;
  debug(message: string): void;
  debug(fields: Record<string, unknown>, message?: string): void;
  trace(message: string): void;
  trace(fields: Record<string, unknown>, message?: string): void;
  fatal(message: string): void;
  fatal(fields: Record<string, unknown>, message?: string): void;
  /** Create a derived logger with permanent context fields. */
  child(bindings: Record<string, unknown>): ILogger;
}
