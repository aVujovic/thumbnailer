import type { ILogger } from '../logger.js';

export interface LogLine {
  level: 'info' | 'error' | 'warn' | 'debug' | 'trace' | 'fatal';
  fields: Record<string, unknown>;
  message?: string;
}

/**
 * Captures log lines for assertions instead of writing anywhere. Implements the
 * overloaded ILogger contract: first arg may be a message string or a fields
 * object followed by an optional message.
 */
export class FakeLogger implements ILogger {
  readonly lines: LogLine[] = [];

  private record(level: LogLine['level'], a: string | Record<string, unknown>, b?: string): void {
    if (typeof a === 'string') this.lines.push({ level, fields: {}, message: a });
    else this.lines.push({ level, fields: a, message: b });
  }

  info(a: string | Record<string, unknown>, b?: string): void { this.record('info', a, b); }
  error(a: string | Record<string, unknown>, b?: string): void { this.record('error', a, b); }
  warn(a: string | Record<string, unknown>, b?: string): void { this.record('warn', a, b); }
  debug(a: string | Record<string, unknown>, b?: string): void { this.record('debug', a, b); }
  trace(a: string | Record<string, unknown>, b?: string): void { this.record('trace', a, b); }
  fatal(a: string | Record<string, unknown>, b?: string): void { this.record('fatal', a, b); }

  child(): ILogger {
    return this; // flat capture is enough for tests
  }

  /** Convenience: count lines at a given level. */
  countAt(level: LogLine['level']): number {
    return this.lines.filter((l) => l.level === level).length;
  }
}

export default FakeLogger;
