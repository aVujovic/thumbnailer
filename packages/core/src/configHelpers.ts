import { z } from 'zod';
import { homedir } from 'node:os';
import { resolve } from 'node:path';

/**
 * Resolve a user-supplied path to an absolute one. Expands a leading `~` (which
 * dotenv does NOT expand — only the shell does) and resolves relative paths
 * against the cwd, so `~/Movies` or `./out` work whether set in .env or inline.
 */
export function resolvePath(p: string): string {
  if (p === '~') return homedir();
  if (p.startsWith('~/')) return resolve(homedir(), p.slice(2));
  return resolve(p);
}

/**
 * Coerces an env string into a boolean. Accepts 'true'/'false' (case-insensitive),
 * plus '1'/'0'. Use `.default('false')` / `.optional()` at the call site.
 *
 * Env vars are always strings, so a plain z.boolean() can't be used directly —
 * this is the shared parser so every service treats booleans the same way.
 */
export const booleanish = z
  .string()
  .transform((v) => v.trim().toLowerCase())
  .pipe(z.enum(['true', 'false', '1', '0']))
  .transform((v) => v === 'true' || v === '1');

/** Split a comma-separated env string into a trimmed, non-empty list. */
export const csv = z.string().transform((v) =>
  v
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean),
);
