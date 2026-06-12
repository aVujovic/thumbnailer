import type { z } from 'zod';

/**
 * Builds an env-driven config provider validated by a service-supplied zod
 * schema. Each service defines its own schema (config.schema.ts) and passes it
 * to createService; we validate `process.env` against it once at startup and
 * fail fast with a readable error if anything is missing/invalid.
 *
 * Env-driven because these are headless workers deployed via docker-compose —
 * 12-factor config fits better than a config file.
 */
export function makeConfigProvider<S extends z.ZodTypeAny>(
  schema: S,
): () => z.infer<S> {
  return () => {
    const result = schema.safeParse(process.env);
    if (!result.success) {
      const issues = result.error.issues
        .map((i) => `  - ${i.path.join('.') || '(root)'}: ${i.message}`)
        .join('\n');
      throw new Error(`Invalid configuration:\n${issues}`);
    }
    return result.data;
  };
}
