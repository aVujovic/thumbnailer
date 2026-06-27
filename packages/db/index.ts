import { PrismaClient as RawPrismaClient } from './generated/prisma/client/index.js';

export { PrismaClient } from './generated/prisma/client/index.js';
export { Prisma as PrismaNamespace } from './generated/prisma/client/index.js';
export * from './generated/prisma/client/index.js';

export type CreatePrismaOptions = {
  log?: ('query' | 'info' | 'warn' | 'error')[];
  /** Connection string. Falls back to DATABASE_URL. */
  url?: string;
};

/**
 * Build a PrismaClient. The single place a client is constructed — services
 * receive it through DI (core's db provider), never `new` it themselves, so the
 * connection is shared and torn down in one place.
 */
export function createPrismaClient(opts: CreatePrismaOptions = {}) {
  const url = opts.url ?? process.env.DATABASE_URL;
  if (!url) throw new Error('DATABASE_URL must be set (or pass opts.url)');
  return new RawPrismaClient({
    datasources: { db: { url } },
    log: opts.log,
  });
}

export type PrismaType = ReturnType<typeof createPrismaClient>;
/** Alias kept for repositories typed `prisma: Prisma`. */
export type Prisma = PrismaType;
