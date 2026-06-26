import { createPrismaClient, type PrismaType } from '@thumbnailer/db';
import type { ILogger } from '@thumbnailer/domain';

/** The DB handle injected through DI — the Prisma client. */
export type Db = PrismaType;

interface ProviderDeps {
  logger: ILogger;
  config: { databaseUrl?: string; logLevel?: string };
}

/**
 * Builds the shared Prisma client from config. Registered into the DI container
 * by services that talk to the DB (only db-flush-service today), so the
 * connection is created once and injected through constructors — never `new`d in
 * domain code. The caller registers `$disconnect` with the signals registry.
 */
const dbProvider = ({ config }: ProviderDeps): Db => {
  if (!config?.databaseUrl) {
    throw new Error('config.databaseUrl is required for the db provider');
  }
  return createPrismaClient({ url: config.databaseUrl });
};

export default dbProvider;
