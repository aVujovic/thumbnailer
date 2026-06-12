import { readFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { resolve } from 'node:path';
import { z } from 'zod';

/** A tenant's settings. Just a scan root for now; easily extended (output dir,
 *  video extensions, retry policy) per tenant later. */
const TenantSchema = z.object({
  scanRoot: z.string().min(1),
});

/** The tenant catalog: tenantId → settings. */
export const TenantCatalogSchema = z.record(z.string().min(1), TenantSchema);
export type TenantCatalog = z.infer<typeof TenantCatalogSchema>;

/** Expand `~` and resolve to an absolute path (same rules as the services). */
function resolvePath(p: string): string {
  if (p === '~') return homedir();
  if (p.startsWith('~/')) return resolve(homedir(), p.slice(2));
  return resolve(p);
}

/**
 * Resolves a tenantId to an absolute scan root using a catalog. The catalog is
 * the orchestrator's control-plane knowledge — services downstream never see
 * tenant→path mapping, only the resolved path (plus the tenantId for tracking).
 */
export class TenantResolver {
  constructor(private readonly catalog: TenantCatalog) {}

  /** Load and validate a tenants.json file. */
  static fromFile(path: string): TenantResolver {
    let raw: unknown;
    try {
      raw = JSON.parse(readFileSync(path, 'utf-8'));
    } catch (err) {
      throw new Error(`failed to read tenants file '${path}': ${(err as Error).message}`);
    }
    const result = TenantCatalogSchema.safeParse(raw);
    if (!result.success) {
      throw new Error(`invalid tenants file '${path}': ${result.error.issues[0]?.message}`);
    }
    return new TenantResolver(result.data);
  }

  /** Resolve a tenantId to its absolute scan root. Throws if unknown. */
  resolveScanRoot(tenantId: string): string {
    const tenant = this.catalog[tenantId];
    if (!tenant) {
      const known = Object.keys(this.catalog).join(', ') || '(none)';
      throw new Error(`unknown tenant '${tenantId}'. Known tenants: ${known}`);
    }
    return resolvePath(tenant.scanRoot);
  }

  /** List configured tenant ids (for help / validation). */
  tenantIds(): string[] {
    return Object.keys(this.catalog);
  }
}

export default TenantResolver;
