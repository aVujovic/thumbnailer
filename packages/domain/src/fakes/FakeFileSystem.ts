import type { FileInfo, IFileSystem } from '../filesystem.js';

/** A node in the fake tree. Directories have `children`; files have `size`. */
export interface FakeEntry {
  /** POSIX permission bits. Default 0o644 (file) / 0o755 (dir). */
  permissions?: number;
  /** File size in bytes (files only). Default 1. */
  size?: number;
  /** Child paths -> entries (directories only). Presence marks a directory. */
  children?: Record<string, FakeEntry>;
  /**
   * Extra absolute paths returned by `listFiles` in addition to `children`.
   * Used to simulate symlinks — point one back at an ancestor to make a cycle.
   */
  links?: string[];
  /** When set, `listFiles(path)` throws this (simulates permission denied). */
  listError?: Error;
  /** When set, `openFile(path)` throws this (simulates TOCTOU / stat error). */
  openError?: Error;
}

/**
 * In-memory IFileSystem for tests. Built from a nested tree keyed by absolute
 * path segments. Supports the edge cases the scanner must survive:
 *
 *   - permission errors on listFiles / openFile (per-entry)
 *   - TOCTOU: an entry listed by its parent but missing on openFile
 *   - cycles: point a directory's child back at an ancestor path
 *
 * Paths are POSIX-style with '/' separators.
 */
export class FakeFileSystem implements IFileSystem {
  /** Flat path -> entry index, built once from the nested root. */
  private readonly byPath = new Map<string, FakeEntry>();

  constructor(
    root: Record<string, FakeEntry>,
    private readonly rootPath = '',
  ) {
    this.index(rootPath, { children: root });
  }

  // Iterative (not recursive) so building a very deep test tree can't overflow
  // the stack — the fake must survive the same depths the scanner is tested at.
  private index(rootPath: string, root: FakeEntry): void {
    const stack: Array<{ path: string; entry: FakeEntry }> = [{ path: rootPath, entry: root }];
    while (stack.length > 0) {
      const { path, entry } = stack.pop()!;
      this.byPath.set(path || '/', entry);
      for (const [name, child] of Object.entries(entry.children ?? {})) {
        stack.push({ path: `${path}/${name}`, entry: child });
      }
    }
  }

  listFiles(path: string): string[] {
    const entry = this.byPath.get(path || '/');
    if (!entry) throw new Error(`ENOENT: no such directory '${path}'`);
    if (entry.listError) throw entry.listError;
    if (!entry.children) throw new Error(`ENOTDIR: not a directory '${path}'`);
    const childPaths = Object.keys(entry.children).map((name) => `${path}/${name}`);
    return [...childPaths, ...(entry.links ?? [])];
  }

  openFile(path: string): FileInfo {
    const entry = this.byPath.get(path || '/');
    if (!entry) throw new Error(`ENOENT: no such file '${path}'`);
    if (entry.openError) throw entry.openError;
    const isDirectory = entry.children !== undefined;
    const name = path.split('/').pop() ?? path;
    return {
      name,
      isDirectory,
      permissions: entry.permissions ?? (isDirectory ? 0o755 : 0o644),
      size: entry.size ?? (isDirectory ? 0 : 1),
    };
  }
}

export default FakeFileSystem;
