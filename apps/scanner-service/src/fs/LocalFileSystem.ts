import { readdirSync, lstatSync } from 'node:fs';
import { join, basename } from 'node:path';
import type { FileInfo, IFileSystem } from '@thumbnailer/domain';

/**
 * A POSIX-backed IFileSystem over the local disk.
 *
 * The task states a production IFileSystem implementation is assumed (local or
 * remote) — this is the local stand-in so the demo runs against real files.
 * In production this is swapped for the real adapter; nothing else changes,
 * because everything depends on the IFileSystem interface, not this class.
 */
export class LocalFileSystem implements IFileSystem {
  listFiles(path: string): string[] {
    return readdirSync(path).map((name) => join(path, name));
  }

  openFile(path: string): FileInfo {
    // lstat (not stat): do NOT follow symlinks. A followed symlink that points
    // back at an ancestor (common in node_modules / .app bundles) turns the walk
    // into an effectively infinite tree. With lstat a symlink reports as neither
    // a directory nor a regular file, so the walk simply skips it — exactly what
    // `find` does by default. The depth guard is only a backstop.
    const st = lstatSync(path);
    return {
      name: basename(path),
      isDirectory: st.isDirectory(),
      size: st.size,
      permissions: st.mode & 0o777,
    };
  }
}

export default LocalFileSystem;
