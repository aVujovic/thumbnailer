/**
 * Abstract file system, as given in the assessment task. May be backed by a
 * local or remote POSIX-like file system. We do NOT implement this — a
 * production implementation is assumed. We program against the interface and
 * supply a fake in tests.
 */
export interface FileInfo {
  /** Base name of the entry (not the full path). */
  name: string;
  /** POSIX permission bits (e.g. 0o644). */
  permissions: number;
  /** Size in bytes. */
  size: number;
  /** True when the entry is a directory. */
  isDirectory: boolean;
}

export interface IFileSystem {
  /** Return the list of child file paths directly under `path`. */
  listFiles(path: string): string[];
  /** Return information about the entry at `path`. */
  openFile(path: string): FileInfo;
}
