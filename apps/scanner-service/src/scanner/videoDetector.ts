import type { FileInfo } from '@thumbnailer/domain';

/** Why a file was accepted or skipped — surfaced in logs / metrics. */
export type DetectResult =
  | { video: true }
  | { video: false; reason: 'directory' | 'not-video-ext' | 'empty' | 'unreadable' };

/** Owner-read bit (0o400). Best-effort readability check; not full POSIX uid/gid. */
const OWNER_READ = 0o400;

/**
 * Decides whether a file is a thumbnailable video, cheaply, from FileInfo alone
 * (no content read). Extension-based detection by design — magic-byte sniffing
 * is out of scope (see PLAN). Skips directories, empty files, and files we
 * can't read before we ever spend an ffmpeg process on them.
 */
export class VideoDetector {
  private readonly extensions: ReadonlySet<string>;

  constructor(deps: { config: { videoExtensions: string[] } }) {
    this.extensions = new Set(deps.config.videoExtensions);
  }

  detect(info: FileInfo): DetectResult {
    if (info.isDirectory) return { video: false, reason: 'directory' };
    if (!this.hasVideoExtension(info.name)) return { video: false, reason: 'not-video-ext' };
    if (info.size <= 0) return { video: false, reason: 'empty' };
    if ((info.permissions & OWNER_READ) === 0) return { video: false, reason: 'unreadable' };
    return { video: true };
  }

  isVideo(info: FileInfo): boolean {
    return this.detect(info).video;
  }

  private hasVideoExtension(name: string): boolean {
    const dot = name.lastIndexOf('.');
    if (dot <= 0 || dot === name.length - 1) return false; // no/empty extension
    const ext = name.slice(dot + 1).toLowerCase();
    return this.extensions.has(ext);
  }
}

export default VideoDetector;
