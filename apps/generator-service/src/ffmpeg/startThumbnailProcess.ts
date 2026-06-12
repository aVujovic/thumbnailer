import { spawn, type ChildProcess } from 'node:child_process';
import { writeFileSync } from 'node:fs';
import type { IProcess, StartThumbnailProcess } from '@thumbnailer/domain';

/** ffmpeg-related knobs, sourced from the service config (env-driven). */
export interface FfmpegOptions {
  /** Path / name of the ffmpeg binary. Default 'ffmpeg' (on PATH). */
  ffmpegPath: string;
  /** Timestamp to grab the frame at (e.g. '00:00:01'). */
  thumbnailTimestamp: string;
  /** Signal used to kill a hung process. */
  killSignal: NodeJS.Signals;
}

/**
 * Builds the production stand-in for the `startThumbnailProcess` the task
 * assumes exists. All ffmpeg knobs come from config (env), nothing hardcoded.
 *
 * If the ffmpeg binary is on PATH it really runs it (grabs a frame at the
 * configured timestamp). If not, it falls back to writing a tiny placeholder
 * file so the demo still completes end-to-end. Either way it satisfies the
 * IProcess contract, so the ThumbnailGenerator's timeout/kill/retry logic is
 * exercised identically.
 */
export function makeStartThumbnailProcess(opts: FfmpegOptions): StartThumbnailProcess {
  return (videoPath, outputPath): IProcess => {
    let child: ChildProcess | null = null;
    let stderr = '';
    let completion: Promise<number>;

    try {
      child = spawn(
        opts.ffmpegPath,
        ['-y', '-i', videoPath, '-ss', opts.thumbnailTimestamp, '-vframes', '1', outputPath],
        { stdio: ['ignore', 'ignore', 'pipe'] },
      );
    } catch {
      child = null;
    }

    if (child) {
      const proc = child;
      proc.stderr?.on('data', (d) => (stderr += String(d)));
      completion = new Promise<number>((resolve) => {
        proc.on('close', (code) => resolve(code ?? 0));
        proc.on('error', () => {
          // ffmpeg not installed / failed to spawn — fall back to a placeholder.
          try {
            writeFileSync(outputPath, placeholder(videoPath));
            resolve(0);
          } catch (err) {
            stderr += String(err);
            resolve(1);
          }
        });
      });
    } else {
      // spawn threw synchronously — placeholder fallback.
      completion = Promise.resolve(0);
      try {
        writeFileSync(outputPath, placeholder(videoPath));
      } catch (err) {
        stderr += String(err);
        completion = Promise.resolve(1);
      }
    }

    return {
      kill: () => child?.kill(opts.killSignal),
      waitForCompletion: () => completion,
      getStderr: () => stderr,
    };
  };
}

function placeholder(videoPath: string): string {
  return `thumbnail-placeholder for ${videoPath}\n(ffmpeg not available — see startThumbnailProcess.ts)\n`;
}

export default makeStartThumbnailProcess;
