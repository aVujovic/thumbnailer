/**
 * External thumbnail (ffmpeg) process, as given in the assessment task.
 * Launches an external process that reads a video and writes an image. It can
 * fail for many reasons and can run for a long time. We do NOT implement this
 * — a production implementation is assumed; we supply a fake in tests.
 */
export interface IProcess {
  /** Terminate the process. */
  kill(): void;
  /** Block until the process finishes and resolve with its exit status code. */
  waitForCompletion(): Promise<number>;
  /** stderr output of the process (available after completion). */
  getStderr(): string;
}

/**
 * Starts an ffmpeg process that generates a thumbnail for `videoPath` and
 * writes it to `outputPath`. Returns synchronously with a handle; the work
 * happens asynchronously behind `waitForCompletion()`.
 */
export type StartThumbnailProcess = (videoPath: string, outputPath: string) => IProcess;
