import type { WriteCommand } from '@thumbnailer/contracts';
import type { IVideoRepository, VideoWriteOp } from '../repository/VideoRepository.js';

/**
 * Applies a batch of WriteCommands to the durable store. The sink's only job is
 * COMMAND ROUTING: map each command's `op` to a repository operation, then run
 * them in one transaction. It knows nothing about Prisma/SQL/columns — that's the
 * repository's job. Idempotent (a redelivered batch re-applies cleanly) and
 * atomic (all-or-nothing, so a partial failure redelivers the whole batch).
 */
export interface IDbSink {
  applyBatch(commands: WriteCommand[]): Promise<void>;
  close(): Promise<void>;
}

/** Routes WriteCommands to the video repository and commits them atomically. */
export class DbSink implements IDbSink {
  private readonly repo: IVideoRepository;

  constructor(deps: { videoRepository: IVideoRepository }) {
    this.repo = deps.videoRepository;
  }

  async applyBatch(commands: WriteCommand[]): Promise<void> {
    if (commands.length === 0) return;

    const ops: VideoWriteOp[] = commands.map((cmd) =>
      cmd.op === 'upsert'
        ? this.repo.upsertOp(cmd.row)
        : this.repo.markSyncedOp(cmd.key, cmd.syncedAt),
    );

    // All-or-nothing. A throw → the consumer does NOT commit → the batch redelivers.
    await this.repo.transaction(ops);
  }

  async close(): Promise<void> {
    await this.repo.close();
  }
}

export default DbSink;
