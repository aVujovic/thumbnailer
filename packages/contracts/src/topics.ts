/**
 * Topic names shared across services. Single source of truth so the services
 * can never drift on which topic carries what.
 */

/** Video jobs: scanner (producer) -> generator (consumer). */
export const TOPIC_VIDEO_JOBS = 'video-jobs';

/** Commands: orchestrator (producer) -> scanner & generator (consumers). */
export const TOPIC_SCAN_COMMANDS = 'scan-commands';

/** Events: scanner (producer) -> orchestrator / observers (consumers). */
export const TOPIC_SCAN_EVENTS = 'scan-events';

/** Thumbnail ready: generator (producer) -> sync-service (consumer). */
export const TOPIC_THUMBNAIL_READY = 'thumbnail-ready';
