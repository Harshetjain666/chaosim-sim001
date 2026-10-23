/**
 * Damage in a shard's files that recovery must not paper over: a broken
 * sealed segment, a gap in the log, a checkpoint that fails its checksum.
 * A torn final record is not corruption; recovery expects one after a crash.
 */
export class CorruptionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CorruptionError';
  }
}

/**
 * A file this build did not write: one of ours by its magic, but a format
 * version it does not know. That is not damage, so it must never be repaired
 * or replayed, and the message says which version was found.
 */
export class UnsupportedFormatError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'UnsupportedFormatError';
  }
}

export type ClusterErrorCode =
  | 'UNKNOWN_SESSION'
  | 'SHARD_UNAVAILABLE'
  | 'SHARD_FAILED'
  | 'ADVANCE_INTERRUPTED'
  | 'ATTESTATION_EXPIRED'
  | 'BAD_REQUEST'
  | 'MANIFEST_MISMATCH'
  | 'DATA_DIR_LOCKED';

/**
 * An error a caller of the cluster is expected to handle. `retryable` means
 * the request may or may not have been applied and is safe to send again,
 * which holds for every request except `advance`. Ticks are not idempotent,
 * so an advance cut short by a crash fails with ADVANCE_INTERRUPTED instead:
 * some of its ticks may already be durable, and the caller should read where
 * each session stands with `inspect` rather than advance again.
 */
export class ClusterError extends Error {
  readonly code: ClusterErrorCode;
  readonly retryable: boolean;

  constructor(code: ClusterErrorCode, message: string) {
    super(message);
    this.name = 'ClusterError';
    this.code = code;
    this.retryable = code === 'SHARD_UNAVAILABLE';
  }
}
