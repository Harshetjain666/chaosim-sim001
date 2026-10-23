import { open, type FileHandle } from 'node:fs/promises';
import { crc32 } from 'node:zlib';
import { UnsupportedFormatError } from './errors.ts';

/**
 * Length and checksum framing for everything a shard writes to disk.
 *
 *   [u32 LE payload length][u32 LE crc32(payload)][payload]
 *
 * A crash can leave the last frame of a file half written. The length tells
 * the reader where a frame should end and the checksum tells it whether the
 * bytes that landed are the bytes that were written, so a torn tail is always
 * detected rather than parsed as garbage.
 */
export const FRAME_HEADER_BYTES = 8;

/**
 * The largest payload a frame carries. A reader takes a longer length for
 * damage, so a writer refuses to produce one rather than write a file that
 * cannot be read back.
 */
export const MAX_PAYLOAD_BYTES = 256 * 1024 * 1024;

export function encodeFrame(payload: Buffer): Buffer {
  const frame = Buffer.allocUnsafe(FRAME_HEADER_BYTES + payload.length);
  payload.copy(frame, FRAME_HEADER_BYTES);
  sealFrame(frame, 0);
  return frame;
}

/**
 * Frame a payload that was written in place: `buf` has FRAME_HEADER_BYTES
 * free at `start` and the payload from there to its end. A checkpoint builds
 * its payload this way so its columns are copied once, not twice.
 */
export function sealFrame(buf: Buffer, start: number): void {
  const payload = buf.subarray(start + FRAME_HEADER_BYTES);
  // readFrame reports an empty or oversized frame as torn, so never write one.
  if (payload.length === 0 || payload.length > MAX_PAYLOAD_BYTES) {
    throw new RangeError(`a frame holds 1 to ${MAX_PAYLOAD_BYTES} bytes, not ${payload.length}`);
  }
  buf.writeUInt32LE(payload.length, start);
  buf.writeUInt32LE(crc32(payload), start + 4);
}

export type FrameRead =
  | { kind: 'frame'; payload: Buffer; next: number }
  | { kind: 'end' }
  | { kind: 'torn' };

/**
 * Read the frame starting at `offset`. Anything that is not a whole, intact
 * frame is reported as torn. That includes a zero length frame: we never
 * write one, and a run of zero bytes (which some filesystems leave behind
 * after a crash) would otherwise pass the checksum, since crc32 of nothing
 * is zero.
 */
export function readFrame(buf: Buffer, offset: number): FrameRead {
  if (offset === buf.length) return { kind: 'end' };
  if (buf.length - offset < FRAME_HEADER_BYTES) return { kind: 'torn' };

  const length = buf.readUInt32LE(offset);
  const checksum = buf.readUInt32LE(offset + 4);
  const start = offset + FRAME_HEADER_BYTES;
  if (length === 0 || length > MAX_PAYLOAD_BYTES || start + length > buf.length) {
    return { kind: 'torn' };
  }

  const payload = buf.subarray(start, start + length);
  if (crc32(payload) !== checksum) return { kind: 'torn' };
  return { kind: 'frame', payload, next: start + length };
}

/**
 * Every file a shard writes opens with an 8 byte magic: five letters naming
 * the kind of file, then a three digit format version. A file whose kind
 * matches but whose version does not was written by another build, so this
 * throws UnsupportedFormatError for it instead of letting it read as damage.
 */
export function assertKnownFormat(buf: Buffer, magic: Buffer, name: string): void {
  const kind = magic.subarray(0, 5);
  if (buf.length < magic.length || !buf.subarray(0, 5).equals(kind)) return;
  const found = buf.subarray(5, magic.length).toString('latin1');
  const expected = magic.subarray(5).toString('latin1');
  if (found !== expected) {
    throw new UnsupportedFormatError(`${name} is format version ${found}; this build reads version ${expected}`);
  }
}

/** Write all of `buf` at the handle's current position, looping over short writes. */
export async function writeFully(handle: FileHandle, buf: Buffer): Promise<void> {
  let offset = 0;
  while (offset < buf.length) {
    const { bytesWritten } = await handle.write(buf, offset, buf.length - offset);
    offset += bytesWritten;
  }
}

/**
 * Make a directory's entries durable. Creating, renaming, or deleting a file
 * is only crash safe once its directory has been synced too.
 */
export async function fsyncDir(dir: string): Promise<void> {
  // Windows cannot open a directory for syncing; NTFS journals the metadata.
  if (process.platform === 'win32') return;
  const handle = await open(dir, 'r');
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}
