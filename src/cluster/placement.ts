/**
 * Session placement by rendezvous hashing.
 *
 * Every shard scores the session and the highest score wins. Unlike
 * `hash % shards`, growing from N to N + 1 shards moves only the sessions the
 * new shard now wins, about 1 / (N + 1) of them, instead of nearly all of
 * them. Moving a session means moving its state, so the cheap moment to pick
 * the placement function is before there is any state to move.
 */
export function shardFor(sessionId: string, shardCount: number): number {
  const key = fnv1a32(sessionId);
  let best = 0;
  let bestScore = -1;
  for (let shard = 0; shard < shardCount; shard += 1) {
    const score = fmix32(key ^ Math.imul(shard + 1, 0x9e3779b1));
    if (score > bestScore) {
      best = shard;
      bestScore = score;
    }
  }
  return best;
}

/** 32 bit FNV-1a over UTF-16 code units: stable across processes and platforms. */
function fnv1a32(text: string): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < text.length; i += 1) {
    hash ^= text.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
}

/**
 * MurmurHash3's finalizer. FNV-1a mixes its last input bits weakly, so each
 * shard's score is run through this to make the scores independent.
 */
function fmix32(value: number): number {
  let h = value;
  h ^= h >>> 16;
  h = Math.imul(h, 0x85ebca6b);
  h ^= h >>> 13;
  h = Math.imul(h, 0xc2b2ae35);
  h ^= h >>> 16;
  return h >>> 0;
}
