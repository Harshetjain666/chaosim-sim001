/**
 * mulberry32: a tiny, fast, fully deterministic PRNG.
 *
 * The engine itself is deterministic and never calls Math.random(). The chaos
 * load test needs a little jitter to make the synthetic keystroke stream look
 * organic, but that jitter has to be reproducible so the same seed always
 * yields the same trajectory (and therefore the same attestation given a fixed
 * completion timestamp). A seeded generator gives us both.
 */
export function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return function next(): number {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
