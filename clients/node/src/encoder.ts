// SPDX-License-Identifier: MIT
// RESP request encoder: writes frames straight into a reusable slab Buffer.

import { mayBlock } from './blocking.js';

/** A command argument for {@link Commands.call}: strings are sent as UTF-8, Buffers as raw bytes, numbers and bigints as ASCII digits. */
export type Arg = string | Buffer | number | bigint;

/** A pre-encoded command name (one or more words) plus how many words it holds. Create with {@link cmd}. */
export interface Cmd {
  /** `$3\r\nGET\r\n` (and further words for subcommands). */
  readonly frag: Buffer;
  /** Number of RESP bulk words in `frag`. */
  readonly words: number;
  /** Upper-case name, for errors and diagnostics. */
  readonly name: string;
  /** `*N\r\n` + frag, cached per total argument count. */
  readonly pre: (Uint8Array | undefined)[];
  /** @internal The command may carry a blocking wait (see `blocking.ts`); set by {@link cmd}. */
  readonly block?: boolean;
}

/**
 * Pre-encodes a command name (or command plus subcommand words) as RESP bulk strings, for use with
 * {@link Commands.callCmd}. Do it once at start-up and reuse the result.
 *
 * @param words The command name, then any subcommand words: `cmd('GET')`, `cmd('CLIENT', 'ID')`.
 * @returns The reusable {@link Cmd}.
 *
 * @example
 * ```ts
 * const GET = cmd('GET');
 * const value = await client.callCmd(GET, 'user:1');
 * ```
 */
export function cmd(...words: string[]): Cmd {
  let s = '';
  for (const w of words) s += '$' + Buffer.byteLength(w) + '\r\n' + w + '\r\n';
  const name = words.join(' ');
  return { frag: Buffer.from(s, 'latin1'), words: words.length, name, pre: [], block: mayBlock(words[0] ?? '') };
}

const SLAB = 64 * 1024;
const BIG_ARG = 64 * 1024; // Buffers at least this big are written zero-copy when a sink exists.

// Pre-encoded `*N\r\n` headers for small arrays.
const ARR: Buffer[] = [];
for (let i = 0; i < 64; i++) ARR.push(Buffer.from('*' + i + '\r\n', 'latin1'));

type BufferFast = Buffer & {
  utf8Write(s: string, offset: number, max?: number): number;
  latin1Write(s: string, offset: number, max?: number): number;
};
const proto = Buffer.prototype as BufferFast;
const HAS_FAST = typeof proto.utf8Write === 'function';

function digits(n: number): number {
  // n >= 0, safe integer
  if (n < 10) return 1;
  if (n < 100) return 2;
  if (n < 1000) return 3;
  if (n < 10000) return 4;
  if (n < 100000) return 5;
  if (n < 1000000) return 6;
  if (n < 10000000) return 7;
  if (n < 100000000) return 8;
  if (n < 1000000000) return 9;
  let d = 10;
  n = Math.floor(n / 10000000000);
  while (n > 0) {
    d++;
    n = Math.floor(n / 10);
  }
  return d;
}

/**
 * Growable RESP request writer used by the clients. Low-level: most code never needs it.
 *
 * Growable request writer. `frameStart..pos` is the frame being written; `start..frameStart`
 * holds complete frames not yet handed to the sink. With a sink, a full slab is flushed and a
 * fresh slab is taken (old regions are never overwritten, so `subarray` hand-off is zero copy).
 */
export class Encoder {
  buf: BufferFast;
  pos = 0;
  start = 0;
  frameStart = 0;
  /** Receives completed byte ranges (the socket). Absent for pipeline-local encoders. */
  sink: ((chunk: Buffer) => void) | null;
  /**
   * @internal Sees a completed frame `buf[s..e)` that may hold a blocking command (the command timeout adds
   * its wait). Only frames begun with a {@link Cmd} whose `block` is set reach it, so other commands cost nothing.
   */
  onFrame: ((buf: Uint8Array, s: number, e: number) => void) | null = null;
  /** @internal A command begun since the last {@link endFrame} (or {@link take}) may block. */
  mayBlock = false;

  constructor(sink: ((chunk: Buffer) => void) | null = null, size = SLAB) {
    this.buf = Buffer.allocUnsafe(size) as BufferFast;
    this.sink = sink;
  }

  /** Bytes encoded but not yet handed to the sink. */
  get pending(): number {
    return this.pos - this.start;
  }

  /** Makes room for `n` more bytes in the current frame. */
  ensure(n: number): void {
    if (this.pos + n <= this.buf.length) return;
    const sink = this.sink;
    if (sink !== null) {
      if (this.frameStart > this.start) sink(this.buf.subarray(this.start, this.frameStart));
      const partial = this.pos - this.frameStart;
      const size = Math.max(SLAB, (partial + n) * 2);
      const nb = Buffer.allocUnsafe(size) as BufferFast;
      this.buf.copy(nb, 0, this.frameStart, this.pos);
      this.buf = nb;
      this.start = 0;
      this.frameStart = 0;
      this.pos = partial;
    } else {
      const used = this.pos - this.start;
      const size = Math.max(SLAB, (used + n) * 2);
      const nb = Buffer.allocUnsafe(size) as BufferFast;
      this.buf.copy(nb, 0, this.start, this.pos);
      this.frameStart -= this.start;
      this.buf = nb;
      this.start = 0;
      this.pos = used;
    }
  }

  /** Hands every complete frame to the sink. */
  flush(): void {
    if (this.pos > this.start && this.sink !== null) {
      const chunk = this.buf.subarray(this.start, this.pos);
      this.start = this.frameStart = this.pos;
      if (this.buf.length - this.pos < 512) {
        this.buf = Buffer.allocUnsafe(SLAB) as BufferFast;
        this.start = this.frameStart = this.pos = 0;
      }
      this.sink(chunk);
    }
  }

  /** Takes the encoded bytes (pipeline-local encoders) and resets. */
  take(): Buffer {
    this.mayBlock = false;
    const out = this.buf.subarray(this.start, this.pos);
    this.buf = Buffer.allocUnsafe(SLAB) as BufferFast;
    this.start = this.frameStart = this.pos = 0;
    return out;
  }

  /** Set when part of the current frame already went to the sink (zero-copy argument). */
  partial = false;

  /** Discards the frame being written (encode failure). False when part of it was already sent. */
  abortFrame(): boolean {
    this.pos = this.frameStart;
    const ok = !this.partial;
    this.partial = false;
    return ok;
  }

  /** Marks the current frame complete. */
  endFrame(): void {
    if (this.mayBlock && this.onFrame !== null) {
      this.mayBlock = false;
      this.onFrame(this.buf, this.frameStart, this.pos);
    }
    this.frameStart = this.pos;
    this.partial = false;
  }

  /** Writes `*count\r\n` and the pre-encoded command words (one cached copy for common arities). */
  begin(c: Cmd, argc: number): void {
    if (c.block === true) this.mayBlock = true;
    let pre = argc < 8 ? c.pre[argc] : undefined;
    if (pre === undefined) {
      const total = c.words + argc;
      const h = Buffer.from('*' + total + '\r\n', 'latin1');
      pre = new Uint8Array(h.length + c.frag.length);
      pre.set(h, 0);
      pre.set(c.frag, h.length);
      if (argc < 8) c.pre[argc] = pre;
    }
    const n = pre.length;
    if (this.pos + n > this.buf.length) this.ensure(n);
    const b = this.buf;
    const p = this.pos;
    if (n <= 24) for (let i = 0; i < n; i++) b[p + i] = pre[i]!;
    else b.set(pre, p);
    this.pos = p + n;
  }

  /** Writes a raw pre-encoded fragment (e.g. an option word). */
  frag(f: Buffer): void {
    this.ensure(f.length);
    const b = this.buf;
    const p = this.pos;
    if (f.length <= 16) for (let i = 0; i < f.length; i++) b[p + i] = f[i]!;
    else f.copy(b, p);
    this.pos = p + f.length;
  }

  // Writes non-negative safe integer digits at p, returns new position. Space must exist.
  private uint(n: number, p: number): number {
    const d = digits(n);
    const b = this.buf;
    let q = p + d - 1;
    if (n < 4294967296) {
      let v = n >>> 0;
      do {
        const r = (v / 10) >>> 0;
        b[q--] = 48 + v - r * 10;
        v = r;
      } while (v !== 0);
    } else {
      let v = n;
      do {
        const r = Math.floor(v / 10);
        b[q--] = 48 + (v - r * 10);
        v = r;
      } while (v !== 0);
    }
    return p + d;
  }

  /** Writes one bulk argument. */
  arg(a: Arg): void {
    if (typeof a === 'string') this.str(a);
    else if (typeof a === 'number') this.num(a);
    else if (a instanceof Uint8Array) this.bytes(a as Buffer);
    else if (typeof a === 'bigint') this.ascii(a.toString());
    else throw (a as unknown) instanceof Error ? a : new TypeError('ruvio: unsupported argument type ' + typeof a);
  }

  /** Writes a string argument as UTF-8 without measuring it first (write, then fix header). */
  str(s: string): void {
    const len = s.length;
    if (len > 4096 || !HAS_FAST) {
      const n = Buffer.byteLength(s);
      this.ensure(n + 16);
      let p = this.pos;
      const b = this.buf;
      b[p++] = 36;
      p = this.uint(n, p);
      b[p++] = 13; b[p++] = 10;
      p += b.write(s, p, n, 'utf8');
      b[p++] = 13; b[p++] = 10;
      this.pos = p;
      return;
    }
    if (len <= 48) {
      // Short strings: copy char codes while they are ASCII, no native call.
      if (this.pos + len + 16 > this.buf.length) this.ensure(len * 3 + 16);
      const b = this.buf;
      let p = this.pos;
      b[p++] = 36;
      if (len < 10) b[p++] = 48 + len;
      else {
        const t = (len / 10) | 0;
        b[p++] = 48 + t;
        b[p++] = 48 + len - t * 10;
      }
      b[p++] = 13;
      b[p++] = 10;
      let i = 0;
      for (; i < len; i++) {
        const ch = s.charCodeAt(i);
        if (ch > 127) break;
        b[p + i] = ch;
      }
      if (i === len) {
        p += len;
        b[p++] = 13;
        b[p++] = 10;
        this.pos = p;
        return;
      }
      // Non-ASCII: fall through to the UTF-8 path (nothing committed yet).
    }
    const max = len * 3;
    this.ensure(max + 16);
    const b = this.buf;
    const p = this.pos;
    const hd = digits(max);
    const body = p + hd + 3;
    const n = len === 0 ? 0 : b.utf8Write(s, body, max);
    const d = digits(n);
    let q: number;
    if (d === hd) {
      q = p + 1;
    } else {
      const nb = p + d + 3;
      b.copyWithin(nb, body, body + n);
      q = p + 1;
    }
    b[p] = 36; // '$'
    q = this.uint(n, q);
    b[q++] = 13; b[q++] = 10;
    q += n;
    b[q++] = 13; b[q++] = 10;
    this.pos = q;
  }

  /** Writes a latin1/ASCII string argument (option words, formatted numbers). */
  ascii(s: string): void {
    const n = s.length;
    this.ensure(n + 16);
    let p = this.pos;
    const b = this.buf;
    b[p++] = 36;
    p = this.uint(n, p);
    b[p++] = 13; b[p++] = 10;
    for (let i = 0; i < n; i++) b[p + i] = s.charCodeAt(i);
    p += n;
    b[p++] = 13; b[p++] = 10;
    this.pos = p;
  }

  /** Writes a number argument: integers as ASCII digits with no string allocation. */
  num(v: number): void {
    if (Number.isSafeInteger(v)) {
      this.ensure(32);
      const b = this.buf;
      let p = this.pos;
      const neg = v < 0;
      const a = neg ? -v : v;
      const d = digits(a) + (neg ? 1 : 0);
      b[p++] = 36;
      p = this.uint(d, p);
      b[p++] = 13; b[p++] = 10;
      if (neg) b[p++] = 45;
      p = this.uint(a, p);
      b[p++] = 13; b[p++] = 10;
      this.pos = p;
    } else if (v === Infinity) this.ascii('+inf');
    else if (v === -Infinity) this.ascii('-inf');
    else if (Number.isNaN(v)) throw new TypeError('ruvio: NaN is not a valid argument');
    else this.ascii(String(v));
  }

  /** Writes a binary argument. Large buffers go to the sink without a copy. */
  bytes(a: Uint8Array): void {
    const n = a.length;
    this.ensure(16 + (n >= BIG_ARG && this.sink !== null ? 0 : n));
    let p = this.pos;
    const b = this.buf;
    b[p++] = 36;
    p = this.uint(n, p);
    b[p++] = 13; b[p++] = 10;
    if (n >= BIG_ARG && this.sink !== null) {
      // Header goes out with every earlier frame, then the caller's bytes as-is.
      this.pos = p;
      this.partial = true;
      const sink = this.sink;
      sink(this.buf.subarray(this.start, p));
      this.start = p;
      // The current frame now continues after the zero-copy payload; earlier bytes are sent.
      this.frameStart = p;
      sink(a instanceof Buffer ? a : Buffer.from(a.buffer, a.byteOffset, a.byteLength));
      this.ensure(2);
      const b2 = this.buf;
      b2[this.pos++] = 13;
      b2[this.pos++] = 10;
      return;
    }
    if (n <= 32) for (let i = 0; i < n; i++) b[p + i] = a[i]!;
    else b.set(a, p);
    p += n;
    b[p++] = 13; b[p++] = 10;
    this.pos = p;
  }
}
