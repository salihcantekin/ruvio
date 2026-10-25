// SPDX-License-Identifier: MIT
// Incremental, resumable RESP2/RESP3 reply parser.

import { RuvioError } from './errors.js';

/** Returned by {@link Parser.next} when the buffered bytes hold no complete reply. */
export const NEED_MORE: unique symbol = Symbol('ruvio.needMore');

/** A RESP3 push frame (`>`), delivered out of band (the client emits it as the `push` event, e.g. for client-side-cache invalidation). `data` is the frame's elements. */
export class PushMessage {
  constructor(readonly data: unknown[]) {}
}

type BufferFast = Buffer & {
  utf8Slice(start: number, end: number): string;
  latin1Slice(start: number, end: number): string;
};
const HAS_FAST = typeof (Buffer.prototype as BufferFast).utf8Slice === 'function';

const EMPTY = Buffer.alloc(0) as BufferFast;

// What one reply may announce, the same in every client: a bulk string up to 64 MiB, an aggregate of up to 1,048,576 elements.
const MAX_BULK_BYTES = 64 * 1024 * 1024;
const MAX_ELEMENTS = 1 << 20;

// Container kinds on the resumable stack.
const K_ARRAY = 0;
const K_MAP = 1;
const K_PUSH = 2;
const K_SET = 3;
const K_ATTR = 4;

interface Frame {
  kind: number;
  arr: unknown[];
  i: number;
}

/**
 * Incremental RESP2/RESP3 reply parser used by the clients. Low-level: most code never needs it.
 *
 * Parses replies out of the bytes fed to it. The parser works in place on the caller's
 * buffer (which may be a reused read buffer); only an unfinished reply tail is copied into
 * an owned accumulator. Nested replies resume at the element where data ran out, so a large
 * array arriving over many reads is parsed in linear time.
 */
export class Parser {
  private buf: BufferFast = EMPTY;
  private pos = 0;
  private end = 0;
  /** Owned buffer holding an unfinished tail; `buf === acc` while in use. */
  private acc: BufferFast = EMPTY;
  private accLen = 0;
  private inAcc = false;
  /** Bytes the pending element needs in total before parsing is worth retrying. */
  private need = 0;
  private stack: Frame[] = [];
  private depth = 0;
  /** When true, bulk strings are returned as Buffer copies. */
  binary = false;

  /** Hands new bytes to the parser. `chunk[0..len)` is only read until the next feed. */
  feed(chunk: Buffer, len: number = chunk.length): void {
    if (this.inAcc) {
      this.append(chunk, len);
      this.buf = this.acc;
      this.pos = 0;
      this.end = this.accLen;
      this.skip = this.accLen < this.need;
    } else {
      this.buf = chunk as BufferFast;
      this.pos = 0;
      this.end = len;
      this.skip = false;
    }
  }

  /** Set after an incomplete element until enough bytes are fed. */
  private skip = false;

  private append(chunk: Buffer, len: number): void {
    const need = this.accLen + len;
    if (need > this.acc.length) {
      const nb = Buffer.allocUnsafe(Math.max(need, this.acc.length * 2, 4096)) as BufferFast;
      this.acc.copy(nb, 0, 0, this.accLen);
      this.acc = nb;
    }
    chunk.copy(this.acc, this.accLen, 0, len);
    this.accLen += len;
  }

  /** Keeps the bytes from `from` on (an unfinished element) for the next feed. */
  private stash(from: number): void {
    if (this.inAcc) {
      // Shift the tail to the start of the accumulator.
      if (from > 0) this.acc.copy(this.acc, 0, from, this.accLen);
      this.accLen -= from;
    } else {
      const len = this.end - from;
      if (len > this.acc.length) this.acc = Buffer.allocUnsafe(Math.max(len * 2, 4096)) as BufferFast;
      this.buf.copy(this.acc, 0, from, this.end);
      this.accLen = len;
      this.inAcc = true;
    }
    this.buf = this.acc;
    this.pos = 0;
    this.end = 0;
  }

  /** True when no unfinished reply is buffered. */
  get idle(): boolean {
    return !this.inAcc && this.depth === 0;
  }

  /** Drops all state (connection reset). */
  reset(): void {
    this.buf = EMPTY;
    this.pos = this.end = 0;
    this.inAcc = false;
    this.accLen = 0;
    this.need = 0;
    this.depth = 0;
    this.skip = false;
    if (this.acc.length > 1 << 20) this.acc = EMPTY;
  }

  /** Index of the CR of the next CRLF at or after `p`, or -1. */
  private eol(p: number): number {
    const b = this.buf;
    const e = this.end - 1;
    for (; p < e; p++) if (b[p] === 13) return p;
    return -1;
  }

  // Parses a signed decimal in [p, e). Returns a number, or a bigint when beyond 2^53.
  private int(p: number, e: number): number | bigint {
    const b = this.buf;
    let neg = false;
    if (b[p] === 45) {
      neg = true;
      p++;
    } else if (b[p] === 43) p++;
    if (e - p > 15) {
      const s = this.buf.latin1Slice ? this.buf.latin1Slice(p, e) : this.buf.toString('latin1', p, e);
      const big = BigInt(s);
      const v = neg ? -big : big;
      if (v >= -9007199254740991n && v <= 9007199254740991n) return Number(v);
      return v;
    }
    let v = 0;
    for (; p < e; p++) {
      const c = b[p]! - 48;
      if (c < 0 || c > 9) throw new RuvioError('ERR protocol: bad integer');
      v = v * 10 + c;
    }
    return neg ? -v : v;
  }

  private text(s: number, e: number): string {
    const b = this.buf;
    if (HAS_FAST) return b.utf8Slice(s, e);
    return b.toString('utf8', s, e);
  }

  private ascii(s: number, e: number): string {
    const b = this.buf;
    if (e - s <= 8) {
      let r = '';
      for (let i = s; i < e; i++) r += String.fromCharCode(b[i]!);
      return r;
    }
    if (HAS_FAST) return b.latin1Slice(s, e);
    return b.toString('latin1', s, e);
  }

  private push(kind: number, n: number): void {
    let f = this.stack[this.depth];
    const arr = new Array(n);
    if (f === undefined) {
      f = { kind, arr, i: 0 };
      this.stack[this.depth] = f;
    } else {
      f.kind = kind;
      f.arr = arr;
      f.i = 0;
    }
    this.depth++;
  }

  /**
   * Returns the next complete top-level reply, or {@link NEED_MORE}. Server errors come back
   * as {@link RuvioError} values (not thrown); RESP3 pushes as {@link PushMessage}.
   */
  next(): unknown {
    if (this.skip) return NEED_MORE;
    for (;;) {
      const start = this.pos;
      if (start >= this.end) {
        if (this.inAcc) {
          this.inAcc = false;
          this.accLen = 0;
        }
        this.buf = EMPTY;
        this.pos = this.end = 0;
        return NEED_MORE;
      }
      const b = this.buf;
      const t = b[start]!;
      let v: unknown;
      // Bulk string: the hottest path.
      if (t === 36) {
        const cr = this.eol(start + 1);
        if (cr < 0) return this.incomplete(start, 0);
        const n = this.int(start + 1, cr) as number;
        if (n > MAX_BULK_BYTES) throw new RuvioError(`ERR protocol: bulk length ${n} is over the limit of ${MAX_BULK_BYTES}`);
        if (n < 0) {
          v = null;
          this.pos = cr + 2;
        } else {
          const s = cr + 2;
          const e = s + n;
          if (e + 2 > this.end) return this.incomplete(start, e + 2 - start);
          if (this.binary) {
            const out = Buffer.allocUnsafe(n);
            b.copy(out, 0, s, e);
            v = out;
          } else {
            v = n === 0 ? '' : this.text(s, e);
          }
          this.pos = e + 2;
        }
      } else if (t === 58) {
        // ':' integer
        const cr = this.eol(start + 1);
        if (cr < 0) return this.incomplete(start, 0);
        v = this.int(start + 1, cr);
        this.pos = cr + 2;
      } else if (t === 43) {
        // '+' simple string
        const cr = this.eol(start + 1);
        if (cr < 0) return this.incomplete(start, 0);
        if (cr - start === 3 && b[start + 1] === 79 && b[start + 2] === 75) v = 'OK';
        else v = this.binary ? Buffer.from(b.subarray(start + 1, cr)) : this.text(start + 1, cr);
        this.pos = cr + 2;
      } else if (t === 42 || t === 37 || t === 62 || t === 126 || t === 124) {
        // '*' array, '%' map, '>' push, '~' set, '|' attribute
        const cr = this.eol(start + 1);
        if (cr < 0) return this.incomplete(start, 0);
        const n = this.int(start + 1, cr) as number;
        if (n > MAX_ELEMENTS) throw new RuvioError(`ERR protocol: element count ${n} is over the limit of ${MAX_ELEMENTS}`);
        this.pos = cr + 2;
        if (n < 0) v = null;
        else {
          const kind = t === 42 ? K_ARRAY : t === 37 ? K_MAP : t === 62 ? K_PUSH : t === 126 ? K_SET : K_ATTR;
          const len = kind === K_MAP || kind === K_ATTR ? n * 2 : n;
          if (len === 0) {
            v = kind === K_MAP ? {} : kind === K_PUSH ? new PushMessage([]) : kind === K_ATTR ? undefined : [];
            if (kind === K_ATTR) continue;
          } else {
            this.push(kind, len);
            continue;
          }
        }
      } else if (t === 45) {
        // '-' error
        const cr = this.eol(start + 1);
        if (cr < 0) return this.incomplete(start, 0);
        v = new RuvioError(this.text(start + 1, cr));
        this.pos = cr + 2;
      } else if (t === 95) {
        // '_' null
        if (start + 3 > this.end) return this.incomplete(start, 3);
        v = null;
        this.pos = start + 3;
      } else if (t === 35) {
        // '#' boolean
        if (start + 4 > this.end) return this.incomplete(start, 4);
        v = b[start + 1] === 116;
        this.pos = start + 4;
      } else if (t === 44) {
        // ',' double
        const cr = this.eol(start + 1);
        if (cr < 0) return this.incomplete(start, 0);
        const s = this.ascii(start + 1, cr);
        v = s === 'inf' ? Infinity : s === '-inf' ? -Infinity : Number(s);
        this.pos = cr + 2;
      } else if (t === 40) {
        // '(' big number
        const cr = this.eol(start + 1);
        if (cr < 0) return this.incomplete(start, 0);
        v = BigInt(this.ascii(start + 1, cr));
        this.pos = cr + 2;
      } else if (t === 33 || t === 61) {
        // '!' bulk error, '=' verbatim string
        const cr = this.eol(start + 1);
        if (cr < 0) return this.incomplete(start, 0);
        const n = this.int(start + 1, cr) as number;
        if (n > MAX_BULK_BYTES) throw new RuvioError(`ERR protocol: bulk length ${n} is over the limit of ${MAX_BULK_BYTES}`);
        const s = cr + 2;
        const e = s + n;
        if (e + 2 > this.end) return this.incomplete(start, e + 2 - start);
        if (t === 33) v = new RuvioError(this.text(s, e));
        else v = this.binary ? Buffer.from(b.subarray(s + 4, e)) : this.text(s + 4, e);
        this.pos = e + 2;
      } else {
        throw new RuvioError('ERR protocol: unexpected reply type byte ' + t);
      }

      // Attach to open containers; emit once the outermost completes.
      while (this.depth > 0) {
        const f = this.stack[this.depth - 1]!;
        f.arr[f.i++] = v;
        if (f.i < f.arr.length) break;
        this.depth--;
        const arr = f.arr;
        f.arr = null as unknown as unknown[];
        if (f.kind === K_ARRAY || f.kind === K_SET) v = arr;
        else if (f.kind === K_PUSH) v = new PushMessage(arr);
        else if (f.kind === K_MAP) v = toMap(arr);
        else {
          v = undefined; // attribute: dropped, not an element of the parent
          break;
        }
      }
      if (this.depth === 0 && v !== undefined) return v;
    }
  }

  private incomplete(from: number, need: number): typeof NEED_MORE {
    this.stash(from);
    this.need = need;
    this.skip = true;
    return NEED_MORE;
  }
}

function toMap(arr: unknown[]): unknown {
  // RESP3 maps become plain objects when every key is a string, otherwise a Map.
  let plain = true;
  for (let i = 0; i < arr.length; i += 2) if (typeof arr[i] !== 'string') { plain = false; break; }
  if (plain) {
    const o: Record<string, unknown> = {};
    for (let i = 0; i < arr.length; i += 2) o[arr[i] as string] = arr[i + 1];
    return o;
  }
  const m = new Map<unknown, unknown>();
  for (let i = 0; i < arr.length; i += 2) m.set(arr[i], arr[i + 1]);
  return m;
}
