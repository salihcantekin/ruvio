// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

/**
 * How long a blocking command may legitimately wait, so the command timeout can add it. The rule is
 * the same in every Ruvio client: a command that carries its own wait gets `wait + commandTimeoutMs`;
 * a wait of `0` (block forever) or an argument that does not parse means no deadline for that call.
 *
 * - `BLPOP`, `BRPOP`, `BZPOPMIN`, `BZPOPMAX`, `BRPOPLPUSH`, `BLMOVE`: last argument, seconds.
 * - `BLMPOP`, `BZMPOP`: first argument, seconds.
 * - `XREAD`, `XREADGROUP`: `BLOCK ms` (absent: not blocking).
 * - `WAIT`, `WAITAOF`: last argument, milliseconds.
 * - `FILLGET key fillMs WAIT waitMs`: `waitMs`.
 * - `SUBSCRIBE`, `PSUBSCRIBE`, `SSUBSCRIBE`, `MONITOR`: push mode, no deadline.
 *
 * @internal
 */

const NAMES = new Set([
  'BLPOP', 'BRPOP', 'BZPOPMIN', 'BZPOPMAX', 'BRPOPLPUSH', 'BLMOVE', 'BLMPOP', 'BZMPOP', 'XREAD', 'XREADGROUP',
  'WAIT', 'WAITAOF', 'FILLGET', 'MONITOR', 'SUBSCRIBE', 'PSUBSCRIBE', 'SSUBSCRIBE',
]);

/** Whether a command name may carry a blocking wait; checked once per {@link cmd}, not per call. */
export function mayBlock(name: string): boolean {
  return NAMES.has(name.toUpperCase());
}

/** The wait of a command that blocks with no bound. */
export const WAIT_FOREVER = -1;

// Scan state of the last argument read: its offset and length in the buffer.
let ARG_OFF = 0;
let ARG_LEN = 0;

/** Reads `<prefix><digits>\r\n` at `p`; returns the offset after it (or -1) and leaves the number in NUM. */
let NUM = 0;
function header(b: Uint8Array, p: number, e: number, prefix: number): number {
  if (p >= e || b[p] !== prefix) return -1;
  let n = 0;
  let i = p + 1;
  while (i < e) {
    const c = b[i]!;
    if (c < 48 || c > 57) break;
    n = n * 10 + (c - 48);
    if (n > 0x3fffffff) return -1;
    i++;
  }
  if (i === p + 1 || i + 1 >= e || b[i] !== 13 || b[i + 1] !== 10) return -1;
  NUM = n;
  return i + 2;
}

/** Reads one bulk string at `p` into ARG_OFF / ARG_LEN; returns the offset after it, or -1. */
function bulk(b: Uint8Array, p: number, e: number): number {
  const q = header(b, p, e, 36 /* $ */);
  if (q < 0 || q + NUM + 2 > e) return -1;
  ARG_OFF = q;
  ARG_LEN = NUM;
  return q + NUM + 2;
}

/** Case-insensitive compare of the last argument with an upper-case ASCII word. */
function argIs(b: Uint8Array, word: string): boolean {
  if (ARG_LEN !== word.length) return false;
  for (let i = 0; i < ARG_LEN; i++) {
    const c = b[ARG_OFF + i]!;
    if ((c >= 97 && c <= 122 ? c - 32 : c) !== word.charCodeAt(i)) return false;
  }
  return true;
}

function argText(b: Uint8Array): string {
  let s = '';
  for (let i = 0; i < ARG_LEN; i++) s += String.fromCharCode(b[ARG_OFF + i]!);
  return s;
}

function seconds(b: Uint8Array): number {
  const t = argText(b);
  // Each alternative matches the digits before a dot once, so the check is linear in the
  // (already bounded) argument length.
  const v = /^\+?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/.test(t) ? Number(t) : NaN;
  return v > 0 && Number.isFinite(v) ? v * 1000 : WAIT_FOREVER;
}

function millis(b: Uint8Array): number {
  const t = argText(b);
  const v = /^\+?\d+$/.test(t) ? Number(t) : NaN;
  return v > 0 && Number.isFinite(v) ? v : WAIT_FOREVER;
}

// Set by commandWait: the offset after the command, or -1 when the bytes do not parse.
let NEXT = -1;

/**
 * The wait (ms, 0 when not blocking, {@link WAIT_FOREVER} for none) of the RESP command at `p`, and the
 * offset after it in NEXT. A plain command costs a length and first-byte check and allocates nothing.
 */
function commandWait(b: Uint8Array, p: number, e: number): number {
  let q = header(b, p, e, 42 /* * */);
  if (q < 0 || NUM < 1) return (NEXT = -1), 0;
  let n = NUM - 1;
  q = bulk(b, q, e);
  if (q < 0) return (NEXT = -1), 0;
  const len = ARG_LEN;
  const f = b[ARG_OFF]! | 0x20;
  // Length and first letter of every name below; GET, SET, PING, ... stop here.
  let candidate = false;
  switch (len) {
    case 4: candidate = f === 119; break; // w
    case 5: candidate = f === 98 || f === 120; break; // b x
    case 6: case 8: candidate = f === 98; break;
    case 7: candidate = f === 119 || f === 102 || f === 109; break; // w f m
    case 9: candidate = f === 115; break; // s
    case 10: candidate = f === 98 || f === 120 || f === 112 || f === 115; break;
  }
  let kind = 0; // 1 last-arg seconds, 2 first-arg seconds, 3 XREAD, 4 XREADGROUP, 5 last-arg ms, 6 FILLGET, 7 push
  if (candidate) {
    if (argIs(b, 'BLPOP') || argIs(b, 'BRPOP') || argIs(b, 'BZPOPMIN') || argIs(b, 'BZPOPMAX') || argIs(b, 'BRPOPLPUSH') || argIs(b, 'BLMOVE')) kind = 1;
    else if (argIs(b, 'BLMPOP') || argIs(b, 'BZMPOP')) kind = 2;
    else if (argIs(b, 'XREAD')) kind = 3;
    else if (argIs(b, 'XREADGROUP')) kind = 4;
    else if (argIs(b, 'WAIT') || argIs(b, 'WAITAOF')) kind = 5;
    else if (argIs(b, 'FILLGET')) kind = 6;
    else if (argIs(b, 'MONITOR') || argIs(b, 'SUBSCRIBE') || argIs(b, 'PSUBSCRIBE') || argIs(b, 'SSUBSCRIBE')) kind = 7;
  }
  if (kind === 0) {
    for (; n > 0; n--) if ((q = bulk(b, q, e)) < 0) return (NEXT = -1), 0;
    NEXT = q;
    return 0;
  }
  let wait = kind === 7 ? WAIT_FOREVER : 0;
  if ((kind === 1 || kind === 2 || kind === 5) && n === 0) wait = WAIT_FOREVER;
  let wantNext = false; // XREAD: the argument after BLOCK; FILLGET: after WAIT
  let done = false;
  for (let i = 0; i < n; i++) {
    if ((q = bulk(b, q, e)) < 0) return (NEXT = -1), WAIT_FOREVER;
    if (kind === 1 || kind === 5) {
      if (i === n - 1) wait = kind === 1 ? seconds(b) : millis(b);
    } else if (kind === 2) {
      if (i === 0) wait = seconds(b);
    } else if (kind === 3 || kind === 4) {
      if (done || (kind === 4 && i < 3)) continue; // XREADGROUP GROUP <group> <consumer>
      if (wantNext) {
        wait = millis(b);
        done = true;
      } else if (argIs(b, 'STREAMS')) done = true;
      else if (argIs(b, 'BLOCK')) wantNext = true;
    } else if (kind === 6) {
      if (i === 2) wantNext = argIs(b, 'WAIT');
      else if (i === 3 && wantNext) wait = millis(b);
    }
  }
  NEXT = q;
  return wait;
}

/** How many commands the last {@link blockingWait} call saw. */
export let FRAME_COUNT = 0;

/**
 * The widest wait of the commands in `b[s..e)` (one command or a pipeline): ms, 0 when none blocks,
 * {@link WAIT_FOREVER} when one blocks with no bound. Leaves the number of commands in {@link FRAME_COUNT}.
 */
export function blockingWait(b: Uint8Array, s: number, e: number): number {
  let widest = 0;
  let count = 0;
  let p = s;
  while (p < e) {
    const w = commandWait(b, p, e);
    count++;
    if (w === WAIT_FOREVER) widest = WAIT_FOREVER;
    else if (widest !== WAIT_FOREVER && w > widest) widest = w;
    if (NEXT < 0) break;
    p = NEXT;
  }
  FRAME_COUNT = count;
  return widest;
}
