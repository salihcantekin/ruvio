// SPDX-License-Identifier: MIT
// Static reply transforms. Each is a plain function so no closure is allocated per call.

import { ReplyError } from './errors.js';
import type {
  FillResult,
  IdemBeginResult,
  ChangeNote,
  ScanPage,
  SortedSetEntry,
  StreamEntry,
  StreamReadResult,
  PoppedEntry,
  ListPopResult,
  AutoClaimResult,
  PendingSummary,
  PendingRow,
} from './types.js';

/** A reply transform. Throwing rejects the call. */
export type Tf = (v: any) => any;

export function str(v: unknown): string {
  if (typeof v === 'string') return v;
  if (v instanceof Uint8Array) return Buffer.from(v.buffer, v.byteOffset, v.byteLength).toString();
  if (v === null || v === undefined) return '';
  return String(v);
}

export const T = {
  bool: (v: unknown): boolean => v === 1 || v === true || v === 'OK',
  okBool: (v: unknown): boolean => v !== null,
  boolArr: (v: unknown[]): boolean[] => {
    const out = new Array<boolean>(v.length);
    for (let i = 0; i < v.length; i++) out[i] = v[i] === 1 || v[i] === true;
    return out;
  },
  float: (v: unknown): number | null => (v === null ? null : toFloat(v)),
  floatArr: (v: unknown[]): (number | null)[] => v.map((x) => (x === null ? null : toFloat(x))),
  balances: (v: unknown): [number, number] | null =>
    v === null ? null : [T.int((v as unknown[])[0]), T.int((v as unknown[])[1])],
  int: (v: unknown): number => (typeof v === 'number' ? v : Number(str(v))),
  intOrNull: (v: unknown): number | null => (v === null ? null : typeof v === 'number' ? v : Number(str(v))),
  token: (v: unknown): string | null => (v === null ? null : String(typeof v === 'bigint' || typeof v === 'number' ? v : str(v))),
  map: toRecord,
  mapArr: (v: unknown[]): Record<string, unknown>[] => (v ?? []).map((x) => toRecord(x)),
  pairs: (v: unknown): [unknown, unknown][] => {
    if (!Array.isArray(v)) return Object.entries(v as object);
    const out: [unknown, unknown][] = [];
    for (let i = 0; i + 1 < v.length; i += 2) out.push([v[i], v[i + 1]]);
    return out;
  },
  scan: (v: unknown[]): ScanPage<unknown> => ({ cursor: str(v[0]), items: (v[1] as unknown[]) ?? [] }),
  hscan: (v: unknown[]): ScanPage<[unknown, unknown]> => ({ cursor: str(v[0]), items: T.pairs(v[1]) }),
  zscan: (v: unknown[]): ScanPage<SortedSetEntry> => ({ cursor: str(v[0]), items: T.zentries(v[1] as unknown[]) }),
  zentries: (v: unknown[]): SortedSetEntry[] => {
    if (v === null) return [];
    const out: SortedSetEntry[] = [];
    // RESP3 may nest [member, score] pairs.
    if (v.length > 0 && Array.isArray(v[0])) {
      for (const p of v as unknown[][]) out.push({ member: p[0] as string, score: toFloat(p[1]) });
      return out;
    }
    for (let i = 0; i + 1 < v.length; i += 2) out.push({ member: v[i] as string, score: toFloat(v[i + 1]) });
    return out;
  },
  zentry: (v: unknown[]): SortedSetEntry | null => {
    if (v === null || v.length === 0) return null;
    return { member: v[0] as string, score: toFloat(v[1]) };
  },
  bzpop: (v: unknown[]): PoppedEntry | null =>
    v === null ? null : { key: v[0] as string, member: v[1] as string, score: toFloat(v[2]) },
  kv: (v: unknown[]): [string, string] | null => (v === null ? null : [v[0] as string, v[1] as string]),
  lmpop: (v: unknown[]): ListPopResult | null =>
    v === null ? null : { key: v[0] as string, elements: (v[1] as string[]) ?? [] },
  stream: (v: unknown[]): StreamEntry[] => (v === null ? [] : v.map(entry)),
  streamOrIds: (v: unknown[]): StreamEntry[] | string[] =>
    v === null ? [] : v.length > 0 && !Array.isArray(v[0]) ? (v as string[]) : v.filter((x) => x !== null).map(entry),
  xread: (v: unknown): StreamReadResult[] | null => {
    if (v === null) return null;
    if (!Array.isArray(v)) {
      // RESP3 map
      return Object.entries(v as Record<string, unknown[]>).map(([key, es]) => ({ key, entries: T.stream(es) }));
    }
    return (v as unknown[][]).map((s) => {
      let es = s[1] as unknown[];
      // A blocked XREAD/XREADGROUP woken by XADD has been observed to answer [key, [id, fields]]
      // (the entry not wrapped in a list); accept both shapes.
      if (es !== null && es.length === 2 && !Array.isArray(es[0]) && Array.isArray(es[1])) es = [es];
      return { key: s[0] as string, entries: T.stream(es) };
    });
  },
  autoclaim: (v: unknown[]): AutoClaimResult => ({
    next: str(v[0]),
    entries: T.streamOrIds(v[1] as unknown[]),
    deleted: ((v[2] as unknown[]) ?? []).map(str),
  }),
  pendingSummary: (v: unknown[]): PendingSummary => ({
    count: v[0] as number,
    smallest: (v[1] as string | null) ?? null,
    greatest: (v[2] as string | null) ?? null,
    consumers: ((v[3] as unknown[][]) ?? []).map((c) => ({ name: c[0] as string, pending: Number(str(c[1])) })),
  }),
  pendingRows: (v: unknown[][]): PendingRow[] =>
    v.map((r) => ({ id: r[0] as string, consumer: r[1] as string, idleMs: r[2] as number, deliveries: r[3] as number })),
  fill: (v: unknown[]): FillResult => {
    if (!Array.isArray(v) || v.length < 2) throw new ReplyError('FILLGET returned an unexpected reply');
    const state = str(v[0]);
    switch (state) {
      case 'HIT':
        return { state: 'hit', value: v[1] as string, token: null };
      case 'FILL':
        return { state: 'fill', value: null, token: T.token(v[1])! };
      case 'BUSY':
        return { state: 'busy', value: null, token: null };
      case 'STALE':
        return { state: 'stale', value: v[1] as string, token: T.token(v[2] ?? null) };
      default:
        throw new ReplyError('FILLGET returned an unexpected state ' + state);
    }
  },
  idem: (v: unknown[]): IdemBeginResult => {
    const state = str(v[0]) as IdemBeginResult['state'];
    return { state, result: state === 'completed' ? (v[1] as string) : null };
  },
  changes: (v: unknown[][]): ChangeNote[] => v.map((r) => ({ seq: r[0] as number, note: str(r[1]) })),
  geopos: (v: unknown[]): ([number, number] | null)[] =>
    v.map((p) => (p === null || (p as unknown[]).length < 2 ? null : [toFloat((p as unknown[])[0]), toFloat((p as unknown[])[1])])),
  bitfield: (v: unknown[]): (number | null)[] => v as (number | null)[],
  jsonBool: (v: unknown): boolean | null => (v === null ? null : v === 1 || v === 'true' || v === true),
  jsonNum: (v: unknown): number | null => (v === null ? null : Number(str(v))),
  strs: (v: unknown[]): string[] => (v === null ? [] : (v as string[])),
  lines: (v: unknown): string => str(v),
};

function toFloat(v: unknown): number {
  if (typeof v === 'number') return v;
  const s = str(v);
  if (s === 'inf' || s === '+inf') return Infinity;
  if (s === '-inf') return -Infinity;
  return Number(s);
}

function entry(e: unknown): StreamEntry {
  const a = e as unknown[];
  const f = a[1] as unknown[] | null;
  // A list, not an object: field names may repeat and their order matters.
  const fields: [string, string][] = [];
  if (f !== null && f !== undefined) for (let i = 0; i + 1 < f.length; i += 2) fields.push([str(f[i]), f[i + 1] as string]);
  return { id: str(a[0]), fields };
}

/** Flat `[k, v, k, v]` arrays (RESP2) or RESP3 maps become a plain object. */
export function toRecord(v: unknown): Record<string, any> {
  if (v === null || v === undefined) return {};
  if (Array.isArray(v)) {
    const o: Record<string, unknown> = {};
    for (let i = 0; i + 1 < v.length; i += 2) o[str(v[i])] = v[i + 1];
    return o;
  }
  if (v instanceof Map) {
    const o: Record<string, unknown> = {};
    for (const [k, x] of v) o[str(k)] = x;
    return o;
  }
  return v as Record<string, unknown>;
}
