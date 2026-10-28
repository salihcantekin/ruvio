// SPDX-License-Identifier: MIT
// Pre-encoded keyword tables: a keyword given as a string is looked up once and the stored Buffer is
// copied into the frame, so no keyword is scanned or encoded per call. Unknown spellings fall through
// to the normal string path unchanged.

const table = (words: readonly string[]): ReadonlyMap<string, Buffer> => {
  const m = new Map<string, Buffer>();
  for (const w of words) {
    m.set(w, Buffer.from(w, 'latin1'));
    const l = w.toLowerCase();
    m.set(l, Buffer.from(l, 'latin1'));
  }
  return m;
};

/** `EXPIRE` conditions. */
export const KW_EXPIRE_COND = table(['NX', 'XX', 'GT', 'LT']);
/** `GEO` distance units. */
export const KW_GEO_UNIT = table(['M', 'KM', 'FT', 'MI']);
/** `ASC` / `DESC`. */
export const KW_ORDER = table(['ASC', 'DESC']);
/** `LEFT` / `RIGHT`. */
export const KW_SIDE = table(['LEFT', 'RIGHT']);
/** `BEFORE` / `AFTER`. */
export const KW_WHERE = table(['BEFORE', 'AFTER']);
/** `AND` / `OR` / `XOR` / `NOT`. */
export const KW_BITOP = table(['AND', 'OR', 'XOR', 'NOT']);
/** `BYTE` / `BIT`. */
export const KW_BITUNIT = table(['BYTE', 'BIT']);
/** `SUM` / `MIN` / `MAX`. */
export const KW_AGGREGATE = table(['SUM', 'MIN', 'MAX']);
/** `WRAP` / `SAT` / `FAIL`. */
export const KW_OVERFLOW = table(['WRAP', 'SAT', 'FAIL']);
/** `SCAN ... TYPE` names (lower case, as the server reports them). */
export const KW_SCAN_TYPE = table([
  'string', 'json', 'list', 'set', 'hash', 'zset', 'bloomfilter', 'stream', 'lease', 'limit', 'semaphore', 'once',
]);
/** `BITFIELD` operation words. */
export const KW_BITFIELD_OP = table(['GET', 'SET', 'INCRBY', 'OVERFLOW']);
/** `XADD` / `XTRIM` trim strategies. */
export const KW_TRIM_STRATEGY = table(['MAXLEN', 'MINID']);
/** `XADD` / `XTRIM` trim modes: exact `=` or approximate `~`. */
export const KW_TRIM_MODE = table(['=', '~']);
/** `INFO` section names (lower case, spaces removed, as the server matches them). */
export const KW_INFO_SECTION = table([
  'server', 'stats', 'buffers', 'memory', 'memorybreakdown', 'keyspace', 'snapshot', 'cluster', 'replication',
  'commandcounters', 'latencystats', 'logs', 'slabs', 'all', 'default', 'prometheus',
]);
/** `CLIENT LIST TYPE` names. */
export const KW_CLIENT_TYPE = table(['normal', 'master', 'replica', 'pubsub']);

/** The pre-encoded Buffer for `v` when it is a known keyword, else `v` itself. */
export function kw<T extends string>(t: ReadonlyMap<string, Buffer>, v: T): Buffer | T {
  return t.get(v) ?? v;
}
