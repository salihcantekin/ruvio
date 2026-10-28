// SPDX-License-Identifier: MIT
// Public option and reply types.

/** A key: UTF-8 string or raw bytes. Pass a Buffer for binary keys. */
export type Key = string | Buffer;
/** A value argument: UTF-8 string, raw bytes, or a number sent as ASCII text. */
export type Value = string | Buffer | number;

/**
 * Options for {@link Commands.set}.
 *
 * Only one expiry option is sent, checked in the order `ex`, `px`, `exat`, `pxat`, `keepTtl`.
 * `nx` wins over `xx`. `ifEq`, `ifGt` and `ifLt` cannot be combined with `nx`, `xx` or `get`.
 */
export interface SetOptions {
  /** Time to live in seconds (`EX`). Must be positive. */
  ex?: number;
  /** Time to live in milliseconds (`PX`). Must be positive. */
  px?: number;
  /** Absolute deadline as Unix time in seconds (`EXAT`). */
  exat?: number;
  /** Absolute deadline as Unix time in milliseconds (`PXAT`). */
  pxat?: number;
  /** Keep the key's existing TTL instead of clearing it (`KEEPTTL`). */
  keepTtl?: boolean;
  /** Only write when the key does not exist (`NX`). The call returns `null` when it does. */
  nx?: boolean;
  /** Only write when the key already exists (`XX`). The call returns `null` when it does not. */
  xx?: boolean;
  /** Return the previous value instead of `'OK'` (`GET`). `WRONGTYPE` when the old value is not a string. */
  get?: boolean;
  /** Ruvio compare-and-set (`IFEQ`): write only when the current string equals this value; otherwise the call returns `null`. */
  ifEq?: Value;
  /** Ruvio watermark (`IFGT`): write the integer `value` only when the key is missing or `value` is greater than the stored integer; otherwise `null`. */
  ifGt?: boolean;
  /** Ruvio watermark (`IFLT`): like `ifGt` but writes when `value` is smaller than the stored integer. */
  ifLt?: boolean;
}

/**
 * TTL-on-create option shared by the counter, bitmap and JSON write commands.
 * The deadline is set only when the call creates the key; an existing key keeps the deadline it has.
 */
export interface TtlOptions {
  /**
   * Time to live in seconds. A whole number is sent as `EX`, a fractional value as `PX` (milliseconds, rounded, at least 1).
   * Must be positive and finite; anything else rejects the call with a `RangeError` before it is sent.
   */
  ttl?: number;
}

/** Options for {@link Commands.geoadd}. */
export interface GeoAddOptions extends TtlOptions {
  /** Only add new members; never update existing ones. */
  nx?: boolean;
  /** Only update existing members; never add new ones. */
  xx?: boolean;
  /** Count changed members too: the reply is added plus updated instead of added only. */
  ch?: boolean;
}

/** Condition for `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT` and the `HEXPIRE` family. */
export type ExpireCondition = 'NX' | 'XX' | 'GT' | 'LT';
/**
 * Named {@link ExpireCondition} values for autocomplete, e.g. `c.hexpire('k', 60, ['f'], ExpireCondition.Nx)`.
 * Plain string literals (`'NX'`) work the same.
 */
export const ExpireCondition = { Nx: 'NX', Xx: 'XX', Gt: 'GT', Lt: 'LT' } as const;

/** Key type filter for `SCAN`: `string`, `json`, `list`, `set`, `hash`, `zset`, `bloomfilter`, `stream`, `lease`, `limit`, `semaphore` or `once`. */
export type ScanType =
  | 'string' | 'json' | 'list' | 'set' | 'hash' | 'zset' | 'bloomfilter' | 'stream' | 'lease' | 'limit' | 'semaphore' | 'once';
/**
 * Named {@link ScanType} values, e.g. `c.scan(0, { type: ScanType.Hash })`.
 */
export const ScanType = {
  String: 'string', Json: 'json', List: 'list', Set: 'set', Hash: 'hash', Zset: 'zset', Bloomfilter: 'bloomfilter',
  Stream: 'stream', Lease: 'lease', Limit: 'limit', Semaphore: 'semaphore', Once: 'once',
} as const;

/** Options shared by the `SCAN` family ({@link Commands.scan}, `sscan`, `hscan`, `zscan`). */
export interface ScanOptions {
  /** Glob pattern the key (or member, field) must match, e.g. `'user:*'`. */
  match?: string;
  /** How many items to examine per page. A hint: pages may hold more or fewer. */
  count?: number;
  /** Only keys of this type (`SCAN` only). Use {@link ScanType} for the known names. */
  type?: ScanType | (string & {});
}

/** One page of a `SCAN`-style walk. */
export interface ScanPage<T> {
  /** Cursor for the next page; `'0'` when the walk is done. */
  cursor: string;
  /** Items on this page (may be empty even when the walk is not finished). */
  items: T[];
}

/** A sorted set member with its score. */
export interface SortedSetEntry {
  /** Member name. */
  member: string;
  /** Score (`Infinity` and `-Infinity` are possible). */
  score: number;
}

/** A member popped by a blocking sorted-set pop, with the key it came from. */
export interface PoppedEntry extends SortedSetEntry {
  /** The sorted set the member was removed from. */
  key: string;
}

/**
 * Options for {@link Commands.zadd} and {@link Commands.zaddIncr}.
 * `nx` and `xx` are exclusive (`nx` wins); so are `gt` and `lt` (`gt` wins).
 */
export interface ZAddOptions {
  /** Only add new members; never update existing ones. */
  nx?: boolean;
  /** Only update existing members; never add new ones. */
  xx?: boolean;
  /** Only update when the new score is greater than the current one. A missing member is still added. */
  gt?: boolean;
  /** Only update when the new score is less than the current one. A missing member is still added. */
  lt?: boolean;
  /** Count changed members too: the reply is added plus updated instead of added only. */
  ch?: boolean;
  /** Time to live in seconds, set only when this call creates the key; an existing key keeps its deadline (`EX`, or `PX` for a fractional value). Must be positive and finite. */
  ttl?: number;
}

/** Options for {@link Commands.zrange} and {@link Commands.zrangeWithScores}. */
export interface ZRangeOptions {
  /** Treat start/stop as a score range (`'SCORE'`) or a lexicographic range (`'LEX'`). Default: ranks. */
  by?: 'SCORE' | 'LEX';
  /** Reverse the order. With `by`, give the high bound first. */
  rev?: boolean;
  /** Page of the result: skip `offset` members, return at most `count` (a negative count returns the rest). Needs `by`. */
  limit?: { offset: number; count: number };
}

/** Options for the sorted-set union, intersection and difference commands. */
export interface ZStoreOptions {
  /** One multiplier per input key, applied to its scores before combining. Default 1 each. */
  weights?: number[];
  /** How scores of the same member are combined: `'SUM'` (default), `'MIN'` or `'MAX'`; see {@link Aggregate}. */
  aggregate?: Aggregate;
}

/** How `ZUNION` / `ZINTER` combine scores of the same member: `SUM`, `MIN` or `MAX`. */
export type Aggregate = 'SUM' | 'MIN' | 'MAX';
/** Named {@link Aggregate} values, e.g. `{ aggregate: Aggregate.Max }`. */
export const Aggregate = { Sum: 'SUM', Min: 'MIN', Max: 'MAX' } as const;

/** Which end of a list: `LEFT` or `RIGHT` (`LMOVE`, `BLMOVE`, `LMPOP`, `BLMPOP`). */
export type ListSide = 'LEFT' | 'RIGHT';
/** Named {@link ListSide} values, e.g. `c.lmove('a', 'b', ListSide.Left, ListSide.Right)`. */
export const ListSide = { Left: 'LEFT', Right: 'RIGHT' } as const;

/** Where `LINSERT` places the new element relative to the pivot: `BEFORE` or `AFTER`. */
export type InsertPosition = 'BEFORE' | 'AFTER';
/** Named {@link InsertPosition} values, e.g. `c.linsert('l', InsertPosition.Before, 'pivot', 'x')`. */
export const InsertPosition = { Before: 'BEFORE', After: 'AFTER' } as const;

/** Result order of geo queries: `ASC` (nearest first) or `DESC`. */
export type GeoOrder = 'ASC' | 'DESC';
/** Named {@link GeoOrder} values, e.g. `{ order: GeoOrder.Asc }`. */
export const GeoOrder = { Asc: 'ASC', Desc: 'DESC' } as const;

/** Result of {@link Commands.lmpop} and {@link Commands.blmpop}. */
export interface ListPopResult {
  /** The list the elements were popped from. */
  key: string;
  /** The popped elements, in pop order. */
  elements: string[];
}

/** Options for {@link Commands.lpos}. */
export interface LPosOptions {
  /** Which match to return: `1` the first, `2` the second, ...; negative counts from the tail. */
  rank?: number;
  /** Return up to this many positions as an array (`0` means all). */
  count?: number;
  /** Examine at most this many elements (`0` means no limit). */
  maxlen?: number;
}

/** One point for {@link Commands.geoadd}. */
export interface GeoMember {
  /** Degrees east, -180 to 180. */
  longitude: number;
  /** Degrees north, -85.05112878 to 85.05112878. */
  latitude: number;
  /** Name of the point. */
  member: Value;
}

/** Geo distance unit: metres, kilometres, feet or miles (`m`, `km`, `ft`, `mi`; upper case also accepted). */
export type GeoUnit = 'M' | 'KM' | 'FT' | 'MI' | 'm' | 'km' | 'ft' | 'mi';
/** Named {@link GeoUnit} values, e.g. `c.geodist('g', 'a', 'b', GeoUnit.Km)`. */
export const GeoUnit = { M: 'm', Km: 'km', Ft: 'ft', Mi: 'mi' } as const;

/**
 * Options for {@link Commands.geosearch} and {@link Commands.geosearchstore}.
 * Give exactly one centre (`fromMember` or `fromLonLat`) and one shape (`byRadius` or `byBox`).
 */
export interface GeoSearchOptions {
  /** Centre the search on an existing member of the key. */
  fromMember?: Value;
  /** Centre the search on a point `[longitude, latitude]` (degrees). */
  fromLonLat?: [number, number];
  /** Circle: `radius` in `unit`. */
  byRadius?: { radius: number; unit: GeoUnit };
  /** Rectangle: `width` and `height` in `unit`, centred on the centre. */
  byBox?: { width: number; height: number; unit: GeoUnit };
  /** Sort by distance from the centre: `'ASC'` nearest first, `'DESC'` farthest first (see {@link GeoOrder}). Default unsorted. */
  order?: GeoOrder;
  /** Return at most this many matches. */
  count?: number;
  /** With `count`: return any matches found instead of the nearest ones (faster). */
  any?: boolean;
  /** Include each match's `[longitude, latitude]` in {@link GeoMatch.coordinates}. */
  withCoord?: boolean;
  /** Include each match's distance from the centre, in the shape's unit, in {@link GeoMatch.distance}. */
  withDist?: boolean;
  /** Include each match's raw 52-bit geohash integer in {@link GeoMatch.hash}. */
  withHash?: boolean;
}

/** Options for {@link Commands.georadius} and {@link Commands.georadiusbymember}. */
export interface GeoRadiusOptions {
  /** Include each match's coordinates. */
  withCoord?: boolean;
  /** Include each match's distance from the centre, in the call's unit. */
  withDist?: boolean;
  /** Include each match's raw 52-bit geohash integer. */
  withHash?: boolean;
  /** Return at most this many matches. */
  count?: number;
  /** With `count`: return any matches found instead of the nearest ones. */
  any?: boolean;
  /** `'ASC'` nearest first, `'DESC'` farthest first. */
  order?: GeoOrder;
  /** Store the matches in this key (as a geo sorted set) and return the stored count. Replaces the key; same slot required. */
  store?: Key;
  /** Like `store`, but the scores are the distances. */
  storeDist?: Key;
}

/** One result of a geo search made with a `with*` option. */
export interface GeoMatch {
  /** Member name. */
  member: string;
  /** Distance from the centre, in the requested unit (set with `withDist`). */
  distance?: number;
  /** Raw 52-bit geohash integer (set with `withHash`). */
  hash?: number;
  /** `[longitude, latitude]` in degrees (set with `withCoord`). */
  coordinates?: [number, number];
}

/** Options for {@link Commands.bfReserve}. */
export interface BfReserveOptions {
  /** Growth factor of each new layer when the filter fills up. Default 2. */
  expansion?: number;
  /** Fixed-size filter: adding to a full filter is an error instead of growing it. */
  nonScaling?: boolean;
}

/** Options for {@link Commands.bfInsert}. The creation options (all but `noCreate`) apply only when the key is new. */
export interface BfInsertOptions {
  /** Expected number of items when the filter is created. */
  capacity?: number;
  /** Target false-positive rate (0 to 1) when created. */
  error?: number;
  /** Layer growth factor when created. Default 2. */
  expansion?: number;
  /** Fail with `ERR not found` instead of creating a missing filter. */
  noCreate?: boolean;
  /** Create a fixed-size filter. */
  nonScaling?: boolean;
  /** Time to live in seconds, set only when this call creates the filter; an existing filter keeps its deadline. Must be positive and finite. */
  ttl?: number;
}

/** One stream entry. */
export interface StreamEntry {
  /** Entry id, `<ms>-<seq>`. */
  id: string;
  /** `[name, value]` pairs in stored order; a name may repeat. */
  fields: [string, string][];
}

/** Entries read from one stream by `XREAD` or `XREADGROUP`. */
export interface StreamReadResult {
  /** The stream key. */
  key: string;
  /** New entries, oldest first. */
  entries: StreamEntry[];
}

/** Which limit a stream trim applies: `MAXLEN` (entry count) or `MINID` (lowest id kept). */
export type TrimStrategy = 'MAXLEN' | 'MINID' | 'maxlen' | 'minid';
/** Named {@link TrimStrategy} values, e.g. `StreamTrim.of(TrimStrategy.MaxLen, 1000)`. */
export const TrimStrategy = { MaxLen: 'MAXLEN', MinId: 'MINID' } as const;
/** How a stream trim is applied: `'='` exact or `'~'` approximate. Ruvio trims exactly either way. */
export type TrimMode = '=' | '~';
/** Named {@link TrimMode} values, e.g. `StreamTrim.maxLen(1000, TrimMode.Approximate)`. */
export const TrimMode = { Exact: '=', Approximate: '~' } as const;

/**
 * One stream trim rule, accepted by `XADD` (`opts.trim`) and `XTRIM`.
 * `LIMIT` is not supported by the server. Build it with {@link StreamTrim.maxLen} or {@link StreamTrim.minId}.
 *
 * @example
 * ```ts
 * await client.xadd('events', StreamId.New, { type: 'signup' }, { trim: StreamTrim.maxLen(10_000, TrimMode.Approximate) });
 * await client.xtrim('events', StreamTrim.minId('1700000000000-0'));
 * ```
 */
export interface StreamTrim {
  /** `MAXLEN` or `MINID`. */
  strategy: TrimStrategy;
  /** `'='` or `'~'`; omit to send neither. */
  mode?: TrimMode;
  /** An entry count for `MAXLEN`, an id such as `'1700000000000-0'` for `MINID`. */
  threshold: number | string;
}

/** Builders for {@link StreamTrim}. */
export const StreamTrim = {
  /**
   * Keep at most `count` entries (`MAXLEN [=|~] count`).
   *
   * @example
   * ```ts
   * StreamTrim.maxLen(1000);
   * ```
   */
  maxLen(count: number, mode?: TrimMode): StreamTrim {
    return { strategy: TrimStrategy.MaxLen, mode, threshold: count };
  },
  /**
   * Drop entries with ids below `id` (`MINID [=|~] id`).
   *
   * @example
   * ```ts
   * StreamTrim.minId('1700000000000-0', TrimMode.Exact);
   * ```
   */
  minId(id: string, mode?: TrimMode): StreamTrim {
    return { strategy: TrimStrategy.MinId, mode, threshold: id };
  },
  /**
   * Any strategy, mode and threshold.
   *
   * @example
   * ```ts
   * StreamTrim.of(TrimStrategy.MaxLen, 500, TrimMode.Approximate);
   * ```
   */
  of(strategy: TrimStrategy, threshold: number | string, mode?: TrimMode): StreamTrim {
    return { strategy, mode, threshold };
  },
} as const;

/** `INFO` section names the server recognises. An unknown name gives an empty reply. */
export type InfoSection =
  | 'server' | 'stats' | 'buffers' | 'memory' | 'memorybreakdown' | 'keyspace' | 'snapshot' | 'cluster'
  | 'replication' | 'commandcounters' | 'latencystats' | 'logs' | 'slabs' | 'all' | 'default' | 'prometheus';
/**
 * Named {@link InfoSection} values, e.g. `client.info(InfoSection.Memory)`.
 * `MemoryBreakdown` and `CommandCounters` are sent as one word. `Prometheus` returns Prometheus text.
 */
export const InfoSection = {
  Server: 'server', Stats: 'stats', Buffers: 'buffers', Memory: 'memory', MemoryBreakdown: 'memorybreakdown',
  Keyspace: 'keyspace', Snapshot: 'snapshot', Cluster: 'cluster', Replication: 'replication',
  CommandCounters: 'commandcounters', LatencyStats: 'latencystats', Logs: 'logs', Slabs: 'slabs',
  All: 'all', Default: 'default', Prometheus: 'prometheus',
} as const;

/** Connection kind filter for `CLIENT LIST TYPE`: `normal`, `master`, `replica` or `pubsub`. */
export type ClientType = 'normal' | 'master' | 'replica' | 'pubsub';
/** Named {@link ClientType} values, e.g. `client.clientList(ClientType.PubSub)`. */
export const ClientType = { Normal: 'normal', Master: 'master', Replica: 'replica', PubSub: 'pubsub' } as const;

/**
 * Stream id markers for the stream commands; plain strings, so they work wherever an id is accepted.
 *
 * @example
 * ```ts
 * await client.xrange('events', StreamId.Min, StreamId.Max, 100);
 * await client.xadd('events', StreamId.New, { a: 1 });
 * ```
 */
export const StreamId = {
  /** Automatic id for `XADD` (`*`). */
  New: '*',
  /** Only entries added after the call, for `XREAD` / `XGROUP CREATE` (`$`). */
  LastDelivered: '$',
  /** Only entries never delivered to the group, for `XREADGROUP` (`>`). */
  NewMessages: '>',
  /** Lowest possible id for ranges (`-`). */
  Min: '-',
  /** Highest possible id for ranges (`+`). */
  Max: '+',
} as const;

/**
 * Bounds for score ranges (`ZRANGEBYSCORE`, `ZCOUNT`, `ZREMRANGEBYSCORE`...); each returns the string the commands accept.
 *
 * @example
 * ```ts
 * await client.zrangebyscore('board', ScoreBound.inclusive(80), ScoreBound.exclusive(100));
 * await client.zcount('board', ScoreBound.NegativeInfinity, ScoreBound.PositiveInfinity);
 * ```
 */
export const ScoreBound = {
  /** `-inf`. */
  NegativeInfinity: '-inf',
  /** `+inf`. */
  PositiveInfinity: '+inf',
  /** Includes `score` (`score`). */
  inclusive(score: number): string {
    return String(score);
  },
  /** Excludes `score` (`(score`). */
  exclusive(score: number): string {
    return `(${score}`;
  },
} as const;

/**
 * Bounds for lexicographic ranges (`ZRANGEBYLEX`, `ZLEXCOUNT`...); each returns the string the commands accept.
 *
 * @example
 * ```ts
 * await client.zrangebylex('words', LexBound.inclusive('a'), LexBound.exclusive('b'));
 * await client.zrangebylex('words', LexBound.Min, LexBound.Max);
 * ```
 */
export const LexBound = {
  /** Before every member (`-`). */
  Min: '-',
  /** After every member (`+`). */
  Max: '+',
  /** Includes `member` (`[member`). */
  inclusive(member: string): string {
    return `[${member}`;
  },
  /** Excludes `member` (`(member`). */
  exclusive(member: string): string {
    return `(${member}`;
  },
} as const;

/** Options for {@link Commands.xadd}. `maxLen` and `minId` are exclusive. */
export interface XAddOptions {
  /** A typed trim rule ({@link StreamTrim}); used instead of `maxLen`, `minId` and `approximate` when set. */
  trim?: StreamTrim;
  /** Do not create the stream: return `null` when the key is missing (`NOMKSTREAM`). */
  noMkStream?: boolean;
  /** Keep at most this many entries (`MAXLEN`). */
  maxLen?: number;
  /** Drop entries with ids below this one (`MINID`). */
  minId?: string;
  /** Send `~` with the trim option. Accepted; Ruvio trims exactly either way. */
  approximate?: boolean;
  /** Ruvio: hold the entry back from `XREADGROUP` for this many milliseconds. */
  delayMs?: number;
  /** Time to live in seconds, set only when this call creates the stream; an existing stream keeps its deadline. Must be positive and finite. */
  ttl?: number;
}

/** Options for {@link Commands.xtrim}. Give `maxLen` or `minId`, not both. */
export interface XTrimOptions {
  /** A typed trim rule ({@link StreamTrim}); used instead of `maxLen`, `minId` and `approximate` when set. */
  trim?: StreamTrim;
  /** Keep at most this many entries (`MAXLEN`). */
  maxLen?: number;
  /** Drop entries with ids below this one; that id is kept (`MINID`). */
  minId?: string;
  /** Send `~`. Accepted; Ruvio trims exactly either way. */
  approximate?: boolean;
}

/** Options for {@link Commands.xread}. */
export interface XReadOptions {
  /** Maximum entries per stream. */
  count?: number;
  /** Milliseconds to wait for new entries; `0` waits until an entry or shutdown. Blocks the whole connection: use a dedicated client. */
  block?: number;
}

/** Options for {@link Commands.xreadgroup}. */
export interface XReadGroupOptions extends XReadOptions {
  /** Do not add the delivered entries to the pending list (`NOACK`); the group id still advances. */
  noAck?: boolean;
}

/** Options for {@link Commands.xclaim}. */
export interface XClaimOptions {
  /** Set the entry's idle time to this many milliseconds. Not with `time`. */
  idle?: number;
  /** Set the last-delivery time to this Unix-ms timestamp. Not with `idle`. */
  time?: number;
  /** Set the delivery counter. */
  retryCount?: number;
  /** Claim an id that is in the stream but not pending. Does not move the group cursor. */
  force?: boolean;
  /** Return only the claimed ids, not the entries. */
  justId?: boolean;
  /** Set the group's last-delivered id. */
  lastId?: string;
}

/** Result of {@link Commands.xautoclaim}. */
export interface AutoClaimResult {
  /** Id to pass as `start` on the next call; `0-0` when the scan finished. */
  next: string;
  /** The claimed entries, or only their ids with `justId`. */
  entries: StreamEntry[] | string[];
  /** Ids dropped from the pending list because the stream entry no longer exists. */
  deleted: string[];
}

/** Summary of a group's pending entries ({@link Commands.xpending}). */
export interface PendingSummary {
  /** Number of pending entries. */
  count: number;
  /** Lowest pending id, or `null` when none. */
  smallest: string | null;
  /** Highest pending id, or `null` when none. */
  greatest: string | null;
  /** Pending count per consumer, sorted by name. */
  consumers: { name: string; pending: number }[];
}

/** One pending entry ({@link Commands.xpendingRange}). */
export interface PendingRow {
  /** Entry id. */
  id: string;
  /** Consumer that holds it. */
  consumer: string;
  /** Milliseconds since it was last delivered. */
  idleMs: number;
  /** How many times it has been delivered. */
  deliveries: number;
}

/** Result of {@link Commands.fillget}. */
export interface FillResult {
  /** `hit`: cached; `fill`: you must build and FILLSET; `busy`: someone else fills; `stale`: old value. */
  state: 'hit' | 'fill' | 'busy' | 'stale';
  /** The cached value for `hit` and `stale`; `null` otherwise. */
  value: string | null;
  /** The fill token for `fill`, and for `stale` when this caller should refresh. Pass it to {@link Commands.fillset}. */
  token: string | null;
}

/** Result of {@link Commands.idemBegin}. */
export interface IdemBeginResult {
  /**
   * `acquired`: you own the key and should do the work; `pending`: another owner is running it;
   * `conflict`: the key is reserved for a different fingerprint; `completed`: a result is stored.
   */
  state: 'acquired' | 'pending' | 'conflict' | 'completed';
  /** The stored result when `completed`; `null` otherwise. */
  result: string | null;
}

/** One entry of the change feed ({@link Commands.changes}). */
export interface ChangeNote {
  /** Sequence number; pass the last one back as the next cursor. */
  seq: number;
  /** Description of the change. */
  note: string;
}

/** Options for {@link Commands.restore}. */
export interface RestoreOptions {
  /** Overwrite an existing key instead of failing with `BUSYKEY`. */
  replace?: boolean;
  /** Treat `ttlMs` as an absolute Unix-ms deadline. */
  absTtl?: boolean;
  /** Accepted and ignored. */
  idleTime?: number;
  /** Accepted and ignored. */
  freq?: number;
}

/** Filters for {@link Commands.clientKill}. */
export interface ClientKillOptions {
  /** Connection id (see {@link Commands.clientId}). */
  id?: number;
  /** Peer address `ip:port`. */
  addr?: string;
  /** `true` spares the calling connection (`SKIPME yes`); `false` lets it be closed too. */
  skipMe?: boolean;
}

/** Unit of the `start` / `end` range of `BITCOUNT` and `BITPOS`: `BYTE` or `BIT`. */
export type BitUnit = 'BYTE' | 'BIT';
/** Named {@link BitUnit} values, e.g. `c.bitcount('k', 0, 7, BitUnit.Bit)`. */
export const BitUnit = { Byte: 'BYTE', Bit: 'BIT' } as const;

/** Operation of `BITOP`: `AND`, `OR`, `XOR` or `NOT`. */
export type BitOp = 'AND' | 'OR' | 'XOR' | 'NOT';
/** Named {@link BitOp} values, e.g. `c.bitop(BitOp.And, 'dst', 'a', 'b')`. */
export const BitOp = { And: 'AND', Or: 'OR', Xor: 'XOR', Not: 'NOT' } as const;

/** `BITFIELD` field type: signed `i1`..`i64` or unsigned `u1`..`u63`, e.g. `'u8'`, `'i16'`. */
export type BitfieldType = `i${number}` | `u${number}`;
/** `BITFIELD OVERFLOW` mode: `WRAP`, `SAT` or `FAIL`. */
export type BitfieldOverflow = 'WRAP' | 'SAT' | 'FAIL';
/** Named {@link BitfieldOverflow} values, e.g. `BitfieldOp.overflow(BitfieldOverflow.Sat)`. */
export const BitfieldOverflow = { Wrap: 'WRAP', Sat: 'SAT', Fail: 'FAIL' } as const;

/** The answer of {@link Commands.limitSlidingWithRetry} and {@link Commands.limitTokensWithRetry}. */
export interface LimitOutcome {
  /** `true` when this call was let through. */
  allowed: boolean;
  /** Allowance left after this call (`0` when refused). */
  remaining: number;
  /** Milliseconds until the same call would pass (`0` when it was let through). Fit for a `Retry-After` header. */
  retryAfterMs: number;
}
