// SPDX-License-Identifier: MIT
// Typed methods for every command in docs/compatibility-matrix.json.
// Shared by the client (returns promises), pipelines and transactions (queue frames).

import { C } from './command-names.js';
import { cmd, Encoder, type Arg, type Cmd } from './encoder.js';
import { T, type Tf } from './transforms.js';
import type {
  LimitOutcome,
  Key,
  Value,
  SetOptions,
  ExpireCondition,
  ScanOptions,
  ScanPage,
  SortedSetEntry,
  PoppedEntry,
  ZAddOptions,
  TtlOptions,
  ZRangeOptions,
  ZStoreOptions,
  ListPopResult,
  LPosOptions,
  GeoMember,
  GeoAddOptions,
  GeoUnit,
  GeoSearchOptions,
  GeoRadiusOptions,
  GeoMatch,
  BfReserveOptions,
  BfInsertOptions,
  StreamEntry,
  StreamReadResult,
  XAddOptions,
  XTrimOptions,
  StreamTrim,
  InfoSection,
  ClientType,
  XReadOptions,
  XReadGroupOptions,
  XClaimOptions,
  AutoClaimResult,
  PendingSummary,
  PendingRow,
  FillResult,
  IdemBeginResult,
  ChangeNote,
  RestoreOptions,
  ClientKillOptions,
  BitUnit,
  BitOp,
  ListSide,
  InsertPosition,
  BitfieldType,
} from './types.js';
import { KW_AGGREGATE, KW_EXPIRE_COND, KW_BITOP, KW_BITUNIT, KW_CLIENT_TYPE, KW_GEO_UNIT, KW_INFO_SECTION, KW_ORDER, KW_SCAN_TYPE, KW_SIDE, KW_TRIM_MODE, KW_TRIM_STRATEGY, KW_WHERE, kw } from './keywords.js';


// Option words as Buffers: encoded with a single copy, no string scan.
const w = (s: string): Buffer => Buffer.from(s, 'latin1');
const W = {
  EX: w('EX'), PX: w('PX'), EXAT: w('EXAT'), PXAT: w('PXAT'), KEEPTTL: w('KEEPTTL'), NX: w('NX'), XX: w('XX'),
  GET: w('GET'), IFEQ: w('IFEQ'), IFGT: w('IFGT'), IFLT: w('IFLT'), MAX: w('MAX'), WAIT: w('WAIT'), SWR: w('SWR'), BEGIN: w('BEGIN'),
  SLIDING: w('SLIDING'), TOKENS: w('TOKENS'), COST: w('COST'), WITHRETRY: w('WITHRETRY'),
  COMPLETE: w('COMPLETE'), START: w('START'), MATCH: w('MATCH'), COUNT: w('COUNT'), TYPE: w('TYPE'),
  ASYNC: w('ASYNC'), REPLACE: w('REPLACE'), ABSTTL: w('ABSTTL'), IDLETIME: w('IDLETIME'), FREQ: w('FREQ'),
  SAMPLES: w('SAMPLES'), DB: w('DB'), RANK: w('RANK'), MAXLEN: w('MAXLEN'), LIMIT: w('LIMIT'),
  WITHSCORES: w('WITHSCORES'), WITHVALUES: w('WITHVALUES'), BYSCORE: w('BYSCORE'), BYLEX: w('BYLEX'), REV: w('REV'),
  GT: w('GT'), LT: w('LT'), CH: w('CH'), INCR: w('INCR'), WEIGHTS: w('WEIGHTS'), AGGREGATE: w('AGGREGATE'),
  FIELDS: w('FIELDS'), FROMMEMBER: w('FROMMEMBER'), FROMLONLAT: w('FROMLONLAT'), BYRADIUS: w('BYRADIUS'),
  BYBOX: w('BYBOX'), ANY: w('ANY'), WITHCOORD: w('WITHCOORD'), WITHDIST: w('WITHDIST'), WITHHASH: w('WITHHASH'),
  STORE: w('STORE'), STOREDIST: w('STOREDIST'), EXPANSION: w('EXPANSION'), NONSCALING: w('NONSCALING'),
  CAPACITY: w('CAPACITY'), ERROR: w('ERROR'), NOCREATE: w('NOCREATE'), ITEMS: w('ITEMS'), PREFIX: w('PREFIX'),
  PATH: w('PATH'), VALUE: w('VALUE'), AFTER: w('AFTER'), NOMKSTREAM: w('NOMKSTREAM'), MINID: w('MINID'),
  TILDE: w('~'), DELAY: w('DELAY'), BLOCK: w('BLOCK'), STREAMS: w('STREAMS'), MKSTREAM: w('MKSTREAM'),
  GROUP: w('GROUP'), NOACK: w('NOACK'), GTID: w('>'), IDLE: w('IDLE'), JUSTID: w('JUSTID'), TIME: w('TIME'),
  RETRYCOUNT: w('RETRYCOUNT'), FORCE: w('FORCE'), LASTID: w('LASTID'), ON: w('ON'), OFF: w('OFF'), ID: w('ID'),
  ADDR: w('ADDR'), SKIPME: w('SKIPME'), YES: w('yes'), NO: w('no'), SETNAME: w('SETNAME'), AUTH: w('AUTH'),
  REPLICAS: w('REPLICAS'), TIMEOUT: w('TIMEOUT'), OWNER: w('OWNER'), OVERFLOW: w('OVERFLOW'), MIN: w('MIN'),
} as const;

type R<T> = Promise<T>;

/** `LIMIT ... WITHRETRY` reply: `[remaining, retryAfterMs]`, `remaining` -1 when refused. */
function limitOutcome(v: unknown): LimitOutcome {
  const [remaining, retryAfterMs] = v as [number, number];
  return { allowed: remaining >= 0, remaining: Math.max(remaining, 0), retryAfterMs: Math.max(retryAfterMs, 0) };
}

function pushScan(a: Arg[], o?: ScanOptions): Arg[] {
  if (o !== undefined) {
    if (o.match !== undefined) a.push(W.MATCH, o.match);
    if (o.count !== undefined) a.push(W.COUNT, o.count);
    if (o.type !== undefined) a.push(W.TYPE, kw(KW_SCAN_TYPE, o.type));
  }
  return a;
}

/**
 * Appends `EX seconds` (whole seconds) or `PX milliseconds` (a sub-second part) for a TTL-on-create option.
 * A TTL that is not a positive finite number is queued as a RangeError, so the call rejects before anything is sent.
 */
function pushTtl(a: Arg[], seconds: number): Arg[] {
  if (typeof seconds !== 'number' || !Number.isFinite(seconds) || seconds <= 0) {
    a.push(new RangeError('ruvio: ttl must be a positive, finite number of seconds') as unknown as Arg);
  } else if (Number.isInteger(seconds)) a.push(W.EX, seconds);
  else a.push(W.PX, Math.max(1, Math.round(seconds * 1000)));
  return a;
}

function pushAll(a: Arg[], items: readonly Arg[]): Arg[] {
  for (let i = 0; i < items.length; i++) a.push(items[i]!);
  return a;
}

function flatPairs(a: Arg[], pairs: Record<string, Value> | ReadonlyArray<readonly [Key, Value]>): Arg[] {
  if (Array.isArray(pairs)) {
    for (const p of pairs as ReadonlyArray<readonly [Key, Value]>) a.push(p[0], p[1]);
  } else {
    for (const k in pairs) a.push(k, (pairs as Record<string, Value>)[k]!);
  }
  return a;
}

function geoTf(o: { withCoord?: boolean; withDist?: boolean; withHash?: boolean }): Tf | null {
  if (!o.withCoord && !o.withDist && !o.withHash) return null;
  return (v: unknown[]) =>
    v.map((row) => {
      const r = row as unknown[];
      const m: GeoMatch = { member: r[0] as string };
      let i = 1;
      if (o.withDist) m.distance = Number(r[i++]);
      if (o.withHash) m.hash = Number(r[i++]);
      if (o.withCoord) {
        const c = r[i++] as unknown[];
        m.coordinates = [Number(c[0]), Number(c[1])];
      }
      return m;
    });
}

function pushGeoShape(a: Arg[], o: GeoSearchOptions): void {
  if (o.fromMember !== undefined) a.push(W.FROMMEMBER, o.fromMember);
  if (o.fromLonLat !== undefined) a.push(W.FROMLONLAT, o.fromLonLat[0], o.fromLonLat[1]);
  if (o.byRadius !== undefined) a.push(W.BYRADIUS, o.byRadius.radius, kw(KW_GEO_UNIT, o.byRadius.unit));
  if (o.byBox !== undefined) a.push(W.BYBOX, o.byBox.width, o.byBox.height, kw(KW_GEO_UNIT, o.byBox.unit));
  if (o.order !== undefined) a.push(kw(KW_ORDER, o.order));
  if (o.count !== undefined) {
    a.push(W.COUNT, o.count);
    if (o.any) a.push(W.ANY);
  }
}

function pushZStore(a: Arg[], keys: readonly Key[], o?: ZStoreOptions): Arg[] {
  a.push(keys.length);
  pushAll(a, keys);
  if (o !== undefined) {
    if (o.weights !== undefined) {
      a.push(W.WEIGHTS);
      pushAll(a, o.weights);
    }
    if (o.aggregate !== undefined) a.push(W.AGGREGATE, kw(KW_AGGREGATE, o.aggregate));
  }
  return a;
}

function pushStreamId(a: Arg[], o: XTrimOptions | XAddOptions): void {
  if (o.trim !== undefined) {
    pushTrim(a, o.trim);
    return;
  }
  if (o.maxLen !== undefined) {
    a.push(W.MAXLEN);
    if (o.approximate) a.push(W.TILDE);
    a.push(o.maxLen);
  } else if (o.minId !== undefined) {
    a.push(W.MINID);
    if (o.approximate) a.push(W.TILDE);
    a.push(o.minId);
  }
}

function pushTrim(a: Arg[], t: StreamTrim): void {
  a.push(kw(KW_TRIM_STRATEGY, t.strategy));
  if (t.mode !== undefined) a.push(kw(KW_TRIM_MODE, t.mode));
  a.push(t.threshold);
}

function pushFields(a: Arg[], fields: Record<string, Value> | readonly Value[]): Arg[] {
  if (Array.isArray(fields)) return pushAll(a, fields as Value[]);
  for (const k in fields) a.push(k, (fields as Record<string, Value>)[k]!);
  return a;
}

/**
 * Every typed command of the Ruvio server, shared by {@link RuvioClient}, {@link ClusterClient},
 * {@link Pipeline} and {@link Transaction}.
 *
 * On a client each method returns a promise for the reply. On a pipeline or transaction the same
 * method queues the command and returns the builder for chaining; the replies arrive from `exec()`.
 *
 * Conventions that hold for every method:
 * - A `Key` is a string (UTF-8) or a Buffer; a `Value` may also be a number, sent as ASCII text.
 * - Bulk strings come back as `string`. Use the `*Buffer` variants or `client.binary` for raw bytes.
 * - Integer replies are `number`. Integers beyond 2^53 arrive as `bigint` at runtime.
 * - A missing key usually gives `null` for a single value, `0` for a count and `[]` for a list; each method says which.
 * - A server error rejects with {@link RuvioError} (`err.code` is `ERR`, `WRONGTYPE`, `NOPERM`, `CROSSSLOT`, ...).
 *   A connection that fails before the reply rejects with {@link ConnectionError}. Methods only list
 *   the errors specific to them.
 * - On a multi-shard server (`shards > 1`), commands naming several keys need all keys in one hash
 *   slot (use `{tag}` in key names) or the server answers `CROSSSLOT`. {@link ClusterClient} splits
 *   `MGET`, `MSET`, `DEL`, `EXISTS`, `UNLINK` and `TOUCH` per slot (not atomic) and fails any other
 *   cross-slot command with `CROSSSLOT` before sending. See `docs/CLIENT_BEHAVIOR.md`.
 * - Keyless commands such as `DBSIZE`, `KEYS`, `SCAN` and `FLUSHDB` act on one shard only when the
 *   server runs with `port_mode = "per-shard"` (the default) and the client is a plain
 *   {@link RuvioClient}. {@link ClusterClient} fans `DBSIZE`, `KEYS`, `FLUSHDB` and `FLUSHALL` out to every node.
 * - Blocking commands (`BLPOP`, `XREAD ... BLOCK`, `WAIT`, ...) occupy the whole connection: run them on
 *   `client.duplicate()` or `client.isolated()`.
 *
 * @example
 * ```ts
 * const client = createClient('redis://127.0.0.1:6379');
 * await client.set('greeting', 'hello', { ex: 60 });
 * const value = await client.get('greeting'); // 'hello'
 * await client.quit();
 * ```
 */
export abstract class Commands {
  /** @internal Reply flags for this view: bit 0 = bulk strings as Buffers. */
  _fl = 0;

  /** @internal The encoder the next frame goes into. */
  protected abstract _enc(): Encoder;
  /** @internal Called after a frame is complete. */
  protected abstract _done(tf: Tf | null, fl: number): any;
  /** @internal Called when a frame could not be encoded. */
  protected abstract _fail(e: unknown, enc: Encoder): any;

  /** @internal */
  _0(tf: Tf | null, c: Cmd): any {
    const e = this._enc();
    e.begin(c, 0);
    e.endFrame();
    return this._done(tf, this._fl);
  }

  /** @internal */
  _1(tf: Tf | null, c: Cmd, a: Arg): any {
    const e = this._enc();
    try {
      e.begin(c, 1);
      e.arg(a);
    } catch (x) {
      return this._fail(x, e);
    }
    e.endFrame();
    return this._done(tf, this._fl);
  }

  /** @internal */
  _2(tf: Tf | null, c: Cmd, a: Arg, b: Arg): any {
    const e = this._enc();
    try {
      e.begin(c, 2);
      e.arg(a);
      e.arg(b);
    } catch (x) {
      return this._fail(x, e);
    }
    e.endFrame();
    return this._done(tf, this._fl);
  }

  /** @internal */
  _3(tf: Tf | null, c: Cmd, a: Arg, b: Arg, d: Arg): any {
    const e = this._enc();
    try {
      e.begin(c, 3);
      e.arg(a);
      e.arg(b);
      e.arg(d);
    } catch (x) {
      return this._fail(x, e);
    }
    e.endFrame();
    return this._done(tf, this._fl);
  }

  /** @internal */
  _4(tf: Tf | null, c: Cmd, a: Arg, b: Arg, d: Arg, f: Arg): any {
    const e = this._enc();
    try {
      e.begin(c, 4);
      e.arg(a);
      e.arg(b);
      e.arg(d);
      e.arg(f);
    } catch (x) {
      return this._fail(x, e);
    }
    e.endFrame();
    return this._done(tf, this._fl);
  }

  /** @internal */
  _v(tf: Tf | null, c: Cmd, args: readonly Arg[]): any {
    const e = this._enc();
    try {
      const n = args.length;
      e.begin(c, n);
      for (let i = 0; i < n; i++) e.arg(args[i]!);
    } catch (x) {
      return this._fail(x, e);
    }
    e.endFrame();
    return this._done(tf, this._fl);
  }

  /** @internal Key followed by a variadic list. */
  _kv(tf: Tf | null, c: Cmd, key: Arg, rest: readonly Arg[]): any {
    const e = this._enc();
    try {
      const n = rest.length;
      e.begin(c, n + 1);
      e.arg(key);
      for (let i = 0; i < n; i++) e.arg(rest[i]!);
    } catch (x) {
      return this._fail(x, e);
    }
    e.endFrame();
    return this._done(tf, this._fl);
  }

  /** @internal Runs `f` with bulk strings returned as Buffers. */
  _bin<X>(f: () => X): X {
    const prev = this._fl;
    this._fl = prev | 1;
    try {
      return f();
    } finally {
      this._fl = prev;
    }
  }

  /**
   * Sends any command by name and returns the raw reply. Use it for commands that have no typed method.
   * Replies are not converted: simple strings, bulk strings (as strings), integers (numbers, or `bigint` beyond 2^53), arrays and `null` come back as the parser produced them.
   *
   * @param command Command name, e.g. `'GET'`. Case does not matter. Subcommands go in the first argument position: `call('CLIENT', 'ID')`.
   * @param args Command arguments: strings are sent as UTF-8, Buffers as raw bytes, numbers as ASCII.
   * @returns The raw reply.
   * @throws {@link RuvioError} when the server answers with an error (`ERR`, `WRONGTYPE`, `NOPERM`, ...).
   * @throws {@link ConnectionError} when the connection fails before the reply arrives.
   *
   * @example
   * ```ts
   * const n = await client.call('DBSIZE');
   * await client.call('CLIENT', 'ID');
   * ```
   */
  call(command: string, ...args: Arg[]): R<unknown> {
    return this._v(null, cmd(command), args);
  }

  /**
   * Like {@link call}, but bulk strings in the reply come back as `Buffer` instead of `string`, so binary values are not corrupted by UTF-8 decoding.
   *
   * @param command Command name.
   * @param args Command arguments.
   * @returns The raw reply with bulk strings as Buffers.
   *
   * @example
   * ```ts
   * const raw = await client.callBuffer('GET', 'image:1') as Buffer | null;
   * ```
   */
  callBuffer(command: string, ...args: Arg[]): R<unknown> {
    return this._bin(() => this._v(null, cmd(command), args));
  }

  /**
   * Sends a command whose name was pre-encoded with {@link cmd} or taken from {@link Cmds}. This skips the per-call name encoding and is the fastest generic path.
   *
   * @param c Pre-encoded command name.
   * @param args Command arguments.
   * @returns The raw reply.
   *
   * @example
   * ```ts
   * await client.callCmd(Cmds.GET, 'user:1');
   * ```
   */
  callCmd(c: Cmd, ...args: Arg[]): R<unknown> {
    return this._v(null, c, args);
  }

  // ---- connection ------------------------------------------------------------------------

  /**
   * Sends the Ruvio-specific `RUVIO` command, which identifies the server.
   * Without `owner` the reply is the text `PONG is taken`. With `owner` the reply names the shard rule that decides which shard owns a key.
   *
   * @param owner When true, sends `RUVIO OWNER`.
   * @returns The reply text.
   *
   * @example
   * ```ts
   * await client.ruvio();       // 'PONG is taken'
   * await client.ruvio(true);   // describes the shard ownership rule
   * ```
   */
  ruvio(owner?: boolean): R<string> {
    return owner ? this._1(null, C.RUVIO, W.OWNER) : this._0(null, C.RUVIO);
  }
  /**
   * Moves the hash slots `first`..`last` to `shard` (counted from 0) of the same server, which
   * keeps serving them meanwhile (`RUVIO MOVESLOTS first-last shard`). Resolves once the move has
   * started; {@link moveSlotsStatus} follows it. One move runs at a time, and every slot of the
   * range must be on one shard.
   *
   * @returns `'OK'`.
   *
   * @example
   * ```ts
   * await client.moveSlots(0, 1000, 2);
   * ```
   */
  moveSlots(first: number, last: number, shard: number): R<'OK'> {
    return this._3(null, C.RUVIO, 'MOVESLOTS', `${first}-${last}`, shard);
  }
  /**
   * The running or last slot move (`RUVIO MOVESLOTS STATUS`). `state` is `none`, `copying`,
   * `cleaning`, `cancelling`, `done` or `cancelled`; the other names are `first`, `last`,
   * `from`, `to`, `keys`, `rounds`, `elapsed_ms`, `freeze_ms` and `error`.
   *
   * @example
   * ```ts
   * (await client.moveSlotsStatus()).state;   // 'done'
   * ```
   */
  moveSlotsStatus(): R<Record<string, string>> {
    return this._2(T.map, C.RUVIO, 'MOVESLOTS', 'STATUS');
  }
  /**
   * Checks that the server answers (`PING`).
   *
   * @param message Optional text to echo back.
   * @returns `'PONG'`, or `message` when one was given.
   *
   * @example
   * ```ts
   * await client.ping();        // 'PONG'
   * await client.ping('hi');    // 'hi'
   * ```
   */
  ping(message?: Value): R<string> {
    return message === undefined ? this._0(null, C.PING) : this._1(null, C.PING, message);
  }
  /**
   * Sends `HELLO [2|3] [AUTH user pass] [SETNAME name]` and returns the server's properties.
   * Switching to RESP3 on a shared client changes reply shapes for every caller; prefer the `protocol: 3` connection option, which does this during the handshake.
   *
   * @param protocol Protocol version, `2` or `3`. Omit to only read the server properties. An unknown version is a `NOPROTO` error and changes nothing.
   * @param opts `auth` carries `username` and `password` (the server authenticates the connection); `setName` sets the connection name. Both are sent only when `protocol` is given.
   * @returns A map of server properties (server, version, proto, mode, ...).
   * @throws {@link RuvioError} `NOPROTO` for an unsupported version; `WRONGPASS` for bad credentials.
   *
   * @example
   * ```ts
   * const info = await client.hello(3, { auth: { username: 'app', password: 's3cret' } });
   * ```
   */
  hello(protocol?: 2 | 3, opts?: { auth?: { username: string; password: string }; setName?: string }): R<Record<string, unknown>> {
    const a: Arg[] = [];
    if (protocol !== undefined) {
      a.push(protocol);
      if (opts?.auth) a.push(W.AUTH, opts.auth.username, opts.auth.password);
      if (opts?.setName !== undefined) a.push(W.SETNAME, opts.setName);
    }
    return this._v(T.map, C.HELLO, a);
  }
  /**
   * Authenticates the connection (`AUTH`). The client already does this on every (re)connect when you pass `username`/`password` options, so call it only to switch users.
   *
   * @param passwordOrUser The password (one argument), or the user name when `password` is also given.
   * @param password The password for the named user.
   * @returns `'OK'`.
   * @throws {@link RuvioError} `WRONGPASS` when the credentials are wrong.
   *
   * @example
   * ```ts
   * await client.auth('app', 's3cret');
   * ```
   */
  auth(passwordOrUser: string, password?: string): R<'OK'> {
    return password === undefined ? this._1(null, C.AUTH, passwordOrUser) : this._2(null, C.AUTH, passwordOrUser, password);
  }
  /**
   * Switches the logical database (`SELECT db`).
   * On the shared client this changes the database for every caller, and the client re-selects it after a reconnect. Use `duplicate({ db })` for an independent connection.
   *
   * @param db Database index: `0` up to `databases - 1` (16 by default, at most 256). A cluster-mode server allows only `0`.
   * @returns `'OK'`.
   * @throws {@link RuvioError} when `db` is out of range.
   *
   * @example
   * ```ts
   * await client.select(2);
   * ```
   */
  select(db: number): R<'OK'> {
    return this._1(null, C.SELECT, db);
  }
  /**
   * Returns the message unchanged (`ECHO`).
   *
   * @param message Text to echo.
   * @returns The same text.
   *
   * @example
   * ```ts
   * await client.echo('hello'); // 'hello'
   * ```
   */
  echo(message: Value): R<string> {
    return this._1(null, C.ECHO, message);
  }
  /**
   * Returns the whole command table (`COMMAND`).
   *
   * @returns One entry per command, in the Redis 7 shape with empty tips, key specs and subcommands.
   *
   * @example
   * ```ts
   * const table = await client.command();
   * ```
   */
  command(): R<unknown[]> {
    return this._0(null, C.COMMAND);
  }
  /**
   * Returns how many commands the server knows (`COMMAND COUNT`).
   *
   * @returns The command count.
   *
   * @example
   * ```ts
   * const n = await client.commandCount();
   * ```
   */
  commandCount(): R<number> {
    return this._0(null, C.COMMAND_COUNT);
  }
  /**
   * Returns table entries for the named commands (`COMMAND INFO name...`).
   *
   * @param names Command names.
   * @returns One entry per name.
   *
   * @example
   * ```ts
   * const [get] = await client.commandInfo('get');
   * ```
   */
  commandInfo(...names: string[]): R<unknown[]> {
    return this._v(null, C.COMMAND_INFO, names);
  }
  /**
   * Returns documentation entries for the named commands (`COMMAND DOCS [name...]`).
   * Ruvio returns empty maps; the call exists for client compatibility.
   *
   * @param names Command names; none asks for all.
   * @returns The raw reply.
   *
   * @example
   * ```ts
   * await client.commandDocs('get');
   * ```
   */
  commandDocs(...names: string[]): R<unknown> {
    return this._v(null, C.COMMAND_DOCS, names);
  }
  /**
   * Reads this connection's name (`CLIENT GETNAME`).
   *
   * @returns The name, or `null` when the connection has none.
   *
   * @example
   * ```ts
   * await client.clientGetname(); // null
   * ```
   */
  clientGetname(): R<string | null> {
    return this._0(null, C.CLIENT_GETNAME);
  }
  /**
   * Names this connection (`CLIENT SETNAME`). The name is not durable; set the `name` connection option to have it re-applied after every reconnect.
   *
   * @param name New name; an empty string clears it.
   * @returns `'OK'`.
   *
   * @example
   * ```ts
   * await client.clientSetname('worker-1');
   * ```
   */
  clientSetname(name: string): R<'OK'> {
    return this._1(null, C.CLIENT_SETNAME, name);
  }
  /**
   * Turns client-side-cache invalidation on or off (`CLIENT TRACKING ON|OFF`).
   * Turning it on needs RESP3 (`protocol: 3`). The server then records `GET` and `MGET` reads on this shard and sends an `invalidate` push when a tracked key is written. Listen with `client.on('push', ...)`. There is no REDIRECT, BCAST, PREFIX, OPTIN, OPTOUT or NOLOOP, and tracking does not cross shards.
   *
   * @param on `true` for `ON`, `false` for `OFF`.
   * @returns `'OK'`.
   * @throws {@link RuvioError} when enabling without RESP3.
   *
   * @example
   * ```ts
   * const c = createClient({ protocol: 3 });
   * c.on('push', (data) => console.log('invalidate', data));
   * await c.clientTracking(true);
   * await c.get('user:1');
   * ```
   */
  clientTracking(on: boolean): R<'OK'> {
    return this._1(null, C.CLIENT_TRACKING, on ? W.ON : W.OFF);
  }
  /**
   * Holds this connection's replies until `replicas` replicas have every record written before them on disk (`CLIENT DURABILITY REPLICAS n [TIMEOUT ms]`).
   * It is per connection and not re-applied after a reconnect; set the `durability` connection option for that. On timeout every reply of the batch becomes `-NOREPLICAS` and the writes stay applied. Refused inside `MULTI` and on a server without replication.
   *
   * @param replicas Replicas that must have the data on disk; `0` turns it off.
   * @param timeoutMs Longest wait in milliseconds (the server default is 5000 when omitted).
   * @returns `'OK'`.
   * @throws {@link RuvioError} `ERR` on a server without replication.
   *
   * @example
   * ```ts
   * await client.clientDurability(1, 2000);
   * ```
   */
  clientDurability(replicas: number, timeoutMs?: number): R<'OK'> {
    return timeoutMs === undefined
      ? this._2(null, C.CLIENT_DURABILITY, W.REPLICAS, replicas)
      : this._4(null, C.CLIENT_DURABILITY, W.REPLICAS, replicas, W.TIMEOUT, timeoutMs);
  }
  /**
   * Returns this connection's id (`CLIENT ID`). Ids are per shard, so on a multi-shard server two connections can share an id.
   *
   * @returns The connection id.
   *
   * @example
   * ```ts
   * const id = await client.clientId();
   * ```
   */
  clientId(): R<number> {
    return this._0(null, C.CLIENT_ID);
  }
  /**
   * Lists connections (`CLIENT LIST [TYPE type]`).
   * Each shard knows only its own connections, so on a multi-shard server the list covers the shard that got the command and ids can repeat across shards. `type` is validated but not used as a filter.
   *
   * @param type Connection type word, see {@link ClientType}: `normal`, `master`, `replica` or `pubsub`.
   * @returns One line of `key=value` fields per connection.
   *
   * @example
   * ```ts
   * const text = await client.clientList();
   * const pubsub = await client.clientList(ClientType.PubSub);
   * ```
   */
  clientList(type?: ClientType | (string & {})): R<string> {
    return type === undefined ? this._0(T.lines, C.CLIENT_LIST) : this._2(T.lines, C.CLIENT_LIST, W.TYPE, kw(KW_CLIENT_TYPE, type));
  }
  /**
   * Returns the calling connection's `CLIENT LIST` line (`CLIENT INFO`).
   *
   * @returns One line of `key=value` fields.
   *
   * @example
   * ```ts
   * const line = await client.clientInfo();
   * ```
   */
  clientInfo(): R<string> {
    return this._0(T.lines, C.CLIENT_INFO);
  }
  /**
   * Closes connections (`CLIENT KILL`).
   * Each shard knows only its own connections, so on a multi-shard server only the shard that got the command is searched. `USER`, `LADDR` and `TYPE` filters are not supported.
   *
   * @param target Legacy form: an `ip:port` string. Otherwise a {@link ClientKillOptions} object with `id`, `addr` and/or `skipMe`.
   * @returns `'OK'` for the legacy string form; the number of closed connections for the filter form.
   * @throws {@link RuvioError} for the legacy form when no such connection exists.
   *
   * @example
   * ```ts
   * await client.clientKill({ id: 42 });          // 1
   * await client.clientKill({ addr: '10.0.0.5:51234', skipMe: true });
   * ```
   */
  clientKill(target: string | ClientKillOptions): R<number | 'OK'> {
    if (typeof target === 'string') return this._1(null, C.CLIENT_KILL, target);
    const a: Arg[] = [];
    if (target.id !== undefined) a.push(W.ID, target.id);
    if (target.addr !== undefined) a.push(W.ADDR, target.addr);
    if (target.skipMe !== undefined) a.push(W.SKIPME, target.skipMe ? W.YES : W.NO);
    return this._v(null, C.CLIENT_KILL, a);
  }
  /**
   * Reads the slow log (`SLOWLOG GET [count]`).
   * Only commands that take the generic dispatcher are timed (not the GET/SET/INCR/PING fast path), and blocking commands are skipped. Each entry holds the command name and its first key, not every argument. The log is per shard; configure it with `slowlog_threshold_micros` and `slowlog_max_len` in the server config.
   *
   * @param count Maximum entries, newest first. Omit for the server default.
   * @returns Raw slow log entries.
   *
   * @example
   * ```ts
   * const entries = await client.slowlogGet(10);
   * ```
   */
  slowlogGet(count?: number): R<unknown[]> {
    return count === undefined ? this._0(null, C.SLOWLOG_GET) : this._1(null, C.SLOWLOG_GET, count);
  }
  /**
   * Returns the number of entries in the slow log (`SLOWLOG LEN`).
   *
   * @returns The entry count.
   *
   * @example
   * ```ts
   * await client.slowlogLen();
   * ```
   */
  slowlogLen(): R<number> {
    return this._0(null, C.SLOWLOG_LEN);
  }
  /**
   * Empties the slow log (`SLOWLOG RESET`).
   *
   * @returns `'OK'`.
   *
   * @example
   * ```ts
   * await client.slowlogReset();
   * ```
   */
  slowlogReset(): R<'OK'> {
    return this._0(null, C.SLOWLOG_RESET);
  }
  /**
   * Sends `LATENCY LATEST`. Ruvio records no latency-spike events, so this is always empty; use `INFO latencystats` instead.
   *
   * @returns An empty array.
   *
   * @example
   * ```ts
   * await client.latencyLatest(); // []
   * ```
   */
  latencyLatest(): R<unknown[]> {
    return this._0(null, C.LATENCY_LATEST);
  }

  // ---- acl -------------------------------------------------------------------------------

  /**
   * Returns the user name this connection is authenticated as (`ACL WHOAMI`).
   *
   * @returns The user name, e.g. `'default'`.
   *
   * @example
   * ```ts
   * await client.aclWhoami();
   * ```
   */
  aclWhoami(): R<string> {
    return this._0(null, C.ACL_WHOAMI);
  }
  /**
   * Lists user names (`ACL USERS`), including users added with {@link aclSetuser} until restart.
   *
   * @returns User names.
   *
   * @example
   * ```ts
   * await client.aclUsers(); // ['default', 'app']
   * ```
   */
  aclUsers(): R<string[]> {
    return this._0(null, C.ACL_USERS);
  }
  /**
   * Lists users as ACL rule lines (`ACL LIST`). Passwords are redacted.
   *
   * @returns One rule line per user.
   *
   * @example
   * ```ts
   * await client.aclList();
   * ```
   */
  aclList(): R<string[]> {
    return this._0(null, C.ACL_LIST);
  }
  /**
   * Describes one user (`ACL GETUSER`). Passwords are counted, not echoed.
   *
   * @param user User name.
   * @returns A map of the user's flags, commands and key patterns, or `null` when the user does not exist.
   *
   * @example
   * ```ts
   * const u = await client.aclGetuser('app');
   * ```
   */
  aclGetuser(user: string): R<Record<string, unknown> | null> {
    return this._1((v) => (v === null ? null : T.map(v)), C.ACL_GETUSER, user);
  }
  /**
   * Lists ACL command categories (`ACL CAT [category]`). Only Ruvio's own categories exist.
   *
   * @param category When given, lists the commands in that category instead.
   * @returns Category or command names.
   *
   * @example
   * ```ts
   * await client.aclCat();
   * ```
   */
  aclCat(category?: string): R<string[]> {
    return category === undefined ? this._0(null, C.ACL_CAT) : this._1(null, C.ACL_CAT, category);
  }
  /**
   * Creates or changes a user with incremental rules (`ACL SETUSER user rule...`).
   * A new user starts with no password, commands or keys. If any rule fails nothing changes. Open connections keep the rules from their last AUTH. Rules must be UTF-8. Changes live in memory until {@link aclSave} writes them to `security.acl_file`.
   *
   * @param user User name.
   * @param rules ACL rules, e.g. `'on'`, `'>secret'`, `'+get'`, `'~app:*'`.
   * @returns `'OK'`.
   * @throws {@link RuvioError} when a rule is invalid.
   *
   * @example
   * ```ts
   * await client.aclSetuser('app', 'on', '>secret', '+get', '+set', '~app:*');
   * ```
   */
  aclSetuser(user: string, ...rules: string[]): R<'OK'> {
    return this._kv(null, C.ACL_SETUSER, user, rules);
  }
  /**
   * Replaces the live user list from `security.acl_file` (`ACL LOAD`). A missing path or a bad line changes nothing. Open connections keep the rules from their last AUTH.
   *
   * @returns `'OK'`.
   * @throws {@link RuvioError} when the file is missing or invalid.
   *
   * @example
   * ```ts
   * await client.aclLoad();
   * ```
   */
  aclLoad(): R<'OK'> {
    return this._0(null, C.ACL_LOAD);
  }
  /**
   * Writes the live user list to `security.acl_file` (`ACL SAVE`), including passwords. A missing path changes nothing.
   *
   * @returns `'OK'`.
   * @throws {@link RuvioError} when `security.acl_file` is not set.
   *
   * @example
   * ```ts
   * await client.aclSave();
   * ```
   */
  aclSave(): R<'OK'> {
    return this._0(null, C.ACL_SAVE);
  }

  // ---- strings ---------------------------------------------------------------------------

  /**
   * Reads a string value (`GET`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @returns The value, or `null` when the key does not exist. Use {@link getBuffer} for binary data.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.set('user:1', 'Ada');
   * await client.get('user:1'); // 'Ada'
   * await client.get('nope');   // null
   * ```
   */
  get(key: Key): R<string | null> {
    return this._1(null, C.GET, key);
  }
  /**
   * Like {@link get}, but returns the raw bytes.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @returns The value as a Buffer, or `null` when the key does not exist.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * const png = await client.getBuffer('image:1');
   * ```
   */
  getBuffer(key: Key): R<Buffer | null> {
    return this._bin(() => this._1(null, C.GET, key));
  }
  /**
   * Stores a string (`SET key value [EX|PX|EXAT|PXAT|KEEPTTL] [NX|XX] [GET]`), or Ruvio's compare-and-set form `SET key value IFEQ expected [...]`.
   * Only one expiry option is sent, in the order `ex`, `px`, `exat`, `pxat`, `keepTtl`; `nx` wins over `xx`. A plain `set` removes any existing TTL. `ifEq`, `ifGt` and `ifLt` cannot be combined with `nx`, `xx` or `get`.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param value Value to store: string, Buffer or number (numbers are sent as ASCII text).
   * @param opts See {@link SetOptions}: `ex`/`px`/`exat`/`pxat`/`keepTtl` for expiry, `nx`/`xx` for conditions, `get` to return the old value, `ifEq` for compare-and-set.
   * @returns `'OK'` when written. `null` when `nx`/`xx`/`ifEq`/`ifGt`/`ifLt` blocked the write. With `get`, the previous value instead (`null` if there was none).
   * @throws {@link RuvioError} `WRONGTYPE` when `get` is used and the key holds a non-string; `ERR` for an invalid expiry or an invalid option mix.
   *
   * @example
   * ```ts
   * await client.set('user:1', 'Ada');
   * await client.set('session:9', 'tok', { ex: 3600 });          // expires in an hour
   * await client.set('lock:a', 'me', { nx: true, px: 5000 });    // null if already held
   * await client.set('cfg', 'v2', { ifEq: 'v1' });               // null if cfg is not 'v1'
   * await client.set('watermark', 1700000000, { ifGt: true });   // null unless greater than the stored number
   * ```
   */
  set(key: Key, value: Value, opts?: SetOptions): R<string | null> {
    if (opts === undefined) return this._2(null, C.SET, key, value);
    const a: Arg[] = [key, value];
    if (opts.ifEq !== undefined) a.push(W.IFEQ, opts.ifEq);
    else if (opts.ifGt) a.push(W.IFGT);
    else if (opts.ifLt) a.push(W.IFLT);
    if (opts.ex !== undefined) a.push(W.EX, opts.ex);
    else if (opts.px !== undefined) a.push(W.PX, opts.px);
    else if (opts.exat !== undefined) a.push(W.EXAT, opts.exat);
    else if (opts.pxat !== undefined) a.push(W.PXAT, opts.pxat);
    else if (opts.keepTtl) a.push(W.KEEPTTL);
    if (opts.nx) a.push(W.NX);
    else if (opts.xx) a.push(W.XX);
    if (opts.get) a.push(W.GET);
    return this._v(null, C.SET, a);
  }
  /**
   * Writes a versioned string (`SETV key value [version]`, Ruvio extension) for optimistic concurrency.
   * The first call without `version` creates the key at version 1. Every later call must pass the current version and receives the next one. A mismatch is an error and leaves the value unchanged. A plain {@link set} replaces the versioned string.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param value Value to store: string, Buffer or number (numbers are sent as ASCII text).
   * @param version The version you last read. Omit only when creating the key.
   * @returns The new version.
   * @throws {@link RuvioError} when `version` does not match the stored version.
   *
   * @example
   * ```ts
   * const v1 = await client.setv('doc:1', 'a');        // 1
   * const v2 = await client.setv('doc:1', 'b', v1);    // 2
   * ```
   */
  setv(key: Key, value: Value, version?: number): R<number> {
    return version === undefined ? this._2(null, C.SETV, key, value) : this._3(null, C.SETV, key, value, version);
  }
  /**
   * Takes `count` from an integer counter only if enough is left (`TAKE key count`, Ruvio extension). Handy for stock or quota.
   *
   * @param key Key holding a decimal integer.
   * @param count Positive amount to subtract.
   * @returns The remainder after the subtraction, or `null` when the key is missing or holds less than `count`. In the `null` case nothing is created or changed.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.set('stock:42', 3);
   * await client.take('stock:42', 2); // 1
   * await client.take('stock:42', 2); // null, still 1
   * ```
   */
  take(key: Key, count: number): R<number | null> {
    return this._2(null, C.TAKE, key, count);
  }
  /**
   * Moves an amount between two integer keys in one step (`TRANSFER source destination amount [MAX max]`, Ruvio extension). Both writes are one WAL record; a missing `destination` starts at 0.
   *
   * @param source Key holding a decimal integer; it must hold at least `amount`.
   * @param destination Integer key that receives the amount. On a cluster it must share a slot with `source` (use a hash tag such as `{user:42}`).
   * @param amount Positive amount to move.
   * @param max Optional ceiling for `destination` after the move.
   * @returns `[sourceAfter, destinationAfter]`, or `null` when `source` is missing or short, or `destination` would overflow or pass `max`. Nothing is written in the `null` case.
   * @throws {@link RuvioError} `WRONGTYPE` for another type, an error when a value is not an integer, `amount` is not positive, or the keys are the same.
   *
   * @example
   * ```ts
   * await client.set('{u}:wallet', 50);
   * await client.transfer('{u}:wallet', '{u}:escrow', 30); // [20, 30]
   * await client.transfer('{u}:wallet', '{u}:escrow', 30); // null, nothing changed
   * ```
   */
  transfer(source: Key, destination: Key, amount: number, max?: number): R<[number, number] | null> {
    return max === undefined
      ? this._3(T.balances, C.TRANSFER, source, destination, amount)
      : this._v(T.balances, C.TRANSFER, [source, destination, amount, W.MAX, max]);
  }
  /**
   * Moves an amount between two integer fields of one hash (`HTRANSFER key from to amount [MAX max]`, Ruvio extension). A missing `to` field starts at 0 and field TTLs stay.
   *
   * @param key Hash key.
   * @param from Field to subtract from.
   * @param to Field to add to.
   * @param amount Positive amount to move.
   * @param max Optional ceiling for the `to` field after the move.
   * @returns `[fromAfter, toAfter]`, or `null` when the key or `from` is missing or short, or `to` would overflow or pass `max`. Nothing is written in the `null` case.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` is not a hash, an error when a field is not an integer.
   *
   * @example
   * ```ts
   * await client.hset('acct', { available: 50 });
   * await client.htransfer('acct', 'available', 'held', 30); // [20, 30]
   * ```
   */
  htransfer(key: Key, from: Key, to: Key, amount: number, max?: number): R<[number, number] | null> {
    return max === undefined
      ? this._4(T.balances, C.HTRANSFER, key, from, to, amount)
      : this._v(T.balances, C.HTRANSFER, [key, from, to, amount, W.MAX, max]);
  }
  /**
   * Deletes a string only if it equals the given value (`DELEX key IFEQ value`, Ruvio extension). The safe way to release a lock you own.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param ifEq Expected current value.
   * @returns `true` when deleted; `false` when the key is missing or the value differs.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * const deleted = await client.delex('lock:a', myToken);
   * ```
   */
  delex(key: Key, ifEq: Value): R<boolean> {
    return this._3(T.bool, C.DELEX, key, W.IFEQ, ifEq);
  }
  /**
   * Reads a string and sets its expiry in one step (`GETEX key ttlMs`, the Ruvio form: milliseconds, no EX/PX words).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param ttlMs New time to live in milliseconds from now.
   * @returns The value, or `null` when the key is missing (nothing is stored).
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.getex('session:9', 60_000); // value, now expires in a minute
   * ```
   */
  getex(key: Key, ttlMs: number): R<string | null> {
    return this._2(null, C.GETEX, key, ttlMs);
  }
  /**
   * Reads several strings (`MGET`).
   * On a multi-shard server the keys must share a slot (use `{tag}` in the names) or the server answers `CROSSSLOT`. A {@link ClusterClient} splits the call per slot instead; that is not atomic.
   *
   * @param keys Keys to operate on.
   * @returns One entry per key, in order: the value, or `null` when the key is missing or holds another type.
   *
   * @example
   * ```ts
   * const [a, b] = await client.mget('a', 'b'); // e.g. ['1', null]
   * ```
   */
  mget(...keys: Key[]): R<(string | null)[]> {
    return this._v(null, C.MGET, keys);
  }
  /**
   * Like {@link mget}, but values come back as Buffers.
   *
   * @param keys Keys to operate on.
   * @returns One entry per key: the raw value, or `null`.
   *
   * @example
   * ```ts
   * const [raw] = await client.mgetBuffer('blob:1', 'blob:2');
   * ```
   */
  mgetBuffer(...keys: Key[]): R<(Buffer | null)[]> {
    return this._bin(() => this._v(null, C.MGET, keys));
  }
  /**
   * Writes several strings (`MSET`). Each key's TTL is cleared.
   * On a multi-shard server the keys must share a slot or the server answers `CROSSSLOT`. A {@link ClusterClient} splits the call per slot; that is not atomic.
   *
   * @param pairs An object `{ key: value }` or an array of `[key, value]` pairs (use pairs for Buffer keys).
   * @returns `'OK'`.
   *
   * @example
   * ```ts
   * await client.mset({ a: 1, b: 2 });
   * await client.mset([['x', 'y'], ['p', 'q']]);
   * ```
   */
  mset(pairs: Record<string, Value> | ReadonlyArray<readonly [Key, Value]>): R<'OK'> {
    return this._v(null, C.MSET, flatPairs([], pairs));
  }
  /**
   * Writes several strings only if none of the keys exists (`MSETNX`). All or nothing.
   * Keys must share a slot on a multi-shard server.
   *
   * @param pairs An object `{ key: value }` or an array of `[key, value]` pairs.
   * @returns `true` when all were written; `false` when at least one key existed (nothing written).
   *
   * @example
   * ```ts
   * await client.msetnx({ a: 1, b: 2 });
   * ```
   */
  msetnx(pairs: Record<string, Value> | ReadonlyArray<readonly [Key, Value]>): R<boolean> {
    return this._v(T.bool, C.MSETNX, flatPairs([], pairs));
  }
  /**
   * Replaces a string and returns the old one (`GETSET`). Clears the TTL. Prefer {@link set} with `get: true`.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param value Value to store: string, Buffer or number (numbers are sent as ASCII text).
   * @returns The previous value, or `null` when the key did not exist.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * const old = await client.getset('counter', 0);
   * ```
   */
  getset(key: Key, value: Value): R<string | null> {
    return this._2(null, C.GETSET, key, value);
  }
  /**
   * Appends to a string, creating it when missing (`APPEND`). An existing TTL is kept.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param value Text to append.
   * @returns The string length in bytes after the append.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.append('log', 'line1\n'); // 6
   * ```
   */
  append(key: Key, value: Value): R<number> {
    return this._2(null, C.APPEND, key, value);
  }
  /**
   * Returns the length of a string in bytes (`STRLEN`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @returns The length, or `0` when the key does not exist.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.strlen('user:1');
   * ```
   */
  strlen(key: Key): R<number> {
    return this._1(null, C.STRLEN, key);
  }
  /**
   * Adds 1 to an integer string, creating it as `0` first when missing (`INCR`).
   * Values are signed 64-bit; integers beyond 2^53 arrive as `bigint` at runtime even though the type says `number`.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param opts {@link TtlOptions}: `ttl` in seconds, applied only when this call creates the key (`INCR key EX|PX ...`).
   * @returns The new value.
   * @throws {@link RuvioError} `ERR` when the value is not an integer or the result overflows 64 bits.
   * @throws RangeError when `opts.ttl` is not a positive finite number (the call rejects before sending).
   *
   * @example
   * ```ts
   * await client.incr('hits'); // 1
   * await client.incr('hits'); // 2
   * await client.incr('hits:today', { ttl: 86_400 }); // the deadline is set once, on creation
   * ```
   */
  incr(key: Key, opts?: TtlOptions): R<number> {
    return opts?.ttl === undefined ? this._1(null, C.INCR, key) : this._v(null, C.INCR, pushTtl([key], opts.ttl));
  }
  /**
   * Subtracts 1 from an integer string, creating it as `0` first when missing (`DECR`). Signed 64-bit.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param opts {@link TtlOptions}: `ttl` in seconds, applied only when this call creates the key.
   * @returns The new value.
   * @throws {@link RuvioError} `ERR` when the value is not an integer or the result overflows.
   * @throws RangeError when `opts.ttl` is not a positive finite number (the call rejects before sending).
   *
   * @example
   * ```ts
   * await client.decr('stock');
   * ```
   */
  decr(key: Key, opts?: TtlOptions): R<number> {
    return opts?.ttl === undefined ? this._1(null, C.DECR, key) : this._v(null, C.DECR, pushTtl([key], opts.ttl));
  }
  /**
   * Adds an integer to an integer string, creating it as `0` first when missing (`INCRBY`). Signed 64-bit.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param delta Whole number to add (may be negative).
   * @param opts {@link TtlOptions}: `ttl` in seconds, applied only when this call creates the key.
   * @returns The new value.
   * @throws {@link RuvioError} `ERR` when the value is not an integer or the result overflows.
   * @throws RangeError when `opts.ttl` is not a positive finite number (the call rejects before sending).
   *
   * @example
   * ```ts
   * await client.incrby('score', 10);
   * ```
   */
  incrby(key: Key, delta: number, opts?: TtlOptions): R<number> {
    return opts?.ttl === undefined ? this._2(null, C.INCRBY, key, delta) : this._v(null, C.INCRBY, pushTtl([key, delta], opts.ttl));
  }
  /**
   * Adds to a counter but refuses to pass a ceiling (`INCRBY key delta MAX limit`, Ruvio extension).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param delta Whole number to add.
   * @param limit Highest value the counter may reach.
   * @param opts {@link TtlOptions}: `ttl` in seconds, applied only when this call creates the key (`INCRBY key delta EX|PX ... MAX limit`).
   * @returns The new value, or `null` (and nothing is written) when the result would exceed `limit`.
   * @throws {@link RuvioError} `ERR` when the value is not an integer.
   * @throws RangeError when `opts.ttl` is not a positive finite number (the call rejects before sending).
   *
   * @example
   * ```ts
   * await client.incrbyMax('seats', 1, 100); // null once 100 is reached
   * ```
   */
  incrbyMax(key: Key, delta: number, limit: number, opts?: TtlOptions): R<number | null> {
    if (opts?.ttl === undefined) return this._4(null, C.INCRBY, key, delta, W.MAX, limit);
    return this._v(null, C.INCRBY, pushTtl([key, delta], opts.ttl).concat(W.MAX, limit));
  }
  /**
   * Subtracts an integer from an integer string, creating it as `0` first when missing (`DECRBY`). Signed 64-bit.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param delta Whole number to subtract.
   * @param opts {@link TtlOptions}: `ttl` in seconds, applied only when this call creates the key.
   * @returns The new value.
   * @throws {@link RuvioError} `ERR` when the value is not an integer or the result overflows.
   * @throws RangeError when `opts.ttl` is not a positive finite number (the call rejects before sending).
   *
   * @example
   * ```ts
   * await client.decrby('stock', 5);
   * ```
   */
  decrby(key: Key, delta: number, opts?: TtlOptions): R<number> {
    return opts?.ttl === undefined ? this._2(null, C.DECRBY, key, delta) : this._v(null, C.DECRBY, pushTtl([key, delta], opts.ttl));
  }
  /**
   * Stores a string with a TTL in seconds (`SETEX`). Same as `set(key, value, { ex: seconds })`.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param seconds Time to live in seconds; must be positive.
   * @param value Value to store: string, Buffer or number (numbers are sent as ASCII text).
   * @returns `'OK'`.
   * @throws {@link RuvioError} `ERR` when `seconds` is not positive.
   *
   * @example
   * ```ts
   * await client.setex('otp:7', 300, '481516');
   * ```
   */
  setex(key: Key, seconds: number, value: Value): R<'OK'> {
    return this._3(null, C.SETEX, key, seconds, value);
  }
  /**
   * Stores a string with a TTL in milliseconds (`PSETEX`). Same as `set(key, value, { px: ms })`.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param ms Time to live in milliseconds; must be positive.
   * @param value Value to store: string, Buffer or number (numbers are sent as ASCII text).
   * @returns `'OK'`.
   * @throws {@link RuvioError} `ERR` when `ms` is not positive.
   *
   * @example
   * ```ts
   * await client.psetex('otp:7', 5000, '481516');
   * ```
   */
  psetex(key: Key, ms: number, value: Value): R<'OK'> {
    return this._3(null, C.PSETEX, key, ms, value);
  }
  /**
   * Stores a string only when the key does not exist (`SETNX`). Prefer {@link set} with `nx: true`, which can also set a TTL.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param value Value to store: string, Buffer or number (numbers are sent as ASCII text).
   * @returns `true` when written, `false` when the key already existed.
   *
   * @example
   * ```ts
   * await client.setnx('init', '1');
   * ```
   */
  setnx(key: Key, value: Value): R<boolean> {
    return this._2(T.bool, C.SETNX, key, value);
  }
  /**
   * Reads a string and deletes the key (`GETDEL`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @returns The value, or `null` when the key did not exist.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * const token = await client.getdel('once:abc');
   * ```
   */
  getdel(key: Key): R<string | null> {
    return this._1(null, C.GETDEL, key);
  }
  /**
   * Adds a decimal number to a string value (`INCRBYFLOAT`). The TTL is kept.
   * Computed in double precision and printed with 16 significant digits; Redis uses long double, so results with more digits can differ in the last places.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param delta Number to add (may be negative).
   * @param opts {@link TtlOptions}: `ttl` in seconds, applied only when this call creates the key.
   * @returns The new value.
   * @throws {@link RuvioError} `ERR` when the value is not a number or the result is not finite.
   * @throws RangeError when `opts.ttl` is not a positive finite number (the call rejects before sending).
   *
   * @example
   * ```ts
   * await client.incrbyfloat('balance', 0.5);
   * ```
   */
  incrbyfloat(key: Key, delta: number, opts?: TtlOptions): R<number> {
    return opts?.ttl === undefined ? this._2(T.float, C.INCRBYFLOAT, key, delta) : this._v(T.float, C.INCRBYFLOAT, pushTtl([key, delta], opts.ttl));
  }
  /**
   * Returns part of a string (`GETRANGE`).
   * A range wholly before the start of the string gives an empty string (Redis 7.0 returns the first byte).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param start First byte (inclusive); negative counts from the end.
   * @param end Last byte (inclusive); negative counts from the end.
   * @returns The substring, or `''` when the key is missing or the range is empty.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.getrange('greeting', 0, 4); // 'Hello'
   * ```
   */
  getrange(key: Key, start: number, end: number): R<string> {
    return this._3(null, C.GETRANGE, key, start, end);
  }
  /**
   * Old name for {@link getrange}; same behaviour.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param start First byte (inclusive); negative counts from the end.
   * @param end Last byte (inclusive); negative counts from the end.
   * @returns The substring, or `''`.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.substr('greeting', 0, 4);
   * ```
   */
  substr(key: Key, start: number, end: number): R<string> {
    return this._3(null, C.SUBSTR, key, start, end);
  }
  /**
   * Overwrites part of a string starting at a byte offset, padding with zero bytes when needed (`SETRANGE`). The TTL is kept.
   * The string is limited to 16 MiB, and each call rewrites the whole value (cost grows with the value size).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param offset Byte position to start writing at; zero or more.
   * @param value Bytes to write.
   * @returns The string length after the write.
   * @throws {@link RuvioError} `ERR` when the result would exceed 16 MiB.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.setrange('greeting', 6, 'World');
   * ```
   */
  setrange(key: Key, offset: number, value: Value): R<number> {
    return this._3(null, C.SETRANGE, key, offset, value);
  }
  /**
   * Starts or joins a cache fill (`FILLGET key fillMs [WAIT waitMs]`, Ruvio extension). Most code should use {@link getOrCreate}, which wraps the full protocol.
   * The reply tells you what to do: `hit` (use `value`), `fill` (you build the value, then call {@link fillset} with `token`), `busy` (someone else is filling; ask again later) or `stale` (serve `value`; if `token` is set, you should refresh). Fill markers are memory-only and lapse after `fillMs`.
   * `waitMs` parks the whole shared connection while it waits: leave it unset on a shared client.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param fillMs How long the fill marker lives, i.e. the time you have to build the value (ms).
   * @param waitMs Optional: hold a would-be `busy` caller up to this long (the server retries every 5 ms) until the value is written or the marker lapses.
   * @returns A {@link FillResult}.
   * @throws {@link RuvioError} when `waitMs` is used inside MULTI or a script.
   *
   * @example
   * ```ts
   * const r = await client.fillget('page:home', 10_000);
   * if (r.state === 'fill') {
   *   const html = await render();
   *   await client.fillset('page:home', html, 60_000, r.token!);
   * }
   * ```
   */
  fillget(key: Key, fillMs: number, waitMs?: number): R<FillResult> {
    return waitMs === undefined
      ? this._2(T.fill, C.FILLGET, key, fillMs)
      : this._4(T.fill, C.FILLGET, key, fillMs, W.WAIT, waitMs);
  }
  /**
   * Stores the value for a fill started with {@link fillget} (`FILLSET key value ttlMs token [SWR staleMs]`, Ruvio extension).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param value Value to cache.
   * @param ttlMs Fresh lifetime in milliseconds.
   * @param token The token from the `fill` (or `stale`) result of {@link fillget}.
   * @param staleMs Optional: keep the value readable as `stale` this long after `ttlMs` while one caller refreshes it. The soft expiry is not persisted.
   * @returns `true` when the token was still live and the value was written; `false` otherwise (for example the marker lapsed).
   * @throws {@link RuvioError} `ERR` for an invalid TTL.
   *
   * @example
   * ```ts
   * await client.fillset('page:home', html, 60_000, token, 30_000);
   * ```
   */
  fillset(key: Key, value: Value, ttlMs: number, token: Value, staleMs?: number): R<boolean> {
    if (staleMs === undefined) return this._4(T.bool, C.FILLSET, key, value, ttlMs, token);
    return this._v(T.bool, C.FILLSET, [key, value, ttlMs, token, W.SWR, staleMs]);
  }

  // ---- Ruvio coordination primitives -----------------------------------------------------

  /**
   * Fixed-window rate limit on one key (`LIMIT key max windowMs`, Ruvio extension). Most code can use {@link rateLimit}.
   * The first call opens a window of `windowMs`; each accepted call uses one of `max`. Rejected calls do not count. The key disappears when the window ends.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param max Calls allowed per window.
   * @param windowMs Window length in milliseconds.
   * @returns The calls left in the window after this one (`0` means this was the last), or `null` when the window is full and the call is refused.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * const left = await client.limit('rl:user:7', 100, 60_000);
   * if (left === null) throw new Error('slow down');
   * ```
   */
  limit(key: Key, max: number, windowMs: number): R<number | null> {
    return this._3(null, C.LIMIT, key, max, windowMs);
  }
  /**
   * Sliding-window rate limit on one key (`LIMIT key max windowMs SLIDING`, Ruvio extension).
   * The previous window counts for the part of it that still overlaps the last `windowMs`, so a burst at a window edge is not doubled.
   * The key keeps two counters, not one entry per call, and keeps the mode it was created with.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param max Calls allowed per window, at most 2147483647.
   * @param windowMs Window length in milliseconds.
   * @returns The calls left after this one, or `null` when the call is refused (nothing is counted).
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds another type, or when it was created by another `LIMIT` mode.
   *
   * @example
   * ```ts
   * if ((await client.limitSliding('rl:user:7', 100, 60_000)) === null) throw new Error('slow down');
   * ```
   */
  limitSliding(key: Key, max: number, windowMs: number): R<number | null> {
    return this._v(null, C.LIMIT, [key, max, windowMs, W.SLIDING]);
  }
  /**
   * Token-bucket rate limit on one key (`LIMIT key capacity refillMs TOKENS COST cost`, Ruvio extension).
   * The bucket holds `capacity` tokens and refills completely over `refillMs`; a call takes `cost` tokens.
   * The key keeps one timestamp and disappears when the bucket is full again.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param capacity Bucket size, 1 to 1000000.
   * @param refillMs Time to refill an empty bucket in milliseconds, up to 4294967295.
   * @param cost Tokens this call takes, 1 to `capacity` (default 1).
   * @returns The tokens left after this call, or `null` when the bucket holds too few (nothing is taken).
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds another type, or when it was created by another `LIMIT` mode.
   *
   * @example
   * ```ts
   * const left = await client.limitTokens('rl:api:7', 10, 1_000, 2); // 10 per second, this call costs 2
   * ```
   */
  limitTokens(key: Key, capacity: number, refillMs: number, cost = 1): R<number | null> {
    return this._v(null, C.LIMIT, [key, capacity, refillMs, W.TOKENS, W.COST, cost]);
  }
  /**
   * {@link limitSliding} with `WITHRETRY`: the answer also says how long to wait.
   *
   * @param key Key to operate on.
   * @param max Calls allowed per window.
   * @param windowMs Window length in milliseconds.
   * @returns A {@link LimitOutcome}; `retryAfterMs` is the wait until the same call would pass.
   * @throws {@link RuvioError} as {@link limitSliding}.
   */
  limitSlidingWithRetry(key: Key, max: number, windowMs: number): R<LimitOutcome> {
    return this._v(limitOutcome, C.LIMIT, [key, max, windowMs, W.SLIDING, W.WITHRETRY]);
  }
  /**
   * {@link limitTokens} with `WITHRETRY`: the answer also says how long to wait.
   *
   * @param key Key to operate on.
   * @param capacity Bucket size.
   * @param refillMs Time to refill an empty bucket in milliseconds.
   * @param cost Tokens this call takes (default 1).
   * @returns A {@link LimitOutcome}; `retryAfterMs` is the wait until the same call would pass.
   * @throws {@link RuvioError} as {@link limitTokens}.
   */
  limitTokensWithRetry(key: Key, capacity: number, refillMs: number, cost = 1): R<LimitOutcome> {
    return this._v(limitOutcome, C.LIMIT, [key, capacity, refillMs, W.TOKENS, W.COST, cost, W.WITHRETRY]);
  }
  /**
   * Starts an idempotent operation (`IDEM key BEGIN fingerprint owner ttlMs`, Ruvio extension). Most code should use {@link idempotent}.
   * Atomically reserves a missing key, or reports what is already there. It is not exactly-once for external effects.
   *
   * @param key Idempotency key.
   * @param fingerprint Hash of the request; the same key with a different fingerprint is a `conflict`. 1 to 128 bytes.
   * @param owner Identifier of this attempt. 1 to 128 bytes.
   * @param ttlMs Retention in milliseconds, 1 ms to 7 days. Neither retries nor completion renew it.
   * @returns An {@link IdemBeginResult}: `acquired` (you run the work, then {@link idemComplete}), `pending` (another owner is running), `conflict` (different fingerprint) or `completed` (with the stored `result`).
   * @throws {@link RuvioError} `ERR` for an invalid token length or TTL.
   *
   * @example
   * ```ts
   * const r = await client.idemBegin('pay:123', hash, owner, 86_400_000);
   * if (r.state === 'acquired') { doWork(); }
   * ```
   */
  idemBegin(key: Key, fingerprint: Value, owner: Value, ttlMs: number): R<IdemBeginResult> {
    return this._v(T.idem, C.IDEM, [key, W.BEGIN, fingerprint, owner, ttlMs]);
  }
  /**
   * Publishes the result of an idempotent operation (`IDEM key COMPLETE fingerprint owner result`, Ruvio extension).
   * Only the matching live owner can publish. An identical repeat is acknowledged without rewriting; a different result is refused.
   *
   * @param key Idempotency key.
   * @param fingerprint Same fingerprint used in {@link idemBegin}.
   * @param owner Same owner used in {@link idemBegin}.
   * @param result Result to store; at most 4 MiB.
   * @returns `1` when published (or identical to the stored result), `0` otherwise.
   *
   * @example
   * ```ts
   * await client.idemComplete('pay:123', hash, owner, JSON.stringify(receipt));
   * ```
   */
  idemComplete(key: Key, fingerprint: Value, owner: Value, result: Value): R<number> {
    return this._v(null, C.IDEM, [key, W.COMPLETE, fingerprint, owner, result]);
  }
  /**
   * Marks "first time only" (`ONCE key ttlMs`, Ruvio extension).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param ttlMs How long the marker lives, in milliseconds. A later call does not refresh it.
   * @returns `true` for the first call within the TTL, `false` for later calls. After the TTL the next call is first again.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * if (await client.once('welcome-email:7', 86_400_000)) await sendEmail();
   * ```
   */
  once(key: Key, ttlMs: number): R<boolean> {
    return this._2(T.bool, C.ONCE, key, ttlMs);
  }
  /**
   * Takes a fenced lease (`LEASE key ttlMs`, Ruvio extension). Most code should use {@link acquireLease}.
   * Tokens increase on every grant, also after expiry or {@link release}, so downstream systems can reject writes from an old holder. `DEL` on the key restarts the sequence at 1.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param ttlMs Lease length in milliseconds.
   * @returns A new fencing token, or `null` while another holder has the lease.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * const token = await client.lease('job:nightly', 30_000);
   * if (token !== null) { useToken(token); }
   * ```
   */
  lease(key: Key, ttlMs: number): R<number | null> {
    return this._2(T.intOrNull, C.LEASE, key, ttlMs);
  }
  /**
   * Takes one slot of a counting semaphore (`SEMAPHORE key max ttlMs`, Ruvio extension). Most code should use {@link acquireSemaphore}.
   * Each holder has its own TTL; the key stores 16 bytes per live holder and vanishes with the last one.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param max Maximum concurrent holders.
   * @param ttlMs How long this holder's slot lasts, in milliseconds.
   * @returns A holder token as a decimal string (it can exceed 2^53), or `null` when all `max` slots are taken.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * const token = await client.semaphore('pool:db', 10, 30_000);
   * if (token !== null) await client.release('pool:db', token);
   * ```
   */
  semaphore(key: Key, max: number, ttlMs: number): R<string | null> {
    return this._3(T.token, C.SEMAPHORE, key, max, ttlMs);
  }
  /**
   * Releases a lease or a semaphore slot (`RELEASE key token`, Ruvio extension).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param token The token returned by {@link lease} or {@link semaphore}.
   * @returns `true` when released; `false` when the token does not match the current holder (nothing changes).
   *
   * @example
   * ```ts
   * await client.release('job:nightly', token);
   * ```
   */
  release(key: Key, token: Value | bigint): R<boolean> {
    return this._2(T.bool, C.RELEASE, key, token);
  }
  /**
   * Moves the deadline of a lease or semaphore slot you hold to now + `ttlMs` (`RENEW key token ttlMs`, Ruvio extension).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param token The token returned by {@link lease} or {@link semaphore}.
   * @param ttlMs New lifetime in milliseconds (positive). It replaces the remaining time.
   * @returns `true` when the deadline moved; `false` when the token expired, was released or belongs to someone else (nothing changes).
   *
   * @example
   * ```ts
   * await client.renew('job:nightly', token, 30_000);
   * ```
   */
  renew(key: Key, token: Value | bigint, ttlMs: number): R<boolean> {
    return this._3(T.bool, C.RENEW, key, token, ttlMs);
  }
  /**
   * Arms the in-memory change feed on this shard (`CHANGES START`, Ruvio extension) and returns the cursor to read from.
   * Nothing is recorded before this call. The feed is not a copy of the WAL and is empty after a restart.
   *
   * @returns The starting cursor for {@link changes}.
   *
   * @example
   * ```ts
   * let cursor = await client.changesStart();
   * ```
   */
  changesStart(): R<number> {
    return this._1(null, C.CHANGES, W.START);
  }
  /**
   * Reads change notes recorded after a cursor (`CHANGES cursor`, Ruvio extension).
   *
   * @param cursor A cursor from {@link changesStart} or the last note's `seq`.
   * @returns Up to 1024 {@link ChangeNote}s, oldest first. Empty when nothing happened.
   *
   * @example
   * ```ts
   * for (const n of await client.changes(cursor)) cursor = n.seq;
   * ```
   */
  changes(cursor: number): R<ChangeNote[]> {
    return this._1(T.changes, C.CHANGES, cursor);
  }

  // ---- keys ------------------------------------------------------------------------------

  /**
   * Deletes keys (`DEL`).
   * On a multi-shard server the keys must share a slot, or the server answers `CROSSSLOT`; a {@link ClusterClient} splits the call per slot (not atomic) and sums the counts.
   *
   * @param keys Keys to operate on.
   * @returns The number of keys that existed and were removed.
   *
   * @example
   * ```ts
   * await client.del('a', 'b'); // 2
   * ```
   */
  del(...keys: Key[]): R<number> {
    return keys.length === 1 ? this._1(null, C.DEL, keys[0]!) : this._v(null, C.DEL, keys);
  }
  /**
   * Deletes keys (`UNLINK`). Ruvio frees synchronously, so this is identical to {@link del}.
   *
   * @param keys Keys to operate on.
   * @returns The number of keys removed.
   *
   * @example
   * ```ts
   * await client.unlink('a');
   * ```
   */
  unlink(...keys: Key[]): R<number> {
    return this._v(null, C.UNLINK, keys);
  }
  /**
   * Counts how many of the keys exist (`EXISTS`). A key named twice counts twice.
   * Keys must share a slot on a multi-shard server; a {@link ClusterClient} splits per slot.
   *
   * @param keys Keys to operate on.
   * @returns The number of existing keys.
   *
   * @example
   * ```ts
   * if ((await client.exists('user:1')) === 1) { found(); }
   * ```
   */
  exists(...keys: Key[]): R<number> {
    return keys.length === 1 ? this._1(null, C.EXISTS, keys[0]!) : this._v(null, C.EXISTS, keys);
  }
  /**
   * Returns the type name of a key (`TYPE`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @returns `'string'`, `'list'`, `'set'`, `'zset'`, `'hash'`, `'stream'` and so on, or `'none'` when the key does not exist.
   *
   * @example
   * ```ts
   * await client.type('user:1'); // 'hash'
   * ```
   */
  type(key: Key): R<string> {
    return this._1(null, C.TYPE, key);
  }
  /**
   * Renames a key, overwriting the target (`RENAME`).
   * Both keys must share a slot on a multi-shard server; a {@link ClusterClient} does not emulate it and fails with `CROSSSLOT` before sending.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param newKey New key name.
   * @returns `'OK'`.
   * @throws {@link RuvioError} `ERR` when `key` does not exist; `CROSSSLOT` when the slots differ.
   *
   * @example
   * ```ts
   * await client.rename('tmp:1', 'final:1');
   * ```
   */
  rename(key: Key, newKey: Key): R<'OK'> {
    return this._2(null, C.RENAME, key, newKey);
  }
  /**
   * Renames a key only if the new name is free (`RENAMENX`). Slot rules as for {@link rename}.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param newKey New key name.
   * @returns `true` when renamed, `false` when `newKey` already exists.
   * @throws {@link RuvioError} `ERR` when `key` does not exist; `CROSSSLOT` when the slots differ.
   *
   * @example
   * ```ts
   * await client.renamenx('a', 'b');
   * ```
   */
  renamenx(key: Key, newKey: Key): R<boolean> {
    return this._2(T.bool, C.RENAMENX, key, newKey);
  }
  /**
   * Reads one page of the key space (`SCAN`). See {@link scanIterator} for a lazy walk of all pages.
   * Shard-local: on a multi-shard server one connection sees only its shard's keys, and cursors walk sorted live keys rather than Redis hash-table cursors. {@link ClusterClient.scanIterator} walks every node.
   *
   * @param cursor Start with `0`; pass the returned cursor for the next page.
   * @param opts {@link ScanOptions}: `match` glob, `count` page-size hint, `type` filter (see {@link ScanType}).
   * @returns A {@link ScanPage}; its `cursor` is `'0'` when the walk is finished. A page may be empty before that.
   *
   * @example
   * ```ts
   * let page = await client.scan(0, { match: 'user:*', count: 100 });
   * while (page.cursor !== '0') page = await client.scan(page.cursor, { match: 'user:*' });
   * ```
   */
  scan(cursor: string | number, opts?: ScanOptions): R<ScanPage<string>> {
    return this._v(T.scan, C.SCAN, pushScan([cursor], opts));
  }
  /**
   * Counts keys in the selected database (`DBSIZE`).
   * Shard-local when the server runs with `port_mode = "per-shard"` (the default): a plain client counts only the shard it is connected to. {@link ClusterClient} asks every node and sums.
   *
   * @returns The key count.
   *
   * @example
   * ```ts
   * await client.dbsize();
   * ```
   */
  dbsize(): R<number> {
    return this._0(null, C.DBSIZE);
  }
  /**
   * Deletes every key of the selected database (`FLUSHDB`).
   * Careful with multi-shard servers in `port_mode = "per-shard"` (the default): a plain client clears only the shard it is connected to, while {@link ClusterClient} sends it to every node.
   *
   * @param async Accepted for compatibility (`ASYNC`); Ruvio always clears synchronously.
   * @returns `'OK'`.
   *
   * @example
   * ```ts
   * await client.flushdb();
   * ```
   */
  flushdb(async?: boolean): R<'OK'> {
    return async ? this._1(null, C.FLUSHDB, W.ASYNC) : this._0(null, C.FLUSHDB);
  }
  /**
   * Deletes every key of every database (`FLUSHALL`).
   * Same multi-shard rule as {@link flushdb}: a plain client clears only its own shard in per-shard mode; {@link ClusterClient} clears all nodes.
   *
   * @param async Accepted for compatibility (`ASYNC`); Ruvio always clears synchronously.
   * @returns `'OK'`.
   *
   * @example
   * ```ts
   * await client.flushall();
   * ```
   */
  flushall(async?: boolean): R<'OK'> {
    return async ? this._1(null, C.FLUSHALL, W.ASYNC) : this._0(null, C.FLUSHALL);
  }
  /**
   * Swaps two logical databases (`SWAPDB`). O(1).
   * Refused in cluster mode and in scripts. Clients blocked on keys that now exist are not woken.
   *
   * @param a First database index.
   * @param b Second database index.
   * @returns `'OK'`.
   * @throws {@link RuvioError} `ERR` when an index is out of range or the server is in cluster mode.
   *
   * @example
   * ```ts
   * await client.swapdb(0, 1);
   * ```
   */
  swapdb(a: number, b: number): R<'OK'> {
    return this._2(null, C.SWAPDB, a, b);
  }
  /**
   * Moves a key to another logical database (`MOVE`). Refused in cluster mode and in scripts.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param db Target database index.
   * @returns `true` when moved; `false` when the key is missing or the target already has it.
   * @throws {@link RuvioError} `ERR` when `db` does not exist.
   *
   * @example
   * ```ts
   * await client.move('user:1', 2);
   * ```
   */
  move(key: Key, db: number): R<boolean> {
    return this._2(T.bool, C.MOVE, key, db);
  }
  /**
   * Sets a TTL in seconds (`EXPIRE key seconds [NX|XX|GT|LT]`).
   * A TTL of zero or less deletes the key and still returns `true`, as Redis does.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param seconds Time to live in seconds.
   * @param cond Optional {@link ExpireCondition}: `NX` (no deadline yet), `XX` (has a deadline), `GT` (new deadline later), `LT` (new deadline earlier). A key without a deadline counts as infinite.
   * @returns `true` when applied; `false` when the key does not exist or the condition failed.
   *
   * @example
   * ```ts
   * await client.expire('session:9', 3600);
   * await client.expire('session:9', 7200, ExpireCondition.Gt); // only ever extends
   * ```
   */
  expire(key: Key, seconds: number, cond?: ExpireCondition): R<boolean> {
    return cond === undefined ? this._2(T.bool, C.EXPIRE, key, seconds) : this._3(T.bool, C.EXPIRE, key, seconds, kw(KW_EXPIRE_COND, cond));
  }
  /**
   * Sets a TTL in milliseconds (`PEXPIRE key ms [NX|XX|GT|LT]`). A value of zero or less deletes the key and returns `true`.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param ms Time to live in milliseconds.
   * @param cond Optional {@link ExpireCondition} (see {@link expire}).
   * @returns `true` when applied; `false` when the key does not exist or the condition failed.
   *
   * @example
   * ```ts
   * await client.pexpire('session:9', 1500);
   * ```
   */
  pexpire(key: Key, ms: number, cond?: ExpireCondition): R<boolean> {
    return cond === undefined ? this._2(T.bool, C.PEXPIRE, key, ms) : this._3(T.bool, C.PEXPIRE, key, ms, kw(KW_EXPIRE_COND, cond));
  }
  /**
   * Sets an absolute deadline in Unix seconds (`EXPIREAT key unixSeconds [NX|XX|GT|LT]`). A deadline in the past deletes the key and still returns `true`.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param unixSeconds Deadline as Unix time in seconds.
   * @param cond Optional {@link ExpireCondition} (see {@link expire}).
   * @returns `true` when applied; `false` when the key does not exist or the condition failed.
   *
   * @example
   * ```ts
   * await client.expireat('promo', Math.floor(Date.now() / 1000) + 86_400);
   * ```
   */
  expireat(key: Key, unixSeconds: number, cond?: ExpireCondition): R<boolean> {
    return cond === undefined ? this._2(T.bool, C.EXPIREAT, key, unixSeconds) : this._3(T.bool, C.EXPIREAT, key, unixSeconds, kw(KW_EXPIRE_COND, cond));
  }
  /**
   * Sets an absolute deadline in Unix milliseconds (`PEXPIREAT key unixMs [NX|XX|GT|LT]`). A deadline in the past deletes the key and still returns `true`.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param unixMs Deadline as Unix time in milliseconds.
   * @param cond Optional {@link ExpireCondition} (see {@link expire}).
   * @returns `true` when applied; `false` when the key does not exist or the condition failed.
   *
   * @example
   * ```ts
   * await client.pexpireat('promo', Date.now() + 5000);
   * ```
   */
  pexpireat(key: Key, unixMs: number, cond?: ExpireCondition): R<boolean> {
    return cond === undefined ? this._2(T.bool, C.PEXPIREAT, key, unixMs) : this._3(T.bool, C.PEXPIREAT, key, unixMs, kw(KW_EXPIRE_COND, cond));
  }
  /**
   * Remaining time to live in seconds (`TTL`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @returns Seconds left, `-1` when the key has no expiry, `-2` when the key does not exist.
   *
   * @example
   * ```ts
   * await client.ttl('session:9'); // 3598
   * ```
   */
  ttl(key: Key): R<number> {
    return this._1(null, C.TTL, key);
  }
  /**
   * Remaining time to live in milliseconds (`PTTL`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @returns Milliseconds left, `-1` when the key has no expiry, `-2` when the key does not exist.
   *
   * @example
   * ```ts
   * await client.pttl('session:9');
   * ```
   */
  pttl(key: Key): R<number> {
    return this._1(null, C.PTTL, key);
  }
  /**
   * Removes a key's TTL (`PERSIST`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @returns `true` when a TTL was removed; `false` when the key is missing or had no TTL.
   *
   * @example
   * ```ts
   * await client.persist('session:9');
   * ```
   */
  persist(key: Key): R<boolean> {
    return this._1(T.bool, C.PERSIST, key);
  }
  /**
   * Lists keys matching a glob pattern (`KEYS`). Use {@link scan} or {@link scanIterator} on real data.
   * It walks the database in one call and refuses databases above 100,000 keys per shard. Glob supports `*`, `?`, `[abc]`, `[^a-z]` and `\` escapes. A {@link ClusterClient} asks every node and joins the arrays; a plain client on a per-shard server sees only its own shard.
   *
   * @param pattern Glob pattern, e.g. `'user:*'`.
   * @returns Matching key names, in no guaranteed order.
   * @throws {@link RuvioError} `ERR` when the shard holds more than 100,000 keys.
   *
   * @example
   * ```ts
   * const ids = await client.keys('user:*');
   * ```
   */
  keys(pattern: string): R<string[]> {
    return this._1(null, C.KEYS, pattern);
  }
  /**
   * Returns a random key (`RANDOMKEY`). It starts at a pseudo-random slot of the key index, so it is not uniform.
   *
   * @returns A key name, or `null` when the database is empty.
   *
   * @example
   * ```ts
   * await client.randomkey();
   * ```
   */
  randomkey(): R<string | null> {
    return this._0(null, C.RANDOMKEY);
  }
  /**
   * Counts existing keys (`TOUCH`). Ruvio does not update the access time used by eviction. Keys must share a shard, like {@link exists}.
   *
   * @param keys Keys to operate on.
   * @returns The number of keys that exist.
   *
   * @example
   * ```ts
   * await client.touch('a', 'b');
   * ```
   */
  touch(...keys: Key[]): R<number> {
    return this._v(null, C.TOUCH, keys);
  }
  /**
   * Absolute expiry as Unix seconds (`EXPIRETIME`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @returns The deadline in Unix seconds, `-1` when the key has no expiry, `-2` when it does not exist.
   *
   * @example
   * ```ts
   * await client.expiretime('promo');
   * ```
   */
  expiretime(key: Key): R<number> {
    return this._1(null, C.EXPIRETIME, key);
  }
  /**
   * Absolute expiry as Unix milliseconds (`PEXPIRETIME`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @returns The deadline in Unix milliseconds, `-1` when the key has no expiry, `-2` when it does not exist.
   *
   * @example
   * ```ts
   * await client.pexpiretime('promo');
   * ```
   */
  pexpiretime(key: Key): R<number> {
    return this._1(null, C.PEXPIRETIME, key);
  }
  /**
   * Copies a key (`COPY`). Works for strings, lists, sets, hashes and sorted sets, with their TTL. It copies the whole value as one atomic operation (cost grows with size).
   * Streams, JSON, Bloom filters and hashes with field TTLs are refused, and the `DB` option is not supported. Both keys must share a slot.
   *
   * @param source Key to read from.
   * @param destination Key that receives the result (any existing value is replaced).
   * @param opts `replace: true` overwrites an existing destination.
   * @returns `true` when copied; `false` when the source is missing or the destination exists and `replace` was not set.
   * @throws {@link RuvioError} `ERR` for unsupported types; `CROSSSLOT` when the slots differ.
   *
   * @example
   * ```ts
   * await client.copy('cfg', 'cfg:backup', { replace: true });
   * ```
   */
  copy(source: Key, destination: Key, opts?: { replace?: boolean }): R<boolean> {
    return opts?.replace
      ? this._3(T.bool, C.COPY, source, destination, W.REPLACE)
      : this._2(T.bool, C.COPY, source, destination);
  }
  /**
   * Reports the internal encoding Redis 7.0 would use (`OBJECT ENCODING`), e.g. `int`, `embstr`, `raw`, `listpack`, `hashtable`, `skiplist`. This names the Redis encoding, not Ruvio's physical layout.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @returns The encoding name, or `null` when the key does not exist.
   *
   * @example
   * ```ts
   * await client.objectEncoding('user:1');
   * ```
   */
  objectEncoding(key: Key): R<string | null> {
    return this._1(null, C.OBJECT_ENCODING, key);
  }
  /**
   * Seconds since the key was last accessed (`OBJECT IDLETIME`). Needs an LRU `maxmemory-policy`.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @returns Idle seconds, or `null` when the key does not exist.
   * @throws {@link RuvioError} `ERR` when the eviction policy is not an LRU policy.
   *
   * @example
   * ```ts
   * await client.objectIdletime('user:1');
   * ```
   */
  objectIdletime(key: Key): R<number | null> {
    return this._1(null, C.OBJECT_IDLETIME, key);
  }
  /**
   * Access frequency counter (`OBJECT FREQ`). Needs `maxmemory-policy allkeys-lfu`.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @returns The counter, or `null` when the key does not exist.
   * @throws {@link RuvioError} `ERR` when the eviction policy is not `allkeys-lfu`.
   *
   * @example
   * ```ts
   * await client.objectFreq('user:1');
   * ```
   */
  objectFreq(key: Key): R<number | null> {
    return this._1(null, C.OBJECT_FREQ, key);
  }
  /**
   * Bytes a key uses in memory (`MEMORY USAGE key [SAMPLES n]`). The size is exact for Ruvio's accounting (key, value, collection storage plus entry overhead) and is not comparable byte for byte with Redis.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param samples Accepted for compatibility.
   * @returns Size in bytes, or `null` when the key does not exist.
   *
   * @example
   * ```ts
   * await client.memoryUsage('user:1');
   * ```
   */
  memoryUsage(key: Key, samples?: number): R<number | null> {
    return samples === undefined
      ? this._1(null, C.MEMORY_USAGE, key)
      : this._3(null, C.MEMORY_USAGE, key, W.SAMPLES, samples);
  }
  /**
   * Serialises a key (`DUMP`) into Ruvio's own payload (`RVD1` plus a checksummed record). Only Ruvio's {@link restore} reads it; it is not Redis RDB.
   * Supports strings, lists, sets, hashes and sorted sets. Streams, JSON, Bloom filters and hashes with field TTLs are refused. The TTL is not part of the payload.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @returns The payload, or `null` when the key does not exist.
   * @throws {@link RuvioError} `ERR` for unsupported types.
   *
   * @example
   * ```ts
   * const blob = await client.dump('user:1');
   * if (blob) await client.restore('user:1:copy', 0, blob);
   * ```
   */
  dump(key: Key): R<Buffer | null> {
    return this._bin(() => this._1(null, C.DUMP, key));
  }
  /**
   * Recreates a key from a {@link dump} payload (`RESTORE key ttlMs payload [REPLACE] [ABSTTL] [IDLETIME n] [FREQ n]`). One atomic operation.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param ttlMs Time to live in milliseconds; `0` for none. With `absTtl` it is an absolute Unix-ms deadline (a past deadline creates nothing).
   * @param payload The Buffer from {@link dump}.
   * @param opts {@link RestoreOptions}: `replace`, `absTtl`; `idleTime` and `freq` are accepted and ignored.
   * @returns `'OK'`.
   * @throws {@link RuvioError} `BUSYKEY` when the key exists and `replace` is not set; `ERR` for a payload that is not Ruvio's.
   *
   * @example
   * ```ts
   * await client.restore('user:1', 0, blob, { replace: true });
   * ```
   */
  restore(key: Key, ttlMs: number, payload: Buffer, opts?: RestoreOptions): R<'OK'> {
    const a: Arg[] = [key, ttlMs, payload];
    if (opts !== undefined) {
      if (opts.replace) a.push(W.REPLACE);
      if (opts.absTtl) a.push(W.ABSTTL);
      if (opts.idleTime !== undefined) a.push(W.IDLETIME, opts.idleTime);
      if (opts.freq !== undefined) a.push(W.FREQ, opts.freq);
    }
    return this._v(null, C.RESTORE, a);
  }

  // ---- lists -----------------------------------------------------------------------------

  /**
   * Pushes values onto the head of a list, creating it when missing (`LPUSH`). With several values the last one ends up first.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param values Values to push.
   * @returns The list length after the push.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.lpush('queue', 'a', 'b'); // list is now [b, a]
   * ```
   */
  lpush(key: Key, ...values: Value[]): R<number> {
    return values.length === 1 ? this._2(null, C.LPUSH, key, values[0]!) : this._kv(null, C.LPUSH, key, values);
  }
  /**
   * Pushes values onto the tail of a list, creating it when missing (`RPUSH`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param values Values to push, in order.
   * @returns The list length after the push.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.rpush('queue', 'a', 'b'); // 2
   * ```
   */
  rpush(key: Key, ...values: Value[]): R<number> {
    return values.length === 1 ? this._2(null, C.RPUSH, key, values[0]!) : this._kv(null, C.RPUSH, key, values);
  }
  /**
   * Like {@link lpush}, but only when the list already exists (`LPUSHX`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param values Values to push.
   * @returns The list length after the push, or `0` when the key does not exist (nothing is created).
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.lpushx('queue', 'a');
   * ```
   */
  lpushx(key: Key, ...values: Value[]): R<number> {
    return this._kv(null, C.LPUSHX, key, values);
  }
  /**
   * Like {@link rpush}, but only when the list already exists (`RPUSHX`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param values Values to push.
   * @returns The list length after the push, or `0` when the key does not exist.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.rpushx('queue', 'a');
   * ```
   */
  rpushx(key: Key, ...values: Value[]): R<number> {
    return this._kv(null, C.RPUSHX, key, values);
  }
  /**
   * Pushes to the head and trims the tail to a maximum length (`LPUSHCAP key cap value...`, Ruvio extension). Good for "latest N items" lists; see {@link pushRecent}.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param cap Maximum list length to keep.
   * @param values Values to push.
   * @returns The list length after the push and trim.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.lpushcap('recent:7', 50, 'item-9'); // never more than 50 items
   * ```
   */
  lpushcap(key: Key, cap: number, ...values: Value[]): R<number> {
    return this._v(null, C.LPUSHCAP, pushAll([key, cap], values));
  }
  /**
   * Pushes to the tail and trims the head to a maximum length (`RPUSHCAP key cap value...`, Ruvio extension).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param cap Maximum list length to keep.
   * @param values Values to push.
   * @returns The list length after the push and trim.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.rpushcap('log:app', 1000, 'line');
   * ```
   */
  rpushcap(key: Key, cap: number, ...values: Value[]): R<number> {
    return this._v(null, C.RPUSHCAP, pushAll([key, cap], values));
  }
  /**
   * Removes and returns elements from the head of a list (`LPOP [count]`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param count Optional positive number of elements. Without it a single element is returned.
   * @returns Without `count`: the element, or `null` when the list is empty or missing. With `count`: an array of up to `count` elements, or `null` when the list is empty or missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.lpop('queue');      // 'a'
   * await client.lpop('queue', 2);   // ['b', 'c']
   * ```
   */
  lpop(key: Key): R<string | null>;
  /**
   * Removes and returns elements from the head of a list (`LPOP [count]`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param count Optional positive number of elements. Without it a single element is returned.
   * @returns Without `count`: the element, or `null` when the list is empty or missing. With `count`: an array of up to `count` elements, or `null` when the list is empty or missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.lpop('queue');      // 'a'
   * await client.lpop('queue', 2);   // ['b', 'c']
   * ```
   */
  lpop(key: Key, count: number): R<string[] | null>;
  /**
   * Removes and returns elements from the head of a list (`LPOP [count]`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param count Optional positive number of elements. Without it a single element is returned.
   * @returns Without `count`: the element, or `null` when the list is empty or missing. With `count`: an array of up to `count` elements, or `null` when the list is empty or missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.lpop('queue');      // 'a'
   * await client.lpop('queue', 2);   // ['b', 'c']
   * ```
   */
  lpop(key: Key, count?: number): R<string | string[] | null>;
  lpop(key: Key, count?: number): R<any> {
    return count === undefined ? this._1(null, C.LPOP, key) : this._2(null, C.LPOP, key, count);
  }
  /**
   * Removes and returns elements from the tail of a list (`RPOP [count]`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param count Optional positive number of elements. Without it a single element is returned.
   * @returns Without `count`: the element, or `null` when the list is empty or missing. With `count`: an array of up to `count` elements, or `null` when the list is empty or missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.rpop('queue');
   * ```
   */
  rpop(key: Key): R<string | null>;
  /**
   * Removes and returns elements from the tail of a list (`RPOP [count]`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param count Optional positive number of elements. Without it a single element is returned.
   * @returns Without `count`: the element, or `null` when the list is empty or missing. With `count`: an array of up to `count` elements, or `null` when the list is empty or missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.rpop('queue');
   * ```
   */
  rpop(key: Key, count: number): R<string[] | null>;
  /**
   * Removes and returns elements from the tail of a list (`RPOP [count]`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param count Optional positive number of elements. Without it a single element is returned.
   * @returns Without `count`: the element, or `null` when the list is empty or missing. With `count`: an array of up to `count` elements, or `null` when the list is empty or missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.rpop('queue');
   * ```
   */
  rpop(key: Key, count?: number): R<string | string[] | null>;
  rpop(key: Key, count?: number): R<any> {
    return count === undefined ? this._1(null, C.RPOP, key) : this._2(null, C.RPOP, key, count);
  }
  /**
   * Returns the list length (`LLEN`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @returns The length, or `0` when the key does not exist.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.llen('queue');
   * ```
   */
  llen(key: Key): R<number> {
    return this._1(null, C.LLEN, key);
  }
  /**
   * Reads one element by position (`LINDEX`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param index Zero-based position; negative counts from the tail (`-1` is the last element).
   * @returns The element, or `null` when the index is out of range or the key is missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.lindex('queue', -1);
   * ```
   */
  lindex(key: Key, index: number): R<string | null> {
    return this._2(null, C.LINDEX, key, index);
  }
  /**
   * Reads a range of elements (`LRANGE`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param start First position (inclusive); negative counts from the tail.
   * @param stop Last position (inclusive); `-1` means the end.
   * @returns The elements, or an empty array when the key is missing or the range is empty.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.lrange('queue', 0, -1); // everything
   * ```
   */
  lrange(key: Key, start: number, stop: number): R<string[]> {
    return this._3(null, C.LRANGE, key, start, stop);
  }
  /**
   * Keeps only a range of the list and drops the rest (`LTRIM`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param start First position to keep (inclusive); negative counts from the tail.
   * @param stop Last position to keep (inclusive).
   * @returns `'OK'`. A missing key is not an error; an empty range removes the key.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.ltrim('recent:7', 0, 49);
   * ```
   */
  ltrim(key: Key, start: number, stop: number): R<'OK'> {
    return this._3(null, C.LTRIM, key, start, stop);
  }
  /**
   * Blocking pop from the head of the first non-empty list (`BLPOP key... timeout`).
   * Blocks the connection, so every other caller of a shared client waits too: use a dedicated client (`duplicate()` or `isolated()`). It is shard-local, with at most 1,024 blocked clients per shard.
   *
   * @param keys One key or several; the first non-empty list wins.
   * @param timeoutSeconds Seconds to wait; `0` waits forever.
   * @returns `[key, element]`, or `null` on timeout.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * const worker = client.duplicate();
   * const job = await worker.blpop('jobs', 5); // ['jobs', 'payload'] or null
   * ```
   */
  blpop(keys: Key | Key[], timeoutSeconds: number): R<[string, string] | null> {
    const a: Arg[] = Array.isArray(keys) ? [...keys] : [keys];
    a.push(timeoutSeconds);
    return this._v(T.kv, C.BLPOP, a);
  }
  /**
   * Blocking pop from the tail of the first non-empty list (`BRPOP key... timeout`). Same blocking rules as {@link blpop}.
   *
   * @param keys One key or several; the first non-empty list wins.
   * @param timeoutSeconds Seconds to wait; `0` waits forever.
   * @returns `[key, element]`, or `null` on timeout.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * const job = await worker.brpop(['high', 'low'], 5);
   * ```
   */
  brpop(keys: Key | Key[], timeoutSeconds: number): R<[string, string] | null> {
    const a: Arg[] = Array.isArray(keys) ? [...keys] : [keys];
    a.push(timeoutSeconds);
    return this._v(T.kv, C.BRPOP, a);
  }
  /**
   * Inserts an element before or after a pivot element (`LINSERT`). Rewrites the list (cost grows with its length).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param where `'BEFORE'` or `'AFTER'` the pivot (see {@link InsertPosition}).
   * @param pivot Existing element to insert next to (the first match is used).
   * @param value Element to insert.
   * @returns The new list length, `-1` when the pivot was not found, `0` when the key does not exist.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.linsert('queue', 'BEFORE', 'b', 'a');
   * ```
   */
  linsert(key: Key, where: InsertPosition, pivot: Value, value: Value): R<number> {
    return this._4(null, C.LINSERT, key, kw(KW_WHERE, where), pivot, value);
  }
  /**
   * Replaces the element at a position (`LSET`). Rewrites the list.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param index Zero-based position; negative counts from the tail.
   * @param value New element.
   * @returns `'OK'`.
   * @throws {@link RuvioError} `ERR` when the key does not exist or the index is out of range.
   *
   * @example
   * ```ts
   * await client.lset('queue', 0, 'first');
   * ```
   */
  lset(key: Key, index: number, value: Value): R<'OK'> {
    return this._3(null, C.LSET, key, index, value);
  }
  /**
   * Removes occurrences of an element (`LREM`). Rewrites the list.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param count Positive: remove that many from the head. Negative: from the tail. `0`: remove all.
   * @param value Element to remove.
   * @returns The number of elements removed.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.lrem('queue', 0, 'dup');
   * ```
   */
  lrem(key: Key, count: number, value: Value): R<number> {
    return this._3(null, C.LREM, key, count, value);
  }
  /**
   * Finds the position of an element (`LPOS key value [RANK r] [COUNT n] [MAXLEN m]`). Read-only, scans the list.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param value Element to look for.
   * @param opts {@link LPosOptions}: `rank` picks the nth match (negative scans from the tail), `count` returns up to that many positions (`0` means all), `maxlen` limits how many elements are scanned.
   * @returns Without `count`: the position of the match, or `null` when not found. With `count`: an array of positions (possibly empty).
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.lpos('queue', 'b');                    // 1
   * await client.lpos('queue', 'b', { count: 0 });      // [1, 4]
   * ```
   */
  lpos(key: Key, value: Value): R<number | null>;
  /**
   * Finds the position of an element (`LPOS key value [RANK r] [COUNT n] [MAXLEN m]`). Read-only, scans the list.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param value Element to look for.
   * @param opts {@link LPosOptions}: `rank` picks the nth match (negative scans from the tail), `count` returns up to that many positions (`0` means all), `maxlen` limits how many elements are scanned.
   * @returns Without `count`: the position of the match, or `null` when not found. With `count`: an array of positions (possibly empty).
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.lpos('queue', 'b');                    // 1
   * await client.lpos('queue', 'b', { count: 0 });      // [1, 4]
   * ```
   */
  lpos(key: Key, value: Value, opts: LPosOptions & { count: number }): R<number[]>;
  /**
   * Finds the position of an element (`LPOS key value [RANK r] [COUNT n] [MAXLEN m]`). Read-only, scans the list.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param value Element to look for.
   * @param opts {@link LPosOptions}: `rank` picks the nth match (negative scans from the tail), `count` returns up to that many positions (`0` means all), `maxlen` limits how many elements are scanned.
   * @returns Without `count`: the position of the match, or `null` when not found. With `count`: an array of positions (possibly empty).
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.lpos('queue', 'b');                    // 1
   * await client.lpos('queue', 'b', { count: 0 });      // [1, 4]
   * ```
   */
  lpos(key: Key, value: Value, opts?: LPosOptions): R<number | null | number[]>;
  /**
   * Finds the position of an element (`LPOS key value [RANK r] [COUNT n] [MAXLEN m]`). Read-only, scans the list.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param value Element to look for.
   * @param opts {@link LPosOptions}: `rank` picks the nth match (negative scans from the tail), `count` returns up to that many positions (`0` means all), `maxlen` limits how many elements are scanned.
   * @returns Without `count`: the position of the match, or `null` when not found. With `count`: an array of positions (possibly empty).
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.lpos('queue', 'b');                    // 1
   * await client.lpos('queue', 'b', { count: 0 });      // [1, 4]
   * ```
   */
  lpos(key: Key, value: Value, opts: LPosOptions): R<number | null | number[]>;
  lpos(key: Key, value: Value, opts?: LPosOptions): R<any> {
    if (opts === undefined) return this._2(null, C.LPOS, key, value);
    const a: Arg[] = [key, value];
    if (opts.rank !== undefined) a.push(W.RANK, opts.rank);
    if (opts.count !== undefined) a.push(W.COUNT, opts.count);
    if (opts.maxlen !== undefined) a.push(W.MAXLEN, opts.maxlen);
    return this._v(null, C.LPOS, a);
  }
  /**
   * Atomically pops from one list and pushes to another (`LMOVE`), as one atomic operation.
   * Both keys must share a slot on a multi-shard server.
   *
   * @param source List to pop from.
   * @param destination List to push to (may be the same list to rotate it).
   * @param from Side to pop from: `'LEFT'` or `'RIGHT'` (see {@link ListSide}).
   * @param to Side to push to: `'LEFT'` or `'RIGHT'` (see {@link ListSide}).
   * @returns The moved element, or `null` when the source is empty or missing.
   * @throws {@link RuvioError} `WRONGTYPE`; `CROSSSLOT` when the slots differ.
   *
   * @example
   * ```ts
   * await client.lmove('pending', 'working', 'LEFT', 'RIGHT');
   * ```
   */
  lmove(source: Key, destination: Key, from: ListSide, to: ListSide): R<string | null> {
    return this._4(null, C.LMOVE, source, destination, kw(KW_SIDE, from), kw(KW_SIDE, to));
  }
  /**
   * Pops from the tail of one list and pushes onto the head of another (`RPOPLPUSH`). Same as `lmove(source, destination, 'RIGHT', 'LEFT')`.
   *
   * @param source Key to read from.
   * @param destination Key that receives the result (any existing value is replaced).
   * @returns The moved element, or `null` when the source is empty or missing.
   * @throws {@link RuvioError} `WRONGTYPE`; `CROSSSLOT` when the slots differ.
   *
   * @example
   * ```ts
   * await client.rpoplpush('pending', 'working');
   * ```
   */
  rpoplpush(source: Key, destination: Key): R<string | null> {
    return this._2(null, C.RPOPLPUSH, source, destination);
  }
  /**
   * Blocking {@link lmove} (`BLMOVE`). Blocks the connection, so use a dedicated client (`duplicate()`).
   * Ruvio retries every 5 ms instead of parking a waiter, counts against the 1,024 blocked-client limit, and refuses the command inside MULTI or scripts.
   *
   * @param source Key to read from.
   * @param destination Key that receives the result (any existing value is replaced).
   * @param from Side to pop from: `'LEFT'` or `'RIGHT'` (see {@link ListSide}).
   * @param to Side to push to: `'LEFT'` or `'RIGHT'` (see {@link ListSide}).
   * @param timeoutSeconds Seconds to wait; `0` waits forever.
   * @returns The moved element, or `null` on timeout.
   *
   * @example
   * ```ts
   * const job = await worker.blmove('pending', 'working', 'LEFT', 'RIGHT', 5);
   * ```
   */
  blmove(source: Key, destination: Key, from: ListSide, to: ListSide, timeoutSeconds: number): R<string | null> {
    return this._v(null, C.BLMOVE, [source, destination, kw(KW_SIDE, from), kw(KW_SIDE, to), timeoutSeconds]);
  }
  /**
   * Pops from the first non-empty list of several (`LMPOP numkeys key... LEFT|RIGHT [COUNT n]`).
   *
   * @param keys Lists to try, in order.
   * @param where Side to pop from: `'LEFT'` or `'RIGHT'` (see {@link ListSide}).
   * @param count Maximum elements to pop (default 1).
   * @returns A {@link ListPopResult} with the list `key` and the popped `elements`, or `null` when all lists are empty.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * const r = await client.lmpop(['a', 'b'], 'LEFT', 10);
   * ```
   */
  lmpop(keys: Key[], where: ListSide, count?: number): R<ListPopResult | null> {
    const a: Arg[] = [keys.length, ...keys, kw(KW_SIDE, where)];
    if (count !== undefined) a.push(W.COUNT, count);
    return this._v(T.lmpop, C.LMPOP, a);
  }
  /**
   * Blocking {@link lmpop} (`BLMPOP`). Blocks the connection: use a dedicated client. Ruvio retries every 5 ms, counts against the 1,024 blocked-client limit, and refuses it inside MULTI or scripts.
   *
   * @param timeoutSeconds Seconds to wait; `0` waits forever.
   * @param keys Lists to try, in order.
   * @param where Side to pop from: `'LEFT'` or `'RIGHT'` (see {@link ListSide}).
   * @param count Maximum elements to pop (default 1).
   * @returns A {@link ListPopResult}, or `null` on timeout.
   *
   * @example
   * ```ts
   * const r = await worker.blmpop(5, ['a', 'b'], 'LEFT');
   * ```
   */
  blmpop(timeoutSeconds: number, keys: Key[], where: ListSide, count?: number): R<ListPopResult | null> {
    const a: Arg[] = [timeoutSeconds, keys.length, ...keys, kw(KW_SIDE, where)];
    if (count !== undefined) a.push(W.COUNT, count);
    return this._v(T.lmpop, C.BLMPOP, a);
  }

  // ---- sets ------------------------------------------------------------------------------

  /**
   * Adds members to a set, creating it when missing (`SADD`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param members Member names.
   * @returns The number of members that were new.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.sadd('tags', 'a', 'b'); // 2
   * ```
   */
  sadd(key: Key, ...members: Value[]): R<number> {
    return members.length === 1 ? this._2(null, C.SADD, key, members[0]!) : this._kv(null, C.SADD, key, members);
  }
  /**
   * Removes members from a set (`SREM`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param members Member names.
   * @returns The number of members that were removed. A set that becomes empty is deleted.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.srem('tags', 'a');
   * ```
   */
  srem(key: Key, ...members: Value[]): R<number> {
    return this._kv(null, C.SREM, key, members);
  }
  /**
   * Tests set membership (`SISMEMBER`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param member Member name.
   * @returns `true` when the member is in the set; `false` when it is not or the key is missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.sismember('tags', 'a');
   * ```
   */
  sismember(key: Key, member: Value): R<boolean> {
    return this._2(T.bool, C.SISMEMBER, key, member);
  }
  /**
   * Tests several members at once (`SMISMEMBER`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param members Member names.
   * @returns One boolean per member, in order; all `false` when the key is missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.smismember('tags', 'a', 'z'); // [true, false]
   * ```
   */
  smismember(key: Key, ...members: Value[]): R<boolean[]> {
    return this._kv(T.boolArr, C.SMISMEMBER, key, members);
  }
  /**
   * Returns the number of members (`SCARD`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @returns The cardinality, or `0` when the key is missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.scard('tags');
   * ```
   */
  scard(key: Key): R<number> {
    return this._1(null, C.SCARD, key);
  }
  /**
   * Returns all members (`SMEMBERS`). The reply is sorted.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @returns The members, or an empty array when the key is missing. For big sets prefer {@link sscanIterator}.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.smembers('tags');
   * ```
   */
  smembers(key: Key): R<string[]> {
    return this._1(null, C.SMEMBERS, key);
  }
  /**
   * Intersection of sets (`SINTER`). The reply is sorted. Keys must share a slot on a multi-shard server.
   *
   * @param keys Keys to operate on.
   * @returns Members present in every set; empty when any key is missing.
   * @throws {@link RuvioError} `WRONGTYPE`; `CROSSSLOT` when the slots differ.
   *
   * @example
   * ```ts
   * await client.sinter('a', 'b');
   * ```
   */
  sinter(...keys: Key[]): R<string[]> {
    return this._v(null, C.SINTER, keys);
  }
  /**
   * Union of sets (`SUNION`). The reply is sorted. Keys must share a slot on a multi-shard server.
   *
   * @param keys Keys to operate on.
   * @returns Members present in any set.
   * @throws {@link RuvioError} `WRONGTYPE`; `CROSSSLOT` when the slots differ.
   *
   * @example
   * ```ts
   * await client.sunion('a', 'b');
   * ```
   */
  sunion(...keys: Key[]): R<string[]> {
    return this._v(null, C.SUNION, keys);
  }
  /**
   * Members of the first set that are in none of the others (`SDIFF`). The reply is sorted. Keys must share a slot.
   *
   * @param keys First key is the base set.
   * @returns The difference.
   * @throws {@link RuvioError} `WRONGTYPE`; `CROSSSLOT` when the slots differ.
   *
   * @example
   * ```ts
   * await client.sdiff('a', 'b');
   * ```
   */
  sdiff(...keys: Key[]): R<string[]> {
    return this._v(null, C.SDIFF, keys);
  }
  /**
   * Stores the intersection of sets in `destination` (`SINTERSTORE`). Keys must share a slot.
   *
   * @param destination Key that receives the result (any existing value is replaced).
   * @param keys Source sets.
   * @returns The size of the stored set.
   * @throws {@link RuvioError} `WRONGTYPE`; `CROSSSLOT` when the slots differ.
   *
   * @example
   * ```ts
   * await client.sinterstore('both', 'a', 'b');
   * ```
   */
  sinterstore(destination: Key, ...keys: Key[]): R<number> {
    return this._kv(null, C.SINTERSTORE, destination, keys);
  }
  /**
   * Stores the union of sets in `destination` (`SUNIONSTORE`). Keys must share a slot.
   *
   * @param destination Key that receives the result (any existing value is replaced).
   * @param keys Source sets.
   * @returns The size of the stored set.
   * @throws {@link RuvioError} `WRONGTYPE`; `CROSSSLOT` when the slots differ.
   *
   * @example
   * ```ts
   * await client.sunionstore('all', 'a', 'b');
   * ```
   */
  sunionstore(destination: Key, ...keys: Key[]): R<number> {
    return this._kv(null, C.SUNIONSTORE, destination, keys);
  }
  /**
   * Stores the difference of sets in `destination` (`SDIFFSTORE`). Keys must share a slot.
   *
   * @param destination Key that receives the result (any existing value is replaced).
   * @param keys Source sets; the first is the base.
   * @returns The size of the stored set.
   * @throws {@link RuvioError} `WRONGTYPE`; `CROSSSLOT` when the slots differ.
   *
   * @example
   * ```ts
   * await client.sdiffstore('only-a', 'a', 'b');
   * ```
   */
  sdiffstore(destination: Key, ...keys: Key[]): R<number> {
    return this._kv(null, C.SDIFFSTORE, destination, keys);
  }
  /**
   * Size of the intersection without building it (`SINTERCARD`). Keys must share a slot.
   *
   * @param keys Sets to intersect.
   * @param limit Stop counting at this value (`0` or omitted means no limit).
   * @returns The intersection size, capped at `limit`.
   * @throws {@link RuvioError} `WRONGTYPE`; `CROSSSLOT` when the slots differ.
   *
   * @example
   * ```ts
   * await client.sintercard(['a', 'b'], 100);
   * ```
   */
  sintercard(keys: Key[], limit?: number): R<number> {
    const a: Arg[] = [keys.length, ...keys];
    if (limit !== undefined) a.push(W.LIMIT, limit);
    return this._v(null, C.SINTERCARD, a);
  }
  /**
   * Moves a member from one set to another (`SMOVE`). Keys must share a slot.
   *
   * @param source Key to read from.
   * @param destination Key that receives the result (any existing value is replaced).
   * @param member Member name.
   * @returns `true` when moved; `false` when the member was not in the source.
   * @throws {@link RuvioError} `WRONGTYPE`; `CROSSSLOT` when the slots differ.
   *
   * @example
   * ```ts
   * await client.smove('todo', 'done', 'task1');
   * ```
   */
  smove(source: Key, destination: Key, member: Value): R<boolean> {
    return this._3(T.bool, C.SMOVE, source, destination, member);
  }
  /**
   * Removes and returns random members (`SPOP [count]`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param count Optional number of members. Without it a single member is returned.
   * @returns Without `count`: a member, or `null` when the set is empty or missing. With `count`: an array of up to `count` members (empty when none).
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.spop('lottery');       // 'ticket-7'
   * await client.spop('lottery', 3);    // ['t1', 't2', 't3']
   * ```
   */
  spop(key: Key): R<string | null>;
  /**
   * Removes and returns random members (`SPOP [count]`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param count Optional number of members. Without it a single member is returned.
   * @returns Without `count`: a member, or `null` when the set is empty or missing. With `count`: an array of up to `count` members (empty when none).
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.spop('lottery');       // 'ticket-7'
   * await client.spop('lottery', 3);    // ['t1', 't2', 't3']
   * ```
   */
  spop(key: Key, count: number): R<string[]>;
  /**
   * Removes and returns random members (`SPOP [count]`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param count Optional number of members. Without it a single member is returned.
   * @returns Without `count`: a member, or `null` when the set is empty or missing. With `count`: an array of up to `count` members (empty when none).
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.spop('lottery');       // 'ticket-7'
   * await client.spop('lottery', 3);    // ['t1', 't2', 't3']
   * ```
   */
  spop(key: Key, count?: number): R<string | string[] | null>;
  spop(key: Key, count?: number): R<any> {
    return count === undefined ? this._1(null, C.SPOP, key) : this._2(T.strs, C.SPOP, key, count);
  }
  /**
   * Returns random members without removing them (`SRANDMEMBER [count]`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param count Optional. Positive: up to that many distinct members. Negative: exactly `|count|` members, repeats allowed. `0`: empty array.
   * @returns Without `count`: a member, or `null` when the set is empty or missing. With `count`: an array.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.srandmember('tags', 2);
   * ```
   */
  srandmember(key: Key): R<string | null>;
  /**
   * Returns random members without removing them (`SRANDMEMBER [count]`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param count Optional. Positive: up to that many distinct members. Negative: exactly `|count|` members, repeats allowed. `0`: empty array.
   * @returns Without `count`: a member, or `null` when the set is empty or missing. With `count`: an array.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.srandmember('tags', 2);
   * ```
   */
  srandmember(key: Key, count: number): R<string[]>;
  /**
   * Returns random members without removing them (`SRANDMEMBER [count]`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param count Optional. Positive: up to that many distinct members. Negative: exactly `|count|` members, repeats allowed. `0`: empty array.
   * @returns Without `count`: a member, or `null` when the set is empty or missing. With `count`: an array.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.srandmember('tags', 2);
   * ```
   */
  srandmember(key: Key, count?: number): R<string | string[] | null>;
  srandmember(key: Key, count?: number): R<any> {
    return count === undefined ? this._1(null, C.SRANDMEMBER, key) : this._2(T.strs, C.SRANDMEMBER, key, count);
  }
  /**
   * Reads one page of set members (`SSCAN`). See {@link sscanIterator} for a lazy walk.
   * The cursor is a position in member order. A missing key returns cursor `'0'` and no items.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param cursor Cursor from the previous page; start with `0`. A returned cursor of `"0"` means the walk is complete.
   * @param opts {@link ScanOptions}: `match` glob and `count` page-size hint.
   * @returns A {@link ScanPage} of members.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * const page = await client.sscan('tags', 0, { match: 'a*' });
   * ```
   */
  sscan(key: Key, cursor: string | number, opts?: ScanOptions): R<ScanPage<string>> {
    return this._v(T.scan, C.SSCAN, pushScan([key, cursor], opts));
  }

  // ---- hashes ----------------------------------------------------------------------------

  /**
   * Sets hash fields (`HSET`). Pass one field and value, or an object / array of pairs for several.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param field Field name (single-field form).
   * @param value Field value (single-field form).
   * @param fields Object `{ field: value }` or array of `[field, value]` pairs (multi-field form).
   * @returns The number of fields that were new (updated fields are not counted).
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.hset('user:1', 'name', 'Ada');
   * await client.hset('user:1', { name: 'Ada', age: 36 });
   * ```
   */
  hset(key: Key, field: Key, value: Value): R<number>;
  /**
   * Sets hash fields (`HSET`). Pass one field and value, or an object / array of pairs for several.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param field Field name (single-field form).
   * @param value Field value (single-field form).
   * @param fields Object `{ field: value }` or array of `[field, value]` pairs (multi-field form).
   * @returns The number of fields that were new (updated fields are not counted).
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.hset('user:1', 'name', 'Ada');
   * await client.hset('user:1', { name: 'Ada', age: 36 });
   * ```
   */
  hset(key: Key, fields: Record<string, Value> | ReadonlyArray<readonly [Key, Value]>): R<number>;
  /**
   * Sets hash fields (`HSET`). Pass one field and value, or an object / array of pairs for several.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param field Field name (single-field form).
   * @param value Field value (single-field form).
   * @param fields Object `{ field: value }` or array of `[field, value]` pairs (multi-field form).
   * @returns The number of fields that were new (updated fields are not counted).
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.hset('user:1', 'name', 'Ada');
   * await client.hset('user:1', { name: 'Ada', age: 36 });
   * ```
   */
  hset(key: Key, fieldOrFields: Key | Record<string, Value> | ReadonlyArray<readonly [Key, Value]>, value?: Value): R<number>;
  hset(key: Key, f: any, value?: Value): R<number> {
    if (value !== undefined) return this._3(null, C.HSET, key, f, value);
    return this._v(null, C.HSET, flatPairs([key], f));
  }
  /**
   * Sets hash fields like {@link hset} but sends `HMSET`, which replies `OK` instead of the count of new fields.
   * Redis marks `HMSET` deprecated; prefer `hset` in new code.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param fields An object `{ field: value }` or an array of `[field, value]` pairs.
   * @returns `'OK'`.
   *
   * @example
   * ```ts
   * await client.hmset('user:1', { name: 'Ada', city: 'London' });
   * ```
   */
  hmset(key: Key, fields: Record<string, Value> | ReadonlyArray<readonly [Key, Value]>): R<'OK'> {
    return this._v(null, C.HMSET, flatPairs([key], fields));
  }
  /**
   * Reads one hash field (`HGET`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param field Hash field name.
   * @returns The value, or `null` when the field or key is missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.hget('user:1', 'name');
   * ```
   */
  hget(key: Key, field: Key): R<string | null> {
    return this._2(null, C.HGET, key, field);
  }
  /**
   * Like {@link hget}, but returns the raw bytes.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param field Hash field name.
   * @returns The value as a Buffer, or `null`.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * const raw = await client.hgetBuffer('user:1', 'avatar');
   * ```
   */
  hgetBuffer(key: Key, field: Key): R<Buffer | null> {
    return this._bin(() => this._2(null, C.HGET, key, field));
  }
  /**
   * Deletes hash fields (`HDEL`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param fields Hash field names.
   * @returns The number of fields removed. A hash that becomes empty is deleted.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.hdel('user:1', 'age');
   * ```
   */
  hdel(key: Key, ...fields: Key[]): R<number> {
    return fields.length === 1 ? this._2(null, C.HDEL, key, fields[0]!) : this._kv(null, C.HDEL, key, fields);
  }
  /**
   * Number of fields in a hash (`HLEN`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @returns The count, or `0` when the key is missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.hlen('user:1');
   * ```
   */
  hlen(key: Key): R<number> {
    return this._1(null, C.HLEN, key);
  }
  /**
   * Reads the whole hash (`HGETALL`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @returns An object of field to value; `{}` when the key is missing. Field order is unspecified. For large hashes prefer {@link hscanIterator}.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * const user = await client.hgetall('user:1'); // { name: 'Ada' }
   * ```
   */
  hgetall(key: Key): R<Record<string, string>> {
    return this._1(T.map, C.HGETALL, key);
  }
  /**
   * Reads several fields (`HMGET`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param fields Hash field names.
   * @returns One entry per field, in order; `null` for missing fields (all `null` when the key is missing).
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * const [name, age] = await client.hmget('user:1', 'name', 'age');
   * ```
   */
  hmget(key: Key, ...fields: Key[]): R<(string | null)[]> {
    return this._kv(null, C.HMGET, key, fields);
  }
  /**
   * Tests whether a field exists (`HEXISTS`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param field Hash field name.
   * @returns `true` or `false`.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.hexists('user:1', 'name');
   * ```
   */
  hexists(key: Key, field: Key): R<boolean> {
    return this._2(T.bool, C.HEXISTS, key, field);
  }
  /**
   * Lists field names (`HKEYS`); order is unspecified.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @returns Field names, or `[]` when the key is missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.hkeys('user:1');
   * ```
   */
  hkeys(key: Key): R<string[]> {
    return this._1(null, C.HKEYS, key);
  }
  /**
   * Lists values (`HVALS`); order is unspecified.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @returns Values, or `[]` when the key is missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.hvals('user:1');
   * ```
   */
  hvals(key: Key): R<string[]> {
    return this._1(null, C.HVALS, key);
  }
  /**
   * Adds an integer to a field, creating it as `0` when missing (`HINCRBY`). Keeps a field TTL.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param field Hash field name.
   * @param delta Whole number to add (may be negative).
   * @param opts {@link TtlOptions}: `ttl` in seconds, applied to the key only when this call creates it.
   * @returns The new value.
   * @throws {@link RuvioError} `ERR` when the field is not an integer or the result overflows; nothing is written then.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   * @throws RangeError when `opts.ttl` is not a positive finite number (the call rejects before sending).
   *
   * @example
   * ```ts
   * await client.hincrby('stats', 'views', 1);
   * ```
   */
  hincrby(key: Key, field: Key, delta: number, opts?: TtlOptions): R<number> {
    return opts?.ttl === undefined ? this._3(null, C.HINCRBY, key, field, delta) : this._v(null, C.HINCRBY, pushTtl([key, field, delta], opts.ttl));
  }
  /**
   * Adds a decimal number to a field (`HINCRBYFLOAT`). Computed in double precision and printed with 16 significant digits (Redis uses long double). Keeps a field TTL.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param field Hash field name.
   * @param delta Number to add (may be negative).
   * @returns The new value.
   * @throws {@link RuvioError} `ERR` when the field is not a number.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.hincrbyfloat('acct', 'balance', 2.5);
   * ```
   */
  hincrbyfloat(key: Key, field: Key, delta: number): R<number> {
    return this._3(T.float, C.HINCRBYFLOAT, key, field, delta);
  }
  /**
   * Sets a field only when it does not exist (`HSETNX`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param field Hash field name.
   * @param value Value to store: string, Buffer or number (numbers are sent as ASCII text).
   * @returns `true` when set; `false` when the field already existed (no write).
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.hsetnx('user:1', 'created', Date.now());
   * ```
   */
  hsetnx(key: Key, field: Key, value: Value): R<boolean> {
    return this._3(T.bool, C.HSETNX, key, field, value);
  }
  /**
   * Length in bytes of a field value (`HSTRLEN`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param field Hash field name.
   * @returns The length, or `0` when the key or field is missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.hstrlen('user:1', 'name');
   * ```
   */
  hstrlen(key: Key, field: Key): R<number> {
    return this._2(null, C.HSTRLEN, key, field);
  }
  /**
   * Reads one page of hash entries (`HSCAN`). See {@link hscanIterator} for a lazy walk.
   * Pages follow storage order. A missing key returns cursor `'0'` and no items.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param cursor Cursor from the previous page; start with `0`. A returned cursor of `"0"` means the walk is complete.
   * @param opts {@link ScanOptions}: `match` glob (on field names) and `count` page-size hint.
   * @returns A {@link ScanPage} of `[field, value]` pairs.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * const page = await client.hscan('user:1', 0, { count: 100 });
   * ```
   */
  hscan(key: Key, cursor: string | number, opts?: ScanOptions): R<ScanPage<[string, string]>> {
    return this._v(T.hscan, C.HSCAN, pushScan([key, cursor], opts));
  }
  private _hfe(c: Cmd, key: Key, time: number | null, fields: readonly Key[], cond?: ExpireCondition): R<number[]> {
    const a: Arg[] = time === null ? [key] : [key, time];
    if (cond !== undefined) a.push(cond);
    a.push(W.FIELDS, fields.length);
    pushAll(a, fields);
    return this._v(null, c, a);
  }
  /**
   * Sets a TTL in seconds on hash fields (`HEXPIRE key seconds [NX|XX|GT|LT] FIELDS n field...`).
   * `HSET` and `HDEL` on a field drop its TTL; `HINCRBY` keeps it. Absolute deadlines are limited to 2^46-1 ms.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param seconds Time to live in seconds. Zero or negative deletes the field (result `2`).
   * @param fields Fields to expire.
   * @param cond Optional condition: `NX` (no TTL yet), `XX` (has a TTL), `GT` (new TTL greater), `LT` (new TTL smaller).
   * @returns One code per field: `-2` field or key missing, `0` condition not met, `1` TTL set, `2` field deleted.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.hexpire('session:9', 60, ['token']); // [1]
   * ```
   */
  hexpire(key: Key, seconds: number, fields: Key[], cond?: ExpireCondition): R<number[]> {
    return this._hfe(C.HEXPIRE, key, seconds, fields, cond);
  }
  /**
   * Like {@link hexpire}, with the TTL in milliseconds (`HPEXPIRE`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param ms Time to live in milliseconds.
   * @param fields Fields to expire.
   * @param cond Optional `NX`, `XX`, `GT` or `LT`.
   * @returns One code per field: `-2` missing, `0` condition not met, `1` set, `2` deleted.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.hpexpire('session:9', 1500, ['token']);
   * ```
   */
  hpexpire(key: Key, ms: number, fields: Key[], cond?: ExpireCondition): R<number[]> {
    return this._hfe(C.HPEXPIRE, key, ms, fields, cond);
  }
  /**
   * Like {@link hexpire}, with an absolute deadline in Unix seconds (`HEXPIREAT`). A time in the past deletes the field (result `2`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param unixSeconds Deadline in Unix seconds.
   * @param fields Fields to expire.
   * @param cond Optional `NX`, `XX`, `GT` or `LT`.
   * @returns One code per field: `-2` missing, `0` condition not met, `1` set, `2` deleted.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.hexpireat('session:9', Math.floor(Date.now() / 1000) + 60, ['token']);
   * ```
   */
  hexpireat(key: Key, unixSeconds: number, fields: Key[], cond?: ExpireCondition): R<number[]> {
    return this._hfe(C.HEXPIREAT, key, unixSeconds, fields, cond);
  }
  /**
   * Like {@link hexpire}, with an absolute deadline in Unix milliseconds (`HPEXPIREAT`). A time in the past deletes the field (result `2`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param unixMs Deadline in Unix milliseconds.
   * @param fields Fields to expire.
   * @param cond Optional `NX`, `XX`, `GT` or `LT`.
   * @returns One code per field: `-2` missing, `0` condition not met, `1` set, `2` deleted.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.hpexpireat('session:9', Date.now() + 5000, ['token']);
   * ```
   */
  hpexpireat(key: Key, unixMs: number, fields: Key[], cond?: ExpireCondition): R<number[]> {
    return this._hfe(C.HPEXPIREAT, key, unixMs, fields, cond);
  }
  /**
   * Remaining TTL of hash fields in seconds, rounded up (`HTTL`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param fields Hash field names.
   * @returns One value per field: seconds left, `-1` field has no TTL, `-2` field or key missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.httl('session:9', 'token');
   * ```
   */
  httl(key: Key, ...fields: Key[]): R<number[]> {
    return this._hfe(C.HTTL, key, null, fields);
  }
  /**
   * Remaining TTL of hash fields in milliseconds (`HPTTL`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param fields Hash field names.
   * @returns One value per field: milliseconds left, `-1` no TTL, `-2` field or key missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.hpttl('session:9', 'token');
   * ```
   */
  hpttl(key: Key, ...fields: Key[]): R<number[]> {
    return this._hfe(C.HPTTL, key, null, fields);
  }
  /**
   * Absolute expiry of hash fields as Unix seconds, rounded up (`HEXPIRETIME`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param fields Hash field names.
   * @returns One value per field: Unix seconds, `-1` no TTL, `-2` field or key missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.hexpiretime('session:9', 'token');
   * ```
   */
  hexpiretime(key: Key, ...fields: Key[]): R<number[]> {
    return this._hfe(C.HEXPIRETIME, key, null, fields);
  }
  /**
   * Absolute expiry of hash fields as Unix milliseconds (`HPEXPIRETIME`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param fields Hash field names.
   * @returns One value per field: Unix milliseconds, `-1` no TTL, `-2` field or key missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.hpexpiretime('session:9', 'token');
   * ```
   */
  hpexpiretime(key: Key, ...fields: Key[]): R<number[]> {
    return this._hfe(C.HPEXPIRETIME, key, null, fields);
  }
  /**
   * Removes the TTL from hash fields (`HPERSIST`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param fields Hash field names.
   * @returns One code per field: `1` TTL removed, `-1` field had no TTL, `-2` field or key missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.hpersist('session:9', 'token');
   * ```
   */
  hpersist(key: Key, ...fields: Key[]): R<number[]> {
    return this._hfe(C.HPERSIST, key, null, fields);
  }
  /**
   * Returns random fields (`HRANDFIELD [count [WITHVALUES]]`). Reads the whole hash, so cost grows with its size.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param count Optional. Positive: up to that many distinct fields. Negative: `|count|` fields, repeats allowed.
   * @param withValues With `count`, return `[field, value]` pairs instead of field names.
   * @returns Without `count`: a field name or `null`. With `count`: an array of names, or of `[field, value]` pairs with `withValues`.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.hrandfield('user:1');                 // 'name'
   * await client.hrandfield('user:1', 2, true);        // [['name','Ada'], ['age','36']]
   * ```
   */
  hrandfield(key: Key): R<string | null>;
  /**
   * Returns random fields (`HRANDFIELD [count [WITHVALUES]]`). Reads the whole hash, so cost grows with its size.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param count Optional. Positive: up to that many distinct fields. Negative: `|count|` fields, repeats allowed.
   * @param withValues With `count`, return `[field, value]` pairs instead of field names.
   * @returns Without `count`: a field name or `null`. With `count`: an array of names, or of `[field, value]` pairs with `withValues`.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.hrandfield('user:1');                 // 'name'
   * await client.hrandfield('user:1', 2, true);        // [['name','Ada'], ['age','36']]
   * ```
   */
  hrandfield(key: Key, count: number): R<string[]>;
  /**
   * Returns random fields (`HRANDFIELD [count [WITHVALUES]]`). Reads the whole hash, so cost grows with its size.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param count Optional. Positive: up to that many distinct fields. Negative: `|count|` fields, repeats allowed.
   * @param withValues With `count`, return `[field, value]` pairs instead of field names.
   * @returns Without `count`: a field name or `null`. With `count`: an array of names, or of `[field, value]` pairs with `withValues`.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.hrandfield('user:1');                 // 'name'
   * await client.hrandfield('user:1', 2, true);        // [['name','Ada'], ['age','36']]
   * ```
   */
  hrandfield(key: Key, count: number, withValues: true): R<[string, string][]>;
  /**
   * Returns random fields (`HRANDFIELD [count [WITHVALUES]]`). Reads the whole hash, so cost grows with its size.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param count Optional. Positive: up to that many distinct fields. Negative: `|count|` fields, repeats allowed.
   * @param withValues With `count`, return `[field, value]` pairs instead of field names.
   * @returns Without `count`: a field name or `null`. With `count`: an array of names, or of `[field, value]` pairs with `withValues`.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.hrandfield('user:1');                 // 'name'
   * await client.hrandfield('user:1', 2, true);        // [['name','Ada'], ['age','36']]
   * ```
   */
  hrandfield(key: Key, count?: number, withValues?: boolean): R<string | null | string[] | [string, string][]>;
  hrandfield(key: Key, count?: number, withValues?: boolean): R<any> {
    if (count === undefined) return this._1(null, C.HRANDFIELD, key);
    if (withValues) return this._3(T.pairs, C.HRANDFIELD, key, count, W.WITHVALUES);
    return this._2(null, C.HRANDFIELD, key, count);
  }

  // ---- sorted sets -----------------------------------------------------------------------

  /**
   * Adds or updates members of a sorted set (`ZADD key [NX|XX] [GT|LT] [CH] score member ...`).
   * Pass one score and member, or an array of members (`{ member, score }` objects or `[score, member]` pairs).
   * `nx` and `xx` are exclusive (`nx` wins); so are `gt` and `lt`. `gt`/`lt` still add a missing member.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param score Score of the member (single form). Doubles, `-Infinity` and `Infinity` are accepted.
   * @param member Member name (single form).
   * @param members Array of `{ member, score }` objects or `[score, member]` pairs (multi form).
   * @param opts {@link ZAddOptions}: `nx` only add, `xx` only update, `gt`/`lt` only update when the new score is greater/smaller, `ch` count changed members too, `ttl` (seconds) sets a deadline only when this call creates the key (`EX`/`PX`).
   * @returns The number of members added; with `ch`, added plus changed.
   * @throws {@link RuvioError} `ERR` for a non-numeric score or a bad option mix; `WRONGTYPE`.
   *
   * @example
   * ```ts
   * await client.zadd('board', 100, 'ada');
   * await client.zadd('board', [{ member: 'bob', score: 80 }, [90, 'cy']], { ch: true });
   * ```
   */
  zadd(key: Key, score: number, member: Value, opts?: ZAddOptions): R<number>;
  /**
   * Adds or updates members of a sorted set (`ZADD key [NX|XX] [GT|LT] [CH] score member ...`).
   * Pass one score and member, or an array of members (`{ member, score }` objects or `[score, member]` pairs).
   * `nx` and `xx` are exclusive (`nx` wins); so are `gt` and `lt`. `gt`/`lt` still add a missing member.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param score Score of the member (single form). Doubles, `-Infinity` and `Infinity` are accepted.
   * @param member Member name (single form).
   * @param members Array of `{ member, score }` objects or `[score, member]` pairs (multi form).
   * @param opts {@link ZAddOptions}: `nx` only add, `xx` only update, `gt`/`lt` only update when the new score is greater/smaller, `ch` count changed members too, `ttl` (seconds) sets a deadline only when this call creates the key (`EX`/`PX`).
   * @returns The number of members added; with `ch`, added plus changed.
   * @throws {@link RuvioError} `ERR` for a non-numeric score or a bad option mix; `WRONGTYPE`.
   *
   * @example
   * ```ts
   * await client.zadd('board', 100, 'ada');
   * await client.zadd('board', [{ member: 'bob', score: 80 }, [90, 'cy']], { ch: true });
   * ```
   */
  zadd(key: Key, members: ReadonlyArray<SortedSetEntry | readonly [number, Value]>, opts?: ZAddOptions): R<number>;
  /**
   * Adds or updates members of a sorted set (`ZADD key [NX|XX] [GT|LT] [CH] score member ...`).
   * Pass one score and member, or an array of members (`{ member, score }` objects or `[score, member]` pairs).
   * `nx` and `xx` are exclusive (`nx` wins); so are `gt` and `lt`. `gt`/`lt` still add a missing member.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param score Score of the member (single form). Doubles, `-Infinity` and `Infinity` are accepted.
   * @param member Member name (single form).
   * @param members Array of `{ member, score }` objects or `[score, member]` pairs (multi form).
   * @param opts {@link ZAddOptions}: `nx` only add, `xx` only update, `gt`/`lt` only update when the new score is greater/smaller, `ch` count changed members too, `ttl` (seconds) sets a deadline only when this call creates the key (`EX`/`PX`).
   * @returns The number of members added; with `ch`, added plus changed.
   * @throws {@link RuvioError} `ERR` for a non-numeric score or a bad option mix; `WRONGTYPE`.
   *
   * @example
   * ```ts
   * await client.zadd('board', 100, 'ada');
   * await client.zadd('board', [{ member: 'bob', score: 80 }, [90, 'cy']], { ch: true });
   * ```
   */
  zadd(key: Key, scoreOrMembers: number | ReadonlyArray<SortedSetEntry | readonly [number, Value]>, memberOrOpts?: Value | ZAddOptions, opts?: ZAddOptions): R<number>;
  zadd(key: Key, a: any, b?: any, c?: any): R<number> {
    if (typeof a === 'number' && c === undefined) return this._3(null, C.ZADD, key, a, b);
    const opts: ZAddOptions | undefined = typeof a === 'number' ? c : b;
    const args: Arg[] = [key];
    pushZAddFlags(args, opts);
    if (typeof a === 'number') args.push(a, b);
    else for (const e of a) Array.isArray(e) ? args.push(e[0], e[1]) : args.push(e.score, e.member);
    return this._v(null, C.ZADD, args);
  }
  /**
   * Increments one member's score (`ZADD ... INCR score member`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param increment Amount added to the score (the member is created with it if missing).
   * @param member Member name.
   * @param opts {@link ZAddOptions} conditions (`nx`, `xx`, `gt`, `lt`) and `ttl`.
   * @returns The new score, or `null` when a condition blocked the update.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.zaddIncr('board', 5, 'ada'); // 105
   * ```
   */
  zaddIncr(key: Key, increment: number, member: Value, opts?: ZAddOptions): R<number | null> {
    const args: Arg[] = [key];
    pushZAddFlags(args, opts);
    args.push(W.INCR, increment, member);
    return this._v(T.float, C.ZADD, args);
  }
  /**
   * Reads members by rank, score or lexicographic range (`ZRANGE key start stop [BYSCORE|BYLEX] [REV] [LIMIT offset count]`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param start Range start. A rank by default; with `by: 'SCORE'` a score bound (`-inf`, `+inf`, `(5` for exclusive); with `by: 'LEX'` `-`, `+`, `[member` or `(member`.
   * @param stop Range end, in the same form as `start`. For ranks, negative counts from the end.
   * @param opts {@link ZRangeOptions}: `by`, `rev` (with `rev` give the high bound first), `limit` (`{ offset, count }`; a negative count returns the rest).
   * @returns Member names in order; empty when the key is missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.zrange('board', 0, -1);                        // lowest to highest
   * await client.zrange('board', '(80', '+inf', { by: 'SCORE', limit: { offset: 0, count: 10 } });
   * ```
   */
  zrange(key: Key, start: Value, stop: Value, opts?: ZRangeOptions): R<string[]> {
    return opts === undefined ? this._3(null, C.ZRANGE, key, start, stop) : this._v(null, C.ZRANGE, zrangeArgs(key, start, stop, opts, false));
  }
  /**
   * Like {@link zrange}, but returns scores too (`ZRANGE ... WITHSCORES`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param start Range start (see {@link zrange}).
   * @param stop Range end (see {@link zrange}).
   * @param opts {@link ZRangeOptions}.
   * @returns {@link SortedSetEntry} objects `{ member, score }`; empty when the key is missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * const top = await client.zrangeWithScores('board', 0, 2, { rev: true });
   * ```
   */
  zrangeWithScores(key: Key, start: Value, stop: Value, opts?: ZRangeOptions): R<SortedSetEntry[]> {
    return this._v(T.zentries, C.ZRANGE, zrangeArgs(key, start, stop, opts, true));
  }
  /**
   * Reads members by rank from the highest score down (`ZREVRANGE`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param start First rank (0 is the highest score).
   * @param stop Last rank, inclusive; `-1` is the end.
   * @returns Member names.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.zrevrange('board', 0, 9); // top 10
   * ```
   */
  zrevrange(key: Key, start: number, stop: number): R<string[]> {
    return this._3(null, C.ZREVRANGE, key, start, stop);
  }
  /**
   * Like {@link zrevrange}, with scores (`ZREVRANGE ... WITHSCORES`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param start First rank.
   * @param stop Last rank, inclusive.
   * @returns {@link SortedSetEntry} objects.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.zrevrangeWithScores('board', 0, 2);
   * ```
   */
  zrevrangeWithScores(key: Key, start: number, stop: number): R<SortedSetEntry[]> {
    return this._4(T.zentries, C.ZREVRANGE, key, start, stop, W.WITHSCORES);
  }
  /**
   * Reads members whose score is in a range, lowest first (`ZRANGEBYSCORE`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param min Lower bound: a number, `-inf`, or `(5` for exclusive.
   * @param max Upper bound: a number, `+inf`, or `(5` for exclusive.
   * @param limit Optional `{ offset, count }` page; a negative `count` returns the rest.
   * @returns Member names.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.zrangebyscore('board', 80, '(100');
   * ```
   */
  zrangebyscore(key: Key, min: Value, max: Value, limit?: { offset: number; count: number }): R<string[]> {
    const a: Arg[] = [key, min, max];
    if (limit !== undefined) a.push(W.LIMIT, limit.offset, limit.count);
    return this._v(null, C.ZRANGEBYSCORE, a);
  }
  /**
   * Like {@link zrangebyscore}, with scores.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param min Lower bound: a number, `-inf`, or `(5` for exclusive.
   * @param max Upper bound: a number, `+inf`, or `(5` for exclusive.
   * @param limit Optional `{ offset, count }` page.
   * @returns {@link SortedSetEntry} objects.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.zrangebyscoreWithScores('board', '-inf', '+inf', { offset: 0, count: 5 });
   * ```
   */
  zrangebyscoreWithScores(key: Key, min: Value, max: Value, limit?: { offset: number; count: number }): R<SortedSetEntry[]> {
    const a: Arg[] = [key, min, max, W.WITHSCORES];
    if (limit !== undefined) a.push(W.LIMIT, limit.offset, limit.count);
    return this._v(T.zentries, C.ZRANGEBYSCORE, a);
  }
  /**
   * Reads members in lexicographic order within a range (`ZRANGEBYLEX`). Meant for sets where all scores are equal; the score is ignored for ordering.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param min Lower bound: `-`, `+`, `[member` (inclusive) or `(member` (exclusive).
   * @param max Upper bound in the same form.
   * @param limit Optional `{ offset, count }` page; a negative `count` returns the rest.
   * @returns Member names.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.zrangebylex('words', '[a', '(b');
   * ```
   */
  zrangebylex(key: Key, min: Value, max: Value, limit?: { offset: number; count: number }): R<string[]> {
    const a: Arg[] = [key, min, max];
    if (limit !== undefined) a.push(W.LIMIT, limit.offset, limit.count);
    return this._v(null, C.ZRANGEBYLEX, a);
  }
  /**
   * Like {@link zrangebylex}, in reverse order (`ZREVRANGEBYLEX`). The first bound is the high one.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param max Upper bound: `+`, `-`, `[member` or `(member`.
   * @param min Lower bound in the same form.
   * @param limit Optional `{ offset, count }` page.
   * @returns Member names, highest first.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.zrevrangebylex('words', '+', '-');
   * ```
   */
  zrevrangebylex(key: Key, max: Value, min: Value, limit?: { offset: number; count: number }): R<string[]> {
    const a: Arg[] = [key, max, min];
    if (limit !== undefined) a.push(W.LIMIT, limit.offset, limit.count);
    return this._v(null, C.ZREVRANGEBYLEX, a);
  }
  /**
   * Removes members (`ZREM`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param members Member names.
   * @returns The number of members removed. A set that becomes empty is deleted.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.zrem('board', 'bob');
   * ```
   */
  zrem(key: Key, ...members: Value[]): R<number> {
    return this._kv(null, C.ZREM, key, members);
  }
  /**
   * Adds to a member's score, creating it when missing (`ZINCRBY`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param increment Amount to add (may be negative).
   * @param member Member name.
   * @param opts {@link TtlOptions}: `ttl` in seconds, applied to the key only when this call creates it.
   * @returns The new score.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   * @throws RangeError when `opts.ttl` is not a positive finite number (the call rejects before sending).
   *
   * @example
   * ```ts
   * await client.zincrby('board', 10, 'ada');
   * ```
   */
  zincrby(key: Key, increment: number, member: Value, opts?: TtlOptions): R<number> {
    return opts?.ttl === undefined ? this._3(T.float, C.ZINCRBY, key, increment, member) : this._v(T.float, C.ZINCRBY, pushTtl([key, increment, member], opts.ttl));
  }
  /**
   * Number of members (`ZCARD`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @returns The count, or `0` when the key is missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.zcard('board');
   * ```
   */
  zcard(key: Key): R<number> {
    return this._1(null, C.ZCARD, key);
  }
  /**
   * Score of one member (`ZSCORE`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param member Member name.
   * @returns The score, or `null` when the member or key is missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.zscore('board', 'ada'); // 105
   * ```
   */
  zscore(key: Key, member: Value): R<number | null> {
    return this._2(T.float, C.ZSCORE, key, member);
  }
  /**
   * Scores of several members (`ZMSCORE`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param members Member names.
   * @returns One entry per member: the score or `null`.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.zmscore('board', 'ada', 'nobody'); // [105, null]
   * ```
   */
  zmscore(key: Key, ...members: Value[]): R<(number | null)[]> {
    return this._kv(T.floatArr, C.ZMSCORE, key, members);
  }
  /**
   * Rank of a member, lowest score first (`ZRANK`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param member Member name.
   * @returns The zero-based rank, or `null` when the member or key is missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.zrank('board', 'ada');
   * ```
   */
  zrank(key: Key, member: Value): R<number | null> {
    return this._2(null, C.ZRANK, key, member);
  }
  /**
   * Rank of a member, highest score first (`ZREVRANK`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param member Member name.
   * @returns The zero-based rank (0 is the top), or `null` when the member or key is missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.zrevrank('board', 'ada'); // 0
   * ```
   */
  zrevrank(key: Key, member: Value): R<number | null> {
    return this._2(null, C.ZREVRANK, key, member);
  }
  /**
   * Removes and returns the lowest-scored members (`ZPOPMIN [count]`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param count Number of members (default 1).
   * @returns {@link SortedSetEntry} objects, lowest first; empty when the key is missing or `count` is 0.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * const [lowest] = await client.zpopmin('board');
   * ```
   */
  zpopmin(key: Key, count?: number): R<SortedSetEntry[]> {
    return count === undefined ? this._1(T.zentries, C.ZPOPMIN, key) : this._2(T.zentries, C.ZPOPMIN, key, count);
  }
  /**
   * Removes and returns the members that are due (`ZPOPBYSCORE key max MIN min COUNT count WITHSCORES`):
   * up to `count` members whose score is between `min` and `max`, lowest score first. With scores set to due
   * times this takes the jobs that are ready.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param max Upper bound: a number, `+inf`, or `(5` for exclusive.
   * @param min Lower bound: a number, `-inf` (default), or `(5` for exclusive.
   * @param count Number of members, 1 to 10000 (default 1).
   * @returns {@link SortedSetEntry} objects, lowest first; empty when nothing is due.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * const jobs = await client.zpopbyscore('scheduled', Date.now(), '-inf', 100);
   * ```
   */
  zpopbyscore(key: Key, max: Value, min: Value = '-inf', count = 1): R<SortedSetEntry[]> {
    return this._v(T.zentries, C.ZPOPBYSCORE, [key, max, W.MIN, min, W.COUNT, count, W.WITHSCORES]);
  }
  /**
   * Moves the members that are due from a sorted set to the tail of a list (`ZMOVEDUE source destination max COUNT count`):
   * up to `count` members whose score is at most `max`, lowest score first. Clients blocked on `destination`
   * are served first. On a sharded server both keys must hash to the same slot.
   *
   * @param source Sorted set to take members from.
   * @param destination List that receives them.
   * @param max Upper bound: a number, `+inf`, or `(5` for exclusive.
   * @param count Number of members, 1 to 10000 (default 1).
   * @returns The number of members moved.
   * @throws {@link RuvioError} `WRONGTYPE` when `source` is not a sorted set or `destination` is not a list.
   *
   * @example
   * ```ts
   * const moved = await client.zmovedue('{q}scheduled', '{q}ready', Date.now(), 100);
   * ```
   */
  zmovedue(source: Key, destination: Key, max: Value, count = 1): R<number> {
    return this._v(null, C.ZMOVEDUE, [source, destination, max, W.COUNT, count]);
  }
  /**
   * Removes and returns the highest-scored members (`ZPOPMAX [count]`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param count Number of members (default 1).
   * @returns {@link SortedSetEntry} objects, highest first; empty when the key is missing or `count` is 0.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * const [best] = await client.zpopmax('board');
   * ```
   */
  zpopmax(key: Key, count?: number): R<SortedSetEntry[]> {
    return count === undefined ? this._1(T.zentries, C.ZPOPMAX, key) : this._2(T.zentries, C.ZPOPMAX, key, count);
  }
  /**
   * Blocking {@link zpopmin} on the first non-empty set (`BZPOPMIN`). Blocks the connection: use a dedicated client (`duplicate()`). Shard-local; at most 1,024 blocked clients per shard. `ZADD`, `ZINCRBY` and `GEOADD` serve the oldest waiting client.
   *
   * @param keys One key or several.
   * @param timeoutSeconds Seconds to wait; `0` waits forever.
   * @returns A {@link PoppedEntry} `{ key, member, score }`, or `null` on timeout.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * const e = await worker.bzpopmin('delayed', 5);
   * ```
   */
  bzpopmin(keys: Key | Key[], timeoutSeconds: number): R<PoppedEntry | null> {
    const a: Arg[] = Array.isArray(keys) ? [...keys] : [keys];
    a.push(timeoutSeconds);
    return this._v(T.bzpop, C.BZPOPMIN, a);
  }
  /**
   * Blocking {@link zpopmax} on the first non-empty set (`BZPOPMAX`). Same blocking rules as {@link bzpopmin}.
   *
   * @param keys One key or several.
   * @param timeoutSeconds Seconds to wait; `0` waits forever.
   * @returns A {@link PoppedEntry}, or `null` on timeout.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * const e = await worker.bzpopmax(['a', 'b'], 5);
   * ```
   */
  bzpopmax(keys: Key | Key[], timeoutSeconds: number): R<PoppedEntry | null> {
    const a: Arg[] = Array.isArray(keys) ? [...keys] : [keys];
    a.push(timeoutSeconds);
    return this._v(T.bzpop, C.BZPOPMAX, a);
  }
  /**
   * Removes members by rank range (`ZREMRANGEBYRANK`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param start First rank; negative counts from the end.
   * @param stop Last rank, inclusive.
   * @returns The number of members removed.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.zremrangebyrank('board', 0, -101); // keep the top 100
   * ```
   */
  zremrangebyrank(key: Key, start: number, stop: number): R<number> {
    return this._3(null, C.ZREMRANGEBYRANK, key, start, stop);
  }
  /**
   * Removes members whose score is in a range (`ZREMRANGEBYSCORE`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param min Lower bound: a number, `-inf`, or `(5` for exclusive.
   * @param max Upper bound: a number, `+inf`, or `(5` for exclusive.
   * @returns The number of members removed.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.zremrangebyscore('events', '-inf', Date.now() - 86_400_000);
   * ```
   */
  zremrangebyscore(key: Key, min: Value, max: Value): R<number> {
    return this._3(null, C.ZREMRANGEBYSCORE, key, min, max);
  }
  /**
   * Removes members in a lexicographic range (`ZREMRANGEBYLEX`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param min Lower bound: `-`, `[member` or `(member`.
   * @param max Upper bound: `+`, `[member` or `(member`.
   * @returns The number of members removed.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.zremrangebylex('words', '[a', '(b');
   * ```
   */
  zremrangebylex(key: Key, min: Value, max: Value): R<number> {
    return this._3(null, C.ZREMRANGEBYLEX, key, min, max);
  }
  /**
   * Counts members with a score in a range (`ZCOUNT`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param min Lower bound: a number, `-inf`, or `(5` for exclusive.
   * @param max Upper bound: a number, `+inf`, or `(5` for exclusive.
   * @returns The count; `0` when the key is missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.zcount('board', 80, '+inf');
   * ```
   */
  zcount(key: Key, min: Value, max: Value): R<number> {
    return this._3(null, C.ZCOUNT, key, min, max);
  }
  /**
   * Counts members in a lexicographic range (`ZLEXCOUNT`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param min Lower bound: `-`, `[member` or `(member`.
   * @param max Upper bound: `+`, `[member` or `(member`.
   * @returns The count.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.zlexcount('words', '-', '+');
   * ```
   */
  zlexcount(key: Key, min: Value, max: Value): R<number> {
    return this._3(null, C.ZLEXCOUNT, key, min, max);
  }
  /**
   * Stores the union of sorted sets (or plain sets, score 1) in `destination` (`ZUNIONSTORE`). Keys must share a slot on a multi-shard server. An empty result removes the destination.
   *
   * @param destination Key that receives the result (any existing value is replaced).
   * @param keys Input keys.
   * @param opts {@link ZStoreOptions}: `weights` (one multiplier per input) and `aggregate` (`'SUM'` default, `'MIN'`, `'MAX'`; see {@link Aggregate}).
   * @returns The size of the stored set.
   * @throws {@link RuvioError} `WRONGTYPE`; `CROSSSLOT` when the slots differ.
   *
   * @example
   * ```ts
   * await client.zunionstore('total', ['{s}a', '{s}b'], { weights: [1, 2] });
   * ```
   */
  zunionstore(destination: Key, keys: Key[], opts?: ZStoreOptions): R<number> {
    return this._v(null, C.ZUNIONSTORE, pushZStore([destination], keys, opts));
  }
  /**
   * Stores the intersection of sorted sets (or plain sets, score 1) in `destination` (`ZINTERSTORE`). Same slot and empty-result rules as {@link zunionstore}.
   *
   * @param destination Key that receives the result (any existing value is replaced).
   * @param keys Input keys.
   * @param opts {@link ZStoreOptions}: `weights`, `aggregate`.
   * @returns The size of the stored set.
   * @throws {@link RuvioError} `WRONGTYPE`; `CROSSSLOT` when the slots differ.
   *
   * @example
   * ```ts
   * await client.zinterstore('both', ['{s}a', '{s}b'], { aggregate: 'MAX' });
   * ```
   */
  zinterstore(destination: Key, keys: Key[], opts?: ZStoreOptions): R<number> {
    return this._v(null, C.ZINTERSTORE, pushZStore([destination], keys, opts));
  }
  /**
   * Stores members of the first set that are in none of the others (`ZDIFFSTORE`). Keys must share a slot. An empty result removes the destination.
   *
   * @param destination Key that receives the result (any existing value is replaced).
   * @param keys Input keys; the first is the base.
   * @returns The size of the stored set.
   * @throws {@link RuvioError} `WRONGTYPE`; `CROSSSLOT` when the slots differ.
   *
   * @example
   * ```ts
   * await client.zdiffstore('only-a', ['{s}a', '{s}b']);
   * ```
   */
  zdiffstore(destination: Key, keys: Key[]): R<number> {
    return this._v(null, C.ZDIFFSTORE, pushZStore([destination], keys));
  }
  /**
   * Union of sorted sets (or plain sets, score 1), returned directly (`ZUNION`). Keys must share a slot.
   *
   * @param keys Input keys.
   * @param opts {@link ZStoreOptions}: `weights`, `aggregate`.
   * @returns Member names.
   * @throws {@link RuvioError} `WRONGTYPE`; `CROSSSLOT` when the slots differ.
   *
   * @example
   * ```ts
   * await client.zunion(['{s}a', '{s}b']);
   * ```
   */
  zunion(keys: Key[], opts?: ZStoreOptions): R<string[]> {
    return this._v(null, C.ZUNION, pushZStore([], keys, opts));
  }
  /**
   * Like {@link zunion}, with the combined scores.
   *
   * @param keys Input keys.
   * @param opts {@link ZStoreOptions}.
   * @returns {@link SortedSetEntry} objects.
   * @throws {@link RuvioError} `WRONGTYPE`; `CROSSSLOT` when the slots differ.
   *
   * @example
   * ```ts
   * await client.zunionWithScores(['{s}a', '{s}b']);
   * ```
   */
  zunionWithScores(keys: Key[], opts?: ZStoreOptions): R<SortedSetEntry[]> {
    const a = pushZStore([], keys, opts);
    a.push(W.WITHSCORES);
    return this._v(T.zentries, C.ZUNION, a);
  }
  /**
   * Intersection of sorted sets (or plain sets), returned directly (`ZINTER`). Keys must share a slot.
   *
   * @param keys Input keys.
   * @param opts {@link ZStoreOptions}: `weights`, `aggregate`.
   * @returns Member names.
   * @throws {@link RuvioError} `WRONGTYPE`; `CROSSSLOT` when the slots differ.
   *
   * @example
   * ```ts
   * await client.zinter(['{s}a', '{s}b']);
   * ```
   */
  zinter(keys: Key[], opts?: ZStoreOptions): R<string[]> {
    return this._v(null, C.ZINTER, pushZStore([], keys, opts));
  }
  /**
   * Like {@link zinter}, with the combined scores.
   *
   * @param keys Input keys.
   * @param opts {@link ZStoreOptions}.
   * @returns {@link SortedSetEntry} objects.
   * @throws {@link RuvioError} `WRONGTYPE`; `CROSSSLOT` when the slots differ.
   *
   * @example
   * ```ts
   * await client.zinterWithScores(['{s}a', '{s}b']);
   * ```
   */
  zinterWithScores(keys: Key[], opts?: ZStoreOptions): R<SortedSetEntry[]> {
    const a = pushZStore([], keys, opts);
    a.push(W.WITHSCORES);
    return this._v(T.zentries, C.ZINTER, a);
  }
  /**
   * Members of the first sorted set that are in none of the others (`ZDIFF`). Keys must share a slot.
   *
   * @param keys Input keys; the first is the base.
   * @returns Member names.
   * @throws {@link RuvioError} `WRONGTYPE`; `CROSSSLOT` when the slots differ.
   *
   * @example
   * ```ts
   * await client.zdiff(['{s}a', '{s}b']);
   * ```
   */
  zdiff(keys: Key[]): R<string[]> {
    return this._v(null, C.ZDIFF, pushZStore([], keys));
  }
  /**
   * Like {@link zdiff}, with scores.
   *
   * @param keys Input keys; the first is the base.
   * @returns {@link SortedSetEntry} objects.
   * @throws {@link RuvioError} `WRONGTYPE`; `CROSSSLOT` when the slots differ.
   *
   * @example
   * ```ts
   * await client.zdiffWithScores(['{s}a', '{s}b']);
   * ```
   */
  zdiffWithScores(keys: Key[]): R<SortedSetEntry[]> {
    const a = pushZStore([], keys);
    a.push(W.WITHSCORES);
    return this._v(T.zentries, C.ZDIFF, a);
  }
  /**
   * Returns random members (`ZRANDMEMBER [count [WITHSCORES]]`). Reads the whole set, so cost grows with its size.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param count Optional. Positive: up to that many distinct members. Negative: `|count|` members, repeats allowed.
   * @param withScores With `count`, return {@link SortedSetEntry} objects instead of names.
   * @returns Without `count`: a member or `null`. With `count`: names, or entries with `withScores`.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.zrandmember('board');
   * await client.zrandmember('board', 3, true);
   * ```
   */
  zrandmember(key: Key): R<string | null>;
  /**
   * Returns random members (`ZRANDMEMBER [count [WITHSCORES]]`). Reads the whole set, so cost grows with its size.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param count Optional. Positive: up to that many distinct members. Negative: `|count|` members, repeats allowed.
   * @param withScores With `count`, return {@link SortedSetEntry} objects instead of names.
   * @returns Without `count`: a member or `null`. With `count`: names, or entries with `withScores`.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.zrandmember('board');
   * await client.zrandmember('board', 3, true);
   * ```
   */
  zrandmember(key: Key, count: number): R<string[]>;
  /**
   * Returns random members (`ZRANDMEMBER [count [WITHSCORES]]`). Reads the whole set, so cost grows with its size.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param count Optional. Positive: up to that many distinct members. Negative: `|count|` members, repeats allowed.
   * @param withScores With `count`, return {@link SortedSetEntry} objects instead of names.
   * @returns Without `count`: a member or `null`. With `count`: names, or entries with `withScores`.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.zrandmember('board');
   * await client.zrandmember('board', 3, true);
   * ```
   */
  zrandmember(key: Key, count: number, withScores: true): R<SortedSetEntry[]>;
  /**
   * Returns random members (`ZRANDMEMBER [count [WITHSCORES]]`). Reads the whole set, so cost grows with its size.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param count Optional. Positive: up to that many distinct members. Negative: `|count|` members, repeats allowed.
   * @param withScores With `count`, return {@link SortedSetEntry} objects instead of names.
   * @returns Without `count`: a member or `null`. With `count`: names, or entries with `withScores`.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.zrandmember('board');
   * await client.zrandmember('board', 3, true);
   * ```
   */
  zrandmember(key: Key, count?: number, withScores?: boolean): R<string | null | string[] | SortedSetEntry[]>;
  zrandmember(key: Key, count?: number, withScores?: boolean): R<any> {
    if (count === undefined) return this._1(null, C.ZRANDMEMBER, key);
    if (withScores) return this._3(T.zentries, C.ZRANDMEMBER, key, count, W.WITHSCORES);
    return this._2(null, C.ZRANDMEMBER, key, count);
  }
  /**
   * Reads one page of sorted set entries (`ZSCAN`). See {@link zscanIterator}.
   * The cursor is a position in score order. A missing key returns cursor `'0'` and no items.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param cursor Cursor from the previous page; start with `0`. A returned cursor of `"0"` means the walk is complete.
   * @param opts {@link ScanOptions}: `match` glob and `count` hint.
   * @returns A {@link ScanPage} of {@link SortedSetEntry}.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * const page = await client.zscan('board', 0);
   * ```
   */
  zscan(key: Key, cursor: string | number, opts?: ScanOptions): R<ScanPage<SortedSetEntry>> {
    return this._v(T.zscan, C.ZSCAN, pushScan([key, cursor], opts));
  }

  // ---- geo -------------------------------------------------------------------------------

  /**
   * Adds geographic points to a key (`GEOADD key [EX|PX] [NX|XX] [CH] longitude latitude member ...`). The key is stored as a sorted set whose score is a 52-bit geohash, so the `z*` commands work on it too.
   * Valid coordinates: longitude -180 to 180 and latitude -85.05112878 to 85.05112878 (degrees). Anything else is rejected and nothing is written.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param members Points to add: `{ longitude, latitude, member }`.
   * @param opts `nx` only add new members, `xx` only update existing ones, `ch` count changed members too, `ttl` (seconds) sets a deadline only when this call creates the key.
   * @returns The number of members added; with `ch`, added plus changed.
   * @throws {@link RuvioError} `ERR` when a longitude/latitude pair is out of range; `WRONGTYPE`.
   *
   * @example
   * ```ts
   * await client.geoadd('shops', [
   *   { longitude: 13.361389, latitude: 38.115556, member: 'Palermo' },
   *   { longitude: 15.087269, latitude: 37.502669, member: 'Catania' },
   * ]);
   * ```
   */
  geoadd(key: Key, members: GeoMember[], opts?: GeoAddOptions): R<number> {
    const a: Arg[] = [key];
    if (opts?.ttl !== undefined) pushTtl(a, opts.ttl);
    if (opts?.nx) a.push(W.NX);
    else if (opts?.xx) a.push(W.XX);
    if (opts?.ch) a.push(W.CH);
    for (const m of members) a.push(m.longitude, m.latitude, m.member);
    return this._v(null, C.GEOADD, a);
  }
  /**
   * Distance between two members (`GEODIST`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param member1 First member.
   * @param member2 Second member.
   * @param unit Result unit: `m` (default), `km`, `ft` or `mi`.
   * @returns The distance in `unit`, or `null` when either member or the key is missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.geodist('shops', 'Palermo', 'Catania', 'km'); // ~166.27
   * ```
   */
  geodist(key: Key, member1: Value, member2: Value, unit?: GeoUnit): R<number | null> {
    return unit === undefined
      ? this._3(T.float, C.GEODIST, key, member1, member2)
      : this._4(T.float, C.GEODIST, key, member1, member2, kw(KW_GEO_UNIT, unit));
  }
  /**
   * Returns geohash strings (`GEOHASH`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param members Member names.
   * @returns One 11-character geohash per member (the last character is always `0`), or `null` for a missing member.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.geohash('shops', 'Palermo'); // ['sqc8b49rny0']
   * ```
   */
  geohash(key: Key, ...members: Value[]): R<(string | null)[]> {
    return this._kv(null, C.GEOHASH, key, members);
  }
  /**
   * Returns coordinates (`GEOPOS`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param members Member names.
   * @returns One `[longitude, latitude]` per member, or `null` for a missing member. Values are the stored (rounded to 52-bit) coordinates.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * const [palermo] = await client.geopos('shops', 'Palermo'); // [13.36, 38.11]
   * ```
   */
  geopos(key: Key, ...members: Value[]): R<([number, number] | null)[]> {
    return this._kv(T.geopos, C.GEOPOS, key, members);
  }
  /**
   * Finds members inside a circle or a box (`GEOSEARCH`).
   * Give exactly one centre (`fromMember` or `fromLonLat`) and one shape (`byRadius` or `byBox`). Distances are in the shape's unit.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param opts {@link GeoSearchOptions}: centre, shape, `order` (`'ASC'` nearest first, `'DESC'`), `count` with `any` (return the first matches found, not the nearest), and `withCoord` / `withDist` / `withHash`.
   * @returns Member names, or {@link GeoMatch} objects (`member`, plus `distance`, `hash`, `coordinates` as requested) when any `with*` option is set. Empty when the key is missing.
   * @throws {@link RuvioError} `ERR` for a missing centre or shape, or an unknown member; `WRONGTYPE`.
   *
   * @example
   * ```ts
   * const near = await client.geosearch('shops', {
   *   fromLonLat: [15, 37],
   *   byRadius: { radius: 200, unit: 'km' },
   *   order: 'ASC',
   *   withDist: true,
   * });
   * ```
   */
  geosearch(key: Key, opts: GeoSearchOptions): R<(string | GeoMatch)[]> {
    const a: Arg[] = [key];
    pushGeoShape(a, opts);
    if (opts.withCoord) a.push(W.WITHCOORD);
    if (opts.withDist) a.push(W.WITHDIST);
    if (opts.withHash) a.push(W.WITHHASH);
    return this._v(geoTf(opts), C.GEOSEARCH, a);
  }
  /**
   * Like {@link geosearch}, but stores the matches in `destination` (`GEOSEARCHSTORE`), replacing it.
   * Keys must share a slot on a multi-shard server.
   *
   * @param destination Key that receives the result (any existing value is replaced).
   * @param source Geo key to search.
   * @param opts {@link GeoSearchOptions} plus `storeDist`: store each distance as the score instead of the geohash.
   * @returns The number of members stored.
   * @throws {@link RuvioError} `ERR` for a missing centre or shape; `CROSSSLOT` when the slots differ.
   *
   * @example
   * ```ts
   * await client.geosearchstore('{s}near', '{s}shops', { fromMember: 'Palermo', byRadius: { radius: 100, unit: 'km' } });
   * ```
   */
  geosearchstore(destination: Key, source: Key, opts: GeoSearchOptions & { storeDist?: boolean }): R<number> {
    const a: Arg[] = [destination, source];
    pushGeoShape(a, opts);
    if (opts.storeDist) a.push(W.STOREDIST);
    return this._v(null, C.GEOSEARCHSTORE, a);
  }
  /**
   * Finds members within a radius of a point (`GEORADIUS`). Prefer {@link geosearch}.
   * With `store` or `storeDist` the matches are written to that key (replacing it, same slot required) and the reply is the stored count.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param longitude Centre longitude, -180 to 180.
   * @param latitude Centre latitude, -85.05112878 to 85.05112878.
   * @param radius Search radius, in `unit`.
   * @param unit Distance unit: `m`, `km`, `ft` or `mi` (see {@link GeoUnit}; upper case also works).
   * @param opts {@link GeoRadiusOptions}: `withCoord`, `withDist`, `withHash`, `count` (+`any`), `order`, `store`, `storeDist`.
   * @returns Member names or {@link GeoMatch} objects; a number when storing.
   * @throws {@link RuvioError} `ERR` for an invalid coordinate; `WRONGTYPE`.
   *
   * @example
   * ```ts
   * await client.georadius('shops', 15, 37, 200, 'km', { withDist: true, order: 'ASC' });
   * ```
   */
  georadius(key: Key, longitude: number, latitude: number, radius: number, unit: GeoUnit, opts?: GeoRadiusOptions): R<(string | GeoMatch)[] | number> {
    const a: Arg[] = [key, longitude, latitude, radius, kw(KW_GEO_UNIT, unit)];
    pushRadius(a, opts);
    return this._v(opts ? geoTf(opts) : null, C.GEORADIUS, a);
  }
  /**
   * Like {@link georadius}, centred on an existing member (`GEORADIUSBYMEMBER`). Prefer {@link geosearch}.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param member Member whose position is the centre.
   * @param radius Search radius, in `unit`.
   * @param unit Distance unit: `m`, `km`, `ft` or `mi` (see {@link GeoUnit}; upper case also works).
   * @param opts {@link GeoRadiusOptions}.
   * @returns Member names or {@link GeoMatch} objects; a number when storing.
   * @throws {@link RuvioError} `ERR` when the member does not exist; `WRONGTYPE`.
   *
   * @example
   * ```ts
   * await client.georadiusbymember('shops', 'Palermo', 100, 'km');
   * ```
   */
  georadiusbymember(key: Key, member: Value, radius: number, unit: GeoUnit, opts?: GeoRadiusOptions): R<(string | GeoMatch)[] | number> {
    const a: Arg[] = [key, member, radius, kw(KW_GEO_UNIT, unit)];
    pushRadius(a, opts);
    return this._v(opts ? geoTf(opts) : null, C.GEORADIUSBYMEMBER, a);
  }

  // ---- bloom -----------------------------------------------------------------------------

  /**
   * Creates a Bloom filter with chosen accuracy (`BF.RESERVE key errorRate capacity [EXPANSION n | NONSCALING]`). RedisBloom wire shape, not certified module compatibility.
   * Filters scale by default: each new layer is `expansion` times larger with half the error rate, and a layer above 64 MiB is refused.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param errorRate Target false-positive rate between 0 and 1, e.g. `0.01`.
   * @param capacity Expected number of items.
   * @param opts {@link BfReserveOptions}: `expansion` growth factor, or `nonScaling` for a fixed-size filter that errors when full.
   * @returns `'OK'`.
   * @throws {@link RuvioError} `ERR` when the key exists or a parameter is invalid.
   *
   * @example
   * ```ts
   * await client.bfReserve('seen:urls', 0.001, 1_000_000);
   * ```
   */
  bfReserve(key: Key, errorRate: number, capacity: number, opts?: BfReserveOptions): R<'OK'> {
    const a: Arg[] = [key, errorRate, capacity];
    if (opts?.expansion !== undefined) a.push(W.EXPANSION, opts.expansion);
    if (opts?.nonScaling) a.push(W.NONSCALING);
    return this._v(null, C.BF_RESERVE, a);
  }
  /**
   * Adds an item to a Bloom filter (`BF.ADD`). A missing key becomes a scalable filter (error 0.01, capacity 100, expansion 2).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param item Item to test or add.
   * @returns `true` when the item was new; `false` when it was probably already present.
   * @throws {@link RuvioError} `ERR` when a non-scaling filter is full.
   *
   * @example
   * ```ts
   * if (await client.bfAdd('seen:urls', url)) crawl(url);
   * ```
   */
  bfAdd(key: Key, item: Value): R<boolean> {
    return this._2(T.bool, C.BF_ADD, key, item);
  }
  /**
   * Adds several items (`BF.MADD`) in one record.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param items Items to add.
   * @returns Per item: `true` when added (new), `false` when probably present.
   * @throws {@link RuvioError} `ERR` when a non-scaling filter is full.
   *
   * @example
   * ```ts
   * await client.bfMadd('seen:urls', 'a', 'b'); // [true, false]
   * ```
   */
  bfMadd(key: Key, ...items: Value[]): R<boolean[]> {
    return this._kv(T.boolArr, C.BF_MADD, key, items);
  }
  /**
   * Adds items, creating the filter with options if needed (`BF.INSERT key [EX|PX] ...`).
   * The creation options apply only when the key is new.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param items Items to add.
   * @param opts {@link BfInsertOptions}: `capacity`, `error`, `expansion`, `nonScaling` (creation only), `noCreate` (fail instead of creating) and `ttl` (seconds; a deadline set only when this call creates the filter).
   * @returns Per item: `true` when added, `false` when probably present.
   * @throws {@link RuvioError} `ERR not found` with `noCreate` on a missing key; `ERR` when a non-scaling filter is full.
   *
   * @example
   * ```ts
   * await client.bfInsert('seen:ids', ['x', 'y'], { capacity: 10_000, error: 0.01 });
   * ```
   */
  bfInsert(key: Key, items: Value[], opts?: BfInsertOptions): R<boolean[]> {
    const a: Arg[] = [key];
    if (opts !== undefined) {
      if (opts.ttl !== undefined) pushTtl(a, opts.ttl);
      if (opts.capacity !== undefined) a.push(W.CAPACITY, opts.capacity);
      if (opts.error !== undefined) a.push(W.ERROR, opts.error);
      if (opts.expansion !== undefined) a.push(W.EXPANSION, opts.expansion);
      if (opts.noCreate) a.push(W.NOCREATE);
      if (opts.nonScaling) a.push(W.NONSCALING);
    }
    a.push(W.ITEMS);
    pushAll(a, items);
    return this._v(T.boolArr, C.BF_INSERT, a);
  }
  /**
   * Describes a filter (`BF.INFO key [field]`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param field Optional single field name: `Capacity`, `Size`, `Number of filters`, `Number of items inserted` or `Expansion rate`.
   * @returns Without `field`: an object of all fields (`Expansion rate` is `null` for a non-scaling filter). With `field`: that value.
   * @throws {@link RuvioError} `ERR` when the key does not exist.
   *
   * @example
   * ```ts
   * const info = await client.bfInfo('seen:urls');
   * ```
   */
  bfInfo(key: Key): R<Record<string, number | null>>;
  /**
   * Describes a filter (`BF.INFO key [field]`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param field Optional single field name: `Capacity`, `Size`, `Number of filters`, `Number of items inserted` or `Expansion rate`.
   * @returns Without `field`: an object of all fields (`Expansion rate` is `null` for a non-scaling filter). With `field`: that value.
   * @throws {@link RuvioError} `ERR` when the key does not exist.
   *
   * @example
   * ```ts
   * const info = await client.bfInfo('seen:urls');
   * ```
   */
  bfInfo(key: Key, field: string): R<unknown>;
  /**
   * Describes a filter (`BF.INFO key [field]`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param field Optional single field name: `Capacity`, `Size`, `Number of filters`, `Number of items inserted` or `Expansion rate`.
   * @returns Without `field`: an object of all fields (`Expansion rate` is `null` for a non-scaling filter). With `field`: that value.
   * @throws {@link RuvioError} `ERR` when the key does not exist.
   *
   * @example
   * ```ts
   * const info = await client.bfInfo('seen:urls');
   * ```
   */
  bfInfo(key: Key, field?: string): R<unknown>;
  bfInfo(key: Key, field?: string): R<any> {
    return field === undefined ? this._1(T.map, C.BF_INFO, key) : this._2(null, C.BF_INFO, key, field);
  }
  /**
   * Number of items added so far (`BF.CARD`). Counted when an item sets a new bit, so it is approximate.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @returns The count; `0` when the key is missing.
   *
   * @example
   * ```ts
   * await client.bfCard('seen:urls');
   * ```
   */
  bfCard(key: Key): R<number> {
    return this._1(null, C.BF_CARD, key);
  }
  /**
   * Tests one item (`BF.EXISTS`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param item Item to test or add.
   * @returns `false` means definitely absent; `true` means probably present (false positives are possible). `false` for a missing key.
   *
   * @example
   * ```ts
   * await client.bfExists('seen:urls', url);
   * ```
   */
  bfExists(key: Key, item: Value): R<boolean> {
    return this._2(T.bool, C.BF_EXISTS, key, item);
  }
  /**
   * Tests several items (`BF.MEXISTS`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param items Items to add.
   * @returns One boolean per item (see {@link bfExists}); all `false` when the key is missing.
   *
   * @example
   * ```ts
   * await client.bfMexists('seen:urls', 'a', 'b');
   * ```
   */
  bfMexists(key: Key, ...items: Value[]): R<boolean[]> {
    return this._kv(T.boolArr, C.BF_MEXISTS, key, items);
  }

  // ---- json ------------------------------------------------------------------------------

  /**
   * Reads a JSON document or part of it (`JSON.GET key [path]`). Only simple `.field` and `[N]` paths are supported (not full RedisJSON).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param path Optional path; defaults to the root `$`.
   * @returns The JSON text, or `null` when the key or path does not exist. See {@link jsonGetAs} for a parsed value.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.jsonSet('user:1', '$', '{"name":"Ada","tags":["a"]}');
   * await client.jsonGet('user:1', '$.name'); // '"Ada"'
   * ```
   */
  jsonGet(key: Key, path?: string): R<string | null> {
    return path === undefined ? this._1(null, C.JSON_GET, key) : this._2(null, C.JSON_GET, key, path);
  }
  /**
   * Like {@link jsonGet}, but parses the reply with `JSON.parse`.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param path Optional path; defaults to `$`.
   * @returns The parsed value (typed as `T`, not checked), or `null` when the key or path does not exist.
   * @throws SyntaxError when the stored text is not valid JSON for `JSON.parse`.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * const user = await client.jsonGetAs<{ name: string }>('user:1');
   * ```
   */
  jsonGetAs<T = unknown>(key: Key, path?: string): R<T | null> {
    const tf = (v: string | null) => (v === null ? null : (JSON.parse(v) as T));
    return path === undefined ? this._1(tf, C.JSON_GET, key) : this._2(tf, C.JSON_GET, key, path);
  }
  /**
   * Writes a JSON value at a path (`JSON.SET key path json [NX|XX] [EX|PX]`).
   * Whole-document updates; only existing intermediate nodes are allowed, and the document is limited to 16 MiB. Writing at `[N]` where N equals the array length appends.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param path Path to write; `$` for the whole document.
   * @param json The JSON text to store (a string such as `'{"a":1}'`, `'"text"'` or `'42'`).
   * @param cond `'NX'` write only when the path is missing; `'XX'` only when it exists.
   * @param opts {@link TtlOptions}: `ttl` in seconds, applied only when this call creates the key.
   * @returns `true` when written; `false` when `NX`/`XX` blocked the write.
   * @throws {@link RuvioError} `ERR` for invalid JSON, a missing parent node or an oversized document; `WRONGTYPE`.
   *
   * @example
   * ```ts
   * await client.jsonSet('user:1', '$', '{"name":"Ada"}');
   * await client.jsonSet('user:1', '$.age', '36', 'NX');
   * ```
   */
  jsonSet(key: Key, path: string, json: Value, cond?: 'NX' | 'XX', opts?: TtlOptions): R<boolean> {
    if (opts?.ttl !== undefined) {
      const a: Arg[] = [key, path, json];
      if (cond !== undefined) a.push(cond === 'NX' ? W.NX : W.XX);
      return this._v(T.okBool, C.JSON_SET, pushTtl(a, opts.ttl));
    }
    return cond === undefined
      ? this._3(T.okBool, C.JSON_SET, key, path, json)
      : this._4(T.okBool, C.JSON_SET, key, path, json, cond === 'NX' ? W.NX : W.XX);
  }
  /**
   * Like {@link jsonSet}, but serialises `value` with `JSON.stringify`.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param path Path to write; `$` for the whole document.
   * @param value Any JSON-serialisable value.
   * @param cond `'NX'` or `'XX'`.
   * @param opts {@link TtlOptions}: `ttl` in seconds, applied only when this call creates the key.
   * @returns `true` when written; `false` when `NX`/`XX` blocked the write.
   * @throws {@link RuvioError} as for {@link jsonSet}.
   *
   * @example
   * ```ts
   * await client.jsonSetAs('user:1', '$', { name: 'Ada', tags: ['a'] });
   * ```
   */
  jsonSetAs<T>(key: Key, path: string, value: T, cond?: 'NX' | 'XX', opts?: TtlOptions): R<boolean> {
    return this.jsonSet(key, path, JSON.stringify(value), cond, opts);
  }
  /**
   * Deletes a value at a path (`JSON.DEL`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param path Optional path; defaults to `$` (the whole key).
   * @returns The number of values removed (`0` when nothing matched).
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.jsonDel('user:1', '$.age');
   * ```
   */
  jsonDel(key: Key, path?: string): R<number> {
    return path === undefined ? this._1(null, C.JSON_DEL, key) : this._2(null, C.JSON_DEL, key, path);
  }
  /**
   * Alias of {@link jsonDel} (`JSON.FORGET`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param path Optional path; defaults to `$`.
   * @returns The number of values removed.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.jsonForget('user:1');
   * ```
   */
  jsonForget(key: Key, path?: string): R<number> {
    return path === undefined ? this._1(null, C.JSON_FORGET, key) : this._2(null, C.JSON_FORGET, key, path);
  }
  /**
   * Reads one path from several documents (`JSON.MGET key... path`). All keys must share a slot.
   *
   * @param keys Keys to operate on.
   * @param path Path to read from every key.
   * @returns One JSON text per key; `null` for a missing key or a key of another type.
   * @throws {@link RuvioError} `CROSSSLOT` when the keys are in different slots.
   *
   * @example
   * ```ts
   * await client.jsonMget(['{u}1', '{u}2'], '$.name');
   * ```
   */
  jsonMget(keys: Key[], path: string): R<(string | null)[]> {
    const a: Arg[] = [...keys, path];
    return this._v(null, C.JSON_MGET, a);
  }
  /**
   * Merges a patch into a document (`JSON.MERGE`, RFC 7396 merge patch; not RFC 6902). Fields set to `null` in the patch are removed.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param path Path to merge at; `$` for the root.
   * @param patch JSON text of the patch.
   * @returns `'OK'`.
   * @throws {@link RuvioError} `ERR` for invalid JSON.
   *
   * @example
   * ```ts
   * await client.jsonMerge('user:1', '$', '{"age":37,"tags":null}');
   * ```
   */
  jsonMerge(key: Key, path: string, patch: Value): R<'OK'> {
    return this._3(null, C.JSON_MERGE, key, path, patch);
  }
  /**
   * Alias of {@link jsonMerge} (`JMERGE`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param path Path to merge at.
   * @param patch JSON text of the patch.
   * @returns `'OK'`.
   *
   * @example
   * ```ts
   * await client.jmerge('user:1', '$', '{"age":37}');
   * ```
   */
  jmerge(key: Key, path: string, patch: Value): R<'OK'> {
    return this._3(null, C.JMERGE, key, path, patch);
  }
  /**
   * Type of the value at a path (`JSON.TYPE`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param path Optional path; defaults to `$`.
   * @returns `'object'`, `'array'`, `'string'`, `'integer'`, `'number'`, `'boolean'` or `'null'`; `null` when the key or path is absent.
   *
   * @example
   * ```ts
   * await client.jsonType('user:1', '$.name'); // 'string'
   * ```
   */
  jsonType(key: Key, path?: string): R<string | null> {
    return path === undefined ? this._1(null, C.JSON_TYPE, key) : this._2(null, C.JSON_TYPE, key, path);
  }
  /**
   * Length in bytes of a string value (`JSON.STRLEN`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param path Optional path; defaults to `$`.
   * @returns The byte length, or `null` when the key or path is absent.
   * @throws {@link RuvioError} `WRONGTYPE` when the value is not a string.
   *
   * @example
   * ```ts
   * await client.jsonStrlen('user:1', '$.name');
   * ```
   */
  jsonStrlen(key: Key, path?: string): R<number | null> {
    return path === undefined ? this._1(null, C.JSON_STRLEN, key) : this._2(null, C.JSON_STRLEN, key, path);
  }
  /**
   * Number of elements of an array (`JSON.ARRLEN`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param path Optional path; defaults to `$`.
   * @returns The length, or `null` when the key or path is absent.
   * @throws {@link RuvioError} `WRONGTYPE` when the value is not an array.
   *
   * @example
   * ```ts
   * await client.jsonArrlen('user:1', '$.tags');
   * ```
   */
  jsonArrlen(key: Key, path?: string): R<number | null> {
    return path === undefined ? this._1(null, C.JSON_ARRLEN, key) : this._2(null, C.JSON_ARRLEN, key, path);
  }
  /**
   * Member names of an object, in document order (`JSON.OBJKEYS`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param path Optional path; defaults to `$`.
   * @returns The names, or `null` when the key or path is absent.
   * @throws {@link RuvioError} `WRONGTYPE` when the value is not an object.
   *
   * @example
   * ```ts
   * await client.jsonObjkeys('user:1');
   * ```
   */
  jsonObjkeys(key: Key, path?: string): R<string[] | null> {
    return path === undefined ? this._1(null, C.JSON_OBJKEYS, key) : this._2(null, C.JSON_OBJKEYS, key, path);
  }
  /**
   * Number of members of an object (`JSON.OBJLEN`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param path Optional path; defaults to `$`.
   * @returns The count, or `null` when the key or path is absent.
   *
   * @example
   * ```ts
   * await client.jsonObjlen('user:1');
   * ```
   */
  jsonObjlen(key: Key, path?: string): R<number | null> {
    return path === undefined ? this._1(null, C.JSON_OBJLEN, key) : this._2(null, C.JSON_OBJLEN, key, path);
  }
  /**
   * Adds a number to a numeric value (`JSON.NUMINCRBY`). Integer arithmetic when both are integers that do not overflow, otherwise a finite double.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param path Path of the number (required).
   * @param delta Number to add.
   * @returns The new value, or `null` when the key or path is absent.
   * @throws {@link RuvioError} `WRONGTYPE` when the value is not a number.
   *
   * @example
   * ```ts
   * await client.jsonNumincrby('user:1', '$.age', 1);
   * ```
   */
  jsonNumincrby(key: Key, path: string, delta: number): R<number | null> {
    return this._3(T.jsonNum, C.JSON_NUMINCRBY, key, path, delta);
  }
  /**
   * Multiplies a numeric value (`JSON.NUMMULTBY`). Integers stay integers unless the product overflows; a non-finite result is refused.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param path Path of the number (required).
   * @param factor Multiplier.
   * @returns The new value, or `null` when the key or path is absent.
   * @throws {@link RuvioError} `WRONGTYPE` when the value is not a number; `ERR` for a non-finite result.
   *
   * @example
   * ```ts
   * await client.jsonNummultby('item:1', '$.price', 1.1);
   * ```
   */
  jsonNummultby(key: Key, path: string, factor: number): R<number | null> {
    return this._3(T.jsonNum, C.JSON_NUMMULTBY, key, path, factor);
  }
  /**
   * Flips a boolean value (`JSON.TOGGLE`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param path Path of the boolean (required).
   * @returns The new value, or `null` when the key or path is absent.
   * @throws {@link RuvioError} `WRONGTYPE` when the value is not a boolean.
   *
   * @example
   * ```ts
   * await client.jsonToggle('user:1', '$.active');
   * ```
   */
  jsonToggle(key: Key, path: string): R<boolean | null> {
    return this._2(T.jsonBool, C.JSON_TOGGLE, key, path);
  }
  /**
   * Appends values to an array (`JSON.ARRAPPEND`). Cost grows with the array length; for append-heavy data use a list.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param path Path of the array (required).
   * @param json Values to append, each as JSON text.
   * @returns The new array length, or `null` when the key or path is absent.
   * @throws {@link RuvioError} `WRONGTYPE` when the value is not an array.
   *
   * @example
   * ```ts
   * await client.jsonArrappend('user:1', '$.tags', '"b"', '"c"');
   * ```
   */
  jsonArrappend(key: Key, path: string, ...json: Value[]): R<number | null> {
    return this._v(null, C.JSON_ARRAPPEND, pushAll([key, path], json));
  }
  /**
   * Inserts values into an array at an index (`JSON.ARRINSERT`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param path Path of the array (required).
   * @param index Insert position; negative counts from the end. An index outside `0..length` is an error.
   * @param json Values to insert, each as JSON text.
   * @returns The new array length, or `null` when the key or path is absent.
   * @throws {@link RuvioError} `ERR` for an index out of range; `WRONGTYPE`.
   *
   * @example
   * ```ts
   * await client.jsonArrinsert('user:1', '$.tags', 0, '"first"');
   * ```
   */
  jsonArrinsert(key: Key, path: string, index: number, ...json: Value[]): R<number | null> {
    return this._v(null, C.JSON_ARRINSERT, pushAll([key, path, index], json));
  }
  /**
   * Removes and returns an array element (`JSON.ARRPOP`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param path Optional path; defaults to `$`.
   * @param index Position to pop; default `-1` (the last). An index outside the array is clamped.
   * @returns The popped element as JSON text, or `null` for an empty array or absent key/path.
   * @throws {@link RuvioError} `WRONGTYPE` when the value is not an array.
   *
   * @example
   * ```ts
   * await client.jsonArrpop('user:1', '$.tags');
   * ```
   */
  jsonArrpop(key: Key, path?: string, index?: number): R<string | null> {
    if (path === undefined) return this._1(null, C.JSON_ARRPOP, key);
    return index === undefined ? this._2(null, C.JSON_ARRPOP, key, path) : this._3(null, C.JSON_ARRPOP, key, path, index);
  }
  /**
   * Keeps only a range of an array (`JSON.ARRTRIM`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param path Path of the array (required).
   * @param start First index to keep (inclusive); negative counts from the end.
   * @param stop Last index to keep (inclusive). A reversed or past-the-end range empties the array.
   * @returns The new length, or `null` when the key or path is absent.
   * @throws {@link RuvioError} `WRONGTYPE` when the value is not an array.
   *
   * @example
   * ```ts
   * await client.jsonArrtrim('user:1', '$.tags', 0, 9);
   * ```
   */
  jsonArrtrim(key: Key, path: string, start: number, stop: number): R<number | null> {
    return this._4(null, C.JSON_ARRTRIM, key, path, start, stop);
  }
  /**
   * Finds the first position of a scalar in an array (`JSON.ARRINDEX`). Scalars only; `1` and `1.0` are different values.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param path Path of the array (required).
   * @param scalarJson The scalar as JSON text, e.g. `'"b"'` or `'3'`.
   * @param start Optional first index to search.
   * @param stop Optional last index; `0` means the end.
   * @returns The index, `-1` when absent from the array, or `null` when the key or path is absent.
   *
   * @example
   * ```ts
   * await client.jsonArrindex('user:1', '$.tags', '"b"'); // 1
   * ```
   */
  jsonArrindex(key: Key, path: string, scalarJson: Value, start?: number, stop?: number): R<number | null> {
    const a: Arg[] = [key, path, scalarJson];
    if (start !== undefined) {
      a.push(start);
      if (stop !== undefined) a.push(stop);
    }
    return this._v(null, C.JSON_ARRINDEX, a);
  }
  /**
   * Empties an array or object and sets a number to 0 (`JSON.CLEAR`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param path Optional path; defaults to `$`.
   * @returns The number of values changed (`0` when already empty or not clearable).
   *
   * @example
   * ```ts
   * await client.jsonClear('user:1', '$.tags');
   * ```
   */
  jsonClear(key: Key, path?: string): R<number> {
    return path === undefined ? this._1(null, C.JSON_CLEAR, key) : this._2(null, C.JSON_CLEAR, key, path);
  }
  /**
   * Appends to a string value (`JSON.STRAPPEND`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param path Path of the string (required).
   * @param jsonString The text to append as a JSON string, e.g. `'"abc"'` (with the quotes).
   * @returns The new string length in bytes, or `null` when the key or path is absent.
   * @throws {@link RuvioError} `WRONGTYPE` when the value is not a string.
   *
   * @example
   * ```ts
   * await client.jsonStrappend('user:1', '$.name', '" Lovelace"');
   * ```
   */
  jsonStrappend(key: Key, path: string, jsonString: Value): R<number | null> {
    return this._3(null, C.JSON_STRAPPEND, key, path, jsonString);
  }
  /**
   * Creates an exact-match index over one JSON field (`JSON.INDEX CREATE name PREFIX prefix PATH path [TYPE ...]`, Ruvio extension). Database 0 only.
   * Equality on one scalar field only. The index is built in the background (`ERR index is building` until done), follows every write, delete, expiry and rename, and keeps its postings in memory (rebuilt at startup). Its definition is stored as a reserved key, visible to `KEYS`/`DBSIZE`, and dropped by `FLUSHDB`/`FLUSHALL` on database 0.
   *
   * @param name Index name.
   * @param prefix Only keys starting with this prefix are indexed.
   * @param path Path of the field to index, e.g. `$.email`.
   * @param type Value type: `'string'`, `'number'` or `'bool'`.
   * @returns `'OK'`.
   * @throws {@link RuvioError} `ERR` when the name exists or the selected database is not 0.
   *
   * @example
   * ```ts
   * await client.jsonIndexCreate('by_email', 'user:', '$.email', 'string');
   * ```
   */
  jsonIndexCreate(name: string, prefix: string, path: string, type?: 'string' | 'number' | 'bool'): R<'OK'> {
    const a: Arg[] = [name, W.PREFIX, prefix, W.PATH, path];
    if (type !== undefined) a.push(W.TYPE, type);
    return this._v(null, C.JSON_INDEX_CREATE, a);
  }
  /**
   * Removes an index (`JSON.INDEX DROP`).
   *
   * @param name Index name.
   * @returns `true` when dropped; `false` when it did not exist.
   *
   * @example
   * ```ts
   * await client.jsonIndexDrop('by_email');
   * ```
   */
  jsonIndexDrop(name: string): R<boolean> {
    return this._1(T.bool, C.JSON_INDEX_DROP, name);
  }
  /**
   * Lists indexes (`JSON.INDEX LIST`). Prints one entry per shard.
   *
   * @returns Index descriptions.
   *
   * @example
   * ```ts
   * await client.jsonIndexList();
   * ```
   */
  jsonIndexList(): R<string[]> {
    return this._0(null, C.JSON_INDEX_LIST);
  }
  /**
   * Finds keys whose indexed field equals a value (`JSON.FIND name VALUE v [COUNT n] [AFTER key]`, Ruvio extension). Runs on every shard and merges.
   *
   * @param name Index name from {@link jsonIndexCreate}.
   * @param value Value to match exactly.
   * @param opts `count` page size (at most 1000); `after` the last key of the previous page, to continue.
   * @returns Matching keys in ascending order.
   * @throws {@link RuvioError} `ERR index is building` until the first build finishes; `ERR` for an unknown index.
   *
   * @example
   * ```ts
   * const keys = await client.jsonFind('by_email', 'ada@example.com', { count: 10 });
   * ```
   */
  jsonFind(name: string, value: Value, opts?: { count?: number; after?: Key }): R<string[]> {
    const a: Arg[] = [name, W.VALUE, value];
    if (opts?.count !== undefined) a.push(W.COUNT, opts.count);
    if (opts?.after !== undefined) a.push(W.AFTER, opts.after);
    return this._v(null, C.JSON_FIND, a);
  }

  // ---- streams ---------------------------------------------------------------------------

  /**
   * Appends an entry to a stream (`XADD key [EX|PX] [NOMKSTREAM] [MAXLEN|MINID [~] n] [DELAY ms] id field value ...`).
   * `maxLen` and `minId` both trim exactly (also with `approximate`) and cannot be combined. An id that is not above the top id is rejected.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param id `'*'` for an automatic id, `'ms-*'` for an automatic sequence, or an explicit `'ms-seq'`.
   * @param fields Object `{ field: value }` or a flat array `[field, value, ...]`.
   * @param opts {@link XAddOptions}: `noMkStream`, `trim` (a {@link StreamTrim}) or the older `maxLen`, `minId`, `approximate`, Ruvio's `delayMs` which hides the entry from {@link xreadgroup} for that long, and `ttl` (seconds) which sets a deadline only when this call creates the stream.
   * @returns The entry id, or `null` when `noMkStream` is set and the key does not exist.
   * @throws {@link RuvioError} `ERR` for an id not above the top id or a bad trim option; `WRONGTYPE`.
   *
   * @example
   * ```ts
   * const id = await client.xadd('events', '*', { type: 'signup', user: 7 }, { maxLen: 10_000 });
   * ```
   */
  xadd(key: Key, id: string, fields: Record<string, Value> | Value[], opts?: XAddOptions): R<string | null> {
    const a: Arg[] = [key];
    if (opts !== undefined) {
      if (opts.ttl !== undefined) pushTtl(a, opts.ttl);
      if (opts.noMkStream) a.push(W.NOMKSTREAM);
      pushStreamId(a, opts);
      if (opts.delayMs !== undefined) a.push(W.DELAY, opts.delayMs);
    }
    a.push(id);
    return this._v(null, C.XADD, pushFields(a, fields));
  }
  /**
   * Number of entries (`XLEN`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @returns The count; `0` when the key is missing.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.xlen('events');
   * ```
   */
  xlen(key: Key): R<number> {
    return this._1(null, C.XLEN, key);
  }
  /**
   * Reads entries between two ids, oldest first (`XRANGE`). No exclusive bounds.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param start Lowest id (`'-'` for the beginning, default).
   * @param end Highest id (`'+'` for the end, default).
   * @param count Optional maximum number of entries.
   * @returns {@link StreamEntry} objects; empty when none.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * const entries = await client.xrange('events', '-', '+', 100);
   * ```
   */
  xrange(key: Key, start = '-', end = '+', count?: number): R<StreamEntry[]> {
    return count === undefined
      ? this._3(T.stream, C.XRANGE, key, start, end)
      : this._v(T.stream, C.XRANGE, [key, start, end, W.COUNT, count]);
  }
  /**
   * Reads entries between two ids, newest first (`XREVRANGE`). Note the argument order: end first, then start. `count` keeps the newest matches.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param end Highest id (`'+'` default).
   * @param start Lowest id (`'-'` default).
   * @param count Optional maximum number of entries.
   * @returns {@link StreamEntry} objects, newest first.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * const latest = await client.xrevrange('events', '+', '-', 10);
   * ```
   */
  xrevrange(key: Key, end = '+', start = '-', count?: number): R<StreamEntry[]> {
    return count === undefined
      ? this._3(T.stream, C.XREVRANGE, key, end, start)
      : this._v(T.stream, C.XREVRANGE, [key, end, start, W.COUNT, count]);
  }
  /**
   * Reads new entries from one or more streams (`XREAD [COUNT n] [BLOCK ms] STREAMS key... id...`).
   * The id is exclusive: `'0'` means from the start and `'$'` means only entries added after the call. `block` parks the whole connection, so use a dedicated client (`duplicate()`). This does not read a consumer group; see {@link xreadgroup}.
   *
   * @param streams Object `{ key: lastId }` or an array of `[key, lastId]` pairs.
   * @param opts {@link XReadOptions}: `count` and `block` (milliseconds; `0` waits until an entry or shutdown).
   * @returns One {@link StreamReadResult} per stream that has entries, or `null` when there are none (or the block timed out).
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * const worker = client.duplicate();
   * const res = await worker.xread({ events: '$' }, { block: 5000, count: 10 });
   * ```
   */
  xread(streams: Record<string, string> | ReadonlyArray<readonly [Key, string]>, opts?: XReadOptions): R<StreamReadResult[] | null> {
    const a: Arg[] = [];
    if (opts?.count !== undefined) a.push(W.COUNT, opts.count);
    if (opts?.block !== undefined) a.push(W.BLOCK, opts.block);
    a.push(W.STREAMS);
    const pairs: (readonly [Key, string])[] = Array.isArray(streams) ? (streams as (readonly [Key, string])[]) : Object.entries(streams);
    for (const p of pairs) a.push(p[0]);
    for (const p of pairs) a.push(p[1]);
    return this._v(T.xread, C.XREAD, a);
  }
  /**
   * Deletes entries by id (`XDEL`). The stream key and its last id remain even when it becomes empty.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param ids Stream entry ids (`ms-seq`).
   * @returns The number of entries deleted (a repeated id counts once).
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.xdel('events', '1700000000000-0');
   * ```
   */
  xdel(key: Key, ...ids: string[]): R<number> {
    return this._kv(null, C.XDEL, key, ids);
  }
  /**
   * Sets the stream's last generated id without adding an entry (`XSETID`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param id Must be greater than or equal to the current top id.
   * @returns `'OK'`.
   * @throws {@link RuvioError} `ERR` when the key is missing or `id` is lower than the top id.
   *
   * @example
   * ```ts
   * await client.xsetid('events', '1800000000000-0');
   * ```
   */
  xsetid(key: Key, id: string): R<'OK'> {
    return this._2(null, C.XSETID, key, id);
  }
  /**
   * Trims a stream (`XTRIM`). Trimming is always exact; the `~` option is accepted. `LIMIT` is not supported.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param opts {@link XTrimOptions} (`maxLen` keeps at most that many entries; `minId` drops entries below the id and keeps that id; not both), or a {@link StreamTrim} such as `StreamTrim.maxLen(1000, TrimMode.Approximate)`.
   * @returns The number of entries removed.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.xtrim('events', { maxLen: 1000 });
   * await client.xtrim('events', StreamTrim.maxLen(1000, TrimMode.Approximate));
   * ```
   */
  xtrim(key: Key, opts: XTrimOptions | StreamTrim): R<number> {
    const a: Arg[] = [key];
    if ('strategy' in opts) pushTrim(a, opts);
    else pushStreamId(a, opts);
    return this._v(null, C.XTRIM, a);
  }
  /**
   * Creates a consumer group (`XGROUP CREATE`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param group Consumer group name.
   * @param id Where the group starts: `'$'` (only new entries), `'0'` (everything), or an explicit id.
   * @param opts `mkStream: true` creates an empty stream when the key is missing.
   * @returns `'OK'`.
   * @throws {@link RuvioError} `BUSYGROUP` when the group exists; `ERR` when the key is missing and `mkStream` is not set.
   *
   * @example
   * ```ts
   * await client.xgroupCreate('events', 'workers', '$', { mkStream: true });
   * ```
   */
  xgroupCreate(key: Key, group: string, id: string, opts?: { mkStream?: boolean }): R<'OK'> {
    return opts?.mkStream
      ? this._4(null, C.XGROUP_CREATE, key, group, id, W.MKSTREAM)
      : this._3(null, C.XGROUP_CREATE, key, group, id);
  }
  /**
   * Registers a consumer before it has pending entries (`XGROUP CREATECONSUMER`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param group Consumer group name.
   * @param consumer Consumer name.
   * @returns `true` when the name is new; `false` when the group already had it.
   * @throws {@link RuvioError} `NOGROUP` when the group does not exist.
   *
   * @example
   * ```ts
   * await client.xgroupCreateconsumer('events', 'workers', 'w1');
   * ```
   */
  xgroupCreateconsumer(key: Key, group: string, consumer: string): R<boolean> {
    return this._3(T.bool, C.XGROUP_CREATECONSUMER, key, group, consumer);
  }
  /**
   * Deletes a consumer group (`XGROUP DESTROY`). A blocked {@link xreadgroup} on it receives `NOGROUP`.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param group Consumer group name.
   * @returns `true` when removed; `false` when it was already gone.
   *
   * @example
   * ```ts
   * await client.xgroupDestroy('events', 'workers');
   * ```
   */
  xgroupDestroy(key: Key, group: string): R<boolean> {
    return this._2(T.bool, C.XGROUP_DESTROY, key, group);
  }
  /**
   * Moves a group's last-delivered id (`XGROUP SETID`). Pending entries stay as they are.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param group Consumer group name.
   * @param id `'$'`, `'0'` or an explicit id.
   * @returns `'OK'`.
   * @throws {@link RuvioError} `NOGROUP` when the group does not exist.
   *
   * @example
   * ```ts
   * await client.xgroupSetid('events', 'workers', '0');
   * ```
   */
  xgroupSetid(key: Key, group: string, id: string): R<'OK'> {
    return this._3(null, C.XGROUP_SETID, key, group, id);
  }
  /**
   * Removes a consumer from a group (`XGROUP DELCONSUMER`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param group Consumer group name.
   * @param consumer Consumer name.
   * @returns How many pending entries that consumer held (`0` for an unknown consumer).
   * @throws {@link RuvioError} `NOGROUP` when the group does not exist.
   *
   * @example
   * ```ts
   * await client.xgroupDelconsumer('events', 'workers', 'w1');
   * ```
   */
  xgroupDelconsumer(key: Key, group: string, consumer: string): R<number> {
    return this._3(null, C.XGROUP_DELCONSUMER, key, group, consumer);
  }
  /**
   * Reads new entries for a consumer of a group (`XREADGROUP GROUP g c [COUNT n] [BLOCK ms] [NOACK] STREAMS key >`).
   * Ruvio reads one stream and only the `>` id (never-delivered entries). With `block`, the call waits on this shard for a later `XADD` and hands the id to the oldest waiting consumer of the group; it parks the connection, so use a dedicated client. Delivered entries stay pending until {@link xack}.
   *
   * @param group Consumer group name.
   * @param consumer Consumer name.
   * @param key Stream to read.
   * @param opts {@link XReadGroupOptions}: `count`, `block` (ms, `0` waits until an entry or shutdown), `noAck` (advance the group but do not add a pending entry).
   * @returns One {@link StreamReadResult}, or `null` when there is nothing new (or the block timed out).
   * @throws {@link RuvioError} `NOGROUP` when the group does not exist.
   *
   * @example
   * ```ts
   * const res = await worker.xreadgroup('workers', 'w1', 'events', { count: 10, block: 5000 });
   * ```
   */
  xreadgroup(group: string, consumer: string, key: Key, opts?: XReadGroupOptions): R<StreamReadResult[] | null> {
    const a: Arg[] = [W.GROUP, group, consumer];
    if (opts?.count !== undefined) a.push(W.COUNT, opts.count);
    if (opts?.block !== undefined) a.push(W.BLOCK, opts.block);
    if (opts?.noAck) a.push(W.NOACK);
    a.push(W.STREAMS, key, W.GTID);
    return this._v(T.xread, C.XREADGROUP, a);
  }
  /**
   * Acknowledges entries so they leave the group's pending list (`XACK`). The stream entry stays stored.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param group Consumer group name.
   * @param ids Entry ids (a repeated id counts once).
   * @returns The number of entries acknowledged.
   *
   * @example
   * ```ts
   * await client.xack('events', 'workers', id);
   * ```
   */
  xack(key: Key, group: string, ...ids: string[]): R<number> {
    return this._v(null, C.XACK, pushAll([key, group], ids));
  }
  /**
   * Summary of a group's pending entries (`XPENDING key group`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param group Consumer group name.
   * @returns A {@link PendingSummary}: `count`, `smallest`/`greatest` id (`null` when empty) and pending counts per consumer, sorted by name.
   * @throws {@link RuvioError} `NOGROUP` when the group does not exist.
   *
   * @example
   * ```ts
   * const s = await client.xpending('events', 'workers');
   * ```
   */
  xpending(key: Key, group: string): R<PendingSummary> {
    return this._2(T.pendingSummary, C.XPENDING, key, group);
  }
  /**
   * Lists pending entries (`XPENDING key group [IDLE ms] start end count [consumer]`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param group Consumer group name.
   * @param start Lowest id, inclusive (`'-'` for the beginning).
   * @param end Highest id, inclusive (`'+'` for the end).
   * @param count Maximum rows.
   * @param opts `idle` only rows idle at least this many milliseconds; `consumer` only that consumer's rows.
   * @returns {@link PendingRow} objects `{ id, consumer, idleMs, deliveries }`.
   * @throws {@link RuvioError} `NOGROUP` when the group does not exist.
   *
   * @example
   * ```ts
   * const rows = await client.xpendingRange('events', 'workers', '-', '+', 100, { idle: 60_000 });
   * ```
   */
  xpendingRange(key: Key, group: string, start: string, end: string, count: number, opts?: { idle?: number; consumer?: string }): R<PendingRow[]> {
    const a: Arg[] = [key, group];
    if (opts?.idle !== undefined) a.push(W.IDLE, opts.idle);
    a.push(start, end, count);
    if (opts?.consumer !== undefined) a.push(opts.consumer);
    return this._v(T.pendingRows, C.XPENDING, a);
  }
  /**
   * Takes over pending entries that have been idle too long (`XAUTOCLAIM`).
   * Scans pending ids after `start`, claims those idle for at least `minIdleMs`, up to `count` (default: the rest of the list).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param group Consumer group name.
   * @param consumer Consumer that takes ownership.
   * @param minIdleMs Minimum idle time in milliseconds.
   * @param start Id to start scanning from (`'0-0'` for the beginning).
   * @param opts `count` maximum entries; `justId` return ids instead of entries.
   * @returns An {@link AutoClaimResult}: `next` start id for the following call (`0-0` when the scan finished), the claimed `entries` (or ids), and `deleted` ids that no longer exist in the stream.
   * @throws {@link RuvioError} `NOGROUP` when the group does not exist.
   *
   * @example
   * ```ts
   * const r = await client.xautoclaim('events', 'workers', 'w2', 60_000, '0-0', { count: 50 });
   * ```
   */
  xautoclaim(key: Key, group: string, consumer: string, minIdleMs: number, start: string, opts?: { count?: number; justId?: boolean }): R<AutoClaimResult> {
    const a: Arg[] = [key, group, consumer, minIdleMs, start];
    if (opts?.count !== undefined) a.push(W.COUNT, opts.count);
    if (opts?.justId) a.push(W.JUSTID);
    return this._v(T.autoclaim, C.XAUTOCLAIM, a);
  }
  /**
   * Takes over specific pending entries (`XCLAIM`). Entries deleted from the stream are dropped from the pending list and not returned.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param group Consumer group name.
   * @param consumer Consumer that takes ownership.
   * @param minIdleMs Only claim entries idle at least this long (ms).
   * @param ids Entry ids to claim (a repeated id is claimed once).
   * @param opts {@link XClaimOptions}: `idle` or `time` (not both) set the idle clock, `retryCount`, `force` (claim an id that is in the stream but not pending), `justId`, `lastId`.
   * @returns The claimed {@link StreamEntry} objects, or ids with `justId`.
   * @throws {@link RuvioError} `NOGROUP`; `ERR` when `idle` and `time` are combined.
   *
   * @example
   * ```ts
   * await client.xclaim('events', 'workers', 'w2', 60_000, [id]);
   * ```
   */
  xclaim(key: Key, group: string, consumer: string, minIdleMs: number, ids: string[], opts?: XClaimOptions): R<StreamEntry[] | string[]> {
    const a: Arg[] = [key, group, consumer, minIdleMs];
    pushAll(a, ids);
    if (opts !== undefined) {
      if (opts.idle !== undefined) a.push(W.IDLE, opts.idle);
      if (opts.time !== undefined) a.push(W.TIME, opts.time);
      if (opts.retryCount !== undefined) a.push(W.RETRYCOUNT, opts.retryCount);
      if (opts.force) a.push(W.FORCE);
      if (opts.justId) a.push(W.JUSTID);
      if (opts.lastId !== undefined) a.push(W.LASTID, opts.lastId);
    }
    return this._v(T.streamOrIds, C.XCLAIM, a);
  }
  /**
   * Describes a stream (`XINFO STREAM`): length, last-generated-id, groups, first and last entry. No radix-tree or entries-added fields.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @returns A map of the fields.
   * @throws {@link RuvioError} `ERR` when the key does not exist.
   *
   * @example
   * ```ts
   * const info = await client.xinfoStream('events');
   * ```
   */
  xinfoStream(key: Key): R<Record<string, unknown>> {
    return this._1(T.map, C.XINFO_STREAM, key);
  }
  /**
   * Lists a stream's groups (`XINFO GROUPS`): name, consumers, pending, last-delivered-id, lag. Consumers are the distinct names on the pending list.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @returns One map per group.
   *
   * @example
   * ```ts
   * await client.xinfoGroups('events');
   * ```
   */
  xinfoGroups(key: Key): R<Record<string, unknown>[]> {
    return this._1(T.mapArr, C.XINFO_GROUPS, key);
  }
  /**
   * Lists a group's consumers (`XINFO CONSUMERS`) with pending count and idle time since the latest delivery. A consumer with an empty pending list is not stored.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param group Consumer group name.
   * @returns One map per consumer.
   * @throws {@link RuvioError} `NOGROUP` when the group does not exist.
   *
   * @example
   * ```ts
   * await client.xinfoConsumers('events', 'workers');
   * ```
   */
  xinfoConsumers(key: Key, group: string): R<Record<string, unknown>[]> {
    return this._2(T.mapArr, C.XINFO_CONSUMERS, key, group);
  }

  // ---- server / introspection ------------------------------------------------------------

  /**
   * Server information (`INFO [section]`).
   *
   * @param section Optional section name, see {@link InfoSection}, e.g. `InfoSection.Memory`; `'prometheus'` returns Prometheus text. An unknown name gives an empty reply.
   * @returns The text report.
   *
   * @example
   * ```ts
   * const text = await client.info(InfoSection.Memory);
   * ```
   */
  info(section?: InfoSection | (string & {})): R<string> {
    return section === undefined ? this._0(T.lines, C.INFO) : this._1(T.lines, C.INFO, kw(KW_INFO_SECTION, section));
  }
  /**
   * Reads server settings (`CONFIG GET pattern`). `requirepass` is redacted.
   *
   * @param pattern Glob over setting names, e.g. `'max*'`.
   * @returns An object of setting name to value.
   *
   * @example
   * ```ts
   * await client.configGet('port_mode');
   * ```
   */
  configGet(pattern: string): R<Record<string, string>> {
    return this._1(T.map, C.CONFIG_GET, pattern);
  }
  /**
   * Changes live settings (`CONFIG SET name value ...`), named as `CONFIG GET` shows them.
   * Every shard of the server applies the change before it answers. All pairs apply or
   * none: an unknown or restart-only name, or an invalid value, rejects and changes
   * nothing. Not written to the config file (see {@link configRewrite}) and not replicated.
   *
   * @param settings Setting names and values.
   * @returns `'OK'`.
   *
   * @example
   * ```ts
   * await client.configSet({ maxmemory: '1073741824', 'maxmemory-policy': 'allkeys-lru' });
   * ```
   */
  configSet(settings: Record<string, Value> | ReadonlyArray<readonly [Key, Value]>): R<'OK'> {
    return this._v(null, C.CONFIG_SET, flatPairs([], settings));
  }
  /**
   * Writes the live settings into the config file the server started from (`CONFIG REWRITE`).
   *
   * @returns `'OK'`.
   *
   * @example
   * ```ts
   * await client.configRewrite();
   * ```
   */
  configRewrite(): R<'OK'> {
    return this._0(null, C.CONFIG_REWRITE);
  }
  /**
   * Makes every shard of the server follow the primary at `host:port` (`REPLICAOF host port`);
   * shard `i` dials `port + i`. Writes are refused at once. A shard continues from its own log
   * when the new primary's history allows it, otherwise it takes a full copy. Lasts until the
   * server restarts.
   *
   * @returns `'OK'`.
   *
   * @example
   * ```ts
   * await client.replicaOf('10.0.0.5', 26379);
   * ```
   */
  replicaOf(host: string, port: number): R<'OK'> {
    return this._2(null, C.REPLICAOF, host, port);
  }
  /**
   * Promotes a replica in place (`REPLICAOF NO ONE`), one epoch above its old primary. `force`
   * adds `FORCE`: promote even when a shard has not finished copying its primary, which may
   * lose writes.
   *
   * @returns `'OK'`.
   *
   * @example
   * ```ts
   * await client.replicaOfNoOne();
   * ```
   */
  replicaOfNoOne(force = false): R<'OK'> {
    return force ? this._1(null, C.REPLICAOF_NO_ONE, 'FORCE') : this._0(null, C.REPLICAOF_NO_ONE);
  }
  /**
   * `ROLE` for the shard this connection is on: a primary answers `['master', sequence, []]`; a
   * replica `['slave', host, port, 'connect' | 'connected', sequence]`.
   *
   * @example
   * ```ts
   * const [role] = await client.role();
   * ```
   */
  role(): R<unknown[]> {
    return this._0(null, C.ROLE);
  }
  /**
   * Writes a checkpoint now (`SAVE`). In-process.
   *
   * @returns `'OK'`.
   *
   * @example
   * ```ts
   * await client.save();
   * ```
   */
  save(): R<'OK'> {
    return this._0(null, C.SAVE);
  }
  /**
   * Writes a checkpoint (`BGSAVE`). Runs in-process, not in a forked child.
   *
   * @returns A status message.
   *
   * @example
   * ```ts
   * await client.bgsave();
   * ```
   */
  bgsave(): R<string> {
    return this._0(null, C.BGSAVE);
  }
  /**
   * Waits for replicas (`WAIT numReplicas timeoutMs`). Blocks only the calling connection until that many replicas have synced this shard's WAL to disk, or the timeout. A missing replica never rolls the write back. Not allowed inside MULTI or scripts. With `port_mode = "per-shard"` it also covers the shards this connection's writes were forwarded to.
   *
   * @param numReplicas Replicas to wait for.
   * @param timeoutMs Milliseconds to wait; `0` waits until shutdown.
   * @returns How many replicas reached the sequence (`0` on a server that is off or a replica).
   *
   * @example
   * ```ts
   * const acked = await worker.wait(1, 2000);
   * ```
   */
  wait(numReplicas: number, timeoutMs: number): R<number> {
    return this._2(null, C.WAIT, numReplicas, timeoutMs);
  }

  // ---- transactions (raw; prefer client.multi() and client.isolated()) -------------------

  /**
   * Marks keys for optimistic locking (`WATCH`). A later write by any connection (including this one) makes the next `EXEC` fail. At most 128 watched keys.
   * Only meaningful on a connection you own: use `client.isolated(c => ...)` or a `duplicate()`d client, not the shared one.
   *
   * @param keys Keys to operate on.
   * @returns `'OK'`.
   *
   * @example
   * ```ts
   * await client.isolated(async (c) => {
   *   await c.watch('balance');
   *   const bal = Number(await c.get('balance'));
   *   const res = await c.multi().set('balance', bal - 1).exec(); // null if balance changed
   * });
   * ```
   */
  watch(...keys: Key[]): R<'OK'> {
    return this._v(null, C.WATCH, keys);
  }
  /**
   * Forgets all watched keys (`UNWATCH`).
   *
   * @returns `'OK'`.
   *
   * @example
   * ```ts
   * await c.unwatch();
   * ```
   */
  unwatch(): R<'OK'> {
    return this._0(null, C.UNWATCH);
  }

  // ---- scripts ---------------------------------------------------------------------------

  /**
   * Runs a Lua script (`EVAL`). Lua 5.1 sandbox; writes are one WAL batch.
   * Declared keys must share a slot; a script with no keys runs on the connection's shard. Scripts cannot call `SELECT`, `FLUSHDB`, `FLUSHALL`, `SWAPDB` or `MOVE`.
   *
   * @param script Lua source.
   * @param keys Keys, available to the script as `KEYS[1..]`.
   * @param args Other arguments, available as `ARGV[1..]`.
   * @returns The script's result, converted to a reply (raw: strings, numbers, arrays, `null`).
   * @throws {@link RuvioError} on a script error; `CROSSSLOT` when the keys are in different slots.
   *
   * @example
   * ```ts
   * const n = await client.eval('return redis.call("INCRBY", KEYS[1], ARGV[1])', ['hits'], [5]);
   * ```
   */
  eval(script: string, keys: Key[] = [], args: Value[] = []): R<unknown> {
    return this._v(null, C.EVAL, pushAll(pushAll([script, keys.length], keys), args));
  }
  /**
   * Runs a cached script by its SHA-1 (`EVALSHA`). The cache is per shard and memory-only, so after a restart the script is gone.
   *
   * @param sha1 Hash returned by {@link scriptLoad}.
   * @param keys Keys (`KEYS`).
   * @param args Other arguments (`ARGV`).
   * @returns The script's result.
   * @throws {@link RuvioError} `NOSCRIPT` when the script is not cached (load it again).
   *
   * @example
   * ```ts
   * const sha = await client.scriptLoad('return 1');
   * await client.evalsha(sha);
   * ```
   */
  evalsha(sha1: string, keys: Key[] = [], args: Value[] = []): R<unknown> {
    return this._v(null, C.EVALSHA, pushAll(pushAll([sha1, keys.length], keys), args));
  }
  /**
   * Caches a script without running it (`SCRIPT LOAD`). The cache is not durable.
   *
   * @param script Lua source.
   * @returns The SHA-1 hex digest.
   *
   * @example
   * ```ts
   * const sha = await client.scriptLoad('return ARGV[1]');
   * ```
   */
  scriptLoad(script: string): R<string> {
    return this._1(null, C.SCRIPT_LOAD, script);
  }
  /**
   * Tests whether scripts are cached (`SCRIPT EXISTS`).
   *
   * @param sha1s Digests to check.
   * @returns One boolean per digest.
   *
   * @example
   * ```ts
   * await client.scriptExists(sha); // [true]
   * ```
   */
  scriptExists(...sha1s: string[]): R<boolean[]> {
    return this._v(T.boolArr, C.SCRIPT_EXISTS, sha1s);
  }
  /**
   * Empties the script cache (`SCRIPT FLUSH`).
   *
   * @returns `'OK'`.
   *
   * @example
   * ```ts
   * await client.scriptFlush();
   * ```
   */
  scriptFlush(): R<'OK'> {
    return this._0(null, C.SCRIPT_FLUSH);
  }
  /**
   * Stops the running script (`SCRIPT KILL`).
   *
   * @returns `'OK'`.
   * @throws {@link RuvioError} `NOTBUSY` when no script is running.
   *
   * @example
   * ```ts
   * await client.scriptKill();
   * ```
   */
  scriptKill(): R<'OK'> {
    return this._0(null, C.SCRIPT_KILL);
  }
  /**
   * Calls a function stored with {@link functionLoad} (`FCALL`). Same key rules as {@link eval}. Lua is not replayed.
   *
   * @param name Function name.
   * @param keys Keys (`KEYS`).
   * @param args Other arguments (`ARGV`).
   * @returns The function's result.
   * @throws {@link RuvioError} when no function has that name.
   *
   * @example
   * ```ts
   * await client.fcall('bump', ['hits'], [1]);
   * ```
   */
  fcall(name: string, keys: Key[] = [], args: Value[] = []): R<unknown> {
    return this._v(null, C.FCALL, pushAll(pushAll([name, keys.length], keys), args));
  }
  /**
   * Stores a named Lua function body (`FUNCTION LOAD name body`, Ruvio form: one named body, not a Redis library shebang). Kept in memory on the shard and lost on restart.
   *
   * @param name Function name.
   * @param body Lua source.
   * @returns The server reply.
   *
   * @example
   * ```ts
   * await client.functionLoad('bump', 'return redis.call("INCR", KEYS[1])');
   * ```
   */
  functionLoad(name: string, body: string): R<unknown> {
    return this._2(null, C.FUNCTION_LOAD, name, body);
  }
  /**
   * Lists loaded functions (`FUNCTION LIST`) in byte order of their names.
   *
   * @returns The function names.
   *
   * @example
   * ```ts
   * await client.functionList();
   * ```
   */
  functionList(): R<unknown[]> {
    return this._0(null, C.FUNCTION_LIST);
  }
  /**
   * Removes a function (`FUNCTION DELETE`).
   *
   * @param name Function name.
   * @returns `true` when removed; `false` when it did not exist.
   *
   * @example
   * ```ts
   * await client.functionDelete('bump');
   * ```
   */
  functionDelete(name: string): R<boolean> {
    return this._1(T.bool, C.FUNCTION_DELETE, name);
  }

  // ---- cluster ---------------------------------------------------------------------------

  /**
   * Slot ownership (`CLUSTER SLOTS`). Static topology of the shards in one process; hosts and ports come from `advertised_host` and `advertised_port`. {@link ClusterClient} reads this itself.
   *
   * @returns Raw slot ranges with master host, port and id.
   * @throws {@link RuvioError} `ERR` ("cluster mode is disabled") on a server running a single shard.
   *
   * @example
   * ```ts
   * const slots = await client.clusterSlots();
   * ```
   */
  clusterSlots(): R<unknown[]> {
    return this._0(null, C.CLUSTER_SLOTS);
  }
  /**
   * Node list (`CLUSTER NODES`). Static topology; every shard is a primary.
   *
   * @returns Text, one line per node.
   * @throws {@link RuvioError} `ERR` when cluster mode is disabled.
   *
   * @example
   * ```ts
   * await client.clusterNodes();
   * ```
   */
  clusterNodes(): R<string> {
    return this._0(T.lines, C.CLUSTER_NODES);
  }
  /**
   * Shard list (`CLUSTER SHARDS`). Every shard is a primary.
   *
   * @returns Raw shard descriptions.
   * @throws {@link RuvioError} `ERR` when cluster mode is disabled.
   *
   * @example
   * ```ts
   * await client.clusterShards();
   * ```
   */
  clusterShards(): R<unknown[]> {
    return this._0(null, C.CLUSTER_SHARDS);
  }
  /**
   * Cluster state (`CLUSTER INFO`): `cluster_state` is always `ok` and every slot is assigned.
   *
   * @returns Text of `field:value` lines.
   * @throws {@link RuvioError} `ERR` when cluster mode is disabled.
   *
   * @example
   * ```ts
   * await client.clusterInfo();
   * ```
   */
  clusterInfo(): R<string> {
    return this._0(T.lines, C.CLUSTER_INFO);
  }
  /**
   * Id of the shard that received the command (`CLUSTER MYID`).
   *
   * @returns The node id.
   * @throws {@link RuvioError} `ERR` when cluster mode is disabled.
   *
   * @example
   * ```ts
   * await client.clusterMyid();
   * ```
   */
  clusterMyid(): R<string> {
    return this._0(null, C.CLUSTER_MYID);
  }
  /**
   * Hash slot of a key (`CLUSTER KEYSLOT`), 0 to 16383. Also works with one shard. Locally: {@link hashSlot}.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @returns The slot number.
   *
   * @example
   * ```ts
   * await client.clusterKeyslot('user:{42}'); // same slot as 'cart:{42}'
   * ```
   */
  clusterKeyslot(key: Key): R<number> {
    return this._1(null, C.CLUSTER_KEYSLOT, key);
  }
  /**
   * Allows reads from replicas on this connection (`READONLY`). Returns OK in cluster mode; there are no replicas to read from.
   *
   * @returns `'OK'`.
   * @throws {@link RuvioError} `ERR` when cluster mode is disabled.
   *
   * @example
   * ```ts
   * await client.readonly();
   * ```
   */
  readonly(): R<'OK'> {
    return this._0(null, C.READONLY);
  }
  /**
   * Undoes {@link readonly} (`READWRITE`).
   *
   * @returns `'OK'`.
   * @throws {@link RuvioError} `ERR` when cluster mode is disabled.
   *
   * @example
   * ```ts
   * await client.readwrite();
   * ```
   */
  readwrite(): R<'OK'> {
    return this._0(null, C.READWRITE);
  }

  // ---- pub/sub (publishing; subscribing lives on Subscriber) ----------------------------

  /**
   * Sends a message to a channel (`PUBLISH`). Delivered to exact `SUBSCRIBE` clients and matching `PSUBSCRIBE` patterns on every shard, in both port modes. Not stored, not durable. A full subscriber queue disconnects that subscriber. Receive with {@link Subscriber}.
   *
   * @param channel Channel name.
   * @param message Message text.
   * @returns The number of subscribers that received it (summed over shards).
   *
   * @example
   * ```ts
   * await client.publish('news', 'hello');
   * ```
   */
  publish(channel: Key, message: Value): R<number> {
    return this._2(null, C.PUBLISH, channel, message);
  }
  /**
   * Sends a message to a sharded channel (`SPUBLISH`). Delivered only to `SSUBSCRIBE` subscribers of that channel on the shard that owns the channel's slot. Not stored. {@link ClusterClient} routes it by channel.
   *
   * @param channel Channel name.
   * @param message Message text.
   * @returns The number of subscribers that received it.
   *
   * @example
   * ```ts
   * await client.spublish('orders:eu', 'created');
   * ```
   */
  spublish(channel: Key, message: Value): R<number> {
    return this._2(null, C.SPUBLISH, channel, message);
  }

  // ---- bitmaps ---------------------------------------------------------------------------

  /**
   * Sets or clears one bit (`SETBIT`), creating and zero-padding the string as needed. The TTL is kept.
   * The bitmap is limited to 16 MiB (offset below 134,217,728) and each call rewrites the whole value.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param offset Bit position, 0 to 134,217,727.
   * @param bit `0` or `1`.
   * @param opts {@link TtlOptions}: `ttl` in seconds, applied only when this call creates the key.
   * @returns The previous value of the bit.
   * @throws {@link RuvioError} `ERR` for an offset out of range; `WRONGTYPE`.
   * @throws RangeError when `opts.ttl` is not a positive finite number (the call rejects before sending).
   *
   * @example
   * ```ts
   * await client.setbit('online', 1234, 1);
   * ```
   */
  setbit(key: Key, offset: number, bit: 0 | 1, opts?: TtlOptions): R<number> {
    return opts?.ttl === undefined ? this._3(null, C.SETBIT, key, offset, bit) : this._v(null, C.SETBIT, pushTtl([key, offset, bit], opts.ttl));
  }
  /**
   * Reads one bit (`GETBIT`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param offset Bit position, up to 2^32-1.
   * @returns `0` or `1`; `0` when the key is missing or the offset is past the end.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.getbit('online', 1234);
   * ```
   */
  getbit(key: Key, offset: number): R<number> {
    return this._2(null, C.GETBIT, key, offset);
  }
  /**
   * Counts set bits (`BITCOUNT [start end [BYTE|BIT]]`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param start First byte (or bit) of the range; negative counts from the end. Only used together with `end`.
   * @param end Last byte (or bit), inclusive.
   * @param unit `'BYTE'` (default) or `'BIT'` (see {@link BitUnit}).
   * @returns The number of bits set; `0` for a missing key.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.bitcount('online');
   * await client.bitcount('online', 0, 7, 'BYTE');
   * ```
   */
  bitcount(key: Key, start?: number, end?: number, unit?: BitUnit): R<number> {
    if (start === undefined || end === undefined) return this._1(null, C.BITCOUNT, key);
    return unit === undefined ? this._3(null, C.BITCOUNT, key, start, end) : this._4(null, C.BITCOUNT, key, start, end, kw(KW_BITUNIT, unit));
  }
  /**
   * Finds the first bit with a given value (`BITPOS`).
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param bit `0` or `1`.
   * @param start Optional first byte (or bit) to search.
   * @param end Optional last byte (or bit), inclusive; needs `start`.
   * @param unit `'BYTE'` (default) or `'BIT'` (see {@link BitUnit}); needs `end`.
   * @returns The bit position, or `-1` when not found.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.bitpos('online', 1);
   * ```
   */
  bitpos(key: Key, bit: 0 | 1, start?: number, end?: number, unit?: BitUnit): R<number> {
    const a: Arg[] = [key, bit];
    if (start !== undefined) {
      a.push(start);
      if (end !== undefined) {
        a.push(end);
        if (unit !== undefined) a.push(kw(KW_BITUNIT, unit));
      }
    }
    return this._v(null, C.BITPOS, a);
  }
  /**
   * Bitwise operation across strings, stored in `destination` (`BITOP`). Result limited to 16 MiB. Keys must share a slot.
   *
   * @param op `'AND'`, `'OR'`, `'XOR'` or `'NOT'` (see {@link BitOp}; `NOT` takes exactly one source).
   * @param destination Key that receives the result (any existing value is replaced).
   * @param keys Source keys.
   * @returns The length in bytes of the stored result.
   * @throws {@link RuvioError} `WRONGTYPE`; `CROSSSLOT` when the slots differ.
   *
   * @example
   * ```ts
   * await client.bitop('AND', '{d}both', '{d}a', '{d}b');
   * ```
   */
  bitop(op: BitOp, destination: Key, ...keys: Key[]): R<number> {
    return this._v(null, C.BITOP, pushAll([kw(KW_BITOP, op), destination], keys));
  }
  /**
   * Reads and writes integer fields inside a string (`BITFIELD key op...`) using raw operation words.
   * Operations: `GET type offset`, `SET type offset value`, `INCRBY type offset delta` and `OVERFLOW WRAP|SAT|FAIL`. Types are `i1`..`i64` (signed) and `u1`..`u63` (unsigned). An offset is a bit number, or `#n` for the nth field of that width. The field must end below 134,217,728 bits. A write rewrites the whole value.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param ops Operation words in order.
   * @returns One result per GET/SET/INCRBY: the number, or `null` when `OVERFLOW FAIL` refused it.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.bitfield('k', 'SET', 'u8', 0, 200, 'GET', 'u8', 0, 'OVERFLOW', 'SAT', 'INCRBY', 'u8', 0, 100);
   * // same words, with pre-encoded keywords (see {@link BitfieldOp}):
   * await client.bitfield('k', ...BitfieldOp.set('u8', 0, 200), ...BitfieldOp.get('u8', 0));
   * ```
   */
  bitfield(key: Key, ...ops: Value[]): R<(number | null)[]> {
    return this._kv(null, C.BITFIELD, key, ops);
  }
  /**
   * Read-only fields (`BITFIELD_RO key GET type offset ...`). Same types and limits as {@link bitfield}.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param gets One `[type, offset]` pair per field to read, e.g. `['u8', 0]`.
   * @returns The field values, in order.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.bitfieldRo('k', ['u8', 0], ['i16', '#1']);
   * ```
   */
  bitfieldRo(key: Key, ...gets: [type: BitfieldType, offset: number | string][]): R<number[]> {
    const a: Arg[] = [key];
    for (const g of gets) a.push(W.GET, g[0], g[1]);
    return this._v(null, C.BITFIELD_RO, a);
  }

  // ---- hyperloglog -----------------------------------------------------------------------

  /**
   * Adds elements to a HyperLogLog (`PFADD`). The dense 12,304-byte encoding is always used and every call rewrites the value.
   *
   * @param key Key to operate on (string, or Buffer for binary keys).
   * @param elements Elements.
   * @returns `true` when the estimate changed; `false` otherwise.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.pfadd('visitors', 'u1', 'u2');
   * ```
   */
  pfadd(key: Key, ...elements: Value[]): R<boolean> {
    return this._kv(T.bool, C.PFADD, key, elements);
  }
  /**
   * Estimated number of distinct elements (`PFCOUNT`). Several keys give the union estimate without storing it. Estimates match Redis 7.
   *
   * @param keys Keys to operate on.
   * @returns The estimate; `0` for missing keys.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.pfcount('visitors');
   * ```
   */
  pfcount(...keys: Key[]): R<number> {
    return this._v(null, C.PFCOUNT, keys);
  }
  /**
   * Merges HyperLogLogs into `destination` (`PFMERGE`). The destination takes part in the union.
   *
   * @param destination Key that receives the result (any existing value is replaced).
   * @param sources Source keys.
   * @returns `'OK'`.
   * @throws {@link RuvioError} `WRONGTYPE` when `key` holds a value of another type.
   *
   * @example
   * ```ts
   * await client.pfmerge('visitors:all', 'visitors:mon', 'visitors:tue');
   * ```
   */
  pfmerge(destination: Key, ...sources: Key[]): R<'OK'> {
    return this._kv(null, C.PFMERGE, destination, sources);
  }
}

function pushZAddFlags(a: Arg[], o?: ZAddOptions): void {
  if (o === undefined) return;
  if (o.nx) a.push(W.NX);
  else if (o.xx) a.push(W.XX);
  if (o.gt) a.push(W.GT);
  else if (o.lt) a.push(W.LT);
  if (o.ch) a.push(W.CH);
  if (o.ttl !== undefined) pushTtl(a, o.ttl);
}

function zrangeArgs(key: Key, start: Value, stop: Value, o: ZRangeOptions | undefined, scores: boolean): Arg[] {
  const a: Arg[] = [key, start, stop];
  if (o !== undefined) {
    if (o.by === 'SCORE') a.push(W.BYSCORE);
    else if (o.by === 'LEX') a.push(W.BYLEX);
    if (o.rev) a.push(W.REV);
    if (o.limit !== undefined) a.push(W.LIMIT, o.limit.offset, o.limit.count);
  }
  if (scores) a.push(W.WITHSCORES);
  return a;
}

function pushRadius(a: Arg[], o?: GeoRadiusOptions): void {
  if (o === undefined) return;
  if (o.withCoord) a.push(W.WITHCOORD);
  if (o.withDist) a.push(W.WITHDIST);
  if (o.withHash) a.push(W.WITHHASH);
  if (o.count !== undefined) {
    a.push(W.COUNT, o.count);
    if (o.any) a.push(W.ANY);
  }
  if (o.order !== undefined) a.push(kw(KW_ORDER, o.order));
  if (o.store !== undefined) a.push(W.STORE, o.store);
  if (o.storeDist !== undefined) a.push(W.STOREDIST, o.storeDist);
}
