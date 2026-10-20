// SPDX-License-Identifier: MIT
// Cluster routing: CLUSTER SLOTS table, one multiplexed connection per node, MOVED handling,
// split/merge of multi-key commands, grouped pipelines and single-slot transactions.
//
// How routing works without touching the typed commands: the cluster client encodes every
// command into a private scratch encoder (the same fast encoder the standalone client uses),
// then reads the key(s) straight out of the encoded RESP bytes. A single command is copied once
// into its node's write buffer; the connection remembers where, so a MOVED retry re-sends it
// without any extra copy.
// Against a standalone server the client detects `cluster mode is disabled` and from then on
// encodes straight into the one connection, exactly like RuvioClient.

import { Helpers, RuvioClient, type BinaryCommands, type ClientOptions } from './client.js';
import { parseSeeds, type Connection, type BatchSink, type MovedHandler } from './connection.js';
import { COMMAND_KEY_RULES, KeyRule, type CommandKeyRule } from './command-keys.js';
import { Encoder } from './encoder.js';
import { ConnectionError, RuvioError } from './errors.js';
import { Batch, PipelineSink, TransactionImpl, type Pipeline, type Transaction } from './pipeline.js';
import { Subscriber, type SubscriberOptions } from './subscriber.js';
import type { Tf } from './transforms.js';
import type { Key, ScanOptions } from './types.js';

/** Number of hash slots in a Ruvio/Redis cluster (16384). Every key maps to exactly one slot, see {@link hashSlot}. */
export const SLOT_COUNT = 16384;

// ---- hash slots -----------------------------------------------------------------------------

const CRC = new Uint16Array(256);
for (let v = 0; v < 256; v++) {
  let crc = v << 8;
  for (let bit = 0; bit < 8; bit++) crc = crc & 0x8000 ? (crc << 1) ^ 0x1021 : crc << 1;
  CRC[v] = crc & 0xffff;
}

/** Slot of the key bytes `b[s..e)`: CRC16-XMODEM of the key or of its first non-empty `{tag}`. */
function slotRange(b: Uint8Array, s: number, e: number): number {
  for (let i = s; i < e; i++) {
    if (b[i] === 123) {
      for (let j = i + 1; j < e; j++) {
        if (b[j] === 125) {
          if (j > i + 1) {
            s = i + 1;
            e = j;
          }
          break;
        }
      }
      break;
    }
  }
  let crc = 0;
  for (let i = s; i < e; i++) crc = ((crc << 8) ^ CRC[((crc >> 8) ^ b[i]!) & 0xff]!) & 0xffff;
  return crc & (SLOT_COUNT - 1);
}

/**
 * Computes the hash slot of a key: CRC16 (XMODEM) of the key modulo 16384. If the key contains
 * `{tag}` with a non-empty tag, only the tag is hashed, so keys sharing a tag share a slot
 * (needed for multi-key commands and transactions in a cluster).
 *
 * @param key The key as a string (hashed as UTF-8) or raw bytes.
 * @returns The slot, 0 to 16383.
 *
 * @example
 * ```ts
 * hashSlot('user:{42}:profile') === hashSlot('user:{42}:cart'); // true
 * ```
 */
export function hashSlot(key: string | Uint8Array): number {
  if (typeof key !== 'string') return slotRange(key, 0, key.length);
  const b = Buffer.from(key, 'utf8');
  return slotRange(b, 0, b.length);
}

// ---- reading keys out of an encoded frame ---------------------------------------------------

let A_H = new Int32Array(64); // offset of each argument's `$` header
let A_OFF = new Int32Array(64); // offset of each argument's data
let A_LEN = new Int32Array(64);
let A_N = 0; // arguments in the frame, command name included
let A_END = 0; // offset just past the frame
let KI = new Int32Array(64); // indexes of the key arguments
let LAST_SPEC: Spec = null as unknown as Spec;
let LAST_SLOT = -1;

/** Reads the RESP array at `p` (our own encoder output, always well-formed). */
function scanFrame(b: Uint8Array, p: number): void {
  let i = p + 1;
  let n = 0;
  while (b[i] !== 13) n = n * 10 + b[i++]! - 48;
  i += 2;
  if (n > A_H.length) {
    const sz = Math.max(n, A_H.length * 2);
    A_H = new Int32Array(sz);
    A_OFF = new Int32Array(sz);
    A_LEN = new Int32Array(sz);
    KI = new Int32Array(sz);
  }
  for (let k = 0; k < n; k++) {
    A_H[k] = i;
    i++; // '$'
    let len = 0;
    while (b[i] !== 13) len = len * 10 + b[i++]! - 48;
    i += 2;
    A_OFF[k] = i;
    A_LEN[k] = len;
    i += len + 2;
  }
  A_N = n;
  A_END = i;
}

const F_SPLIT = 1; // may be split per node when its keys span slots
const F_FANOUT = 2; // keyless, answered by every node and merged
const F_BLOCK = 4; // always blocks the connection
const F_BLOCKOPT = 8; // blocks when a BLOCK / WAIT word is present

/** A command's key rule from the generated table, with the client's own routing flags. */
interface Spec {
  readonly name: string;
  readonly up: Buffer;
  readonly kind: KeyRule;
  readonly a: number;
  readonly b: number;
  readonly c: number;
  readonly words: readonly string[];
  readonly flags: number;
  /** A read that may go to a replica: flagged readonly and not blocking in the command table, and not sent on a blocking connection. */
  readonly read: boolean;
  /** The one key is argument 1 (`kind` Range 1..1). */
  readonly first: boolean;
}

// Server flag bits in the command table (see scripts/gen_client_keys.py).
const TABLE_READONLY = 2;
const TABLE_BLOCKING = 16;

// Key positions come from COMMAND_KEY_RULES; these lists only say how a command is sent.
const FLAG_LISTS: [number, string][] = [
  [F_FANOUT, 'DBSIZE FLUSHALL FLUSHDB KEYS'],
  [F_BLOCK, 'WAIT BLMOVE BRPOPLPUSH BLPOP BRPOP BZPOPMIN BZPOPMAX BLMPOP'],
  [F_SPLIT, 'DEL EXISTS MGET MSET TOUCH UNLINK'],
  [F_BLOCKOPT, 'XREAD XREADGROUP FILLGET'],
];

// Sharded Pub/Sub goes to the node that owns the channel's slot: the channel stands where a
// key would. The server lists no keys for these.
const CHANNEL_RULES: [string, CommandKeyRule][] = [
  ['SPUBLISH', [KeyRule.Range, 1, 1, 1, [], 0]],
  ['SSUBSCRIBE', [KeyRule.Range, 1, -1, 1, [], 0]],
  ['SUNSUBSCRIBE', [KeyRule.Range, 1, -1, 1, [], 0]],
];

function nameHash(b: Uint8Array, o: number, len: number): number {
  let h = 2166136261;
  for (let j = 0; j < len; j++) {
    let c = b[o + j]!;
    if (c >= 97 && c <= 122) c -= 32;
    h = Math.imul(h ^ c, 16777619);
  }
  return h;
}

/** Commands the table does not know route by their first argument. */
const DEFAULT_RULE: CommandKeyRule = [KeyRule.Range, 1, 1, 1, [], 0];

const SPECS = new Map<number, Spec>();
/** @internal Command names with a split / fan-out / blocking flag, for tests. */
export const ROUTED_COMMANDS: ReadonlyMap<string, number> = (() => {
  const flags = new Map<string, number>();
  for (const [f, names] of FLAG_LISTS) for (const name of names.split(' ')) flags.set(name, (flags.get(name) ?? 0) | f);
  const rules = new Map<string, CommandKeyRule>(COMMAND_KEY_RULES);
  for (const [name, rule] of CHANNEL_RULES) rules.set(name, rule);
  for (const name of flags.keys()) if (!rules.has(name)) rules.set(name, DEFAULT_RULE);
  for (const [name, [kind, a, b, c, words, tableFlags]] of rules) {
    const up = Buffer.from(name, 'latin1');
    const h = nameHash(up, 0, up.length);
    if (SPECS.has(h)) throw new Error('ruvio: command name hash collision: ' + name);
    const routed = flags.get(name) ?? 0;
    const read = (tableFlags & TABLE_READONLY) !== 0 && (tableFlags & TABLE_BLOCKING) === 0 && (routed & (F_BLOCK | F_BLOCKOPT)) === 0;
    SPECS.set(h, { name, up, kind, a, b, c, words, flags: routed, read, first: kind === KeyRule.Range && a === 1 && b === 1 });
  }
  return flags;
})();
const DEFAULT_SPEC: Spec = { name: '', up: Buffer.alloc(0), kind: KeyRule.Range, a: 1, b: 1, c: 1, words: [], flags: 0, read: false, first: true };

/** The routing rule of the command in the frame just scanned. */
function specOfFrame(b: Uint8Array): Spec {
  const o = A_OFF[0]!;
  const len = A_LEN[0]!;
  const s = SPECS.get(nameHash(b, o, len));
  if (s === undefined || s.up.length !== len) return DEFAULT_SPEC;
  for (let j = 0; j < len; j++) {
    let c = b[o + j]!;
    if (c >= 97 && c <= 122) c -= 32;
    if (c !== s.up[j]) return DEFAULT_SPEC;
  }
  return s;
}

function uint(b: Uint8Array, idx: number): number {
  if (idx >= A_N) return -1;
  const o = A_OFF[idx]!;
  const len = A_LEN[idx]!;
  if (len === 0 || len > 9) return -1;
  let n = 0;
  for (let j = 0; j < len; j++) {
    const d = b[o + j]! - 48;
    if (d < 0 || d > 9) return -1;
    n = n * 10 + d;
  }
  return n;
}

/** Argument `idx` equals the upper-case word `w`, ignoring case (letters only). */
function wordIs(b: Uint8Array, idx: number, w: string): boolean {
  if (A_LEN[idx] !== w.length) return false;
  const o = A_OFF[idx]!;
  for (let j = 0; j < w.length; j++) if ((b[o + j]! & 0xdf) !== w.charCodeAt(j)) return false;
  return true;
}

/** Fills KI with the key argument indexes of the scanned frame; returns how many. */
function keyIndexes(s: Spec, b: Uint8Array): number {
  const n = A_N;
  let c = 0;
  switch (s.kind) {
    case KeyRule.None:
      return 0;
    case KeyRule.Range: {
      // a..=b every c; a negative b counts from the end.
      const last = s.b < 0 ? n + s.b : s.b;
      for (let i = s.a; i <= last && i < n; i += s.c) KI[c++] = i;
      return c;
    }
    case KeyRule.NumKeys: {
      // The count at a, that many keys after it, and one key at b ahead of the count.
      if (s.b > 0 && s.b < n) KI[c++] = s.b;
      const end = s.a + 1 + uint(b, s.a);
      for (let i = s.a + 1; i < end && i < n; i++) KI[c++] = i;
      return c;
    }
    case KeyRule.KeywordHalf: {
      // The first half of the arguments after the keyword.
      for (let i = 1; i < n; i++) {
        if (wordIs(b, i, s.words[0]!)) {
          const half = ((n - i - 1) / 2) | 0;
          for (let k = 0; k < half; k++) KI[c++] = i + 1 + k;
          return c;
        }
      }
      return 0;
    }
    case KeyRule.KeyAndStore: {
      // The key at a, and the argument after each keyword.
      if (s.a < n) KI[c++] = s.a;
      for (let i = s.a + 1; i + 1 < n; i++) {
        for (const w of s.words) {
          if (wordIs(b, i, w)) {
            KI[c++] = ++i;
            break;
          }
        }
      }
      return c;
    }
  }
}

/** @internal The key arguments of an encoded command, for tests: the keys as strings, in argument order. */
export function keysOfFrame(frame: Uint8Array): string[] {
  scanFrame(frame, 0);
  const spec = specOfFrame(frame);
  const n = keyIndexes(spec, frame);
  const out: string[] = [];
  for (let i = 0; i < n; i++) {
    const a = KI[i]!;
    out.push(Buffer.from(frame.buffer, frame.byteOffset + A_OFF[a]!, A_LEN[a]!).toString('utf8'));
  }
  return out;
}

// ---- errors ---------------------------------------------------------------------------------

function crossSlot(): RuvioError {
  return new RuvioError('CROSSSLOT Keys in request do not hash to the same slot');
}
function noSlot(slot: number): RuvioError {
  return new RuvioError('CLUSTERDOWN Hash slot ' + slot + ' is not served by any known node');
}

function noReplica(slot: number): RuvioError {
  return new RuvioError('NOREPLICA no replica is known for slot ' + slot + ' (readPolicy "replica")');
}

// Routing outcomes that are not a single node.
const T_SPLIT = 1;
const T_CROSS = 2;
const T_FANOUT = 3;
const T_NOSLOT = 4;
const T_NOREPLICA = 5;

/** MOVED and ASK redirects followed per command (one shared budget) before the redirect error is returned. */
const MAX_REDIRECTS = 3;
/** TRYAGAIN retries per command; the wait before retry n (from 0) is `TRYAGAIN_WAIT_MS << n`. */
const MAX_TRYAGAIN = 3;
const TRYAGAIN_WAIT_MS = 5;
/** `ASKING`, written before the command a node was asked to serve for a slot it is handing over. */
const ASKING_FRAME = Buffer.from('*1\r\n$6\r\nASKING\r\n');

/** Error codes the router handles by sending the command again. */
const isRedirect = (code: string): boolean => code === 'MOVED' || code === 'ASK' || code === 'TRYAGAIN';

// ---- nodes ----------------------------------------------------------------------------------

/**
 * One shard (node) of the cluster: its address and its own multiplexed {@link RuvioClient}.
 * Obtain nodes from {@link ClusterClient.nodes} or {@link ClusterClient.nodeFor}.
 */
export class ClusterNode {
  /** `host:port` of the node. */
  readonly addr: string;
  /** Idle dedicated connections kept for blocking commands. */
  readonly idle: RuvioClient[] = [];
  /** The connection used for ordinary commands to this node. You may use it for node-specific calls such as `clusterMyid()`. */
  readonly client: RuvioClient;
  /** @internal The replicas `CLUSTER SLOTS` lists for this node (only read while the read policy is not `'primary'`). */
  replicas: { host: string; port: number }[] = [];
  /** @internal The replica this node's reads go to until the next slot table load; null when it has none or it cannot be used. */
  reader: ClusterNode | null = null;
  /** @internal On a replica node: the node it serves reads for. */
  primary: ClusterNode | null = null;
  constructor(
    readonly host: string,
    readonly port: number,
    options: ClientOptions,
  ) {
    this.addr = host + ':' + port;
    this.client = new RuvioClient(options);
  }
  /** @internal */
  get conn(): Connection {
    return this.client.conn;
  }
}

/**
 * Options for {@link ClusterClient}: all {@link ClientOptions}, applied to the seed and to every
 * node discovered from it (so credentials, TLS and `db` must be the same on all nodes), plus `cluster`.
 *
 * @example
 * ```ts
 * const cluster = createClusterClient({ host: '127.0.0.1', port: 6379, password: 's3cret' });
 * ```
 */
export interface ClusterOptions extends ClientOptions {
  /**
   * Where reads go. `'primary'` (default): every command goes to the shard's primary. `'prefer-replica'`:
   * reads (commands the server flags readonly and not blocking, with a key) go to one replica of the key's shard,
   * picked at random when the slot table loads, and to the primary when that replica cannot be used. `'replica'`:
   * the same, but a read fails when no replica can be used. Writes, transactions, Pub/Sub, blocking and keyless
   * commands always use the primary. Replication is asynchronous, so a replica read may miss a recent write,
   * including your own. A pipeline goes to replicas only when every command in it is a read. Ignored by a
   * standalone server.
   */
  readPolicy?: ReadPolicy;
  /**
   * `'auto'` (default): ask the seed for `CLUSTER SLOTS` and fall back to plain standalone mode when
   * the server answers `cluster mode is disabled` (a single-shard server). `true`: a standalone
   * seed is an error. `false`: treated like `'auto'`.
   */
  cluster?: 'auto' | boolean;
}

/** See {@link ClusterOptions.readPolicy}. */
export type ReadPolicy = 'primary' | 'prefer-replica' | 'replica';

/** MOVED / refresh bookkeeping and the routing logic, shared by a client and its `binary` view. */
class Router {
  ready = false;
  standalone = false;
  /** Encode straight into the home connection (standalone fallback only). */
  direct = false;
  slots: (ClusterNode | undefined)[] = new Array(SLOT_COUNT);
  nodes = new Map<string, ClusterNode>();
  home: ClusterNode;
  /** Scratch encoder: every command is written here first and routed from its bytes. */
  readonly enc = new Encoder(null, 16 * 1024);
  private loading: Promise<void> | null = null;
  private refreshing: Promise<void> | null = null;
  /** Number of CLUSTER SLOTS refreshes done after the first load. */
  refreshes = 0;
  /** Number of MOVED redirects followed. */
  redirects = 0;
  /** Options of every node; the seeds are not part of them (a node is one address). */
  readonly base: ClientOptions;
  readonly strict: boolean;
  readonly policy: ReadPolicy;
  /** The read policy is not `'primary'`: the only thing the routing of a command checks for replicas. */
  readonly reads: boolean;
  /** The nodes reads go to, by address; empty with the `'primary'` policy. */
  replicaNodes = new Map<string, ClusterNode>();
  /** The configured seed addresses, asked for the slot table when no known node answers. */
  private readonly seeds: { host: string; port: number }[] = [];

  constructor(options: ClusterOptions) {
    // A cluster client never follows a primary: it keeps its own MOVED/ASK rules.
    const { cluster, seeds, readPolicy = 'primary', sentinelMaster: _coordinator, ...rest } = options;
    rest.followPrimary = false;
    if (readPolicy !== 'primary' && readPolicy !== 'prefer-replica' && readPolicy !== 'replica') {
      throw new TypeError('ruvio: readPolicy must be "primary", "prefer-replica" or "replica"');
    }
    this.policy = readPolicy;
    this.reads = readPolicy !== 'primary';
    this.strict = cluster === true;
    this.base = rest;
    let host = rest.host ?? '127.0.0.1';
    let port = rest.port ?? 6379;
    if (seeds !== undefined && seeds.length > 0) {
      if (rest.path !== undefined) throw new TypeError('ruvio: seeds cannot be combined with path');
      this.seeds = parseSeeds(seeds);
      host = rest.host = this.seeds[0]!.host;
      port = rest.port = this.seeds[0]!.port;
    }
    this.home = this.track(new ClusterNode(host, port, rest));
    this.nodes.set(this.home.addr, this.home);
  }

  // ---- encoder hand-off ----

  /** The frame just encoded into the scratch encoder, copied out (slow paths only). */
  take(): Buffer {
    const e = this.enc;
    const f = Buffer.from(e.buf.subarray(e.start, e.pos));
    this.reset();
    return f;
  }

  reset(): void {
    const e = this.enc;
    e.pos = e.start = e.frameStart = 0;
    e.partial = false;
  }

  /**
   * Sends the frame just encoded into the scratch encoder. A frame with one owner node and no
   * blocking is copied once into that node's write buffer; every other shape takes the general path.
   */
  send(tf: Tf | null, fl: number): Promise<any> {
    const e = this.enc;
    const b = e.buf;
    const r = this.target(b, e.start, this.reads);
    if (typeof r !== 'number' && ((LAST_SPEC.flags & (F_BLOCK | F_BLOCKOPT)) === 0 || !this.blocking(b))) {
      const conn = r.conn;
      conn.writeRaw(b, e.start, e.pos);
      e.pos = e.start = e.frameStart = 0;
      return conn.trackKept(tf, fl);
    }
    return this.dispatch(this.take(), tf, fl);
  }

  // ---- topology ----

  whenReady(): Promise<void> {
    if (this.ready) return Promise.resolve();
    return (this.loading ??= this.load(true).then(
      () => {
        this.loading = null;
      },
      (e) => {
        this.loading = null;
        throw e;
      },
    ));
  }

  /** Re-reads CLUSTER SLOTS (concurrent callers share one request). */
  refresh(): Promise<void> {
    if (this.standalone) return Promise.resolve();
    return (this.refreshing ??= this.load(false).then(
      () => {
        this.refreshing = null;
        this.refreshes++;
      },
      (e) => {
        this.refreshing = null;
        throw e;
      },
    ));
  }

  /**
   * Reads CLUSTER SLOTS. Nodes with an open socket are asked first, then the other known nodes
   * (each one connect attempt), and when none of them answers every seed that is not a known
   * node. A seed that cannot be opened is not kept in the node list.
   */
  private async load(first: boolean): Promise<void> {
    const queue: (ClusterNode | { host: string; port: number })[] = [];
    for (const n of this.nodes.values()) if (n.conn.isConnected) queue.push(n);
    if (!queue.includes(this.home)) queue.push(this.home);
    for (const n of this.nodes.values()) if (!queue.includes(n)) queue.push(n);
    for (const x of this.seeds) if (!this.nodes.has(x.host + ':' + x.port)) queue.push(x);
    const spare = new Map<string, ClusterNode>();
    let last: unknown = null;
    let reply: unknown[] | null = null;
    let answered: ClusterNode | null = null;
    for (const item of queue) {
      let n: ClusterNode;
      if (item instanceof ClusterNode) n = item;
      else {
        n = this.makeNode(item.host, item.port);
        spare.set(n.addr, n);
      }
      try {
        reply = (await n.client.clusterSlots()) as unknown[];
        answered = n;
        break;
      } catch (e) {
        if (e instanceof RuvioError && first && !this.strict) {
          // "cluster mode is disabled": plain standalone server.
          this.standaloneAt(n);
          for (const x of spare.values()) if (x !== n) this.drop(x);
          return;
        }
        last = e;
        if (spare.get(n.addr) === n) {
          spare.delete(n.addr);
          this.drop(n);
        }
        if (e instanceof RuvioError) break;
      }
    }
    if (reply === null || answered === null) {
      for (const x of spare.values()) this.drop(x);
      throw last ?? new ConnectionError('ruvio: no cluster topology source');
    }
    const seedHost = answered.host;
    const fresh = new Map<string, ClusterNode>();
    const slots: (ClusterNode | undefined)[] = new Array(SLOT_COUNT);
    for (const range of reply as unknown[][]) {
      const from = range[0] as number;
      const to = range[1] as number;
      const m = range[2] as unknown[];
      const host = typeof m[0] === 'string' && m[0] !== '' ? (m[0] as string) : seedHost;
      const port = Number(m[1]);
      const addr = host + ':' + port;
      let node = fresh.get(addr) ?? this.nodes.get(addr) ?? spare.get(addr);
      if (node === undefined) node = this.makeNode(host, port);
      if (this.reads) {
        if (!fresh.has(addr)) node.replicas = [];
        for (let k = 3; k < range.length; k++) this.addReplica(node, range[k], seedHost);
      }
      fresh.set(addr, node);
      for (let s = from; s <= to; s++) slots[s] = node;
    }
    if (fresh.size === 0) {
      for (const x of spare.values()) this.drop(x);
      throw new RuvioError('CLUSTERDOWN CLUSTER SLOTS returned no ranges');
    }
    const old = this.nodes;
    this.slots = slots;
    this.nodes = fresh;
    if (!fresh.has(this.home.addr)) this.home = fresh.values().next().value!;
    this.ready = true;
    for (const [addr, n] of old) if (!fresh.has(addr)) this.drop(n);
    for (const [addr, n] of spare) if (!fresh.has(addr)) this.drop(n);
    if (this.reads) this.pickReplicas();
  }

  /** Records a replica entry of a `CLUSTER SLOTS` range for `node`; an entry that cannot be read is skipped. */
  private addReplica(node: ClusterNode, entry: unknown, seedHost: string): void {
    if (!Array.isArray(entry)) return;
    const port = Number(entry[1]);
    if (!Number.isInteger(port) || port < 0 || port > 65535) return;
    const host = typeof entry[0] === 'string' ? (entry[0] === '' ? seedHost : entry[0]) : null;
    if (host === null) return;
    if (!node.replicas.some((r) => r.host === host && r.port === port)) node.replicas.push({ host, port });
  }

  /**
   * Gives each node one of its replicas to read from, picked with one random number per load so many
   * clients spread over a shard's replicas, and kept until the next load. Replica nodes of the last load
   * that are not picked again are closed.
   */
  private pickReplicas(): void {
    const pick = Math.floor(Math.random() * 0x100000000);
    const old = this.replicaNodes;
    const kept = new Map<string, ClusterNode>();
    for (const n of this.nodes.values()) {
      n.reader = null;
      if (n.replicas.length === 0) continue;
      const { host, port } = n.replicas[pick % n.replicas.length]!;
      const addr = host + ':' + port;
      let r = kept.get(addr) ?? old.get(addr);
      if (r === undefined) r = this.makeReplica(host, port);
      r.primary = n;
      kept.set(addr, r);
      n.reader = r;
    }
    for (const [addr, r] of old) if (!kept.has(addr)) this.drop(r);
    this.replicaNodes = kept;
  }

  private makeReplica(host: string, port: number): ClusterNode {
    const o: ClientOptions = { ...this.base, host, port };
    delete o.path;
    const n = new ClusterNode(host, port, o);
    n.conn.replica = true;
    const frame = (buf: Buffer, off: number, len: number) => Buffer.from(buf.subarray(off, off + len));
    n.conn.keepFrames(
      (e, buf, off, len, tf, fl, res, rej) => {
        const f = frame(buf, off, len);
        (isRedirect(e.code) ? this.moved(e, f, tf, fl) : this.replicaFailed(n, e, f, tf, fl)).then(res, rej);
      },
      (e, buf, off, len, tf, fl, res, rej) => {
        this.replicaFailed(n, e, frame(buf, off, len), tf, fl).then(res, rej);
      },
    );
    return n;
  }

  /**
   * A read sent to replica `n` could not be answered: the replica could not be opened, or it answered
   * `-LOADING` (still copying its primary). It is skipped until the next load, and the read goes to the
   * node the routing now picks (the primary, under `'prefer-replica'`); under `'replica'` the error is returned.
   */
  replicaFailed(n: ClusterNode, e: unknown, frame: Buffer, tf: Tf | null, fl: number): Promise<any> {
    const p = n.primary;
    if (p !== null && p.reader === n) p.reader = null;
    if (this.policy === 'replica') return Promise.reject(e);
    return this.dispatch(frame, tf, fl);
  }

  /** The replica a read of `node`'s slots goes to, or `node` itself (a T_NOREPLICA under `'replica'`) when it has none to use. */
  private readerOf(node: ClusterNode): ClusterNode | number {
    const r = node.reader;
    if (r !== null) return r;
    return this.policy === 'replica' ? T_NOREPLICA : node;
  }

  /** The answering address is a plain server: `n` becomes the one connection, with the other seeds as fallbacks. */
  private standaloneAt(n: ClusterNode): void {
    this.standalone = true;
    this.ready = true;
    if (this.seeds.length < 2) return;
    const start = this.seeds.findIndex((x) => x.host === n.host && x.port === n.port);
    const rotated: string[] = [];
    for (let k = 0; k < this.seeds.length; k++) {
      const x = this.seeds[(Math.max(start, 0) + k) % this.seeds.length]!;
      rotated.push((x.host.includes(':') ? '[' + x.host + ']' : x.host) + ':' + x.port);
    }
    const home = this.track(new ClusterNode(n.host, n.port, { ...this.base, host: n.host, port: n.port, seeds: rotated }));
    const old = this.nodes;
    this.nodes = new Map([[home.addr, home]]);
    this.home = home;
    for (const x of old.values()) this.drop(x);
    if (!old.has(n.addr)) this.drop(n);
  }

  private makeNode(host: string, port: number): ClusterNode {
    const o: ClientOptions = { ...this.base, host, port };
    delete o.path;
    return this.track(new ClusterNode(host, port, o));
  }

  /** Lets the node's connection hand `-MOVED` replies of single commands back to the router. */
  private track(n: ClusterNode): ClusterNode {
    n.conn.keepFrames(this.onMoved, (e, buf, off, len, tf, fl, res, rej) => {
      this.lost(n, e, Buffer.from(buf.subarray(off, off + len)), tf, fl).then(res, rej);
    });
    return n;
  }

  // The frame is copied: the retry may wait for a slot-map refresh, and a reconnect of the node
  // meanwhile starts writing its write slab from the beginning again.
  private readonly onMoved: MovedHandler = (e, buf, off, len, tf, fl, res, rej) => {
    this.moved(e, Buffer.from(buf.subarray(off, off + len)), tf, fl).then(res, rej);
  };

  private drop(n: ClusterNode): void {
    n.client.quit().catch(() => undefined);
    for (const c of n.idle) c.quit().catch(() => undefined);
    n.idle.length = 0;
  }

  nodeAt(addr: string): ClusterNode {
    let n = this.nodes.get(addr);
    if (n === undefined) {
      const i = addr.lastIndexOf(':');
      n = this.makeNode(addr.slice(0, i), Number(addr.slice(i + 1)));
      this.nodes.set(addr, n);
    }
    return n;
  }

  distinctNodes(): ClusterNode[] {
    return [...this.nodes.values()];
  }

  // ---- routing ----

  /**
   * Single owner node, or a T_* code. Leaves the scan in A_* and the rule in LAST_SPEC. With `rd`, a read
   * (see `Spec.read`) with a key goes to the replica of its slot's node.
   */
  target(b: Uint8Array, p: number, rd = false): ClusterNode | number {
    scanFrame(b, p);
    const spec = (LAST_SPEC = specOfFrame(b));
    LAST_SLOT = -1;
    if (spec.first) {
      // Direct path: the first argument is the key.
      if (A_N < 2) return this.home;
      const o = A_OFF[1]!;
      const slot = (LAST_SLOT = slotRange(b, o, o + A_LEN[1]!));
      const node = this.slots[slot];
      if (node === undefined) return T_NOSLOT;
      return rd && spec.read ? this.readerOf(node) : node;
    }
    if (spec.kind === KeyRule.None) return (spec.flags & F_FANOUT) !== 0 ? T_FANOUT : this.home;
    const n = keyIndexes(spec, b);
    if (n === 0) return this.home;
    let slot = -1;
    for (let i = 0; i < n; i++) {
      const a = KI[i]!;
      const s = slotRange(b, A_OFF[a]!, A_OFF[a]! + A_LEN[a]!);
      if (slot < 0) slot = s;
      else if (s !== slot) return (spec.flags & F_SPLIT) !== 0 ? T_SPLIT : T_CROSS;
    }
    LAST_SLOT = slot;
    const node = this.slots[slot];
    if (node === undefined) return T_NOSLOT;
    return rd && spec.read ? this.readerOf(node) : node;
  }

  private blocking(b: Uint8Array): boolean {
    const f = LAST_SPEC.flags;
    if ((f & F_BLOCK) !== 0) return true;
    if ((f & F_BLOCKOPT) === 0) return false;
    for (let i = 1; i < A_N; i++) if (wordIs(b, i, 'BLOCK') || wordIs(b, i, 'WAIT')) return true;
    return false;
  }

  dispatch(frame: Buffer, tf: Tf | null, fl: number, rd = this.reads): Promise<any> {
    const r = this.target(frame, 0, rd);
    if (typeof r !== 'number') return this.sendTo(r, frame, tf, fl, this.blocking(frame));
    switch (r) {
      case T_SPLIT:
        return this.split(frame, tf, fl, rd);
      case T_FANOUT:
        return this.fanout(frame, tf, fl);
      case T_CROSS:
        return Promise.reject(crossSlot());
      case T_NOREPLICA:
        return Promise.reject(noReplica(LAST_SLOT));
      default:
        return Promise.reject(noSlot(LAST_SLOT));
    }
  }

  /** Sends one frame to a node; `-MOVED` replies are followed up to `MAX_REDIRECTS` times. */
  sendTo(node: ClusterNode, frame: Buffer, tf: Tf | null, fl: number, blocking: boolean): Promise<any> {
    if (blocking) return this.sendBlocking(node, frame, tf, fl);
    const conn = node.conn;
    conn.writeFrames(frame);
    return new Promise((resolve, reject) => {
      conn.trackCb(
        resolve,
        (e) => {
          if (e instanceof RuvioError && isRedirect(e.code)) this.moved(e, frame, tf, fl).then(resolve, reject);
          else if (node.primary !== null && replicaFault(e)) this.replicaFailed(node, e, frame, tf, fl).then(resolve, reject);
          else if (e instanceof ConnectionError && e.unreachable) this.lost(node, e, frame, tf, fl).then(resolve, reject);
          else reject(e);
        },
        tf,
        fl,
      );
    });
  }

  private sendBlocking(node: ClusterNode, frame: Buffer, tf: Tf | null, fl: number): Promise<any> {
    const c = node.idle.pop() ?? node.client.duplicate();
    const conn = c.conn;
    conn.writeFrames(frame);
    return conn.track(tf, fl).then(
      (v) => {
        this.release(node, c);
        return v;
      },
      (e) => {
        if (e instanceof ConnectionError) c.disconnect();
        else this.release(node, c);
        if (e instanceof ConnectionError && e.unreachable) return this.lost(node, e, frame, tf, fl);
        return this.onError(e, frame, tf, fl);
      },
    );
  }

  private release(node: ClusterNode, c: RuvioClient): void {
    if (node.idle.length < 8 && !c.conn.isClosed) node.idle.push(c);
    else c.disconnect();
  }

  onError(e: unknown, frame: Buffer, tf: Tf | null, fl: number): Promise<any> {
    if (e instanceof RuvioError && isRedirect(e.code)) return this.moved(e, frame, tf, fl);
    return Promise.reject(e);
  }

  /**
   * `node` could not be opened. The slot table is reloaded (from the other nodes, then the seeds)
   * and the command goes once to the owner the new table names; a command without a key goes to
   * the first node of the new table that can be opened. When nothing better is found the
   * original error is returned.
   */
  async lost(node: ClusterNode, err: ConnectionError, frame: Buffer, tf: Tf | null, fl: number): Promise<any> {
    if (this.standalone || !this.ready) throw err;
    try {
      await this.refresh();
    } catch {
      throw err;
    }
    const r = this.target(frame, 0);
    if (typeof r === 'number') throw err;
    let owner: ClusterNode | null = null;
    if (LAST_SLOT >= 0) {
      if (r !== node || node.conn.isConnected) owner = r;
    } else {
      for (const n of [r, ...this.nodes.values()]) {
        if (n === node && !n.conn.isConnected) continue;
        try {
          await n.conn.connect();
        } catch {
          continue;
        }
        this.home = owner = n;
        break;
      }
    }
    if (owner === null) throw err;
    const conn = owner.conn;
    conn.writeFrames(frame);
    try {
      return await conn.track(tf, fl);
    } catch (e2) {
      if (e2 instanceof RuvioError && isRedirect(e2.code)) return this.moved(e2, frame, tf, fl, 2, 0, true);
      throw e2;
    }
  }

  /**
   * Follows `MOVED`, `ASK` and `TRYAGAIN` for one command, re-sending from the caller's frame.
   * `MOVED slot host:port` points that slot at the named node and sends the command there; the
   * whole table is reloaded only for an unknown node or a second MOVED for the same command.
   * `ASK slot host:port` sends `ASKING` and the command to the named node in one write and leaves
   * the table alone. MOVED and ASK share `MAX_REDIRECTS`; the redirect after that is returned as
   * the error. `TRYAGAIN` waits 5, 10, 20 ms and routes the command again by the current table, at
   * most `MAX_TRYAGAIN` times; the TRYAGAIN after that is returned.
   */
  async moved(e: RuvioError, frame: Buffer, tf: Tf | null, fl: number, hop = 1, tries = 0, movedBefore = false): Promise<any> {
    for (;;) {
      let conn: Connection;
      let asking = false;
      if (e.code === 'TRYAGAIN') {
        if (tries >= MAX_TRYAGAIN) throw e;
        await new Promise((r) => setTimeout(r, TRYAGAIN_WAIT_MS << tries));
        tries++;
        const t = this.target(frame, 0);
        if (typeof t === 'number') throw e;
        conn = t.conn;
      } else {
        const parts = e.message.split(' ');
        const slot = Number(parts[1]);
        const addr = parts[2];
        if (hop > MAX_REDIRECTS || addr === undefined || !(slot >= 0 && slot < SLOT_COUNT)) throw e;
        hop++;
        this.redirects++;
        let node = this.nodes.get(addr);
        if (e.code === 'ASK') {
          node ??= this.nodeAt(addr);
          asking = true;
        } else {
          if (node === undefined || movedBefore) {
            try {
              await this.refresh();
            } catch {
              /* keep the redirect target below */
            }
            node = this.nodeAt(addr);
          }
          movedBefore = true;
          this.slots[slot] = node;
        }
        conn = node.conn;
      }
      try {
        if (!asking) {
          conn.writeFrames(frame);
          return await conn.track(tf, fl);
        }
        conn.writeFrames(ASKING_FRAME);
        const pa = conn.track(null, 0);
        conn.writeFrames(frame);
        const pc = conn.track(tf, fl);
        try {
          await pa;
        } catch (x) {
          pc.catch(() => undefined);
          throw x;
        }
        return await pc;
      } catch (e2) {
        if (!(e2 instanceof RuvioError && isRedirect(e2.code))) throw e2;
        e = e2;
      }
    }
  }

  /** MGET / MSET / DEL / UNLINK / EXISTS / TOUCH across slots: one sub-command per node, merged in caller order. */
  private split(b: Buffer, tf: Tf | null, fl: number, rd: boolean): Promise<any> {
    const spec = LAST_SPEC;
    rd &&= spec.read;
    const step = spec.c; // MSET: key value pairs
    // The server demands one slot per command (CROSSSLOT even inside one shard): group by slot.
    const groups = new Map<number, number[]>();
    for (let i = 1; i < A_N; i += step) {
      const s = slotRange(b, A_OFF[i]!, A_OFF[i]! + A_LEN[i]!);
      if (this.slots[s] === undefined) return Promise.reject(noSlot(s));
      let g = groups.get(s);
      if (g === undefined) groups.set(s, (g = []));
      g.push(i);
    }
    const nameEnd = A_OFF[0]! + A_LEN[0]! + 2;
    const nameBulk = b.subarray(A_H[0], nameEnd);
    const jobs: [ClusterNode, Buffer][] = [];
    const order: number[][] = [];
    for (const [slot, idx] of groups) {
      const owner = this.slots[slot]!;
      const node = rd ? this.readerOf(owner) : owner;
      if (typeof node === 'number') return Promise.reject(noReplica(slot));
      const parts: Buffer[] = [Buffer.from('*' + (1 + idx.length * step) + '\r\n', 'latin1'), nameBulk];
      for (const i of idx) {
        for (let k = 0; k < step; k++) parts.push(b.subarray(A_H[i + k], A_OFF[i + k]! + A_LEN[i + k]! + 2));
      }
      jobs.push([node, Buffer.concat(parts)]);
      order.push(idx);
    }
    const total = A_N - 1;
    const name = spec.name;
    return Promise.all(jobs.map(([n, f]) => this.sendTo(n, f, null, fl, false))).then((rs) => {
      let out: unknown;
      if (name === 'MGET') {
        const arr = new Array(total);
        for (let g = 0; g < rs.length; g++) {
          const part = rs[g] as unknown[];
          const idx = order[g]!;
          for (let k = 0; k < idx.length; k++) arr[idx[k]! - 1] = part[k];
        }
        out = arr;
      } else if (name === 'MSET') {
        out = rs[0];
      } else {
        let sum = 0;
        for (const r of rs) sum += r as number;
        out = sum;
      }
      return tf === null ? out : tf(out);
    });
  }

  /** FLUSHDB / FLUSHALL / DBSIZE / KEYS: ask every node, merge. */
  private fanout(b: Buffer, tf: Tf | null, fl: number): Promise<any> {
    const name = LAST_SPEC.name;
    const nodes = this.distinctNodes();
    return Promise.all(nodes.map((n) => this.sendTo(n, b, null, fl, false))).then((rs) => {
      let out: unknown;
      if (name === 'DBSIZE') {
        let sum = 0;
        for (const r of rs) sum += r as number;
        out = sum;
      } else if (name === 'KEYS') {
        out = ([] as unknown[]).concat(...(rs as unknown[][]));
      } else out = rs[0];
      return tf === null ? out : tf(out);
    });
  }

  // ---- batches ----

  /**
   * Pipeline: one pass over the frames finds each one's owner node. When they all share one node
   * the batch goes out unchanged; otherwise runs of frames for the same node are copied into that
   * node's write buffer in call order, every node is written before any is read, and each reply
   * lands at its command's index.
   */
  runBatch(bytes: Buffer, n: number, fls: number[], out: PipelineSink): void {
    if (this.standalone) {
      const conn = this.home.conn;
      conn.writeFrames(bytes);
      conn.trackBatch(out, fls, n);
      return;
    }
    if (n >= B_NODE.length) {
      B_NODE = new Array(n * 2).fill(null);
      B_OFF = new Int32Array(n * 2 + 1);
    }
    const nodes = B_NODE;
    const offs = B_OFF;
    // Replicas serve a pipeline only when every command in it is a read, so it never reads its own
    // writes from a replica that has not received them.
    let rd = this.reads;
    if (rd) {
      for (let i = 0, q = 0; i < n; i++) {
        scanFrame(bytes, q);
        if (!specOfFrame(bytes).read) {
          rd = false;
          break;
        }
        q = A_END;
      }
    }
    let p = 0;
    let first: ClusterNode | null = null;
    let same = true;
    for (let i = 0; i < n; i++) {
      offs[i] = p;
      const r = this.target(bytes, p, rd);
      p = A_END;
      if (typeof r === 'number') {
        nodes[i] = null;
        same = false;
      } else {
        nodes[i] = r;
        if (i === 0) first = r;
        else if (r !== first) same = false;
      }
    }
    offs[n] = p;
    const sink = new ClusterSink(this, bytes, n, fls, out, rd ? nodes.slice(0, n) : null);
    if (same) {
      const conn = first!.conn;
      conn.writeFrames(bytes);
      conn.trackBatch(sink, fls, n);
      return;
    }
    for (let i = 0; i < n; ) {
      const node = nodes[i];
      if (node === null || node === undefined) {
        const frame = bytes.subarray(offs[i], offs[i + 1]);
        const r = this.target(frame, 0, rd);
        if (typeof r === 'number' && r === T_CROSS) out.set(i, crossSlot());
        else if (typeof r === 'number' && r === T_NOSLOT) out.set(i, noSlot(-1));
        else {
          const k = i;
          this.dispatch(frame, null, fls[i]!, rd).then(
            (v) => out.set(k, v),
            (e) => out.set(k, e),
          );
        }
        i++;
        continue;
      }
      let j = i + 1;
      while (j < n && nodes[j] === node) j++;
      const conn = node.conn;
      conn.writeRaw(bytes, offs[i]!, offs[j]!);
      conn.trackRange(sink, i, j, fls);
      i = j;
    }
  }

  /** The unserved slot that made `batchSlot` return -3. */
  noSlotSlot = -1;

  /**
   * The slot shared by every keyed command in `body`, -1 when there is none, -2 when they differ,
   * -3 when the slot has no owner (see `noSlotSlot`).
   */
  batchSlot(body: Buffer, n: number): number {
    let slot = -1;
    let p = 0;
    for (let i = 0; i < n; i++) {
      const r = this.target(body, p);
      p = A_END;
      if (typeof r === 'number') {
        if (r === T_FANOUT) continue;
        if (r === T_NOSLOT) {
          this.noSlotSlot = LAST_SLOT;
          return -3;
        }
        return -2;
      }
      if (LAST_SLOT < 0) continue;
      if (slot < 0) slot = LAST_SLOT;
      else if (slot !== LAST_SLOT) return -2;
    }
    return slot;
  }
}

// Per-call scratch for runBatch: owner node and start offset of each frame. Used within one
// synchronous call only.
let B_NODE: (ClusterNode | null)[] = new Array(256).fill(null);
let B_OFF = new Int32Array(257);

/** A reply or error that makes a read sent to a replica go elsewhere: the replica could not be opened, or answered `-LOADING`. */
function replicaFault(v: unknown): boolean {
  return (v instanceof ConnectionError && v.unreachable) || (v instanceof RuvioError && (v.code === 'LOADING' || v.code === 'MASTERDOWN'));
}

/** Receives a pipeline's replies at the caller's indexes and follows MOVED per command. */
class ClusterSink implements BatchSink {
  private offs: Int32Array | null = null;
  constructor(
    private readonly r: Router,
    private readonly bytes: Buffer,
    private readonly n: number,
    private readonly fls: number[],
    private readonly out: PipelineSink,
    /** The node each command was sent to; only for a pipeline routed to replicas. */
    private readonly via: (ClusterNode | null)[] | null,
  ) {}
  set(i: number, v: unknown): void {
    if (this.via !== null) {
      const n = this.via[i];
      if (n !== null && n !== undefined && n.primary !== null && replicaFault(v)) {
        this.r.replicaFailed(n, v, this.frame(i), null, this.fls[i]!).then(
          (x) => this.out.set(i, x),
          (e) => this.out.set(i, e),
        );
        return;
      }
    }
    if (v instanceof RuvioError && isRedirect(v.code)) {
      this.r.moved(v, this.frame(i), null, this.fls[i]!).then(
        (x) => this.out.set(i, x),
        (e) => this.out.set(i, e),
      );
      return;
    }
    this.out.set(i, v);
  }
  /** Frame `i` of the batch bytes (rare path: offsets are rebuilt here). */
  private frame(i: number): Buffer {
    let o = this.offs;
    if (o === null) {
      o = this.offs = new Int32Array(this.n + 1);
      let p = 0;
      for (let k = 0; k < this.n; k++) {
        o[k] = p;
        scanFrame(this.bytes, p);
        p = A_END;
      }
      o[this.n] = p;
    }
    return this.bytes.subarray(o[i], o[i + 1]);
  }
}

// ---- batches (client side) ------------------------------------------------------------------

class ClusterPipelineImpl extends Batch {
  constructor(private readonly r: Router) {
    super(null as unknown as Connection);
  }
  exec(): Promise<unknown[]> {
    const n = this.tfs.length;
    if (n === 0) return Promise.resolve([]);
    const bytes = this.benc.take();
    const tfs = this.tfs;
    const fls = this.fls;
    this.tfs = [];
    this.fls = [];
    const r = this.r;
    return new Promise<unknown[]>((resolve, reject) => {
      const out = new PipelineSink(n, tfs, resolve, reject);
      if (r.ready) r.runBatch(bytes, n, fls, out);
      else r.whenReady().then(() => r.runBatch(bytes, n, fls, out), reject);
    });
  }
  async execStrict(): Promise<unknown[]> {
    const out = await this.exec();
    for (const v of out) if (v instanceof Error) throw v;
    return out;
  }
}

class ClusterTransactionImpl extends TransactionImpl {
  constructor(private readonly r: Router) {
    super(null as unknown as Connection);
  }
  override exec(): Promise<unknown[] | null> {
    const r = this.r;
    if (!r.ready) return r.whenReady().then(() => this.exec());
    if (r.standalone) {
      this.conn = r.home.conn;
      return super.exec();
    }
    const n = this.tfs.length;
    const body = this.benc.buf.subarray(this.benc.start, this.benc.pos);
    const slot = r.batchSlot(body, n);
    if (slot === -2) {
      this.discard();
      return Promise.reject(crossSlot());
    }
    if (slot === -3) {
      this.discard();
      return Promise.reject(noSlot(r.noSlotSlot));
    }
    const node = slot < 0 ? r.home : r.slots[slot];
    if (node === undefined) {
      this.discard();
      return Promise.reject(noSlot(slot));
    }
    this.conn = node.conn;
    return super.exec();
  }
}

// ---- the client -----------------------------------------------------------------------------

/**
 * A cluster-aware client. It has the same typed commands, {@link Helpers}, `pipeline()` and
 * `multi()` as {@link RuvioClient}, but every command goes straight to the node (shard) that owns
 * its key, so there is no forwarding hop. Connect it to any one node (the seed): it reads
 * `CLUSTER SLOTS` and opens one multiplexed connection per node. Against a single-shard or
 * standalone server it falls back to a plain single connection by itself.
 *
 * Routing behaviour (see `docs/CLIENT_BEHAVIOR.md`):
 * - `MGET`, `MSET`, `DEL`, `EXISTS`, `UNLINK` and `TOUCH` with keys in several slots are split per slot and
 *   the replies merged (`MGET` keeps key order, counts are summed, `MSET` answers `OK`). This is **not atomic**.
 * - `DBSIZE`, `KEYS`, `FLUSHDB` and `FLUSHALL` are sent to every node and merged (counts summed, key lists joined).
 *   A plain {@link RuvioClient} would reach only the shard it is connected to.
 * - Any other command whose keys span slots (`RENAME`, `COPY`, `LMOVE`, `SINTERSTORE`, `BITOP`, `EVAL`, ...)
 *   rejects with {@link RuvioError} code `CROSSSLOT` before anything is sent. Use `{tag}` in key names to colocate keys.
 *   Inside a pipeline the error sits in that command's slot and the rest still runs.
 * - A transaction (`multi()`) must stay in one slot.
 * - On `-MOVED` the slot is re-pointed and the command re-sent; on `-ASK` the command is sent to the named node after `ASKING` and the
 *   slot is left alone. Both share one budget of 3 redirects; the 4th is returned as an error. `-TRYAGAIN` is retried up to 3 times
 *   after 5, 10 and 20 ms, then returned. Ruvio normally forwards a misrouted key instead of answering `MOVED`, so redirects are rare.
 * - Blocking commands run on a dedicated connection to the owning node so they do not stall the shared one.
 * - `SCAN` and the `CLIENT *`/`SLOWLOG`/`INFO` family reach one node only; use {@link scanIterator} to walk every node.
 *
 * @example
 * ```ts
 * const cluster = createClusterClient('redis://127.0.0.1:6379');
 * await cluster.set('user:{42}:name', 'Ada');
 * await cluster.mget('a', 'b', 'c');          // split across nodes if needed
 * console.log(cluster.slotOf('user:{42}:name'), cluster.nodes.length);
 * await cluster.quit();
 * ```
 */
export class ClusterClient extends Helpers {
  /** @internal */
  readonly r: Router;
  /** The options this client was created with. */
  readonly options: ClusterOptions;
  private _binary: BinaryCommands | null = null;

  /**
   * Creates a cluster client. Nothing connects until the first command or {@link connect}.
   *
   * @param options {@link ClusterOptions}, or a seed URL such as `redis://user:pass@host:7000`
   * (`rediss://` or `ruvios://` enable TLS). The seed may be any one node.
   */
  constructor(options: ClusterOptions | string = {}) {
    super();
    const o = typeof options === 'string' ? parseSeed(options) : { ...options };
    this.options = o;
    this.r = new Router(o);
  }

  protected _enc(): Encoder {
    const r = this.r;
    if (r.standalone) {
      r.direct = true;
      return r.home.conn.encoder();
    }
    r.direct = false;
    return r.enc;
  }
  protected _done(tf: Tf | null, fl: number): Promise<any> {
    const r = this.r;
    if (r.direct) return r.home.conn.track(tf, fl);
    if (!r.ready) {
      const frame = r.take();
      return r.whenReady().then(() => this.routeLate(frame, tf, fl));
    }
    return r.send(tf, fl);
  }
  private routeLate(frame: Buffer, tf: Tf | null, fl: number): Promise<any> {
    const r = this.r;
    if (r.standalone) {
      r.home.conn.writeFrames(frame);
      return r.home.conn.track(tf, fl);
    }
    return r.dispatch(frame, tf, fl);
  }
  protected _fail(e: unknown, enc: Encoder): Promise<never> {
    const r = this.r;
    if (r.direct) {
      if (!enc.abortFrame()) r.home.conn.destroySocket(e as Error);
    } else {
      enc.abortFrame();
      r.reset();
    }
    return Promise.reject(e);
  }

  /**
   * Reads the slot table from the seed and connects to it. Optional: the first command does this.
   * Call it before {@link nodeFor} or {@link nodes} so the topology is known.
   *
   * @throws {@link ConnectionError} when the seed is unreachable; {@link RuvioError} when `cluster: true` and the seed is standalone.
   */
  async connect(): Promise<void> {
    await this.r.whenReady();
    await this.r.home.client.connect();
  }
  /** Waits for in-flight replies on every node, then closes all node connections. The client cannot be used afterwards. */
  async quit(): Promise<void> {
    await Promise.all(this.allClients().map((c) => c.quit().catch(() => undefined)));
  }
  /** Closes every node connection immediately; calls in flight reject with {@link ConnectionError}. */
  disconnect(): void {
    for (const c of this.allClients()) c.disconnect();
  }
  async [Symbol.asyncDispose](): Promise<void> {
    await this.quit();
  }
  private allClients(): RuvioClient[] {
    const out: RuvioClient[] = [];
    for (const n of this.r.nodes.values()) {
      out.push(n.client);
      for (const c of n.idle) out.push(c);
    }
    for (const n of this.r.replicaNodes.values()) out.push(n.client);
    return out;
  }

  /** `true` when the seed reported a cluster topology; `false` for a standalone server or before the topology is loaded (it is known after the first command or {@link connect}). */
  get isCluster(): boolean {
    return this.r.ready && !this.r.standalone;
  }
  /** The nodes of the current slot table (a snapshot; the list changes after a refresh). */
  get nodes(): readonly ClusterNode[] {
    return this.r.distinctNodes();
  }
  /** How many times the slot table was reloaded after the first load. Useful in tests and metrics. */
  get refreshCount(): number {
    return this.r.refreshes;
  }
  /** How many `-MOVED` redirects were followed so far. */
  get redirectCount(): number {
    return this.r.redirects;
  }
  /** Commands waiting for a reply, summed over all node connections. */
  get inFlight(): number {
    let n = 0;
    for (const c of this.allClients()) n += c.inFlight;
    return n;
  }
  /**
   * Reloads the slot table from the cluster (`CLUSTER SLOTS`) now. Concurrent callers share one request.
   * The client also does this by itself after certain `MOVED` replies.
   */
  refreshSlots(): Promise<void> {
    return this.r.refresh();
  }
  /**
   * The node that currently owns a key, e.g. to run a node-specific command on it.
   *
   * @param key The key (its `{tag}` decides the slot).
   * @returns The owning {@link ClusterNode}; the seed node for a standalone server.
   * @throws Error when the topology is not loaded yet (`await connect()` first); {@link RuvioError} `CLUSTERDOWN` when no node serves the slot.
   */
  nodeFor(key: Key): ClusterNode {
    const r = this.r;
    if (!r.ready) throw new Error('ruvio: cluster topology not loaded yet, await connect() first');
    if (r.standalone) return r.home;
    const n = r.slots[hashSlot(key)];
    if (n === undefined) throw noSlot(hashSlot(key));
    return n;
  }
  /** The hash slot (0 to 16383) of a key; same as {@link hashSlot}. */
  slotOf(key: Key): number {
    return hashSlot(key);
  }

  /** The same commands with bulk strings returned as Buffers (binary-safe); shares this client's connections. See {@link RuvioClient.binary}. */
  get binary(): BinaryCommands {
    if (this._binary === null) {
      const view = Object.create(this) as ClusterClient;
      view._fl = 1;
      this._binary = view as unknown as BinaryCommands;
    }
    return this._binary;
  }

  /**
   * Creates a new cluster client with the same options and its own connections.
   *
   * @param overrides Options that replace the copied ones.
   */
  duplicate(overrides: ClusterOptions = {}): ClusterClient {
    return new ClusterClient({ ...this.options, ...overrides });
  }

  /**
   * Runs `fn` with a dedicated connection to the node that owns `key` (for `WATCH`, or blocking reads
   * you want off the shared connection) and closes it afterwards, even if `fn` throws.
   *
   * @param key A key whose owning node is used; keep every key `fn` touches in the same slot.
   * @param fn Receives the dedicated {@link RuvioClient}.
   * @returns Whatever `fn` returns.
   *
   * @example
   * ```ts
   * await cluster.isolated('{acct}:1', async (c) => {
   *   await c.watch('{acct}:1');
   *   return c.multi().incr('{acct}:1').exec();
   * });
   * ```
   */
  async isolated<T>(key: Key, fn: (client: RuvioClient) => Promise<T>): Promise<T> {
    await this.r.whenReady();
    const c = this.nodeFor(key).client.duplicate();
    try {
      return await fn(c);
    } finally {
      await c.quit();
    }
  }

  /**
   * Starts a {@link Pipeline}. Commands are grouped by owning node and sent with one write per node;
   * replies come back in call order. Split commands (`MGET`, `DEL`, ...) are split as usual; other
   * cross-slot commands leave a `CROSSSLOT` error in their slot while the rest runs.
   */
  pipeline(): Pipeline {
    return new ClusterPipelineImpl(this.r) as unknown as Pipeline;
  }

  /**
   * Starts a {@link Transaction}. All keys must hash to one slot (use `{tag}`), otherwise `exec()` rejects with `CROSSSLOT` and nothing is sent.
   */
  multi(): Transaction {
    return new ClusterTransactionImpl(this.r) as unknown as Transaction;
  }

  /**
   * Creates a {@link Subscriber} on the seed node. Any node serves `SUBSCRIBE`/`PSUBSCRIBE`, and `PUBLISH` reaches subscribers on every shard.
   * For sharded channels use {@link subscriberFor}.
   *
   * @param opts {@link SubscriberOptions} (host, port and path are fixed by the cluster client; with `seeds` on a standalone server the subscriber starts at the seed the commands use).
   */
  subscriber(opts: Omit<SubscriberOptions, 'host' | 'port' | 'path'> = {}): Subscriber {
    const h = this.r.home;
    // A standalone fallback with several seeds starts the subscriber at the seed the commands use.
    const seeds = this.r.standalone ? h.conn.seedsFromCurrent() : undefined;
    return new Subscriber(seeds === undefined ? { ...this.r.base, host: h.host, port: h.port, ...opts } : { ...this.r.base, seeds, ...opts });
  }
  /**
   * Creates a {@link Subscriber} on the node that owns `channel`'s slot, which is where `SSUBSCRIBE` and `SPUBLISH` for that channel work.
   *
   * @param channel A sharded channel name (its `{tag}` decides the node).
   * @param opts {@link SubscriberOptions}.
   */
  async subscriberFor(channel: string, opts: Omit<SubscriberOptions, 'host' | 'port' | 'path'> = {}): Promise<Subscriber> {
    await this.r.whenReady();
    const n = this.nodeFor(channel);
    return new Subscriber({ ...this.r.base, host: n.host, port: n.port, ...opts });
  }

  /**
   * Lazily walks `SCAN` on every node, one node after the other, so it yields the keys of the whole cluster.
   * A plain `scan(cursor)` call reaches only one node.
   *
   * @param opts {@link ScanOptions}: `match`, `count`, `type`.
   *
   * @example
   * ```ts
   * for await (const key of cluster.scanIterator({ match: 'session:*' })) console.log(key);
   * ```
   */
  override async *scanIterator(opts?: ScanOptions): AsyncGenerator<string> {
    await this.r.whenReady();
    for (const n of this.r.distinctNodes()) yield* n.client.scanIterator(opts);
  }
}

function parseSeed(url: string): ClusterOptions {
  const u = new URL(url);
  const o: ClusterOptions = { host: u.hostname || '127.0.0.1', port: u.port ? Number(u.port) : 6379 };
  if (u.protocol === 'rediss:' || u.protocol === 'ruvios:') o.tls = true;
  if (u.username) o.username = decodeURIComponent(u.username);
  if (u.password) o.password = decodeURIComponent(u.password);
  return o;
}

/**
 * Creates a {@link ClusterClient}. Shorthand for `new ClusterClient(options)`.
 *
 * @param options A seed URL such as `'redis://127.0.0.1:7000'`, or {@link ClusterOptions}. With the
 * default `cluster: 'auto'` a standalone seed simply works as a plain client.
 *
 * @example
 * ```ts
 * const cluster = createClusterClient({ host: '127.0.0.1', port: 6379 });
 * ```
 */
export function createClusterClient(options: ClusterOptions | string = {}): ClusterClient {
  return new ClusterClient(options);
}

/** @internal test hook: replaces the slot table owner of every slot with `node`. */
export function _forceStaleTable(c: ClusterClient, node: ClusterNode): void {
  c.r.slots.fill(node);
}
