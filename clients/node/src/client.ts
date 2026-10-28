// SPDX-License-Identifier: MIT
// The client: typed commands over one multiplexed connection, plus coordination helpers.

import './polyfill.js';
import { randomBytes } from 'node:crypto';
import { Commands } from './commands.js';
import { Connection, type ConnectionOptions } from './connection.js';
import type { Encoder } from './encoder.js';
import type { Tf } from './transforms.js';
import { str } from './transforms.js';
import { newPipeline, newTransaction, type Pipeline, type Transaction } from './pipeline.js';
import { Subscriber, type SubscriberOptions } from './subscriber.js';
import type { Key, Value, ScanOptions, SortedSetEntry } from './types.js';

/**
 * Options for {@link RuvioClient} and {@link createClient}. These are the {@link ConnectionOptions}
 * (host, port, TLS, credentials, database, protocol, reconnect and so on); see there for each field.
 *
 * @example
 * ```ts
 * const client = createClient({ host: '127.0.0.1', port: 6379, password: 's3cret', db: 1, connectTimeoutMs: 2000 });
 * ```
 */
export interface ClientOptions extends ConnectionOptions {}

/** Replaces `string` with `Buffer` throughout a reply type (used by {@link BinaryCommands}). */
export type Bin<T> = T extends string
  ? Buffer
  : T extends null | undefined | number | boolean | bigint
    ? T
    : T extends Buffer
      ? T
      : T extends readonly (infer U)[]
        ? Bin<U>[]
        : T extends object
          ? { [K in keyof T]: Bin<T[K]> }
          : T;

type AnyFn = (...a: any[]) => any;
/**
 * Every command of {@link Commands} with bulk strings resolved as Buffers. Reach it through
 * `client.binary` for binary-safe values. Each method has the same arguments and meaning as its
 * counterpart on {@link Commands}; only the reply type changes (`string` becomes `Buffer`).
 *
 * @example
 * ```ts
 * await client.binary.set('k', Buffer.from([0xff, 0x00]));
 * const raw = await client.binary.get('k'); // Buffer
 * ```
 */
export type BinaryCommands = {
  [K in keyof Commands as Commands[K] extends AnyFn ? (K extends `_${string}` ? never : K) : never]: Commands[K] extends (
    ...a: infer A
  ) => Promise<infer V>
    ? (...a: A) => Promise<Bin<V>>
    : never;
};

/**
 * A distributed lock held by this process, returned by {@link Helpers.acquireLock}. It is a
 * `SET key token NX PX ttl` string; renewal and release only act while the key still holds this
 * lock's random `token`, so a lock that expired and was taken by someone else is never touched.
 *
 * Supports `await using` (released at scope exit).
 *
 * @example
 * ```ts
 * await using lock = await client.acquireLock('lock:report', 30_000, { waitMs: 5000 });
 * if (lock) {
 *   // work while holding the lock; call lock.renew() for long jobs
 * }
 * ```
 */
export class Lock {
  private released = false;
  constructor(
    private readonly client: Commands,
    readonly key: Key,
    readonly token: string,
    readonly ttlMs: number,
  ) {}

  /**
   * Extends the lock's lifetime if this token still owns it (`SET key token IFEQ token PX ttl`).
   *
   * @param ttlMs New time to live in milliseconds from now. Default: the original TTL.
   * @returns `true` when extended; `false` when the lock was lost (expired or taken over) or already released.
   *
   * @example
   * ```ts
   * if (!(await lock.renew(30_000))) throw new Error('lock lost');
   * ```
   */
  async renew(ttlMs: number = this.ttlMs): Promise<boolean> {
    if (this.released) return false;
    return (await this.client.set(this.key, this.token, { ifEq: this.token, px: ttlMs })) !== null;
  }

  /**
   * Releases the lock: deletes the key only if it still holds this token (`DELEX key IFEQ token`).
   * Calling it again does nothing.
   *
   * @returns `true` when this call deleted the key; `false` when the lock was already lost or released.
   */
  async release(): Promise<boolean> {
    if (this.released) return false;
    this.released = true;
    return this.client.delex(this.key, this.token);
  }

  /** `true` after {@link release} has been called (or the `await using` scope ended). */
  get isReleased(): boolean {
    return this.released;
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.release();
  }
}

/**
 * A fenced lease returned by {@link Helpers.acquireLease} (`LEASE` / `RELEASE`). The `token`
 * increases on every grant, so pass it to downstream systems and let them reject writes carrying
 * an older token. Supports `await using`.
 *
 * @example
 * ```ts
 * const lease = await client.acquireLease('job:nightly', 30_000);
 * if (lease) {
 *   await storage.write(data, { fence: lease.token });
 *   await lease.release();
 * }
 * ```
 */
export class Lease {
  private released = false;
  constructor(
    private readonly client: Commands,
    readonly key: Key,
    readonly token: number,
  ) {}
  /**
   * Extends the lease to now + `ttlMs` (`RENEW key token ttlMs`). The fencing token stays the same.
   *
   * @returns `true` when extended; `false` when the lease had already expired or was released.
   */
  async renew(ttlMs: number): Promise<boolean> {
    if (this.released) return false;
    return this.client.renew(this.key, this.token, ttlMs);
  }
  /**
   * Gives the lease back (`RELEASE key token`). Calling it again does nothing.
   *
   * @returns `true` when released; `false` when the lease had already expired or was released.
   */
  async release(): Promise<boolean> {
    if (this.released) return false;
    this.released = true;
    return this.client.release(this.key, this.token);
  }
  async [Symbol.asyncDispose](): Promise<void> {
    await this.release();
  }
}

/**
 * One slot of a counting semaphore returned by {@link Helpers.acquireSemaphore}
 * (`SEMAPHORE` / `RELEASE`). Each permit has its own TTL. Supports `await using`.
 *
 * @example
 * ```ts
 * const permit = await client.acquireSemaphore('pool:db', 10, 30_000, { waitMs: 2000 });
 * if (permit) {
 *   try { await query(); } finally { await permit.release(); }
 * }
 * ```
 */
export class Permit {
  private released = false;
  constructor(
    private readonly client: Commands,
    readonly key: Key,
    readonly token: string,
  ) {}
  /**
   * Extends this slot to now + `ttlMs` (`RENEW key token ttlMs`).
   *
   * @returns `true` when extended; `false` when the permit had already expired or was released.
   */
  async renew(ttlMs: number): Promise<boolean> {
    if (this.released) return false;
    return this.client.renew(this.key, this.token, ttlMs);
  }
  /**
   * Frees this slot (`RELEASE key token`). Calling it again does nothing.
   *
   * @returns `true` when released; `false` when the permit had already expired or was released.
   */
  async release(): Promise<boolean> {
    if (this.released) return false;
    this.released = true;
    return this.client.release(this.key, this.token);
  }
  async [Symbol.asyncDispose](): Promise<void> {
    await this.release();
  }
}

/** Thrown by {@link Helpers.withLock} and {@link Helpers.getOrCreate} when the lock, or another caller's fill, was not available within the wait. */
export class AcquireTimeoutError extends Error {
  override get name(): string {
    return 'AcquireTimeoutError';
  }
}

/** Thrown by {@link Helpers.idempotent} when the key is already reserved for a different fingerprint (`IDEM BEGIN` answered `conflict`). */
export class IdempotencyConflictError extends Error {
  override get name(): string {
    return 'IdempotencyConflictError';
  }
}

/** Thrown by {@link Helpers.idempotent} when another owner was still running the operation (`pending`) when the wait ended. */
export class IdempotencyPendingError extends Error {
  override get name(): string {
    return 'IdempotencyPendingError';
  }
}

/**
 * Waiting options for the lock, lease and semaphore helpers.
 *
 * @example
 * ```ts
 * await client.acquireLock('lock:a', 10_000, { waitMs: 3000, retryMs: 25 });
 * ```
 */
export interface AcquireOptions {
  /** How long to keep retrying, in milliseconds. `0` (default) tries once and gives up. */
  waitMs?: number;
  /** Delay between attempts in milliseconds (default 50). */
  retryMs?: number;
}

/** Options for {@link Helpers.getOrCreate}. */
export interface GetOrCreateOptions {
  /** How long the fill marker lives, i.e. the factory's time budget (ms, default 10 000). */
  fillMs?: number;
  /** Stale-while-revalidate window in milliseconds: after the TTL, the old value is still served this long while one caller refreshes it (`FILLSET ... SWR`). Default: none. */
  staleMs?: number;
  /** Give up waiting on another caller's fill after this long, in milliseconds (default `fillMs` + 1000); then {@link AcquireTimeoutError} is thrown. */
  waitMs?: number;
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
const newToken = () => randomBytes(16).toString('hex');

function parseUrl(url: string): ClientOptions {
  const u = new URL(url);
  const o: ClientOptions = { host: u.hostname || '127.0.0.1', port: u.port ? Number(u.port) : 6379 };
  if (u.protocol === 'rediss:' || u.protocol === 'ruvios:') o.tls = true;
  if (u.username) o.username = decodeURIComponent(u.username);
  if (u.password) o.password = decodeURIComponent(u.password);
  const db = u.pathname.replace(/^\//, '');
  if (db) o.db = Number(db);
  return o;
}

/**
 * Higher-level helpers (locks, leases, semaphores, rate limits, cache-aside, idempotency,
 * recent-items lists, Bloom "seen before", scan iterators) on top of {@link Commands}. They use
 * only single-key commands, so they behave the same on a {@link RuvioClient} and a
 * {@link ClusterClient}. Everything on {@link Commands} is available here too.
 */
export abstract class Helpers extends Commands {
  // ---- helpers ---------------------------------------------------------------------------

  /**
   * Tries to take a distributed lock (`SET key token NX PX ttl` with a random token).
   *
   * @param key Lock key.
   * @param ttlMs Lock lifetime in milliseconds. Pick it longer than the work, or call {@link Lock.renew}; it expires by itself if the holder crashes.
   * @param opts {@link AcquireOptions} (`waitMs`, `retryMs`) plus an optional `token` to use instead of a random one.
   * @returns A {@link Lock} (supports `await using`), or `null` when another owner still holds it after `waitMs`.
   * @throws {@link RuvioError} `WRONGTYPE` when the key holds a non-string; {@link ConnectionError} on connection loss.
   *
   * @example
   * ```ts
   * await using lock = await client.acquireLock('lock:report', 30_000, { waitMs: 5000 });
   * if (!lock) return; // someone else is running the report
   * ```
   */
  async acquireLock(key: Key, ttlMs: number, opts: AcquireOptions & { token?: string } = {}): Promise<Lock | null> {
    const token = opts.token ?? newToken();
    const deadline = Date.now() + (opts.waitMs ?? 0);
    for (;;) {
      if ((await this.set(key, token, { nx: true, px: ttlMs })) !== null) return new Lock(this, key, token, ttlMs);
      if (Date.now() >= deadline) return null;
      await sleep(opts.retryMs ?? 50);
    }
  }

  /**
   * Runs `fn` while holding a lock and always releases it afterwards (only if still ours).
   *
   * @param key Lock key.
   * @param ttlMs Lock lifetime in milliseconds (see {@link acquireLock}).
   * @param fn Work to run; receives the {@link Lock} so it can call `renew()`.
   * @param opts {@link AcquireOptions}: how long to wait for the lock.
   * @returns Whatever `fn` returns.
   * @throws {@link AcquireTimeoutError} when the lock is not obtained within `waitMs` (default: one try); errors from `fn` are rethrown after release.
   *
   * @example
   * ```ts
   * const total = await client.withLock('lock:billing', 10_000, async () => computeTotal(), { waitMs: 2000 });
   * ```
   */
  async withLock<T>(key: Key, ttlMs: number, fn: (lock: Lock) => Promise<T>, opts: AcquireOptions = {}): Promise<T> {
    const lock = await this.acquireLock(key, ttlMs, opts);
    if (lock === null) throw new AcquireTimeoutError('ruvio: lock ' + str(key) + ' is held');
    try {
      return await fn(lock);
    } finally {
      await lock.release();
    }
  }

  /**
   * Takes a fenced lease (`LEASE key ttlMs`). Unlike a lock, the token increases on every grant,
   * so downstream systems can reject writes from an older holder.
   *
   * @param key Lease key.
   * @param ttlMs Lease length in milliseconds.
   * @param opts {@link AcquireOptions}: how long to retry.
   * @returns A {@link Lease} (supports `await using`), or `null` while another holder has it after `waitMs`.
   * @throws {@link RuvioError} `WRONGTYPE` when the key holds another type.
   *
   * @example
   * ```ts
   * const lease = await client.acquireLease('job:nightly', 60_000);
   * if (lease) console.log('fence token', lease.token);
   * ```
   */
  async acquireLease(key: Key, ttlMs: number, opts: AcquireOptions = {}): Promise<Lease | null> {
    const deadline = Date.now() + (opts.waitMs ?? 0);
    for (;;) {
      const t = await this.lease(key, ttlMs);
      if (t !== null) return new Lease(this, key, t);
      if (Date.now() >= deadline) return null;
      await sleep(opts.retryMs ?? 50);
    }
  }

  /**
   * Takes one of `max` slots of a counting semaphore (`SEMAPHORE key max ttlMs`).
   *
   * @param key Semaphore key.
   * @param max Maximum concurrent holders.
   * @param ttlMs How long this holder's slot lasts, in milliseconds (a crashed holder frees its slot by expiry).
   * @param opts {@link AcquireOptions}: how long to retry.
   * @returns A {@link Permit} (supports `await using`), or `null` when all slots are taken after `waitMs`.
   * @throws {@link RuvioError} `WRONGTYPE` when the key holds another type.
   *
   * @example
   * ```ts
   * const permit = await client.acquireSemaphore('pool:db', 10, 30_000, { waitMs: 1000 });
   * ```
   */
  async acquireSemaphore(key: Key, max: number, ttlMs: number, opts: AcquireOptions = {}): Promise<Permit | null> {
    const deadline = Date.now() + (opts.waitMs ?? 0);
    for (;;) {
      const t = await this.semaphore(key, max, ttlMs);
      if (t !== null) return new Permit(this, key, t);
      if (Date.now() >= deadline) return null;
      await sleep(opts.retryMs ?? 50);
    }
  }

  /**
   * Fixed-window rate limit (`LIMIT key max windowMs`): each accepted call uses one of `max` in the
   * window; refused calls do not count.
   *
   * @param key Limiter key, e.g. `'rl:user:7'`.
   * @param max Calls allowed per window.
   * @param windowMs Window length in milliseconds. The window starts at the first accepted call.
   * @returns `{ allowed: true, remaining }` with the calls left after this one, or `{ allowed: false, remaining: 0 }` when the window is full.
   * @throws {@link RuvioError} `WRONGTYPE` when the key holds another type.
   *
   * @example
   * ```ts
   * const { allowed, remaining } = await client.rateLimit('rl:user:7', 100, 60_000);
   * if (!allowed) return res.status(429).end();
   * ```
   */
  async rateLimit(key: Key, max: number, windowMs: number): Promise<{ allowed: boolean; remaining: number }> {
    const r = await this.limit(key, max, windowMs);
    return r === null ? { allowed: false, remaining: 0 } : { allowed: true, remaining: r };
  }

  /**
   * Cache-aside with stampede protection (`FILLGET` / `FILLSET`). On a miss exactly one caller runs
   * `factory`; the others poll with a short backoff (2 ms doubling to 50 ms; the server-side `WAIT` is
   * never used because it would park the shared connection) and then receive the stored value.
   * With `staleMs`, an expired value is returned at once while one caller refreshes it in the background.
   *
   * @param key Cache key.
   * @param ttlMs How long the produced value stays fresh, in milliseconds.
   * @param factory Builds the value (sync or async). Runs in one caller only. If it throws, the error propagates to that caller and the fill marker lapses after `fillMs`.
   * @param opts {@link GetOrCreateOptions}: `fillMs`, `staleMs`, `waitMs`.
   * @returns The cached or freshly built value as a string.
   * @throws {@link AcquireTimeoutError} when another caller's fill does not finish within `waitMs`.
   *
   * @example
   * ```ts
   * const html = await client.getOrCreate('page:home', 60_000, async () => renderHome(), { staleMs: 30_000 });
   * ```
   */
  async getOrCreate(key: Key, ttlMs: number, factory: () => Promise<Value> | Value, opts: GetOrCreateOptions = {}): Promise<string> {
    const fillMs = opts.fillMs ?? 10_000;
    const deadline = Date.now() + (opts.waitMs ?? fillMs + 1000);
    let backoff = 2;
    for (;;) {
      const r = await this.fillget(key, fillMs);
      switch (r.state) {
        case 'hit':
          return r.value!;
        case 'fill': {
          const v = await factory();
          await this.fillset(key, v, ttlMs, r.token!, opts.staleMs);
          return str(v);
        }
        case 'stale':
          if (r.token !== null) {
            const token = r.token;
            // Refresh in the background; the stale value answers this caller now.
            Promise.resolve()
              .then(factory)
              .then((v) => this.fillset(key, v, ttlMs, token, opts.staleMs))
              .catch(() => undefined);
          }
          return r.value!;
        case 'busy':
          if (Date.now() >= deadline) throw new AcquireTimeoutError('ruvio: getOrCreate timed out waiting for ' + str(key));
          await sleep(backoff);
          backoff = Math.min(backoff * 2, 50);
      }
    }
  }

  /**
   * Runs `fn` at most once per (key, fingerprint) within `ttlMs` (`IDEM BEGIN` / `COMPLETE`). A repeat
   * returns the stored result with `replayed: true` without running `fn`. This is not exactly-once for
   * external effects: if `fn` runs but the process dies before completion, the key stays pending until `ttlMs`.
   *
   * @param key Idempotency key, e.g. the client-supplied request id.
   * @param fingerprint Hash of the request body. Reusing the key with a different fingerprint is a conflict (1 to 128 bytes).
   * @param ttlMs How long the key and result are kept, in milliseconds (1 ms to 7 days).
   * @param fn The work. Its return value is stored (at most 4 MiB) and returned to repeats.
   * @param opts `owner` id of this attempt (default random), `waitMs` how long to poll while another owner runs it (default 0: fail at once), `retryMs` poll delay (default 20), `abortOnError` (default false).
   * With `abortOnError`, a `fn` that throws first sends `IDEM ABORT`, so a retry runs it again at once instead of
   * finding the key pending until `ttlMs`; the original error is rethrown, and a failed abort is ignored. Use it only
   * when a failed `fn` did not take effect or is safe to run again.
   * @returns `{ result, replayed }`.
   * @throws {@link IdempotencyConflictError} when the key belongs to a different fingerprint; {@link IdempotencyPendingError} when another owner is still running after `waitMs`.
   *
   * @example
   * ```ts
   * const { result, replayed } = await client.idempotent('pay:123', sha256(body), 86_400_000, async () => charge(body));
   * ```
   */
  async idempotent(
    key: Key,
    fingerprint: Value,
    ttlMs: number,
    fn: () => Promise<Value> | Value,
    opts: { owner?: string; waitMs?: number; retryMs?: number; abortOnError?: boolean } = {},
  ): Promise<{ result: string; replayed: boolean }> {
    const owner = opts.owner ?? newToken();
    const deadline = Date.now() + (opts.waitMs ?? 0);
    for (;;) {
      const r = await this.idemBegin(key, fingerprint, owner, ttlMs);
      switch (r.state) {
        case 'acquired': {
          let v: Value;
          try {
            v = await fn();
          } catch (e) {
            if (opts.abortOnError) await this.idemAbort(key, fingerprint, owner).catch(() => undefined);
            throw e;
          }
          await this.idemComplete(key, fingerprint, owner, v);
          return { result: str(v), replayed: false };
        }
        case 'completed':
          return { result: r.result ?? '', replayed: true };
        case 'conflict':
          throw new IdempotencyConflictError('ruvio: ' + str(key) + ' is reserved for another fingerprint');
        case 'pending':
          if (Date.now() >= deadline) throw new IdempotencyPendingError('ruvio: ' + str(key) + ' is still running elsewhere');
          await sleep(opts.retryMs ?? 20);
      }
    }
  }

  /**
   * Adds items to a "recent items" list, newest first, keeping at most `cap` (`LPUSHCAP`).
   *
   * @param key List key.
   * @param cap Maximum number of items to keep.
   * @param values Items to add.
   * @returns The list length after trimming.
   *
   * @example
   * ```ts
   * await client.pushRecent('recent:7', 50, 'item-9');
   * ```
   */
  pushRecent(key: Key, cap: number, ...values: Value[]): Promise<number> {
    return this.lpushcap(key, cap, ...values);
  }
  /**
   * Reads the newest items added with {@link pushRecent} (`LRANGE key 0 n-1`).
   *
   * @param key List key.
   * @param n How many items to return.
   * @returns Up to `n` items, newest first; empty when the key is missing.
   */
  recent(key: Key, n: number): Promise<string[]> {
    return this.lrange(key, 0, n - 1);
  }

  /**
   * Records items in a Bloom filter and reports, per item, whether it was probably seen before (`BF.MADD`).
   * A missing key becomes a scalable filter (error 0.01, capacity 100).
   *
   * @param key Bloom filter key.
   * @param items Items to record.
   * @returns One boolean per item: `true` when probably seen before (false positives are possible), `false` when new.
   *
   * @example
   * ```ts
   * const [seen] = await client.seenBefore('seen:urls', url);
   * if (!seen) crawl(url);
   * ```
   */
  async seenBefore(key: Key, ...items: Value[]): Promise<boolean[]> {
    const added = await this.bfMadd(key, ...items);
    for (let i = 0; i < added.length; i++) added[i] = !added[i];
    return added;
  }
  /**
   * Tests Bloom filter membership without recording (`BF.MEXISTS`).
   *
   * @param key Bloom filter key.
   * @param items Items to test.
   * @returns One boolean per item: `false` is definite, `true` means probably present.
   */
  mightContain(key: Key, ...items: Value[]): Promise<boolean[]> {
    return this.bfMexists(key, ...items);
  }

  /**
   * Lazily walks the whole key space with `SCAN`, one page at a time, yielding keys.
   * On a plain client against a multi-shard server in per-shard mode this covers only the connected
   * shard; {@link ClusterClient} overrides it to walk every node. Keys written during the walk may or may not appear.
   *
   * @param opts {@link ScanOptions}: `match`, `count`, `type`.
   *
   * @example
   * ```ts
   * for await (const key of client.scanIterator({ match: 'session:*' })) await client.del(key);
   * ```
   */
  async *scanIterator(opts?: ScanOptions): AsyncGenerator<string> {
    let cursor = '0';
    do {
      const p = await this.scan(cursor, opts);
      cursor = p.cursor;
      for (const k of p.items) yield k;
    } while (cursor !== '0');
  }
  /**
   * Lazily walks a hash with `HSCAN`, yielding `[field, value]` pairs.
   *
   * @param key Hash key.
   * @param opts {@link ScanOptions} without `type`.
   *
   * @example
   * ```ts
   * for await (const [field, value] of client.hscanIterator('user:1')) console.log(field, value);
   * ```
   */
  async *hscanIterator(key: Key, opts?: Omit<ScanOptions, 'type'>): AsyncGenerator<[string, string]> {
    let cursor = '0';
    do {
      const p = await this.hscan(key, cursor, opts);
      cursor = p.cursor;
      for (const e of p.items) yield e;
    } while (cursor !== '0');
  }
  /**
   * Lazily walks a set with `SSCAN`, yielding members.
   *
   * @param key Set key.
   * @param opts {@link ScanOptions} without `type`.
   */
  async *sscanIterator(key: Key, opts?: Omit<ScanOptions, 'type'>): AsyncGenerator<string> {
    let cursor = '0';
    do {
      const p = await this.sscan(key, cursor, opts);
      cursor = p.cursor;
      for (const m of p.items) yield m;
    } while (cursor !== '0');
  }
  /**
   * Lazily walks a sorted set with `ZSCAN`, yielding `{ member, score }` entries.
   *
   * @param key Sorted set key.
   * @param opts {@link ScanOptions} without `type`.
   */
  async *zscanIterator(key: Key, opts?: Omit<ScanOptions, 'type'>): AsyncGenerator<SortedSetEntry> {
    let cursor = '0';
    do {
      const p = await this.zscan(key, cursor, opts);
      cursor = p.cursor;
      for (const e of p.items) yield e;
    } while (cursor !== '0');
  }
}

/**
 * The Ruvio client: typed commands over one pipelined socket, plus the {@link Helpers}.
 *
 * All calls share that socket: frames from concurrent callers are coalesced into one write and
 * replies resolve in order, so many `await`s in flight cost few system calls. The socket opens
 * on the first command (or on {@link connect}) and, unless `reconnect: false`, opens again on
 * the next command after a drop. Commands in flight when the socket drops reject with
 * {@link ConnectionError}; a command is never re-sent automatically, because it may have run.
 *
 * Blocking commands (`BLPOP`, `XREAD ... BLOCK`, `FILLGET ... WAIT`, `WAIT`) park that socket for
 * every caller: run them on {@link duplicate}. `WATCH` also needs a connection of its own, see
 * {@link isolated}.
 *
 * Against a multi-shard server (`port_mode = "per-shard"`) one connection works for every key:
 * the server forwards commands for keys that another shard owns, at the cost of a hop. Use
 * {@link ClusterClient} to talk to each shard directly.
 *
 * @example
 * ```ts
 * import { createClient } from 'ruvio-client';
 *
 * const client = createClient({ host: '127.0.0.1', port: 6379 });
 * await client.set('greeting', 'hello');
 * console.log(await client.get('greeting')); // 'hello'
 * await client.quit();
 * ```
 */
export class RuvioClient extends Helpers {
  /** @internal */
  readonly conn: Connection;
  /** The options this client was created with (a copy; changing it has no effect). */
  readonly options: ClientOptions;
  private _binary: BinaryCommands | null = null;

  /**
   * Creates a client. Nothing connects until the first command or {@link connect}.
   *
   * @param options {@link ClientOptions}, or a URL string such as `redis://user:pass@host:6379/2`
   * (`rediss://` or `ruvios://` turn TLS on; the path is the database number).
   */
  constructor(options: ClientOptions | string = {}) {
    super();
    this.options = typeof options === 'string' ? parseUrl(options) : { ...options };
    this.conn = new Connection(this.options);
  }

  protected _enc(): Encoder {
    return this.conn.encoder();
  }
  protected _done(tf: Tf | null, fl: number): Promise<any> {
    return this.conn.track(tf, fl);
  }
  protected _fail(e: unknown, enc: Encoder): Promise<never> {
    if (!enc.abortFrame()) this.conn.destroySocket(e as Error);
    return Promise.reject(e);
  }

  /**
   * Opens the socket and waits for the handshake (AUTH/HELLO, SELECT, SETNAME). Optional: commands connect lazily.
   *
   * @throws {@link ConnectionError} when the connection fails, is refused or times out (`connectTimeoutMs`), or the handshake is rejected (bad password).
   */
  connect(): Promise<void> {
    return this.conn.connect();
  }
  /**
   * Waits for the replies still in flight, then closes the socket. The client cannot be used afterwards.
   * Prefer this over {@link disconnect} for a clean shutdown.
   */
  quit(): Promise<void> {
    return this.conn.quit();
  }
  /** Closes the socket immediately. Calls still in flight reject with {@link ConnectionError}. The client cannot be used afterwards. */
  disconnect(): void {
    this.conn.disconnect();
  }
  async [Symbol.asyncDispose](): Promise<void> {
    await this.quit();
  }
  /** Number of commands sent (or queued) that are still waiting for their reply. */
  get inFlight(): number {
    return this.conn.inFlight;
  }
  /**
   * Subscribes to connection events.
   *
   * - `connect`: the socket is up.
   * - `disconnect` (error): an established connection dropped.
   * - `connectError` (error): opening a connection failed.
   * - `error` (error): same errors as above; emitted only if you have an `error` listener.
   * - `push` (data): RESP3 out-of-band data, e.g. `CLIENT TRACKING` invalidations.
   * - `unexpected` (reply): a reply that matched no waiting command.
   *
   * @param event Event name.
   * @param listener Callback.
   * @returns This client, for chaining.
   *
   * @example
   * ```ts
   * client.on('disconnect', (err) => console.warn('lost connection', err.message));
   * ```
   */
  on(event: 'connect' | 'disconnect' | 'connectError' | 'error' | 'push' | 'unexpected', listener: (...a: any[]) => void): this {
    this.conn.on(event, listener);
    return this;
  }
  /** Removes a listener added with {@link on}. */
  off(event: string, listener: (...a: any[]) => void): this {
    this.conn.off(event, listener);
    return this;
  }

  /**
   * The same commands with bulk strings returned as Buffers, so binary values survive intact.
   * It shares this client's connection.
   *
   * @example
   * ```ts
   * const bytes = await client.binary.get('image:1'); // Buffer | null
   * ```
   */
  get binary(): BinaryCommands {
    if (this._binary === null) {
      const view = Object.create(this) as RuvioClient;
      view._fl = 1;
      this._binary = view as unknown as BinaryCommands;
    }
    return this._binary;
  }

  /**
   * Switches the logical database (`SELECT db`) and remembers it so a reconnect selects it again.
   * It affects every caller of this client; use `duplicate({ db })` for an independent connection.
   *
   * @param db Database index, `0` to `databases - 1` (16 by default).
   * @returns `'OK'`.
   * @throws {@link RuvioError} when `db` is out of range.
   */
  override select(db: number): Promise<'OK'> {
    this.conn.db = db;
    return super.select(db);
  }

  /**
   * Creates a new client with the same options and the current database, on its own socket. Use it
   * for blocking commands, `WATCH` and anything else that must not share the main connection.
   * Close it with {@link quit} when done.
   *
   * @param overrides Options that replace the copied ones, e.g. `{ db: 3 }`.
   * @returns A new, not yet connected, client.
   *
   * @example
   * ```ts
   * const worker = client.duplicate();
   * const job = await worker.blpop('jobs', 0);
   * ```
   */
  duplicate(overrides: ClientOptions = {}): RuvioClient {
    return new RuvioClient({ ...this.options, db: this.conn.db, ...overrides });
  }

  /**
   * Runs `fn` with a dedicated connection ({@link duplicate}) and closes it afterwards, even if `fn` throws.
   * Use it for `WATCH`/`MULTI`/`EXEC` optimistic transactions and one-off blocking reads.
   *
   * @param fn Receives the dedicated client.
   * @returns Whatever `fn` returns.
   *
   * @example
   * ```ts
   * const ok = await client.isolated(async (c) => {
   *   await c.watch('balance');
   *   const bal = Number(await c.get('balance'));
   *   return (await c.multi().set('balance', bal - 1).exec()) !== null; // null: someone changed it
   * });
   * ```
   */
  async isolated<T>(fn: (client: RuvioClient) => Promise<T>): Promise<T> {
    const c = this.duplicate();
    try {
      return await fn(c);
    } finally {
      await c.quit();
    }
  }

  /**
   * Starts a {@link Pipeline}: queue commands with the usual methods, then `exec()` sends them in one write
   * and resolves all replies in order. Not atomic: other callers' commands may run between yours.
   *
   * @example
   * ```ts
   * const [a, b] = await client.pipeline().get('a').incr('n').exec();
   * ```
   */
  pipeline(): Pipeline {
    return newPipeline(this.conn);
  }

  /**
   * Starts a {@link Transaction}: queued commands are sent as `MULTI ... EXEC` in one contiguous
   * write and run atomically on the server. At most 1,024 commands may be queued. For `WATCH` use
   * {@link isolated}.
   *
   * @example
   * ```ts
   * const res = await client.multi().incr('a').incr('b').exec(); // [1, 1]
   * ```
   */
  multi(): Transaction {
    return newTransaction(this.conn);
  }

  /**
   * Creates a {@link Subscriber}: a separate Pub/Sub connection with this client's address and credentials.
   *
   * @param opts {@link SubscriberOptions} (`binary`, `reconnectDelayMs`, `iteratorBuffer`, plus any connection option to override).
   *
   * @example
   * ```ts
   * const sub = client.subscriber();
   * await sub.subscribe('news');
   * for await (const m of sub) console.log(m.channel, m.message);
   * ```
   */
  subscriber(opts: Omit<SubscriberOptions, keyof ConnectionOptions> = {}): Subscriber {
    // A subscriber starts at the seed the command connection is on.
    const seeds = this.conn.seedsFromCurrent();
    return new Subscriber(seeds === undefined ? { ...this.options, ...opts } : { ...this.options, seeds, ...opts });
  }
}

/**
 * Creates a {@link RuvioClient}. Shorthand for `new RuvioClient(options)`.
 *
 * @param options {@link ClientOptions}, or a URL string `redis://[user:pass@]host[:port][/db]`
 * (`rediss://` or `ruvios://` enable TLS).
 *
 * @example
 * ```ts
 * const client = createClient('redis://127.0.0.1:6379/0');
 * ```
 */
export function createClient(options: ClientOptions | string = {}): RuvioClient {
  return new RuvioClient(options);
}
