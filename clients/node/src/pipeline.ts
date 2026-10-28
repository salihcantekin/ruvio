// SPDX-License-Identifier: MIT
// Explicit pipelines (one write, many replies) and MULTI/EXEC transactions.

import { Commands } from './commands.js';
import { Encoder, cmd } from './encoder.js';
import { ConnectionError, ReplyError, RuvioError } from './errors.js';
import type { Connection, BatchSink } from './connection.js';
import type { Tf } from './transforms.js';

const C_MULTI = cmd('MULTI');
const C_EXEC = cmd('EXEC');

type AnyFn = (...a: any[]) => any;
/** The command surface of {@link Commands} with every method returning the builder, for chaining. */
export type Chain<Self> = {
  [K in keyof Commands as Commands[K] extends AnyFn ? (K extends `_${string}` ? never : K) : never]: Commands[K] extends (
    ...a: infer A
  ) => any
    ? (...a: A) => Self
    : never;
};

/** @internal Shared queueing logic of pipelines and transactions. */
export abstract class Batch extends Commands {
  /** @internal */
  protected conn: Connection;
  /** @internal */
  protected readonly benc = new Encoder(null, 4096);
  /** @internal */
  protected tfs: (Tf | null)[] = [];
  /** @internal */
  protected fls: number[] = [];

  constructor(conn: Connection) {
    super();
    this.conn = conn;
  }

  /** Number of queued commands. */
  get length(): number {
    return this.tfs.length;
  }

  protected _enc(): Encoder {
    return this.benc;
  }

  protected _done(tf: Tf | null, fl: number): any {
    this.tfs.push(tf);
    this.fls.push(fl);
    return this;
  }

  protected _fail(e: unknown, enc: Encoder): any {
    enc.abortFrame();
    throw e;
  }
}

/**
 * Queues commands and sends them in one write. Each method returns the pipeline; `exec()`
 * resolves every reply in order. A failing command leaves its {@link RuvioError} in its slot
 * (it does not reject the whole batch); a lost connection rejects `exec()`.
 */
class PipelineImpl extends Batch {
  /** Sends the queued commands. Each slot holds the transformed reply or a {@link RuvioError}. */
  exec(): Promise<unknown[]> {
    const n = this.tfs.length;
    if (n === 0) return Promise.resolve([]);
    const block = this.benc.mayBlock;
    const bytes = this.benc.take();
    const tfs = this.tfs;
    const fls = this.fls;
    this.tfs = [];
    this.fls = [];
    return new Promise<unknown[]>((resolve, reject) => {
      const sink = new PipelineSink(n, tfs, resolve, reject);
      this.conn.writeFrames(bytes, block);
      this.conn.trackBatch(sink, fls, n);
    });
  }

  /** Like {@link exec}, but rejects with the first failed command's error. */
  async execStrict(): Promise<unknown[]> {
    const out = await this.exec();
    for (const v of out) if (v instanceof Error) throw v;
    return out;
  }
}

/** @internal Collects the replies of a pipeline. */
export class PipelineSink implements BatchSink {
  private readonly vals: unknown[];
  private left: number;
  private conn: ConnectionError | null = null;
  constructor(
    n: number,
    private readonly tfs: (Tf | null)[],
    private readonly resolve: (v: unknown[]) => void,
    private readonly reject: (e: unknown) => void,
  ) {
    this.vals = new Array(n);
    this.left = n;
  }
  set(i: number, v: unknown): void {
    if (v instanceof ConnectionError) this.conn = v;
    else if (!(v instanceof Error)) {
      const tf = this.tfs[i];
      if (tf !== null && tf !== undefined) {
        try {
          v = tf(v);
        } catch (e) {
          v = e;
        }
      }
    }
    this.vals[i] = v;
    if (--this.left === 0) {
      if (this.conn !== null) this.reject(this.conn);
      else this.resolve(this.vals);
    }
  }
}

/**
 * `MULTI` ... `EXEC` sent as one contiguous write, so other callers on the shared connection
 * cannot interleave. `exec()` resolves the transformed replies, or `null` when a WATCHed key
 * changed (only meaningful on a dedicated connection, see `client.isolated()`).
 */
class TransactionImpl extends Batch {
  /** Sends MULTI, the queued commands and EXEC. */
  exec(): Promise<unknown[] | null> {
    const n = this.tfs.length;
    const block = this.benc.mayBlock;
    const body = this.benc.take();
    const tfs = this.tfs;
    const fls = this.fls;
    this.tfs = [];
    this.fls = [];
    let anyBin = 0;
    for (const f of fls) anyBin |= f & 1;
    const allFl = new Uint8Array(n + 2);
    allFl[n + 1] = anyBin;
    return new Promise<unknown[] | null>((resolve, reject) => {
      const sink = new TxSink(n, tfs, fls, anyBin !== 0, resolve, reject);
      const e = this.conn.encoder();
      e.begin(C_MULTI, 0);
      e.endFrame();
      this.conn.writeFrames(body, block);
      e.begin(C_EXEC, 0);
      e.endFrame();
      this.conn.trackBatch(sink, allFl, n + 2);
    });
  }

  /** Drops the queued commands. Nothing was sent yet, so no round trip is needed. */
  discard(): void {
    this.benc.take();
    this.tfs = [];
    this.fls = [];
  }
}

/** @internal Collects the replies of a transaction. */
export class TxSink implements BatchSink {
  private left: number;
  private readonly queued: unknown[];
  private exec: unknown = null;
  private failure: unknown = null;
  constructor(
    private readonly n: number,
    private readonly tfs: (Tf | null)[],
    private readonly fls: number[],
    private readonly anyBin: boolean,
    private readonly resolve: (v: unknown[] | null) => void,
    private readonly reject: (e: unknown) => void,
  ) {
    this.left = n + 2;
    this.queued = new Array(n);
  }
  set(i: number, v: unknown): void {
    if (v instanceof ConnectionError) this.failure = v;
    if (i === 0) {
      if (v instanceof Error && this.failure === null) this.failure = v;
    } else if (i <= this.n) {
      this.queued[i - 1] = v;
    } else {
      this.exec = v;
    }
    if (--this.left > 0) return;
    if (this.failure !== null) return this.reject(this.failure);
    const ex = this.exec;
    if (ex instanceof Error) {
      // EXECABORT: attach the queue-time errors.
      const errs = this.queued.filter((q) => q instanceof RuvioError) as RuvioError[];
      (ex as RuvioError & { queueErrors?: RuvioError[] }).queueErrors = errs;
      if (errs.length > 0) ex.message += ' (' + errs.map((x) => x.message).join('; ') + ')';
      return this.reject(ex);
    }
    if (ex === null) return this.resolve(null);
    if (!Array.isArray(ex)) return this.reject(new ReplyError('EXEC returned an unexpected reply'));
    let arr = ex as unknown[];
    if (arr.length < this.n) {
      // The server ran only the commands it queued: put each queue-time error in its own slot.
      const full = new Array(this.n);
      let j = 0;
      for (let k = 0; k < this.n; k++) full[k] = this.queued[k] instanceof Error ? this.queued[k] : arr[j++];
      arr = full;
    }
    for (let k = 0; k < arr.length; k++) {
      let v = arr[k];
      if (v instanceof Error) continue;
      if (this.anyBin && (this.fls[k]! & 1) === 0) v = unbin(v);
      const tf = this.tfs[k];
      if (tf !== null && tf !== undefined) {
        try {
          v = tf(v);
        } catch (e) {
          v = e;
        }
      }
      arr[k] = v;
    }
    this.resolve(arr);
  }
}

function unbin(v: unknown): unknown {
  if (v instanceof Buffer) return v.toString();
  if (Array.isArray(v)) return v.map(unbin);
  return v;
}

/**
 * A chainable pipeline from `client.pipeline()`. It has every command method of {@link Commands}
 * (same arguments), but each one queues the command and returns the pipeline instead of a promise.
 * `exec()` then sends everything in one write and resolves the replies in call order, already
 * converted the same way as for single calls (for example `get` gives a string, `sismember` a boolean).
 *
 * A pipeline is not atomic: commands of other callers on the same connection may run between yours.
 * On a {@link ClusterClient} the commands are grouped per node and replies still come back in call order.
 * A pipeline is single use: after `exec()` the queue is empty.
 *
 * @example
 * ```ts
 * const results = await client.pipeline()
 *   .set('a', 1)
 *   .incr('a')
 *   .get('a')
 *   .exec();                  // ['OK', 2, '2']
 * ```
 */
export interface Pipeline extends Chain<Pipeline> {
  /** Number of commands queued so far. */
  readonly length: number;
  /**
   * Sends the queued commands in one write.
   *
   * @returns One entry per command, in order: the converted reply, or a {@link RuvioError} object in the slot of a command that failed (a failure does not reject the whole batch). Resolves `[]` for an empty pipeline.
   * @throws {@link ConnectionError} when the connection is lost before all replies arrive.
   */
  exec(): Promise<unknown[]>;
  /**
   * Like {@link exec}, but rejects with the first failed command's error instead of leaving it in the result.
   *
   * @returns The converted replies, all successful.
   * @throws {@link RuvioError} for the first command that failed; {@link ConnectionError} on connection loss.
   */
  execStrict(): Promise<unknown[]>;
}

/**
 * A chainable `MULTI` / `EXEC` transaction from `client.multi()`. It has every command method of
 * {@link Commands}, each queuing the command and returning the transaction. `exec()` sends
 * `MULTI`, the queue and `EXEC` as one contiguous write and the server runs them atomically
 * (one WAL record).
 *
 * Limits: at most 1,024 queued commands; commands not allowed in `MULTI` (blocking ones, `WAIT`, `FILLGET ... WAIT`) are refused.
 * On a {@link ClusterClient} every key must hash to one slot, otherwise `exec()` rejects with `CROSSSLOT`.
 * For optimistic locking add `WATCH` on a dedicated connection: `client.isolated(c => c.watch(...))`.
 *
 * @example
 * ```ts
 * const res = await client.multi().incr('a').incr('b').exec(); // [1, 1]
 * ```
 */
export interface Transaction extends Chain<Transaction> {
  /** Number of commands queued so far. */
  readonly length: number;
  /**
   * Sends `MULTI`, the queued commands and `EXEC`.
   *
   * @returns The converted replies in order, or `null` when a watched key changed and the server discarded the transaction. A command that failed when it ran has its {@link RuvioError} in its slot.
   * @throws {@link RuvioError} `EXECABORT` when the server refused a command while queueing (wrong arity, unknown, `NOPERM`, ...); its message lists the queue-time errors. `CROSSSLOT` on a cluster client with keys in several slots.
   * @throws {@link ConnectionError} when the connection is lost.
   */
  exec(): Promise<unknown[] | null>;
  /** Drops the queued commands. Nothing was sent yet, so no round trip happens. */
  discard(): void;
}

/** @internal */
export function newPipeline(conn: Connection): Pipeline {
  return new PipelineImpl(conn) as unknown as Pipeline;
}

/** @internal */
export function newTransaction(conn: Connection): Transaction {
  return new TransactionImpl(conn) as unknown as Transaction;
}

export { PipelineImpl, TransactionImpl };
