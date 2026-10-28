// SPDX-License-Identifier: MIT
/**
 * Fast, zero-dependency, pipelined RESP client for the Ruvio server (Node.js 18+).
 *
 * - {@link createClient} / {@link RuvioClient}: one multiplexed connection with a typed method for
 *   every Ruvio command, plus helpers for locks, leases, semaphores, rate limits, cache-aside
 *   (`getOrCreate`), idempotency, recent-items lists and Bloom "seen before".
 * - {@link createClusterClient} / {@link ClusterClient}: the same API routed to the shard that owns each key.
 * - `client.pipeline()` and `client.multi()`: batched and atomic command groups.
 * - `client.subscriber()` ({@link Subscriber}): Pub/Sub with automatic re-subscription.
 * - Errors: {@link RuvioError} (the server said no), {@link ConnectionError} (the socket failed).
 *
 * @example
 * ```ts
 * import { createClient } from 'ruvio-client';
 *
 * const client = createClient('redis://127.0.0.1:6379');
 * await client.set('user:1', 'Ada', { ex: 3600 });
 * const name = await client.get('user:1');        // 'Ada'
 * const [a, b] = await client.pipeline().incr('x').incr('x').exec();
 * await client.quit();
 * ```
 *
 * @packageDocumentation
 */
import './polyfill.js';

export {
  RuvioClient,
  createClient,
  Lock,
  Lease,
  Permit,
  AcquireTimeoutError,
  IdempotencyConflictError,
  IdempotencyPendingError,
} from './client.js';
export type { ClientOptions, BinaryCommands, Bin, AcquireOptions, GetOrCreateOptions } from './client.js';
export { Commands } from './commands.js';
export { Cmds } from './command-names.js';
export type { Pipeline, Transaction } from './pipeline.js';
export { ClusterClient, ClusterNode, createClusterClient, hashSlot, SLOT_COUNT } from './cluster.js';
export type { ClusterOptions, ReadPolicy } from './cluster.js';
export { Subscriber } from './subscriber.js';
export type { PubSubMessage, SubscriberOptions } from './subscriber.js';
export type { ConnectionOptions } from './connection.js';
export { RuvioError, ConnectionError, ReplyError, isTransient, mayHaveRun } from './errors.js';
export { Encoder, cmd } from './encoder.js';
export type { Arg, Cmd } from './encoder.js';
export { Parser, NEED_MORE, PushMessage } from './parser.js';
export { BitfieldOp } from './bitfield.js';
export * from './types.js';
