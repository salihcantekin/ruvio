// SPDX-License-Identifier: MIT

/**
 * The server answered a command with an error (`-ERR ...`, `-WRONGTYPE ...`, `-NOPERM ...`,
 * `-CROSSSLOT ...`). The command did not take effect. `message` is the full error text and `code`
 * is its first word, so branch on `err.code`.
 *
 * @example
 * ```ts
 * try {
 *   await client.lpush('a-string-key', 'x');
 * } catch (err) {
 *   if (err instanceof RuvioError && err.code === 'WRONGTYPE') console.log('not a list');
 * }
 * ```
 */
export class RuvioError extends Error {
  /** The error prefix sent by the server, e.g. `ERR`, `WRONGTYPE`, `NOSCRIPT`. */
  readonly code: string;
  /** The command name that failed. Reserved: the client does not fill it in today. */
  command?: string;

  constructor(message: string) {
    super(message);
    const sp = message.indexOf(' ');
    this.code = sp < 0 ? message : message.slice(0, sp);
  }

  override get name(): string {
    return 'RuvioError';
  }
}

/**
 * The connection failed or closed before a reply arrived (also: connect timeout, refused connection,
 * client already closed). The command may or may not have run, and the client never re-sends it on
 * its own. The low-level cause, if any, is in `cause`.
 */
export class ConnectionError extends Error {
  /** @internal Set when opening the connection failed (as opposed to an established connection dropping). */
  unreachable = false;
  override get name(): string {
    return 'ConnectionError';
  }
}

/** A reply had a shape the typed method does not understand (for example an unknown `FILLGET` state). It points to a client/server version mismatch. */
export class ReplyError extends Error {
  override get name(): string {
    return 'ReplyError';
  }
}

const TRANSIENT_CODES: ReadonlySet<string> = new Set(['LOADING', 'MASTERDOWN', 'CLUSTERDOWN', 'TRYAGAIN', 'READONLY', 'NOREPLICAS']);

/**
 * Tells whether the same call can succeed later without any change on the caller's side: the connection
 * failed, timed out or could not be opened ({@link ConnectionError}), or the server answered `LOADING`,
 * `MASTERDOWN`, `CLUSTERDOWN`, `TRYAGAIN`, `READONLY` or `NOREPLICAS`. False for every other server reply
 * (`WRONGTYPE`, `OOM`, a syntax error), for {@link ReplyError}, for an aborted signal and for any other error.
 *
 * The client already follows `MOVED`/`ASK` and waits out `TRYAGAIN` during a slot move; this judges what
 * still reaches the caller, for a retry policy of your own (cockatiel, a loop). Retry only when it is true
 * and the command is safe to repeat (a read, a `SET` of the same value, a command with an idempotency key)
 * or {@link mayHaveRun} is false.
 *
 * @example
 * ```ts
 * import { handleWhen, retry, ExponentialBackoff } from 'cockatiel';
 * // INCR is not safe to repeat: retry it only when it surely did not run.
 * const policy = retry(handleWhen((e) => isTransient(e) && !mayHaveRun(e)), { maxAttempts: 3, backoff: new ExponentialBackoff() });
 * const visits = await policy.execute(() => client.incr('visits'));
 * ```
 */
export function isTransient(error: unknown): boolean {
  if (error instanceof RuvioError) return TRANSIENT_CODES.has(error.code);
  return error instanceof ConnectionError;
}

/**
 * Tells whether the failed call may have changed data, so repeating a write that is not safe to repeat
 * could apply it twice. False only for a server error reply ({@link RuvioError}), which the server sends
 * instead of running the command; `NOREPLICAS` is the exception, because the write was applied on the
 * primary and only the replica confirmation timed out. A failed connection or a timeout may have run.
 */
export function mayHaveRun(error: unknown): boolean {
  if (error instanceof RuvioError) return error.code === 'NOREPLICAS';
  return true;
}
