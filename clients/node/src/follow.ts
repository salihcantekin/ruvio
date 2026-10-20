// SPDX-License-Identifier: MIT
// Finding the primary again after a failover: short-lived probe sockets that ask `ROLE` (or a
// coordinator). Nothing here touches the state of a connection.

import * as net from 'node:net';
import * as tls from 'node:tls';
import { Parser, NEED_MORE } from './parser.js';
import { RuvioError } from './errors.js';
import type { ConnectionOptions } from './connection.js';

/** @internal A node address. */
export interface Addr {
  readonly host: string;
  readonly port: number;
}

/** @internal `host:port` text of an address, IPv6 in brackets. */
export function addrText(a: Addr): string {
  return (a.host.includes(':') ? '[' + a.host + ']' : a.host) + ':' + a.port;
}

const same = (a: Addr, b: Addr): boolean => a.port === b.port && a.host === b.host;

/** @internal Owns the probe sockets of one search, so cancelling it leaves nothing open. */
export class ProbeScope {
  dead = false;
  private readonly socks = new Set<net.Socket>();
  private timer: NodeJS.Timeout | null = null;
  private wake: (() => void) | null = null;

  add(s: net.Socket): void {
    this.socks.add(s);
  }
  remove(s: net.Socket): void {
    this.socks.delete(s);
  }
  /** Destroys every open probe and ends a pending {@link sleep}. */
  cancel(): void {
    this.dead = true;
    for (const s of this.socks) s.destroy();
    this.socks.clear();
    if (this.timer !== null) clearTimeout(this.timer);
    this.timer = null;
    const w = this.wake;
    this.wake = null;
    if (w !== null) w();
  }
  /** Waits `ms` in a timer; returns early when the scope is cancelled. */
  sleep(ms: number): Promise<void> {
    if (this.dead) return Promise.resolve();
    return new Promise<void>((resolve) => {
      this.wake = resolve;
      this.timer = setTimeout(() => {
        this.timer = null;
        this.wake = null;
        resolve();
      }, ms);
    });
  }
}

/** How long a probe waits for a reply: the smaller of the connect and command timeouts that are set (0 = no limit). */
function replyWait(o: ConnectionOptions): number {
  const connect = o.connectTimeoutMs ?? 5000;
  const command = o.commandTimeoutMs ?? 30_000;
  if (connect > 0 && command > 0) return Math.min(connect, command);
  return connect > 0 ? connect : command;
}

/**
 * One bounded question to one address: connect, `AUTH` as configured, the command, one reply, close.
 * Resolves the reply, or `undefined` when the address cannot be reached, times out, drops or answers
 * an error. Rejects with the server's error when `AUTH` is refused.
 */
function ask(addr: Addr, o: ConnectionOptions, args: string[], scope: ProbeScope): Promise<unknown> {
  return new Promise((resolve, reject) => {
    if (scope.dead) return resolve(undefined);
    const connectMs = o.connectTimeoutMs ?? 5000;
    const waitMs = replyWait(o);
    let socket: net.Socket;
    if (o.tls) {
      const topts: tls.ConnectionOptions = o.tls === true ? {} : { ...o.tls };
      socket = tls.connect({ host: addr.host, port: addr.port, servername: addr.host, ...topts });
    } else {
      socket = net.connect({ host: addr.host, port: addr.port });
    }
    scope.add(socket);
    const parser = new Parser();
    let timer: NodeJS.Timeout | null = null;
    let settled = false;
    let authPending = o.password !== undefined;
    const finish = (value: unknown, error?: unknown): void => {
      if (settled) return;
      settled = true;
      if (timer !== null) clearTimeout(timer);
      scope.remove(socket);
      socket.destroy();
      if (error !== undefined) reject(error);
      else resolve(value);
    };
    const arm = (ms: number): void => {
      if (timer !== null) clearTimeout(timer);
      timer = ms > 0 ? setTimeout(() => finish(undefined), ms) : null;
    };
    arm(connectMs);
    socket.on('error', () => finish(undefined));
    socket.on('close', () => finish(undefined));
    socket.once(o.tls ? 'secureConnect' : 'connect', () => {
      arm(waitMs);
      let out = '';
      if (o.password !== undefined) out += frame(o.username !== undefined ? ['AUTH', o.username, o.password] : ['AUTH', o.password]);
      out += frame(args);
      socket.write(out);
    });
    socket.on('data', (d: Buffer) => {
      parser.feed(d, d.length);
      for (;;) {
        let v: unknown;
        try {
          v = parser.next();
        } catch {
          return finish(undefined);
        }
        if (v === NEED_MORE) return;
        if (authPending) {
          authPending = false;
          if (v instanceof RuvioError) return finish(undefined, v);
          continue;
        }
        return finish(v instanceof RuvioError ? undefined : v);
      }
    });
  });
}

function frame(args: string[]): string {
  let s = '*' + args.length + '\r\n';
  for (const a of args) s += '$' + Buffer.byteLength(a) + '\r\n' + a + '\r\n';
  return s;
}

type Role = { master: true } | { master: false; hint: Addr | null };

/** `ROLE` reply: first element `master`, or `slave` with the primary's host and port (an integer; a string is accepted). */
function readRole(v: unknown): Role | null {
  if (!Array.isArray(v) || v.length === 0) return null;
  if (v[0] === 'master') return { master: true };
  if (v[0] !== 'slave') return null;
  const host = v[1];
  const raw = v[2];
  const port = typeof raw === 'number' ? raw : typeof raw === 'string' && /^\d{1,5}$/.test(raw) ? Number(raw) : 0;
  if (typeof host !== 'string' || host === '' || !Number.isInteger(port) || port < 1 || port > 65535) return { master: false, hint: null };
  return { master: false, hint: { host, port } };
}

async function askRole(addr: Addr, o: ConnectionOptions, scope: ProbeScope): Promise<Role | null> {
  const reply = await ask(addr, o, ['ROLE'], scope);
  return reply === undefined ? null : readRole(reply);
}

/** The address `SENTINEL get-master-addr-by-name` names, when the coordinator answers one. */
async function askSentinel(addr: Addr, name: string, o: ConnectionOptions, scope: ProbeScope): Promise<Addr | null> {
  const reply = await ask(addr, o, ['SENTINEL', 'get-master-addr-by-name', name], scope);
  if (!Array.isArray(reply) || reply.length < 2) return null;
  const host = reply[0];
  const raw = reply[1];
  const port = typeof raw === 'number' ? raw : typeof raw === 'string' && /^\d{1,5}$/.test(raw) ? Number(raw) : 0;
  if (typeof host !== 'string' || host === '' || !Number.isInteger(port) || port < 1 || port > 65535) return null;
  return { host, port };
}

/**
 * @internal One round: the address that answers `ROLE` with `master`, asking `seeds` in order from
 * `start` (wrapping), then the primary a replica reports, once per address. With `sentinel` the
 * seeds are coordinators: each names a primary and the first that `ROLE` confirms wins. `avoid` is
 * never taken (the node that just answered `READONLY`). Resolves `null` when no address is
 * confirmed, and rejects only when `AUTH` is refused.
 */
export async function findPrimary(
  seeds: readonly Addr[],
  start: number,
  avoid: Addr | null,
  o: ConnectionOptions,
  sentinel: string | undefined,
  scope: ProbeScope,
): Promise<Addr | null> {
  const queue: Addr[] = [];
  for (let k = 0; k < seeds.length; k++) queue.push(seeds[(start + k) % seeds.length]!);
  if (sentinel !== undefined) {
    for (const coordinator of queue) {
      if (scope.dead) return null;
      const named = await askSentinel(coordinator, sentinel, o, scope);
      if (named === null || (avoid !== null && same(named, avoid))) continue;
      const role = await askRole(named, o, scope);
      if (role !== null && role.master) return named;
    }
    return null;
  }
  const seen: Addr[] = queue.slice();
  for (let i = 0; i < queue.length; i++) {
    if (scope.dead) return null;
    const addr = queue[i]!;
    const role = await askRole(addr, o, scope);
    if (role === null) continue;
    if (role.master) {
      if (avoid === null || !same(addr, avoid)) return addr;
    } else if (role.hint !== null && !seen.some((x) => same(x, role.hint!))) {
      seen.push(role.hint);
      queue.push(role.hint);
    }
  }
  return null;
}
