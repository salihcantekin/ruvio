// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

// Package ruvio is a fast, allocation-light client for the Ruvio server. It speaks
// RESP2 and, on request, RESP3 (HELLO 3), uses only the standard library and needs
// Go 1.22 or later.
//
// # Getting started
//
// Create one Client for the whole program and share it between goroutines. It
// connects lazily; Connect connects and checks the server at once.
//
//	c, err := ruvio.Connect(ctx, ruvio.Options{Addr: "127.0.0.1:6379"})
//	if err != nil {
//	    return err
//	}
//	defer c.Close()
//
//	if err := c.Set(ctx, "greeting", "hello"); err != nil {
//	    return err
//	}
//	v, ok, err := c.Get(ctx, "greeting") // "hello", true, nil
//
// Every method takes a context.Context first. A cancelled or expired context stops
// the wait and returns ctx.Err(), but a command that was already written may still run
// on the server.
//
// # Results and errors
//
// Typed methods return plain Go values. A reply that can be absent returns an extra
// ok bool (Get, LPop, ZScore ...) or nullable elements (NullString, NullFloat,
// NullInt); a missing key is not an error. Counting commands return 0 for a missing
// key. Each method documents what a missing key gives.
//
// Errors come in four kinds:
//
//   - *Error is a reply of the server (WRONGTYPE, ERR ..., NOSCRIPT, BUSYKEY ...).
//     Use errors.As and Error.Prefix to branch on the first word.
//   - ErrClosed (also matched with errors.Is when a connection failed and the cause
//     is wrapped) means the client was closed or the connection broke. The command
//     that finds a dropped connection fails and is never sent twice; the next
//     command opens a new connection.
//   - ErrCrossSlot is returned, without sending anything, when keys of one command
//     or transaction do not share a hash slot in cluster mode.
//   - ctx.Err() for a cancelled or timed-out context, and *UnexpectedReplyError when
//     a reply has a shape the method does not expect.
//
// The helper errors ErrTxAborted (a WATCHed key changed), ErrNotAcquired (a lock,
// lease or permit is held elsewhere), ErrIdemPending and ErrIdemConflict belong to
// Tx, the With... helpers and Idempotent.
//
// # Connections
//
// One Client multiplexes every concurrent call over a single TCP connection: frames
// queued while a write is in flight are coalesced into one write and replies are
// matched to callers in order, so many goroutines share it without a pool.
// Blocking commands (BLPop, XRead with Block, Wait ...), transactions and Pub/Sub
// use dedicated connections, so they never stall the shared one. Use Client.Conn
// to run several commands on one connection (SELECT, WATCH, CLIENT TRACKING).
//
// # Pipelines and transactions
//
// Client.Pipeline and Client.Pipelined send many commands in one write and return the
// replies in order. Client.Transaction and Client.Tx run MULTI/EXEC, with WATCH for
// optimistic locking.
//
// # Pub/Sub
//
// Client.PubSub opens a subscription on its own connection and delivers messages on
// a Go channel.
//
// # Seed addresses
//
// Options.Seeds lists host:port addresses to use in place of Options.Addr. Connecting
// tries them in order and skips one that cannot be reached; after a dropped connection
// the next command's reconnect starts with the seed after the one that dropped. In cluster
// mode the seeds are also asked for the slot map when no known node answers.
//
// # Cluster mode
//
// Set Options.Cluster to give the client the address of any shard. It learns every
// shard from CLUSTER SLOTS, opens a connection to each lazily and sends each key to the
// shard that owns its hash slot (see KeySlot), so no request pays a forwarding hop.
// A single-shard server answers "cluster mode is disabled" and the client then
// behaves exactly as without the option.
//
// A Ruvio server with several shards (shards > 1, port_mode "per-shard") listens on
// one port per shard and forwards a key sent to the wrong shard instead of answering
// MOVED. A client without Options.Cluster works against any one port, with the hop,
// but keyless commands then see only that shard: DBSIZE, KEYS, SCAN and FLUSHDB/FLUSHALL
// affect the shard that answered. With Options.Cluster the client fans these out to
// every shard (DBSIZE adds, KEYS joins, SCAN walks the shards one after the other).
// The cluster client also:
//
//   - splits MGET, MSET, DEL, UNLINK, EXISTS and TOUCH across slots and merges the
//     replies; the pieces are sent separately, so the call is not atomic;
//   - fails every other multi-key command whose keys span slots (RENAME, COPY, LMOVE,
//     SMOVE, SINTERSTORE, BITOP, EVAL ...) with ErrCrossSlot before sending anything;
//   - follows a MOVED reply at most three times, and does not follow ASK;
//   - groups a pipeline per shard and returns the replies in the original order; a
//     non-splittable command with keys in several slots makes the whole pipeline fail
//     with ErrCrossSlot, and a transaction must stay in one slot (use hash tags such as
//     "{user:1}:cart", or Client.TxFor and Client.ConnFor).
//
// Only database 0 exists on a sharded server.
//
// # Helpers
//
// Beyond the commands, the client offers higher level helpers built on Ruvio
// commands: AcquireLock and WithLock (distributed lock), AcquireLease and WithLease
// (fenced lease), AcquirePermit and WithPermit (semaphore), RateLimit, GetOrCreate and
// GetOrCreateJSON (cache-aside with stampede protection), Idempotent (request
// deduplication), ScanIter and friends (cursor iterators), and PushRecent, Recent,
// SeenBefore, MaybeSeen, JSONSetValue and JSONGetValue (typed shortcuts).
//
// # Sending any command
//
// Client.Do sends any command and returns the raw Value, for commands without a typed
// method. Value converts to Go types with Str, AsInt64, AsFloat64 and Strings.
package ruvio
