// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"time"
)

// Del removes keys of any type and returns how many existed (DEL). Missing keys are
// ignored, so 0 means nothing was deleted.
//
// In cluster mode keys in different slots are deleted with one command per slot and the
// counts are added; the call is not atomic. Use hash tags to keep related keys on one
// shard.
//
//	n, err := c.Del(ctx, "session:1", "session:2")
func (c *cmds) Del(ctx context.Context, keys ...string) (int64, error) {
	return c.intR(ctx, newCmd(1+len(keys), cDEL).ss(keys))
}

// Unlink removes keys and returns how many existed (UNLINK). Ruvio frees memory
// synchronously, so it behaves exactly like Del. Split across slots in cluster mode like
// Del.
func (c *cmds) Unlink(ctx context.Context, keys ...string) (int64, error) {
	return c.intR(ctx, newCmd(1+len(keys), cUNLINK).ss(keys))
}

// Exists returns how many of keys exist (EXISTS). A key listed twice counts twice. Split
// across slots in cluster mode and summed. Use Exists with one key to test for presence.
func (c *cmds) Exists(ctx context.Context, keys ...string) (int64, error) {
	return c.intR(ctx, newCmd(1+len(keys), cEXISTS).ss(keys))
}

// Touch returns how many of keys exist (TOUCH). Split across slots in cluster mode and
// summed.
func (c *cmds) Touch(ctx context.Context, keys ...string) (int64, error) {
	return c.intR(ctx, newCmd(1+len(keys), cTOUCH).ss(keys))
}

// Type returns the type name of the value at key (TYPE): "string", "list", "set",
// "zset", "hash", "stream", "json", or one of the Ruvio types "bloomfilter", "lease",
// "limit", "semaphore" and "once". It returns "none" for a missing key.
func (c *cmds) Type(ctx context.Context, key string) (string, error) {
	return c.strR(ctx, newCmd(2, cTYPE).s(key))
}

// Rename renames key to newKey, replacing newKey if it exists (RENAME). It returns a
// *Error "ERR no such key" when key does not exist; renaming a key to itself succeeds.
// In cluster mode both keys must hash to the same slot (use a hash tag), otherwise it
// fails with ErrCrossSlot and nothing is sent.
func (c *cmds) Rename(ctx context.Context, key, newKey string) error {
	return c.ok(ctx, newCmd(3, cRENAME).s(key).s(newKey))
}

// RenameNX renames key to newKey only when newKey does not exist (RENAMENX). It returns
// true when renamed and false when newKey already exists. A missing key returns "ERR no
// such key". Both keys must share a slot in cluster mode (ErrCrossSlot otherwise).
func (c *cmds) RenameNX(ctx context.Context, key, newKey string) (bool, error) {
	return c.boolR(ctx, newCmd(3, cRENAMENX).s(key).s(newKey))
}

// Copy copies the value and TTL of src to dst (COPY) and reports whether it copied. It
// returns false when src does not exist, or when dst exists and replace is false; with
// replace it overwrites dst. Copying a key onto itself returns the error "source and
// destination objects are the same". The copy stays in the selected database (there is
// no DB option). JSON documents are copied with their member order; hashes with field
// TTLs and some Ruvio types (streams, bloom filters, leases, counters) are refused with
// a server error. Both keys must share a slot in cluster mode (ErrCrossSlot otherwise).
func (c *cmds) Copy(ctx context.Context, src, dst string, replace bool) (bool, error) {
	if replace {
		return c.boolR(ctx, newCmd(4, cCOPY).s(src).s(dst).t(tREPLACE))
	}
	return c.boolR(ctx, newCmd(3, cCOPY).s(src).s(dst))
}

// ScanOptions filters the pages of SCAN, HSCAN, SSCAN and ZSCAN. The zero value asks for
// the server default page.
type ScanOptions struct {
	// Match is a glob pattern (* ? [abc]) applied to keys, fields or members.
	// Empty means no filter.
	Match string
	// Count is the page size hint (the server default is 10). Zero or less sends
	// no COUNT.
	Count int64
	// Type keeps only keys of this type (SCAN only; names as returned by Type; use the ScanType
	// constants such as ScanHash).
	Type ScanType
}

func (o *ScanOptions) n() int {
	n := 0
	if o.Match != "" {
		n += 2
	}
	if o.Count > 0 {
		n += 2
	}
	if o.Type != "" {
		n += 2
	}
	return n
}

func (o *ScanOptions) append(r *request) {
	if o.Match != "" {
		r.t(tMATCH).s(o.Match)
	}
	if o.Count > 0 {
		r.t(tCOUNT).i(o.Count)
	}
	if o.Type != "" {
		r.t(tTYPE).kw(o.Type.token(), string(o.Type))
	}
}

func scanReply(v Value) (string, []string, error) {
	if !v.isAgg() || len(v.Array) != 2 {
		return "", nil, unexpected(v)
	}
	items, err := v.Array[1].Strings()
	return v.Array[0].Str(), items, err
}

// Scan returns one page of keys and the cursor for the next page (SCAN). Start with
// cursor "0"; pass the returned cursor back until it is "0" again, which means the walk
// is finished. The page may be empty while the returned cursor is not "0".
//
// The page size is a hint (o.Count, server default 10) bounding the work per call, so a
// page can hold fewer keys. A key that exists for the whole walk is returned at least
// once, but a key can be returned twice if the table was rebuilt during the walk; do not
// assume uniqueness. o.Match is a glob pattern (* ? [abc]) applied to keys after they
// are read. o.Type keeps one type, using the names returned by Type; an unknown type
// name is "ERR syntax error". The server scans only the selected database, and only the
// shard that answered. In cluster mode the client walks every node, one after the other,
// and folds the node into the cursor, so the cursor is only valid with the same client.
//
// Prefer ScanIter, which loops for you.
//
//	cursor := "0"
//	for {
//	    var keys []string
//	    var err error
//	    cursor, keys, err = c.Scan(ctx, cursor, ruvio.ScanOptions{Match: "user:*", Count: 100})
//	    if err != nil {
//	        return err
//	    }
//	    process(keys)
//	    if cursor == "0" {
//	        break
//	    }
//	}
func (c *cmds) Scan(ctx context.Context, cursor string, o ScanOptions) (string, []string, error) {
	r := newCmd(2+o.n(), cSCAN).s(cursor)
	o.append(r)
	v, err := c.value(ctx, r)
	if err != nil {
		return "", nil, err
	}
	return scanReply(v)
}

// Keys returns all keys matching a glob pattern (KEYS; * ? [abc]). The server refuses it
// with "ERR KEYS is limited to 100000 keys per database, use SCAN" when the database
// holds more than 100000 keys, so use Scan or ScanIter for anything but small databases.
// In cluster mode it runs on every node and the results are joined.
func (c *cmds) Keys(ctx context.Context, pattern string) ([]string, error) {
	return c.strs(ctx, newCmd(2, cKEYS).s(pattern))
}

// RandomKey returns a random key of the selected database (RANDOMKEY). ok is false on an
// empty database. It goes to a single node in cluster mode.
func (c *cmds) RandomKey(ctx context.Context) (string, bool, error) {
	return c.strNil(ctx, newCmd(1, cRANDOMKEY))
}

// DBSize returns the number of keys in the selected database (DBSIZE), counting keys
// past their TTL that have not been reclaimed yet. The server counts only the shard that
// answered; in cluster mode the client asks every node and adds the counts.
func (c *cmds) DBSize(ctx context.Context) (int64, error) {
	return c.intR(ctx, newCmd(1, cDBSIZE))
}

// Select switches the logical database (SELECT db) for this connection. db must be in 0
// to databases-1 (16 by default), otherwise the server returns "ERR DB index is out of
// range". A sharded server (shards > 1) only has database 0.
//
// On a Client this changes the one shared connection for every caller, drops the idle
// dedicated connections and is applied again after a reconnect. Set Options.DB instead,
// or take a dedicated Conn, so the change cannot surprise other goroutines. On a Conn it
// affects only that connection, which is closed rather than reused when released.
func (c *cmds) Select(ctx context.Context, db int) error {
	err := c.ok(ctx, newCmd(2, cSELECT).i(int64(db)))
	if err == nil {
		c.ex.selected(int64(db))
	}
	return err
}

// FlushDB deletes every key of the selected database (FLUSHDB; synchronous, logged as
// one record). With a per-shard multi-shard server it clears only the shard that
// answered, so a plain client sees part of the data disappear; with Options.Cluster the
// client sends it to every node. Use with care.
func (c *cmds) FlushDB(ctx context.Context) error { return c.ok(ctx, newCmd(1, cFLUSHDB)) }

// FlushAll deletes every key of every database (FLUSHALL). Like FlushDB it only clears
// the shard that answered unless the client runs in cluster mode, where it is sent to
// every node.
func (c *cmds) FlushAll(ctx context.Context) error { return c.ok(ctx, newCmd(1, cFLUSHALL)) }

// SwapDB exchanges two logical databases in O(1) (SWAPDB a b); connections on either see
// the other's keys. Indexes outside 0..databases-1 are an error, and the command is
// refused on a sharded server.
func (c *cmds) SwapDB(ctx context.Context, a, b int) error {
	return c.ok(ctx, newCmd(3, cSWAPDB).i(int64(a)).i(int64(b)))
}

// Move moves key to database db of the same server (MOVE). It returns true when moved
// and false when the key is missing or already exists in db. Moving to the current
// database is an error. Refused on a sharded server.
func (c *cmds) Move(ctx context.Context, key string, db int) (bool, error) {
	return c.boolR(ctx, newCmd(3, cMOVE).s(key).i(int64(db)))
}

// Expire sets a TTL on key (EXPIRE). The duration is truncated to whole seconds. It
// returns true when the TTL was set and false when the key does not exist. A TTL of zero
// or less deletes the key and returns true. Use PExpire for sub-second precision. For the
// NX/XX/GT/LT conditions use ExpireIf.
func (c *cmds) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return c.boolR(ctx, newCmd(3, cEXPIRE).s(key).i(int64(ttl/time.Second)))
}

// PExpire sets a TTL on key in milliseconds (PEXPIRE), rounding a positive
// sub-millisecond duration up to 1 ms. Returns true when set and false when the key does
// not exist. A TTL of zero or less deletes the key and returns true.
func (c *cmds) PExpire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return c.boolR(ctx, newCmd(3, cPEXPIRE).s(key).i(ms(ttl)))
}

// ExpireAt sets an absolute deadline on key, in whole Unix seconds (EXPIREAT). Returns
// true when set and false when the key does not exist. A time in the past deletes the
// key and returns true.
func (c *cmds) ExpireAt(ctx context.Context, key string, at time.Time) (bool, error) {
	return c.boolR(ctx, newCmd(3, cEXPIREAT).s(key).i(at.Unix()))
}

// PExpireAt sets an absolute deadline in Unix milliseconds (PEXPIREAT). Returns true
// when set and false when the key does not exist. A time in the past deletes the key.
func (c *cmds) PExpireAt(ctx context.Context, key string, at time.Time) (bool, error) {
	return c.boolR(ctx, newCmd(3, cPEXPIREAT).s(key).i(at.UnixMilli()))
}

// ExpireIf is Expire with a condition (EXPIRE key seconds NX|XX|GT|LT). A key without a
// deadline counts as having an infinite one, so GT never applies to it and LT always
// does. It returns false when the key does not exist or the condition is not met, and
// nothing changes. An empty cond sends a plain EXPIRE.
func (c *cmds) ExpireIf(ctx context.Context, key string, ttl time.Duration, cond ExpireCondition) (bool, error) {
	return c.boolR(ctx, cond.append(newCmd(3+cond.n(), cEXPIRE).s(key).i(int64(ttl/time.Second))))
}

// PExpireIf is PExpire with a condition (PEXPIRE key ms NX|XX|GT|LT); the condition rules
// are those of ExpireIf.
func (c *cmds) PExpireIf(ctx context.Context, key string, ttl time.Duration, cond ExpireCondition) (bool, error) {
	return c.boolR(ctx, cond.append(newCmd(3+cond.n(), cPEXPIRE).s(key).i(ms(ttl))))
}

// ExpireAtIf is ExpireAt with a condition (EXPIREAT key unix-seconds NX|XX|GT|LT); the
// condition rules are those of ExpireIf.
func (c *cmds) ExpireAtIf(ctx context.Context, key string, at time.Time, cond ExpireCondition) (bool, error) {
	return c.boolR(ctx, cond.append(newCmd(3+cond.n(), cEXPIREAT).s(key).i(at.Unix())))
}

// PExpireAtIf is PExpireAt with a condition (PEXPIREAT key unix-ms NX|XX|GT|LT); the
// condition rules are those of ExpireIf.
func (c *cmds) PExpireAtIf(ctx context.Context, key string, at time.Time, cond ExpireCondition) (bool, error) {
	return c.boolR(ctx, cond.append(newCmd(3+cond.n(), cPEXPIREAT).s(key).i(at.UnixMilli())))
}

// TTL returns the remaining life of key in seconds (TTL): -1 when the key exists without
// a TTL and -2 when it does not exist.
func (c *cmds) TTL(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(2, cTTL).s(key))
}

// PTTL returns the remaining life of key in milliseconds (PTTL): -1 when the key has no
// TTL and -2 when it does not exist.
func (c *cmds) PTTL(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(2, cPTTL).s(key))
}

// ExpireTime returns the absolute expiry of key as Unix seconds (EXPIRETIME): -1 when
// there is no TTL and -2 when the key does not exist.
func (c *cmds) ExpireTime(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(2, cEXPIRETIME).s(key))
}

// PExpireTime returns the absolute expiry of key as Unix milliseconds (PEXPIRETIME): -1
// when there is no TTL and -2 when the key does not exist.
func (c *cmds) PExpireTime(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(2, cPEXPIRETIME).s(key))
}

// Persist removes the TTL of key (PERSIST). It returns true when a TTL was removed and
// false when the key is missing or had none.
func (c *cmds) Persist(ctx context.Context, key string) (bool, error) {
	return c.boolR(ctx, newCmd(2, cPERSIST).s(key))
}

// ObjectEncoding returns the encoding name Redis would report for key (OBJECT ENCODING),
// such as "int", "embstr" or "raw" for strings. It describes the logical shape, not
// Ruvio's internal layout. ok is false when the key does not exist.
func (c *cmds) ObjectEncoding(ctx context.Context, key string) (string, bool, error) {
	return c.strNil(ctx, newCmd(3, cOBJECT).t(tENCODING).s(key))
}

// ObjectIdleTime returns seconds since key was last accessed (OBJECT IDLETIME). The
// server only tracks this under an LRU maxmemory-policy; otherwise it returns an error.
// A missing key gives an *UnexpectedReplyError (null reply).
func (c *cmds) ObjectIdleTime(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(3, cOBJECT).t(tIDLETIME).s(key))
}

// ObjectFreq returns the access frequency counter of key (OBJECT FREQ). Only available
// under the allkeys-lfu policy, otherwise the server returns an error. A missing key
// gives an *UnexpectedReplyError (null reply).
func (c *cmds) ObjectFreq(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(3, cOBJECT).t(tFREQ).s(key))
}

// MemoryUsage returns the bytes used by key including bookkeeping overhead (MEMORY
// USAGE). ok is false when the key does not exist. samples > 0 sends SAMPLES but the
// server ignores it: the size is always exact.
func (c *cmds) MemoryUsage(ctx context.Context, key string, samples int64) (int64, bool, error) {
	if samples > 0 {
		return c.intNil(ctx, newCmd(5, cMEMORY).t(tUSAGE).s(key).t(tSAMPLES).i(samples))
	}
	return c.intNil(ctx, newCmd(3, cMEMORY).t(tUSAGE).s(key))
}

// Dump serializes the value and type of key into an opaque payload (DUMP) that Restore
// can load, here or on another Ruvio server. The format is Ruvio's own, not Redis RDB,
// so payloads do not move to or from Redis. ok is false when the key does not exist.
// Some types cannot be dumped (streams, bloom filters, leases, counters, hashes with
// field TTLs) and return a server error.
func (c *cmds) Dump(ctx context.Context, key string) ([]byte, bool, error) {
	return c.bytesNil(ctx, newCmd(2, cDUMP).s(key))
}

// RestoreOptions are the RESTORE modifiers.
type RestoreOptions struct {
	// Replace overwrites an existing key instead of failing with BUSYKEY.
	Replace bool
	// AbsTTL makes the ttlMs argument an absolute Unix-millisecond deadline.
	AbsTTL bool
}

// Restore recreates key from a Dump payload (RESTORE key ttl payload [REPLACE]
// [ABSTTL]).
//
// ttlMs is 0 for no expiry, otherwise a relative TTL in milliseconds, or an absolute
// Unix-millisecond deadline when o.AbsTTL is set. A deadline already in the past leaves
// no key (an existing one is deleted when o.Replace is set). If key exists and o.Replace
// is false the server returns "BUSYKEY Target key name already exists.". A negative
// ttlMs is "ERR Invalid TTL value, must be >= 0"; a corrupt or foreign payload is a
// server error.
func (c *cmds) Restore(ctx context.Context, key string, ttlMs int64, payload []byte, o RestoreOptions) error {
	n := 4
	if o.Replace {
		n++
	}
	if o.AbsTTL {
		n++
	}
	r := newCmd(n, cRESTORE).s(key).i(ttlMs).b(payload)
	if o.Replace {
		r.t(tREPLACE)
	}
	if o.AbsTTL {
		r.t(tABSTTL)
	}
	return c.ok(ctx, r)
}

// ---- connection --------------------------------------------------------------

// Ping sends PING and returns nil when the server answers PONG. It connects first if the
// client is not connected yet, which makes it the usual way to check a client eagerly
// (Connect does this for you). In cluster mode it goes to the seed node.
func (c *cmds) Ping(ctx context.Context) error { return c.ok(ctx, newCmd(1, cPING)) }

// Echo returns message unchanged (ECHO), a round trip test.
func (c *cmds) Echo(ctx context.Context, message string) (string, error) {
	return c.strR(ctx, newCmd(2, cECHO).s(message))
}

// Hello negotiates the protocol with HELLO protocol (2 or 3) and returns the server's
// property map (server, version, proto, id, mode, role ...). Any other version is
// rejected with a NOPROTO error and nothing changes. The Go parser understands both
// protocols, so replies keep working after the switch.
//
// On a Client this changes the shared connection for all callers and is not repeated
// after a reconnect; use Options.Protocol (which sends HELLO 3 on every new connection)
// to make RESP3 permanent. RESP3 is needed for ClientTracking.
func (c *cmds) Hello(ctx context.Context, protocol int) (map[string]Value, error) {
	v, err := c.value(ctx, newCmd(2, cHELLO).i(int64(protocol)))
	if err != nil {
		return nil, err
	}
	return toValueMap(v)
}

// HelloAuth is HELLO protocol AUTH username password: it switches the protocol and
// authenticates in one step. A bad password returns a WRONGPASS error and leaves the
// previous protocol and name in place. Options.Username and Options.Password do this on
// every connection for you.
func (c *cmds) HelloAuth(ctx context.Context, protocol int, username, password string) (map[string]Value, error) {
	v, err := c.value(ctx, newCmd(5, cHELLO).i(int64(protocol)).t(tAUTH).s(username).s(password))
	if err != nil {
		return nil, err
	}
	return toValueMap(v)
}

func toValueMap(v Value) (map[string]Value, error) {
	if !v.isAgg() || len(v.Array)%2 != 0 {
		return nil, unexpected(v)
	}
	m := make(map[string]Value, len(v.Array)/2)
	for i := 0; i < len(v.Array); i += 2 {
		m[v.Array[i].Str()] = v.Array[i+1]
	}
	return m, nil
}

// Auth authenticates the connection (AUTH [username] password). An empty username sends
// the one-argument form for the default user. It returns a WRONGPASS error for a bad
// pair and "ERR AUTH <password> called without any password configured" when the server
// has no password. On a Client it authenticates only the connection that sent it and is
// not repeated after a reconnect; use Options.Username and Options.Password instead.
func (c *cmds) Auth(ctx context.Context, username, password string) error {
	if username == "" {
		return c.ok(ctx, newCmd(2, cAUTH).s(password))
	}
	return c.ok(ctx, newCmd(3, cAUTH).s(username).s(password))
}

// ClientID returns the id of the connection that answered (CLIENT ID).
func (c *cmds) ClientID(ctx context.Context) (int64, error) {
	return c.intR(ctx, newCmd(2, cCLIENT).t(tID))
}

// ClientList returns the CLIENT LIST text, one line per connection with key=value
// fields. Pass ids (each greater than 0) to list only those connections. With a
// per-shard server it lists the connections of the shard that answered.
func (c *cmds) ClientList(ctx context.Context, ids ...int64) (string, error) {
	if len(ids) == 0 {
		return c.strR(ctx, newCmd(2, cCLIENT).t(tLIST))
	}
	r := newCmd(3+len(ids), cCLIENT).t(tLIST).t(tID)
	for _, id := range ids {
		r.i(id)
	}
	return c.strR(ctx, r)
}

// ClientListType returns the CLIENT LIST text for one connection type (CLIENT LIST TYPE
// type): ClientNormal, ClientMaster, ClientReplica or ClientPubSub.
//
//	text, err := c.ClientListType(ctx, ruvio.ClientNormal)
func (c *cmds) ClientListType(ctx context.Context, typ ClientType) (string, error) {
	return c.strR(ctx, newCmd(4, cCLIENT).t(tLIST).t(tTYPE).kw(typ.token(), string(typ)))
}

// ClientInfo returns the CLIENT LIST line of the connection that sent the command
// (CLIENT INFO).
func (c *cmds) ClientInfo(ctx context.Context) (string, error) {
	return c.strR(ctx, newCmd(2, cCLIENT).t(tINFO))
}

// ClientKillOptions selects the connections for ClientKill. At least one filter must be
// set. A zero ID and an empty Addr mean "not set".
type ClientKillOptions struct {
	ID     int64  // connection id from ClientID or ClientList; 0 = not set
	Addr   string // "ip:port" of the connection; empty = not set
	SkipMe *bool  // nil keeps the server default (spare the caller)
}

// ClientKill closes connections that match the filters and returns how many were closed
// (CLIENT KILL ID id / ADDR ip:port / SKIPME yes|no). At least one of ID and Addr (or
// SkipMe) must be set, otherwise the server answers with a syntax error. ID must be
// greater than 0. SkipMe nil keeps the server default (yes: the calling connection is
// spared); point it at false to allow killing yourself.
func (c *cmds) ClientKill(ctx context.Context, o ClientKillOptions) (int64, error) {
	n := 2
	if o.ID != 0 {
		n += 2
	}
	if o.Addr != "" {
		n += 2
	}
	if o.SkipMe != nil {
		n += 2
	}
	r := newCmd(n, cCLIENT).t(tKILL)
	if o.ID != 0 {
		r.t(tID).i(o.ID)
	}
	if o.Addr != "" {
		r.t(tADDR).s(o.Addr)
	}
	if o.SkipMe != nil {
		if *o.SkipMe {
			r.t(tSKIPME).s("yes")
		} else {
			r.t(tSKIPME).s("no")
		}
	}
	return c.intR(ctx, r)
}

// ClientGetName returns the name set with ClientSetName or Options.ClientName ("" when
// unset).
func (c *cmds) ClientGetName(ctx context.Context) (string, error) {
	v, err := c.value(ctx, newCmd(2, cCLIENT).t(tGETNAME))
	return v.Str(), err
}

// ClientSetName names the connection (CLIENT SETNAME). An empty name clears it. The name
// is shown in ClientList and applies to one connection only; Options.ClientName sets it
// on every connection the client opens.
func (c *cmds) ClientSetName(ctx context.Context, name string) error {
	return c.ok(ctx, newCmd(3, cCLIENT).t(tSETNAME).s(name))
}

// ClientTracking turns client-side caching invalidation on or off for this connection
// (CLIENT TRACKING ON|OFF).
//
// ON needs RESP3 (Options.Protocol = 3) and an Options.OnPush callback to receive the
// invalidate frames. After ON the server remembers the keys this connection reads with
// GET and MGET (misses included) and pushes one invalidation when a tracked key is
// written, then forgets it. Tracking is per shard, so a cluster client only hears about
// keys on the shard it asked. The REDIRECT, BCAST, PREFIX, OPTIN, OPTOUT and NOLOOP
// options are not offered. On a Client the shared connection is tracked for every
// caller.
func (c *cmds) ClientTracking(ctx context.Context, on bool) error {
	if on {
		return c.ok(ctx, newCmd(3, cCLIENT).t(tTRACKING).t(tON))
	}
	return c.ok(ctx, newCmd(3, cCLIENT).t(tTRACKING).t(tOFF))
}

// ClientDurability holds this connection's replies until replicas replicas have on disk
// every record written before them (CLIENT DURABILITY REPLICAS n [TIMEOUT ms]). It is
// checked once per batch of replies. When timeout is not met, every reply of that batch
// is the error NOREPLICAS and the writes stay applied. A timeout of 0 or less sends no
// TIMEOUT (the server default, 5 seconds); replicas 0 turns it off. It applies to one
// connection only and is refused inside MULTI and on a server without replication;
// Options.DurabilityReplicas sets it on every connection the client opens.
func (c *cmds) ClientDurability(ctx context.Context, replicas int, timeout time.Duration) error {
	if timeout > 0 {
		return c.ok(ctx, newCmd(6, cCLIENT).t(tDURABILITY).t(tREPLICAS).i(int64(replicas)).t(tTIMEOUT).i(ms(timeout)))
	}
	return c.ok(ctx, newCmd(4, cCLIENT).t(tDURABILITY).t(tREPLICAS).i(int64(replicas)))
}

// ---- server ------------------------------------------------------------------

// Info returns the INFO text (INFO [section ...]) as "name:value" lines. With no
// sections it returns the default set. This client sends the sections as separate
// arguments; the server accepts at most one (INFO expects zero or one argument), so pass
// a single section name such as "memory" or "prometheus" ("INFO prometheus" returns
// Prometheus text). It reports the shard that answered.
func (c *cmds) Info(ctx context.Context, sections ...string) (string, error) {
	return c.strR(ctx, newCmd(1+len(sections), cINFO).ss(sections))
}

// InfoOf returns the INFO text of one typed section (INFO section), for example
// InfoMemory or InfoPrometheus. An unknown section name gives an empty reply.
//
//	text, err := c.InfoOf(ctx, ruvio.InfoKeyspace)
func (c *cmds) InfoOf(ctx context.Context, section InfoSection) (string, error) {
	return c.strR(ctx, newCmd(2, cINFO).kw(section.token(), string(section)))
}

// Wait blocks until at least replicas replicas have synced this shard's writes, or
// timeout passes, and returns how many replicas reached that point (WAIT replicas ms). A
// timeout of 0 waits until the replicas catch up or the server shuts down; only ctx
// stops it earlier. The timeout is sent in whole milliseconds (rounded up). It runs on a
// dedicated connection so the shared one is not held. In a per-shard server it also
// covers the shards this connection's writes were forwarded to.
func (c *cmds) Wait(ctx context.Context, replicas int64, timeout time.Duration) (int64, error) {
	v, err := c.ex.execBlocking(ctx, newCmd(3, cWAIT).i(replicas).i(ms(timeout)))
	if err != nil {
		return 0, err
	}
	return v.AsInt64()
}

// ConfigGet returns the settings whose names match the glob pattern (CONFIG GET pattern)
// as a name-to-value map. requirepass is redacted.
func (c *cmds) ConfigGet(ctx context.Context, pattern string) (map[string]string, error) {
	return c.strMap(ctx, newCmd(3, cCONFIG).t(tGET).s(pattern))
}

// ConfigSet changes live settings (CONFIG SET name value ...): nameValues is name1, value1,
// name2, value2 ..., with the names CONFIG GET shows. Every shard of the server applies
// the change before it answers. All pairs apply or none: an unknown or restart-only name,
// or an invalid value, returns an error and changes nothing. The change is not written to
// the config file (see ConfigRewrite) and not replicated.
func (c *cmds) ConfigSet(ctx context.Context, nameValues ...string) error {
	return c.ok(ctx, newCmd(2+len(nameValues), cCONFIG).s("SET").ss(nameValues))
}

// ConfigRewrite writes the live settings into the config file the server started from
// (CONFIG REWRITE).
func (c *cmds) ConfigRewrite(ctx context.Context) error {
	return c.ok(ctx, newCmd(2, cCONFIG).s("REWRITE"))
}

// ReplicaOf makes every shard of the server follow the primary at host:port (REPLICAOF host
// port); shard i dials port + i. Writes are refused at once. A shard continues from its own
// log when the new primary's history allows it, otherwise it takes a full copy. Lasts until
// the server restarts.
func (c *cmds) ReplicaOf(ctx context.Context, host string, port int) error {
	return c.ok(ctx, newCmd(3, cREPLICAOF).s(host).i(int64(port)))
}

// ReplicaOfNoOne promotes a replica in place (REPLICAOF NO ONE), one epoch above its old
// primary. force adds FORCE: promote even when a shard has not finished copying its
// primary, which may lose writes.
func (c *cmds) ReplicaOfNoOne(ctx context.Context, force bool) error {
	if force {
		return c.ok(ctx, newCmd(4, cREPLICAOF).s("NO").s("ONE").s("FORCE"))
	}
	return c.ok(ctx, newCmd(3, cREPLICAOF).s("NO").s("ONE"))
}

// Role returns the raw ROLE reply of the shard this connection is on: a primary answers
// ["master", sequence, []]; a replica ["slave", host, port, "connect"|"connected",
// sequence].
func (c *cmds) Role(ctx context.Context) (Value, error) {
	return c.value(ctx, newCmd(1, cROLE))
}

// Save runs a checkpoint in the server process and returns when it is written (SAVE). It
// blocks the shard while it runs.
func (c *cmds) Save(ctx context.Context) error { return c.ok(ctx, newCmd(1, cSAVE)) }

// BgSave starts a checkpoint (BGSAVE) and returns the server's status text ("Background
// saving started"). Ruvio runs it inside the process, not in a forked child.
func (c *cmds) BgSave(ctx context.Context) (string, error) { return c.strR(ctx, newCmd(1, cBGSAVE)) }

// Command returns the full COMMAND reply: one entry per command in COMMAND INFO form.
// Only useful for tooling; the reply is large.
func (c *cmds) Command(ctx context.Context) (Value, error) { return c.value(ctx, newCmd(1, cCOMMAND)) }

// CommandCount returns the number of commands the server knows (COMMAND COUNT).
func (c *cmds) CommandCount(ctx context.Context) (int64, error) {
	return c.intR(ctx, newCmd(2, cCOMMAND).t(tCOUNT))
}

// CommandInfo returns the COMMAND INFO entries (arity, flags, key positions) for the
// named commands. Unknown names give a nil entry.
func (c *cmds) CommandInfo(ctx context.Context, names ...string) (Value, error) {
	return c.value(ctx, newCmd(2+len(names), cCOMMAND).t(tINFO).ss(names))
}

// CommandDocs returns COMMAND DOCS for the named commands.
func (c *cmds) CommandDocs(ctx context.Context, names ...string) (Value, error) {
	return c.value(ctx, newCmd(2+len(names), cCOMMAND).t(tDOCS).ss(names))
}

// SlowLogGet returns the newest slow log entries (SLOWLOG GET [n]) as a raw Value, an
// array of entries. n >= 0 sends that count; n < 0 sends no count, which the server
// treats as 10. Entries are per shard.
func (c *cmds) SlowLogGet(ctx context.Context, n int64) (Value, error) {
	if n < 0 {
		return c.value(ctx, newCmd(2, cSLOWLOG).t(tGET))
	}
	return c.value(ctx, newCmd(3, cSLOWLOG).t(tGET).i(n))
}

// SlowLogLen returns how many entries the slow log holds (SLOWLOG LEN).
func (c *cmds) SlowLogLen(ctx context.Context) (int64, error) {
	return c.intR(ctx, newCmd(2, cSLOWLOG).t(tLEN))
}

// SlowLogReset clears the slow log (SLOWLOG RESET).
func (c *cmds) SlowLogReset(ctx context.Context) error {
	return c.ok(ctx, newCmd(2, cSLOWLOG).t(tRESET))
}

// LatencyLatest sends LATENCY LATEST. Ruvio keeps no latency events, so the list is
// always empty.
func (c *cmds) LatencyLatest(ctx context.Context) (Value, error) {
	return c.value(ctx, newCmd(2, cLATENCY).t(tLATEST))
}

// ---- ACL -----------------------------------------------------------------------

// ACLWhoAmI returns the name of the user this connection is authenticated as (ACL
// WHOAMI), "default" when none was chosen.
func (c *cmds) ACLWhoAmI(ctx context.Context) (string, error) {
	return c.strR(ctx, newCmd(2, cACL).t(tWHOAMI))
}

// ACLUsers lists the configured user names (ACL USERS).
func (c *cmds) ACLUsers(ctx context.Context) ([]string, error) {
	return c.strs(ctx, newCmd(2, cACL).t(tUSERS))
}

// ACLList returns one rule line per user (ACL LIST); passwords are redacted.
func (c *cmds) ACLList(ctx context.Context) ([]string, error) {
	return c.strs(ctx, newCmd(2, cACL).t(tLIST))
}

// ACLGetUser returns the flat field list of one user (ACL GETUSER): flags, passwords,
// command and key rules. A nil Value (IsNil) means the user does not exist.
func (c *cmds) ACLGetUser(ctx context.Context, user string) (Value, error) {
	return c.value(ctx, newCmd(3, cACL).t(tGETUSER).s(user))
}

// ACLCat lists the command categories, or the commands of one category when a name is
// given (ACL CAT [category]).
func (c *cmds) ACLCat(ctx context.Context, category ...string) ([]string, error) {
	return c.strs(ctx, newCmd(2+len(category), cACL).t(tCAT).ss(category))
}

// ACLSetUser creates or changes a user with rule tokens (ACL SETUSER user rule ...),
// applied one after another to the existing rules. The tokens are the ones of the
// server's security config: for example "on", "off", ">password", "#sha-hash", "~key*",
// "+get", "-@dangerous". An open connection keeps the rules it had when it
// authenticated. The user name must not be empty.
func (c *cmds) ACLSetUser(ctx context.Context, user string, rules ...string) error {
	return c.ok(ctx, newCmd(3+len(rules), cACL).t(tSETUSER).s(user).ss(rules))
}

// ACLLoad replaces the live users with the contents of the server's security.acl_file
// (ACL LOAD). It returns an error when no file is configured.
func (c *cmds) ACLLoad(ctx context.Context) error { return c.ok(ctx, newCmd(2, cACL).t(tLOAD)) }

// ACLSave writes the current users to security.acl_file (ACL SAVE), storing a hash per
// password. It returns an error when no file is configured.
func (c *cmds) ACLSave(ctx context.Context) error { return c.ok(ctx, newCmd(2, cACL).t(tSAVE)) }

// ---- cluster -------------------------------------------------------------------

// ClusterSlots returns the raw CLUSTER SLOTS reply: ranges of hash slots with the host
// and port of the shard that owns each. Against a server with one shard it returns "ERR
// cluster mode is disabled"; the cluster client (Options.Cluster) uses this to detect a
// standalone server.
func (c *cmds) ClusterSlots(ctx context.Context) (Value, error) {
	return c.value(ctx, newCmd(2, cCLUSTER).t(tSLOTS))
}

// ClusterNodes returns the CLUSTER NODES text, one line per shard. Single-shard servers
// answer "cluster mode is disabled".
func (c *cmds) ClusterNodes(ctx context.Context) (string, error) {
	return c.strR(ctx, newCmd(2, cCLUSTER).t(tNODES))
}

// ClusterShards returns the raw CLUSTER SHARDS reply. Single-shard servers answer
// "cluster mode is disabled".
func (c *cmds) ClusterShards(ctx context.Context) (Value, error) {
	return c.value(ctx, newCmd(2, cCLUSTER).t(tSHARDS))
}

// ClusterInfo returns the CLUSTER INFO text. Single-shard servers answer "cluster mode
// is disabled".
func (c *cmds) ClusterInfo(ctx context.Context) (string, error) {
	return c.strR(ctx, newCmd(2, cCLUSTER).t(tINFO))
}

// ClusterMyID returns the id of the shard that answered (CLUSTER MYID). Single-shard
// servers answer "cluster mode is disabled".
func (c *cmds) ClusterMyID(ctx context.Context) (string, error) {
	return c.strR(ctx, newCmd(2, cCLUSTER).t(tMYID))
}

// ClusterKeySlot asks the server for the hash slot of key (CLUSTER KEYSLOT). It works on
// single-shard servers too. KeySlot computes the same value locally without a round
// trip.
func (c *cmds) ClusterKeySlot(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(3, cCLUSTER).t(tKEYSLOT).s(key))
}

// ReadOnly sends READONLY. A Ruvio server answers OK and serves reads on any connection, so
// it has no effect; a cluster client with Options.ReadPolicy sends it to replicas by itself.
func (c *cmds) ReadOnly(ctx context.Context) error { return c.ok(ctx, newCmd(1, cREADONLY)) }

// ReadWrite sends READWRITE. It only returns OK; there are no replicas to read from.
func (c *cmds) ReadWrite(ctx context.Context) error { return c.ok(ctx, newCmd(1, cREADWRITE)) }

// ---- scripting ------------------------------------------------------------------

func scriptReq(name []byte, head string, keys []string, args []any) (*request, error) {
	r := newCmd(3+len(keys)+len(args), name).s(head).i(int64(len(keys))).ss(keys)
	var err error
	for _, a := range args {
		if r.buf, err = appendArg(r.buf, a); err != nil {
			putReq(r)
			return nil, err
		}
	}
	return r, nil
}

// Eval runs a Lua 5.1 script (EVAL script numkeys key... arg...) and returns its result
// as a raw Value.
//
// keys are the key names the script touches (KEYS in Lua); args are plain arguments
// (ARGV) of the types Do accepts. In cluster mode all keys must hash to the same slot,
// otherwise the call fails with ErrCrossSlot; a script with no keys runs on the seed
// node. Inside the script use ruvio.call / ruvio.pcall, and only declared keys. Script
// errors come back as *Error; limits on instructions, time and memory abort a runaway
// script (SCRIPT KILL cannot, it answers NOTBUSY). Scripts obey the ACL of the calling
// user.
//
//	v, err := c.Eval(ctx, "return redis.call('GET', KEYS[1])", []string{"k"})
func (c *cmds) Eval(ctx context.Context, script string, keys []string, args ...any) (Value, error) {
	r, err := scriptReq(cEVAL, script, keys, args)
	if err != nil {
		return Value{}, err
	}
	return c.value(ctx, r)
}

// EvalSHA runs a script already cached on the server by its SHA1 (EVALSHA sha numkeys
// key... arg...). It returns a *Error with prefix NOSCRIPT when the script is not cached
// on the shard that runs it; load it with ScriptLoad first. The script cache is per
// shard and lost on restart.
func (c *cmds) EvalSHA(ctx context.Context, sha string, keys []string, args ...any) (Value, error) {
	r, err := scriptReq(cEVALSHA, sha, keys, args)
	if err != nil {
		return Value{}, err
	}
	return c.value(ctx, r)
}

// EvalCached runs a script by its SHA1 and sends the whole script only when the shard
// does not have it: EVALSHA, then EVAL once on NOSCRIPT. The SHA1 is computed here, and
// EVAL also caches the script on that shard. The retry is safe because NOSCRIPT is
// answered before any command of the script ran, so a script never runs twice.
//
//	v, err := c.EvalCached(ctx, "return ARGV[1]", nil, "x")
func (c *cmds) EvalCached(ctx context.Context, script string, keys []string, args ...any) (Value, error) {
	sum := sha1.Sum([]byte(script))
	v, err := c.EvalSHA(ctx, hex.EncodeToString(sum[:]), keys, args...)

	var se *Error
	if errors.As(err, &se) && se.Prefix() == "NOSCRIPT" {
		return c.Eval(ctx, script, keys, args...)
	}
	return v, err
}

// ScriptLoad caches a script and returns its SHA1 (SCRIPT LOAD) without running it. In
// cluster mode it is sent to every node so EvalSHA works wherever the keys live.
func (c *cmds) ScriptLoad(ctx context.Context, script string) (string, error) {
	return c.strR(ctx, newCmd(3, cSCRIPT).t(tLOAD).s(script))
}

// ScriptExists reports, per SHA1, whether the script is cached (SCRIPT EXISTS sha ...).
// At least one SHA1 is required.
func (c *cmds) ScriptExists(ctx context.Context, shas ...string) ([]bool, error) {
	return c.bools(ctx, newCmd(2+len(shas), cSCRIPT).t(tEXISTS).ss(shas))
}

// ScriptFlush empties the script cache (SCRIPT FLUSH); sent to every node in cluster
// mode.
func (c *cmds) ScriptFlush(ctx context.Context) error {
	return c.ok(ctx, newCmd(2, cSCRIPT).t(tFLUSH))
}

// ScriptKill sends SCRIPT KILL. Ruvio stops long scripts with built-in limits instead,
// so the server answers NOTBUSY.
func (c *cmds) ScriptKill(ctx context.Context) error {
	return c.ok(ctx, newCmd(2, cSCRIPT).t(tKILL))
}

// FCall runs a function stored with FunctionLoad (FCALL name numkeys key... arg...). A
// name that was not loaded returns "ERR function not found". Key and argument rules are
// those of Eval.
func (c *cmds) FCall(ctx context.Context, function string, keys []string, args ...any) (Value, error) {
	r, err := scriptReq(cFCALL, function, keys, args)
	if err != nil {
		return Value{}, err
	}
	return c.value(ctx, r)
}

// FunctionLoad stores a named Lua function body (FUNCTION LOAD name body) and returns
// the name. Note the Ruvio form: a name and a body, not a Redis library. Run it with
// FCall. Functions are per shard and kept in memory only, so they must be loaded again
// after a restart; in cluster mode the call goes to every node.
func (c *cmds) FunctionLoad(ctx context.Context, name, body string) (string, error) {
	return c.strR(ctx, newCmd(4, cFUNCTION).t(tLOAD).s(name).s(body))
}

// FunctionList returns the names of the loaded functions (FUNCTION LIST).
func (c *cmds) FunctionList(ctx context.Context) ([]string, error) {
	return c.strs(ctx, newCmd(2, cFUNCTION).t(tLIST))
}

// FunctionDelete removes a function (FUNCTION DELETE name) and reports whether it
// existed.
func (c *cmds) FunctionDelete(ctx context.Context, name string) (bool, error) {
	return c.boolR(ctx, newCmd(3, cFUNCTION).t(tDELETE).s(name))
}

// ---- pub/sub publishing (subscriptions live on PubSub) --------------------------

// Publish sends message to every subscriber of channel, including pattern subscribers,
// and returns how many received it on the shard that took the command (PUBLISH). A
// classic PUBLISH reaches subscribers on every shard of the server process, but the
// count covers only the local shard, and delivery to other shards is best effort.
// Messages are not stored: with no subscriber they vanish. Use SPublish for sharded
// channels.
func (c *cmds) Publish(ctx context.Context, channel, message string) (int64, error) {
	return c.intR(ctx, newCmd(3, cPUBLISH).s(channel).s(message))
}

// SPublish publishes on a shard channel (SPUBLISH). It is routed to the shard owning the
// channel's hash slot and returns the number of subscribers there. Subscribe with
// PubSub.SSubscribe. Keyspace notices (if the server enables them) also arrive on shard
// channels.
func (c *cmds) SPublish(ctx context.Context, channel, message string) (int64, error) {
	return c.intR(ctx, newCmd(3, cSPUBLISH).s(channel).s(message))
}
