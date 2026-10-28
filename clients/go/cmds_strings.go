// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"fmt"
	"time"
)

// ---- strings ---------------------------------------------------------------

// Get returns the string stored at key (GET).
//
// ok is false, with an empty string and a nil error, when the key does not exist or has
// expired. A key that holds another type (list, hash, JSON ...) returns a *Error with
// prefix WRONGTYPE. A cancelled or expired ctx returns ctx.Err().
//
//	v, ok, err := c.Get(ctx, "greeting")
//	if err != nil {
//	    return err
//	}
//	if !ok {
//	    // key is missing
//	}
func (c *cmds) Get(ctx context.Context, key string) (string, bool, error) {
	return c.strNil(ctx, newCmd(2, cGET).s(key))
}

// GetBytes is Get for binary values: the returned slice is a copy owned by the caller.
// It sends GET and reports a missing key as ok == false, like Get.
func (c *cmds) GetBytes(ctx context.Context, key string) ([]byte, bool, error) {
	return c.bytesNil(ctx, newCmd(2, cGET).s(key))
}

// Set stores value at key (SET key value), replacing any value of any type and clearing
// an existing TTL. It returns nil on success. Use SetArgs to add an expiry or a NX/XX
// condition. Keys and values over the server limits (1 MiB and 16 MiB by default) are
// refused with a server error.
func (c *cmds) Set(ctx context.Context, key, value string) error {
	return c.ok(ctx, newCmd(3, cSET).s(key).s(value))
}

// SetBytes is Set for binary values (SET key value with value sent as raw bytes).
func (c *cmds) SetBytes(ctx context.Context, key string, value []byte) error {
	return c.ok(ctx, newCmd(3, cSET).s(key).b(value))
}

// SetOptions are the modifiers of SET. The zero value sends a plain SET.
//
// Expiry fields are mutually exclusive and are checked in this order: TTL, then
// ExpireAt, then KeepTTL; the first one that is set wins and the others are ignored.
type SetOptions struct {
	// TTL is a relative expiry, sent as PX in milliseconds (rounded up to at least
	// 1 ms). Zero or negative means no TTL option.
	TTL time.Duration
	// ExpireAt is an absolute expiry, sent as PXAT in Unix milliseconds. The zero
	// time means none. Ignored when TTL is set.
	ExpireAt time.Time
	// KeepTTL keeps the key's current TTL instead of clearing it (KEEPTTL).
	// Ignored when TTL or ExpireAt is set.
	KeepTTL bool
	// NX writes only if the key does not exist. It wins over XX.
	NX bool
	// XX writes only if the key already exists.
	XX bool
}

func (o *SetOptions) count() int {
	n := 0
	if o.TTL > 0 || !o.ExpireAt.IsZero() {
		n += 2
	} else if o.KeepTTL {
		n++
	}
	if o.NX || o.XX {
		n++
	}
	return n
}

func (o *SetOptions) append(r *request) {
	switch {
	case o.TTL > 0:
		r.t(tPX).i(ms(o.TTL))
	case !o.ExpireAt.IsZero():
		r.t(tPXAT).i(o.ExpireAt.UnixMilli())
	case o.KeepTTL:
		r.t(tKEEPTTL)
	}
	if o.NX {
		r.t(tNX)
	} else if o.XX {
		r.t(tXX)
	}
}

// SetArgs runs SET key value with the given options and reports whether the value was
// written.
//
// TTL > 0 sends PX with the duration in milliseconds (a sub-millisecond TTL is rounded
// up to 1 ms). A non-zero ExpireAt sends PXAT with the absolute time in Unix
// milliseconds. KeepTTL sends KEEPTTL so an existing TTL survives the overwrite. NX
// writes only when the key does not exist and XX only when it does; if both are set, NX
// wins. With no expiry option the write clears an existing TTL.
//
// It returns false, with a nil error, when NX or XX prevented the write. A TTL of zero
// or less is ignored (no expiry option is sent). An expiry the server rejects returns a
// *Error such as "ERR invalid expire time in 'set' command".
//
//	ok, err := c.SetArgs(ctx, "session:42", token, ruvio.SetOptions{TTL: 30 * time.Minute, NX: true})
func (c *cmds) SetArgs(ctx context.Context, key, value string, o SetOptions) (bool, error) {
	v, err := c.value(ctx, o.build(cSET, key, value, false))
	return err == nil && v.Kind != KindNil, err
}

func (o *SetOptions) build(name []byte, key, value string, get bool) *request {
	n := 3 + o.count()
	if get {
		n++
	}
	r := newCmd(n, name).s(key).s(value)
	o.append(r)
	if get {
		r.t(tGET)
	}
	return r
}

// SetGet runs SET key value ... GET and returns the previous value. old is "" and ok is
// false when the key did not exist. The options are those of SetArgs. The new value is
// written unless NX or XX prevented it, in which case the current value is still
// returned. A previous value of a non-string type returns WRONGTYPE and nothing is
// written.
func (c *cmds) SetGet(ctx context.Context, key, value string, o SetOptions) (string, bool, error) {
	return c.strNil(ctx, o.build(cSET, key, value, true))
}

// SetIfEq is a compare-and-set (Ruvio extension, SET key value IFEQ expected): it writes
// value only when the string at key currently equals expected, and reports whether it
// wrote.
//
// The result is false, with a nil error, when the key is missing or holds a different
// string. TTL, ExpireAt and KeepTTL in o apply to the write; NX and XX are ignored
// because IFEQ cannot be combined with them. A key that holds a non-string returns
// WRONGTYPE. Lock.Refresh uses this command.
//
//	swapped, err := c.SetIfEq(ctx, "config:version", "v2", "v1", ruvio.SetOptions{})
func (c *cmds) SetIfEq(ctx context.Context, key, value, expected string, o SetOptions) (bool, error) {
	o.NX, o.XX = false, false
	r := newCmd(5+o.count(), cSET).s(key).s(value).t(tIFEQ).s(expected)
	o.append(r)
	v, err := c.value(ctx, r)
	return err == nil && v.Kind != KindNil, err
}

// SetIfGreater writes value (Ruvio extension, SET key value IFGT) when the key is missing or
// value is greater than the stored integer, and reports whether it wrote. Use it to keep a
// watermark, a version or the highest bid.
//
// The result is false, with a nil error, when the stored number is greater or equal. TTL,
// ExpireAt and KeepTTL in o apply to the write; NX and XX are ignored because IFGT cannot be
// combined with them. A stored string that is not an integer returns an error and writes
// nothing; a key that holds another type returns WRONGTYPE.
//
//	advanced, err := c.SetIfGreater(ctx, "watermark", 1700000000, ruvio.SetOptions{})
func (c *cmds) SetIfGreater(ctx context.Context, key string, value int64, o SetOptions) (bool, error) {
	return c.setIfCompare(ctx, key, value, tIFGT, o)
}

// SetIfLess is SetIfGreater for a smaller number (SET key value IFLT): lowest price,
// earliest deadline.
func (c *cmds) SetIfLess(ctx context.Context, key string, value int64, o SetOptions) (bool, error) {
	return c.setIfCompare(ctx, key, value, tIFLT, o)
}

func (c *cmds) setIfCompare(ctx context.Context, key string, value int64, condition []byte, o SetOptions) (bool, error) {
	o.NX, o.XX = false, false
	r := newCmd(4+o.count(), cSET).s(key).i(value).t(condition)
	o.append(r)
	v, err := c.value(ctx, r)
	return err == nil && v.Kind != KindNil, err
}

// SetEX stores value with a TTL given in whole seconds (SETEX key seconds value). The
// duration is truncated to seconds, so a ttl below one second becomes 0 and the server
// returns "ERR invalid expire time in 'setex' command". Use PSetEX or SetArgs for
// sub-second TTLs.
func (c *cmds) SetEX(ctx context.Context, key, value string, ttl time.Duration) error {
	return c.ok(ctx, newCmd(4, cSETEX).s(key).i(int64(ttl/time.Second)).s(value))
}

// PSetEX stores value with a TTL in milliseconds (PSETEX key ms value). A
// sub-millisecond ttl is rounded up to 1 ms; zero or negative values are rejected by the
// server with "invalid expire time".
func (c *cmds) PSetEX(ctx context.Context, key, value string, ttl time.Duration) error {
	return c.ok(ctx, newCmd(4, cPSETEX).s(key).i(ms(ttl)).s(value))
}

// SetNX writes value only when key does not exist (SETNX). It returns true when it wrote
// and false when the key already existed (of any type).
func (c *cmds) SetNX(ctx context.Context, key, value string) (bool, error) {
	return c.boolR(ctx, newCmd(3, cSETNX).s(key).s(value))
}

// MGet returns the values of keys (MGET), in order. The result has one NullString per
// key; Valid is false for a missing key and also for a key that holds a non-string (MGET
// never returns WRONGTYPE).
//
// In cluster mode keys in different slots are fetched with one command per slot and
// merged in key order; this is not atomic. A hash tag such as "{user:1}:name" keeps keys
// on one shard.
//
//	vals, err := c.MGet(ctx, "a", "b")
//	if err == nil && vals[0].Valid {
//	    fmt.Println(vals[0].Val)
//	}
func (c *cmds) MGet(ctx context.Context, keys ...string) ([]NullString, error) {
	return c.nullStrs(ctx, newCmd(1+len(keys), cMGET).ss(keys))
}

// MSet writes pairs (MSET): keyValues is key1, value1, key2, value2 ... An odd number of
// arguments is a server error. Existing TTLs are cleared. In cluster mode pairs are sent
// per slot and the call is not atomic: a failure can leave some pairs written.
//
//	err := c.MSet(ctx, "a", "1", "b", "2")
func (c *cmds) MSet(ctx context.Context, keyValues ...string) error {
	return c.ok(ctx, newCmd(1+len(keyValues), cMSET).ss(keyValues))
}

// MSetNX writes all pairs only when none of the keys exist (MSETNX): keyValues is key1,
// value1, key2, value2 ... It returns true when every pair was written and false
// (nothing written) when at least one key already existed. The keys must share a hash
// slot in cluster mode, otherwise the call fails with ErrCrossSlot (MSETNX is not split
// across slots).
func (c *cmds) MSetNX(ctx context.Context, keyValues ...string) (bool, error) {
	return c.boolR(ctx, newCmd(1+len(keyValues), cMSETNX).ss(keyValues))
}

// GetSet sets key to value and returns the old value (GETSET). ok is false when the key
// did not exist. The TTL is cleared. A non-string key returns WRONGTYPE.
func (c *cmds) GetSet(ctx context.Context, key, value string) (string, bool, error) {
	return c.strNil(ctx, newCmd(3, cGETSET).s(key).s(value))
}

// GetDel returns the value at key and deletes the key (GETDEL). ok is false when the key
// did not exist. A non-string key returns WRONGTYPE.
func (c *cmds) GetDel(ctx context.Context, key string) (string, bool, error) {
	return c.strNil(ctx, newCmd(2, cGETDEL).s(key))
}

// GetEx returns the string at key and sets its expiry to now + ttl (Ruvio GETEX key ms).
// ok is false, and nothing changes, when the key does not exist.
//
// Unlike Redis, Ruvio's GETEX takes one TTL in milliseconds and always sets it; there is
// no PERSIST form. ttl must be at least 1 ms (a sub-millisecond value is rounded up);
// zero or negative values return "GETEX TTL must be a positive integer". A non-string
// key returns WRONGTYPE. A plain Get does not touch the TTL.
func (c *cmds) GetEx(ctx context.Context, key string, ttl time.Duration) (string, bool, error) {
	return c.strNil(ctx, newCmd(3, cGETEX).s(key).i(ms(ttl)))
}

// Append appends value to the string at key and returns the new length (APPEND). A
// missing key is created, as with Set. An existing TTL is kept. A non-string key returns
// WRONGTYPE.
func (c *cmds) Append(ctx context.Context, key, value string) (int64, error) {
	return c.intR(ctx, newCmd(3, cAPPEND).s(key).s(value))
}

// StrLen returns the byte length of the string at key (STRLEN), or 0 when the key does
// not exist. A non-string key returns WRONGTYPE.
func (c *cmds) StrLen(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(2, cSTRLEN).s(key))
}

// Incr increments the integer stored at key by one (INCR) and returns the new value. A
// missing key counts as 0, so the first call returns 1. An existing TTL is kept. The
// value must be a signed 64-bit decimal integer; otherwise the server returns "ERR value
// is not an integer or out of range" (also on overflow) and the value is unchanged. A
// non-string key returns WRONGTYPE.
func (c *cmds) Incr(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(2, cINCR).s(key))
}

// Decr decrements the integer stored at key by one (DECR) and returns the new value.
// Missing key, TTL and error behaviour are those of Incr.
func (c *cmds) Decr(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(2, cDECR).s(key))
}

// IncrBy adds delta (any signed 64-bit value) to the integer stored at key (INCRBY) and
// returns the new value. Missing key, TTL and error behaviour are those of Incr; an
// overflowing result is an error and leaves the value unchanged.
func (c *cmds) IncrBy(ctx context.Context, key string, delta int64) (int64, error) {
	return c.intR(ctx, newCmd(3, cINCRBY).s(key).i(delta))
}

// IncrByMax adds delta to the integer at key unless the result would pass limit (Ruvio
// INCRBY key delta MAX limit). It is a counter with a ceiling in one atomic step.
//
// It returns the new value and ok == true when the add was applied. When the result
// would be greater than limit, nothing is written and ok is false (new is 0). Missing
// key, TTL and error behaviour are those of Incr.
//
//	n, ok, err := c.IncrByMax(ctx, "seats:left", 1, 100)
func (c *cmds) IncrByMax(ctx context.Context, key string, delta, limit int64) (int64, bool, error) {
	return c.intNil(ctx, newCmd(5, cINCRBY).s(key).i(delta).t(tMAX).i(limit))
}

// DecrBy subtracts delta from the integer stored at key (DECRBY) and returns the new
// value. Missing key, TTL and error behaviour are those of Incr.
func (c *cmds) DecrBy(ctx context.Context, key string, delta int64) (int64, error) {
	return c.intR(ctx, newCmd(3, cDECRBY).s(key).i(delta))
}

// IncrByFloat adds a floating point delta to the number stored at key (INCRBYFLOAT) and
// returns the new value. A missing key counts as 0. The sum is computed in float64 and
// stored with 16 significant digits (Redis uses long double, so the last digits can
// differ). The value must be a valid number, otherwise the server returns "ERR value is
// not a valid float"; NaN is rejected and an infinite result is an error. An existing
// TTL is kept.
func (c *cmds) IncrByFloat(ctx context.Context, key string, delta float64) (float64, error) {
	return c.floatR(ctx, newCmd(3, cINCRBYFLOAT).s(key).f(delta))
}

// ---- TTL on create ----------------------------------------------------------
//
// The WithTTL methods below send EX seconds (when ttl is a whole number of seconds) or
// PX milliseconds (otherwise). The deadline is set only when the call creates the key; an
// existing key keeps its current deadline. A ttl of zero or less returns ErrInvalidTTL
// without contacting the server.

// IncrWithTTL is Incr that also sets a deadline when it creates the key (INCR key EX|PX).
// An existing key keeps its deadline.
func (c *cmds) IncrWithTTL(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	tok, v, err := ttlArg(ttl)
	if err != nil {
		return 0, err
	}
	return c.intR(ctx, newCmd(4, cINCR).s(key).t(tok).i(v))
}

// DecrWithTTL is Decr that also sets a deadline when it creates the key (DECR key
// EX|PX). An existing key keeps its deadline.
func (c *cmds) DecrWithTTL(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	tok, v, err := ttlArg(ttl)
	if err != nil {
		return 0, err
	}
	return c.intR(ctx, newCmd(4, cDECR).s(key).t(tok).i(v))
}

// IncrByWithTTL is IncrBy that also sets a deadline when it creates the key (INCRBY key
// delta EX|PX). An existing key keeps its deadline.
func (c *cmds) IncrByWithTTL(ctx context.Context, key string, delta int64, ttl time.Duration) (int64, error) {
	tok, v, err := ttlArg(ttl)
	if err != nil {
		return 0, err
	}
	return c.intR(ctx, newCmd(5, cINCRBY).s(key).i(delta).t(tok).i(v))
}

// DecrByWithTTL is DecrBy that also sets a deadline when it creates the key (DECRBY key
// delta EX|PX). An existing key keeps its deadline.
func (c *cmds) DecrByWithTTL(ctx context.Context, key string, delta int64, ttl time.Duration) (int64, error) {
	tok, v, err := ttlArg(ttl)
	if err != nil {
		return 0, err
	}
	return c.intR(ctx, newCmd(5, cDECRBY).s(key).i(delta).t(tok).i(v))
}

// IncrByMaxWithTTL is IncrByMax that also sets a deadline when it creates the key
// (INCRBY key delta EX|PX MAX limit). The deadline is set only when the add is applied
// and creates the key; when the result would pass limit nothing is written (ok is false)
// and no key is created.
func (c *cmds) IncrByMaxWithTTL(ctx context.Context, key string, delta, limit int64, ttl time.Duration) (int64, bool, error) {
	tok, v, err := ttlArg(ttl)
	if err != nil {
		return 0, false, err
	}
	return c.intNil(ctx, newCmd(7, cINCRBY).s(key).i(delta).t(tok).i(v).t(tMAX).i(limit))
}

// IncrByFloatWithTTL is IncrByFloat that also sets a deadline when it creates the key
// (INCRBYFLOAT key delta EX|PX). An existing key keeps its deadline.
func (c *cmds) IncrByFloatWithTTL(ctx context.Context, key string, delta float64, ttl time.Duration) (float64, error) {
	tok, v, err := ttlArg(ttl)
	if err != nil {
		return 0, err
	}
	return c.floatR(ctx, newCmd(5, cINCRBYFLOAT).s(key).f(delta).t(tok).i(v))
}

// GetRange returns the substring of the string at key between the byte offsets start and
// end, both inclusive (GETRANGE). Negative offsets count from the end (-1 is the last
// byte). Offsets beyond the string are clamped. A missing key, or a range that lies
// wholly before the start of the string, returns "" with a nil error. A non-string key
// returns WRONGTYPE.
func (c *cmds) GetRange(ctx context.Context, key string, start, end int64) (string, error) {
	return c.strR(ctx, newCmd(4, cGETRANGE).s(key).i(start).i(end))
}

// Substr is the older name of GetRange (SUBSTR) and behaves identically.
func (c *cmds) Substr(ctx context.Context, key string, start, end int64) (string, error) {
	return c.strR(ctx, newCmd(4, cSUBSTR).s(key).i(start).i(end))
}

// SetRange overwrites part of the string at key, starting at the byte offset, and
// returns the new length (SETRANGE). A missing key is created; a gap between the current
// end and offset is padded with zero bytes. offset must be >= 0 (otherwise "offset is
// out of range") and the resulting string at most 16 MiB (a larger one is refused with a
// server error). An existing TTL is kept. A non-string key returns WRONGTYPE.
func (c *cmds) SetRange(ctx context.Context, key string, offset int64, value string) (int64, error) {
	return c.intR(ctx, newCmd(4, cSETRANGE).s(key).i(offset).s(value))
}

// ---- Ruvio string extensions ----------------------------------------------

// SetV writes a versioned string (Ruvio SETV): an optimistic-concurrency write where the
// stored version must match what the caller last saw. It returns the new version.
//
// Pass version 0 (or less) to write without a version check: the call creates the key or
// converts a plain string, and the new version is 1. Pass the current version to update
// a versioned string; the new version is version + 1. If the key is missing, is not
// versioned or holds a different version, the server returns "ERR version mismatch" (a
// *Error) and nothing is written. A key of another type returns WRONGTYPE. A versioned
// string cannot be used with Incr and friends.
//
//	ver, err := c.SetV(ctx, "doc:1", "first", 0)  // ver == 1
//	ver, err = c.SetV(ctx, "doc:1", "second", ver) // ver == 2
func (c *cmds) SetV(ctx context.Context, key, value string, version int64) (int64, error) {
	if version <= 0 {
		return c.intR(ctx, newCmd(3, cSETV).s(key).s(value))
	}
	return c.intR(ctx, newCmd(4, cSETV).s(key).s(value).i(version))
}

// Take subtracts count from the integer at key only if the key holds at least that much
// (Ruvio TAKE key count); use it for tokens, stock or quotas where the value must never
// go negative.
//
// It returns the value left after the subtraction and ok == true on success. When the
// key is missing or holds fewer than count, nothing changes and ok is false (remaining
// is 0). count must be > 0 ("TAKE count must be a positive integer" otherwise). A value
// that is not an integer returns "ERR value is not an integer or out of range".
func (c *cmds) Take(ctx context.Context, key string, count int64) (remaining int64, ok bool, err error) {
	return c.intNil(ctx, newCmd(3, cTAKE).s(key).i(count))
}

// Transfer moves amount from the integer at source to the integer at destination in
// one step, only when source holds at least that much (Ruvio TRANSFER source destination
// amount). A missing destination starts at 0, and both keys are written as one WAL record.
//
// It returns the two values after the move and ok == true. When source is missing or
// holds fewer than amount, or destination would overflow, nothing changes and ok is false.
// amount must be > 0, source and destination must differ and, on a sharded server, share a
// slot ("CROSSSLOT"; a hash tag such as {user:42} puts them together). A value that is not
// an integer returns "ERR value is not an integer or out of range".
func (c *cmds) Transfer(ctx context.Context, source, destination string, amount int64) (sourceAfter, destinationAfter int64, ok bool, err error) {
	return c.pairNil(ctx, newCmd(4, cTRANSFER).s(source).s(destination).i(amount))
}

// TransferMax is Transfer that also refuses (ok == false, nothing written) when the
// destination would end above max (TRANSFER source destination amount MAX max).
func (c *cmds) TransferMax(ctx context.Context, source, destination string, amount, max int64) (sourceAfter, destinationAfter int64, ok bool, err error) {
	return c.pairNil(ctx, newCmd(6, cTRANSFER).s(source).s(destination).i(amount).t(tMAX).i(max))
}

// DelIfEq deletes key only when its string equals expected (Ruvio DELEX key IFEQ
// expected) and reports whether it deleted. It returns false when the key is missing or
// holds a different value. It is the safe way to release a lock you hold. A non-string
// key returns WRONGTYPE.
func (c *cmds) DelIfEq(ctx context.Context, key, expected string) (bool, error) {
	return c.boolR(ctx, newCmd(4, cDELEX).s(key).t(tIFEQ).s(expected))
}

// Limit counts one call against a fixed-window rate limit (Ruvio LIMIT key max
// window_ms).
//
// The first call starts a window of the given length (rounded up to whole milliseconds);
// each call inside the window uses one of max slots. It returns allowed == true with
// remaining = the allowance left after this call (0 on the last permitted call). When
// the window is full, allowed is false, remaining is 0 and the counter is not increased;
// it resets when the window ends. max and window must be positive, otherwise the server
// returns "LIMIT maximum and window must be positive integers". The key must not hold
// another type (WRONGTYPE). Use RateLimit for the allowed-first return order.
func (c *cmds) Limit(ctx context.Context, key string, max int64, window time.Duration) (remaining int64, allowed bool, err error) {
	return c.intNil(ctx, newCmd(4, cLIMIT).s(key).i(max).i(ms(window)))
}

// LimitOutcome is the reply of LimitSlidingWithRetry and LimitTokensWithRetry.
type LimitOutcome struct {
	// Allowed is true when the call was let through.
	Allowed bool
	// Remaining is the allowance left after the call (0 when refused).
	Remaining int64
	// RetryAfter is the wait until the same call would pass (0 when it was let through),
	// rounded up to whole milliseconds. Fit for a Retry-After header.
	RetryAfter time.Duration
}

// LimitSliding counts one call against a sliding-window limit (Ruvio LIMIT key max
// window_ms SLIDING).
//
// The previous window counts for the part of it that still overlaps the last window, so a
// burst at a window edge is not doubled. The key keeps two counters, not one entry per
// call. It returns allowed == true with remaining = the allowance left after this call;
// when the call does not fit, allowed is false and nothing is counted. max must be from 1
// to 2147483647. A key keeps the mode it was created with: another mode on it returns a
// server error. The key must not hold another type (WRONGTYPE).
func (c *cmds) LimitSliding(ctx context.Context, key string, max int64, window time.Duration) (remaining int64, allowed bool, err error) {
	return c.intNil(ctx, newCmd(5, cLIMIT).s(key).i(max).i(ms(window)).t(tSLIDING))
}

// LimitTokens takes cost tokens from a token bucket (Ruvio LIMIT key capacity refill_ms
// TOKENS COST cost).
//
// The bucket holds capacity tokens (1 to 1000000) and refills completely over refill (up
// to 4294967295 ms). It returns allowed == true with the tokens left; when the bucket
// holds fewer than cost tokens, allowed is false and nothing is taken. The key keeps one
// timestamp and disappears when the bucket is full again. cost is from 1 to capacity. A
// key keeps the mode it was created with. The key must not hold another type (WRONGTYPE).
func (c *cmds) LimitTokens(ctx context.Context, key string, capacity int64, refill time.Duration, cost int64) (remaining int64, allowed bool, err error) {
	return c.intNil(ctx, newCmd(7, cLIMIT).s(key).i(capacity).i(ms(refill)).t(tTOKENS).t(tCOST).i(cost))
}

// LimitSlidingWithRetry is LimitSliding with WITHRETRY: the outcome also says how long to
// wait before the same call would pass.
func (c *cmds) LimitSlidingWithRetry(ctx context.Context, key string, max int64, window time.Duration) (LimitOutcome, error) {
	return c.limitOutcome(ctx, newCmd(6, cLIMIT).s(key).i(max).i(ms(window)).t(tSLIDING).t(tWITHRETRY))
}

// LimitTokensWithRetry is LimitTokens with WITHRETRY: the outcome also says how long to
// wait before the same call would pass.
func (c *cmds) LimitTokensWithRetry(ctx context.Context, key string, capacity int64, refill time.Duration, cost int64) (LimitOutcome, error) {
	return c.limitOutcome(ctx, newCmd(8, cLIMIT).s(key).i(capacity).i(ms(refill)).t(tTOKENS).t(tCOST).i(cost).t(tWITHRETRY))
}

func (c *cmds) limitOutcome(ctx context.Context, r *request) (LimitOutcome, error) {
	v, err := c.ex.exec(ctx, r)
	if err != nil {
		return LimitOutcome{}, err
	}
	if !v.isAgg() || len(v.Array) != 2 {
		return LimitOutcome{}, unexpected(v)
	}
	remaining, err := v.Array[0].AsInt64()
	if err != nil {
		return LimitOutcome{}, err
	}
	retry, err := v.Array[1].AsInt64()
	if err != nil {
		return LimitOutcome{}, err
	}
	return LimitOutcome{
		Allowed:    remaining >= 0,
		Remaining:  max(remaining, 0),
		RetryAfter: time.Duration(max(retry, 0)) * time.Millisecond,
	}, nil
}

// Once returns true for the first caller and false for every other call until ttl has
// passed (Ruvio ONCE key ttl_ms). It is a deduplication marker: the key is created for
// ttl (at least 1 ms) and then disappears. ttl <= 0 returns a server error. The key must
// not hold another type (WRONGTYPE).
func (c *cmds) Once(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return c.boolR(ctx, newCmd(3, cONCE).s(key).i(ms(ttl)))
}

// Lease acquires a fenced lease on key for ttl (Ruvio LEASE key ttl_ms).
//
// When the lease is free it returns a token and ok == true. Tokens only increase (they
// start from the server clock when the key is created), so a stale holder's token can be
// rejected by the resource it protects. While another holder has the lease it returns ok
// == false. The lease expires by itself after ttl (at least 1 ms; zero is a server
// error). Free it early with Release. Prefer AcquireLease, which wraps the token in a
// handle. The key must not hold another type (WRONGTYPE).
func (c *cmds) Lease(ctx context.Context, key string, ttl time.Duration) (token int64, ok bool, err error) {
	return c.intNil(ctx, newCmd(3, cLEASE).s(key).i(ms(ttl)))
}

// Semaphore takes one of max slots on key for ttl (Ruvio SEMAPHORE key max ttl_ms) and
// returns the holder's token. ok is false when all max slots are held. Each holder has
// its own TTL, so a crashed holder frees its slot when ttl passes; Release frees one
// early. max must be 1..4294967295 and ttl at least 1 ms, otherwise the server returns
// an error. The key must not hold another type (WRONGTYPE). See AcquirePermit.
func (c *cmds) Semaphore(ctx context.Context, key string, max int64, ttl time.Duration) (token int64, ok bool, err error) {
	return c.intNil(ctx, newCmd(4, cSEMAPHORE).s(key).i(max).i(ms(ttl)))
}

// Renew moves the deadline of a lease or semaphore slot you hold to now + ttl (Ruvio RENEW
// key token ttl). It returns true when token was a live holder and false when it expired,
// was released or belongs to someone else. ttl replaces the remaining time. The fencing
// token does not change and no holder is ever created.
func (c *cmds) Renew(ctx context.Context, key string, token int64, ttl time.Duration) (bool, error) {
	return c.boolR(ctx, newCmd(4, cRENEW).s(key).i(token).i(ms(ttl)))
}

// Release frees a lease or a semaphore slot (Ruvio RELEASE key token). It returns true
// when token was a live holder and false when the token is unknown, already released or
// expired. It never releases someone else's hold.
func (c *cmds) Release(ctx context.Context, key string, token int64) (bool, error) {
	return c.boolR(ctx, newCmd(3, cRELEASE).s(key).i(token))
}

// IdemState is the outcome of IdemBegin.
type IdemState string

const (
	// IdemAcquired means this attempt now owns a new pending record: run the
	// operation, then call IdemComplete.
	IdemAcquired IdemState = "acquired"
	// IdemPending means the same fingerprint was begun and has not completed yet;
	// do not run the operation again.
	IdemPending IdemState = "pending"
	// IdemConflict means the key is in use with a different fingerprint.
	IdemConflict IdemState = "conflict"
	// IdemCompleted means the operation already finished; IdemResult.Result holds
	// the saved result.
	IdemCompleted IdemState = "completed"
)

// IdemResult is the reply of IdemBegin. Result is only set when State is IdemCompleted
// (it may be empty).
type IdemResult struct {
	// State is the outcome (IdemAcquired, IdemPending, IdemCompleted or IdemConflict).
	State  IdemState
	Result []byte // saved result when State is IdemCompleted
}

// IdemBegin starts or looks up an idempotent operation (Ruvio IDEM key BEGIN fingerprint
// owner ttl_ms).
//
// fingerprint identifies the request (use a digest of everything that defines it) and
// owner is a unique token for this attempt; both must be 1 to 128 bytes. ttl is how long
// the record is kept, from 1 ms to 7 days (604800000 ms); it is fixed at the first BEGIN
// and later calls do not extend it. The returned State says what to do: IdemAcquired
// (run it), IdemPending (another attempt is running), IdemConflict (key reused with
// another fingerprint) or IdemCompleted (reuse Result). Only IdemAcquired writes
// anything. Idempotent wraps the whole flow.
func (c *cmds) IdemBegin(ctx context.Context, key, fingerprint, owner string, ttl time.Duration) (IdemResult, error) {
	v, err := c.value(ctx, newCmd(6, cIDEM).s(key).t(tBEGIN).s(fingerprint).s(owner).i(ms(ttl)))
	if err != nil {
		return IdemResult{}, err
	}
	if !v.isAgg() || len(v.Array) == 0 {
		return IdemResult{}, unexpected(v)
	}
	res := IdemResult{State: IdemState(v.Array[0].Str())}
	if len(v.Array) > 1 {
		res.Result = v.Array[1].Bytes
	}
	return res, nil
}

// IdemComplete stores result for the pending record that this fingerprint and owner
// began (Ruvio IDEM key COMPLETE fingerprint owner result). It returns 1 when the result
// was stored (a byte-identical repeat also returns 1) and 0 when the record is missing
// or expired, the owner or fingerprint differs, or a different result is already stored.
// result is binary-safe, at most 4 MiB. Completing never extends the record's lifetime.
func (c *cmds) IdemComplete(ctx context.Context, key, fingerprint, owner string, result []byte) (int64, error) {
	return c.intR(ctx, newCmd(6, cIDEM).s(key).t(tCOMPLETE).s(fingerprint).s(owner).b(result))
}

// IdemAbort gives up the pending record that this fingerprint and owner began (Ruvio
// IDEM key ABORT fingerprint owner), so the next IdemBegin acquires the key at once. It
// returns true when the record was removed and false when nothing changed: the key is
// missing or expired, the record is completed (its result is never removed), or it is
// held by another owner or fingerprint.
//
// Abort only when the work did not take effect or is safe to run again; a retry after an
// abort runs it again. Idempotent does this for you with WithAbortOnError.
func (c *cmds) IdemAbort(ctx context.Context, key, fingerprint, owner string) (bool, error) {
	return c.boolR(ctx, newCmd(5, cIDEM).s(key).t(tABORT).s(fingerprint).s(owner))
}

// ChangesStart arms the in-memory change feed of the shard that answers and returns the
// cursor to pass to Changes (Ruvio CHANGES START). Notes are only collected after the
// first call. The feed is kept in memory per shard and is lost on restart.
func (c *cmds) ChangesStart(ctx context.Context) (int64, error) {
	return c.intR(ctx, newCmd(2, cCHANGES).t(tSTART))
}

// Change is one entry of the change feed: Seq is its sequence number and Note a short
// text such as "SET key", "DEL key" or "SETV key 3".
type Change struct {
	Seq  int64  // sequence number; pass the last one seen to Changes
	Note string // short text such as "SET key"
}

// Changes returns the notes recorded after cursor, oldest first (Ruvio CHANGES cursor).
// Pass the value from ChangesStart the first time and the Seq of the last note you
// processed afterwards. The shard keeps only the newest 1024 notes, so poll often enough
// not to miss any. It returns an empty slice when nothing is new, including when
// ChangesStart was never called.
func (c *cmds) Changes(ctx context.Context, cursor int64) ([]Change, error) {
	v, err := c.value(ctx, newCmd(2, cCHANGES).i(cursor))
	if err != nil {
		return nil, err
	}
	if !v.isAgg() {
		return nil, unexpected(v)
	}
	out := make([]Change, 0, len(v.Array))
	for _, row := range v.Array {
		if len(row.Array) < 2 {
			return nil, unexpected(v)
		}
		out = append(out, Change{Seq: row.Array[0].Int, Note: row.Array[1].Str()})
	}
	return out, nil
}

// FillState is the outcome of FillGet.
type FillState string

const (
	// FillHit means the cached value is fresh; FillResult.Value holds it.
	FillHit FillState = "HIT"
	// FillFill means the key is missing and this caller was chosen to compute it;
	// pass FillResult.Token to FillSet.
	FillFill FillState = "FILL"
	// FillBusy means the key is missing and another caller is computing it.
	FillBusy FillState = "BUSY"
	// FillStale means the value is past its TTL but inside the stale window; Value
	// holds it. If Token is set this caller should refresh it.
	FillStale FillState = "STALE"
)

// FillResult is the reply of FillGet. Value is set for FillHit and FillStale. Token is
// set for FillFill, and for FillStale when this caller was chosen to refresh.
type FillResult struct {
	// State is the outcome (FillHit, FillStale, FillFill or FillBusy).
	State FillState
	Value string // cached value (FillHit, FillStale)
	Token string // fill token for FillSet (FillFill, FillStale)
}

func toFill(v Value) (FillResult, error) {
	if !v.isAgg() || len(v.Array) < 2 {
		return FillResult{}, unexpected(v)
	}
	r := FillResult{State: FillState(v.Array[0].Str())}
	switch r.State {
	case FillHit:
		r.Value = v.Array[1].Str()
	case FillFill:
		r.Token = v.Array[1].Str()
	case FillStale:
		r.Value = v.Array[1].Str()
		if len(v.Array) > 2 {
			r.Token = v.Array[2].Str()
		}
	}
	return r, nil
}

// FillGet reads a cache-aside key with stampede protection (Ruvio FILLGET key fill_ms).
// The State tells what to do: FillHit use Value; FillFill compute the value and store it
// with FillSet and Token; FillBusy wait and ask again; FillStale use Value and, if Token
// is set, refresh it.
//
// fill is how long the "someone is filling" marker lives (at least 1 ms); if the filler
// dies, the marker lapses after that time and another caller is chosen. The marker is
// kept in memory only. More pending fills than the server allows returns "ERR too many
// pending fills". A key holding a non-string returns WRONGTYPE. GetOrCreate implements
// the full loop.
func (c *cmds) FillGet(ctx context.Context, key string, fill time.Duration) (FillResult, error) {
	v, err := c.value(ctx, newCmd(3, cFILLGET).s(key).i(ms(fill)))
	if err != nil {
		return FillResult{}, err
	}
	return toFill(v)
}

// FillGetWait is FillGet with WAIT (FILLGET key fill_ms WAIT wait_ms): instead of
// answering FillBusy at once, the server holds the call until the filler stores the
// value (FillHit), the marker lapses (this caller becomes FillFill) or wait ends
// (FillBusy). Both durations must be at least 1 ms. Because it can park for up to wait,
// it runs on a dedicated connection and never stalls the shared one; it is refused
// inside MULTI and scripts.
func (c *cmds) FillGetWait(ctx context.Context, key string, fill, wait time.Duration) (FillResult, error) {
	v, err := c.ex.execBlocking(ctx, newCmd(5, cFILLGET).s(key).i(ms(fill)).t(tWAIT).i(ms(wait)))
	if err != nil {
		return FillResult{}, err
	}
	return toFill(v)
}

// FillSet stores the computed value for a FillGet token (Ruvio FILLSET key value ttl_ms
// token [SWR stale_ms]) and returns true when it was stored.
//
// ttl (at least 1 ms) is how long the value is fresh. When stale > 0 the value stays
// readable for that much longer after ttl as FillStale. It returns false, and writes
// nothing, when the token is no longer the live filler (for example the marker lapsed
// and another caller took over), so a slow filler cannot overwrite a newer value. token
// must be the string from FillResult.Token.
func (c *cmds) FillSet(ctx context.Context, key, value string, ttl time.Duration, token string, stale time.Duration) (bool, error) {
	if stale > 0 {
		return c.boolR(ctx, newCmd(7, cFILLSET).s(key).s(value).i(ms(ttl)).s(token).t(tSWR).i(ms(stale)))
	}
	return c.boolR(ctx, newCmd(5, cFILLSET).s(key).s(value).i(ms(ttl)).s(token))
}

// Ruvio sends RUVIO with no argument. The server answers with a fixed text ("PONG is
// taken"); it is mostly useful to check that the server is Ruvio.
func (c *cmds) Ruvio(ctx context.Context) (string, error) {
	return c.strR(ctx, newCmd(1, cRUVIO))
}

// RuvioOwner sends RUVIO OWNER and returns the server's text describing which shard owns
// keys (the shard rule).
func (c *cmds) RuvioOwner(ctx context.Context) (string, error) {
	return c.strR(ctx, newCmd(2, cRUVIO).t(tOWNER))
}

// MoveSlots moves the hash slots first..last to shard (counted from 0) of the same server,
// which keeps serving them meanwhile (RUVIO MOVESLOTS first-last shard). It returns once
// the move has started; MoveSlotsStatus follows it. One move runs at a time, and every slot
// of the range must be on one shard.
func (c *cmds) MoveSlots(ctx context.Context, first, last, shard int) error {
	return c.ok(ctx, newCmd(4, cRUVIO).s("MOVESLOTS").s(fmt.Sprintf("%d-%d", first, last)).i(int64(shard)))
}

// MoveSlotsStatus returns the running or last slot move (RUVIO MOVESLOTS STATUS) as a
// name-to-value map. state is none, copying, cleaning, cancelling, done or cancelled; the
// other names are first, last, from, to, keys, rounds, elapsed_ms, freeze_ms and error.
func (c *cmds) MoveSlotsStatus(ctx context.Context) (map[string]string, error) {
	return c.strMap(ctx, newCmd(3, cRUVIO).s("MOVESLOTS").s("STATUS"))
}

// ---- bitmaps & HyperLogLog ---------------------------------------------------

// SetBit sets or clears the bit at offset and returns the previous value (SETBIT). bit
// must be 0 or 1 (anything else is a server error). Bit 0 is the most significant bit of
// the first byte. The string grows with zero bytes as needed, up to 16 MiB (offset below
// 134217728). A missing key is created; an existing TTL is kept. A non-string key
// returns WRONGTYPE.
func (c *cmds) SetBit(ctx context.Context, key string, offset int64, bit int) (int64, error) {
	return c.intR(ctx, newCmd(4, cSETBIT).s(key).i(offset).i(int64(bit)))
}

// SetBitWithTTL is SetBit that also sets a deadline when it creates the key (SETBIT key
// offset bit EX|PX). An existing key keeps its deadline. A ttl of zero or less returns
// ErrInvalidTTL.
func (c *cmds) SetBitWithTTL(ctx context.Context, key string, offset int64, bit int, ttl time.Duration) (int64, error) {
	tok, v, err := ttlArg(ttl)
	if err != nil {
		return 0, err
	}
	return c.intR(ctx, newCmd(6, cSETBIT).s(key).i(offset).i(int64(bit)).t(tok).i(v))
}

// GetBit returns the bit at offset (GETBIT): 0 for a missing key or an offset beyond the
// end. Bit 0 is the most significant bit of the first byte. WRONGTYPE for a non-string.
func (c *cmds) GetBit(ctx context.Context, key string, offset int64) (int64, error) {
	return c.intR(ctx, newCmd(3, cGETBIT).s(key).i(offset))
}

// BitCount counts the bits set to 1 in the whole string (BITCOUNT). A missing key
// returns 0.
func (c *cmds) BitCount(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(2, cBITCOUNT).s(key))
}

// BitCountRange counts set bits between start and end, inclusive (BITCOUNT key start end
// [BIT]). The positions are byte offsets, or bit offsets when bitUnits is true; negative
// values count from the end. An empty or inverted range returns 0.
func (c *cmds) BitCountRange(ctx context.Context, key string, start, end int64, bitUnits bool) (int64, error) {
	if bitUnits {
		return c.intR(ctx, newCmd(5, cBITCOUNT).s(key).i(start).i(end).t(tBIT))
	}
	return c.intR(ctx, newCmd(4, cBITCOUNT).s(key).i(start).i(end))
}

// BitCountUnit counts set bits in [start, end] using unit BitUnitByte or BitUnitBit.
//
//	n, err := c.BitCountUnit(ctx, "k", 0, 15, ruvio.BitUnitBit)
func (c *cmds) BitCountUnit(ctx context.Context, key string, start, end int64, unit BitUnit) (int64, error) {
	return c.intR(ctx, newCmd(5, cBITCOUNT).s(key).i(start).i(end).kw(unit.token(), string(unit)))
}

// BitPos returns the position of the first bit equal to bit (0 or 1) (BITPOS). The
// optional startEnd are byte offsets: one value is the start, two values are start and
// end (inclusive; negative counts from the end); more are a syntax error. The bit-unit
// form is not offered here, use Do.
//
// If nothing matches it returns -1. A missing key returns 0 when looking for 0 and -1
// when looking for 1. Searching for 0 in an all-ones string with no explicit end returns
// the position just past the string (it is treated as padded with zeros).
func (c *cmds) BitPos(ctx context.Context, key string, bit int, startEnd ...int64) (int64, error) {
	r := newCmd(3+len(startEnd), cBITPOS).s(key).i(int64(bit))
	for _, x := range startEnd {
		r.i(x)
	}
	return c.intR(ctx, r)
}

// BitOp combines source strings bitwise into dest and returns the length of the result
// in bytes (BITOP). op is BitAnd, BitOr, BitXor or BitNot (NOT takes exactly one key).
// Shorter sources are padded with zero bytes; missing keys count as empty. An empty
// result deletes dest. The result is limited to 16 MiB. All keys must share a hash slot
// in cluster mode (otherwise ErrCrossSlot).
func (c *cmds) BitOp(ctx context.Context, op BitOperation, dest string, keys ...string) (int64, error) {
	return c.intR(ctx, newCmd(3+len(keys), cBITOP).kw(op.token(), string(op)).s(dest).ss(keys))
}

// BitFieldOp is one sub-command of BITFIELD.
//
// Op is BitFieldGet, BitFieldSet, BitFieldIncrBy or BitFieldOverflow (case-insensitive). Type is the field type:
// i1 to i64 (signed) or u1 to u63 (unsigned), for example BitU8 (or BitSigned and BitUnsigned for any width). Offset is a bit offset
// such as "0" or "100", or "#n" to mean n times the field width. Value is the operand of
// SET and INCRBY and is ignored for GET. For OVERFLOW only Type is used and holds the
// policy: BitOverflowWrap, BitOverflowSat or BitOverflowFail; it applies to the INCRBY and SET operations that
// follow it.
type BitFieldOp struct {
	Op     BitFieldOpName // GET, SET, INCRBY or OVERFLOW
	Type   string         // i1..i64 or u1..u63; the policy for OVERFLOW
	Offset string         // bit offset, or "#n" for n times the field width
	Value  int64          // operand of SET and INCRBY
}

func bitfieldReq(name []byte, key string, ops []BitFieldOp) *request {
	n := 2
	for _, o := range ops {
		switch o.Op {
		case "OVERFLOW", "overflow":
			n += 2
		case "GET", "get":
			n += 3
		default:
			n += 4
		}
	}
	r := newCmd(n, name).s(key)
	for _, o := range ops {
		switch o.Op {
		case "OVERFLOW", "overflow":
			r.kw(o.Op.token(), string(o.Op)).kw(overflowToken(o.Type), o.Type)
		case "GET", "get":
			r.kw(o.Op.token(), string(o.Op)).s(o.Type).s(o.Offset)
		default:
			r.kw(o.Op.token(), string(o.Op)).s(o.Type).s(o.Offset).i(o.Value)
		}
	}
	return r
}

// BitField runs several bitfield operations in one call (BITFIELD) and returns one entry
// per GET, SET and INCRBY (OVERFLOW produces none). GET returns the field, SET the
// field's previous value and INCRBY its new value. An entry is not Valid when OVERFLOW
// FAIL refused the operation. Writing extends the string to cover the field and keeps
// the TTL; fields must end below 16 MiB.
//
//	r, err := c.BitField(ctx, "counters",
//	    ruvio.BitFieldOp{Op: ruvio.BitFieldOverflow, Type: ruvio.BitOverflowSat},
//	    ruvio.BitFieldOp{Op: ruvio.BitFieldIncrBy, Type: ruvio.BitU8, Offset: "0", Value: 10},
//	    ruvio.BitFieldOp{Op: ruvio.BitFieldGet, Type: ruvio.BitU8, Offset: "0"},
//	)
func (c *cmds) BitField(ctx context.Context, key string, ops ...BitFieldOp) ([]NullInt, error) {
	v, err := c.value(ctx, bitfieldReq(cBITFIELD, key, ops))
	if err != nil {
		return nil, err
	}
	return toNullInts(v)
}

// BitFieldRO runs read-only BITFIELD_RO with GET operations only; it is safe to send to
// read paths. SET and INCRBY return the server error "BITFIELD_RO only supports the GET
// subcommand".
func (c *cmds) BitFieldRO(ctx context.Context, key string, ops ...BitFieldOp) ([]NullInt, error) {
	v, err := c.value(ctx, bitfieldReq(cBITFIELD_RO, key, ops))
	if err != nil {
		return nil, err
	}
	return toNullInts(v)
}

// NullInt is an integer that may be absent: Valid is false for a null reply.
type NullInt struct {
	// Val is the value; 0 when Valid is false.
	Val   int64
	Valid bool // false for a null reply
}

func toNullInts(v Value) ([]NullInt, error) {
	if !v.isAgg() {
		return nil, unexpected(v)
	}
	out := make([]NullInt, len(v.Array))
	for i, e := range v.Array {
		if e.Kind != KindNil {
			n, err := e.AsInt64()
			if err != nil {
				return nil, err
			}
			out[i] = NullInt{n, true}
		}
	}
	return out, nil
}

// PFAdd adds elements to the HyperLogLog at key (PFADD) and returns true when the
// estimate changed (also when the key was created). A missing key is created. The key
// must hold a HyperLogLog (a string with the HYLL header, dense form only), otherwise
// the server returns an error such as WRONGTYPE.
func (c *cmds) PFAdd(ctx context.Context, key string, elements ...string) (bool, error) {
	return c.boolR(ctx, newCmd(2+len(elements), cPFADD).s(key).ss(elements))
}

// PFCount returns the estimated number of distinct elements added to the HyperLogLog at
// key (PFCOUNT). With several keys it returns the estimate of their union without
// storing it. A missing key counts as empty. Keys must share a hash slot in cluster
// mode.
func (c *cmds) PFCount(ctx context.Context, keys ...string) (int64, error) {
	return c.intR(ctx, newCmd(1+len(keys), cPFCOUNT).ss(keys))
}

// PFMerge merges the HyperLogLogs in sources (and dest itself, if it exists) into dest
// (PFMERGE). dest is created when missing. Keys must share a hash slot in cluster mode.
func (c *cmds) PFMerge(ctx context.Context, dest string, sources ...string) error {
	return c.ok(ctx, newCmd(2+len(sources), cPFMERGE).s(dest).ss(sources))
}
