// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"time"
)

// ---- lists ---------------------------------------------------------------------

// LPush inserts values at the head of the list at key (LPUSH) and returns the list
// length afterwards. A missing key is created. Several values are pushed one after
// another, so the last value ends up first. A key of another type returns WRONGTYPE.
//
//	n, err := c.LPush(ctx, "queue", "a", "b") // list is now [b a]
func (c *cmds) LPush(ctx context.Context, key string, values ...string) (int64, error) {
	return c.intR(ctx, newCmd(2+len(values), cLPUSH).s(key).ss(values))
}

// RPush appends values at the tail of the list at key (RPUSH) and returns the list
// length afterwards. A missing key is created. A key of another type returns WRONGTYPE.
func (c *cmds) RPush(ctx context.Context, key string, values ...string) (int64, error) {
	return c.intR(ctx, newCmd(2+len(values), cRPUSH).s(key).ss(values))
}

// LPushX pushes at the head only when the list already exists (LPUSHX) and returns the
// new length; it returns 0 and creates nothing when the key is missing.
func (c *cmds) LPushX(ctx context.Context, key string, values ...string) (int64, error) {
	return c.intR(ctx, newCmd(2+len(values), cLPUSHX).s(key).ss(values))
}

// RPushX pushes at the tail only when the list already exists (RPUSHX) and returns the
// new length; it returns 0 and creates nothing when the key is missing.
func (c *cmds) RPushX(ctx context.Context, key string, values ...string) (int64, error) {
	return c.intR(ctx, newCmd(2+len(values), cRPUSHX).s(key).ss(values))
}

// LPushCap pushes values at the head and then trims the tail so the list holds at most
// cap items (Ruvio LPUSHCAP key cap value ...). It is an atomic "recent items" list. It
// returns the length after trimming (never more than cap). cap must be > 0 ("cap must be
// a positive integer"). At least one value is required. See PushRecent and Recent.
func (c *cmds) LPushCap(ctx context.Context, key string, cap int64, values ...string) (int64, error) {
	return c.intR(ctx, newCmd(3+len(values), cLPUSHCAP).s(key).i(cap).ss(values))
}

// RPushCap pushes values at the tail and then trims the head so the list holds at most
// cap items (Ruvio RPUSHCAP key cap value ...). It returns the length after trimming.
// cap must be > 0.
func (c *cmds) RPushCap(ctx context.Context, key string, cap int64, values ...string) (int64, error) {
	return c.intR(ctx, newCmd(3+len(values), cRPUSHCAP).s(key).i(cap).ss(values))
}

// LPop removes and returns the first element (LPOP). ok is false when the key does not
// exist or the list is empty. A key of another type returns WRONGTYPE. Ruvio deletes a
// list when its last element is removed.
func (c *cmds) LPop(ctx context.Context, key string) (string, bool, error) {
	return c.strNil(ctx, newCmd(2, cLPOP).s(key))
}

// LPopCount removes and returns up to count elements from the head (LPOP key count), in
// pop order. It returns a nil slice when the key does not exist. count should be
// positive.
func (c *cmds) LPopCount(ctx context.Context, key string, count int64) ([]string, error) {
	return c.strs(ctx, newCmd(3, cLPOP).s(key).i(count))
}

// RPop removes and returns the last element (RPOP). ok is false when the key does not
// exist or the list is empty.
func (c *cmds) RPop(ctx context.Context, key string) (string, bool, error) {
	return c.strNil(ctx, newCmd(2, cRPOP).s(key))
}

// RPopCount removes and returns up to count elements from the tail (RPOP key count),
// last element first. It returns a nil slice when the key does not exist.
func (c *cmds) RPopCount(ctx context.Context, key string, count int64) ([]string, error) {
	return c.strs(ctx, newCmd(3, cRPOP).s(key).i(count))
}

// LLen returns the length of the list at key (LLEN), or 0 when the key does not exist.
// WRONGTYPE for another type.
func (c *cmds) LLen(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(2, cLLEN).s(key))
}

// LIndex returns the element at index (LINDEX); negative indexes count from the end (-1
// is the last element). ok is false when the key is missing or the index is out of
// range.
func (c *cmds) LIndex(ctx context.Context, key string, index int64) (string, bool, error) {
	return c.strNil(ctx, newCmd(3, cLINDEX).s(key).i(index))
}

// LRange returns the elements from start to stop, both inclusive (LRANGE). Negative
// indexes count from the end, so LRange(key, 0, -1) is the whole list. Out of range
// bounds are clamped; a missing key or an empty range gives an empty result.
func (c *cmds) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return c.strs(ctx, newCmd(4, cLRANGE).s(key).i(start).i(stop))
}

// LTrim keeps only the elements from start to stop, both inclusive, and deletes the rest
// (LTRIM). Negative indexes count from the end. An empty range removes the key.
func (c *cmds) LTrim(ctx context.Context, key string, start, stop int64) error {
	return c.ok(ctx, newCmd(4, cLTRIM).s(key).i(start).i(stop))
}

// LInsert inserts value before (before == true) or after the first element equal to
// pivot (LINSERT) and returns the new length. It returns -1 when pivot is not found and
// 0 when the key does not exist. The list is rewritten, so it costs O(N).
func (c *cmds) LInsert(ctx context.Context, key string, before bool, pivot, value string) (int64, error) {
	where := tAFTER
	if before {
		where = tBEFORE
	}
	return c.intR(ctx, newCmd(5, cLINSERT).s(key).t(where).s(pivot).s(value))
}

// LInsertAt is LInsert with a typed position: InsertBefore or InsertAfter.
//
//	n, err := c.LInsertAt(ctx, "k", ruvio.InsertAfter, "pivot", "value")
func (c *cmds) LInsertAt(ctx context.Context, key string, pos ListPosition, pivot, value string) (int64, error) {
	return c.intR(ctx, newCmd(5, cLINSERT).s(key).kw(pos.token(), string(pos)).s(pivot).s(value))
}

// LSet sets the element at index (LSET); negative indexes count from the end. A missing
// key or an index out of range returns a server error. The list is rewritten, O(N).
func (c *cmds) LSet(ctx context.Context, key string, index int64, value string) error {
	return c.ok(ctx, newCmd(4, cLSET).s(key).i(index).s(value))
}

// LRem removes elements equal to value and returns how many were removed (LREM). count >
// 0 removes up to count matches from the head, count < 0 up to |count| from the tail,
// and count == 0 removes all matches. O(N).
func (c *cmds) LRem(ctx context.Context, key string, count int64, value string) (int64, error) {
	return c.intR(ctx, newCmd(4, cLREM).s(key).i(count).s(value))
}

// LPosOptions are the LPOS modifiers. Zero values are left out of the command.
type LPosOptions struct {
	// Rank selects the Nth match: 1 is the first, 2 the second, -1 the last (search
	// from the tail). Zero means the default (1).
	Rank int64
	// MaxLen limits how many elements are compared; zero means the whole list.
	MaxLen int64
}

func lposReq(key, elem string, o LPosOptions, count int64, withCount bool) *request {
	n := 3
	if o.Rank != 0 {
		n += 2
	}
	if withCount {
		n += 2
	}
	if o.MaxLen != 0 {
		n += 2
	}
	r := newCmd(n, cLPOS).s(key).s(elem)
	if o.Rank != 0 {
		r.t(tRANK).i(o.Rank)
	}
	if withCount {
		r.t(tCOUNT).i(count)
	}
	if o.MaxLen != 0 {
		r.t(tMAXLEN).i(o.MaxLen)
	}
	return r
}

// LPos returns the index of the first element equal to elem (LPOS). ok is false when
// there is no match or the key is missing. o.Rank picks the Nth match (negative searches
// from the tail; 0 means the default, 1); o.MaxLen limits how many elements are compared
// (0 = all).
func (c *cmds) LPos(ctx context.Context, key, elem string, o LPosOptions) (int64, bool, error) {
	return c.intNil(ctx, lposReq(key, elem, o, 0, false))
}

// LPosCount returns the indexes of up to count matches of elem (LPOS ... COUNT count);
// count 0 returns all matches. Rank and MaxLen work as in LPos. A missing key returns an
// empty result.
func (c *cmds) LPosCount(ctx context.Context, key, elem string, count int64, o LPosOptions) ([]int64, error) {
	return c.ints(ctx, lposReq(key, elem, o, count, true))
}

// LMove atomically pops an element from one end of src and pushes it to one end of dst,
// and returns it (LMOVE). from and to are the ends to pop from and push to (ListSide: ListLeft or
// ListRight; any other value is a syntax error). src and dst may be the same list (a
// rotation). ok is false and nothing moves when src is missing or empty. In cluster mode both keys must share a
// slot (ErrCrossSlot).
//
//	v, ok, err := c.LMove(ctx, "pending", "working", ruvio.ListLeft, ruvio.ListRight)
func (c *cmds) LMove(ctx context.Context, src, dst string, from, to ListSide) (string, bool, error) {
	return c.strNil(ctx, newCmd(5, cLMOVE).s(src).s(dst).kw(from.token(), string(from)).kw(to.token(), string(to)))
}

// RPopLPush moves the last element of src to the head of dst and returns it (RPOPLPUSH,
// same as LMove with RIGHT, LEFT). ok is false when src is missing or empty. Same-slot
// rule in cluster mode.
func (c *cmds) RPopLPush(ctx context.Context, src, dst string) (string, bool, error) {
	return c.strNil(ctx, newCmd(3, cRPOPLPUSH).s(src).s(dst))
}

// BLMove is LMove that waits up to timeout for src to receive an element (BLMOVE).
// timeout 0 waits forever (until ctx ends or the server shuts down). ok is false when
// the wait timed out. It runs on a dedicated connection, so it does not delay other
// calls. The server does not park a waiter: it retries every 5 ms; it is refused inside
// MULTI and scripts. Both keys must share a slot in cluster mode.
func (c *cmds) BLMove(ctx context.Context, src, dst string, from, to ListSide, timeout time.Duration) (string, bool, error) {
	v, err := c.ex.execBlocking(ctx, newCmd(6, cBLMOVE).s(src).s(dst).kw(from.token(), string(from)).kw(to.token(), string(to)).f(secs(timeout)))
	if err != nil || v.Kind == KindNil {
		return "", false, err
	}
	return v.Str(), true, nil
}

// KeyValue is a (key, element) pair returned by a blocking pop: Key tells which of the
// watched lists the element came from.
type KeyValue struct {
	Key   string // list the element came from
	Value string // the popped element
}

func (c *cmds) bpop(ctx context.Context, name []byte, timeout time.Duration, keys []string) (KeyValue, bool, error) {
	v, err := c.ex.execBlocking(ctx, newCmd(2+len(keys), name).ss(keys).f(secs(timeout)))
	if err != nil || v.Kind == KindNil {
		return KeyValue{}, false, err
	}
	if len(v.Array) != 2 {
		return KeyValue{}, false, unexpected(v)
	}
	return KeyValue{v.Array[0].Str(), v.Array[1].Str()}, true, nil
}

// BLPop waits up to timeout for an element at the head of any of keys, checked in order
// (BLPOP). It returns the key and the element. ok is false when the timeout expired.
// timeout 0 waits forever, until ctx is cancelled or the server shuts down. The timeout
// is sent as decimal seconds (rounded up to a millisecond). It runs on a dedicated
// connection, so other calls keep flowing. A push serves the oldest waiter first. At
// most 1024 waiters per shard, more return "ERR max number of blocking clients reached".
// Blocking pops are shard-local: in cluster mode all keys must share a slot.
//
//	kv, ok, err := c.BLPop(ctx, 5*time.Second, "jobs")
func (c *cmds) BLPop(ctx context.Context, timeout time.Duration, keys ...string) (KeyValue, bool, error) {
	return c.bpop(ctx, cBLPOP, timeout, keys)
}

// BRPop is BLPop for the tail of the lists (BRPOP). Same timeout, ok and cluster rules.
func (c *cmds) BRPop(ctx context.Context, timeout time.Duration, keys ...string) (KeyValue, bool, error) {
	return c.bpop(ctx, cBRPOP, timeout, keys)
}

// KeyValues is the reply of LMPop and BLMPop: Key is the list that was popped and Values
// the elements in pop order.
type KeyValues struct {
	Key    string   // list that was popped
	Values []string // popped elements, in pop order
}

func mpopReq(name []byte, timeout *time.Duration, dir ListSide, count int64, keys []string) *request {
	n := 3 + len(keys)
	if timeout != nil {
		n++
	}
	if count > 0 {
		n += 2
	}
	r := newCmd(n, name)
	if timeout != nil {
		r.f(secs(*timeout))
	}
	r.i(int64(len(keys))).ss(keys).kw(dir.token(), string(dir))
	if count > 0 {
		r.t(tCOUNT).i(count)
	}
	return r
}

func toKeyValues(v Value) (KeyValues, bool, error) {
	if v.Kind == KindNil {
		return KeyValues{}, false, nil
	}
	if len(v.Array) != 2 {
		return KeyValues{}, false, unexpected(v)
	}
	vals, err := v.Array[1].Strings()
	return KeyValues{v.Array[0].Str(), vals}, err == nil, err
}

// LMPop pops elements from the first non-empty list among keys (LMPOP numkeys key ...
// LEFT|RIGHT [COUNT n]). dir is ListLeft or ListRight; count is how many to pop (0 sends no
// COUNT, meaning 1; negative counts are ignored the same way). ok is false when all
// lists are empty or missing. Keys must share a slot in cluster mode.
func (c *cmds) LMPop(ctx context.Context, dir ListSide, count int64, keys ...string) (KeyValues, bool, error) {
	v, err := c.value(ctx, mpopReq(cLMPOP, nil, dir, count, keys))
	if err != nil {
		return KeyValues{}, false, err
	}
	return toKeyValues(v)
}

// BLMPop is LMPop that waits up to timeout for one of the lists to get an element
// (BLMPOP). timeout 0 waits forever; ok is false on timeout. Runs on a dedicated
// connection; the server retries every 5 ms and refuses it inside MULTI and scripts.
func (c *cmds) BLMPop(ctx context.Context, timeout time.Duration, dir ListSide, count int64, keys ...string) (KeyValues, bool, error) {
	v, err := c.ex.execBlocking(ctx, mpopReq(cBLMPOP, &timeout, dir, count, keys))
	if err != nil {
		return KeyValues{}, false, err
	}
	return toKeyValues(v)
}

// ---- sets ------------------------------------------------------------------------

// SAdd adds members to the set at key and returns how many were new (SADD). A missing
// key is created. WRONGTYPE for another type.
func (c *cmds) SAdd(ctx context.Context, key string, members ...string) (int64, error) {
	return c.intR(ctx, newCmd(2+len(members), cSADD).s(key).ss(members))
}

// SRem removes members and returns how many were present (SREM). The set is deleted when
// it becomes empty.
func (c *cmds) SRem(ctx context.Context, key string, members ...string) (int64, error) {
	return c.intR(ctx, newCmd(2+len(members), cSREM).s(key).ss(members))
}

// SIsMember reports whether member is in the set (SISMEMBER); false for a missing key.
func (c *cmds) SIsMember(ctx context.Context, key, member string) (bool, error) {
	return c.boolR(ctx, newCmd(3, cSISMEMBER).s(key).s(member))
}

// SMIsMember reports membership for each of members, in order (SMISMEMBER); all false
// for a missing key.
func (c *cmds) SMIsMember(ctx context.Context, key string, members ...string) ([]bool, error) {
	return c.bools(ctx, newCmd(2+len(members), cSMISMEMBER).s(key).ss(members))
}

// SCard returns the number of members (SCARD), 0 for a missing key.
func (c *cmds) SCard(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(2, cSCARD).s(key))
}

// SMembers returns all members (SMEMBERS), sorted by the server. O(N): for big sets use
// SScan or SScanIter. A missing key gives an empty result.
func (c *cmds) SMembers(ctx context.Context, key string) ([]string, error) {
	return c.strs(ctx, newCmd(2, cSMEMBERS).s(key))
}

// SInter returns the members present in every set (SINTER), sorted. A missing key counts
// as an empty set. Keys must share a slot in cluster mode (ErrCrossSlot).
func (c *cmds) SInter(ctx context.Context, keys ...string) ([]string, error) {
	return c.strs(ctx, newCmd(1+len(keys), cSINTER).ss(keys))
}

// SUnion returns the members present in any set (SUNION), sorted. Same-slot rule in
// cluster mode.
func (c *cmds) SUnion(ctx context.Context, keys ...string) ([]string, error) {
	return c.strs(ctx, newCmd(1+len(keys), cSUNION).ss(keys))
}

// SDiff returns the members of the first set that are in none of the others (SDIFF),
// sorted. Same-slot rule in cluster mode.
func (c *cmds) SDiff(ctx context.Context, keys ...string) ([]string, error) {
	return c.strs(ctx, newCmd(1+len(keys), cSDIFF).ss(keys))
}

// SInterStore stores the intersection of keys at dst, replacing it, and returns the
// number of members stored (SINTERSTORE). An empty result removes dst. All keys, dst
// included, must share a slot in cluster mode.
func (c *cmds) SInterStore(ctx context.Context, dst string, keys ...string) (int64, error) {
	return c.intR(ctx, newCmd(2+len(keys), cSINTERSTORE).s(dst).ss(keys))
}

// SUnionStore stores the union of keys at dst, replacing it, and returns the number of
// members stored (SUNIONSTORE). Same-slot rule in cluster mode.
func (c *cmds) SUnionStore(ctx context.Context, dst string, keys ...string) (int64, error) {
	return c.intR(ctx, newCmd(2+len(keys), cSUNIONSTORE).s(dst).ss(keys))
}

// SDiffStore stores the difference (first set minus the rest) at dst, replacing it, and
// returns the number of members stored (SDIFFSTORE). Same-slot rule in cluster mode.
func (c *cmds) SDiffStore(ctx context.Context, dst string, keys ...string) (int64, error) {
	return c.intR(ctx, newCmd(2+len(keys), cSDIFFSTORE).s(dst).ss(keys))
}

// SInterCard returns the size of the intersection without building it (SINTERCARD
// numkeys key ... [LIMIT n]). A limit > 0 stops counting at limit; 0 means no limit.
// Same-slot rule in cluster mode.
func (c *cmds) SInterCard(ctx context.Context, limit int64, keys ...string) (int64, error) {
	if limit > 0 {
		return c.intR(ctx, newCmd(4+len(keys), cSINTERCARD).i(int64(len(keys))).ss(keys).t(tLIMIT).i(limit))
	}
	return c.intR(ctx, newCmd(2+len(keys), cSINTERCARD).i(int64(len(keys))).ss(keys))
}

// SMove moves member from src to dst (SMOVE). It returns true when moved and false when
// member is not in src. Both keys must share a slot in cluster mode.
func (c *cmds) SMove(ctx context.Context, src, dst, member string) (bool, error) {
	return c.boolR(ctx, newCmd(4, cSMOVE).s(src).s(dst).s(member))
}

// SPop removes and returns one random member (SPOP). ok is false for a missing key or an
// empty set.
func (c *cmds) SPop(ctx context.Context, key string) (string, bool, error) {
	return c.strNil(ctx, newCmd(2, cSPOP).s(key))
}

// SPopCount removes and returns up to count random members (SPOP key count). A missing
// key gives an empty result.
func (c *cmds) SPopCount(ctx context.Context, key string, count int64) ([]string, error) {
	return c.strs(ctx, newCmd(3, cSPOP).s(key).i(count))
}

// SRandMember returns one random member without removing it (SRANDMEMBER). ok is false
// for a missing key.
func (c *cmds) SRandMember(ctx context.Context, key string) (string, bool, error) {
	return c.strNil(ctx, newCmd(2, cSRANDMEMBER).s(key))
}

// SRandMemberCount returns count random members without removing them (SRANDMEMBER key
// count). A positive count returns distinct members, at most the set size; a negative
// count returns |count| members and may repeat them, up to 1048576, beyond which the
// server answers "count is out of range".
func (c *cmds) SRandMemberCount(ctx context.Context, key string, count int64) ([]string, error) {
	return c.strs(ctx, newCmd(3, cSRANDMEMBER).s(key).i(count))
}

func (c *cmds) keyScan(ctx context.Context, name []byte, key, cursor string, o ScanOptions) (string, []string, error) {
	o.Type = ""
	r := newCmd(3+o.n(), name).s(key).s(cursor)
	o.append(r)
	v, err := c.value(ctx, r)
	if err != nil {
		return "", nil, err
	}
	return scanReply(v)
}

// SScan returns one page of members and the next cursor (SSCAN). Start with cursor "0"
// and stop when the returned cursor is "0". o.Match filters members; o.Count is a page
// size hint (server default 10); o.Type is ignored. A member can appear twice if the set
// was rebuilt during the walk, but none is skipped. Use SScanIter to loop for you.
func (c *cmds) SScan(ctx context.Context, key, cursor string, o ScanOptions) (string, []string, error) {
	return c.keyScan(ctx, cSSCAN, key, cursor, o)
}

// ---- hashes ----------------------------------------------------------------------

// HSet sets fields of the hash at key (HSET): fieldValues is field1, value1, field2,
// value2 ... It returns how many fields were new (updated fields are not counted). An
// odd number of arguments is a server error. A missing key is created. Setting a field
// drops its field TTL.
//
//	n, err := c.HSet(ctx, "user:1", "name", "Ada", "city", "London")
func (c *cmds) HSet(ctx context.Context, key string, fieldValues ...string) (int64, error) {
	return c.intR(ctx, newCmd(2+len(fieldValues), cHSET).s(key).ss(fieldValues))
}

// HSetBytes sets one field to a binary value (HSET key field value) and returns 1 if the
// field was new, 0 if it was updated.
func (c *cmds) HSetBytes(ctx context.Context, key, field string, value []byte) (int64, error) {
	return c.intR(ctx, newCmd(4, cHSET).s(key).s(field).b(value))
}

// HMSet sets fields of the hash at key like HSet but sends HMSET, which replies OK
// instead of the count of new fields. Redis marks HMSET deprecated; prefer HSet.
//
//	err := c.HMSet(ctx, "user:1", "name", "Ada", "city", "London")
func (c *cmds) HMSet(ctx context.Context, key string, fieldValues ...string) error {
	return c.ok(ctx, newCmd(2+len(fieldValues), cHMSET).s(key).ss(fieldValues))
}

// HSetNX sets the field only when it does not exist (HSETNX) and returns true when it
// wrote.
func (c *cmds) HSetNX(ctx context.Context, key, field, value string) (bool, error) {
	return c.boolR(ctx, newCmd(4, cHSETNX).s(key).s(field).s(value))
}

// HGet returns the value of field (HGET). ok is false when the key or the field is
// missing. WRONGTYPE for another type.
func (c *cmds) HGet(ctx context.Context, key, field string) (string, bool, error) {
	return c.strNil(ctx, newCmd(3, cHGET).s(key).s(field))
}

// HGetBytes is HGet for binary values; the returned slice is owned by the caller.
func (c *cmds) HGetBytes(ctx context.Context, key, field string) ([]byte, bool, error) {
	return c.bytesNil(ctx, newCmd(3, cHGET).s(key).s(field))
}

// HDel removes fields and returns how many existed (HDEL). The hash is deleted when its
// last field is removed.
func (c *cmds) HDel(ctx context.Context, key string, fields ...string) (int64, error) {
	return c.intR(ctx, newCmd(2+len(fields), cHDEL).s(key).ss(fields))
}

// HLen returns the number of fields (HLEN), 0 for a missing key.
func (c *cmds) HLen(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(2, cHLEN).s(key))
}

// HGetAll returns every field and value as a map (HGETALL). A missing key gives an empty
// map. O(N): use HScan or HScanIter for big hashes. Map order is Go's; the server's
// order is unspecified.
func (c *cmds) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return c.strMap(ctx, newCmd(2, cHGETALL).s(key))
}

// HMGet returns the values of fields in order (HMGET); a field that does not exist (or a
// missing key) has Valid == false.
func (c *cmds) HMGet(ctx context.Context, key string, fields ...string) ([]NullString, error) {
	return c.nullStrs(ctx, newCmd(2+len(fields), cHMGET).s(key).ss(fields))
}

// HExists reports whether field exists (HEXISTS).
func (c *cmds) HExists(ctx context.Context, key, field string) (bool, error) {
	return c.boolR(ctx, newCmd(3, cHEXISTS).s(key).s(field))
}

// HKeys returns the field names (HKEYS). Order is unspecified by the server.
func (c *cmds) HKeys(ctx context.Context, key string) ([]string, error) {
	return c.strs(ctx, newCmd(2, cHKEYS).s(key))
}

// HVals returns the values (HVALS), in the same order as HKeys would.
func (c *cmds) HVals(ctx context.Context, key string) ([]string, error) {
	return c.strs(ctx, newCmd(2, cHVALS).s(key))
}

// HIncrBy adds delta to the integer in field and returns the new value (HINCRBY). A
// missing key or field counts as 0. A value that is not an integer is a server error.
// The field TTL is kept.
func (c *cmds) HIncrBy(ctx context.Context, key, field string, delta int64) (int64, error) {
	return c.intR(ctx, newCmd(4, cHINCRBY).s(key).s(field).i(delta))
}

// HIncrByWithTTL is HIncrBy that also sets a deadline on the key when the call creates
// it (HINCRBY key field delta EX|PX). EX is sent for whole seconds, PX otherwise; an
// existing hash keeps its deadline. A ttl of zero or less returns ErrInvalidTTL.
func (c *cmds) HIncrByWithTTL(ctx context.Context, key, field string, delta int64, ttl time.Duration) (int64, error) {
	tok, v, err := ttlArg(ttl)
	if err != nil {
		return 0, err
	}
	return c.intR(ctx, newCmd(6, cHINCRBY).s(key).s(field).i(delta).t(tok).i(v))
}

// HTransfer is Transfer between two integer fields of one hash (HTRANSFER key from to
// amount). A missing to field starts at 0 and field TTLs stay. ok is false, and nothing
// is written, when the key or the from field is missing or short or to would overflow.
func (c *cmds) HTransfer(ctx context.Context, key, from, to string, amount int64) (fromAfter, toAfter int64, ok bool, err error) {
	return c.pairNil(ctx, newCmd(5, cHTRANSFER).s(key).s(from).s(to).i(amount))
}

// HTransferMax is HTransfer that also refuses when to would end above max (HTRANSFER
// ... MAX max).
func (c *cmds) HTransferMax(ctx context.Context, key, from, to string, amount, max int64) (fromAfter, toAfter int64, ok bool, err error) {
	return c.pairNil(ctx, newCmd(7, cHTRANSFER).s(key).s(from).s(to).i(amount).t(tMAX).i(max))
}

// HIncrByFloat adds a float to field and returns the new value (HINCRBYFLOAT). A missing
// field counts as 0; a non-numeric value is a server error.
func (c *cmds) HIncrByFloat(ctx context.Context, key, field string, delta float64) (float64, error) {
	return c.floatR(ctx, newCmd(4, cHINCRBYFLOAT).s(key).s(field).f(delta))
}

// HStrLen returns the byte length of the field's value (HSTRLEN), 0 when the field or
// key is missing.
func (c *cmds) HStrLen(ctx context.Context, key, field string) (int64, error) {
	return c.intR(ctx, newCmd(3, cHSTRLEN).s(key).s(field))
}

// HScan returns one page of the hash as a flat field, value, field, value ... list and
// the next cursor (HSCAN). Start with cursor "0" and stop when the returned cursor is
// "0". o.Match filters field names; o.Count is a page size hint. A field can appear
// twice if the table was rebuilt during the walk. Use HScanIter to loop for you.
func (c *cmds) HScan(ctx context.Context, key, cursor string, o ScanOptions) (string, []string, error) {
	return c.keyScan(ctx, cHSCAN, key, cursor, o)
}

// HRandField returns count random field names (HRANDFIELD key count). A positive count
// gives distinct fields, a negative count gives |count| fields that may repeat (up to
// 1048576).
func (c *cmds) HRandField(ctx context.Context, key string, count int64) ([]string, error) {
	return c.strs(ctx, newCmd(3, cHRANDFIELD).s(key).i(count))
}

// HRandFieldWithValues is HRandField with WITHVALUES: it returns field, value, field,
// value ... as a flat list.
func (c *cmds) HRandFieldWithValues(ctx context.Context, key string, count int64) ([]string, error) {
	return c.strs(ctx, newCmd(4, cHRANDFIELD).s(key).i(count).t(tWITHVALUES))
}

// FieldExpireCond is the optional condition of the HEXPIRE family: "" (always), "NX"
// (only if the field has no TTL), "XX" (only if it has one), "GT" (only if the new TTL
// is later) or "LT" (only if earlier).
type FieldExpireCond string

func fieldExpReq(name []byte, key string, t int64, cond FieldExpireCond, fields []string) *request {
	n := 5 + len(fields)
	if cond != "" {
		n++
	}
	r := newCmd(n, name).s(key).i(t)
	if cond != "" {
		r.s(string(cond))
	}
	return r.t(tFIELDS).i(int64(len(fields))).ss(fields)
}

// HExpire sets a TTL on hash fields (HEXPIRE key seconds [cond] FIELDS n field ...). ttl
// is truncated to whole seconds and must not be negative. cond may be empty. It returns
// one code per field, in order: -2 the field (or key) does not exist, 0 the condition
// was not met, 1 the TTL was set, 2 the field was deleted because the time was 0 or
// already past. Expired fields disappear from every command and a hash whose fields have
// all expired counts as missing. At least one field is required; the largest accepted
// deadline is 2^46-1 ms.
func (c *cmds) HExpire(ctx context.Context, key string, ttl time.Duration, cond FieldExpireCond, fields ...string) ([]int64, error) {
	return c.ints(ctx, fieldExpReq(cHEXPIRE, key, int64(ttl/time.Second), cond, fields))
}

// HPExpire is HExpire with a TTL in milliseconds (HPEXPIRE); a positive sub-millisecond
// ttl is rounded up to 1 ms. Result codes as in HExpire.
func (c *cmds) HPExpire(ctx context.Context, key string, ttl time.Duration, cond FieldExpireCond, fields ...string) ([]int64, error) {
	return c.ints(ctx, fieldExpReq(cHPEXPIRE, key, ms(ttl), cond, fields))
}

// HExpireAt sets an absolute deadline on hash fields in Unix seconds (HEXPIREAT). Result
// codes as in HExpire; a time in the past deletes the fields (code 2).
func (c *cmds) HExpireAt(ctx context.Context, key string, at time.Time, cond FieldExpireCond, fields ...string) ([]int64, error) {
	return c.ints(ctx, fieldExpReq(cHEXPIREAT, key, at.Unix(), cond, fields))
}

// HPExpireAt sets an absolute deadline on hash fields in Unix milliseconds (HPEXPIREAT).
// Result codes as in HExpire.
func (c *cmds) HPExpireAt(ctx context.Context, key string, at time.Time, cond FieldExpireCond, fields ...string) ([]int64, error) {
	return c.ints(ctx, fieldExpReq(cHPEXPIREAT, key, at.UnixMilli(), cond, fields))
}

func fieldsReq(name []byte, key string, fields []string) *request {
	return newCmd(4+len(fields), name).s(key).t(tFIELDS).i(int64(len(fields))).ss(fields)
}

// HTTL returns the remaining TTL in seconds of each field (HTTL key FIELDS n field ...):
// -1 for a field without TTL and -2 for a missing field.
func (c *cmds) HTTL(ctx context.Context, key string, fields ...string) ([]int64, error) {
	return c.ints(ctx, fieldsReq(cHTTL, key, fields))
}

// HPTTL returns the remaining TTL in milliseconds of each field (-1 no TTL, -2 missing
// field).
func (c *cmds) HPTTL(ctx context.Context, key string, fields ...string) ([]int64, error) {
	return c.ints(ctx, fieldsReq(cHPTTL, key, fields))
}

// HExpireTime returns the absolute deadline of each field in Unix seconds (-1 no TTL, -2
// missing field).
func (c *cmds) HExpireTime(ctx context.Context, key string, fields ...string) ([]int64, error) {
	return c.ints(ctx, fieldsReq(cHEXPIRETIME, key, fields))
}

// HPExpireTime returns the absolute deadline of each field in Unix milliseconds (-1 no
// TTL, -2 missing field).
func (c *cmds) HPExpireTime(ctx context.Context, key string, fields ...string) ([]int64, error) {
	return c.ints(ctx, fieldsReq(cHPEXPIRETIME, key, fields))
}

// HPersist removes the TTL of each field (HPERSIST): 1 when a TTL was removed, -1 when
// the field had none, -2 when the field or key does not exist.
func (c *cmds) HPersist(ctx context.Context, key string, fields ...string) ([]int64, error) {
	return c.ints(ctx, fieldsReq(cHPERSIST, key, fields))
}
