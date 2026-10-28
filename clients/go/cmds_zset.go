// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"time"
)

// ZAddOptions are the ZADD modifiers; all false sends a plain ZADD.
//
// NX only adds new members and never updates; XX only updates existing members and never
// adds; GT updates only when the new score is greater than the current one (new members
// are still added); LT only when it is lower; CH makes the reply count changed members
// (added plus updated) instead of added members only. NX cannot be combined with XX, GT
// or LT, and GT cannot be combined with LT; the server answers "syntax error".
//
// TTL, when positive, sets a deadline on the key only if this call creates it (EX
// seconds for whole seconds, otherwise PX milliseconds); an existing key keeps its
// deadline. A negative TTL makes the call return ErrInvalidTTL.
type ZAddOptions struct {
	TTL time.Duration // deadline to set when the call creates the key
	NX  bool          // only add new members
	XX  bool          // only update existing members
	GT  bool          // update only when the new score is greater
	LT  bool          // update only when the new score is lower
	CH  bool          // count changed members, not only added ones
}

func (o *ZAddOptions) n() int {
	n := 0
	if o.TTL != 0 {
		n += 2
	}
	for _, b := range [...]bool{o.NX, o.XX, o.GT, o.LT, o.CH} {
		if b {
			n++
		}
	}
	return n
}

func (o *ZAddOptions) append(r *request) {
	if tok, v, _ := optTTL(o.TTL); tok != nil {
		r.t(tok).i(v)
	}
	if o.NX {
		r.t(tNX)
	}
	if o.XX {
		r.t(tXX)
	}
	if o.GT {
		r.t(tGT)
	}
	if o.LT {
		r.t(tLT)
	}
	if o.CH {
		r.t(tCH)
	}
}

// ZAdd adds members with scores to the sorted set at key and returns how many members
// were new (ZADD). Members that already exist get their score updated and are not
// counted. A missing key is created. Scores are float64 (+Inf and -Inf allowed, NaN
// rejected with "score cannot be NaN"). Members are ordered by score, then bytewise by
// name. A key of another type returns WRONGTYPE.
//
//	n, err := c.ZAdd(ctx, "board", ruvio.Z{Member: "ada", Score: 10}, ruvio.Z{Member: "bob", Score: 7})
func (c *cmds) ZAdd(ctx context.Context, key string, members ...Z) (int64, error) {
	return c.ZAddArgs(ctx, key, ZAddOptions{}, members...)
}

// ZAddArgs is ZADD with the modifiers in o. The result is the number of new members, or
// with o.CH the number of members added or whose score changed. With NX or XX or GT or
// LT it can be 0 because nothing qualified.
func (c *cmds) ZAddArgs(ctx context.Context, key string, o ZAddOptions, members ...Z) (int64, error) {
	if _, _, err := optTTL(o.TTL); err != nil {
		return 0, err
	}
	r := newCmd(2+o.n()+2*len(members), cZADD).s(key)
	o.append(r)
	for _, m := range members {
		r.f(m.Score).s(m.Member)
	}
	return c.intR(ctx, r)
}

// ZAddIncr is ZADD ... INCR: it adds member.Score to the member's current score (a
// missing member starts at 0) and returns the new score. ok is false when a condition in
// o (NX, XX, GT, LT) stopped the update. Only one member is allowed.
func (c *cmds) ZAddIncr(ctx context.Context, key string, o ZAddOptions, member Z) (float64, bool, error) {
	if _, _, err := optTTL(o.TTL); err != nil {
		return 0, false, err
	}
	r := newCmd(5+o.n(), cZADD).s(key)
	o.append(r)
	r.t(tINCR).f(member.Score).s(member.Member)
	return c.floatNil(ctx, r)
}

// ZRem removes members and returns how many existed (ZREM). The key is deleted when the
// set becomes empty.
func (c *cmds) ZRem(ctx context.Context, key string, members ...string) (int64, error) {
	return c.intR(ctx, newCmd(2+len(members), cZREM).s(key).ss(members))
}

// ZIncrBy adds delta to the score of member and returns the new score (ZINCRBY). A
// missing member is added with score delta; a missing key is created.
func (c *cmds) ZIncrBy(ctx context.Context, key string, delta float64, member string) (float64, error) {
	return c.floatR(ctx, newCmd(4, cZINCRBY).s(key).f(delta).s(member))
}

// ZIncrByWithTTL is ZIncrBy that also sets a deadline when it creates the key (ZINCRBY
// key delta member EX|PX). An existing key keeps its deadline. A ttl of zero or less
// returns ErrInvalidTTL.
func (c *cmds) ZIncrByWithTTL(ctx context.Context, key string, delta float64, member string, ttl time.Duration) (float64, error) {
	tok, v, err := ttlArg(ttl)
	if err != nil {
		return 0, err
	}
	return c.floatR(ctx, newCmd(6, cZINCRBY).s(key).f(delta).s(member).t(tok).i(v))
}

// ZCard returns the number of members (ZCARD), 0 for a missing key.
func (c *cmds) ZCard(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(2, cZCARD).s(key))
}

// ZScore returns the score of member (ZSCORE). ok is false when the member or the key is
// missing.
func (c *cmds) ZScore(ctx context.Context, key, member string) (float64, bool, error) {
	return c.floatNil(ctx, newCmd(3, cZSCORE).s(key).s(member))
}

// ZMScore returns the scores of members in order (ZMSCORE); Valid is false for members
// that do not exist.
func (c *cmds) ZMScore(ctx context.Context, key string, members ...string) ([]NullFloat, error) {
	v, err := c.value(ctx, newCmd(2+len(members), cZMSCORE).s(key).ss(members))
	if err != nil {
		return nil, err
	}
	if !v.isAgg() {
		return nil, unexpected(v)
	}
	out := make([]NullFloat, len(v.Array))
	for i, e := range v.Array {
		if e.Kind != KindNil {
			f, err := e.AsFloat64()
			if err != nil {
				return nil, err
			}
			out[i] = NullFloat{f, true}
		}
	}
	return out, nil
}

// ZRank returns the 0-based position of member when sorted by ascending score (ZRANK).
// ok is false when the member or key is missing.
func (c *cmds) ZRank(ctx context.Context, key, member string) (int64, bool, error) {
	return c.intNil(ctx, newCmd(3, cZRANK).s(key).s(member))
}

// ZRevRank returns the 0-based position of member when sorted by descending score
// (ZREVRANK); ok is false when missing.
func (c *cmds) ZRevRank(ctx context.Context, key, member string) (int64, bool, error) {
	return c.intNil(ctx, newCmd(3, cZREVRANK).s(key).s(member))
}

// ZCount counts members whose score is between min and max (ZCOUNT). The bounds are
// strings: a number such as "1.5" (inclusive), "(1.5" (exclusive), "-inf" or "+inf".
//
//	n, err := c.ZCount(ctx, "board", "(5", "+inf") // score > 5
func (c *cmds) ZCount(ctx context.Context, key, min, max string) (int64, error) {
	return c.intR(ctx, newCmd(4, cZCOUNT).s(key).s(min).s(max))
}

// ZLexCount counts members between two lexicographic bounds (ZLEXCOUNT). Bounds are "-"
// (lowest), "+" (highest), "[member" (inclusive) or "(member" (exclusive). It is
// meaningful when all members have the same score. Other bound forms return "min or max
// not valid string range item".
func (c *cmds) ZLexCount(ctx context.Context, key, min, max string) (int64, error) {
	return c.intR(ctx, newCmd(4, cZLEXCOUNT).s(key).s(min).s(max))
}

// ZRangeArgs is the full ZRANGE form (ZRANGE key start stop [BYSCORE|BYLEX] [REV] [LIMIT
// offset count]).
//
// By default Start and Stop are 0-based rank indexes, negative counting from the end.
// With ByScore they are score bounds ("1", "(1", "-inf", "+inf") and with ByLex lex
// bounds ("-", "+", "[a", "(a"); ByScore and ByLex are mutually exclusive. With Rev the
// order is reversed, and for ByScore or ByLex Start is the higher bound and Stop the
// lower one (as in Redis). Limit is only allowed together with ByScore or ByLex;
// otherwise the server returns a syntax error.
type ZRangeArgs struct {
	Key   string // sorted set
	Start string // rank index, score bound or lex bound (see above)
	// Stop is the end of the range, in the same form as Start.
	Stop    string
	ByScore bool   // Start and Stop are score bounds
	ByLex   bool   // Start and Stop are lexicographic bounds
	Rev     bool   // reverse order
	Limit   *Limit // optional paging; needs ByScore or ByLex
}

// Limit is the LIMIT offset count of range commands: skip Offset members, then return at
// most Count. A negative Count returns everything from the offset on. Offset must be >=
// 0.
type Limit struct {
	Offset int64 // members to skip, >= 0
	Count  int64 // members to return; negative means all the rest
}

func zrangeReq(a *ZRangeArgs, withScores bool) *request {
	n := 4
	if a.ByScore || a.ByLex {
		n++
	}
	if a.Rev {
		n++
	}
	if a.Limit != nil {
		n += 3
	}
	if withScores {
		n++
	}
	r := newCmd(n, cZRANGE).s(a.Key).s(a.Start).s(a.Stop)
	if a.ByScore {
		r.t(tBYSCORE)
	} else if a.ByLex {
		r.t(tBYLEX)
	}
	if a.Rev {
		r.t(tREV)
	}
	if a.Limit != nil {
		r.t(tLIMIT).i(a.Limit.Offset).i(a.Limit.Count)
	}
	if withScores {
		r.t(tWITHSCORES)
	}
	return r
}

// ZRange returns members by rank from start to stop, both inclusive, lowest score first
// (ZRANGE). Negative indexes count from the end, so 0 and -1 is the whole set. A missing
// key gives an empty result.
func (c *cmds) ZRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return c.strs(ctx, newCmd(4, cZRANGE).s(key).i(start).i(stop))
}

// ZRangeWithScores is ZRange WITHSCORES: it returns each member with its score.
func (c *cmds) ZRangeWithScores(ctx context.Context, key string, start, stop int64) ([]Z, error) {
	return c.zs(ctx, newCmd(5, cZRANGE).s(key).i(start).i(stop).t(tWITHSCORES))
}

// ZRangeArgs runs the full ZRANGE form described by a and returns member names. See the
// ZRangeArgs type for the meaning of Start, Stop, ByScore, ByLex, Rev and Limit.
func (c *cmds) ZRangeArgs(ctx context.Context, a ZRangeArgs) ([]string, error) {
	return c.strs(ctx, zrangeReq(&a, false))
}

// ZRangeArgsWithScores is ZRangeArgs with WITHSCORES. WITHSCORES is not valid with
// ByLex.
func (c *cmds) ZRangeArgsWithScores(ctx context.Context, a ZRangeArgs) ([]Z, error) {
	return c.zs(ctx, zrangeReq(&a, true))
}

// ZRevRange returns members by rank from start to stop, highest score first (ZREVRANGE);
// index 0 is the highest-scored member.
func (c *cmds) ZRevRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return c.strs(ctx, newCmd(4, cZREVRANGE).s(key).i(start).i(stop))
}

// ZRevRangeWithScores is ZRevRange WITHSCORES.
func (c *cmds) ZRevRangeWithScores(ctx context.Context, key string, start, stop int64) ([]Z, error) {
	return c.zs(ctx, newCmd(5, cZREVRANGE).s(key).i(start).i(stop).t(tWITHSCORES))
}

func rangeByReq(name []byte, key, min, max string, lim *Limit, withScores bool) *request {
	n := 4
	if lim != nil {
		n += 3
	}
	if withScores {
		n++
	}
	r := newCmd(n, name).s(key).s(min).s(max)
	if withScores {
		r.t(tWITHSCORES)
	}
	if lim != nil {
		r.t(tLIMIT).i(lim.Offset).i(lim.Count)
	}
	return r
}

// ZRangeByScore returns members with min <= score <= max, lowest first (ZRANGEBYSCORE).
// min and max are strings: a number, "(number" for exclusive, "-inf" or "+inf". lim may
// be nil; otherwise it skips and caps the result. For highest first use ZRangeArgs with
// ByScore and Rev.
//
//	top, err := c.ZRangeByScore(ctx, "board", "5", "+inf", &ruvio.Limit{Offset: 0, Count: 10})
func (c *cmds) ZRangeByScore(ctx context.Context, key, min, max string, lim *Limit) ([]string, error) {
	return c.strs(ctx, rangeByReq(cZRANGEBYSCORE, key, min, max, lim, false))
}

// ZRangeByScoreWithScores is ZRangeByScore WITHSCORES.
func (c *cmds) ZRangeByScoreWithScores(ctx context.Context, key, min, max string, lim *Limit) ([]Z, error) {
	return c.zs(ctx, rangeByReq(cZRANGEBYSCORE, key, min, max, lim, true))
}

// ZRangeByLex returns members within a lexicographic range, lowest first (ZRANGEBYLEX).
// Bounds are "-", "+", "[member" (inclusive) or "(member" (exclusive). Use it when all
// members share one score. lim may be nil.
func (c *cmds) ZRangeByLex(ctx context.Context, key, min, max string, lim *Limit) ([]string, error) {
	return c.strs(ctx, rangeByReq(cZRANGEBYLEX, key, min, max, lim, false))
}

// ZRevRangeByLex is ZRangeByLex in reverse order (ZREVRANGEBYLEX). As in Redis, the
// higher bound comes first: pass max, then min.
func (c *cmds) ZRevRangeByLex(ctx context.Context, key, max, min string, lim *Limit) ([]string, error) {
	return c.strs(ctx, rangeByReq(cZREVRANGEBYLEX, key, max, min, lim, false))
}

// ZPopMin removes and returns the lowest-scored members (ZPOPMIN). count <= 0 sends no
// count and pops one member; otherwise up to count. The result is empty when the key is
// missing or empty. Members come lowest first.
func (c *cmds) ZPopMin(ctx context.Context, key string, count int64) ([]Z, error) {
	if count <= 0 {
		return c.zs(ctx, newCmd(2, cZPOPMIN).s(key))
	}
	return c.zs(ctx, newCmd(3, cZPOPMIN).s(key).i(count))
}

// ZPopMax removes and returns the highest-scored members (ZPOPMAX); count rules are
// those of ZPopMin. Members come highest first.
func (c *cmds) ZPopMax(ctx context.Context, key string, count int64) ([]Z, error) {
	if count <= 0 {
		return c.zs(ctx, newCmd(2, cZPOPMAX).s(key))
	}
	return c.zs(ctx, newCmd(3, cZPOPMAX).s(key).i(count))
}

// ZPopByScore removes and returns up to count members (1 to 10000) whose score is between
// min and max, lowest score first (ZPOPBYSCORE ... WITHSCORES). With scores set to due times
// this takes the jobs that are ready. Bounds use the ZCount syntax: -inf, +inf, and a ( prefix
// for exclusive. The result is empty when nothing is due.
func (c *cmds) ZPopByScore(ctx context.Context, key, min, max string, count int64) ([]Z, error) {
	return c.zs(ctx, newCmd(8, cZPOPBYSCORE).s(key).s(max).s("MIN").s(min).t(tCOUNT).i(count).t(tWITHSCORES))
}

// ZMoveDue moves up to count members (1 to 10000) whose score is at most max from the sorted
// set source to the tail of the list destination, lowest score first, and returns how many
// moved (ZMOVEDUE). Clients blocked on destination are served first. In cluster mode both
// keys must share a slot.
func (c *cmds) ZMoveDue(ctx context.Context, source, destination, max string, count int64) (int64, error) {
	return c.intR(ctx, newCmd(6, cZMOVEDUE).s(source).s(destination).s(max).t(tCOUNT).i(count))
}

// KeyZ is the reply of BZPopMin and BZPopMax: the sorted set the member came from, with
// the member and its score (the embedded Z).
type KeyZ struct {
	// Key is the sorted set the member was popped from.
	Key string
	Z
}

func (c *cmds) bzpop(ctx context.Context, name []byte, timeout time.Duration, keys []string) (KeyZ, bool, error) {
	v, err := c.ex.execBlocking(ctx, newCmd(2+len(keys), name).ss(keys).f(secs(timeout)))
	if err != nil || v.Kind == KindNil {
		return KeyZ{}, false, err
	}
	if len(v.Array) != 3 {
		return KeyZ{}, false, unexpected(v)
	}
	f, err := v.Array[2].AsFloat64()
	return KeyZ{v.Array[0].Str(), Z{v.Array[1].Str(), f}}, err == nil, err
}

// BZPopMin waits up to timeout for a member to pop from the first non-empty sorted set
// among keys (BZPOPMIN), taking the lowest score. ok is false when the timeout passed.
// timeout 0 waits forever, until ctx ends or the server shuts down. It runs on a
// dedicated connection, so other calls keep flowing; a ZADD, ZINCRBY or GEOADD serves
// the oldest waiter. At most 1024 waiters per shard. In cluster mode all keys must share
// a slot.
func (c *cmds) BZPopMin(ctx context.Context, timeout time.Duration, keys ...string) (KeyZ, bool, error) {
	return c.bzpop(ctx, cBZPOPMIN, timeout, keys)
}

// BZPopMax is BZPopMin taking the highest score (BZPOPMAX). Same timeout, ok and cluster
// rules.
func (c *cmds) BZPopMax(ctx context.Context, timeout time.Duration, keys ...string) (KeyZ, bool, error) {
	return c.bzpop(ctx, cBZPOPMAX, timeout, keys)
}

// ZRemRangeByRank removes members with rank from start to stop, both inclusive
// (ZREMRANGEBYRANK; negative counts from the end) and returns how many were removed.
func (c *cmds) ZRemRangeByRank(ctx context.Context, key string, start, stop int64) (int64, error) {
	return c.intR(ctx, newCmd(4, cZREMRANGEBYRANK).s(key).i(start).i(stop))
}

// ZRemRangeByScore removes members whose score is between min and max (ZREMRANGEBYSCORE;
// same bound syntax as ZCount) and returns how many were removed.
func (c *cmds) ZRemRangeByScore(ctx context.Context, key, min, max string) (int64, error) {
	return c.intR(ctx, newCmd(4, cZREMRANGEBYSCORE).s(key).s(min).s(max))
}

// ZRemRangeByLex removes members in a lexicographic range (ZREMRANGEBYLEX; bounds as in
// ZLexCount) and returns how many were removed.
func (c *cmds) ZRemRangeByLex(ctx context.Context, key, min, max string) (int64, error) {
	return c.intR(ctx, newCmd(4, cZREMRANGEBYLEX).s(key).s(min).s(max))
}

// ZStore describes the inputs of ZUNION, ZINTER and their STORE forms. Keys are the
// source sorted sets. Weights is empty, or has exactly one factor per key that
// multiplies that set's scores first. Aggregate says how scores of a member present in
// several sets are combined: AggSum (the default when empty), AggMin or AggMax. Aggregate
// and Weights are not used by the difference commands.
type ZStore struct {
	Keys      []string  // source sorted sets
	Weights   []float64 // empty, or one multiplier per key
	Aggregate Aggregate // "", AggSum, AggMin or AggMax
}

func zsetOpReq(name []byte, dst string, z *ZStore, withScores bool) *request {
	n := 2 + len(z.Keys)
	if dst != "" {
		n++
	}
	if len(z.Weights) > 0 {
		n += 1 + len(z.Weights)
	}
	if z.Aggregate != "" {
		n += 2
	}
	if withScores {
		n++
	}
	r := newCmd(n, name)
	if dst != "" {
		r.s(dst)
	}
	r.i(int64(len(z.Keys))).ss(z.Keys)
	if len(z.Weights) > 0 {
		r.t(tWEIGHTS)
		for _, w := range z.Weights {
			r.f(w)
		}
	}
	if z.Aggregate != "" {
		r.t(tAGGREGATE).kw(z.Aggregate.token(), string(z.Aggregate))
	}
	if withScores {
		r.t(tWITHSCORES)
	}
	return r
}

// ZUnionStore stores the union of the sets in z at dst, replacing it, and returns the
// number of members stored (ZUNIONSTORE). All source keys and dst must share a hash slot
// in cluster mode (ErrCrossSlot otherwise).
func (c *cmds) ZUnionStore(ctx context.Context, dst string, z ZStore) (int64, error) {
	return c.intR(ctx, zsetOpReq(cZUNIONSTORE, dst, &z, false))
}

// ZInterStore stores the intersection of the sets in z at dst and returns the number of
// members stored (ZINTERSTORE). Same-slot rule in cluster mode.
func (c *cmds) ZInterStore(ctx context.Context, dst string, z ZStore) (int64, error) {
	return c.intR(ctx, zsetOpReq(cZINTERSTORE, dst, &z, false))
}

// ZDiffStore stores the members of the first set that are in none of the others at dst
// and returns the number stored (ZDIFFSTORE). Same-slot rule in cluster mode.
func (c *cmds) ZDiffStore(ctx context.Context, dst string, keys ...string) (int64, error) {
	return c.intR(ctx, zsetOpReq(cZDIFFSTORE, dst, &ZStore{Keys: keys}, false))
}

// ZUnion returns the member names of the union of the sets in z (ZUNION). Same-slot rule
// in cluster mode.
func (c *cmds) ZUnion(ctx context.Context, z ZStore) ([]string, error) {
	return c.strs(ctx, zsetOpReq(cZUNION, "", &z, false))
}

// ZUnionWithScores is ZUnion WITHSCORES: members with their combined scores.
func (c *cmds) ZUnionWithScores(ctx context.Context, z ZStore) ([]Z, error) {
	return c.zs(ctx, zsetOpReq(cZUNION, "", &z, true))
}

// ZInter returns the members present in every set of z (ZINTER).
func (c *cmds) ZInter(ctx context.Context, z ZStore) ([]string, error) {
	return c.strs(ctx, zsetOpReq(cZINTER, "", &z, false))
}

// ZInterWithScores is ZInter WITHSCORES.
func (c *cmds) ZInterWithScores(ctx context.Context, z ZStore) ([]Z, error) {
	return c.zs(ctx, zsetOpReq(cZINTER, "", &z, true))
}

// ZDiff returns the members of the first set that are in none of the others (ZDIFF).
func (c *cmds) ZDiff(ctx context.Context, keys ...string) ([]string, error) {
	return c.strs(ctx, zsetOpReq(cZDIFF, "", &ZStore{Keys: keys}, false))
}

// ZDiffWithScores is ZDiff WITHSCORES.
func (c *cmds) ZDiffWithScores(ctx context.Context, keys ...string) ([]Z, error) {
	return c.zs(ctx, zsetOpReq(cZDIFF, "", &ZStore{Keys: keys}, true))
}

// ZRandMember returns random members without removing them (ZRANDMEMBER key count). A
// positive count gives distinct members (at most the set size); a negative count gives
// |count| members that may repeat, up to 1048576 (larger values return "count is out of
// range"). A missing key gives an empty result.
func (c *cmds) ZRandMember(ctx context.Context, key string, count int64) ([]string, error) {
	return c.strs(ctx, newCmd(3, cZRANDMEMBER).s(key).i(count))
}

// ZRandMemberWithScores is ZRandMember WITHSCORES.
func (c *cmds) ZRandMemberWithScores(ctx context.Context, key string, count int64) ([]Z, error) {
	return c.zs(ctx, newCmd(4, cZRANDMEMBER).s(key).i(count).t(tWITHSCORES))
}

// ZScan returns one page of the sorted set as a flat member, score, member, score ...
// list and the next cursor (ZSCAN). Start with cursor "0" and stop at "0". Pages follow
// score order by position, so a concurrent write can shift members between pages.
// o.Match filters member names; o.Count is a size hint. Use ZScanIter to loop for you.
func (c *cmds) ZScan(ctx context.Context, key, cursor string, o ScanOptions) (string, []string, error) {
	return c.keyScan(ctx, cZSCAN, key, cursor, o)
}

// ---- geo -----------------------------------------------------------------------------

// GeoLocation describes a member for GeoAdd and is also what the search methods return.
// When adding, set Name, Longitude and Latitude. In search results Name is always set;
// Longitude/Latitude are filled with WithCoord, Dist with WithDist (in the query's unit)
// and GeoHash (the raw 52-bit integer geohash score) with WithHash; fields not requested
// stay zero.
type GeoLocation struct {
	Name      string  // member name
	Longitude float64 // degrees, -180 to 180
	Latitude  float64 // degrees, -85.05112878 to 85.05112878
	Dist      float64 // distance from the center (WithDist), in the query unit
	GeoHash   int64   // raw 52-bit geohash score (WithHash)
}

// GeoAddOptions are the GEOADD modifiers. NX only adds new members, XX only updates
// existing ones (NX and XX together are a server syntax error), CH makes the result
// count changed members as well as new ones. TTL, when positive, sets a deadline on the
// key only if this call creates it (EX for whole seconds, otherwise PX); an existing key
// keeps its deadline, and a negative TTL returns ErrInvalidTTL.
type GeoAddOptions struct {
	TTL time.Duration // deadline to set when the call creates the key
	NX  bool          // only add new members
	XX  bool          // only update existing members
	CH  bool          // count changed members as well as new ones
}

// GeoAdd adds members with coordinates to the geo index at key (GEOADD) and returns how
// many members were new (or, with o.CH, new or moved). The index is a sorted set whose
// score is a 52-bit geohash, so Type reports "zset" and ZRange etc. work on it.
//
// Longitude is in degrees from -180 to 180 and latitude from -85.05112878 to
// 85.05112878; a pair outside these ranges returns "ERR invalid longitude,latitude pair"
// and the whole call writes nothing. At least one location is required. A key of another
// type returns WRONGTYPE.
//
//	n, err := c.GeoAdd(ctx, "shops", ruvio.GeoAddOptions{},
//	    ruvio.GeoLocation{Name: "central", Longitude: 13.361389, Latitude: 38.115556},
//	    ruvio.GeoLocation{Name: "harbour", Longitude: 15.087269, Latitude: 37.502669})
func (c *cmds) GeoAdd(ctx context.Context, key string, o GeoAddOptions, locs ...GeoLocation) (int64, error) {
	ttok, tv, err := optTTL(o.TTL)
	if err != nil {
		return 0, err
	}
	n := 2 + 3*len(locs)
	if ttok != nil {
		n += 2
	}
	for _, b := range [...]bool{o.NX, o.XX, o.CH} {
		if b {
			n++
		}
	}
	r := newCmd(n, cGEOADD).s(key)
	if ttok != nil {
		r.t(ttok).i(tv)
	}
	if o.NX {
		r.t(tNX)
	}
	if o.XX {
		r.t(tXX)
	}
	if o.CH {
		r.t(tCH)
	}
	for _, l := range locs {
		r.f(l.Longitude).f(l.Latitude).s(l.Name)
	}
	return c.intR(ctx, r)
}

// GeoDist returns the distance between two members (GEODIST). unit is GeoMeters (also
// the default when empty), GeoKilometers, GeoFeet or GeoMiles; any other unit returns "unsupported unit
// provided. please use M, KM, FT, MI". ok is false when either member or the key is
// missing. Distances are great-circle estimates.
func (c *cmds) GeoDist(ctx context.Context, key, m1, m2 string, unit GeoUnit) (float64, bool, error) {
	if unit == "" {
		return c.floatNil(ctx, newCmd(4, cGEODIST).s(key).s(m1).s(m2))
	}
	return c.floatNil(ctx, newCmd(5, cGEODIST).s(key).s(m1).s(m2).kw(unit.token(), string(unit)))
}

// GeoHash returns the standard 11-character geohash string of each member (GEOHASH); an
// entry has Valid == false for a missing member.
func (c *cmds) GeoHash(ctx context.Context, key string, members ...string) ([]NullString, error) {
	return c.nullStrs(ctx, newCmd(2+len(members), cGEOHASH).s(key).ss(members))
}

// GeoPos is one result of GeoPosition; Valid is false for a member that does not exist.
type GeoPos struct {
	Longitude, Latitude float64 // degrees
	Valid               bool    // false when the member does not exist
}

// GeoPosition returns the longitude and latitude of each member (GEOPOS), in order, with
// Valid == false for missing members. The coordinates are those of the stored geohash
// cell, so they can differ slightly from the values that were added.
func (c *cmds) GeoPosition(ctx context.Context, key string, members ...string) ([]GeoPos, error) {
	v, err := c.value(ctx, newCmd(2+len(members), cGEOPOS).s(key).ss(members))
	if err != nil {
		return nil, err
	}
	out := make([]GeoPos, len(v.Array))
	for i, e := range v.Array {
		if len(e.Array) == 2 {
			lon, _ := e.Array[0].AsFloat64()
			lat, _ := e.Array[1].AsFloat64()
			out[i] = GeoPos{lon, lat, true}
		}
	}
	return out, nil
}

// GeoSearchQuery describes GEOSEARCH and GEOSEARCHSTORE.
//
// Center: set Member to search around an existing member (FROMMEMBER), otherwise
// Longitude and Latitude are used (FROMLONLAT). Shape: a Radius greater than 0 searches
// a circle (BYRADIUS); otherwise Width and Height define a box (BYBOX). Unit (GeoUnit) is
// GeoMeters (default when empty), GeoKilometers, GeoFeet or GeoMiles for the Radius,
// Width and Height. Sort (GeoOrder) is GeoAsc or GeoDesc by distance from the center,
// or empty. Count > 0 limits the number of
// results; the nearest are kept and, when Count is given without Any, the result is
// sorted ascending. Any with Count returns the first matches found, in no particular
// order, which is faster. Any without Count is an error. WithDist, WithHash and
// WithCoord add the distance, raw geohash integer and coordinates to each result (they
// are not allowed for the store form).
type GeoSearchQuery struct {
	Member              string   // FROMMEMBER center; empty means use Longitude/Latitude
	Longitude, Latitude float64  // FROMLONLAT center
	Radius              float64  // BYRADIUS when > 0
	Width, Height       float64  // BYBOX when Radius is 0
	Unit                GeoUnit  // GeoMeters (default), GeoKilometers, GeoFeet or GeoMiles
	Sort                GeoOrder // "", GeoAsc or GeoDesc
	Count               int64    // max results when > 0
	Any                 bool     // with Count: return the first matches found
	WithDist            bool     // fill GeoLocation.Dist
	WithHash            bool     // fill GeoLocation.GeoHash
	WithCoord           bool     // fill GeoLocation.Longitude and Latitude
}

func (q *GeoSearchQuery) unit(r *request) {
	r.kw(q.Unit.token(), string(q.Unit))
}

func (q *GeoSearchQuery) n(store bool) int {
	n := 2 // FROM...
	if q.Member == "" {
		n++
	}
	if q.Radius > 0 {
		n += 3
	} else {
		n += 4
	}
	if q.Sort != "" {
		n++
	}
	if q.Count > 0 {
		n += 2
		if q.Any {
			n++
		}
	}
	if !store {
		for _, b := range [...]bool{q.WithDist, q.WithHash, q.WithCoord} {
			if b {
				n++
			}
		}
	}
	return n
}

func (q *GeoSearchQuery) append(r *request, store bool) {
	if q.Member != "" {
		r.t(tFROMMEMBER).s(q.Member)
	} else {
		r.t(tFROMLONLAT).f(q.Longitude).f(q.Latitude)
	}
	if q.Radius > 0 {
		r.t(tBYRADIUS).f(q.Radius)
		q.unit(r)
	} else {
		r.t(tBYBOX).f(q.Width).f(q.Height)
		q.unit(r)
	}
	if q.Sort != "" {
		r.kw(q.Sort.token(), string(q.Sort))
	}
	if q.Count > 0 {
		r.t(tCOUNT).i(q.Count)
		if q.Any {
			r.t(tANY)
		}
	}
	if !store {
		if q.WithDist {
			r.t(tWITHDIST)
		}
		if q.WithHash {
			r.t(tWITHHASH)
		}
		if q.WithCoord {
			r.t(tWITHCOORD)
		}
	}
}

func toGeoLocations(v Value, dist, hash, coord bool) ([]GeoLocation, error) {
	if !v.isAgg() {
		return nil, unexpected(v)
	}
	out := make([]GeoLocation, len(v.Array))
	for i, e := range v.Array {
		if !e.isAgg() {
			out[i].Name = e.Str()
			continue
		}
		j := 0
		if len(e.Array) == 0 {
			return nil, unexpected(v)
		}
		out[i].Name = e.Array[j].Str()
		j++
		if dist && j < len(e.Array) {
			out[i].Dist, _ = e.Array[j].AsFloat64()
			j++
		}
		if hash && j < len(e.Array) {
			out[i].GeoHash, _ = e.Array[j].AsInt64()
			j++
		}
		if coord && j < len(e.Array) && len(e.Array[j].Array) == 2 {
			out[i].Longitude, _ = e.Array[j].Array[0].AsFloat64()
			out[i].Latitude, _ = e.Array[j].Array[1].AsFloat64()
		}
	}
	return out, nil
}

// GeoSearch returns the members inside the area described by q (GEOSEARCH key ...).
// Without Sort or Count the order is unspecified. A missing key gives an empty result; a
// Member that is not in the set returns "ERR could not decode requested zset member".
// Radius and box dimensions must not be negative; a coordinate pair out of range returns
// "ERR invalid longitude,latitude pair".
//
//	shops, err := c.GeoSearch(ctx, "shops", ruvio.GeoSearchQuery{
//	    Longitude: 15, Latitude: 37, Radius: 200, Unit: ruvio.GeoKilometers,
//	    Sort: ruvio.GeoAsc, Count: 5, WithDist: true,
//	})
func (c *cmds) GeoSearch(ctx context.Context, key string, q GeoSearchQuery) ([]GeoLocation, error) {
	r := newCmd(2+q.n(false), cGEOSEARCH).s(key)
	q.append(r, false)
	v, err := c.value(ctx, r)
	if err != nil {
		return nil, err
	}
	return toGeoLocations(v, q.WithDist, q.WithHash, q.WithCoord)
}

// GeoSearchStore runs the search of q on src and stores the result in dst, replacing it,
// then returns the number of members stored (GEOSEARCHSTORE). dst is a geo index with
// the same coordinates. With storeDist the stored score is the distance from the center
// in the query's unit instead of the geohash. The With... fields of q must be false. dst
// and src must share a hash slot in cluster mode.
func (c *cmds) GeoSearchStore(ctx context.Context, dst, src string, q GeoSearchQuery, storeDist bool) (int64, error) {
	n := 3 + q.n(true)
	if storeDist {
		n++
	}
	r := newCmd(n, cGEOSEARCHSTORE).s(dst).s(src)
	q.append(r, true)
	if storeDist {
		r.t(tSTOREDIST)
	}
	return c.intR(ctx, r)
}

// GeoRadiusQuery holds the options of GEORADIUS and GEORADIUSBYMEMBER. Radius is a
// distance in Unit (GeoUnit; GeoMeters by default, GeoKilometers, GeoFeet, GeoMiles) and
// must not be negative. Sort (GeoOrder) is GeoAsc, GeoDesc or empty; Count > 0 limits the result (and sorts ascending unless Any is
// set); Any needs Count. WithDist, WithHash and WithCoord add details to each result and
// cannot be used with the store forms.
type GeoRadiusQuery struct {
	Radius    float64  // search distance, >= 0
	Unit      GeoUnit  // GeoMeters (default), GeoKilometers, GeoFeet or GeoMiles
	Sort      GeoOrder // "", GeoAsc or GeoDesc
	Count     int64    // max results when > 0
	Any       bool     // with Count: return the first matches found
	WithDist  bool     // fill GeoLocation.Dist
	WithHash  bool     // fill GeoLocation.GeoHash
	WithCoord bool     // fill GeoLocation.Longitude and Latitude
}

func (q *GeoRadiusQuery) build(r *request, store, storeDist string) {
	r.f(q.Radius).kw(q.Unit.token(), string(q.Unit))
	if store == "" {
		if q.WithDist {
			r.t(tWITHDIST)
		}
		if q.WithHash {
			r.t(tWITHHASH)
		}
		if q.WithCoord {
			r.t(tWITHCOORD)
		}
	}
	if q.Count > 0 {
		r.t(tCOUNT).i(q.Count)
		if q.Any {
			r.t(tANY)
		}
	}
	if q.Sort != "" {
		r.kw(q.Sort.token(), string(q.Sort))
	}
	if store != "" {
		r.t(tSTORE).s(store)
	}
	if storeDist != "" {
		r.t(tSTOREDIST).s(storeDist)
	}
}

func (q *GeoRadiusQuery) n(store, storeDist string) int {
	n := 2
	if store == "" {
		for _, b := range [...]bool{q.WithDist, q.WithHash, q.WithCoord} {
			if b {
				n++
			}
		}
	} else {
		n += 2
	}
	if storeDist != "" {
		n += 2
	}
	if q.Count > 0 {
		n += 2
		if q.Any {
			n++
		}
	}
	if q.Sort != "" {
		n++
	}
	return n
}

// GeoRadius returns members within q.Radius of the point (lon, lat) (GEORADIUS). It is
// the older form of GeoSearch; prefer GeoSearch for new code. Same result rules as
// GeoSearch.
func (c *cmds) GeoRadius(ctx context.Context, key string, lon, lat float64, q GeoRadiusQuery) ([]GeoLocation, error) {
	r := newCmd(4+q.n("", ""), cGEORADIUS).s(key).f(lon).f(lat)
	q.build(r, "", "")
	v, err := c.value(ctx, r)
	if err != nil {
		return nil, err
	}
	return toGeoLocations(v, q.WithDist, q.WithHash, q.WithCoord)
}

// GeoRadiusStore is GeoRadius that stores the result: store receives the members with
// their geohash scores (STORE), storeDist receives them with the distance as score
// (STOREDIST). Set at least one (an empty name is not sent). It returns the number of
// members stored. Keys must share a hash slot in cluster mode.
func (c *cmds) GeoRadiusStore(ctx context.Context, key string, lon, lat float64, q GeoRadiusQuery, store, storeDist string) (int64, error) {
	r := newCmd(4+q.n(store, storeDist), cGEORADIUS).s(key).f(lon).f(lat)
	q.build(r, store, storeDist)
	return c.intR(ctx, r)
}

// GeoRadiusByMember is GeoRadius around an existing member of the set instead of a
// coordinate (GEORADIUSBYMEMBER). A member that is not in the set returns "ERR could not
// decode requested zset member".
func (c *cmds) GeoRadiusByMember(ctx context.Context, key, member string, q GeoRadiusQuery) ([]GeoLocation, error) {
	r := newCmd(3+q.n("", ""), cGEORADIUSBYMEMBER).s(key).s(member)
	q.build(r, "", "")
	v, err := c.value(ctx, r)
	if err != nil {
		return nil, err
	}
	return toGeoLocations(v, q.WithDist, q.WithHash, q.WithCoord)
}

// GeoRadiusByMemberStore is GeoRadiusByMember that stores the result in store and/or
// storeDist, like GeoRadiusStore, and returns the number of members stored.
func (c *cmds) GeoRadiusByMemberStore(ctx context.Context, key, member string, q GeoRadiusQuery, store, storeDist string) (int64, error) {
	r := newCmd(3+q.n(store, storeDist), cGEORADIUSBYMEMBER).s(key).s(member)
	q.build(r, store, storeDist)
	return c.intR(ctx, r)
}
