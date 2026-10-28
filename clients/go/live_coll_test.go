// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"sort"
	"testing"
	"time"
)

func TestLiveLists(t *testing.T) {
	c, ctx := live(t)
	mi := must[int64](t)
	ms := must[[]string](t)

	eq(t, mi(c.RPush(ctx, "l", "a", "b", "c")), int64(3))
	eq(t, mi(c.LPush(ctx, "l", "z")), int64(4))
	eq(t, mi(c.LPushX(ctx, "nope", "x")), int64(0))
	eq(t, mi(c.RPushX(ctx, "l", "d")), int64(5))
	eq(t, ms(c.LRange(ctx, "l", 0, -1)), []string{"z", "a", "b", "c", "d"})
	eq(t, mi(c.LLen(ctx, "l")), int64(5))
	v, ok, _ := c.LIndex(ctx, "l", 1)
	eq(t, []any{v, ok}, []any{"a", true})
	eq(t, mi(c.LInsert(ctx, "l", true, "b", "ab")), int64(6))
	eq(t, mi(c.LInsert(ctx, "l", false, "b", "bc")), int64(7))
	noErr(t, c.LSet(ctx, "l", 0, "Z"))
	eq(t, mi(c.RPush(ctx, "l", "a")), int64(8))
	eq(t, mi(c.LRem(ctx, "l", 0, "a")), int64(2))
	eq(t, ms(c.LRange(ctx, "l", 0, -1)), []string{"Z", "ab", "b", "bc", "c", "d"})
	p, ok, _ := c.LPos(ctx, "l", "b", LPosOptions{})
	eq(t, []any{p, ok}, []any{int64(2), true})
	_, ok, _ = c.LPos(ctx, "l", "zz", LPosOptions{MaxLen: 3})
	eq(t, ok, false)
	eq(t, must[[]int64](t)(c.LPosCount(ctx, "l", "c", 0, LPosOptions{Rank: 1})), []int64{4})
	noErr(t, c.LTrim(ctx, "l", 1, -1))
	v, _, _ = c.LPop(ctx, "l")
	eq(t, v, "ab")
	v, _, _ = c.RPop(ctx, "l")
	eq(t, v, "d")
	eq(t, ms(c.LPopCount(ctx, "l", 2)), []string{"b", "bc"})
	eq(t, ms(c.RPopCount(ctx, "l", 5)), []string{"c"})
	_, ok, _ = c.LPop(ctx, "l")
	eq(t, ok, false)

	eq(t, mi(c.LPushCap(ctx, "cap", 2, "a", "b", "c")), int64(2))
	eq(t, ms(c.LRange(ctx, "cap", 0, -1)), []string{"c", "b"})
	eq(t, mi(c.RPushCap(ctx, "rcap", 2, "a", "b", "c")), int64(2))
	eq(t, ms(c.LRange(ctx, "rcap", 0, -1)), []string{"b", "c"})

	c.RPush(ctx, "src", "1", "2", "3")
	v, ok, _ = c.LMove(ctx, "src", "dst", "LEFT", "RIGHT")
	eq(t, []any{v, ok}, []any{"1", true})
	v, _, _ = c.RPopLPush(ctx, "src", "dst")
	eq(t, v, "3")
	v, ok, err := c.BLMove(ctx, "src", "dst", "RIGHT", "LEFT", time.Second)
	noErr(t, err)
	eq(t, []any{v, ok}, []any{"2", true})
	_, ok, err = c.BLMove(ctx, "src", "dst", "RIGHT", "LEFT", 50*time.Millisecond)
	noErr(t, err)
	eq(t, ok, false)
	eq(t, ms(c.LRange(ctx, "dst", 0, -1)), []string{"2", "3", "1"})

	kv, ok, err := c.LMPop(ctx, "LEFT", 2, "empty", "dst")
	noErr(t, err)
	eq(t, []any{kv, ok}, []any{KeyValues{"dst", []string{"2", "3"}}, true})
	kv, ok, err = c.BLMPop(ctx, time.Second, "RIGHT", 0, "dst")
	noErr(t, err)
	eq(t, []any{kv, ok}, []any{KeyValues{"dst", []string{"1"}}, true})
	_, ok, err = c.LMPop(ctx, "LEFT", 1, "dst")
	noErr(t, err)
	eq(t, ok, false)

	// blocking pops on a dedicated connection do not stall the shared one
	go func() {
		time.Sleep(50 * time.Millisecond)
		c.RPush(ctx, "bl", "x")
		c.RPush(ctx, "br", "y")
	}()
	start := time.Now()
	for i := 0; i < 5; i++ {
		noErr(t, c.Ping(ctx))
	}
	if time.Since(start) > 40*time.Millisecond {
		t.Fatal("shared connection stalled")
	}
	r, ok, err := c.BLPop(ctx, 2*time.Second, "bl")
	noErr(t, err)
	eq(t, []any{r, ok}, []any{KeyValue{"bl", "x"}, true})
	r, ok, err = c.BRPop(ctx, 2*time.Second, "br")
	noErr(t, err)
	eq(t, []any{r, ok}, []any{KeyValue{"br", "y"}, true})
	_, ok, err = c.BLPop(ctx, 50*time.Millisecond, "bl")
	noErr(t, err)
	eq(t, ok, false)
}

func TestLiveSets(t *testing.T) {
	c, ctx := live(t)
	mi := must[int64](t)
	ms := must[[]string](t)
	mb := must[bool](t)

	eq(t, mi(c.SAdd(ctx, "s1", "a", "b", "c")), int64(3))
	c.SAdd(ctx, "s2", "b", "c", "d")
	eq(t, mi(c.SRem(ctx, "s1", "zz")), int64(0))
	eq(t, mb(c.SIsMember(ctx, "s1", "a")), true)
	eq(t, must[[]bool](t)(c.SMIsMember(ctx, "s1", "a", "z")), []bool{true, false})
	eq(t, mi(c.SCard(ctx, "s1")), int64(3))
	eq(t, ms(c.SMembers(ctx, "s1")), []string{"a", "b", "c"})
	eq(t, ms(c.SInter(ctx, "s1", "s2")), []string{"b", "c"})
	u := ms(c.SUnion(ctx, "s1", "s2"))
	sort.Strings(u)
	eq(t, u, []string{"a", "b", "c", "d"})
	eq(t, ms(c.SDiff(ctx, "s1", "s2")), []string{"a"})
	eq(t, mi(c.SInterStore(ctx, "si", "s1", "s2")), int64(2))
	eq(t, mi(c.SUnionStore(ctx, "su", "s1", "s2")), int64(4))
	eq(t, mi(c.SDiffStore(ctx, "sd", "s1", "s2")), int64(1))
	eq(t, mi(c.SInterCard(ctx, 0, "s1", "s2")), int64(2))
	eq(t, mi(c.SInterCard(ctx, 1, "s1", "s2")), int64(1))
	eq(t, mb(c.SMove(ctx, "s1", "s2", "a")), true)
	v, ok, _ := c.SRandMember(ctx, "s2")
	if !ok || v == "" {
		t.Fatal("srandmember")
	}
	eq(t, len(ms(c.SRandMemberCount(ctx, "s2", 2))), 2)
	eq(t, len(ms(c.SRandMemberCount(ctx, "s2", -6))), 6)
	_, ok, _ = c.SPop(ctx, "s2")
	eq(t, ok, true)
	eq(t, len(ms(c.SPopCount(ctx, "s2", 10))), 3)

	for i := 0; i < 25; i++ {
		c.SAdd(ctx, "big", string(rune('a'+i)))
	}
	_, page, err := c.SScan(ctx, "big", "0", ScanOptions{Count: 10})
	noErr(t, err)
	if len(page) == 0 {
		t.Fatal("empty sscan page")
	}
	it := c.SScanIter("big", ScanOptions{Match: "*", Count: 4})
	var all []string
	for it.Next(ctx) {
		all = append(all, it.Val())
	}
	noErr(t, it.Err())
	eq(t, len(all), 25)
}

func TestLiveHashes(t *testing.T) {
	c, ctx := live(t)
	mi := must[int64](t)
	ms := must[[]string](t)
	mb := must[bool](t)
	mis := must[[]int64](t)

	eq(t, mi(c.HSet(ctx, "h", "a", "1", "b", "2")), int64(2))
	eq(t, mi(c.HSetBytes(ctx, "h", "bin", []byte{0, 1})), int64(1))
	bb, _, _ := c.HGetBytes(ctx, "h", "bin")
	eq(t, bb, []byte{0, 1})
	eq(t, mb(c.HSetNX(ctx, "h", "a", "x")), false)
	eq(t, mb(c.HSetNX(ctx, "h", "c", "3")), true)
	noErr(t, c.HMSet(ctx, "hm", "a", "1", "b", "2"))
	eq(t, must[[]NullString](t)(c.HMGet(ctx, "hm", "a", "b")), []NullString{{"1", true}, {"2", true}})
	v, ok, _ := c.HGet(ctx, "h", "a")
	eq(t, []any{v, ok}, []any{"1", true})
	eq(t, mi(c.HDel(ctx, "h", "bin")), int64(1))
	eq(t, mi(c.HLen(ctx, "h")), int64(3))
	eq(t, must[map[string]string](t)(c.HGetAll(ctx, "h")), map[string]string{"a": "1", "b": "2", "c": "3"})
	eq(t, must[[]NullString](t)(c.HMGet(ctx, "h", "a", "z")), []NullString{{"1", true}, {}})
	eq(t, mb(c.HExists(ctx, "h", "b")), true)
	eq(t, ms(c.HKeys(ctx, "h")), []string{"a", "b", "c"})
	eq(t, ms(c.HVals(ctx, "h")), []string{"1", "2", "3"})
	eq(t, mi(c.HIncrBy(ctx, "h", "a", 5)), int64(6))
	eq(t, must[float64](t)(c.HIncrByFloat(ctx, "h", "a", 0.5)), 6.5)
	eq(t, mi(c.HStrLen(ctx, "h", "a")), int64(3))
	eq(t, len(ms(c.HRandField(ctx, "h", 2))), 2)
	eq(t, len(ms(c.HRandFieldWithValues(ctx, "h", 2))), 4)

	it := c.HScanIter("h", ScanOptions{Count: 1})
	var flat []string
	for it.Next(ctx) {
		flat = append(flat, it.Val())
	}
	noErr(t, it.Err())
	eq(t, len(flat), 6)
	_, page, err := c.HScan(ctx, "h", "0", ScanOptions{Match: "a"})
	noErr(t, err)
	eq(t, page, []string{"a", "6.5"})

	// field TTLs
	eq(t, mis(c.HExpire(ctx, "h", 100*time.Second, "", "a", "nope")), []int64{1, -2})
	eq(t, mis(c.HExpire(ctx, "h", 200*time.Second, "NX", "a")), []int64{0})
	eq(t, mis(c.HPExpire(ctx, "h", 50*time.Second, "LT", "a")), []int64{1})
	if ttl := mis(c.HTTL(ctx, "h", "a", "b")); ttl[0] < 49 || ttl[0] > 50 || ttl[1] != -1 {
		t.Fatalf("httl %v", ttl)
	}
	if p := mis(c.HPTTL(ctx, "h", "a")); p[0] <= 0 || p[0] > 50000 {
		t.Fatalf("hpttl %v", p)
	}
	at := time.Now().Add(time.Hour)
	eq(t, mis(c.HExpireAt(ctx, "h", at, "", "b")), []int64{1})
	eq(t, mis(c.HPExpireAt(ctx, "h", at, "GT", "c")), []int64{0}) // no TTL counts as infinite
	eq(t, mis(c.HPExpireAt(ctx, "h", at, "", "c")), []int64{1})
	if et := mis(c.HExpireTime(ctx, "h", "b")); et[0] < at.Unix()-1 || et[0] > at.Unix()+1 {
		t.Fatalf("hexpiretime %v", et)
	}
	if et := mis(c.HPExpireTime(ctx, "h", "c")); et[0]/1000 != at.UnixMilli()/1000 {
		t.Fatalf("hpexpiretime %v", et)
	}
	eq(t, mis(c.HPersist(ctx, "h", "a", "b", "c", "nope")), []int64{1, 1, 1, -2})
	eq(t, mis(c.HPExpireAt(ctx, "h", time.Now().Add(-time.Hour), "", "c")), []int64{2})
	keys := ms(c.HKeys(ctx, "h"))
	sort.Strings(keys)
	eq(t, keys, []string{"a", "b"})
}
