// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"math"
	"testing"
	"time"
)

func TestLiveSortedSets(t *testing.T) {
	c, ctx := live(t)
	mi := must[int64](t)
	ms := must[[]string](t)
	mz := must[[]Z](t)

	eq(t, mi(c.ZAdd(ctx, "z", Z{"a", 1}, Z{"b", 2}, Z{"c", 3})), int64(3))
	eq(t, mi(c.ZAddArgs(ctx, "z", ZAddOptions{XX: true, CH: true}, Z{"a", 5}, Z{"new", 1})), int64(1))
	eq(t, mi(c.ZAddArgs(ctx, "z", ZAddOptions{NX: true}, Z{"a", 9}, Z{"d", 4})), int64(1))
	eq(t, mi(c.ZAddArgs(ctx, "z", ZAddOptions{GT: true, CH: true}, Z{"a", 1})), int64(0))
	eq(t, mi(c.ZAddArgs(ctx, "z", ZAddOptions{LT: true, CH: true}, Z{"a", 1})), int64(1))
	f, ok, err := c.ZAddIncr(ctx, "z", ZAddOptions{}, Z{"a", 0.5})
	noErr(t, err)
	eq(t, []any{f, ok}, []any{1.5, true})
	_, ok, _ = c.ZAddIncr(ctx, "z", ZAddOptions{NX: true}, Z{"a", 1})
	eq(t, ok, false)
	// z: a=1.5 b=2 c=3 d=4
	eq(t, ms(c.ZRange(ctx, "z", 0, -1)), []string{"a", "b", "c", "d"})
	eq(t, mz(c.ZRangeWithScores(ctx, "z", 0, 0)), []Z{{"a", 1.5}})
	eq(t, ms(c.ZRevRange(ctx, "z", 0, 1)), []string{"d", "c"})
	eq(t, mz(c.ZRevRangeWithScores(ctx, "z", 0, 0)), []Z{{"d", 4}})
	eq(t, ms(c.ZRangeArgs(ctx, ZRangeArgs{Key: "z", Start: "(2", Stop: "+inf", ByScore: true, Limit: &Limit{0, 1}})), []string{"c"})
	// With BYSCORE REV the bounds are max, min, as in Redis.
	eq(t, ms(c.ZRangeArgs(ctx, ZRangeArgs{Key: "z", Start: "+inf", Stop: "-inf", ByScore: true, Rev: true, Limit: &Limit{1, -1}})), []string{"c", "b", "a"})
	eq(t, mz(c.ZRangeArgsWithScores(ctx, ZRangeArgs{Key: "z", Start: "0", Stop: "1"})), []Z{{"a", 1.5}, {"b", 2}})
	eq(t, ms(c.ZRangeByScore(ctx, "z", "2", "3", nil)), []string{"b", "c"})
	eq(t, mz(c.ZRangeByScoreWithScores(ctx, "z", "-inf", "+inf", &Limit{3, 5})), []Z{{"d", 4}})
	eq(t, mi(c.ZCard(ctx, "z")), int64(4))
	s, ok, _ := c.ZScore(ctx, "z", "c")
	eq(t, []any{s, ok}, []any{3.0, true})
	_, ok, _ = c.ZScore(ctx, "z", "nope")
	eq(t, ok, false)
	eq(t, must[[]NullFloat](t)(c.ZMScore(ctx, "z", "b", "nope")), []NullFloat{{2, true}, {}})
	r, ok, _ := c.ZRank(ctx, "z", "c")
	eq(t, []any{r, ok}, []any{int64(2), true})
	r, _, _ = c.ZRevRank(ctx, "z", "c")
	eq(t, r, int64(1))
	eq(t, must[float64](t)(c.ZIncrBy(ctx, "z", 10, "a")), 11.5)
	eq(t, mi(c.ZCount(ctx, "z", "(2", "+inf")), int64(3))
	eq(t, mi(c.ZRem(ctx, "z", "a", "nope")), int64(1))

	c.ZAdd(ctx, "lex", Z{"a", 0}, Z{"b", 0}, Z{"c", 0}, Z{"d", 0})
	eq(t, ms(c.ZRangeByLex(ctx, "lex", "[b", "+", nil)), []string{"b", "c", "d"})
	eq(t, ms(c.ZRangeByLex(ctx, "lex", "-", "+", &Limit{1, 2})), []string{"b", "c"})
	eq(t, ms(c.ZRevRangeByLex(ctx, "lex", "(d", "-", nil)), []string{"c", "b", "a"})
	eq(t, mi(c.ZLexCount(ctx, "lex", "[a", "(c")), int64(2))
	eq(t, ms(c.ZRangeArgs(ctx, ZRangeArgs{Key: "lex", Start: "[b", Stop: "[c", ByLex: true})), []string{"b", "c"})
	eq(t, mi(c.ZRemRangeByLex(ctx, "lex", "[a", "[a")), int64(1))
	eq(t, mi(c.ZRemRangeByRank(ctx, "lex", 0, 0)), int64(1))
	eq(t, mi(c.ZRemRangeByScore(ctx, "lex", "-inf", "+inf")), int64(2))

	c.ZAdd(ctx, "p", Z{"x", 1}, Z{"y", 2}, Z{"w", 3})
	eq(t, mz(c.ZPopMin(ctx, "p", 0)), []Z{{"x", 1}})
	eq(t, mz(c.ZPopMax(ctx, "p", 5)), []Z{{"w", 3}, {"y", 2}})
	eq(t, len(mz(c.ZPopMin(ctx, "p", 1))), 0)

	c.ZAdd(ctx, "due", Z{"a", 1}, Z{"b", 2}, Z{"c", 3}, Z{"d", 9})
	eq(t, mz(c.ZPopByScore(ctx, "due", "-inf", "1", 5)), []Z{{"a", 1}})
	eq(t, mi(c.ZMoveDue(ctx, "due", "ready", "3", 5)), int64(2))
	eq(t, ms(c.LRange(ctx, "ready", 0, -1)), []string{"b", "c"})
	eq(t, len(mz(c.ZPopByScore(ctx, "due", "-inf", "3", 5))), 0)
	go func() {
		time.Sleep(30 * time.Millisecond)
		c.ZAdd(ctx, "bz", Z{"m", 7}, Z{"n", 8})
	}()
	kz, ok, err := c.BZPopMin(ctx, 2*time.Second, "bz")
	noErr(t, err)
	eq(t, []any{kz, ok}, []any{KeyZ{"bz", Z{"m", 7}}, true})
	kz, ok, err = c.BZPopMax(ctx, time.Second, "bz")
	noErr(t, err)
	eq(t, []any{kz, ok}, []any{KeyZ{"bz", Z{"n", 8}}, true})
	_, ok, err = c.BZPopMax(ctx, 50*time.Millisecond, "bz")
	noErr(t, err)
	eq(t, ok, false)

	c.ZAdd(ctx, "u1", Z{"a", 1}, Z{"b", 2})
	c.ZAdd(ctx, "u2", Z{"b", 3}, Z{"c", 4})
	eq(t, mi(c.ZUnionStore(ctx, "us", ZStore{Keys: []string{"u1", "u2"}})), int64(3))
	eq(t, mi(c.ZInterStore(ctx, "is", ZStore{Keys: []string{"u1", "u2"}, Weights: []float64{2, 1}, Aggregate: "MAX"})), int64(1))
	s, _, _ = c.ZScore(ctx, "is", "b")
	eq(t, s, 4.0)
	eq(t, mi(c.ZDiffStore(ctx, "ds", "u1", "u2")), int64(1))
	eq(t, ms(c.ZUnion(ctx, ZStore{Keys: []string{"u1", "u2"}})), []string{"a", "c", "b"})
	eq(t, mz(c.ZUnionWithScores(ctx, ZStore{Keys: []string{"u1", "u2"}, Aggregate: "MIN"})), []Z{{"a", 1}, {"b", 2}, {"c", 4}})
	eq(t, ms(c.ZInter(ctx, ZStore{Keys: []string{"u1", "u2"}})), []string{"b"})
	eq(t, mz(c.ZInterWithScores(ctx, ZStore{Keys: []string{"u1", "u2"}})), []Z{{"b", 5}})
	eq(t, ms(c.ZDiff(ctx, "u1", "u2")), []string{"a"})
	eq(t, mz(c.ZDiffWithScores(ctx, "u1", "u2")), []Z{{"a", 1}})
	eq(t, len(ms(c.ZRandMember(ctx, "us", 2))), 2)
	eq(t, len(mz(c.ZRandMemberWithScores(ctx, "us", -5))), 5)

	it := c.ZScanIter("us", ScanOptions{Count: 1})
	var flat []string
	for it.Next(ctx) {
		flat = append(flat, it.Val())
	}
	noErr(t, it.Err())
	eq(t, len(flat), 6)
	_, page, err := c.ZScan(ctx, "us", "0", ScanOptions{Match: "a"})
	noErr(t, err)
	eq(t, page, []string{"a", "1"})
}

func TestLiveGeo(t *testing.T) {
	c, ctx := live(t)
	mi := must[int64](t)
	eq(t, mi(c.GeoAdd(ctx, "g", GeoAddOptions{}, GeoLocation{Name: "Palermo", Longitude: 13.361389, Latitude: 38.115556},
		GeoLocation{Name: "Catania", Longitude: 15.087269, Latitude: 37.502669})), int64(2))
	eq(t, mi(c.GeoAdd(ctx, "g", GeoAddOptions{XX: true, CH: true}, GeoLocation{Name: "Catania", Longitude: 15.087269, Latitude: 37.6})), int64(1))
	eq(t, mi(c.GeoAdd(ctx, "g", GeoAddOptions{NX: true}, GeoLocation{Name: "Catania", Longitude: 15.087269, Latitude: 37.502669})), int64(0))
	c.GeoAdd(ctx, "g", GeoAddOptions{XX: true}, GeoLocation{Name: "Catania", Longitude: 15.087269, Latitude: 37.502669})
	d, ok, err := c.GeoDist(ctx, "g", "Palermo", "Catania", "km")
	noErr(t, err)
	if !ok || math.Abs(d-166.27) > 0.1 {
		t.Fatalf("geodist %v", d)
	}
	_, ok, _ = c.GeoDist(ctx, "g", "Palermo", "nope", "")
	eq(t, ok, false)
	h := must[[]NullString](t)(c.GeoHash(ctx, "g", "Palermo", "nope"))
	if len(h) != 2 || len(h[0].Val) != 11 || h[1].Valid {
		t.Fatalf("geohash %v", h)
	}
	pos := must[[]GeoPos](t)(c.GeoPosition(ctx, "g", "Palermo", "nope"))
	if !pos[0].Valid || math.Abs(pos[0].Longitude-13.361389) > 1e-4 || pos[1].Valid {
		t.Fatalf("geopos %v", pos)
	}
	res := must[[]GeoLocation](t)(c.GeoSearch(ctx, "g", GeoSearchQuery{Longitude: 15, Latitude: 37, Radius: 200, Unit: "km",
		Sort: "ASC", WithDist: true, WithCoord: true, WithHash: true}))
	if len(res) != 2 || res[0].Name != "Catania" || res[0].Dist <= 0 || res[0].GeoHash == 0 || res[0].Longitude == 0 {
		t.Fatalf("geosearch %+v", res)
	}
	res = must[[]GeoLocation](t)(c.GeoSearch(ctx, "g", GeoSearchQuery{Member: "Palermo", Width: 400, Height: 400, Unit: "km", Count: 1, Any: true}))
	eq(t, len(res), 1)
	eq(t, mi(c.GeoSearchStore(ctx, "gs", "g", GeoSearchQuery{Longitude: 15, Latitude: 37, Radius: 200, Unit: "km"}, false)), int64(2))
	eq(t, mi(c.GeoSearchStore(ctx, "gsd", "g", GeoSearchQuery{Longitude: 15, Latitude: 37, Radius: 200, Unit: "km"}, true)), int64(2))
	res = must[[]GeoLocation](t)(c.GeoRadius(ctx, "g", 15, 37, GeoRadiusQuery{Radius: 100, Unit: "km", WithDist: true}))
	if len(res) != 1 || res[0].Name != "Catania" || res[0].Dist <= 0 {
		t.Fatalf("georadius %+v", res)
	}
	eq(t, mi(c.GeoRadiusStore(ctx, "g", 15, 37, GeoRadiusQuery{Radius: 200, Unit: "km"}, "gr", "")), int64(2))
	eq(t, mi(c.GeoRadiusStore(ctx, "g", 15, 37, GeoRadiusQuery{Radius: 200, Unit: "km"}, "", "grd")), int64(2))
	res = must[[]GeoLocation](t)(c.GeoRadiusByMember(ctx, "g", "Palermo", GeoRadiusQuery{Radius: 200, Unit: "km", Sort: "DESC", Count: 5}))
	eq(t, len(res), 2)
	eq(t, res[0].Name, "Catania")
	eq(t, mi(c.GeoRadiusByMemberStore(ctx, "g", "Palermo", GeoRadiusQuery{Radius: 10, Unit: "km"}, "grm", "")), int64(1))
}
