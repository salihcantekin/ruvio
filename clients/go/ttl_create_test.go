// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// ttlFake answers every command with a reply of the shape the typed method expects.
func ttlFake(t *testing.T) *fakeNode {
	return newFakeNode(t, func(f *fakeNode, args []string) string {
		switch strings.ToUpper(args[0]) {
		case "PING":
			return "+PONG\r\n"
		case "INCRBYFLOAT", "ZINCRBY":
			return bulk("1.5")
		case "XADD":
			return bulk("1-1")
		case "JSON.SET":
			return "+OK\r\n"
		case "BF.INSERT":
			return "*1\r\n:1\r\n"
		case "ZADD":
			for _, a := range args {
				if a == "INCR" {
					return bulk("2")
				}
			}
		}
		return ":1\r\n"
	})
}

func TestTTLOnCreateSendsExactArguments(t *testing.T) {
	ctx := context.Background()
	f := ttlFake(t)
	c, err := Connect(ctx, Options{Addr: f.addr})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	sec, sub := 90*time.Second, 1500*time.Millisecond
	cases := []struct {
		name string
		run  func() error
		want string
	}{
		{"Incr EX", func() error { _, e := c.IncrWithTTL(ctx, "k", sec); return e }, "INCR k EX 90"},
		{"Incr PX", func() error { _, e := c.IncrWithTTL(ctx, "k", sub); return e }, "INCR k PX 1500"},
		{"Incr sub-ms", func() error { _, e := c.IncrWithTTL(ctx, "k", time.Microsecond); return e }, "INCR k PX 1"},
		{"Decr", func() error { _, e := c.DecrWithTTL(ctx, "k", sec); return e }, "DECR k EX 90"},
		{"IncrBy", func() error { _, e := c.IncrByWithTTL(ctx, "k", 5, sec); return e }, "INCRBY k 5 EX 90"},
		{"DecrBy", func() error { _, e := c.DecrByWithTTL(ctx, "k", 5, sub); return e }, "DECRBY k 5 PX 1500"},
		{"IncrByMax", func() error { _, _, e := c.IncrByMaxWithTTL(ctx, "k", 1, 10, sec); return e }, "INCRBY k 1 EX 90 MAX 10"},
		{"IncrByFloat", func() error { _, e := c.IncrByFloatWithTTL(ctx, "k", 0.5, sec); return e }, "INCRBYFLOAT k 0.5 EX 90"},
		{"HIncrBy", func() error { _, e := c.HIncrByWithTTL(ctx, "k", "f", 2, sec); return e }, "HINCRBY k f 2 EX 90"},
		{"ZIncrBy", func() error { _, e := c.ZIncrByWithTTL(ctx, "k", 2, "m", sub); return e }, "ZINCRBY k 2 m PX 1500"},
		{"SetBit", func() error { _, e := c.SetBitWithTTL(ctx, "k", 7, 1, sec); return e }, "SETBIT k 7 1 EX 90"},
		{"ZAdd TTL first", func() error {
			_, e := c.ZAddArgs(ctx, "k", ZAddOptions{TTL: sec, NX: true, CH: true}, Z{Member: "m", Score: 1})
			return e
		}, "ZADD k EX 90 NX CH 1 m"},
		{"ZAdd no TTL", func() error { _, e := c.ZAdd(ctx, "k", Z{Member: "m", Score: 1}); return e }, "ZADD k 1 m"},
		{"ZAddIncr", func() error {
			_, _, e := c.ZAddIncr(ctx, "k", ZAddOptions{TTL: sub, GT: true}, Z{Member: "m", Score: 1})
			return e
		}, "ZADD k PX 1500 GT INCR 1 m"},
		{"GeoAdd", func() error {
			_, e := c.GeoAdd(ctx, "k", GeoAddOptions{TTL: sec, XX: true}, GeoLocation{Name: "p", Longitude: 1, Latitude: 2})
			return e
		}, "GEOADD k EX 90 XX 1 2 p"},
		{"XAdd", func() error {
			_, _, e := c.XAdd(ctx, XAddArgs{Stream: "k", TTL: sec, NoMkStream: true, MaxLen: 5, Values: []string{"a", "b"}})
			return e
		}, "XADD k EX 90 NOMKSTREAM MAXLEN 5 * a b"},
		{"XAdd delay", func() error {
			_, _, e := c.XAdd(ctx, XAddArgs{Stream: "k", TTL: sub, Delay: 10 * time.Millisecond, Values: []string{"a", "b"}})
			return e
		}, "XADD k PX 1500 DELAY 10 * a b"},
		{"JSONSet", func() error { return c.JSONSetWithTTL(ctx, "k", "$", "1", sec) }, "JSON.SET k $ 1 EX 90"},
		{"JSONSetMode", func() error { _, e := c.JSONSetModeWithTTL(ctx, "k", "$", "1", "NX", sub); return e }, "JSON.SET k $ 1 NX PX 1500"},
		{"BFInsert", func() error {
			_, e := c.BFInsert(ctx, "k", BFInsertOptions{TTL: sec, Capacity: 50}, "a", "b")
			return e
		}, "BF.INSERT k EX 90 CAPACITY 50 ITEMS a b"},
		{"ExpireIf", func() error { _, e := c.ExpireIf(ctx, "k", sec, ExpireGt); return e }, "EXPIRE k 90 GT"},
		{"ExpireIf none", func() error { _, e := c.ExpireIf(ctx, "k", sec, ""); return e }, "EXPIRE k 90"},
		{"PExpireIf", func() error { _, e := c.PExpireIf(ctx, "k", sub, ExpireNx); return e }, "PEXPIRE k 1500 NX"},
		{"ExpireAtIf", func() error { _, e := c.ExpireAtIf(ctx, "k", time.Unix(1000, 0), ExpireXx); return e }, "EXPIREAT k 1000 XX"},
		{"PExpireAtIf", func() error { _, e := c.PExpireAtIf(ctx, "k", time.UnixMilli(1500), ExpireLt); return e }, "PEXPIREAT k 1500 LT"},
	}
	for _, tc := range cases {
		before := len(f.commands())
		if err := tc.run(); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		got := f.commands()
		if len(got) != before+1 || got[before] != tc.want {
			t.Errorf("%s: sent %v, want %q", tc.name, got[before:], tc.want)
		}
	}
}

func TestTTLOnCreateRejectsBadDurationsWithoutSending(t *testing.T) {
	ctx := context.Background()
	f := ttlFake(t)
	c, err := Connect(ctx, Options{Addr: f.addr})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	before := len(f.commands())
	for _, ttl := range []time.Duration{0, -time.Second} {
		errs := []error{
			func() error { _, e := c.IncrWithTTL(ctx, "k", ttl); return e }(),
			func() error { _, e := c.DecrWithTTL(ctx, "k", ttl); return e }(),
			func() error { _, e := c.IncrByWithTTL(ctx, "k", 1, ttl); return e }(),
			func() error { _, e := c.DecrByWithTTL(ctx, "k", 1, ttl); return e }(),
			func() error { _, _, e := c.IncrByMaxWithTTL(ctx, "k", 1, 2, ttl); return e }(),
			func() error { _, e := c.IncrByFloatWithTTL(ctx, "k", 1, ttl); return e }(),
			func() error { _, e := c.HIncrByWithTTL(ctx, "k", "f", 1, ttl); return e }(),
			func() error { _, e := c.ZIncrByWithTTL(ctx, "k", 1, "m", ttl); return e }(),
			func() error { _, e := c.SetBitWithTTL(ctx, "k", 1, 1, ttl); return e }(),
			c.JSONSetWithTTL(ctx, "k", "$", "1", ttl),
			func() error { _, e := c.JSONSetModeWithTTL(ctx, "k", "$", "1", "NX", ttl); return e }(),
		}
		for i, e := range errs {
			if !errors.Is(e, ErrInvalidTTL) {
				t.Errorf("ttl %v method %d: got %v, want ErrInvalidTTL", ttl, i, e)
			}
		}
	}
	// Option fields: zero means no TTL; negative is rejected.
	neg := -time.Second
	_, e1 := c.ZAddArgs(ctx, "k", ZAddOptions{TTL: neg}, Z{Member: "m"})
	_, _, e2 := c.ZAddIncr(ctx, "k", ZAddOptions{TTL: neg}, Z{Member: "m"})
	_, e3 := c.GeoAdd(ctx, "k", GeoAddOptions{TTL: neg}, GeoLocation{Name: "p"})
	_, _, e4 := c.XAdd(ctx, XAddArgs{Stream: "k", TTL: neg, Values: []string{"a", "b"}})
	_, e5 := c.BFInsert(ctx, "k", BFInsertOptions{TTL: neg}, "a")
	for i, e := range []error{e1, e2, e3, e4, e5} {
		if !errors.Is(e, ErrInvalidTTL) {
			t.Errorf("option %d: got %v, want ErrInvalidTTL", i, e)
		}
	}
	if got := len(f.commands()); got != before {
		t.Fatalf("%d commands were sent for rejected TTLs", got-before)
	}
}
