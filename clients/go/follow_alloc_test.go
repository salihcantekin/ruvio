// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"
)

// A client that follows a failover (two Seeds, following on) allocates exactly what a
// single-seed client allocates per command while the connection is healthy, and keeps no
// state that grows with the number of commands. Runs against the failover pair that
// scripts/run_with_servers.sh starts (RUVIO_FAILOVER_ADDRS, primary first); skipped without it.

func TestFollowAllocsMatchSingleSeed(t *testing.T) {
	pair, _ := followPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	connect := func(seeds []string) *Client {
		t.Helper()
		c, err := Connect(ctx, Options{Seeds: seeds, CommandTimeout: 5 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	// The first seed is the primary; both clients talk to it.
	single := connect(pair[:1])
	following := connect(pair[:])
	if single.follow || !following.follow {
		t.Fatalf("follow flags: single %v, following %v", single.follow, following.follow)
	}

	val := strings.Repeat("v", 32)
	const key = "follow:alloc"
	type op struct {
		name string
		run  func(c *Client, p *Pipeline) error
	}
	ops := []op{
		{"SET+GET", func(c *Client, _ *Pipeline) error {
			if err := c.Set(ctx, key, val); err != nil {
				return err
			}
			_, _, err := c.Get(ctx, key)
			return err
		}},
		{"pipeline of 16", func(c *Client, p *Pipeline) error {
			for i := 0; i < 8; i++ {
				p.Set(key, val)
				p.Get(key)
			}
			_, err := p.Exec(ctx)
			return err
		}},
	}
	// The same loop on both clients, warmed up first (pools, buffers). A reader goroutine
	// allocates too, and AllocsPerRun counts the whole process, so a few runs are averaged
	// and the smaller of two attempts is taken.
	measure := func(c *Client, o op) float64 {
		p := c.Pipeline()
		var failed error
		step := func() {
			if err := o.run(c, p); err != nil {
				failed = err
			}
		}
		for i := 0; i < 30; i++ {
			step()
		}
		best := testing.AllocsPerRun(100, step)
		if again := testing.AllocsPerRun(100, step); again < best {
			best = again
		}
		if failed != nil {
			t.Fatal(o.name, failed)
		}
		return best
	}
	for _, o := range ops {
		s, f := measure(single, o), measure(following, o)
		t.Logf("%s: single %.3f allocs, following %.3f allocs", o.name, s, f)
		if s == 0 {
			t.Fatalf("%s: single-seed client reports no allocations; the measurement is broken", o.name)
		}
		if f != s {
			t.Errorf("%s: following allocates %.3f per run, single seed %.3f", o.name, f, s)
		}
	}

	// The follow state does not grow: the dial list keeps its two seeds and the heap after
	// many commands is no larger than before them.
	heap := func() uint64 {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	before := heap()
	p := following.Pipeline()
	for i := 0; i < 1250; i++ { // 20000 commands in 1250 round trips
		if err := ops[1].run(following, p); err != nil {
			t.Fatal(err)
		}
	}
	after := heap()
	t.Logf("heap before %d B, after 20000 commands %d B", before, after)
	if after > before+64<<10 {
		t.Errorf("heap grew from %d to %d bytes over 20000 commands while following", before, after)
	}
	if got := len(following.nodeList()); got != 2 {
		t.Errorf("dial list has %d entries after healthy commands, want 2", got)
	}
}
