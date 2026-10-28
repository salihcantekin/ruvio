// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

// replicaSlots is a CLUSTER SLOTS reply with one range that lists the replicas after its
// primary, and one malformed replica entry the client must skip.
func replicaSlots(primary string, replicas ...string) string {
	host, port, _ := net.SplitHostPort(primary)
	var b strings.Builder
	fmt.Fprintf(&b, "*1\r\n*%d\r\n:0\r\n:16383\r\n*3\r\n%s:%s\r\n%s", 4+len(replicas), bulk(host), port, bulk("idp"))
	for _, r := range replicas {
		rh, rp, _ := net.SplitHostPort(r)
		fmt.Fprintf(&b, "*3\r\n%s:%s\r\n%s", bulk(rh), rp, bulk("idr"))
	}
	b.WriteString("*1\r\n:7\r\n") // malformed: no host or port
	return b.String()
}

// replicatedPair starts a primary that owns every slot and lists replica (or nothing when
// it is empty) in CLUSTER SLOTS, and returns a client with the given policy.
func replicatedPair(t *testing.T, policy ReadPolicy, replica string, hp func(*fakeNode, []string) string) (*fakeNode, *Client) {
	if hp == nil {
		hp = defaultFake
	}
	var p *fakeNode
	p = newFakeNode(t, func(f *fakeNode, args []string) string {
		if args[0] == "CLUSTER" {
			if replica == "" {
				return replicaSlots(p.addr)
			}
			return replicaSlots(p.addr, replica)
		}
		return hp(f, args)
	})
	c := NewClient(Options{Addr: p.addr, Cluster: true, ReadPolicy: policy, DialTimeout: 2 * time.Second})
	t.Cleanup(func() { c.Close() })
	return p, c
}

func TestReplicaSlotsKeepReplicasAndSkipMalformedOnes(t *testing.T) {
	r := newFakeNode(t, defaultFake)
	_, c := replicatedPair(t, ReadPreferReplica, r.addr, nil)
	if err := c.RefreshSlots(context.Background()); err != nil {
		t.Fatal(err)
	}
	tp := c.cl.t.Load()
	if len(tp.nodes) != 1 || len(tp.replicas[0]) != 1 || tp.replicas[0][0] != r.addr {
		t.Fatalf("nodes %v replicas %v", tp.addrs, tp.replicas)
	}
	if len(tp.read) != 1 || tp.read[0] == nil || tp.read[0].node.opts.Addr != r.addr {
		t.Fatalf("read routes %v", tp.read)
	}
}

func TestReplicaPolicyPrimaryOpensNoReplicaConnection(t *testing.T) {
	r := newFakeNode(t, defaultFake)
	p, c := replicatedPair(t, ReadPrimary, r.addr, nil)
	ctx := context.Background()
	if _, err := getS(c, ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if r.accepts.Load() != 0 || p.count("GET") != 1 || c.cl.t.Load().read != nil {
		t.Fatalf("replica accepts %d, primary %v", r.accepts.Load(), p.commands())
	}
}

func TestReplicaReadsGoToTheReplicaAndWritesToThePrimary(t *testing.T) {
	r := newFakeNode(t, defaultFake)
	p, c := replicatedPair(t, ReadPreferReplica, r.addr, nil)
	ctx := context.Background()
	got, err := getS(c, ctx, "k")
	if err != nil || got != "v:k@"+r.addr {
		t.Fatalf("GET = %q, %v", got, err)
	}
	if err := c.Set(ctx, "k", "v"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Do(ctx, "PING"); err != nil { // keyless: the seed
		t.Fatal(err)
	}
	if _, err := c.Do(ctx, "BLPOP", "q", "0.01"); err != nil { // blocking: the primary
		t.Fatal(err)
	}
	// READONLY is the first thing the replica connection receives
	r.mu.Lock()
	first := strings.Join(r.got[0], " ")
	r.mu.Unlock()
	if first != "READONLY" {
		t.Fatalf("first command on the replica: %q", first)
	}
	if cmds := r.commands(); len(cmds) != 2 || cmds[1] != "GET k" {
		t.Fatalf("replica saw %v", cmds)
	}
	if p.count("SET") != 1 || p.count("PING") != 1 || p.count("BLPOP") != 1 || p.count("GET") != 0 {
		t.Fatalf("primary saw %v", p.commands())
	}
}

func TestReplicaSplitReadsGoToTheReplicaAndSplitWritesToThePrimary(t *testing.T) {
	r := newFakeNode(t, defaultFake)
	p, c := replicatedPair(t, ReadPreferReplica, r.addr, nil)
	ctx := context.Background()
	keys := []string{keyFor(true, 1), keyFor(false, 1)}
	if _, err := c.MGet(ctx, keys...); err != nil {
		t.Fatal(err)
	}
	if n, err := c.Exists(ctx, keys...); err != nil || n != 2 {
		t.Fatalf("EXISTS = %d, %v", n, err)
	}
	if n, err := c.Del(ctx, keys...); err != nil || n != 2 {
		t.Fatalf("DEL = %d, %v", n, err)
	}
	if r.count("MGET") != 2 || r.count("EXISTS") != 2 || r.count("DEL") != 0 {
		t.Fatalf("replica saw %v", r.commands())
	}
	if p.count("DEL") != 2 || p.count("MGET") != 0 || p.count("EXISTS") != 0 {
		t.Fatalf("primary saw %v", p.commands())
	}
}

func TestReplicaPipelines(t *testing.T) {
	r := newFakeNode(t, defaultFake)
	p, c := replicatedPair(t, ReadPreferReplica, r.addr, nil)
	ctx := context.Background()
	if _, err := c.Pipelined(ctx, func(pl *Pipeline) { pl.Set("a", "1"); pl.Get("a") }); err != nil {
		t.Fatal(err)
	}
	if len(r.commands()) != 0 || p.count("SET") != 1 || p.count("GET") != 1 {
		t.Fatalf("mixed pipeline: replica %v primary %v", r.commands(), p.commands())
	}
	vals, err := c.Pipelined(ctx, func(pl *Pipeline) { pl.Get("a"); pl.Get("b"); pl.Do("MGET", keyFor(true, 3), keyFor(false, 3)) })
	if err != nil {
		t.Fatal(err)
	}
	if vals[0].Str() != "v:a@"+r.addr || vals[1].Str() != "v:b@"+r.addr || len(vals[2].Array) != 2 {
		t.Fatalf("replies %v", vals)
	}
	if p.count("GET") != 1 || p.count("MGET") != 0 || r.count("GET") != 2 || r.count("MGET") != 2 {
		t.Fatalf("read pipeline: replica %v primary %v", r.commands(), p.commands())
	}
	// a keyless read in an all-read pipeline stays on the seed
	if _, err := c.Pipelined(ctx, func(pl *Pipeline) { pl.Get("a"); pl.Do("RANDOMKEY") }); err != nil {
		t.Fatal(err)
	}
	if p.count("RANDOMKEY") != 1 || r.count("GET") != 3 {
		t.Fatalf("keyless: replica %v primary %v", r.commands(), p.commands())
	}
}

func TestReplicaUnreachableFallsBackOrFails(t *testing.T) {
	ctx := context.Background()
	p, c := replicatedPair(t, ReadPreferReplica, closedAddr(t), nil)
	if got, err := getS(c, ctx, "k"); err != nil || got != "v:k@"+p.addr {
		t.Fatalf("prefer: GET = %q, %v", got, err)
	}
	vals, err := c.Pipelined(ctx, func(pl *Pipeline) { pl.Get("a"); pl.Get("b") })
	if err != nil || vals[0].Str() != "v:a@"+p.addr {
		t.Fatalf("prefer: pipeline %v, %v", vals, err)
	}

	_, c = replicatedPair(t, ReadReplica, closedAddr(t), nil)
	if _, err := getS(c, ctx, "k"); err == nil {
		t.Fatal("replica policy: GET succeeded without a reachable replica")
	}
}

func TestReplicaLoadingFallsBackAndIsSkipped(t *testing.T) {
	ctx := context.Background()
	loading := func(f *fakeNode, args []string) string {
		if args[0] == "GET" || args[0] == "MGET" {
			return "-LOADING Ruvio is loading the dataset in memory\r\n"
		}
		return defaultFake(f, args)
	}
	r := newFakeNode(t, loading)
	p, c := replicatedPair(t, ReadPreferReplica, r.addr, nil)
	for i := 0; i < 2; i++ {
		if got, err := getS(c, ctx, "k"); err != nil || got != "v:k@"+p.addr {
			t.Fatalf("prefer: GET = %q, %v", got, err)
		}
	}
	if r.count("GET") != 1 || p.count("GET") != 2 {
		t.Fatalf("replica %v primary %v", r.commands(), p.commands())
	}

	r = newFakeNode(t, loading)
	p, c = replicatedPair(t, ReadPreferReplica, r.addr, nil)
	if _, err := c.MGet(ctx, keyFor(true, 1), keyFor(false, 1)); err != nil || p.count("MGET") != 2 {
		t.Fatalf("prefer: split MGET: %v, primary %v", err, p.commands())
	}

	r = newFakeNode(t, loading)
	_, c = replicatedPair(t, ReadReplica, r.addr, nil)
	if _, err := getS(c, ctx, "k"); err == nil || !strings.HasPrefix(err.Error(), "LOADING") {
		t.Fatalf("replica policy: GET error %v", err)
	}
	if _, err := getS(c, ctx, "k"); err == nil || !strings.Contains(err.Error(), "no replica") {
		t.Fatalf("replica policy, skipped: GET error %v", err)
	}
}

func TestReplicaPolicyWithoutAReplica(t *testing.T) {
	ctx := context.Background()
	_, c := replicatedPair(t, ReadReplica, "", nil)
	if _, err := getS(c, ctx, "k"); err == nil || !strings.Contains(err.Error(), "no replica") {
		t.Fatalf("GET error %v", err)
	}
	if err := c.Set(ctx, "k", "v"); err != nil {
		t.Fatalf("write: %v", err)
	}
	p, c := replicatedPair(t, ReadPreferReplica, "", nil)
	if got, err := getS(c, ctx, "k"); err != nil || got != "v:k@"+p.addr {
		t.Fatalf("prefer: GET = %q, %v", got, err)
	}
}

func TestReplicaDroppedReadIsNotSentAgain(t *testing.T) {
	r := newFakeNode(t, func(f *fakeNode, args []string) string {
		if args[0] == "GET" {
			return "DROP"
		}
		return defaultFake(f, args)
	})
	p, c := replicatedPair(t, ReadPreferReplica, r.addr, nil)
	if _, err := getS(c, context.Background(), "k"); err == nil {
		t.Fatal("GET succeeded on a dropped socket")
	}
	if r.count("GET") != 1 || p.count("GET") != 0 {
		t.Fatalf("replica %v primary %v", r.commands(), p.commands())
	}
}
