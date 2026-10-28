// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func closedAddr(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func TestConnectSkipsASeedThatCannotBeReached(t *testing.T) {
	live := okFake(t)
	c, err := Connect(context.Background(), Options{
		Seeds:       []string{closedAddr(t), live.addr},
		DialTimeout: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := live.commands(); strings.Join(got, "|") != "PING|PING" {
		t.Fatalf("got %v", got)
	}
}

func TestConnectWithEverySeedUnreachableReturnsTheLastConnectError(t *testing.T) {
	_, err := Connect(context.Background(), Options{
		Seeds:       []string{closedAddr(t), closedAddr(t)},
		DialTimeout: 500 * time.Millisecond,
	})
	if err == nil || !isConnectError(err) {
		t.Fatalf("want a connect error, got %v", err)
	}
}

func TestRefusedAuthStopsBeforeTheNextSeed(t *testing.T) {
	denied := newFakeNode(t, func(f *fakeNode, args []string) string {
		if args[0] == "AUTH" {
			return "-WRONGPASS invalid username-password pair\r\n"
		}
		return "+OK\r\n"
	})
	next := okFake(t)
	_, err := Connect(context.Background(), Options{Seeds: []string{denied.addr, next.addr}, Password: "bad"})
	var se *Error
	if !errorsAs(err, &se) || se.Prefix() != "WRONGPASS" {
		t.Fatalf("want WRONGPASS, got %v", err)
	}
	if next.accepts.Load() != 0 {
		t.Fatal("the second seed must not be tried after a refused AUTH")
	}
}

func errorsAs(err error, target **Error) bool {
	e, ok := err.(*Error)
	if ok {
		*target = e
	}
	return ok
}

func TestADroppedConnectionMovesOnToTheNextSeed(t *testing.T) {
	var drop atomic.Bool
	first := newFakeNode(t, func(f *fakeNode, args []string) string {
		if strings.ToUpper(args[0]) == "GET" && drop.Load() {
			return "DROP"
		}
		return defaultFake(f, args)
	})
	second := okFake(t)
	c, err := Connect(context.Background(), Options{Seeds: []string{first.addr, second.addr}, DisableFollowPrimary: true})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	drop.Store(true)
	if _, err := getS(c, ctx, "k"); err == nil {
		t.Fatal("the command that hit the drop must fail")
	}
	// The first seed still accepts, but the reconnect must go to the second one.
	if v, err := getS(c, ctx, "k"); err != nil || v != "value" {
		t.Fatalf("%q %v", v, err)
	}
	if first.accepts.Load() != 1 || second.accepts.Load() != 1 {
		t.Fatalf("accepts: first %d second %d", first.accepts.Load(), second.accepts.Load())
	}
}

func TestADedicatedConnectionStartsAtTheAddressTheCommandConnectionIsOn(t *testing.T) {
	var drop atomic.Bool
	first := newFakeNode(t, func(f *fakeNode, args []string) string {
		if strings.ToUpper(args[0]) == "GET" && drop.Load() {
			return "DROP"
		}
		return defaultFake(f, args)
	})
	second := okFake(t)
	c, err := Connect(context.Background(), Options{Seeds: []string{first.addr, second.addr}, DisableFollowPrimary: true})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	drop.Store(true)
	getS(c, ctx, "k")
	if _, err := getS(c, ctx, "k"); err != nil { // reconnects to the second seed
		t.Fatal(err)
	}
	before := first.accepts.Load()
	d, err := c.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if first.accepts.Load() != before {
		t.Fatal("a dedicated connection must start at the second seed, where the command connection is")
	}
	deadline := time.Now().Add(2 * time.Second)
	for second.accepts.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if second.accepts.Load() != 2 {
		t.Fatalf("second seed accepts: %d", second.accepts.Load())
	}
}

func TestACustomDialReceivesTheSeedBeingTried(t *testing.T) {
	live := okFake(t)
	var mu sync.Mutex
	var tried []string
	dead := "127.0.0.1:1"
	c := NewClient(Options{
		Seeds: []string{dead, live.addr},
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			mu.Lock()
			tried = append(tried, addr)
			mu.Unlock()
			if addr == dead {
				return nil, &net.OpError{Op: "dial", Err: net.ErrClosed}
			}
			var d net.Dialer
			return d.DialContext(ctx, network, addr)
		},
	})
	defer c.Close()
	if err := c.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(tried, []string{dead, live.addr}) {
		t.Fatalf("dialed %v", tried)
	}
}

func TestAMalformedSeedIsRefused(t *testing.T) {
	for _, seeds := range [][]string{{":6379"}, {"127.0.0.1"}, {"127.0.0.1:x"}, {"127.0.0.1:0"}, {"127.0.0.1:70000"}, {"[::1]"}} {
		_, err := Connect(context.Background(), Options{Seeds: seeds})
		if err == nil || !strings.Contains(err.Error(), "invalid seed") {
			t.Fatalf("%v: got %v", seeds, err)
		}
	}
	c := NewClient(Options{Seeds: []string{"[::1]:6379", "localhost:6380"}})
	defer c.Close()
	if c.cfgErr != nil {
		t.Fatalf("valid seeds refused: %v", c.cfgErr)
	}
}

// seedCluster starts a single-node "cluster" that owns every slot and answers GET.
func seedCluster(t *testing.T, drop *atomic.Bool) *fakeNode {
	var node *fakeNode
	node = newFakeNode(t, func(f *fakeNode, args []string) string {
		switch strings.ToUpper(args[0]) {
		case "CLUSTER":
			return slotsReply("0", "16383", node.addr)
		case "GET":
			if drop != nil && drop.Load() {
				return "DROP"
			}
			return bulk("value")
		}
		return "+OK\r\n"
	})
	return node
}

func TestALostNodeReloadsTheSlotMapFromASeed(t *testing.T) {
	var drop atomic.Bool
	gone := seedCluster(t, &drop)
	live := seedCluster(t, nil)
	c := NewClient(Options{Seeds: []string{gone.addr, live.addr}, Cluster: true, DialTimeout: 500 * time.Millisecond})
	defer c.Close()
	ctx := context.Background()
	if v, err := getS(c, ctx, "key"); err != nil || v != "value" {
		t.Fatalf("%q %v", v, err)
	}
	if !reflect.DeepEqual(c.Nodes(), []string{gone.addr}) {
		t.Fatalf("nodes %v", c.Nodes())
	}
	drop.Store(true)
	if _, err := getS(c, ctx, "key"); err == nil {
		t.Fatal("the command that hit the drop must fail")
	}
	gone.ln.Close() // the node cannot be opened again
	if v, err := getS(c, ctx, "key"); err != nil || v != "value" {
		t.Fatalf("%q %v", v, err)
	}
	if !reflect.DeepEqual(c.Nodes(), []string{live.addr}) {
		t.Fatalf("nodes %v", c.Nodes())
	}
	// A keyless command whose node is gone goes to the first reachable node of the new map.
	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestAFailedReloadLeavesNoSeedEndpointsBehind(t *testing.T) {
	var drop atomic.Bool
	gone := seedCluster(t, &drop)
	c := NewClient(Options{Seeds: []string{gone.addr, closedAddr(t)}, Cluster: true, DialTimeout: 500 * time.Millisecond})
	defer c.Close()
	ctx := context.Background()
	if _, err := getS(c, ctx, "key"); err != nil {
		t.Fatal(err)
	}
	drop.Store(true)
	getS(c, ctx, "key")
	gone.ln.Close()
	if _, err := getS(c, ctx, "key"); err == nil || !isConnectError(err) {
		t.Fatalf("want the connect error, got %v", err)
	}
	c.cl.mu.Lock()
	n := len(c.cl.nodes)
	c.cl.mu.Unlock()
	if n != 1 {
		t.Fatalf("seed entries stayed in the node list: %d", n)
	}
}
