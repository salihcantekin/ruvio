// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestLiveMultiNodeMoved runs against the multi-node cluster scripts/run_with_servers.sh
// starts: node a (RUVIO_MULTINODE_A_ADDR, slots 0-8191), node b (RUVIO_MULTINODE_B_ADDR,
// slots 8192-16383) and a node with an older map (RUVIO_MULTINODE_STALE_ADDR) that gives
// every slot to node a. Skipped without them.
func TestLiveMultiNodeMoved(t *testing.T) {
	a, b, stale := os.Getenv("RUVIO_MULTINODE_A_ADDR"), os.Getenv("RUVIO_MULTINODE_B_ADDR"), os.Getenv("RUVIO_MULTINODE_STALE_ADDR")
	if a == "" || b == "" || stale == "" {
		t.Skip("RUVIO_MULTINODE_A_ADDR / RUVIO_MULTINODE_B_ADDR / RUVIO_MULTINODE_STALE_ADDR not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// node b's slot (12182) and node a's (5061)
	onB, onA := "{foo}go:multinode", "{bar}go:multinode"
	if KeySlot(onB) != 12182 || KeySlot(onA) != 5061 {
		t.Fatalf("slots %d, %d", KeySlot(onB), KeySlot(onA))
	}

	// Seeded with the stale node, the client first sends node b's key to node a, which
	// answers MOVED.
	c, err := Connect(ctx, Options{Addr: stale, Cluster: true})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for key, value := range map[string]string{onB: "b", onA: "a"} {
		if err := c.Set(ctx, key, value); err != nil {
			t.Fatal(err)
		}
		if v, ok, err := c.Get(ctx, key); err != nil || !ok || v != value {
			t.Fatalf("GET %s = %q, %v, %v", key, v, ok, err)
		}
	}

	// each key is on the node that owns it
	for addr, key := range map[string]string{b: onB, a: onA} {
		node := NewClient(Options{Addr: addr})
		v, ok, err := node.Get(ctx, key)
		node.Close()
		if err != nil || !ok || v != map[string]string{onB: "b", onA: "a"}[key] {
			t.Fatalf("GET %s on %s = %q, %v, %v", key, addr, v, ok, err)
		}
	}
}
