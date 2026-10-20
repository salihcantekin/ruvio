// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// liveReplicas runs against the pair scripts/run_with_servers.sh starts (a 2-shard primary
// on RUVIO_REPLICATED_ADDR, its 2-shard replica on RUVIO_REPLICA_ADDR); skipped without them.
func liveReplicas(t *testing.T) (primary, replica string) {
	t.Helper()
	primary, replica = os.Getenv("RUVIO_REPLICATED_ADDR"), os.Getenv("RUVIO_REPLICA_ADDR")
	if primary == "" || replica == "" {
		t.Skip("RUVIO_REPLICATED_ADDR / RUVIO_REPLICA_ADDR not set")
	}
	return primary, replica
}

// replicaShard connects, without cluster routing, to the replica shard that holds key.
func replicaShard(t *testing.T, replica, key string) *Client {
	t.Helper()
	host, port, _ := net.SplitHostPort(replica)
	p, _ := strconv.Atoi(port)
	if KeySlot(key) >= 8192 {
		p++
	}
	c := NewClient(Options{Addr: net.JoinHostPort(host, strconv.Itoa(p))})
	t.Cleanup(func() { c.Close() })
	return c
}

func getCommands(t *testing.T, ctx context.Context, c *Client) int {
	t.Helper()
	info, err := c.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(info, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "get_commands:"); ok {
			n, err := strconv.Atoi(v)
			if err != nil {
				t.Fatal(err)
			}
			return n
		}
	}
	t.Fatal("INFO reports no get_commands")
	return 0
}

func TestLiveReplicaReads(t *testing.T) {
	primary, replica := liveReplicas(t)
	for _, tc := range []struct {
		policy ReadPolicy
		name   string
		grows  int
	}{
		{ReadPreferReplica, "prefer", 1},
		{ReadReplica, "replica", 1},
		{ReadPrimary, "primary", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			key := "go:replica-reads:" + tc.name
			c, err := Connect(ctx, Options{Addr: primary, Cluster: true, ReadPolicy: tc.policy})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			shard := replicaShard(t, replica, key)

			if err := c.Set(ctx, key, "v1"); err != nil {
				t.Fatal(err)
			}
			// replication is asynchronous: wait until the replica shard has the key
			for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
				if v, ok, err := shard.Get(ctx, key); err == nil && ok && v == "v1" {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("the replica did not copy %s", key)
				}
			}
			before := getCommands(t, ctx, shard)
			if v, ok, err := c.Get(ctx, key); err != nil || !ok || v != "v1" {
				t.Fatalf("GET = %q, %v, %v", v, ok, err)
			}
			if got := getCommands(t, ctx, shard) - before; got != tc.grows {
				t.Fatalf("replica shard served %d GETs, want %d", got, tc.grows)
			}
		})
	}
}

func TestLiveDurabilityOnReplicatedPrimary(t *testing.T) {
	primary := os.Getenv("RUVIO_REPLICATED_ADDR")
	if primary == "" {
		t.Skip("RUVIO_REPLICATED_ADDR not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, cluster := range []bool{false, true} {
		c, err := Connect(ctx, Options{Addr: primary, Cluster: cluster, DurabilityReplicas: 1, DurabilityTimeout: 5 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		// the key is on the first shard (slot below 8192) so the plain client reaches it too
		key := "go:durable:{b}"
		if err := c.Set(ctx, key, "v"); err != nil {
			t.Fatal(err)
		}
		if v, ok, err := c.Get(ctx, key); err != nil || !ok || v != "v" {
			t.Fatalf("GET = %q, %v, %v", v, ok, err)
		}
		c.Close()
	}
}
