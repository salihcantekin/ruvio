// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

// A cluster client leaves a demoted primary after its READONLY reply. Runs against the
// 2-shard failover pair that scripts/run_with_servers.sh starts (RUVIO_CLUSTER_FAILOVER_ADDRS,
// shard 0 of each, and RUVIO_CLUSTER_FAILOVER_REPLICATION, both primary first); skipped
// without it. No coordinator runs: the test swaps the roles with REPLICAOF, as a coordinator does.
func TestLiveAClusterClientReloadsItsSlotsAfterReadonlyFromADemotedPrimary(t *testing.T) {
	split := func(name string) (out [2]string) {
		parts := strings.Split(os.Getenv(name), ",")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			t.Skip(name + " not set")
		}
		copy(out[:], parts)
		return out
	}
	addrs, repl := split("RUVIO_CLUSTER_FAILOVER_ADDRS"), split("RUVIO_CLUSTER_FAILOVER_REPLICATION")
	// Whichever node is the primary now, so the test can run again on the same servers.
	old, next := 0, 1
	if !rawIsMaster(rawCmd(t, addrs[0], "ROLE")) {
		old, next = 1, 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c, err := Connect(ctx, Options{Addr: addrs[old], Cluster: true, DisableFollowPrimary: true, DialTimeout: 2 * time.Second, CommandTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	// One key on each shard: {a} hashes to slot 15495 (shard 1), {b} to slot 3300 (shard 0).
	keys := []string{fmt.Sprintf("cluster-failover:go:%d:{a}", os.Getpid()), fmt.Sprintf("cluster-failover:go:%d:{b}", os.Getpid())}
	for _, key := range keys {
		if err := c.Set(ctx, key, "before"); err != nil {
			t.Fatal(err)
		}
	}
	// A replica that still copies refuses to be promoted without FORCE.
	waitFollow(t, "the replica to hold the first writes", func() bool {
		for _, key := range keys {
			if v, _ := rawCmd(t, addrs[next], "GET", key).(string); v != "before" {
				return false
			}
		}
		return true
	})
	rawCmd(t, addrs[next], "REPLICAOF", "NO", "ONE")
	host, port, _ := net.SplitHostPort(repl[next])
	rawCmd(t, addrs[old], "REPLICAOF", host, port)
	waitFollow(t, "the old primary to become a replica", func() bool { return !rawIsMaster(rawCmd(t, addrs[old], "ROLE")) })

	// The demoted node names its new primary once that primary has accepted it.
	started := time.Now()
	for {
		err := c.Set(ctx, keys[0], "after")
		if err == nil {
			break
		}
		if !isReadonly(err) {
			t.Fatal(err)
		}
		if time.Since(started) > 5*time.Second {
			t.Fatalf("still refused after %v: %v", time.Since(started), err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if v, _ := rawCmd(t, addrs[next], "GET", keys[0]).(string); v != "after" {
		t.Fatalf("the new primary holds %q", v)
	}
	// The other shard's slots moved too: no write is refused once the table is reloaded.
	for round := 0; round < 10; round++ {
		for _, key := range keys {
			if err := c.Set(ctx, key, fmt.Sprint("v", round)); err != nil {
				t.Fatalf("round %d: %v", round, err)
			}
		}
	}
	for _, key := range keys {
		if v, _ := rawCmd(t, addrs[next], "GET", key).(string); v != "v9" {
			t.Fatalf("the new primary holds %q for %s", v, key)
		}
	}
}
