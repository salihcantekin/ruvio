// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// A client follows a failover that real coordinators carry out. scripts/run_with_servers.sh
// starts a group of two data nodes (RUVIO_COORDINATOR_GROUP_ADDRS, their pids in
// RUVIO_COORDINATOR_GROUP_PIDS) watched by three `ruvio coordinator` processes
// (RUVIO_COORDINATOR_ADDRS, the group named RUVIO_COORDINATOR_MASTER). The test kills the
// primary with SIGKILL, so the group serves one run of the script. Skipped without the variables.

// The coordinators notice the kill after down_after_ms (1 s) and promote within a few ticks.
const coordinatedPatience = 15 * time.Second

func TestLiveClientsReachTheNewPrimaryAfterTheCoordinatorsFailOver(t *testing.T) {
	list := func(name string) []string {
		value := os.Getenv(name)
		if value == "" {
			t.Skip(name + " not set")
		}
		return strings.Split(value, ",")
	}
	coordinators := list("RUVIO_COORDINATOR_ADDRS")
	master := list("RUVIO_COORDINATOR_MASTER")[0]
	members := list("RUVIO_COORDINATOR_GROUP_ADDRS")
	pids := list("RUVIO_COORDINATOR_GROUP_PIDS")

	old, next := 0, 1
	if !rawIsMaster(rawCmd(t, members[0], "ROLE")) {
		old, next = 1, 0
	}
	if rawIsMaster(rawCmd(t, members[next], "ROLE")) || !rawIsMaster(rawCmd(t, members[old], "ROLE")) {
		t.Fatal("the coordinated group does not have exactly one primary")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// One client asks the coordinators for the primary, the other has the data nodes as seeds.
	clients := [2]*Client{
		NewClient(Options{Seeds: coordinators, SentinelMaster: master, DialTimeout: time.Second, CommandTimeout: 5 * time.Second}),
		NewClient(Options{Seeds: members, DialTimeout: time.Second, CommandTimeout: 5 * time.Second}),
	}
	keys := [2]string{
		fmt.Sprintf("coordinated:%d:sentinel", os.Getpid()),
		fmt.Sprintf("coordinated:%d:seeds", os.Getpid()),
	}
	for i, c := range clients {
		defer c.Close()
		if err := c.Set(ctx, keys[i], "before"); err != nil {
			t.Fatal(err)
		}
	}
	// Only a replica that holds every acknowledged write is promoted.
	deadline := time.Now().Add(coordinatedPatience)
	for _, key := range keys {
		for {
			if v, _ := rawCmd(t, members[next], "GET", key).(string); v == "before" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("gave up waiting for the replica to hold the first writes")
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	pid, err := strconv.Atoi(pids[old])
	if err != nil {
		t.Fatal(err)
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Kill(); err != nil {
		t.Fatalf("kill -9 %d: %v", pid, err)
	}

	// Each client writes until a write succeeds, both at once, so neither waits for the other.
	started := time.Now()
	var took [2]time.Duration
	var failed [2]error
	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				err := c.Set(ctx, keys[i], "after")
				if err == nil {
					took[i] = time.Since(started)
					return
				}
				if time.Since(started) > coordinatedPatience {
					failed[i] = err
					return
				}
				time.Sleep(50 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
	for i, err := range failed {
		if err != nil {
			t.Fatalf("no write through client %d succeeded within %v: %v", i, coordinatedPatience, err)
		}
	}
	t.Logf("first write after the kill: SentinelMaster %v, seeds %v", took[0], took[1])

	if !rawIsMaster(rawCmd(t, members[next], "ROLE")) {
		t.Fatal("the surviving member is not the primary")
	}
	for _, key := range keys {
		if v, _ := rawCmd(t, members[next], "GET", key).(string); v != "after" {
			t.Fatalf("the new primary holds %q for %s", v, key)
		}
	}
}
