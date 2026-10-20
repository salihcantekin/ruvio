// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// A client with two seeds follows a failover on real servers. scripts/run_with_servers.sh
// starts a pair that can each be primary or replica (RUVIO_FAILOVER_ADDRS, their replication
// addresses in RUVIO_FAILOVER_REPLICATION, primary first); no coordinator runs, the test swaps
// the roles itself with REPLICAOF, as a coordinator does. Skipped without the variables.

const followPatience = 10 * time.Second

func followPair(t *testing.T) (clients, replication [2]string) {
	t.Helper()
	split := func(name string) (out [2]string) {
		parts := strings.Split(os.Getenv(name), ",")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			t.Skip(name + " not set")
		}
		copy(out[:], parts)
		return out
	}
	return split("RUVIO_FAILOVER_ADDRS"), split("RUVIO_FAILOVER_REPLICATION")
}

// rawCmd sends one command over a plain RESP2 socket and returns the reply: a string for
// simple, bulk and error replies, an int64 for integers, a []any for arrays, nil for null.
func rawCmd(t *testing.T, addr string, args ...string) any {
	t.Helper()
	nc, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	nc.SetDeadline(time.Now().Add(5 * time.Second))
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	if _, err := io.WriteString(nc, b.String()); err != nil {
		t.Fatal(err)
	}
	v, err := readRaw(bufio.NewReader(nc))
	if err != nil {
		t.Fatalf("%v: %v", args, err)
	}
	if s, ok := v.(rawError); ok {
		t.Fatalf("%v on %s: %s", args, addr, string(s))
	}
	return v
}

type rawError string

func readRaw(br *bufio.Reader) (any, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return nil, fmt.Errorf("empty reply line")
	}
	body := line[1:]
	switch line[0] {
	case '+':
		return body, nil
	case '-':
		return rawError(body), nil
	case ':':
		n, err := strconv.ParseInt(body, 10, 64)
		return n, err
	case '$':
		n, err := strconv.Atoi(body)
		if err != nil || n < 0 {
			return nil, err
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(br, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, err := strconv.Atoi(body)
		if err != nil || n < 0 {
			return nil, err
		}
		out := make([]any, n)
		for i := range out {
			if out[i], err = readRaw(br); err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	return nil, fmt.Errorf("unexpected reply %q", line)
}

func rawIsMaster(v any) bool {
	items, ok := v.([]any)
	if !ok || len(items) == 0 {
		return false
	}
	kind, _ := items[0].(string)
	return kind == "master"
}

func waitFollow(t *testing.T, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(followPatience)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("gave up waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestLiveAClientFollowsThePrimaryAfterTheRolesAreSwapped(t *testing.T) {
	addrs, repl := followPair(t)
	// Whichever node is the primary now, so the test can run again on the same servers.
	old, next := 0, 1
	if !rawIsMaster(rawCmd(t, addrs[0], "ROLE")) {
		old, next = 1, 0
	}
	opts := Options{Seeds: []string{addrs[old], addrs[next]}, DialTimeout: 2 * time.Second, CommandTimeout: 5 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	key := fmt.Sprintf("failover:%d", os.Getpid())

	c := NewClient(opts)
	defer c.Close()
	if err := c.Set(ctx, key, "before"); err != nil {
		t.Fatal(err)
	}
	// A replica that still copies refuses to be promoted without FORCE.
	waitFollow(t, "the replica to hold the first write", func() bool {
		v, _ := rawCmd(t, addrs[next], "GET", key).(string)
		return v == "before"
	})
	rawCmd(t, addrs[next], "REPLICAOF", "NO", "ONE")
	host, port, _ := net.SplitHostPort(repl[next])
	rawCmd(t, addrs[old], "REPLICAOF", host, port)

	started := time.Now()
	if err := c.Set(ctx, key, "after"); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(started); d > 5*time.Second {
		t.Fatalf("following took %v", d)
	}
	if v, _ := rawCmd(t, addrs[next], "GET", key).(string); v != "after" {
		t.Fatalf("the new primary holds %q", v)
	}

	// Without the option the same situation is the READONLY error.
	off := opts
	off.DisableFollowPrimary = true
	stuck := NewClient(off)
	defer stuck.Close()
	// stuck connects to its first seed, the old primary, which is a replica now.
	wantReadonly(t, stuck.Set(ctx, key, "never"))

	// A dropped socket: the reconnect starts at the old primary (now a replica) but takes the
	// node that answers master.
	id, err := c.Do(ctx, "CLIENT", "ID")
	if err != nil {
		t.Fatal(err)
	}
	rawCmd(t, addrs[next], "CLIENT", "KILL", "ID", strconv.FormatInt(id.Int, 10))
	waitFollow(t, "the client to see its socket closed", func() bool { return c.Ping(ctx) != nil })
	// Right after the reconnect, before any write: the client's own socket is on the primary.
	role, err := c.Role(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if role.Kind != KindArray || len(role.Array) == 0 || role.Array[0].Str() != "master" {
		t.Fatalf("role after the reconnect: %v", role)
	}
	if err := c.Set(ctx, key, "again"); err != nil {
		t.Fatal(err)
	}
	if v, _ := rawCmd(t, addrs[next], "GET", key).(string); v != "again" {
		t.Fatalf("the new primary holds %q", v)
	}
}
