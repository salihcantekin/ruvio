// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- slot table, hashtags, key extraction -----------------------------------------------------

func TestHashSlotKnownValues(t *testing.T) {
	// values reported by CLUSTER KEYSLOT on a Ruvio 3-shard server
	for key, want := range map[string]int{"foo": 12182, "bar": 5061, "baz": 4813, "a": 15495, "b": 3300, "c": 7365, "{a}b": 15495} {
		if got := KeySlot(key); got != want {
			t.Errorf("KeySlot(%q) = %d, want %d", key, got, want)
		}
		if got := hashSlot([]byte(key)); got != want {
			t.Errorf("hashSlot(%q) = %d, want %d", key, got, want)
		}
	}
	// hashtag rules: first non-empty {tag}; "{}" and an unclosed "{" hash the whole key.
	if KeySlot("{user1}.a") != KeySlot("{user1}.b") || KeySlot("{user1}.a") != KeySlot("user1") {
		t.Error("hashtag must hash only the tag")
	}
	if KeySlot("a{}b") == KeySlot("") {
		t.Error("empty tag hashes the whole key")
	}
	if KeySlot("x{abc") == KeySlot("abc") {
		t.Error("unclosed tag hashes the whole key")
	}
	if KeySlot("{a}{b}") != KeySlot("a") {
		t.Error("only the first tag counts")
	}
}

func TestCommandKeys(t *testing.T) {
	cases := []struct {
		args []string
		want []string
	}{
		{[]string{"GET", "k"}, []string{"k"}},
		{[]string{"get", "k"}, []string{"k"}},
		{[]string{"PING"}, nil},
		{[]string{"PING", "hello"}, nil},
		{[]string{"MGET", "a", "b", "c"}, []string{"a", "b", "c"}},
		{[]string{"DEL", "a", "b"}, []string{"a", "b"}},
		{[]string{"MSET", "a", "1", "b", "2"}, []string{"a", "b"}},
		{[]string{"MSETNX", "a", "1", "b", "2"}, []string{"a", "b"}},
		{[]string{"RENAME", "a", "b"}, []string{"a", "b"}},
		{[]string{"LMOVE", "a", "b", "LEFT", "RIGHT"}, []string{"a", "b"}},
		{[]string{"BLMOVE", "a", "b", "LEFT", "RIGHT", "1"}, []string{"a", "b"}},
		{[]string{"GEOSEARCHSTORE", "dst", "src", "FROMLONLAT", "1", "1"}, []string{"dst", "src"}},
		{[]string{"BLPOP", "a", "b", "1.5"}, []string{"a", "b"}},
		{[]string{"BZPOPMIN", "a", "1"}, []string{"a"}},
		{[]string{"EVAL", "return 1", "2", "k1", "k2", "arg"}, []string{"k1", "k2"}},
		{[]string{"EVAL", "return 1", "0", "arg"}, nil},
		{[]string{"EVALSHA", "sha", "1", "k1", "x"}, []string{"k1"}},
		{[]string{"FCALL", "fn", "1", "k1"}, []string{"k1"}},
		{[]string{"ZUNION", "2", "a", "b", "WITHSCORES"}, []string{"a", "b"}},
		{[]string{"ZINTER", "2", "a", "b"}, []string{"a", "b"}},
		{[]string{"ZUNIONSTORE", "dst", "2", "a", "b"}, []string{"dst", "a", "b"}},
		{[]string{"ZDIFFSTORE", "dst", "1", "a"}, []string{"dst", "a"}},
		{[]string{"LMPOP", "2", "a", "b", "LEFT"}, []string{"a", "b"}},
		{[]string{"BLMPOP", "0", "2", "a", "b", "LEFT"}, []string{"a", "b"}},
		{[]string{"SINTERCARD", "2", "a", "b", "LIMIT", "3"}, []string{"a", "b"}},
		{[]string{"XREAD", "COUNT", "2", "STREAMS", "s1", "s2", "0", "0"}, []string{"s1", "s2"}},
		{[]string{"XREADGROUP", "GROUP", "g", "c", "BLOCK", "10", "STREAMS", "s1", ">"}, []string{"s1"}},
		{[]string{"OBJECT", "ENCODING", "k"}, []string{"k"}},
		{[]string{"MEMORY", "USAGE", "k"}, []string{"k"}},
		{[]string{"XGROUP", "CREATE", "s", "g", "$"}, []string{"s"}},
		{[]string{"XINFO", "STREAM", "s"}, []string{"s"}},
		{[]string{"BITOP", "AND", "dst", "a", "b"}, []string{"dst", "a", "b"}},
		{[]string{"JSON.MGET", "a", "b", "$.x"}, []string{"a", "b"}},
		{[]string{"GEORADIUS", "g", "1", "1", "5", "km", "STORE", "d"}, []string{"g", "d"}},
		{[]string{"GEORADIUSBYMEMBER", "g", "m", "5", "km", "STOREDIST", "d"}, []string{"g", "d"}},
		{[]string{"WATCH", "a", "b"}, []string{"a", "b"}},
		{[]string{"PFMERGE", "d", "a", "b"}, []string{"d", "a", "b"}},
		{[]string{"SPUBLISH", "ch", "msg"}, []string{"ch"}},
		{[]string{"SSUBSCRIBE", "ch"}, []string{"ch"}},
		{[]string{"SCRIPT", "LOAD", "x"}, nil},
		{[]string{"CLUSTER", "KEYSLOT", "k"}, nil},
		{[]string{"CLIENT", "SETNAME", "n"}, nil},
		{[]string{"JSON.FIND", "idx", "q"}, nil},
		{[]string{"JSON.INDEX", "CREATE", "idx"}, nil},
		{[]string{"IDEM", "k", "BEGIN", "fp", "o", "100"}, []string{"k"}},
		{[]string{"UNKNOWNCMD", "k"}, []string{"k"}},
	}
	for _, c := range cases {
		if got := CommandKeys(c.args...); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%v: keys %v, want %v", c.args, got, c.want)
		}
	}
}

// Every command of the compatibility matrix must have an explicit routing entry.
func TestRoutingCoversMatrix(t *testing.T) {
	raw, err := os.ReadFile("../../docs/compatibility-matrix.json")
	if err != nil {
		t.Skip(err)
	}
	var m struct {
		Commands []struct{ Name string } `json:"commands"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	for _, c := range m.Commands {
		first := strings.Fields(c.Name)[0]
		if _, ok := cmdSpecs[first]; !ok {
			t.Errorf("%s has no routing entry", c.Name)
		}
	}
	t.Logf("routing entries for all %d matrix commands", len(m.Commands))
}

func TestRouteFrame(t *testing.T) {
	enc := func(args ...string) []byte {
		b := appendArrayHeader(nil, len(args))
		for _, a := range args {
			b = appendBulkString(b, a)
		}
		return b
	}
	_, s, _, err := routeFrame(enc("GET", "foo"))
	if err != nil || s != 12182 {
		t.Fatalf("GET foo: %d %v", s, err)
	}
	if _, s, _, _ = routeFrame(enc("PING")); s != -1 {
		t.Fatalf("PING slot %d", s)
	}
	if _, s, _, _ = routeFrame(enc("SUNION", "{t}a", "{t}b")); s != KeySlot("t") {
		t.Fatalf("same tag slot %d", s)
	}
	if _, s, _, _ = routeFrame(enc("SUNION", "a", "b")); s != slotCross {
		t.Fatalf("cross slot %d", s)
	}
	if _, _, _, err = routeFrame([]byte("garbage")); err == nil {
		t.Fatal("malformed frame accepted")
	}
}

func TestRouteFrameDoesNotAllocate(t *testing.T) {
	b := appendArrayHeader(nil, 3)
	b = append(b, cSET...)
	b = appendBulkString(b, "some:key:1")
	b = appendBulkString(b, "value")
	if n := testing.AllocsPerRun(100, func() { routeFrame(b) }); n != 0 {
		t.Errorf("routeFrame allocates %v", n)
	}
	m := appendArrayHeader(nil, 4)
	m = append(m, cSUNION...)
	m = appendBulkString(m, "{t}a")
	m = appendBulkString(m, "{t}b")
	m = appendBulkString(m, "{t}c")
	if n := testing.AllocsPerRun(100, func() { routeFrame(m) }); n != 0 {
		t.Errorf("routeFrame (multi-key) allocates %v", n)
	}
}

func TestParseMoved(t *testing.T) {
	s, a, ok := parseMoved([]byte("MOVED 12182 127.0.0.1:17312"))
	if !ok || s != 12182 || a != "127.0.0.1:17312" {
		t.Fatal(s, a, ok)
	}
	for _, bad := range []string{"ERR x", "MOVED x host:1", "MOVED 99999 h:1", "MOVED 5", "MOVED 5 "} {
		if _, _, ok := parseMoved([]byte(bad)); ok {
			t.Errorf("accepted %q", bad)
		}
	}
}

// ---- scripted fake server ----------------------------------------------------------------------

type fakeNode struct {
	t       *testing.T
	ln      net.Listener
	addr    string
	mu      sync.Mutex
	got     [][]string
	handler func(f *fakeNode, args []string) string // raw RESP reply; "DROP" closes the connection
	accepts atomic.Int32
}

func newFakeNode(t *testing.T, h func(f *fakeNode, args []string) string) *fakeNode {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeNode{t: t, ln: ln, addr: ln.Addr().String(), handler: h}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			f.accepts.Add(1)
			go f.serve(nc)
		}
	}()
	return f
}

func (f *fakeNode) serve(nc net.Conn) {
	defer nc.Close()
	br := bufio.NewReader(nc)
	for {
		args, err := readFakeCmd(br)
		if err != nil {
			return
		}
		f.mu.Lock()
		f.got = append(f.got, args)
		f.mu.Unlock()
		rep := f.handler(f, args)
		if rep == "DROP" {
			return
		}
		if _, err := io.WriteString(nc, rep); err != nil {
			return
		}
	}
}

func readFakeCmd(br *bufio.Reader) ([]string, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return nil, err
	}
	n, _ := strconv.Atoi(strings.TrimSpace(line[1:]))
	args := make([]string, n)
	for i := range args {
		l, err := br.ReadString('\n')
		if err != nil {
			return nil, err
		}
		sz, _ := strconv.Atoi(strings.TrimSpace(l[1:]))
		buf := make([]byte, sz+2)
		if _, err := io.ReadFull(br, buf); err != nil {
			return nil, err
		}
		args[i] = string(buf[:sz])
	}
	return args, nil
}

// commands returns the received commands except CLUSTER SLOTS.
func (f *fakeNode) commands() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, a := range f.got {
		if a[0] == "CLUSTER" {
			continue
		}
		out = append(out, strings.Join(a, " "))
	}
	return out
}

func (f *fakeNode) count(name string) int {
	n := 0
	for _, c := range f.commands() {
		if strings.HasPrefix(c, name) {
			n++
		}
	}
	return n
}

func getS(c *Client, ctx context.Context, key string) (string, error) {
	v, _, err := c.Get(ctx, key)
	return v, err
}

func bulk(s string) string { return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s) }

func slotsReply(parts ...string) string {
	// parts: lo, hi, addr triples
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(parts)/3)
	for i := 0; i < len(parts); i += 3 {
		host, port, _ := net.SplitHostPort(parts[i+2])
		p, _ := strconv.Atoi(port)
		fmt.Fprintf(&b, "*3\r\n:%s\r\n:%s\r\n*3\r\n%s:%d\r\n%s", parts[i], parts[i+1], bulk(host), p, bulk("id"+port))
	}
	return b.String()
}

// twoNodes starts nodes A (slots 0-8191) and B (8192-16383). Both answer CLUSTER SLOTS;
// other commands go to the per-node handler (default: +OK, ECHO-style replies for GET).
func twoNodes(t *testing.T, ha, hb func(f *fakeNode, args []string) string) (a, b *fakeNode) {
	var slots string
	wrap := func(h func(f *fakeNode, args []string) string) func(*fakeNode, []string) string {
		return func(f *fakeNode, args []string) string {
			if args[0] == "CLUSTER" {
				return slots
			}
			if h != nil {
				return h(f, args)
			}
			return defaultFake(f, args)
		}
	}
	a = newFakeNode(t, wrap(ha))
	b = newFakeNode(t, wrap(hb))
	slots = slotsReply("0", "8191", a.addr, "8192", "16383", b.addr)
	return
}

func defaultFake(f *fakeNode, args []string) string {
	switch strings.ToUpper(args[0]) {
	case "GET":
		return bulk("v:" + args[1] + "@" + f.addr)
	case "SET":
		return "+OK\r\n"
	case "MGET":
		var b strings.Builder
		fmt.Fprintf(&b, "*%d\r\n", len(args)-1)
		for _, k := range args[1:] {
			b.WriteString(bulk("v:" + k))
		}
		return b.String()
	case "MSET":
		return "+OK\r\n"
	case "DEL", "EXISTS", "TOUCH", "UNLINK":
		return fmt.Sprintf(":%d\r\n", len(args)-1)
	case "PING":
		return "+PONG\r\n"
	case "INCR":
		return ":1\r\n"
	case "SUNION", "SINTER", "SDIFF":
		return "*0\r\n"
	}
	return "+OK\r\n"
}

// keyFor returns a key whose slot is below (low) or at/above 8192.
func keyFor(low bool, i int) string {
	for n := i; ; n++ {
		k := fmt.Sprintf("key:%d", n)
		if (KeySlot(k) < 8192) == low {
			return k
		}
	}
}

func newClusterClient(t *testing.T, seed string) *Client {
	c := NewClient(Options{Addr: seed, Cluster: true, DialTimeout: 2 * time.Second})
	t.Cleanup(func() { c.Close() })
	return c
}

func TestFakeRoutingByKey(t *testing.T) {
	a, b := twoNodes(t, nil, nil)
	c := newClusterClient(t, a.addr)
	ctx := context.Background()
	ka, kb := keyFor(true, 0), keyFor(false, 0)
	va, err := getS(c, ctx, ka)
	if err != nil || va != "v:"+ka+"@"+a.addr {
		t.Fatalf("%q %v", va, err)
	}
	vb, err := getS(c, ctx, kb)
	if err != nil || vb != "v:"+kb+"@"+b.addr {
		t.Fatalf("%q %v", vb, err)
	}
	if !c.IsCluster() || len(c.Nodes()) != 2 {
		t.Fatal(c.Nodes())
	}
	if a.count("GET") != 1 || b.count("GET") != 1 {
		t.Fatalf("a=%v b=%v", a.commands(), b.commands())
	}
	// a hashtag pins keys together
	if _, err := getS(c, ctx, "{"+ka+"}x"); err != nil {
		t.Fatal(err)
	}
	if a.count("GET") != 2 {
		t.Fatalf("hashtag routed wrongly: a=%v", a.commands())
	}
}

func TestFakeStandaloneFallback(t *testing.T) {
	n := newFakeNode(t, func(f *fakeNode, args []string) string {
		if args[0] == "CLUSTER" {
			return "-ERR cluster mode is disabled\r\n"
		}
		return defaultFake(f, args)
	})
	c := newClusterClient(t, n.addr)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := getS(c, ctx, keyFor(i%2 == 0, i)); err != nil {
			t.Fatal(err)
		}
	}
	if c.IsCluster() || c.Nodes() != nil {
		t.Fatal("must report standalone")
	}
	if n.count("GET") != 3 {
		t.Fatal(n.commands())
	}
	// CLUSTER SLOTS asked exactly once
	n.mu.Lock()
	asked := 0
	for _, a := range n.got {
		if a[0] == "CLUSTER" {
			asked++
		}
	}
	n.mu.Unlock()
	if asked != 1 {
		t.Fatalf("CLUSTER SLOTS sent %d times", asked)
	}
	// cross-slot multi-key commands are the server's business in standalone mode
	if _, err := c.SUnion(ctx, "a", "b"); err != nil {
		t.Fatal(err)
	}
}

func TestFakeCrossSlotFailsFast(t *testing.T) {
	a, b := twoNodes(t, nil, nil)
	c := newClusterClient(t, a.addr)
	ctx := context.Background()
	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	ka, kb := keyFor(true, 0), keyFor(false, 0)
	_, err := c.SUnion(ctx, ka, kb)
	if !errors.Is(err, ErrCrossSlot) {
		t.Fatalf("want ErrCrossSlot, got %v", err)
	}
	if !strings.Contains(err.Error(), "SUNION") {
		t.Fatalf("error should name the command: %v", err)
	}
	if err = c.Rename(ctx, ka, kb); !errors.Is(err, ErrCrossSlot) {
		t.Fatalf("RENAME across slots: %v", err)
	}
	if n := len(a.commands()) + len(b.commands()); n != 1 { // only the PING
		t.Fatalf("cross-slot call sent something: a=%v b=%v", a.commands(), b.commands())
	}
	// the same keys with a shared hashtag work
	if _, err := c.SUnion(ctx, "{x}"+ka, "{x}"+kb); err != nil {
		t.Fatal(err)
	}
}

func TestFakeSplitKeepsCallerOrder(t *testing.T) {
	a, b := twoNodes(t, nil, nil)
	c := newClusterClient(t, a.addr)
	ctx := context.Background()
	var keys []string
	for i := 0; i < 12; i++ {
		keys = append(keys, keyFor(i%3 != 0, i*7))
	}
	got, err := c.MGet(ctx, keys...)
	if err != nil {
		t.Fatal(err)
	}
	for i, k := range keys {
		if !got[i].Valid || got[i].Val != "v:"+k {
			t.Fatalf("position %d: %+v, want %s", i, got[i], k)
		}
	}
	// one MGET per distinct slot, none carrying a foreign key
	for _, f := range []*fakeNode{a, b} {
		f.mu.Lock()
		for _, cmd := range f.got {
			if cmd[0] != "MGET" {
				continue
			}
			s := KeySlot(cmd[1])
			for _, k := range cmd[2:] {
				if KeySlot(k) != s {
					t.Fatalf("MGET part mixes slots: %v", cmd)
				}
			}
			if (s < 8192) != (f == a) {
				t.Fatalf("part sent to the wrong node: %v", cmd)
			}
		}
		f.mu.Unlock()
	}
	n, err := c.Del(ctx, keys...)
	if err != nil || n != int64(len(keys)) {
		t.Fatalf("DEL sum %d %v", n, err)
	}
	if err := c.MSet(ctx, keys[0], "1", keys[1], "2", keys[2], "3"); err != nil {
		t.Fatal(err)
	}
}

// slotsAsked counts the CLUSTER SLOTS a node received.
func (f *fakeNode) slotsAsked() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, a := range f.got {
		if a[0] == "CLUSTER" {
			n++
		}
	}
	return n
}

func TestFakeMovedPatchesSlotWithoutReload(t *testing.T) {
	k := keyFor(true, 0) // the table says node A, but B really owns it
	var b *fakeNode
	a, b := twoNodes(t, func(f *fakeNode, args []string) string {
		if strings.ToUpper(args[0]) == "GET" {
			return fmt.Sprintf("-MOVED %d %s\r\n", KeySlot(args[1]), b.addr)
		}
		return defaultFake(f, args)
	}, nil)
	c := newClusterClient(t, a.addr)
	ctx := context.Background()
	v, err := getS(c, ctx, k)
	if err != nil || v != "v:"+k+"@"+b.addr {
		t.Fatalf("%q %v", v, err)
	}
	if a.count("GET") != 1 || b.count("GET") != 1 {
		t.Fatalf("a=%v b=%v", a.commands(), b.commands())
	}
	// the slot now points at B: no more detours through A
	if _, err := getS(c, ctx, k); err != nil {
		t.Fatal(err)
	}
	if a.count("GET") != 1 || b.count("GET") != 2 {
		t.Fatalf("table not patched: a=%v b=%v", a.commands(), b.commands())
	}
	// a MOVED for another slot (of another command) patches that slot too, still no reload
	if _, err := getS(c, ctx, keyFor(true, 50)); err != nil {
		t.Fatal(err)
	}
	if n := a.slotsAsked() + b.slotsAsked(); n != 1 { // discovery only
		t.Fatalf("CLUSTER SLOTS sent %d times; a known node must only patch the slot", n)
	}
}

func TestFakeMovedFollowsThreeRedirects(t *testing.T) {
	// A always sends GET to B; B bounces the first GET back to A. The command goes
	// A -> B -> (reload) A -> (reload) B: three redirects, then the value.
	var a, b *fakeNode
	var bounced atomic.Bool
	a, b = twoNodes(t, func(f *fakeNode, args []string) string {
		if strings.ToUpper(args[0]) == "GET" {
			return fmt.Sprintf("-MOVED %d %s\r\n", KeySlot(args[1]), b.addr)
		}
		return defaultFake(f, args)
	}, func(f *fakeNode, args []string) string {
		if strings.ToUpper(args[0]) == "GET" && bounced.CompareAndSwap(false, true) {
			return fmt.Sprintf("-MOVED %d %s\r\n", KeySlot(args[1]), a.addr)
		}
		return defaultFake(f, args)
	})
	c := newClusterClient(t, a.addr)
	k := keyFor(true, 0)
	v, err := getS(c, context.Background(), k)
	if err != nil || v != "v:"+k+"@"+b.addr {
		t.Fatalf("%q %v", v, err)
	}
	if a.count("GET") != 2 || b.count("GET") != 2 {
		t.Fatalf("a=%v b=%v", a.commands(), b.commands())
	}
	// discovery, then a reload on the second and on the third MOVED (not on the first)
	if n := a.slotsAsked() + b.slotsAsked(); n != 3 {
		t.Fatalf("CLUSTER SLOTS sent %d times, want 3", n)
	}
}

func TestFakeMovedGivesUpAfterThreeRedirects(t *testing.T) {
	var a, b *fakeNode
	mv := func(to func() *fakeNode) func(*fakeNode, []string) string {
		return func(f *fakeNode, args []string) string {
			if strings.ToUpper(args[0]) == "GET" {
				return fmt.Sprintf("-MOVED %d %s\r\n", KeySlot(args[1]), to().addr)
			}
			return defaultFake(f, args)
		}
	}
	a, b = twoNodes(t, mv(func() *fakeNode { return b }), mv(func() *fakeNode { return a }))
	c := newClusterClient(t, a.addr)
	_, err := getS(c, context.Background(), keyFor(true, 0))
	var se *Error
	if !errors.As(err, &se) || se.Prefix() != "MOVED" {
		t.Fatalf("want the fourth MOVED returned, got %v", err)
	}
	if a.count("GET")+b.count("GET") != 4 {
		t.Fatalf("expected the first send and three redirects: a=%v b=%v", a.commands(), b.commands())
	}
	// the same limit holds for a command repaired inside a pipeline
	vals, err := c.Pipelined(context.Background(), func(p *Pipeline) { p.Get(keyFor(true, 0)) })
	if !errors.As(err, &se) || se.Prefix() != "MOVED" || vals[0].Kind != KindError {
		t.Fatalf("pipeline: %v %v", vals, err)
	}
	if a.count("GET")+b.count("GET") != 8 {
		t.Fatalf("pipeline redirects: a=%v b=%v", a.commands(), b.commands())
	}
}

func TestFakeMovedToUnknownNodeRefreshesTable(t *testing.T) {
	k := keyFor(true, 0)
	var slots atomic.Value
	var c3 *fakeNode
	c3 = newFakeNode(t, func(f *fakeNode, args []string) string {
		if args[0] == "CLUSTER" {
			return slots.Load().(string)
		}
		return defaultFake(f, args)
	})
	var a, b *fakeNode
	a, b = twoNodes(t, func(f *fakeNode, args []string) string {
		if strings.ToUpper(args[0]) == "GET" {
			return fmt.Sprintf("-MOVED %d %s\r\n", KeySlot(args[1]), c3.addr)
		}
		return defaultFake(f, args)
	}, nil)
	// after the move node C owns 0-4095 and A keeps the rest of its range
	slots.Store(slotsReply("0", "4095", c3.addr, "4096", "8191", a.addr, "8192", "16383", b.addr))
	a.handler = func(f *fakeNode, args []string) string {
		if args[0] == "CLUSTER" {
			return slots.Load().(string)
		}
		if strings.ToUpper(args[0]) == "GET" {
			return fmt.Sprintf("-MOVED %d %s\r\n", KeySlot(args[1]), c3.addr)
		}
		return defaultFake(f, args)
	}
	c := newClusterClient(t, a.addr)
	ctx := context.Background()
	if err := c.Ping(ctx); err != nil { // discover with the old two-node view
		t.Fatal(err)
	}
	slots.Store(slotsReply("0", "4095", c3.addr, "4096", "8191", a.addr, "8192", "16383", b.addr))
	v, err := getS(c, ctx, k)
	if err != nil || !strings.HasSuffix(v, "@"+c3.addr) {
		t.Fatalf("%q %v", v, err)
	}
	if len(c.Nodes()) != 3 {
		t.Fatalf("table not refreshed: %v", c.Nodes())
	}
}

func TestFakeConnectionLossIsIsolatedAndNotReplayed(t *testing.T) {
	var drop atomic.Bool
	a, b := twoNodes(t, func(f *fakeNode, args []string) string {
		if strings.ToUpper(args[0]) == "INCR" && drop.CompareAndSwap(true, false) {
			return "DROP"
		}
		return defaultFake(f, args)
	}, nil)
	c := newClusterClient(t, a.addr)
	ctx := context.Background()
	ka, kb := keyFor(true, 0), keyFor(false, 0)
	if _, err := c.Incr(ctx, ka); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Incr(ctx, kb); err != nil {
		t.Fatal(err)
	}
	drop.Store(true)
	if _, err := c.Incr(ctx, ka); err == nil {
		t.Fatal("expected a transport error")
	} else if !errors.Is(err, ErrClosed) {
		t.Fatalf("unexpected error type: %v", err)
	}
	if a.count("INCR") != 2 {
		t.Fatalf("a failed non-idempotent command must not be replayed: %v", a.commands())
	}
	// node B is unaffected, node A reconnects lazily
	if _, err := c.Incr(ctx, kb); err != nil {
		t.Fatalf("other node broken: %v", err)
	}
	if _, err := c.Incr(ctx, ka); err != nil {
		t.Fatalf("no lazy reconnect: %v", err)
	}
	if b.accepts.Load() != 1 {
		t.Fatalf("node B reconnected: %d connections", b.accepts.Load())
	}
	if a.accepts.Load() != 2 {
		t.Fatalf("node A connections: %d", a.accepts.Load())
	}
}

func TestFakePipelineGroupsByNodeAndKeepsOrder(t *testing.T) {
	a, b := twoNodes(t, nil, nil)
	c := newClusterClient(t, a.addr)
	ctx := context.Background()
	var keys []string
	for i := 0; i < 10; i++ {
		keys = append(keys, keyFor(i%2 == 0, i*11))
	}
	vals, err := c.Pipelined(ctx, func(p *Pipeline) {
		for _, k := range keys {
			p.Get(k)
		}
		p.Do("PING")
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, k := range keys {
		low := KeySlot(k) < 8192
		node := b.addr
		if low {
			node = a.addr
		}
		if vals[i].Str() != "v:"+k+"@"+node {
			t.Fatalf("reply %d = %q", i, vals[i].Str())
		}
	}
	if vals[len(keys)].Str() != "PONG" {
		t.Fatalf("PING reply %v", vals[len(keys)])
	}
	// one pipelined write per node: each node saw its GETs back to back
	if a.count("GET") != 5 || b.count("GET") != 5 {
		t.Fatalf("a=%v b=%v", a.commands(), b.commands())
	}
}

func TestFakePipelineMovedIsRepaired(t *testing.T) {
	var b *fakeNode
	ka := keyFor(true, 0)
	a, b := twoNodes(t, func(f *fakeNode, args []string) string {
		if strings.ToUpper(args[0]) == "GET" && args[1] == ka {
			return fmt.Sprintf("-MOVED %d %s\r\n", KeySlot(args[1]), b.addr)
		}
		return defaultFake(f, args)
	}, nil)
	c := newClusterClient(t, a.addr)
	ctx := context.Background()
	vals, err := c.Pipelined(ctx, func(p *Pipeline) { p.Get(keyFor(true, 100)); p.Get(ka); p.Get(keyFor(true, 200)) })
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(vals[1].Str(), "@"+b.addr) {
		t.Fatalf("moved reply not repaired: %v", vals[1])
	}
	if !strings.HasSuffix(vals[0].Str(), "@"+a.addr) || !strings.HasSuffix(vals[2].Str(), "@"+a.addr) {
		t.Fatalf("neighbours disturbed: %v %v", vals[0], vals[2])
	}
}

func TestFakePipelineSplitsCrossSlotCommands(t *testing.T) {
	a, b := twoNodes(t, nil, nil)
	c := newClusterClient(t, a.addr)
	ctx := context.Background()
	var keys []string
	for i := 0; i < 6; i++ {
		keys = append(keys, keyFor(i%2 == 0, i*13))
	}
	ka := keyFor(true, 500)
	vals, err := c.Pipelined(ctx, func(p *Pipeline) {
		p.Set(ka, "1")
		p.Do(append([]any{"MGET"}, toAny(keys)...)...)
		p.Get(keyFor(false, 500))
		p.Del(keys...)
		p.Do(append([]any{"EXISTS"}, toAny(keys[:3])...)...)
		p.Do("MSET", keys[0], "x", keys[1], "y")
		p.Do("PING")
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 7 || vals[0].Str() != "OK" || vals[6].Str() != "PONG" {
		t.Fatalf("replies %v", vals)
	}
	if vals[1].Kind != KindArray || len(vals[1].Array) != len(keys) {
		t.Fatalf("MGET reply %v", vals[1])
	}
	for i, k := range keys {
		if vals[1].Array[i].Str() != "v:"+k {
			t.Fatalf("MGET position %d = %v, want %s", i, vals[1].Array[i], k)
		}
	}
	if !strings.HasSuffix(vals[2].Str(), "@"+b.addr) {
		t.Fatalf("GET reply %v", vals[2])
	}
	if vals[3].Int != int64(len(keys)) || vals[4].Int != 3 || vals[5].Str() != "OK" {
		t.Fatalf("merged replies %v %v %v", vals[3], vals[4], vals[5])
	}
	// every command sent carries one slot, and goes to the node owning it
	for _, f := range []*fakeNode{a, b} {
		f.mu.Lock()
		for _, cmd := range f.got {
			step := 1
			switch cmd[0] {
			case "MSET":
				step = 2
			case "MGET", "DEL", "EXISTS":
			default:
				continue
			}
			s := KeySlot(cmd[1])
			for i := 1; i < len(cmd); i += step {
				if KeySlot(cmd[i]) != s {
					t.Errorf("%s part mixes slots: %v", cmd[0], cmd)
				}
			}
			if (s < 8192) != (f == a) {
				t.Errorf("part sent to the wrong node: %v", cmd)
			}
		}
		f.mu.Unlock()
	}
	// a split command keeps its place on each node: the SET before it is sent first
	if cmds := a.commands(); !strings.HasPrefix(cmds[0], "SET "+ka) || !strings.HasPrefix(cmds[1], "MGET") {
		t.Fatalf("order on node A: %v", cmds)
	}
}

func TestFakePipelineSplitPartErrorStaysAtItsIndex(t *testing.T) {
	var b *fakeNode
	moved := keyFor(true, 7) // its part is answered with MOVED to B, then served there
	a, b := twoNodes(t, func(f *fakeNode, args []string) string {
		if strings.ToUpper(args[0]) == "MGET" && args[1] == moved {
			return fmt.Sprintf("-MOVED %d %s\r\n", KeySlot(moved), b.addr)
		}
		return defaultFake(f, args)
	}, func(f *fakeNode, args []string) string {
		if strings.ToUpper(args[0]) == "DEL" {
			return "-ERR boom\r\n"
		}
		return defaultFake(f, args)
	})
	c := newClusterClient(t, a.addr)
	ctx := context.Background()
	kl, kh := keyFor(true, 0), keyFor(false, 0)
	vals, err := c.Pipelined(ctx, func(p *Pipeline) {
		p.Get(kl)
		p.Del(kl, kh)
		p.Do("MGET", moved, kh)
		p.Get(kh)
	})
	var se *Error
	if !errors.As(err, &se) || !strings.Contains(se.Msg, "boom") {
		t.Fatalf("want the DEL part error, got %v", err)
	}
	if vals[1].Kind != KindError || !strings.Contains(vals[1].Str(), "boom") {
		t.Fatalf("DEL reply %v", vals[1])
	}
	if vals[0].Kind == KindError || vals[3].Kind == KindError {
		t.Fatalf("neighbours disturbed: %v %v", vals[0], vals[3])
	}
	if vals[2].Kind != KindArray || len(vals[2].Array) != 2 || vals[2].Array[0].Str() != "v:"+moved {
		t.Fatalf("moved MGET part not repaired: %v", vals[2])
	}
	if b.count("MGET "+moved) != 1 {
		t.Fatalf("b=%v", b.commands())
	}
}

func TestFakePipelineCrossSlotNonSplitFails(t *testing.T) {
	a, b := twoNodes(t, nil, nil)
	c := newClusterClient(t, a.addr)
	ctx := context.Background()
	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	_, err := c.Pipelined(ctx, func(p *Pipeline) {
		p.Get(keyFor(true, 0))
		p.Do("SUNION", keyFor(true, 0), keyFor(false, 0))
	})
	if !errors.Is(err, ErrCrossSlot) || !strings.Contains(err.Error(), "SUNION") {
		t.Fatalf("want ErrCrossSlot (SUNION), got %v", err)
	}
	// a split command inside MULTI is not split: the transaction must stay in one slot
	_, err = c.Pipelined(ctx, func(p *Pipeline) {
		p.Do("MULTI")
		p.Do("MGET", "{t}a", keyFor(false, 0))
		p.Do("EXEC")
	})
	if !errors.Is(err, ErrCrossSlot) {
		t.Fatalf("want ErrCrossSlot, got %v", err)
	}
	if len(a.commands())+len(b.commands()) != 1 { // only the PING
		t.Fatalf("a=%v b=%v", a.commands(), b.commands())
	}
}

func toAny(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

func TestFakePipelineCrossSlotTransactionFails(t *testing.T) {
	a, b := twoNodes(t, nil, nil)
	c := newClusterClient(t, a.addr)
	ctx := context.Background()
	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	_, err := c.Pipelined(ctx, func(p *Pipeline) {
		p.Do("MULTI")
		p.Set(keyFor(true, 0), "1")
		p.Set(keyFor(false, 0), "2")
		p.Do("EXEC")
	})
	if !errors.Is(err, ErrCrossSlot) {
		t.Fatalf("want ErrCrossSlot, got %v", err)
	}
	if len(a.commands())+len(b.commands()) != 1 {
		t.Fatalf("a=%v b=%v", a.commands(), b.commands())
	}
}

func TestFakeFanOutAndScan(t *testing.T) {
	a, b := twoNodes(t, func(f *fakeNode, args []string) string {
		switch args[0] {
		case "DBSIZE":
			return ":3\r\n"
		case "KEYS":
			return "*1\r\n" + bulk("ka")
		case "SCAN":
			if args[1] == "0" {
				return "*2\r\n" + bulk("7") + "*1\r\n" + bulk("a1")
			}
			return "*2\r\n" + bulk("0") + "*1\r\n" + bulk("a2")
		}
		return defaultFake(f, args)
	}, func(f *fakeNode, args []string) string {
		switch args[0] {
		case "DBSIZE":
			return ":4\r\n"
		case "KEYS":
			return "*1\r\n" + bulk("kb")
		case "SCAN":
			return "*2\r\n" + bulk("0") + "*1\r\n" + bulk("b1")
		}
		return defaultFake(f, args)
	})
	_ = b
	c := newClusterClient(t, a.addr)
	ctx := context.Background()
	if n, err := c.DBSize(ctx); err != nil || n != 7 {
		t.Fatalf("DBSIZE %d %v", n, err)
	}
	if ks, err := c.Keys(ctx, "*"); err != nil || len(ks) != 2 {
		t.Fatalf("KEYS %v %v", ks, err)
	}
	var all []string
	cur := "0"
	for i := 0; i < 10; i++ {
		next, keys, err := c.Scan(ctx, cur, ScanOptions{})
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, keys...)
		if next == "0" {
			break
		}
		cur = next
	}
	if !reflect.DeepEqual(all, []string{"a1", "a2", "b1"}) {
		t.Fatalf("scan walked %v", all)
	}
}

func TestFakeNodeDownAtStartupRecovers(t *testing.T) {
	a, b := twoNodes(t, nil, nil)
	c := newClusterClient(t, a.addr)
	ctx := context.Background()
	kb := keyFor(false, 0)
	b.ln.Close() // B refuses new connections
	if _, err := getS(c, ctx, kb); err == nil {
		t.Fatal("expected a dial error")
	}
	if _, err := getS(c, ctx, keyFor(true, 0)); err != nil {
		t.Fatalf("A must keep working: %v", err)
	}
}

func TestMayBlockFindsBlockingCommandsInAPipeline(t *testing.T) {
	frames := func(cmds ...[]string) ([]byte, []int) {
		var buf []byte
		offs := []int{0}
		for _, c := range cmds {
			args := make([]any, len(c))
			for i, a := range c {
				args[i] = a
			}
			var err error
			if buf, err = AppendCommand(buf, args...); err != nil {
				t.Fatal(err)
			}
			offs = append(offs, len(buf))
		}
		return buf, offs
	}
	cases := []struct {
		cmds [][]string
		want bool
	}{
		{[][]string{{"GET", "a"}, {"SET", "b", "1"}}, false},
		{[][]string{{"GET", "a"}, {"blpop", "q", "0"}}, true},
		{[][]string{{"XREAD", "BLOCK", "0", "STREAMS", "s", "$"}}, true},
		{[][]string{{"BITCOUNT", "a"}, {"WATCH", "a"}}, false},
		{[][]string{{"wait", "1", "0"}}, true},
	}
	for _, c := range cases {
		buf, offs := frames(c.cmds...)
		if got := mayBlock(buf, offs, len(c.cmds)); got != c.want {
			t.Errorf("mayBlock(%v) = %v, want %v", c.cmds, got, c.want)
		}
	}
}

// askReply is an -ASK reply for the slot of args[1] naming node to.
func askReply(args []string, to *fakeNode) string {
	return fmt.Sprintf("-ASK %d %s\r\n", KeySlot(args[1]), to.addr)
}

func TestFakeAskIsFollowedWithoutPatchingTheTable(t *testing.T) {
	k := keyFor(true, 0)
	var b *fakeNode
	var asked atomic.Bool
	a, b := twoNodes(t, func(f *fakeNode, args []string) string {
		if strings.ToUpper(args[0]) == "GET" && asked.CompareAndSwap(false, true) {
			return askReply(args, b)
		}
		return defaultFake(f, args)
	}, nil)
	c := newClusterClient(t, a.addr)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	v, err := getS(c, ctx, k)
	if err != nil || v != "v:"+k+"@"+b.addr {
		t.Fatalf("%q %v", v, err)
	}
	if got := b.commands(); len(got) != 2 || got[0] != "ASKING" || got[1] != "GET "+k {
		t.Fatalf("node B got %v, want ASKING then the GET", got)
	}
	// the table was not touched: the next GET for the slot goes to A again
	v, err = getS(c, ctx, k)
	if err != nil || v != "v:"+k+"@"+a.addr {
		t.Fatalf("%q %v", v, err)
	}
	if a.count("GET") != 2 || b.count("GET") != 1 {
		t.Fatalf("a=%v b=%v", a.commands(), b.commands())
	}
}

func TestFakeAskToUnknownNodeIsFollowed(t *testing.T) {
	var c3 *fakeNode
	a, _ := twoNodes(t, func(f *fakeNode, args []string) string {
		if strings.ToUpper(args[0]) == "GET" {
			return askReply(args, c3)
		}
		return defaultFake(f, args)
	}, nil)
	c3 = newFakeNode(t, nil)
	c3.handler = defaultFake
	k := keyFor(true, 0)
	c := newClusterClient(t, a.addr)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	v, err := getS(c, ctx, k)
	if err != nil || v != "v:"+k+"@"+c3.addr {
		t.Fatalf("%q %v", v, err)
	}
	if got := c3.commands(); len(got) != 2 || got[0] != "ASKING" {
		t.Fatalf("node C got %v", got)
	}
}

func TestFakeTryAgainThenSuccess(t *testing.T) {
	k := keyFor(true, 0)
	var sent atomic.Int32
	a, _ := twoNodes(t, func(f *fakeNode, args []string) string {
		if strings.ToUpper(args[0]) == "GET" && sent.Add(1) == 1 {
			return "-TRYAGAIN slot is being handed over\r\n"
		}
		return defaultFake(f, args)
	}, nil)
	c := newClusterClient(t, a.addr)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	v, err := getS(c, ctx, k)
	if err != nil || v != "v:"+k+"@"+a.addr {
		t.Fatalf("%q %v", v, err)
	}
	if d := time.Since(start); d < 5*time.Millisecond {
		t.Fatalf("retried after %v, want a wait of at least 5ms", d)
	}
	if a.count("GET") != 2 {
		t.Fatal(a.commands())
	}
}

func TestFakeTryAgainGivesUpAfterSevenRetries(t *testing.T) {
	k := keyFor(true, 0)
	a, _ := twoNodes(t, func(f *fakeNode, args []string) string {
		if strings.ToUpper(args[0]) == "GET" {
			return "-TRYAGAIN slot is being handed over\r\n"
		}
		return defaultFake(f, args)
	}, nil)
	c := newClusterClient(t, a.addr)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	_, err := getS(c, ctx, k)
	var se *Error
	if !errors.As(err, &se) || se.Prefix() != "TRYAGAIN" {
		t.Fatalf("want the TRYAGAIN error, got %v", err)
	}
	if n := a.count("GET"); n != 8 {
		t.Fatalf("GET sent %d times, want 8", n)
	}
	// Waits of 5, 10, 20, 40, 80, 160 and 320 ms came before the seven retries.
	if elapsed := time.Since(start); elapsed < 635*time.Millisecond {
		t.Fatalf("gave up after %v, want at least 635ms", elapsed)
	}
}

func TestFakeTryAgainWaitEndsWithContext(t *testing.T) {
	k := keyFor(true, 0)
	a, _ := twoNodes(t, func(f *fakeNode, args []string) string {
		if strings.ToUpper(args[0]) == "GET" {
			return "-TRYAGAIN slot is being handed over\r\n"
		}
		return defaultFake(f, args)
	}, nil)
	c := newClusterClient(t, a.addr)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := getS(c, ctx, k); err == nil {
		t.Fatal("want an error from the cancelled context")
	}
}

func TestFakeAskAndMovedShareTheRedirectBudget(t *testing.T) {
	// ASK, MOVED, ASK, ASK: the fourth redirect is returned to the caller.
	k := keyFor(true, 0)
	var a, b *fakeNode
	var gets atomic.Int32
	reply := func(f *fakeNode, args []string) string {
		if strings.ToUpper(args[0]) != "GET" {
			return defaultFake(f, args)
		}
		switch gets.Add(1) {
		case 1, 3, 4:
			return askReply(args, b)
		default:
			return fmt.Sprintf("-MOVED %d %s\r\n", KeySlot(args[1]), a.addr)
		}
	}
	a, b = twoNodes(t, reply, reply)
	c := newClusterClient(t, a.addr)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := getS(c, ctx, k)
	var se *Error
	if !errors.As(err, &se) || se.Prefix() != "ASK" {
		t.Fatalf("want the fourth redirect (ASK) returned, got %v", err)
	}
	if n := int(gets.Load()); n != 4 {
		t.Fatalf("GET sent %d times, want 4", n)
	}
}
