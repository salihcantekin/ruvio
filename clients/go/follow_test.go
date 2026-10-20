// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// Scripted nodes that answer ROLE, refuse writes with READONLY when they are replicas, and
// (for the coordinator case) name a primary for SENTINEL get-master-addr-by-name. The live
// counterpart is follow_live_test.go.

type nodeMode int

const (
	modeMaster  nodeMode = iota
	modeReplica          // reports roleHint as its primary
	modeLiar             // says master but refuses writes anyway
	modeNames            // a coordinator that names roleHint
)

type roleNode struct {
	t     *testing.T
	ln    net.Listener
	addr  string
	port  int
	mu    sync.Mutex
	mode  nodeMode
	hint  int
	got   [][]string
	conns []net.Conn
}

func newRoleNode(t *testing.T, mode nodeMode) *roleNode {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	n := &roleNode{t: t, ln: ln, addr: ln.Addr().String(), port: ln.Addr().(*net.TCPAddr).Port, mode: mode}
	t.Cleanup(func() {
		ln.Close()
		n.dropConns()
	})
	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			n.mu.Lock()
			n.conns = append(n.conns, nc)
			n.mu.Unlock()
			go n.serve(nc)
		}
	}()
	return n
}

func (n *roleNode) become(mode nodeMode, hint int) {
	n.mu.Lock()
	n.mode, n.hint = mode, hint
	n.mu.Unlock()
}

func (n *roleNode) dropConns() {
	n.mu.Lock()
	conns := n.conns
	n.conns = nil
	n.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
}

func (n *roleNode) serve(nc net.Conn) {
	defer nc.Close()
	br := bufio.NewReader(nc)
	for {
		args, err := readFakeCmd(br)
		if err != nil {
			return
		}
		n.mu.Lock()
		n.got = append(n.got, args)
		mode, hint := n.mode, n.hint
		n.mu.Unlock()
		if _, err := io.WriteString(nc, roleAnswer(args, mode, hint)); err != nil {
			return
		}
	}
}

func roleAnswer(args []string, mode nodeMode, hint int) string {
	name := strings.ToUpper(args[0])
	writes := name == "SET" || name == "INCR" || name == "DEL"
	switch {
	case name == "ROLE" && (mode == modeMaster || mode == modeLiar):
		return "*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n"
	case name == "ROLE" && mode == modeReplica:
		return fmt.Sprintf("*5\r\n$5\r\nslave\r\n$9\r\n127.0.0.1\r\n:%d\r\n$9\r\nconnected\r\n:0\r\n", hint)
	case name == "SENTINEL" && mode == modeNames:
		p := fmt.Sprint(hint)
		return fmt.Sprintf("*2\r\n$9\r\n127.0.0.1\r\n$%d\r\n%s\r\n", len(p), p)
	case writes && (mode == modeReplica || mode == modeLiar):
		return "-READONLY You can't write against a read only replica.\r\n"
	case name == "PING":
		return "+PONG\r\n"
	case name == "GET":
		return "$-1\r\n"
	case name == "INCR":
		return ":1\r\n"
	}
	return "+OK\r\n"
}

func (n *roleNode) seen(name string) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	c := 0
	for _, a := range n.got {
		if strings.EqualFold(a[0], name) {
			c++
		}
	}
	return c
}

func (n *roleNode) total() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.got)
}

func (n *roleNode) commands() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]string, len(n.got))
	for i, a := range n.got {
		out[i] = strings.Join(a, " ")
	}
	return out
}

func followOptions(nodes ...*roleNode) Options {
	seeds := make([]string, len(nodes))
	for i, n := range nodes {
		seeds[i] = n.addr
	}
	return Options{Seeds: seeds, DialTimeout: time.Second, CommandTimeout: 5 * time.Second}
}

func followClient(t *testing.T, o Options) (*Client, context.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	c := NewClient(o)
	c.followDelay, c.followMaxDelay = 10*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { c.Close() })
	return c, ctx
}

func wantReadonly(t *testing.T, err error) {
	t.Helper()
	if !isReadonly(err) {
		t.Fatalf("want READONLY, got %v", err)
	}
}

func counts(name string, nodes ...*roleNode) []int {
	out := make([]int, len(nodes))
	for i, n := range nodes {
		out[i] = n.seen(name)
	}
	return out
}

func wantCounts(t *testing.T, name string, want []int, nodes ...*roleNode) {
	t.Helper()
	if got := counts(name, nodes...); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("%s per node: got %v want %v", name, got, want)
	}
}

func TestReadonlyFollowsTheNewPrimaryAndSendsTheCommandOnce(t *testing.T) {
	old := newRoleNode(t, modeMaster)
	next := newRoleNode(t, modeReplica)
	c, ctx := followClient(t, followOptions(old, next))

	if err := c.Set(ctx, "k", "1"); err != nil {
		t.Fatal(err)
	}
	wantCounts(t, "SET", []int{1, 0}, old, next)

	old.become(modeReplica, next.port)
	next.become(modeMaster, 0)

	if err := c.Set(ctx, "k", "2"); err != nil {
		t.Fatal(err)
	}
	// Refused once by the old node, then sent once to the new primary.
	wantCounts(t, "SET", []int{2, 1}, old, next)

	if err := c.Set(ctx, "k", "3"); err != nil {
		t.Fatal(err)
	}
	wantCounts(t, "SET", []int{2, 2}, old, next)
}

func TestAHealthyConnectionSendsNothingExtra(t *testing.T) {
	a := newRoleNode(t, modeMaster)
	b := newRoleNode(t, modeReplica)
	c, ctx := followClient(t, followOptions(a, b))

	for i := 0; i < 50; i++ {
		if err := c.Set(ctx, "k", "v"); err != nil {
			t.Fatal(err)
		}
	}
	if a.total() != 50 || b.total() != 0 {
		t.Fatalf("commands: %v and %v", a.commands(), b.commands())
	}
}

func TestAReplicaNamesItsPrimaryWhenThatIsNoSeed(t *testing.T) {
	replica := newRoleNode(t, modeMaster)
	hidden := newRoleNode(t, modeMaster)
	c, ctx := followClient(t, Options{Seeds: []string{replica.addr, closedAddr(t)}, DialTimeout: time.Second})

	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	replica.become(modeReplica, hidden.port)

	if err := c.Set(ctx, "k", "v"); err != nil {
		t.Fatal(err)
	}
	wantCounts(t, "SET", []int{1, 1}, replica, hidden)
}

func TestFollowingOffReturnsReadonlyAndSendsNoRole(t *testing.T) {
	a := newRoleNode(t, modeMaster)
	b := newRoleNode(t, modeMaster)
	o := followOptions(a, b)
	o.DisableFollowPrimary = true
	c, ctx := followClient(t, o)

	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	a.become(modeReplica, b.port)
	wantReadonly(t, c.Set(ctx, "k", "v"))
	if a.seen("ROLE") != 0 || b.total() != 0 {
		t.Fatalf("a %v b %v", a.commands(), b.commands())
	}
}

func TestOneSeedAndNoCoordinatorNeverFollows(t *testing.T) {
	a := newRoleNode(t, modeReplica)
	c, ctx := followClient(t, Options{Addr: a.addr, DialTimeout: time.Second})

	wantReadonly(t, c.Set(ctx, "k", "v"))
	if a.seen("ROLE") != 0 {
		t.Fatalf("a lone address must not probe: %v", a.commands())
	}
}

func TestAReconnectTakesTheNodeThatSaysMasterNotTheNextSeed(t *testing.T) {
	first := newRoleNode(t, modeMaster)
	second := newRoleNode(t, modeMaster)
	third := newRoleNode(t, modeMaster)
	c, ctx := followClient(t, followOptions(first, second, third))

	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	// The second seed became a replica while the connection was down.
	second.become(modeReplica, third.port)
	first.become(modeReplica, third.port)
	first.dropConns()

	if err := c.Ping(ctx); err == nil {
		t.Fatal("the command that finds the drop must fail")
	}
	if err := c.Set(ctx, "k", "v"); err != nil {
		t.Fatal(err)
	}
	wantCounts(t, "SET", []int{0, 0, 1}, first, second, third)
}

func TestAReconnectWithoutAnyPrimaryUsesTheFirstSeedThatAnswers(t *testing.T) {
	first := newRoleNode(t, modeMaster)
	second := newRoleNode(t, modeMaster)
	c, ctx := followClient(t, followOptions(first, second))

	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	first.become(modeReplica, 0)
	second.become(modeReplica, 0)
	first.dropConns()

	if err := c.Ping(ctx); err == nil {
		t.Fatal("the command that finds the drop must fail")
	}
	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestWithoutAnyPrimaryTheReadonlyErrorComesBackWithinTheBackoff(t *testing.T) {
	first := newRoleNode(t, modeMaster)
	second := newRoleNode(t, modeMaster)
	c, ctx := followClient(t, followOptions(first, second))
	c.followRounds = 3

	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	first.become(modeReplica, 0)
	second.become(modeReplica, 0)

	started := time.Now()
	wantReadonly(t, c.Set(ctx, "k", "v"))
	if d := time.Since(started); d > 5*time.Second {
		t.Fatalf("took %v", d)
	}
	wantCounts(t, "SET", []int{1, 0}, first, second)
	if first.seen("ROLE") != 3 {
		t.Fatalf("want one round per attempt, got %d ROLE", first.seen("ROLE"))
	}
	// The connection stayed where it was.
	first.become(modeMaster, 0)
	if err := c.Set(ctx, "k", "v"); err != nil {
		t.Fatal(err)
	}
}

func TestAWriteRefusedTwiceIsNotSentAThirdTime(t *testing.T) {
	old := newRoleNode(t, modeMaster)
	liar := newRoleNode(t, modeMaster)
	c, ctx := followClient(t, followOptions(old, liar))

	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	old.become(modeReplica, 0)
	liar.become(modeLiar, 0)

	wantReadonly(t, c.Set(ctx, "k", "v"))
	wantCounts(t, "SET", []int{1, 1}, old, liar)
}

func TestAWriteRefusedInsideATransactionFollowsButIsNotSentAgain(t *testing.T) {
	old := newRoleNode(t, modeMaster)
	next := newRoleNode(t, modeReplica)
	c, ctx := followClient(t, followOptions(old, next))

	if _, err := c.Do(ctx, "MULTI"); err != nil {
		t.Fatal(err)
	}
	old.become(modeReplica, next.port)
	next.become(modeMaster, 0)

	_, err := c.Do(ctx, "SET", "k", "v")
	wantReadonly(t, err)
	wantCounts(t, "SET", []int{1, 0}, old, next)

	// The client moved: the next command is not inside the lost transaction, and is retried
	// like any other.
	if err := c.Set(ctx, "k", "v"); err != nil {
		t.Fatal(err)
	}
	wantCounts(t, "SET", []int{1, 1}, old, next)
}

func TestPipelineAndDedicatedConnectionsReturnReadonlyAsIs(t *testing.T) {
	old := newRoleNode(t, modeMaster)
	next := newRoleNode(t, modeMaster)
	c, ctx := followClient(t, followOptions(old, next))

	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	old.become(modeReplica, next.port)

	vals, err := c.Pipelined(ctx, func(p *Pipeline) { p.Set("k", "v") })
	if len(vals) != 1 || vals[0].Kind != KindError {
		t.Fatalf("want an error at index 0, got %v (%v)", vals, err)
	}
	d, err := c.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	wantReadonly(t, d.Set(ctx, "k", "v"))
	if next.total() != 0 || old.seen("ROLE") != 0 {
		t.Fatalf("nothing may follow: old %v next %v", old.commands(), next.commands())
	}
}

func TestProbesSendAuthAndTheQuestionOnly(t *testing.T) {
	old := newRoleNode(t, modeMaster)
	next := newRoleNode(t, modeReplica)
	o := followOptions(old, next)
	o.Password, o.DB = "pw", 3
	c, ctx := followClient(t, o)

	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	old.become(modeReplica, next.port)
	next.become(modeMaster, 0)
	if err := c.Set(ctx, "k", "v"); err != nil {
		t.Fatal(err)
	}
	// The probe, then the real connection with its own handshake.
	want := "AUTH pw|ROLE|AUTH pw|SELECT 3|SET k v"
	if got := strings.Join(next.commands(), "|"); got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestACoordinatorNameFindsThePrimaryAtConnectAndAfterAFailover(t *testing.T) {
	coord := newRoleNode(t, modeNames)
	one := newRoleNode(t, modeMaster)
	two := newRoleNode(t, modeReplica)
	coord.become(modeNames, one.port)

	o := followOptions(coord)
	o.SentinelMaster = "main"
	c, ctx := followClient(t, o)

	if err := c.Set(ctx, "k", "1"); err != nil {
		t.Fatal(err)
	}
	wantCounts(t, "SET", []int{1, 0}, one, two)
	if coord.seen("SENTINEL") != 1 {
		t.Fatalf("coordinator saw %v", coord.commands())
	}
	if got := coord.commands()[0]; got != "SENTINEL get-master-addr-by-name main" {
		t.Fatalf("got %q", got)
	}

	one.become(modeReplica, 0)
	two.become(modeMaster, 0)
	coord.become(modeNames, two.port)
	if err := c.Set(ctx, "k", "2"); err != nil {
		t.Fatal(err)
	}
	wantCounts(t, "SET", []int{2, 1}, one, two)

	// After a dropped socket the coordinator is asked again.
	one.become(modeMaster, 0)
	two.become(modeReplica, 0)
	coord.become(modeNames, one.port)
	two.dropConns()
	if err := c.Ping(ctx); err == nil {
		t.Fatal("the command that finds the drop must fail")
	}
	if err := c.Set(ctx, "k", "3"); err != nil {
		t.Fatal(err)
	}
	wantCounts(t, "SET", []int{3, 1}, one, two)
}

func TestACoordinatorThatNamesNoPrimaryFailsTheConnect(t *testing.T) {
	coord := newRoleNode(t, modeMaster)
	o := followOptions(coord)
	o.SentinelMaster = "main"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := Connect(ctx, o)
	if err == nil {
		c.Close()
		t.Fatal("Connect must fail")
	}
	if !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), `"main"`) {
		t.Fatalf("want a protocol error naming the master, got %v", err)
	}
}

func TestAHungSeedCostsOneBoundedWait(t *testing.T) {
	// Accepts and never answers.
	silent, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { silent.Close() })
	var held []net.Conn
	var heldMu sync.Mutex
	go func() {
		for {
			nc, err := silent.Accept()
			if err != nil {
				return
			}
			heldMu.Lock()
			held = append(held, nc)
			heldMu.Unlock()
		}
	}()
	t.Cleanup(func() {
		heldMu.Lock()
		defer heldMu.Unlock()
		for _, nc := range held {
			nc.Close()
		}
	})
	live := newRoleNode(t, modeMaster)
	other := newRoleNode(t, modeReplica)
	o := Options{Seeds: []string{live.addr, silent.Addr().String(), other.addr}, DialTimeout: 300 * time.Millisecond, CommandTimeout: 5 * time.Second}
	c, ctx := followClient(t, o)

	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	live.become(modeReplica, other.port)
	other.become(modeMaster, 0)

	started := time.Now()
	if err := c.Set(ctx, "k", "v"); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(started); d > 3*time.Second {
		t.Fatalf("took %v", d)
	}
}

func TestAProbeWithARefusedAuthFailsTheCommand(t *testing.T) {
	old := newRoleNode(t, modeMaster)
	denied := newFakeNode(t, func(f *fakeNode, args []string) string {
		if args[0] == "AUTH" {
			return "-WRONGPASS invalid username-password pair\r\n"
		}
		return "+OK\r\n"
	})
	o := Options{Seeds: []string{old.addr, denied.addr}, DialTimeout: time.Second}
	c, ctx := followClient(t, o)

	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	old.become(modeReplica, 0)
	c.opts.Password = "bad"

	err := c.Set(ctx, "k", "v")
	var se *Error
	if !errors.As(err, &se) || se.Prefix() != "WRONGPASS" {
		t.Fatalf("want WRONGPASS, got %v", err)
	}
}

func TestParseRoleAndReadonlyText(t *testing.T) {
	bulk := func(s string) Value { return Value{Kind: KindBulk, Bytes: []byte(s)} }
	arr := func(v ...Value) Value { return Value{Kind: KindArray, Array: v} }
	cases := []struct {
		name   string
		v      Value
		master bool
		hint   string
		ok     bool
	}{
		{"master", arr(bulk("master"), Value{Kind: KindInt}), true, "", true},
		{"replica with integer port", arr(bulk("slave"), bulk("h"), Value{Kind: KindInt, Int: 7}), false, "h:7", true},
		{"replica with string port", arr(bulk("slave"), bulk("h"), bulk("8")), false, "h:8", true},
		{"replica port 0 reports nothing", arr(bulk("slave"), bulk("h"), Value{Kind: KindInt}), false, "", true},
		{"replica without host reports nothing", arr(bulk("slave"), bulk(""), Value{Kind: KindInt, Int: 7}), false, "", true},
		{"other role", arr(bulk("sentinel")), false, "", false},
		{"not an array", bulk("master"), false, "", false},
	}
	for _, c := range cases {
		r, ok := parseRole(c.v)
		if ok != c.ok || r.master != c.master || r.hint != c.hint {
			t.Errorf("%s: got %+v %v", c.name, r, ok)
		}
	}
	for text, want := range map[string]bool{
		"READONLY": true, "READONLY You can't write": true, "READONLYX": false, "READONLY-ish": false, "ERR READONLY": false,
	} {
		if got := isReadonly(&Error{Msg: text}); got != want {
			t.Errorf("isReadonly(%q) = %v", text, got)
		}
	}
	if isReadonly(errors.New("READONLY")) {
		t.Error("only a server error counts")
	}
}
