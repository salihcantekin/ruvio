// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ErrCrossSlot is returned, without sending anything, for a multi-key command, pipeline
// or transaction whose keys do not all hash to one slot and that the client cannot
// split. MGET, MSET, DEL, UNLINK, EXISTS and TOUCH are split per node automatically
// (their replies are merged and the split is not atomic). Use hash tags such as
// {user:1}:a and {user:1}:b to put related keys on one slot.
var ErrCrossSlot = errors.New("ruvio: CROSSSLOT keys in request don't hash to the same slot")

const (
	noOwner   = 0xFFFF
	slotCross = -2 // routeFrame: the keys of one command span several slots
)

// topology is an immutable slot table; updates copy it.
type topology struct {
	owner [slotCount]uint16 // index into nodes, noOwner when the slot is unassigned
	nodes []*Client
	addrs []string

	// replicas lists, per node, the replica addresses CLUSTER SLOTS gave after it. read
	// holds, per node, the replica its reads go to (nil when it has none); empty with
	// ReadPrimary. The routes are shared by the copies a MOVED makes, so a replica marked
	// unusable stays so until the next load.
	replicas [][]string
	read     []*replicaRoute
}

// replicaRoute is the replica chosen for one node's reads.
type replicaRoute struct {
	node *Client
	bad  atomic.Bool // could not be connected, or answered LOADING: skipped until the next load
}

func (t *topology) nodeAt(slot int, seed *Client) *Client {
	if i := t.owner[slot]; i != noOwner {
		return t.nodes[i]
	}
	return seed
}

func (t *topology) indexOf(addr string) int {
	for i, a := range t.addrs {
		if a == addr {
			return i
		}
	}
	return -1
}

// cluster routes commands over one multiplexed Client per node. The seed Client (the one
// the caller created) doubles as the node it points at; the other nodes are internal
// Clients with cluster mode off, so every per-node fast path is the standalone one.
type cluster struct {
	seed       *Client
	t          atomic.Pointer[topology]
	standalone atomic.Bool // the seed answered "cluster mode is disabled"
	policy     ReadPolicy  // Options.ReadPolicy

	mu    sync.Mutex // guards nodes, refresh and slot patches
	nodes map[string]*Client

	// seeds are the configured Options.Seeds addresses, asked for the slot map when no
	// known node answers. Empty without Options.Seeds.
	seeds []string
}

func newCluster(seed *Client) *cluster {
	cl := &cluster{seed: seed, policy: seed.opts.ReadPolicy, nodes: map[string]*Client{seed.opts.Addr: seed}}
	if len(seed.addrs) > 1 {
		cl.seeds = seed.addrs
	}
	return cl
}

// node returns the Client for addr, creating it (no connection yet). Caller holds cl.mu.
func (cl *cluster) node(addr string) *Client {
	if n := cl.nodes[addr]; n != nil {
		return n
	}
	o := cl.seed.opts
	o.Addr = addr
	o.Seeds = nil
	o.Cluster = false
	o.SentinelMaster = ""
	o.DisableFollowPrimary = true
	n := NewClient(o)
	cl.nodes[addr] = n
	return n
}

// replicaNode returns the Client for the replica at addr, creating it (no connection yet);
// it sends READONLY on every connection it opens. Caller holds cl.mu.
func (cl *cluster) replicaNode(addr string) *Client {
	if n := cl.nodes[addr]; n != nil {
		return n
	}
	n := cl.node(addr)
	n.readOnly = true
	return n
}

func (cl *cluster) close() {
	cl.mu.Lock()
	nodes := make([]*Client, 0, len(cl.nodes))
	for _, n := range cl.nodes {
		if n != cl.seed {
			nodes = append(nodes, n)
		}
	}
	cl.mu.Unlock()
	for _, n := range nodes {
		n.Close()
	}
}

// topo returns the slot table, discovering it on first use. A nil table means the seed
// is a standalone server: callers fall back to the plain single-connection path.
func (cl *cluster) topo(ctx context.Context) (*topology, error) {
	if t := cl.t.Load(); t != nil {
		return t, nil
	}
	if cl.standalone.Load() {
		return nil, nil
	}
	return cl.refresh(ctx, true)
}

func isClusterDisabled(err error) bool {
	var e *Error
	return errors.As(err, &e) && (strings.Contains(e.Msg, "cluster mode is disabled") ||
		strings.Contains(e.Msg, "unknown command") || strings.Contains(e.Msg, "unknown subcommand"))
}

// open reports whether the node has a connection that has not failed.
func (c *Client) open() bool {
	cn := c.cur.Load()
	return cn != nil && !cn.dead.Load()
}

// refresh reloads the slot table with CLUSTER SLOTS. first is the discovery call: a
// "cluster mode is disabled" reply switches the client to standalone for good.
//
// Nodes with an open socket are asked first. When none of them answers (the usual case after
// a node went away), every other known node and then every seed is opened in turn, one
// connect attempt each, until one returns a slot map. Seeds that were only added for this
// call and are not in the new table are dropped again.
func (cl *cluster) refresh(ctx context.Context, first bool) (*topology, error) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	if first {
		if t := cl.t.Load(); t != nil {
			return t, nil
		}
		if cl.standalone.Load() {
			return nil, nil
		}
	}
	known := []*Client{cl.seed}
	if t := cl.t.Load(); t != nil {
		for _, n := range t.nodes {
			if n != cl.seed {
				known = append(known, n)
			}
		}
	}
	var lastErr error
	// ask returns done=true when the call is finished: t is the new table (nil when the
	// server turned out to be standalone) or err the final error.
	ask := func(n *Client) (t *topology, done bool, err error) {
		v, err := n.execLocal(ctx, newCmd(2, cCLUSTER).t(tSLOTS))
		if err != nil {
			if first && isClusterDisabled(err) {
				cl.standalone.Store(true)
				return nil, true, nil
			}
			lastErr = err
			if ctx.Err() != nil {
				return nil, true, err
			}
			return nil, false, nil
		}
		t, err = cl.build(v, n.opts.Addr)
		if err != nil {
			return nil, true, err
		}
		cl.t.Store(t)
		return t, true, nil
	}
	for _, n := range known {
		if n.open() {
			if t, done, err := ask(n); done {
				return t, err
			}
		}
	}
	for _, n := range known {
		if !n.open() {
			if t, done, err := ask(n); done {
				return t, err
			}
		}
	}
	var added []string
	defer func() {
		for _, addr := range added {
			if t := cl.t.Load(); t != nil && t.indexOf(addr) >= 0 {
				continue
			}
			if n := cl.nodes[addr]; n != nil {
				delete(cl.nodes, addr)
				n.Close()
			}
		}
	}()
	for _, addr := range cl.seeds {
		if cl.nodes[addr] != nil {
			continue
		}
		added = append(added, addr)
		if t, done, err := ask(cl.node(addr)); done {
			return t, err
		}
	}
	return nil, lastErr
}

// build turns a CLUSTER SLOTS reply from fromAddr into a topology; a node address the reply
// leaves empty means fromAddr's host. Caller holds cl.mu.
func (cl *cluster) build(v Value, fromAddr string) (*topology, error) {
	if v.Kind != KindArray || len(v.Array) == 0 {
		return nil, fmt.Errorf("ruvio: invalid CLUSTER SLOTS reply: %s", v.String())
	}
	ranges := append([]Value(nil), v.Array...)
	sort.SliceStable(ranges, func(i, j int) bool {
		a, _ := ranges[i].Array[0].AsInt64()
		b, _ := ranges[j].Array[0].AsInt64()
		return a < b
	})
	seedHost, _, _ := net.SplitHostPort(fromAddr)
	t := &topology{}
	for i := range t.owner {
		t.owner[i] = noOwner
	}
	for _, rg := range ranges {
		if rg.Kind != KindArray || len(rg.Array) < 3 || rg.Array[2].Kind != KindArray || len(rg.Array[2].Array) < 2 {
			return nil, fmt.Errorf("ruvio: invalid CLUSTER SLOTS range: %s", rg.String())
		}
		lo, e1 := rg.Array[0].AsInt64()
		hi, e2 := rg.Array[1].AsInt64()
		port, e3 := rg.Array[2].Array[1].AsInt64()
		if e1 != nil || e2 != nil || e3 != nil || lo < 0 || hi < lo || hi >= slotCount {
			return nil, fmt.Errorf("ruvio: invalid CLUSTER SLOTS range: %s", rg.String())
		}
		host := rg.Array[2].Array[0].Str()
		if host == "" {
			host = seedHost
		}
		addr := net.JoinHostPort(host, strconv.FormatInt(port, 10))
		idx := t.indexOf(addr)
		if idx < 0 {
			idx = len(t.addrs)
			t.addrs = append(t.addrs, addr)
			t.nodes = append(t.nodes, cl.node(addr))
			t.replicas = append(t.replicas, nil)
		}
		// Replicas follow the primary; one that cannot be read is skipped, not fatal.
		for _, rep := range rg.Array[3:] {
			if rep.Kind != KindArray || len(rep.Array) < 2 {
				continue
			}
			rport, err := rep.Array[1].AsInt64()
			if err != nil || rport < 1 || rport > 65535 {
				continue
			}
			rhost := rep.Array[0].Str()
			if rhost == "" {
				rhost = seedHost
			}
			raddr := net.JoinHostPort(rhost, strconv.FormatInt(rport, 10))
			if !slices.Contains(t.replicas[idx], raddr) {
				t.replicas[idx] = append(t.replicas[idx], raddr)
			}
		}
		for s := lo; s <= hi; s++ {
			t.owner[s] = uint16(idx)
		}
	}
	if cl.policy != ReadPrimary {
		// Each node's reads go to one of its replicas, picked at random per load so many
		// clients spread over the replicas, and kept until the next load.
		pick := rand.Uint64()
		t.read = make([]*replicaRoute, len(t.nodes))
		for i, reps := range t.replicas {
			if len(reps) > 0 {
				t.read[i] = &replicaRoute{node: cl.replicaNode(reps[pick%uint64(len(reps))])}
			}
		}
	}
	return t, nil
}

// parseMoved decodes "MOVED <slot> host:port".
func parseMoved(msg []byte) (slot int, addr string, ok bool) {
	if len(msg) < 8 || string(msg[:6]) != "MOVED " {
		return 0, "", false
	}
	return parseSlotAddr(msg[6:])
}

// parseAsk decodes "ASK <slot> host:port".
func parseAsk(msg []byte) (slot int, addr string, ok bool) {
	if len(msg) < 6 || string(msg[:4]) != "ASK " {
		return 0, "", false
	}
	return parseSlotAddr(msg[4:])
}

// isTryAgain reports whether an error reply is -TRYAGAIN.
func isTryAgain(msg []byte) bool {
	return len(msg) >= 8 && string(msg[:8]) == "TRYAGAIN"
}

// isRedirectErr reports whether an error reply is one the cluster client follows: -MOVED,
// -ASK or -TRYAGAIN.
func isRedirectErr(msg []byte) bool {
	if _, _, ok := parseMoved(msg); ok {
		return true
	}
	if _, _, ok := parseAsk(msg); ok {
		return true
	}
	return isTryAgain(msg)
}

// parseSlotAddr decodes "<slot> host:port", the part after the prefix of -MOVED and -ASK.
func parseSlotAddr(rest []byte) (slot int, addr string, ok bool) {
	sp := -1
	for i, c := range rest {
		if c == ' ' {
			sp = i
			break
		}
	}
	if sp <= 0 {
		return 0, "", false
	}
	n, ok := atoiBytes(rest[:sp])
	if !ok || n >= slotCount || sp+1 >= len(rest) {
		return 0, "", false
	}
	return n, string(rest[sp+1:]), true
}

// maxRedirects is how many -MOVED and -ASK replies one command follows before the last
// one is returned to the caller.
const maxRedirects = 3

// maxTryAgain is how many times one command is sent again after -TRYAGAIN; the wait before
// retry n is tryAgainWait << (n-1).
const (
	maxTryAgain  = 3
	tryAgainWait = 5 * time.Millisecond
)

// applyMoved updates the table after -MOVED and returns the node to retry on: the one the
// MOVED named. The slot is patched at once; the whole table is reloaded only when the target
// is unknown or when reload is set (the second or later MOVED of one command).
func (cl *cluster) applyMoved(ctx context.Context, slot int, addr string, reload bool) (*Client, error) {
	cl.mu.Lock()
	t := cl.t.Load()
	n := cl.node(addr)
	idx := -1
	if t != nil {
		idx = t.indexOf(addr)
	}
	if idx >= 0 {
		nt := *t
		nt.owner[slot] = uint16(idx)
		cl.t.Store(&nt)
	}
	cl.mu.Unlock()
	if idx < 0 || reload {
		if _, err := cl.refresh(ctx, false); err != nil && idx < 0 {
			return nil, err
		}
	}
	return n, nil
}

// rt sends r on c and waits. ok=false: r was handed back to the reader (or recycled); the
// caller must not touch it. ok=true: r is the caller's, its reply is in r.val / r.err.
//
// With keep set, a connection that could not be opened is reported as ok=true with the error
// in r.err and r untouched, so the caller can send it elsewhere.
func (c *Client) rt(ctx context.Context, r *request, blocking, keep bool) (bool, error) {
	if !blocking {
		cn, err := c.shared(ctx)
		if err != nil {
			return openFailed(r, err, keep)
		}
		return cn.roundTrip(ctx, r)
	}
	cn, err := c.acquire(ctx)
	if err != nil {
		return openFailed(r, err, keep)
	}
	ok, err := cn.roundTrip(ctx, r)
	if !ok || r.err != nil {
		cn.close() // a reply may still be in flight: never reuse this connection
	} else {
		c.release(cn)
	}
	return ok, err
}

func openFailed(r *request, err error, keep bool) (bool, error) {
	if keep && isConnectError(err) {
		r.err = err
		return true, nil
	}
	putReq(r)
	return false, err
}

// stats sums the counters of every node connection.
func (cl *cluster) stats() Stats {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	var s Stats
	for _, n := range cl.nodes {
		if cn := n.cur.Load(); cn != nil {
			s.Writes += cn.writes.Load()
			s.BytesWritten += cn.bytesWritten.Load()
		}
	}
	return s
}

// Nodes returns the addresses of the cluster nodes in slot order. It returns nil for a
// standalone server or before the first command has discovered the topology.
func (c *Client) Nodes() []string {
	if c.cl == nil {
		return nil
	}
	if t := c.cl.t.Load(); t != nil {
		return append([]string(nil), t.addrs...)
	}
	return nil
}

// IsCluster reports whether the client discovered a cluster. It is false for a
// standalone server (including a single-shard server) and before the first command.
func (c *Client) IsCluster() bool { return c.cl != nil && c.cl.t.Load() != nil }

// RefreshSlots reloads the slot table now with CLUSTER SLOTS. The client also refreshes
// it by itself after a MOVED reply. It does nothing and returns nil when Options.Cluster
// is not set.
func (c *Client) RefreshSlots(ctx context.Context) error {
	if c.cl == nil {
		return nil
	}
	_, err := c.cl.refresh(ctx, c.cl.t.Load() == nil)
	return err
}
