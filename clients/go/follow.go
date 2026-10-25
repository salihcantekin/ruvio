// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// Following a failover (see Options.DisableFollowPrimary). Everything here runs only on a
// reconnect or after a READONLY reply; a healthy connection never reaches it.

const (
	followRounds   = 8
	followDelay    = 100 * time.Millisecond
	followMaxDelay = 2 * time.Second
)

// nodeList is the list of addresses the dial index points into. Empty in sentinel mode until
// a primary has been found.
func (c *Client) nodeList() []string {
	if p := c.nodes.Load(); p != nil {
		return *p
	}
	return nil
}

// nodeIndex returns the index of addr in the dial list, which gains it when it is new. In
// sentinel mode the list is just the primary.
func (c *Client) nodeIndex(addr string) int {
	c.nodesMu.Lock()
	defer c.nodesMu.Unlock()
	if c.sentinel {
		c.nodes.Store(&[]string{addr})
		return 0
	}
	cur := c.nodeList()
	for i, a := range cur {
		if a == addr {
			return i
		}
	}
	next := append(append(make([]string, 0, len(cur)+1), cur...), addr)
	c.nodes.Store(&next)
	return len(next) - 1
}

// following reports whether the failover rules apply now: a cluster client (a multi-node
// slot map) never runs them.
func (c *Client) following() bool {
	return c.follow && (c.cl == nil || c.cl.standalone.Load())
}

// dialShared opens the shared connection. After a drop, with following on, the node that
// answers ROLE with master comes first; when none is found or it cannot be opened, the
// usual seed order applies.
func (c *Client) dialShared(ctx context.Context, start int, reconnect bool) (*conn, error) {
	if reconnect && c.following() {
		if cn, err := c.dialFound(ctx, start); cn != nil || err != nil {
			return cn, err
		}
	}
	return c.dialAt(ctx, c.opts.OnPush, start)
}

// dialFound runs one round and opens a connection to the primary it finds. (nil, nil) means
// the caller falls back to the seed order.
func (c *Client) dialFound(ctx context.Context, start int) (*conn, error) {
	addr, ok, err := c.findPrimary(ctx, start, new(string))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	cn, err := c.dialAddr(ctx, c.opts.OnPush, c.nodeIndex(addr), addr)
	if err != nil && (!isConnectError(err) || ctx.Err() != nil) {
		return nil, err
	}
	return cn, nil
}

// dialPrimary is the first connect in sentinel mode: one round, no waiting.
func (c *Client) dialPrimary(ctx context.Context, onPush func(Value)) (*conn, error) {
	addr, ok, err := c.findPrimary(ctx, 0, new(string))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%w: no coordinator names a primary for master %q", ErrProtocol, c.opts.SentinelMaster)
	}
	return c.dialAddr(ctx, onPush, c.nodeIndex(addr), addr)
}

// findPrimary is one round: the address that answered master, or none. start is the first
// index of the node list to ask; *avoid is an address that must not be taken (the one that
// just answered READONLY) while it still says master: a replaced primary does. Once it answers
// ROLE as a replica *avoid is cleared, so a later round takes it when it is promoted. A refused
// AUTH is returned as the error.
func (c *Client) findPrimary(ctx context.Context, start int, avoid *string) (string, bool, error) {
	if c.sentinel {
		for _, seed := range c.addrs {
			addr, ok, err := c.askSentinel(ctx, seed)
			if err != nil {
				return "", false, err
			}
			if !ok {
				continue
			}
			role, ok, err := c.askRole(ctx, addr)
			if err != nil {
				return "", false, err
			}
			if ok && addr == *avoid && !role.master {
				*avoid = ""
			}
			if ok && role.master && addr != *avoid {
				return addr, true, nil
			}
		}
		return "", false, nil
	}
	nodes := c.nodeList()
	queue := make([]string, 0, len(nodes)+1)
	seen := make(map[string]struct{}, len(nodes)+1)
	for step := range nodes {
		a := nodes[(start+step)%len(nodes)]
		queue = append(queue, a)
		seen[a] = struct{}{}
	}
	for len(queue) > 0 {
		addr := queue[0]
		queue = queue[1:]
		role, ok, err := c.askRole(ctx, addr)
		if err != nil {
			return "", false, err
		}
		if ctx.Err() != nil {
			return "", false, nil
		}
		if ok && addr == *avoid && !role.master {
			*avoid = ""
		}
		switch {
		case !ok:
		case role.master:
			if addr != *avoid {
				return addr, true, nil
			}
		case role.hint != "":
			if _, dup := seen[role.hint]; !dup {
				seen[role.hint] = struct{}{}
				queue = append(queue, role.hint)
			}
		}
	}
	return "", false, nil
}

type roleReply struct {
	master bool
	hint   string // host:port a replica reports for its primary, "" when it reports none
}

func (c *Client) askRole(ctx context.Context, addr string) (roleReply, bool, error) {
	v, ok, err := c.probe(ctx, addr, "ROLE")
	if err != nil || !ok {
		return roleReply{}, false, err
	}
	role, ok := parseRole(v)
	return role, ok, nil
}

// parseRole reads the ROLE reply: master, or slave with the host and port it reports.
func parseRole(v Value) (roleReply, bool) {
	if v.Kind != KindArray || len(v.Array) == 0 {
		return roleReply{}, false
	}
	switch v.Array[0].Str() {
	case "master":
		return roleReply{master: true}, true
	case "slave":
		var r roleReply
		if len(v.Array) >= 3 {
			if port, ok := portOf(v.Array[2]); ok {
				if host := v.Array[1].Str(); host != "" {
					r.hint = net.JoinHostPort(host, strconv.Itoa(port))
				}
			}
		}
		return r, true
	}
	return roleReply{}, false
}

// portOf reads a port from an integer or a string.
func portOf(v Value) (int, bool) {
	var n int64
	switch v.Kind {
	case KindInt:
		n = v.Int
	case KindBulk, KindSimple, KindVerbatim:
		var err error
		if n, err = strconv.ParseInt(string(v.Bytes), 10, 32); err != nil {
			return 0, false
		}
	default:
		return 0, false
	}
	if n < 1 || n > 65535 {
		return 0, false
	}
	return int(n), true
}

// askSentinel asks a coordinator which node is the primary of Options.SentinelMaster.
func (c *Client) askSentinel(ctx context.Context, addr string) (string, bool, error) {
	v, ok, err := c.probe(ctx, addr, "SENTINEL", "get-master-addr-by-name", c.opts.SentinelMaster)
	if err != nil || !ok || v.Kind != KindArray || len(v.Array) != 2 {
		return "", false, err
	}
	host := v.Array[0].Str()
	port, ok := portOf(v.Array[1])
	if host == "" || !ok {
		return "", false, nil
	}
	return net.JoinHostPort(host, strconv.Itoa(port)), true, nil
}

// probe is a short-lived socket that asks one question: it connects (bounded by DialTimeout),
// sends AUTH when configured and nothing else, sends the command, reads one reply and closes.
// A reply wait is the smaller of DialTimeout and CommandTimeout, so a hung node costs one
// bounded wait. ok is false for a node that cannot be connected, times out, drops or answers
// with an error; a refused AUTH is returned as err.
func (c *Client) probe(ctx context.Context, addr string, args ...any) (v Value, ok bool, err error) {
	if ctx.Err() != nil {
		return Value{}, false, nil
	}
	dctx, cancel := context.WithTimeout(ctx, c.opts.DialTimeout)
	var nc net.Conn
	nc, err = c.transport(dctx, addr)
	cancel()
	if err != nil {
		return Value{}, false, nil
	}
	cn := newConn(nc, nil)
	defer cn.close()
	wait := c.opts.DialTimeout
	if t := c.opts.CommandTimeout; t > 0 && t < wait {
		wait = t
	}
	ask := func(cmd ...any) (Value, error) {
		r := getReq()
		var aerr error
		if r.buf, aerr = AppendCommand(r.buf, cmd...); aerr != nil {
			putReq(r)
			return Value{}, aerr
		}
		rctx, cancel := context.WithTimeout(ctx, wait)
		defer cancel()
		return cn.exec(rctx, r)
	}
	if c.opts.Password != "" {
		cmd := []any{"AUTH", c.opts.Password}
		if c.opts.Username != "" {
			cmd = []any{"AUTH", c.opts.Username, c.opts.Password}
		}
		if _, aerr := ask(cmd...); aerr != nil {
			if isServerErr(aerr) {
				return Value{}, false, aerr
			}
			return Value{}, false, nil
		}
	}
	v, qerr := ask(args...)
	if qerr != nil {
		return Value{}, false, nil
	}
	return v, true, nil
}

// isReadonly reports a refused write: the error text is READONLY alone or READONLY followed
// by a space.
func isReadonly(err error) bool {
	e, ok := err.(*Error)
	if !ok {
		return false
	}
	m := e.Msg
	return strings.HasPrefix(m, "READONLY") && (len(m) == 8 || m[8] == ' ')
}

// execFollow runs one command on the shared connection with the failover rules: a READONLY
// reply moves the client to the primary and sends the command once more.
func (c *Client) execFollow(ctx context.Context, cn *conn, r *request) (Value, error) {
	v, kept, err := cn.execKeep(ctx, r)
	if !kept {
		return v, err
	}
	if err != nil && isReadonly(err) {
		var release bool
		if v, release, err = c.afterReadonly(ctx, cn, r, v, err); !release {
			return v, err
		}
	}
	putReq(r)
	return v, err
}

// afterReadonly follows the primary and, unless a client-opened transaction was lost with
// the old socket, sends r once more. release is false when r now belongs to the reader.
func (c *Client) afterReadonly(ctx context.Context, from *conn, r *request, v Value, refused error) (Value, bool, error) {
	retry := !c.inMulti.Load()
	cn, moved, err := c.followPrimary(ctx, from)
	if err != nil {
		return Value{}, true, err
	}
	if !moved {
		return v, true, refused
	}
	if !retry {
		return v, true, refused
	}
	r.rearm()
	v, kept, err := cn.execKeep(ctx, r)
	return v, kept, err
}

// followPrimary replaces the shared connection by one to the primary: rounds with a growing
// wait (100 ms doubling to 2 s, up to 8 rounds), avoiding the address that answered READONLY.
// Concurrent callers are serialized; one that finds the connection already replaced uses it.
// moved is false when no round found a primary.
func (c *Client) followPrimary(ctx context.Context, from *conn) (cn *conn, moved bool, err error) {
	c.dialMu.Lock()
	defer c.dialMu.Unlock()
	if c.closed.Load() {
		return nil, false, ErrClosed
	}
	if cur := c.cur.Load(); cur != from && cur != nil && !cur.dead.Load() {
		return cur, true, nil
	}
	// Kept across rounds: cleared once the refusing node says it is a replica, so its promotion
	// a moment later is taken instead of waiting out every round.
	refused := ""
	if nodes := c.nodeList(); int(from.addr) < len(nodes) {
		refused = nodes[from.addr]
	}
	start := int(from.addr) + 1
	if c.sentinel {
		start = 0
	}
	delay := c.followDelay
	for round := 0; round < c.followRounds; round++ {
		if round > 0 {
			t := time.NewTimer(delay)
			select {
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
				return nil, false, nil
			case <-c.done:
				t.Stop()
				return nil, false, ErrClosed
			}
			if delay *= 2; delay > c.followMaxDelay {
				delay = c.followMaxDelay
			}
		}
		addr, ok, err := c.findPrimary(ctx, start, &refused)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			continue
		}
		ncn, err := c.dialAddr(ctx, c.opts.OnPush, c.nodeIndex(addr), addr)
		if err != nil {
			if !isConnectError(err) {
				return nil, false, err
			}
			continue
		}
		c.cur.Store(ncn)
		c.inMulti.Store(false)
		from.close()
		c.closeIdle()
		return ncn, true, nil
	}
	return nil, false, nil
}

// closeIdle drops the idle dedicated connections, which may be on the node that was left.
func (c *Client) closeIdle() {
	c.idleMu.Lock()
	idle := c.idle
	c.idle = nil
	c.idleMu.Unlock()
	for _, cn := range idle {
		cn.close()
	}
}

// trackMulti keeps the transaction flag of the shared connection for a command sent with
// Do: set by an acknowledged MULTI, cleared by EXEC or DISCARD.
func (c *Client) trackMulti(args []any, v Value, err error) {
	if len(args) == 0 {
		return
	}
	var name string
	switch a := args[0].(type) {
	case string:
		name = a
	case []byte:
		name = string(a)
	default:
		return
	}
	switch {
	case strings.EqualFold(name, "MULTI"):
		if err == nil && v.Kind == KindSimple {
			c.inMulti.Store(true)
		}
	case strings.EqualFold(name, "EXEC"), strings.EqualFold(name, "DISCARD"):
		c.inMulti.Store(false)
	}
}
