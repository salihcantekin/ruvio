// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

func (cl *cluster) exec(ctx context.Context, r *request) (Value, error) {
	return cl.route(ctx, r, false)
}

func (cl *cluster) execBlocking(ctx context.Context, r *request) (Value, error) {
	return cl.route(ctx, r, true)
}

func (cl *cluster) route(ctx context.Context, r *request, blocking bool) (Value, error) {
	t, err := cl.topo(ctx)
	if err != nil {
		putReq(r)
		return Value{}, err
	}
	if t == nil {
		if blocking {
			return cl.seed.execBlockingLocal(ctx, r)
		}
		return cl.seed.execLocal(ctx, r)
	}
	spec, slot, _, err := routeFrame(r.buf)
	if err != nil {
		putReq(r)
		return Value{}, err
	}
	switch {
	case spec.scan:
		return cl.scan(ctx, t, r)
	case spec.fan != fanNone && fansOut(spec, r.buf):
		return cl.fanOut(ctx, t, spec, r)
	case slot == slotCross:
		if spec.split {
			return cl.split(ctx, t, spec, r)
		}
		err := crossSlotErr(r.buf)
		putReq(r)
		return Value{}, err
	}
	if cl.policy != ReadPrimary && slot >= 0 && !blocking && spec.reads() {
		return cl.runRead(ctx, t, slot, r)
	}
	return cl.run(ctx, t, slot, r, blocking)
}

// replicaFor returns the replica the reads of slot go to, connected. It returns nil when
// the read goes to the primary (ReadPreferReplica with no usable replica), and an error
// under ReadReplica when there is none.
func (cl *cluster) replicaFor(ctx context.Context, t *topology, slot int) (*replicaRoute, error) {
	var rt *replicaRoute
	if ni := t.owner[slot]; ni != noOwner && int(ni) < len(t.read) {
		rt = t.read[ni]
	}
	if rt != nil && !rt.bad.Load() {
		_, err := rt.node.shared(ctx)
		if err == nil {
			return rt, nil
		}
		if ctx.Err() != nil {
			return nil, err
		}
		rt.bad.Store(true)
		if cl.policy == ReadReplica {
			return nil, err
		}
		return nil, nil
	}
	if cl.policy == ReadReplica {
		return nil, fmt.Errorf("ruvio: no replica is known for slot %d (ReadReplica)", slot)
	}
	return nil, nil
}

// isLoading reports whether an error reply says a replica is still copying its primary
// (LOADING) or its copy stalled (MASTERDOWN).
func isLoading(msg []byte) bool {
	return bytes.HasPrefix(msg, []byte("LOADING")) || bytes.HasPrefix(msg, []byte("MASTERDOWN"))
}

// runRead sends the read r for slot to the slot's replica. A replica that answers LOADING
// is skipped from then on, and the read goes to the primary unless the policy is
// ReadReplica. It recycles r.
func (cl *cluster) runRead(ctx context.Context, t *topology, slot int, r *request) (Value, error) {
	rt, err := cl.replicaFor(ctx, t, slot)
	if err != nil {
		putReq(r)
		return Value{}, err
	}
	if rt == nil {
		return cl.run(ctx, t, slot, r, false)
	}
	ok, err := rt.node.rt(ctx, r, false, false)
	if !ok {
		return Value{}, err
	}
	if r.err != nil {
		err := r.err // the socket failed after the command was sent: not sent again
		putReq(r)
		return Value{}, err
	}
	v := r.val
	if v.Kind == KindError {
		loading := isLoading(v.Bytes)
		if loading {
			rt.bad.Store(true)
		}
		if _, _, isMoved := parseMoved(v.Bytes); (loading && cl.policy != ReadReplica) || isMoved {
			r.val, r.vals, r.err = Value{}, nil, nil // the command was not executed: send it again
			r.state.Store(stateWaiting)
			return cl.run(ctx, t, slot, r, false)
		}
		putReq(r)
		return v, &Error{Msg: b2s(v.Bytes)}
	}
	putReq(r)
	return v, nil
}

func crossSlotErr(buf []byte) error {
	var st [2][]byte
	if a, _, _, ok := frameArgs(buf, 1, st[:0]); ok && len(a) == 1 {
		return fmt.Errorf("%w (%s)", ErrCrossSlot, strings.ToUpper(string(a[0])))
	}
	return ErrCrossSlot
}

// run sends r to the node owning slot (the seed for slot < 0), following -MOVED. It
// recycles r.
func (cl *cluster) run(ctx context.Context, t *topology, slot int, r *request, blocking bool) (Value, error) {
	node := cl.seed
	if slot >= 0 {
		node = t.nodeAt(slot, cl.seed)
	}
	return cl.send(ctx, node, r, blocking, 0)
}

// send sends r on node and follows -MOVED and -ASK until the command has been redirected
// maxRedirects times in all; redirects is how many it has already followed. Each
// -MOVED patches the slot and goes to the node it named; from the second redirect on it
// also reloads the table. -ASK sends ASKING and the command to the named node and leaves
// the table alone. -TRYAGAIN sends the command again, routed by the current table, up to
// maxTryAgain times. It recycles r.
func (cl *cluster) send(ctx context.Context, node *Client, r *request, blocking bool, redirects int) (Value, error) {
	return cl.drive(ctx, node, false, r, blocking, redirects, 0)
}

// drive is the loop behind send; asking says the first send goes out behind ASKING, and
// retries is how many -TRYAGAIN retries the command has used.
func (cl *cluster) drive(ctx context.Context, node *Client, asking bool, r *request, blocking bool, redirects, retries int) (Value, error) {
	reloaded := false
	for {
		var ok bool
		var err error
		if asking {
			asking = false
			var v Value
			if v, ok, err = cl.askRT(ctx, node, r, blocking); !ok {
				putReq(r)
				if v.Kind == KindError {
					return v, &Error{Msg: b2s(v.Bytes)}
				}
				return Value{}, err
			}
		} else {
			ok, err = node.rt(ctx, r, blocking, !reloaded)
			if !ok {
				return Value{}, err
			}
		}
		if r.err != nil {
			err := r.err
			if !reloaded && isConnectError(err) {
				// The node cannot be opened: reload the slot map from the other nodes and
				// the seeds, and send the command to the node the new map names.
				reloaded = true
				if nn := cl.reloadNode(ctx, r); nn != nil {
					node = nn
					r.val, r.vals, r.err = Value{}, nil, nil
					r.state.Store(stateWaiting)
					continue
				}
			}
			putReq(r)
			return Value{}, err
		}
		v := r.val
		if v.Kind == KindError {
			nn, ask, err := cl.follow(ctx, v.Bytes, r, &redirects, &retries)
			if err != nil {
				putReq(r)
				return Value{}, err
			}
			if nn != nil {
				node, asking = nn, ask
				r.val, r.vals, r.err = Value{}, nil, nil // the command was not executed: send it again
				r.state.Store(stateWaiting)
				continue
			}
			putReq(r)
			return v, &Error{Msg: b2s(v.Bytes)}
		}
		putReq(r)
		return v, nil
	}
}

// follow decides what to do with the error reply msg to r: it returns the node to send r
// to again (behind ASKING when ask is set), or a nil node when the reply goes back to the
// caller (not a redirect, or its budget is spent). -TRYAGAIN waits before it returns.
func (cl *cluster) follow(ctx context.Context, msg []byte, r *request, redirects, retries *int) (node *Client, ask bool, err error) {
	if ms, addr, ok := parseMoved(msg); ok {
		if *redirects >= maxRedirects {
			return nil, false, nil
		}
		*redirects++
		node, err = cl.applyMoved(ctx, ms, addr, *redirects >= 2)
		return node, false, err
	}
	if _, addr, ok := parseAsk(msg); ok {
		if *redirects >= maxRedirects {
			return nil, false, nil
		}
		*redirects++
		cl.mu.Lock()
		node = cl.node(addr)
		cl.mu.Unlock()
		return node, true, nil
	}
	if isTryAgain(msg) {
		if *retries >= maxTryAgain {
			return nil, false, nil
		}
		wait := tryAgainWait << *retries
		*retries++
		tm := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			tm.Stop()
			return nil, false, ctx.Err()
		case <-tm.C:
		}
		node = cl.seed
		if _, slot, _, rerr := routeFrame(r.buf); rerr == nil && slot >= 0 {
			if t := cl.t.Load(); t != nil {
				node = t.nodeAt(slot, cl.seed)
			}
		}
		return node, false, nil
	}
	return nil, false, nil
}

// askFrame is ASKING as a RESP frame.
const askFrame = "*1\r\n$6\r\nASKING\r\n"

// askRT sends ASKING and the command in r to node in one write and puts the command's
// reply in r.val. ok=false: the call failed; a failed ASKING comes back as v with
// KindError, any other failure as err. The caller recycles r either way.
func (cl *cluster) askRT(ctx context.Context, node *Client, r *request, blocking bool) (v Value, ok bool, err error) {
	q := getReq()
	q.buf = append(append(q.buf, askFrame...), r.buf...)
	q.expect = 2
	q.vals = make([]Value, 0, 2)
	rok, err := node.rt(ctx, q, blocking, false)
	if !rok {
		return Value{}, false, err
	}
	if q.err != nil {
		err = q.err
		putReq(q)
		return Value{}, false, err
	}
	if len(q.vals) != 2 {
		putReq(q)
		return Value{}, false, ErrProtocol
	}
	first, second := q.vals[0], q.vals[1]
	putReq(q)
	if first.Kind == KindError {
		return first, false, nil
	}
	r.val = second
	return Value{}, true, nil
}

// reloadNode reloads the slot table after a node could not be opened and returns the node
// that should now get r: the owner of its slot, or for a keyless command the first node of
// the new table that can be opened. It returns nil when the table cannot be reloaded.
func (cl *cluster) reloadNode(ctx context.Context, r *request) *Client {
	_, slot, _, err := routeFrame(r.buf)
	if err != nil {
		return nil
	}
	if _, err := cl.refresh(ctx, false); err != nil {
		return nil
	}
	t := cl.t.Load()
	if t == nil {
		return nil
	}
	if slot >= 0 {
		return t.nodeAt(slot, cl.seed)
	}
	for _, n := range t.nodes {
		if _, err := n.shared(ctx); err == nil {
			return n
		}
	}
	return nil
}

// redirect resends the command in r after its first -MOVED, -ASK or -TRYAGAIN reply msg
// (from a pipelined write), with the same limits and table updates as send. It recycles r.
func (cl *cluster) redirect(ctx context.Context, msg []byte, r *request) (Value, error) {
	redirects, retries := 0, 0
	nn, ask, err := cl.follow(ctx, msg, r, &redirects, &retries)
	if err != nil {
		putReq(r)
		return Value{}, err
	}
	if nn == nil {
		putReq(r)
		return Value{Kind: KindError, Bytes: msg}, &Error{Msg: b2s(msg)}
	}
	return cl.drive(ctx, nn, ask, r, false, redirects, retries)
}

// routeFrame reads the command at the start of b and reports how to route it. slot is the
// hash slot of its keys, -1 for a keyless command, or slotCross when the keys of one
// command span several slots.
func routeFrame(b []byte) (spec cmdSpec, slot, nargs int, err error) {
	var stack [2][]byte
	args, n, _, ok := frameArgs(b, 2, stack[:0])
	if !ok || n == 0 {
		return spec, -1, 0, ErrProtocol
	}
	spec, _ = specOf(args[0])
	switch r := spec.keys; {
	case r.kind == ruleNone:
		return spec, -1, n, nil
	case r.kind == ruleRange && r.a == 1 && r.b == 1: // the key is argument 1 (the common case)
		if n < 2 {
			return spec, -1, n, nil
		}
		return spec, hashSlot(args[1]), n, nil
	}
	var all [16][]byte
	full, _, _, ok := frameArgs(b, 0, all[:0])
	if !ok {
		return spec, -1, n, ErrProtocol
	}
	var ks [16][]byte
	keys := keysOf(spec.keys, full, ks[:0])
	if len(keys) == 0 {
		return spec, -1, n, nil
	}
	slot = hashSlot(keys[0])
	for _, k := range keys[1:] {
		if hashSlot(k) != slot {
			return spec, slotCross, n, nil
		}
	}
	return spec, slot, n, nil
}

// fansOut reports whether the command really goes to every node (SCRIPT/FUNCTION only for
// their state-changing subcommands).
func fansOut(spec cmdSpec, buf []byte) bool {
	if spec.fan != fanSub {
		return true
	}
	var st [2][]byte
	args, _, _, ok := frameArgs(buf, 2, st[:0])
	if !ok || len(args) < 2 {
		return false
	}
	s := args[1]
	return upperEq(s, "LOAD") || upperEq(s, "FLUSH") || upperEq(s, "DELETE") || upperEq(s, "RESTORE")
}

// ---- multi-key split ---------------------------------------------------------------------

type slotPart struct {
	slot int
	pos  []int // key (or pair) positions in the caller's order
}

// splitPlan is one MGET, MSET, DEL, UNLINK, EXISTS or TOUCH whose keys span slots, cut
// into one command per slot. args alias the caller's frame.
type splitPlan struct {
	args      [][]byte
	keys      [][]byte
	parts     []slotPart
	canonical []byte // the pre-encoded command name
	pairs     bool   // MSET: key value key value ...
	isMGet    bool
}

func newSplitPlan(spec cmdSpec, frame []byte) (*splitPlan, bool) {
	args, _, _, ok := frameArgs(frame, 0, nil)
	if !ok {
		return nil, false
	}
	p := &splitPlan{args: args, pairs: spec.keys.c == 2, isMGet: upperEq(args[0], "MGET"), canonical: preEncBytes(args[0])}
	if p.pairs {
		for i := 1; i < len(args); i += 2 {
			p.keys = append(p.keys, args[i])
		}
	} else {
		p.keys = args[1:]
	}
	index := make(map[int]int, 4)
	for i, k := range p.keys {
		s := hashSlot(k)
		pi, seen := index[s]
		if !seen {
			pi = len(p.parts)
			index[s] = pi
			p.parts = append(p.parts, slotPart{slot: s})
		}
		p.parts[pi].pos = append(p.parts[pi].pos, i)
	}
	return p, true
}

// appendPart appends the command for part pi to dst.
func (p *splitPlan) appendPart(dst *request, pi int) {
	part := p.parts[pi]
	per := 1
	if p.pairs {
		per = 2
	}
	dst.buf = appendArrayHeader(dst.buf, 1+per*len(part.pos))
	dst.buf = append(dst.buf, p.canonical...)
	for _, i := range part.pos {
		if p.pairs {
			dst.b(p.args[1+2*i]).b(p.args[2+2*i])
		} else {
			dst.b(p.keys[i])
		}
	}
}

// merge builds the caller's reply from the per-part replies (MGET in key order, MSET OK,
// counts summed). An error reply of a part is returned as the error.
func (p *splitPlan) merge(replies []Value) (Value, error) {
	for _, v := range replies {
		if v.Kind == KindError {
			return v, &Error{Msg: b2s(v.Bytes)}
		}
	}
	switch {
	case p.isMGet:
		out := make([]Value, len(p.keys))
		for pi, part := range p.parts {
			if replies[pi].Kind != KindArray || len(replies[pi].Array) != len(part.pos) {
				return Value{}, unexpected(replies[pi])
			}
			for j, i := range part.pos {
				out[i] = replies[pi].Array[j]
			}
		}
		return Value{Kind: KindArray, Array: out}, nil
	case p.pairs:
		return Value{Kind: KindSimple, Bytes: []byte("OK")}, nil
	}
	var sum int64
	for _, v := range replies {
		n, err := v.AsInt64()
		if err != nil {
			return Value{}, err
		}
		sum += n
	}
	return Value{Kind: KindInt, Int: sum}, nil
}

// split runs a split command: one command per slot, all concurrent, replies re-assembled
// in the caller's order. Parts are not atomic with respect to each other.
func (cl *cluster) split(ctx context.Context, t *topology, spec cmdSpec, r *request) (Value, error) {
	p, ok := newSplitPlan(spec, r.buf)
	if !ok {
		putReq(r)
		return Value{}, ErrProtocol
	}
	parts := p.parts
	replies := make([]Value, len(parts))
	errs := make([]error, len(parts))
	// One pipelined write per node (the server wants a single slot per command, so a node
	// gets one command per slot it owns); the nodes run concurrently.
	type nodeGroup struct{ parts []int }
	var order []int
	groups := map[int]*nodeGroup{}
	readParts := cl.policy != ReadPrimary && spec.reads()
	for pi, part := range parts {
		ni := int(t.owner[part.slot])
		if readParts {
			rt, err := cl.replicaFor(ctx, t, part.slot)
			if err != nil {
				putReq(r)
				return Value{}, err
			}
			if rt != nil {
				ni = -2 - ni // a negative key names the replica of node -2-ni
			}
		}
		g := groups[ni]
		if g == nil {
			g = &nodeGroup{}
			groups[ni] = g
			order = append(order, ni)
		}
		g.parts = append(g.parts, pi)
	}
	var wg sync.WaitGroup
	do := func(ni int) {
		g := groups[ni]
		node := cl.seed
		var rt *replicaRoute
		switch {
		case ni <= -2:
			rt = t.read[-2-ni]
			node = rt.node
		case ni != noOwner:
			node = t.nodes[ni]
		}
		sub := getReq()
		for _, pi := range g.parts {
			p.appendPart(sub, pi)
		}
		ok, err := node.rtMany(ctx, sub, len(g.parts))
		if !ok || sub.err != nil {
			if ok {
				err = sub.err
				putReq(sub)
			}
			for _, pi := range g.parts {
				errs[pi] = err
			}
			return
		}
		for j, pi := range g.parts {
			v := sub.vals[j]
			if v.Kind == KindError {
				if rt != nil && isLoading(v.Bytes) {
					rt.bad.Store(true)
					if cl.policy != ReadReplica { // read this part from the primary
						again := getReq()
						p.appendPart(again, pi)
						replies[pi], errs[pi] = cl.run(ctx, t, parts[pi].slot, again, false)
						continue
					}
				}
				if _, _, isMoved := parseMoved(v.Bytes); isMoved { // repair this part alone
					again := getReq()
					p.appendPart(again, pi)
					replies[pi], errs[pi] = cl.redirect(ctx, v.Bytes, again)
					continue
				}
				errs[pi] = &Error{Msg: b2s(v.Bytes)}
				continue
			}
			replies[pi] = v
		}
		putReq(sub)
	}
	for _, ni := range order[1:] {
		wg.Add(1)
		go func(ni int) { defer wg.Done(); do(ni) }(ni)
	}
	do(order[0])
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			putReq(r)
			return Value{}, err
		}
	}
	v, err := p.merge(replies)
	putReq(r)
	return v, err
}

func preEncBytes(name []byte) []byte {
	// name is an argument; upper-case it so the frame is canonical.
	return preEnc(strings.ToUpper(string(name)))
}

// ---- fan-out (DBSIZE, FLUSHDB, KEYS, SCRIPT LOAD ...) ------------------------------------------

func (cl *cluster) fanOut(ctx context.Context, t *topology, spec cmdSpec, r *request) (Value, error) {
	nodes := t.nodes
	replies := make([]Value, len(nodes))
	errs := make([]error, len(nodes))
	var wg sync.WaitGroup
	do := func(i int) {
		sub := getReq()
		sub.buf = append(sub.buf, r.buf...)
		replies[i], errs[i] = nodes[i].execLocal(ctx, sub)
	}
	for i := 1; i < len(nodes); i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); do(i) }(i)
	}
	do(0)
	wg.Wait()
	putReq(r)
	for _, err := range errs {
		if err != nil {
			return Value{}, err
		}
	}
	switch spec.fan {
	case fanSum:
		var sum int64
		for _, v := range replies {
			n, err := v.AsInt64()
			if err != nil {
				return Value{}, err
			}
			sum += n
		}
		return Value{Kind: KindInt, Int: sum}, nil
	case fanConcat:
		var out []Value
		for _, v := range replies {
			if v.Kind != KindArray {
				return Value{}, unexpected(v)
			}
			out = append(out, v.Array...)
		}
		return Value{Kind: KindArray, Array: out}, nil
	}
	return replies[0], nil
}

// ---- SCAN across nodes -----------------------------------------------------------------------------

// scan walks the nodes one after the other. The cursor handed to the caller encodes the
// node: client = server*nodes + nodeIndex; "0" ends the walk after the last node.
func (cl *cluster) scan(ctx context.Context, t *topology, r *request) (Value, error) {
	args, _, _, ok := frameArgs(r.buf, 0, nil)
	if !ok || len(args) < 2 {
		putReq(r)
		return Value{}, &Error{Msg: "ERR wrong number of arguments for 'scan' command"}
	}
	cursor, err := strconv.ParseUint(string(args[1]), 10, 64)
	if err != nil {
		putReq(r)
		return Value{}, &Error{Msg: "ERR invalid cursor"}
	}
	nn := uint64(len(t.nodes))
	node := int(cursor % nn)
	sub := getReq()
	sub.buf = appendArrayHeader(sub.buf, len(args))
	sub.buf = append(sub.buf, cSCAN...)
	sub.u(cursor / nn)
	for _, a := range args[2:] {
		sub.b(a)
	}
	putReq(r)
	v, err := t.nodes[node].execLocal(ctx, sub)
	if err != nil {
		return v, err
	}
	if v.Kind != KindArray || len(v.Array) < 2 {
		return Value{}, unexpected(v)
	}
	next, err := v.Array[0].AsInt64()
	if err != nil {
		return Value{}, err
	}
	var out uint64
	switch {
	case next != 0:
		if uint64(next) > (1<<63-1)/nn-1 {
			return Value{}, errors.New("ruvio: scan cursor overflow")
		}
		out = uint64(next)*nn + uint64(node)
	case uint64(node)+1 < nn:
		out = uint64(node) + 1
	}
	return Value{Kind: KindArray, Array: []Value{
		{Kind: KindBulk, Bytes: strconv.AppendUint(nil, out, 10)}, v.Array[1],
	}}, nil
}
