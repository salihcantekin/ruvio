// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"fmt"
	"sync"
)

// ---- pipelines ---------------------------------------------------------------------------------------

func (c *Client) execManyLocal(ctx context.Context, r *request, n int) ([]Value, error) {
	cn, err := c.shared(ctx)
	if err != nil {
		putReq(r)
		return nil, err
	}
	return cn.execMany(ctx, r, n)
}

// rtMany sends a pipeline of n commands; see rt for the ownership rules.
func (c *Client) rtMany(ctx context.Context, r *request, n int) (bool, error) {
	r.expect = n
	r.vals = make([]Value, 0, n)
	return c.rt(ctx, r, false, false)
}

func (cl *cluster) execMany(ctx context.Context, r *request, n int) ([]Value, error) {
	t, err := cl.topo(ctx)
	if err != nil {
		putReq(r)
		return nil, err
	}
	if t == nil {
		return cl.seed.execManyLocal(ctx, r, n)
	}
	// pass 1: where does each command go? Allocation-free: the frame offsets and node indexes
	// go to pooled scratch, which the spread path reuses.
	first, uniform, sawMulti, differ, split, firstSlot := -1, true, false, false, false, -1
	s := pipePool.Get().(*pipeScratch)
	defer pipePool.Put(s)
	s.offs, s.target = s.offs[:0], s.target[:0]
	seedIndex := len(t.nodes) // keyless commands, and slots without an owner, go to the seed
	// A pipeline goes to replicas only when every command in it is a read, so it never reads
	// its own writes from a replica that has not received them.
	replicaReads := cl.policy != ReadPrimary
	cur := -1
	off := 0
	for i := 0; i < n; i++ {
		s.offs = append(s.offs, off)
		spec, slot, _, err := routeFrame(r.buf[off:])
		if err != nil {
			putReq(r)
			return nil, err
		}
		end, ok := frameEnd(r.buf[off:])
		if !ok {
			putReq(r)
			return nil, ErrProtocol
		}
		off += end
		if replicaReads && !spec.reads() {
			replicaReads = false
		}
		if upperEq(cmdName(r.buf[off-end:]), "MULTI") {
			sawMulti = true
		}
		if slot == slotCross {
			if !spec.split {
				err := crossSlotErr(r.buf[off-end:]) // before r is recycled
				putReq(r)
				return nil, err
			}
			split, differ, uniform = true, true, false // sent per slot, like the plain command
			s.target = append(s.target, cur)
			continue
		}
		if slot < 0 {
			s.target = append(s.target, cur) // keyless commands follow the previous keyed one
			continue
		}
		if firstSlot < 0 {
			firstSlot = slot
		} else if slot != firstSlot {
			differ = true
		}
		ni := int(t.owner[slot])
		if first < 0 {
			first = ni
		} else if ni != first {
			uniform = false
		}
		if cur = ni; ni == noOwner {
			cur = seedIndex
		}
		s.target = append(s.target, cur)
	}
	s.offs = append(s.offs, off)
	if replicaReads && n > 0 {
		return cl.pipeReads(ctx, t, r, n, s, split)
	}
	if sawMulti && differ {
		putReq(r)
		return nil, fmt.Errorf("%w (MULTI)", ErrCrossSlot)
	}
	if sawMulti { // a transaction travels as one unit to the node of its slot
		uniform = true
	}
	if uniform {
		node := cl.seed
		if firstSlot >= 0 && first != noOwner {
			node = t.nodes[first]
		}
		return cl.pipeOne(ctx, node, r, n)
	}
	if split {
		return cl.pipeMany(ctx, t, r, n, split)
	}
	return cl.pipeSpread(ctx, t, r, n, s)
}

// pipeReads sends a pipeline of reads: each command goes to the replica of its slot's node
// (or to the primary where there is none), keyless commands to the seed. s holds the frame
// offsets of the pipeline. A split command in it is read on its own, so the pipeline runs one
// command at a time. It recycles r.
func (cl *cluster) pipeReads(ctx context.Context, t *topology, r *request, n int, s *pipeScratch, split bool) ([]Value, error) {
	if split {
		return cl.pipeReadsEach(ctx, r, n, s)
	}
	s.target = s.target[:0]
	for i := 0; i < n; i++ {
		_, slot, _, _ := routeFrame(r.buf[s.offs[i]:])
		ni := len(t.nodes) // the seed
		if slot >= 0 {
			rt, err := cl.replicaFor(ctx, t, slot)
			if err != nil {
				putReq(r)
				return nil, err
			}
			if o := int(t.owner[slot]); rt != nil {
				ni = o + len(t.nodes) + 1 // the replica of node o
			} else if o != noOwner {
				ni = o
			}
		}
		s.target = append(s.target, ni)
	}
	return cl.pipeSpread(ctx, t, r, n, s)
}

// pipeReadsEach runs the commands of a pipeline of reads one after the other, each through
// the routing of a plain command.
func (cl *cluster) pipeReadsEach(ctx context.Context, r *request, n int, s *pipeScratch) ([]Value, error) {
	out := make([]Value, n)
	for i := 0; i < n; i++ {
		sub := getReq()
		sub.buf = append(sub.buf, r.buf[s.offs[i]:s.offs[i+1]]...)
		v, err := cl.exec(ctx, sub)
		if err != nil && v.Kind != KindError {
			putReq(r)
			return nil, err
		}
		out[i] = v
	}
	putReq(r)
	return out, nil
}

// pipeScratch is the per-pipeline routing state of execMany, pooled so a pipeline spread over
// nodes allocates only its reply slice.
type pipeScratch struct {
	offs   []int      // frame start of each command, then the end of the last
	target []int      // node index of each command (len(nodes) is the seed)
	subs   []*request // one pipelined request per node in use
	vals   [][]Value  // reply buffers handed to each node's request, kept between calls
	conns  []*conn    // the connection each request went to
	leads  []bool     // whether this goroutine reads that connection's replies
}

var pipePool = sync.Pool{New: func() any { return new(pipeScratch) }}

// pipeSpread sends a pipeline whose commands go to several nodes: each node's frames are
// copied once into one request, every node is written before any reply is awaited (so the
// pipeline costs one round trip, without a goroutine per node), the caller reads the replies of
// every node nobody else is reading itself (one node if a command may block), and each reply is
// stored at its command's index.
func (cl *cluster) pipeSpread(ctx context.Context, t *topology, r *request, n int, s *pipeScratch) ([]Value, error) {
	if err := ctx.Err(); err != nil {
		putReq(r)
		return nil, err
	}
	for i := 0; i < n && s.target[i] < 0; i++ { // leading keyless commands join the first keyed node
		for _, tg := range s.target {
			if tg >= 0 {
				s.target[i] = tg
				break
			}
		}
	}
	nodes := len(t.nodes) + 1
	if len(t.read) > 0 {
		nodes += len(t.nodes) // the replicas follow the seed
	}
	for len(s.subs) < nodes {
		s.subs = append(s.subs, nil)
		s.vals = append(s.vals, nil)
	}
	subs := s.subs[:nodes]
	for i := 0; i < n; i++ {
		ni := s.target[i]
		sub := subs[ni]
		if sub == nil {
			sub = getReq()
			sub.expect = 0
			subs[ni] = sub
		}
		sub.buf = append(sub.buf, r.buf[s.offs[i]:s.offs[i+1]]...)
		sub.expect++
	}
	// Like roundTrip: the caller may read its own replies when its context cannot be canceled
	// without a deadline.
	canLead := ctx.Done() == nil
	if !canLead {
		_, canLead = ctx.Deadline()
	}
	if len(s.conns) < nodes {
		s.conns = append(s.conns, make([]*conn, nodes-len(s.conns))...)
		s.leads = append(s.leads, make([]bool, nodes-len(s.leads))...)
	}
	var first error
	for ni, sub := range subs {
		if sub == nil {
			continue
		}
		node := cl.seed
		if ni < len(t.nodes) {
			node = t.nodes[ni]
		} else if ni > len(t.nodes) {
			node = t.read[ni-len(t.nodes)-1].node
		}
		// A non-nil reply slice marks a pipeline for the reader, even for one command.
		if sub.vals = s.vals[ni][:0]; sub.vals == nil {
			sub.vals = make([]Value, 0, sub.expect)
		}
		cn, err := node.shared(ctx)
		if err == nil {
			// Every node is written before any is read. A node with nobody reading lets this
			// goroutine read its replies below, without a hand-off to the node's reader.
			s.conns[ni] = cn
			cn.setDeadline(ctx, sub)
			s.leads[ni], err = cn.send(sub, canLead)
		}
		if err != nil {
			if first == nil {
				first = err
			}
			putReq(sub)
			subs[ni], s.conns[ni], s.leads[ni] = nil, nil, false
			continue
		}
	}
	// This goroutine reads the nodes one after another. A pipeline that may block (BLPOP, XREAD
	// BLOCK, WAIT, ...) reads only one node here and hands the others to their background readers:
	// holding a second node's read side while the first waits would stall every other caller on
	// that node until the blocking command returns.
	if mayBlock(r.buf, s.offs, n) {
		led := false
		for ni := range subs {
			if s.leads[ni] {
				if led {
					s.conns[ni].releaseRead()
					s.leads[ni] = false
				}
				led = true
			}
		}
	}
	out := make([]Value, n)
	for ni, sub := range subs {
		if sub == nil {
			continue
		}
		subs[ni] = nil
		var ok bool
		var err error
		if s.leads[ni] {
			s.conns[ni].lead(sub)
			ok = true
		} else {
			ok, err = sub.wait(ctx)
		}
		s.conns[ni], s.leads[ni] = nil, false
		if !ok { // abandoned: the reader owns the request now
			s.vals[ni] = nil
			if first == nil {
				first = err
			}
			continue
		}
		if err == nil {
			err = sub.err
		}
		if err != nil {
			if first == nil {
				first = err
			}
			putReq(sub)
			continue
		}
		j := 0
		for i := 0; i < n; i++ {
			if s.target[i] == ni {
				out[i] = sub.vals[j]
				j++
			}
		}
		s.vals[ni] = sub.vals[:0]
		putReq(sub)
	}
	if first != nil {
		putReq(r)
		return nil, first
	}
	for i := range out {
		if out[i].Kind == KindError {
			if isRedirectErr(out[i].Bytes) {
				cl.redo(ctx, r.buf[s.offs[i]:s.offs[i+1]], &out[i])
			}
		}
	}
	putReq(r)
	return out, nil
}

func cmdName(b []byte) []byte {
	var st [1][]byte
	a, _, _, ok := frameArgs(b, 1, st[:0])
	if !ok || len(a) == 0 {
		return nil
	}
	return a[0]
}

// frameAt returns the i-th command frame of a pipeline buffer.
func frameAt(buf []byte, i int) []byte {
	off := 0
	for ; i > 0; i-- {
		end, _ := frameEnd(buf[off:])
		off += end
	}
	end, _ := frameEnd(buf[off:])
	return buf[off : off+end]
}

// pipeOne sends the whole pipeline to one node and repairs -MOVED, -ASK and -TRYAGAIN replies.
func (cl *cluster) pipeOne(ctx context.Context, node *Client, r *request, n int) ([]Value, error) {
	ok, err := node.rtMany(ctx, r, n)
	if !ok {
		return nil, err
	}
	if r.err != nil {
		err := r.err
		putReq(r)
		return nil, err
	}
	vals := r.vals
	for i := range vals {
		if vals[i].Kind == KindError {
			if isRedirectErr(vals[i].Bytes) {
				cl.redo(ctx, frameAt(r.buf, i), &vals[i])
			}
		}
	}
	putReq(r)
	return vals, nil
}

// redo sends one command again after -MOVED, -ASK or -TRYAGAIN (the table is updated first) and stores the
// outcome in dst.
func (cl *cluster) redo(ctx context.Context, frame []byte, dst *Value) {
	req := getReq()
	req.buf = append(req.buf, frame...)
	v, err := cl.redirect(ctx, dst.Bytes, req)
	if err != nil && v.Kind != KindError {
		v = Value{Kind: KindError, Bytes: []byte(err.Error())}
	}
	*dst = v
}

// pipeMany groups the commands by node, sends each group as one pipelined write
// (concurrently) and puts the replies back in the caller's order. With split set, a split
// command whose keys span slots goes out as one command per slot, in its place on each
// node, and its merged reply takes its index.
func (cl *cluster) pipeMany(ctx context.Context, t *topology, r *request, n int, split bool) ([]Value, error) {
	offs := make([]int, n+1)
	target := make([]int, n)
	var plans []*splitPlan // per command, nil unless it is split
	if split {
		plans = make([]*splitPlan, n)
	}
	cur := -1
	off := 0
	for i := 0; i < n; i++ {
		spec, slot, _, _ := routeFrame(r.buf[off:])
		end, _ := frameEnd(r.buf[off:])
		offs[i] = off
		off += end
		if slot == slotCross {
			p, ok := newSplitPlan(spec, r.buf[offs[i]:off])
			if !ok {
				putReq(r)
				return nil, ErrProtocol
			}
			plans[i] = p
			slot = p.parts[0].slot
		}
		if slot >= 0 {
			cur = int(t.owner[slot])
		}
		target[i] = cur // keyless commands follow the previous keyed one
	}
	offs[n] = off
	for i := 0; i < n && target[i] < 0; i++ { // leading keyless commands join the first keyed group
		for _, tg := range target {
			if tg >= 0 {
				target[i] = tg
				break
			}
		}
	}
	// item is one command sent: command i whole (part -1) or part of split command i.
	type item struct{ i, part int }
	type group struct {
		node  int
		items []item
	}
	var groups []group
	pos := map[int]int{}
	add := func(tg int, it item) {
		gi, ok := pos[tg]
		if !ok {
			gi = len(groups)
			pos[tg] = gi
			groups = append(groups, group{node: tg})
		}
		groups[gi].items = append(groups[gi].items, it)
	}
	var partVals [][]Value // per command, the replies of its parts
	if split {
		partVals = make([][]Value, n)
	}
	for i, tg := range target {
		if split && plans[i] != nil {
			partVals[i] = make([]Value, len(plans[i].parts))
			for pi, part := range plans[i].parts {
				add(int(t.owner[part.slot]), item{i, pi})
			}
			continue
		}
		add(tg, item{i, -1})
	}
	out := make([]Value, n)
	errs := make([]error, len(groups))
	var wg sync.WaitGroup
	do := func(gi int) {
		g := groups[gi]
		node := cl.seed
		if g.node >= 0 && g.node != noOwner {
			node = t.nodes[g.node]
		}
		sub := getReq()
		for _, it := range g.items {
			if it.part >= 0 {
				plans[it.i].appendPart(sub, it.part)
			} else {
				sub.buf = append(sub.buf, r.buf[offs[it.i]:offs[it.i+1]]...)
			}
		}
		ok, err := node.rtMany(ctx, sub, len(g.items))
		if !ok {
			errs[gi] = err
			return
		}
		if sub.err != nil {
			errs[gi] = sub.err
			putReq(sub)
			return
		}
		for j, it := range g.items {
			v := sub.vals[j]
			if it.part < 0 {
				out[it.i] = v
				continue
			}
			if v.Kind == KindError {
				if isRedirectErr(v.Bytes) { // repair this part alone
					again := getReq()
					plans[it.i].appendPart(again, it.part)
					var err error
					if v, err = cl.redirect(ctx, v.Bytes, again); err != nil && v.Kind != KindError {
						v = Value{Kind: KindError, Bytes: []byte(err.Error())}
					}
				}
			}
			partVals[it.i][it.part] = v
		}
		putReq(sub)
	}
	for gi := 1; gi < len(groups); gi++ {
		wg.Add(1)
		go func(gi int) { defer wg.Done(); do(gi) }(gi)
	}
	do(0)
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			putReq(r)
			return nil, err
		}
	}
	for i := range out {
		if split && plans[i] != nil {
			v, err := plans[i].merge(partVals[i])
			if err != nil && v.Kind != KindError {
				v = Value{Kind: KindError, Bytes: []byte(err.Error())}
			}
			out[i] = v
			continue
		}
		if out[i].Kind == KindError {
			if isRedirectErr(out[i].Bytes) {
				cl.redo(ctx, r.buf[offs[i]:offs[i+1]], &out[i])
			}
		}
	}
	putReq(r)
	return out, nil
}

// mayBlock reports whether a pipeline holds a command that can wait on the server: a blocking
// pop or move, XREAD/XREADGROUP (BLOCK), or WAIT/WAITAOF. Only names starting with B, X or W are
// compared in full; other commands cost one byte test after their name is read.
func mayBlock(buf []byte, offs []int, n int) bool {
	for i := 0; i < n; i++ {
		name := cmdName(buf[offs[i]:offs[i+1]])
		if len(name) == 0 {
			continue
		}
		switch name[0] | 0x20 {
		case 'b', 'x', 'w':
		default:
			continue
		}
		for _, blocking := range blockingNames {
			if upperEq(name, blocking) {
				return true
			}
		}
	}
	return false
}

var blockingNames = [...]string{
	"BLPOP", "BRPOP", "BLMOVE", "BRPOPLPUSH", "BLMPOP", "BZPOPMIN", "BZPOPMAX", "BZMPOP",
	"XREAD", "XREADGROUP", "WAIT", "WAITAOF",
}
