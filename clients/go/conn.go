// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// ErrClosed is returned after Close, or for requests that were pending when the
// connection failed (the cause is wrapped).
var ErrClosed = errors.New("ruvio: connection closed")

const (
	stateWaiting uint32 = iota
	stateDone
	stateAbandoned
)

// request is one queued command (or a pipeline of several). Requests and their
// buffers and done channels are pooled; a request goes back to the pool only after
// both the caller and the reader are finished with it.
type request struct {
	buf    []byte
	val    Value
	vals   []Value
	expect int
	err    error
	state  atomic.Uint32
	done   chan struct{}
	// deadline is when the call fails with context.DeadlineExceeded, in nanoseconds on the
	// clock of mono(); 0 means none. Set by setDeadline, checked by the reader (see expire).
	deadline int64
	// skip > 0 marks a stand-in left in the FIFO for an expired request: it swallows that
	// many late replies.
	skip int
}

var reqPool = sync.Pool{New: func() any {
	return &request{buf: make([]byte, 0, 256), done: make(chan struct{}, 1)}
}}

func getReq() *request {
	r := reqPool.Get().(*request)
	r.expect = 1
	r.state.Store(stateWaiting)
	return r
}

func putReq(r *request) {
	if cap(r.buf) > 64<<10 {
		r.buf = make([]byte, 0, 256)
	} else {
		r.buf = r.buf[:0]
	}
	r.val = Value{}
	r.vals = nil
	r.err = nil
	reqPool.Put(r)
}

// newCmd starts a command frame with n total arguments (name included).
func newCmd(n int, name []byte) *request {
	r := getReq()
	r.buf = appendArrayHeader(r.buf, n)
	r.buf = append(r.buf, name...)
	return r
}

func (r *request) s(v string) *request   { r.buf = appendBulkString(r.buf, v); return r }
func (r *request) b(v []byte) *request   { r.buf = appendBulkBytes(r.buf, v); return r }
func (r *request) i(v int64) *request    { r.buf = appendBulkInt(r.buf, v); return r }
func (r *request) u(v uint64) *request   { r.buf = appendBulkUint(r.buf, v); return r }
func (r *request) f(v float64) *request  { r.buf = appendBulkFloat(r.buf, v); return r }
func (r *request) t(pre []byte) *request { r.buf = append(r.buf, pre...); return r }
func (r *request) ss(v []string) *request {
	for _, s := range v {
		r.buf = appendBulkString(r.buf, s)
	}
	return r
}

// complete is called by the reader (or on failure) exactly once per request.
func (r *request) complete() {
	if r.skip != 0 { // a stand-in for an expired request: nobody waits on it
		return
	}
	if r.state.CompareAndSwap(stateWaiting, stateDone) {
		r.done <- struct{}{}
		return
	}
	// caller abandoned it (context canceled): the reader owns it now.
	putReq(r)
}

// wait blocks for the reply. On a canceled context it abandons the request unless
// the reply already arrived. ok=false means the request must not be touched again.
func (r *request) wait(ctx context.Context) (ok bool, err error) {
	if ctx.Done() == nil {
		<-r.done
		return true, nil
	}
	select {
	case <-r.done:
		return true, nil
	case <-ctx.Done():
		if r.state.CompareAndSwap(stateWaiting, stateAbandoned) {
			return false, ctx.Err()
		}
		<-r.done
		return true, nil
	}
}

// conn is one multiplexed connection.
//
// Writes: callers append encoded frames under mu and push the request onto the FIFO.
// The first caller that finds no write in flight writes inline (serial calls go out at
// once); frames that queue up during that write are flushed together by the writer
// goroutine (coalescing without timers).
//
// Reads: exactly one goroutine owns the read side at a time (the "reading" token,
// guarded by mu). A caller that finds nobody reading takes the token and reads its own
// reply directly (no goroutine hand-off for serial calls); when it is done and other
// requests are still pending it hands the token to the background reader goroutine,
// which keeps resolving the FIFO in order while there is work.
type conn struct {
	nc        net.Conn
	mu        sync.Mutex
	wbuf      []byte
	spare     []byte // owned by whoever holds the writing token
	writing   bool
	reading   bool // a goroutine owns the read side
	permanent bool // the background reader never releases (Pub/Sub, push frames)
	pend      ring
	err       error
	dead      atomic.Bool
	wake      chan struct{}
	rwake     chan struct{}
	quit      chan struct{}
	onPush    func(Value)
	addr      int32                     // index of the dialed address in Client.addrs
	sink      atomic.Pointer[sinkFuncs] // Pub/Sub mode: every reply goes here
	timeout   time.Duration             // Options.CommandTimeout; <= 0 means none
	armed     int64                     // the socket read deadline (mono() clock, 0: none); guarded by mu

	// read state, owned by the holder of the reading token
	rbuf   []byte
	rr, rw int
	p      parser

	bytesWritten atomic.Uint64
	writes       atomic.Uint64
}

func newConn(nc net.Conn, onPush func(Value)) *conn {
	c := &conn{
		nc:     nc,
		wbuf:   make([]byte, 0, 4096),
		spare:  make([]byte, 0, 4096),
		wake:   make(chan struct{}, 1),
		rwake:  make(chan struct{}, 1),
		quit:   make(chan struct{}),
		onPush: onPush,
		rbuf:   make([]byte, 16<<10),
	}
	go c.writeLoop()
	go c.readLoop()
	return c
}

// keepReading makes the background reader own the read side for good (Pub/Sub,
// RESP3 push frames must be read even when no call is waiting).
func (c *conn) keepReading() {
	c.mu.Lock()
	c.permanent = true
	if !c.reading && c.err == nil {
		c.reading = true
		c.signalReader()
	}
	c.mu.Unlock()
}

func (c *conn) signalReader() {
	select {
	case c.rwake <- struct{}{}:
	default:
	}
}

// send enqueues r. lead reports that the caller now owns the read side and must read
// its reply itself (only offered when canLead).
func (c *conn) send(r *request, canLead bool) (lead bool, err error) {
	c.mu.Lock()
	if c.err != nil {
		err := c.err
		c.mu.Unlock()
		return false, err
	}
	c.wbuf = append(c.wbuf, r.buf...)
	c.pend.push(r)
	if r.deadline != 0 && (c.armed == 0 || r.deadline < c.armed || c.armed <= r.deadline-int64(c.timeout)/2) {
		// Armed once per half timeout on a steady stream of calls, not once per call; an earlier
		// wake-up than a request needs is harmless (expire re-arms).
		c.arm(r.deadline)
	}
	if !c.reading {
		c.reading = true
		if canLead {
			lead = true
		} else {
			c.signalReader()
		}
	}
	if c.writing {
		c.mu.Unlock()
		return lead, nil
	}
	c.writing = true
	out := c.wbuf
	c.wbuf = c.spare[:0]
	c.spare = nil
	c.mu.Unlock()
	c.flush(out)
	return lead, nil
}

// sendRaw enqueues bytes that expect no FIFO reply (Pub/Sub mode).
func (c *conn) sendRaw(b []byte) error {
	c.mu.Lock()
	if c.err != nil {
		err := c.err
		c.mu.Unlock()
		return err
	}
	c.wbuf = append(c.wbuf, b...)
	if c.writing {
		c.mu.Unlock()
		return nil
	}
	c.writing = true
	out := c.wbuf
	c.wbuf = c.spare[:0]
	c.spare = nil
	c.mu.Unlock()
	c.flush(out)
	return nil
}

// flush writes out while holding the writing token, then hands the token to the
// writer goroutine when more frames arrived meanwhile.
func (c *conn) flush(out []byte) {
	_, err := c.nc.Write(out)
	c.writes.Add(1)
	c.bytesWritten.Add(uint64(len(out)))
	if cap(out) > 1<<20 {
		out = make([]byte, 0, 4096)
	}
	c.mu.Lock()
	c.spare = out[:0]
	if err != nil {
		c.mu.Unlock()
		c.fail(err)
		return
	}
	if len(c.wbuf) == 0 {
		c.writing = false
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *conn) writeLoop() {
	for {
		select {
		case <-c.wake:
		case <-c.quit:
			return
		}
		c.mu.Lock()
		if c.err != nil || len(c.wbuf) == 0 {
			c.writing = false
			c.mu.Unlock()
			continue
		}
		out := c.wbuf
		c.wbuf = c.spare[:0]
		c.spare = nil
		c.mu.Unlock()
		c.flush(out)
	}
}

// pump delivers every complete reply already buffered; when there is none it does
// one socket read. Only the holder of the reading token calls it.
func (c *conn) pump() error {
	delivered := false
	for c.rr < c.rw {
		v, n, done, err := c.p.next(c.rbuf[c.rr:c.rw])
		c.rr += n
		if err != nil {
			return err
		}
		if !done {
			break
		}
		c.deliver(v)
		delivered = true
	}
	if c.rr == c.rw {
		c.rr, c.rw = 0, 0
		if len(c.rbuf) > 1<<20 {
			c.rbuf = make([]byte, 16<<10) // release a buffer grown by a big reply
		}
	}
	if delivered {
		return nil
	}
	need := c.p.need
	if need < 512 {
		need = 512
	}
	if len(c.rbuf)-c.rw < need {
		if c.rr > 0 {
			copy(c.rbuf, c.rbuf[c.rr:c.rw])
			c.rw -= c.rr
			c.rr = 0
		}
		if len(c.rbuf)-c.rw < need {
			nb := make([]byte, max(2*len(c.rbuf), c.rw+need))
			copy(nb, c.rbuf[:c.rw])
			c.rbuf = nb
		}
	}
	n, err := c.nc.Read(c.rbuf[c.rw:])
	c.rw += n
	if err != nil && n == 0 {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	return nil
}

// releaseRead gives up the reading token, passing it to the background reader when
// requests are still pending.
func (c *conn) releaseRead() {
	c.mu.Lock()
	if c.err == nil && (c.pend.size > 0 || c.permanent) {
		c.signalReader() // the token moves to the background reader
	} else {
		c.reading = false
	}
	c.mu.Unlock()
}

func (c *conn) readLoop() {
	for {
		select {
		case <-c.rwake:
		case <-c.quit:
			return
		}
		for {
			if err := c.pump(); err != nil {
				if errors.Is(err, os.ErrDeadlineExceeded) && !c.dead.Load() {
					c.expire()
					continue
				}
				c.fail(err)
				return
			}
			c.mu.Lock()
			if c.err != nil {
				c.mu.Unlock()
				return
			}
			if c.pend.size == 0 && !c.permanent && c.rr == c.rw {
				c.reading = false
				c.mu.Unlock()
				break
			}
			c.mu.Unlock()
		}
	}
}

// lead reads until r completes. A request past its deadline is failed by expire, which the
// socket read deadline wakes; the connection stays usable.
func (c *conn) lead(r *request) {
	for r.state.Load() == stateWaiting {
		if err := c.pump(); err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) && !c.dead.Load() {
				c.expire()
				continue
			}
			c.fail(err)
			break
		}
	}
	c.releaseRead()
	<-r.done
}

// arm sets the socket read deadline; the caller holds mu.
func (c *conn) arm(deadline int64) {
	c.armed = deadline
	if deadline == 0 {
		c.nc.SetReadDeadline(time.Time{})
		return
	}
	c.nc.SetReadDeadline(epoch.Add(time.Duration(deadline)))
}

// expire runs when the socket read deadline passes: every pending request past its own
// deadline completes with context.DeadlineExceeded, a stand-in takes its place in the FIFO
// to swallow its late reply, and the read deadline moves to the earliest deadline left.
func (c *conn) expire() {
	now := mono()
	next := int64(0)
	c.mu.Lock()
	q := &c.pend
	for i := 0; i < q.size; i++ {
		r := q.buf[(q.head+i)%len(q.buf)]
		if r.deadline == 0 || r.skip != 0 {
			continue
		}
		if r.deadline > now {
			if next == 0 || r.deadline < next {
				next = r.deadline
			}
			continue
		}
		r.deadline = 0
		if r.state.Load() != stateWaiting {
			continue // abandoned by its caller: the reader completes and recycles it as usual
		}
		left := 1
		if r.vals != nil {
			left = r.expect - len(r.vals)
		}
		q.buf[(q.head+i)%len(q.buf)] = &request{skip: left}
		r.err = context.DeadlineExceeded
		r.complete()
	}
	c.arm(next)
	c.mu.Unlock()
}

type sinkFuncs struct {
	value func(Value)
	fail  func(error)
}

func (c *conn) deliver(v Value) {
	if s := c.sink.Load(); s != nil {
		s.value(v)
		return
	}
	if v.Kind == KindPush {
		if c.onPush != nil {
			c.onPush(v)
		}
		return
	}
	c.mu.Lock()
	r := c.pend.peek()
	if r == nil {
		c.mu.Unlock()
		c.fail(ErrProtocol)
		return
	}
	if r.skip != 0 { // the late reply of an expired request
		if r.skip--; r.skip == 0 {
			c.pend.pop()
		}
		c.mu.Unlock()
		return
	}
	if r.vals == nil { // single command (a pipeline always has a non-nil reply slice, even for one command)
		c.pend.pop()
		c.mu.Unlock()
		r.val = v
		r.complete()
		return
	}
	r.vals = append(r.vals, v)
	if len(r.vals) < r.expect {
		c.mu.Unlock()
		return
	}
	c.pend.pop()
	c.mu.Unlock()
	r.complete()
}

func (c *conn) fail(err error) {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return
	}
	if err != ErrClosed {
		err = &connError{err}
	}
	c.err = err
	c.dead.Store(true)
	pend := c.pend.drain()
	c.mu.Unlock()
	close(c.quit)
	c.nc.Close()
	if s := c.sink.Load(); s != nil {
		s.fail(err)
	}
	for _, r := range pend {
		r.err = err
		r.complete()
	}
}

func (c *conn) close() { c.fail(ErrClosed) }

// roundTrip sends r and waits for it, reading the reply directly when nobody else
// is reading. A caller can lead when its context has no cancel channel or has a
// deadline (enforced with the socket read deadline).
func (c *conn) roundTrip(ctx context.Context, r *request) (bool, error) {
	if err := ctx.Err(); err != nil {
		putReq(r)
		return false, err
	}
	c.setDeadline(ctx, r)
	canLead := ctx.Done() == nil
	if !canLead {
		_, canLead = ctx.Deadline()
	}
	lead, err := c.send(r, canLead)
	if err != nil {
		putReq(r)
		return false, err
	}
	if lead {
		c.lead(r)
		return true, nil
	}
	return r.wait(ctx)
}

// setDeadline gives r its deadline: the context's, or else Options.CommandTimeout plus the
// wait of a blocking command (none when it may wait forever). No allocation, no timer: one
// socket read deadline per connection covers every pending request.
func (c *conn) setDeadline(ctx context.Context, r *request) {
	r.deadline = 0
	if dl, has := ctx.Deadline(); has {
		if r.deadline = int64(dl.Sub(epoch)); r.deadline <= 0 {
			r.deadline = 1
		}
		return
	}
	if c.timeout <= 0 {
		return
	}
	limit := c.timeout
	// A blocking command waits its own time on top; one that waits forever has none.
	if w := requestWait(r.buf); w == waitForever {
		return
	} else {
		limit += w
	}
	r.deadline = mono() + int64(limit)
}

// epoch anchors mono(), a monotonic clock in nanoseconds that starts above 0.
var epoch = time.Now().Add(-time.Second)

func mono() int64 { return int64(time.Since(epoch)) }

// exec sends r and waits for its reply. It recycles r.
func (c *conn) exec(ctx context.Context, r *request) (Value, error) {
	ok, err := c.roundTrip(ctx, r)
	if !ok {
		return Value{}, err
	}
	v, err := r.val, r.err
	putReq(r)
	if err == nil && v.Kind == KindError {
		err = &Error{Msg: b2s(v.Bytes)}
	}
	return v, err
}

// execKeep is exec for a caller that may send r again: it does not recycle r, which the
// caller must putReq when kept is true. kept is false when the call was abandoned and the
// reader owns r.
func (c *conn) execKeep(ctx context.Context, r *request) (v Value, kept bool, err error) {
	ok, err := c.roundTrip(ctx, r)
	if !ok {
		return Value{}, false, err
	}
	v, err = r.val, r.err
	if err == nil && v.Kind == KindError {
		err = &Error{Msg: b2s(v.Bytes)}
	}
	return v, true, err
}

// rearm readies a completed request to be sent again.
func (r *request) rearm() {
	r.val = Value{}
	r.vals = nil
	r.err = nil
	r.skip = 0
	r.expect = 1
	r.state.Store(stateWaiting)
}

// execMany sends one frame holding n commands and returns n replies (errors in place).
func (c *conn) execMany(ctx context.Context, r *request, n int) ([]Value, error) {
	r.expect = n
	r.vals = make([]Value, 0, n)
	ok, err := c.roundTrip(ctx, r)
	if !ok {
		return nil, err
	}
	vals, err := r.vals, r.err
	putReq(r)
	return vals, err
}

type connError struct{ err error }

func (e *connError) Error() string   { return "ruvio: connection failed: " + e.err.Error() }
func (e *connError) Unwrap() error   { return e.err }
func (e *connError) Is(t error) bool { return t == ErrClosed }

// ring is a growable FIFO of pending requests.
type ring struct {
	buf        []*request
	head, size int
}

func (q *ring) push(r *request) {
	if q.size == len(q.buf) {
		n := len(q.buf) * 2
		if n == 0 {
			n = 64
		}
		nb := make([]*request, n)
		for i := 0; i < q.size; i++ {
			nb[i] = q.buf[(q.head+i)%len(q.buf)]
		}
		q.buf, q.head = nb, 0
	}
	q.buf[(q.head+q.size)%len(q.buf)] = r
	q.size++
}

func (q *ring) peek() *request {
	if q.size == 0 {
		return nil
	}
	return q.buf[q.head]
}

func (q *ring) pop() {
	q.buf[q.head] = nil
	q.head = (q.head + 1) % len(q.buf)
	q.size--
}

func (q *ring) drain() []*request {
	out := make([]*request, 0, q.size)
	for q.size > 0 {
		out = append(out, q.peek())
		q.pop()
	}
	return out
}
