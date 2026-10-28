// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Options configures a Client. The zero value connects to 127.0.0.1:6379 without
// authentication, on database 0, speaking RESP2.
//
// The connection settings are applied, in this order, on every connection the client
// opens (the shared one, dedicated ones and Pub/Sub ones, also after a reconnect):
// HELLO 3 / AUTH, SELECT, CLIENT SETNAME, then CLIENT DURABILITY.
type Options struct {
	// Addr is host:port. Default 127.0.0.1:6379. With Cluster it is the seed: the
	// address of any shard.
	Addr string
	// Seeds is a list of host:port addresses (IPv6 as "[::1]:6379") used in place of
	// Addr when not empty. Connecting tries them in order, one attempt each, and keeps the
	// first that answers; a seed that cannot be reached is skipped, while a refused AUTH
	// or SELECT stops at once. After the connection drops, the next command's reconnect
	// tries every seed once, starting with the one after the address that dropped. With
	// Cluster, CLUSTER SLOTS comes from the first reachable seed and the seeds are asked
	// again when no known node answers a slot map reload. A malformed entry makes every
	// command fail with an error that names it. Default empty.
	Seeds []string
	// Username and Password send AUTH on every new connection when Password is set.
	// An empty Username authenticates the default user.
	Username string
	// Password is the AUTH password; see Username.
	Password string
	// DB is the logical database selected on every new connection (SELECT). The
	// default is 0. A sharded server only has database 0.
	DB int
	// ClientName sends CLIENT SETNAME on every new connection.
	ClientName string
	// DurabilityReplicas, when above 0, sends CLIENT DURABILITY REPLICAS n on every new
	// connection: the server holds that connection's replies until n replicas have the
	// writes on disk, and a connect fails if the server refuses it (for example without
	// replication). Default 0: nothing is sent.
	DurabilityReplicas int
	// DurabilityTimeout is the TIMEOUT sent with DurabilityReplicas (whole milliseconds).
	// Default 0: no TIMEOUT is sent and the server uses its own (5 seconds).
	DurabilityTimeout time.Duration
	// Protocol 3 sends HELLO 3 on connect; 0 or 2 stays on RESP2 (nothing is sent).
	// RESP3 is needed for ClientTracking and OnPush.
	Protocol int
	// DialTimeout bounds connecting and the handshake when the context has no
	// deadline. Default 5 seconds.
	DialTimeout time.Duration
	// CommandTimeout bounds one command, from sending it to reading its reply, when its
	// context has no deadline; the command then fails with context.DeadlineExceeded and
	// the connection stays usable. Default (0) 30 seconds; a negative value means no
	// limit, so a half-open peer blocks a call made with context.Background() until the
	// connection drops. A blocking command gets its own wait on top (BLPop with 5s may
	// take 5s plus this), and one that waits forever (timeout 0) has no limit. Pub/Sub
	// connections are not bounded.
	CommandTimeout time.Duration
	// Dial overrides the dialer (for TLS, unix sockets, tests). The network argument
	// is always "tcp" and addr is the address being tried: Options.Addr, the seed (with
	// Seeds) or the node address in cluster mode. Set Options.TLS instead of dialing TLS here, unless you need your own dialer.
	Dial func(ctx context.Context, network, addr string) (net.Conn, error)
	// TLS, when not nil, wraps every connection the default dialer opens (first connect,
	// reconnects, pooled and dedicated connections, cluster nodes, replicas and failover
	// probes) in TLS. The server's certificate chain and host name are verified against
	// the config's RootCAs (the system roots when nil) unless the config sets
	// InsecureSkipVerify, which removes the protection TLS gives against someone in the
	// middle and is meant for throwaway tests. ServerName defaults to the host of the
	// address being dialed. Set RootCAs for a private CA and Certificates for a server
	// that demands client certificates. The handshake is part of the connect, so a refused
	// certificate fails the connect. It does not apply when Dial is set: return your own
	// *tls.Conn from Dial then. Default nil: plain TCP.
	TLS *tls.Config
	// MaxIdleDedicated is how many dedicated connections (blocking commands,
	// transactions) are kept for reuse. Default 4.
	MaxIdleDedicated int
	// OnPush receives RESP3 push frames (CLIENT TRACKING invalidations) on the
	// shared connection. It runs on the reader goroutine and must not block. Setting
	// it makes the shared connection read continuously.
	OnPush func(Value)
	// Cluster turns on cluster routing. On first use the client asks the seed (Addr) for
	// CLUSTER SLOTS, builds the slot table and keeps one multiplexed connection per node
	// (connected lazily). A seed that answers "cluster mode is disabled" is a standalone
	// server: the client then behaves exactly as without this option.
	Cluster bool
	// ReadPolicy is where a cluster client sends read-only commands; see ReadPolicy. A
	// standalone connection ignores it. Default ReadPrimary.
	ReadPolicy ReadPolicy
	// DisableFollowPrimary turns off following a failover. By default a standalone client
	// with several Seeds (or a SentinelMaster) asks ROLE to find the node that is the
	// primary: when it reconnects, and after a command is refused with READONLY, in which
	// case the command is sent once more on the connection to the primary. With one address
	// and no SentinelMaster nothing changes, and a cluster client never follows. While the
	// connection is healthy nothing extra is sent. Dedicated connections (Conn, Tx, blocking
	// commands) and the commands of a pipeline are not moved by a READONLY reply.
	DisableFollowPrimary bool
	// SentinelMaster is a master name: Seeds (or Addr) are then coordinators, not data
	// nodes. The client asks them "SENTINEL get-master-addr-by-name <name>", checks the
	// named node with ROLE, and connects to it, on the first connect, after a reconnect and
	// after READONLY. The first connect fails with an error that wraps ErrProtocol and names
	// the master when no coordinator names a primary. AUTH credentials go to coordinators
	// and data nodes alike. Ignored with Cluster or DisableFollowPrimary. Default empty.
	SentinelMaster string
}

// ReadPolicy is where a cluster client sends read-only commands: those the server flags
// readonly and not blocking, such as GET, HGETALL or ZRANGE, including the per-slot parts
// of a split MGET or EXISTS. The replicas of each shard come from CLUSTER SLOTS, which lists
// them after the primary.
//
// Replication is asynchronous: a read from a replica may miss recent writes, including the
// caller's own. Writes, transactions, blocking commands, Pub/Sub, keyless and fan-out
// commands, and any pipeline that holds a command that is not a read always go to the
// primaries.
type ReadPolicy uint8

const (
	// ReadPrimary sends every command to the primary of its slot. This is the default.
	ReadPrimary ReadPolicy = iota
	// ReadPreferReplica sends reads to one replica of the slot's shard, picked at random
	// when the slot map is loaded, and to the primary when the shard has none or the
	// replica cannot be connected or answers LOADING. Such a replica is skipped until the
	// next slot map load.
	ReadPreferReplica
	// ReadReplica sends reads to a replica only: with none known or usable the read fails,
	// and a LOADING reply is returned as the error.
	ReadReplica
)

// Client is a connection to one Ruvio server (or, with Options.Cluster, to a whole
// sharded server). It is safe for concurrent use by many goroutines and meant to be
// created once and shared; do not create one per request. All typed command methods
// (Get, Set, HSet ...) are available on it. Its zero value is not usable; call NewClient
// or Connect.
type Client struct {
	cmds
	opts Options

	cur    atomic.Pointer[conn]
	dialMu sync.Mutex

	idleMu sync.Mutex
	idle   []*conn
	closed atomic.Bool
	db     atomic.Int64

	cl *cluster // non-nil when Options.Cluster is set

	// addrs is the list of addresses to dial: Options.Seeds, or Options.Addr alone. at is
	// the index of the address of the last successful dial, where a dedicated or Pub/Sub
	// connection starts. cfgErr is set when Options.Seeds has a malformed entry.
	addrs  []string
	at     atomic.Int32
	cfgErr error

	// follow is set when the client runs the failover rules (see Options.DisableFollowPrimary);
	// sentinel when addrs are coordinators and the data node comes from them. nodes is the
	// list the dial index points into: addrs plus the primaries found by following (copy on
	// write, guarded by nodesMu), or just the last primary in sentinel mode.
	follow   bool
	sentinel bool
	nodes    atomic.Pointer[[]string]
	nodesMu  sync.Mutex
	inMulti  atomic.Bool // a MULTI sent with Client.Do is open on the shared connection
	done     chan struct{}

	// rounds and the wait between them after a READONLY reply (tests shorten them).
	followRounds   int
	followDelay    time.Duration
	followMaxDelay time.Duration

	// readOnly marks a replica chosen for reads: READONLY is sent on every connection
	// it opens.
	readOnly bool
}

// NewClient returns a Client without connecting. The first command opens the connection,
// and if the connection later drops, the next command opens a new one (there is no
// reconnect option; the command that found the drop fails and is not retried). Defaults:
// Addr "127.0.0.1:6379", DialTimeout 5s, CommandTimeout 30s, MaxIdleDedicated 4. Use Connect to fail fast on
// a wrong address or password.
func NewClient(opts Options) *Client {
	if opts.Addr == "" {
		opts.Addr = "127.0.0.1:6379"
	}
	if opts.DialTimeout == 0 {
		opts.DialTimeout = 5 * time.Second
	}
	if opts.MaxIdleDedicated == 0 {
		opts.MaxIdleDedicated = 4
	}
	if opts.CommandTimeout == 0 {
		opts.CommandTimeout = defaultCommandTimeout
	}
	c := &Client{opts: opts}
	if len(opts.Seeds) > 0 {
		c.cfgErr = checkSeeds(opts.Seeds)
		c.addrs = append([]string(nil), opts.Seeds...)
		c.opts.Addr = c.addrs[0]
		c.opts.Seeds = c.addrs
	} else {
		c.addrs = []string{opts.Addr}
	}
	c.done = make(chan struct{})
	c.followRounds, c.followDelay, c.followMaxDelay = followRounds, followDelay, followMaxDelay
	c.follow = !opts.DisableFollowPrimary && (len(c.addrs) > 1 || opts.SentinelMaster != "")
	c.sentinel = c.follow && !opts.Cluster && opts.SentinelMaster != ""
	if !c.sentinel {
		c.nodes.Store(&c.addrs)
	}
	c.db.Store(int64(opts.DB))
	c.cmds.ex = c
	if opts.Cluster {
		c.cl = newCluster(c)
	}
	return c
}

// checkSeeds reports the first entry of seeds that is not host:port with a port from 1 to
// 65535.
func checkSeeds(seeds []string) error {
	for _, s := range seeds {
		host, port, err := net.SplitHostPort(s)
		if err == nil && host == "" {
			err = errors.New("empty host")
		}
		if err == nil {
			if n, perr := strconv.Atoi(port); perr != nil || n < 1 || n > 65535 {
				err = errors.New("bad port")
			}
		}
		if err != nil {
			return fmt.Errorf("ruvio: invalid seed %q: %w", s, err)
		}
	}
	return nil
}

// connectError is a failure to open a connection (the address could not be reached or
// the handshake broke off), as opposed to a server reply. It prints as the error it wraps.
type connectError struct{ err error }

func (e *connectError) Error() string { return e.err.Error() }
func (e *connectError) Unwrap() error { return e.err }

func isConnectError(err error) bool {
	var ce *connectError
	return errors.As(err, &ce)
}

// Connect is NewClient followed by Ping. It returns the connect, authentication or
// protocol error if the server cannot be reached or the options are rejected, and closes
// the client in that case. ctx bounds the dial and handshake (otherwise
// Options.DialTimeout does).
//
//	c, err := ruvio.Connect(ctx, ruvio.Options{Addr: "10.0.0.5:6379", Password: "secret"})
func Connect(ctx context.Context, opts Options) (*Client, error) {
	c := NewClient(opts)
	if err := c.Ping(ctx); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

// Close closes the shared connection, the idle dedicated connections and, in cluster
// mode, every node connection. Commands that are waiting fail with ErrClosed, as does
// every later command. Pub/Sub, Conn and Tx values obtained earlier keep their own
// connection until they are closed. Calling Close again is a no-op. It always returns
// nil.
func (c *Client) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	close(c.done)
	if cn := c.cur.Swap(nil); cn != nil {
		cn.close()
	}
	if c.cl != nil {
		c.cl.close()
	}
	c.idleMu.Lock()
	for _, cn := range c.idle {
		cn.close()
	}
	c.idle = nil
	c.idleMu.Unlock()
	return nil
}

func (c *Client) shared(ctx context.Context) (*conn, error) {
	if cn := c.cur.Load(); cn != nil && !cn.dead.Load() {
		return cn, nil
	}
	c.dialMu.Lock()
	defer c.dialMu.Unlock()
	if c.closed.Load() {
		return nil, ErrClosed
	}
	if cn := c.cur.Load(); cn != nil && !cn.dead.Load() {
		return cn, nil
	}
	// A dropped connection moves on to the address after the one that dropped.
	start := int(c.at.Load())
	old := c.cur.Load()
	if old != nil {
		start = int(old.addr) + 1
	}
	cn, err := c.dialShared(ctx, start, old != nil)
	if err != nil {
		return nil, err
	}
	c.cur.Store(cn)
	return cn, nil
}

// dial opens a connection starting at the address the last successful dial used.
func (c *Client) dial(ctx context.Context, onPush func(Value)) (*conn, error) {
	return c.dialAt(ctx, onPush, int(c.at.Load()))
}

// dialAt tries the addresses in order from start, wrapping around, one attempt each. An
// address that cannot be reached is skipped; a refused handshake step is returned at once.
// With one address (the default) this is a single attempt. In cluster mode the seed client
// is one node and never moves to another address; the cluster asks the other seeds itself.
func (c *Client) dialAt(ctx context.Context, onPush func(Value), start int) (*conn, error) {
	if c.cfgErr != nil {
		return nil, c.cfgErr
	}
	if c.cl != nil && !c.cl.standalone.Load() {
		return c.dialAddr(ctx, onPush, 0, c.opts.Addr)
	}
	nodes := c.nodeList()
	n := len(nodes)
	if n == 0 { // sentinel mode, no primary known yet
		return c.dialPrimary(ctx, onPush)
	}
	if n == 1 {
		return c.dialAddr(ctx, onPush, 0, nodes[0])
	}
	start %= n
	var last error
	for step := 0; step < n; step++ {
		i := (start + step) % n
		cn, err := c.dialAddr(ctx, onPush, i, nodes[i])
		if err == nil {
			return cn, nil
		}
		if !isConnectError(err) || ctx.Err() != nil {
			return nil, err
		}
		last = err
	}
	return nil, last
}

// transport opens the network connection to addr: Options.Dial when set, otherwise TCP, wrapped in TLS
// when Options.TLS is set.
func (c *Client) transport(ctx context.Context, addr string) (net.Conn, error) {
	if c.opts.Dial != nil {
		return c.opts.Dial(ctx, "tcp", addr)
	}
	if c.opts.TLS != nil {
		d := tls.Dialer{Config: c.opts.TLS}
		return d.DialContext(ctx, "tcp", addr)
	}
	var d net.Dialer
	return d.DialContext(ctx, "tcp", addr)
}

func (c *Client) dialAddr(ctx context.Context, onPush func(Value), idx int, addr string) (*conn, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.opts.DialTimeout)
		defer cancel()
	}
	nc, err := c.transport(ctx, addr)
	if err != nil {
		return nil, &connectError{err}
	}
	if tc, ok := nc.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	cn := newConn(nc, onPush)
	cn.timeout = c.opts.CommandTimeout
	cn.addr = int32(idx)
	if onPush != nil || c.opts.Protocol == 3 {
		cn.keepReading() // push frames can arrive with no call pending
	}
	if err := c.handshake(ctx, cn); err != nil {
		cn.close()
		if isServerErr(err) {
			return nil, err
		}
		return nil, &connectError{err}
	}
	c.at.Store(int32(idx))
	return cn, nil
}

func durabilityReq(o Options) *request {
	if o.DurabilityTimeout > 0 {
		return newCmd(6, cCLIENT).t(tDURABILITY).t(tREPLICAS).i(int64(o.DurabilityReplicas)).t(tTIMEOUT).i(ms(o.DurabilityTimeout))
	}
	return newCmd(4, cCLIENT).t(tDURABILITY).t(tREPLICAS).i(int64(o.DurabilityReplicas))
}

func (c *Client) handshake(ctx context.Context, cn *conn) error {
	o := c.opts
	if o.Protocol == 3 {
		n := 2
		if o.Password != "" {
			n += 3
		}
		r := newCmd(n, cHELLO).s("3")
		if o.Password != "" {
			user := o.Username
			if user == "" {
				user = "default"
			}
			r.t(tAUTH).s(user).s(o.Password)
		}
		if _, err := cn.exec(ctx, r); err != nil {
			return err
		}
	} else if o.Password != "" {
		var r *request
		if o.Username != "" {
			r = newCmd(3, cAUTH).s(o.Username).s(o.Password)
		} else {
			r = newCmd(2, cAUTH).s(o.Password)
		}
		if _, err := cn.exec(ctx, r); err != nil {
			return err
		}
	}
	if db := c.db.Load(); db != 0 {
		if _, err := cn.exec(ctx, newCmd(2, cSELECT).i(db)); err != nil {
			return err
		}
	}
	if o.ClientName != "" {
		if _, err := cn.exec(ctx, newCmd(3, cCLIENT).t(tSETNAME).s(o.ClientName)); err != nil {
			return err
		}
	}
	if o.DurabilityReplicas > 0 {
		if _, err := cn.exec(ctx, durabilityReq(o)); err != nil {
			return err
		}
	}
	if c.readOnly {
		if _, err := cn.exec(ctx, newCmd(1, cREADONLY)); err != nil {
			return err
		}
	}
	return nil
}

// exec implements executor: the shared connection, or the owning node in cluster mode.
func (c *Client) exec(ctx context.Context, r *request) (Value, error) {
	if c.cl != nil {
		return c.cl.exec(ctx, r)
	}
	return c.execLocal(ctx, r)
}

func (c *Client) execLocal(ctx context.Context, r *request) (Value, error) {
	cn, err := c.shared(ctx)
	if err != nil {
		putReq(r)
		return Value{}, err
	}
	if c.follow && c.following() {
		return c.execFollow(ctx, cn, r)
	}
	return cn.exec(ctx, r)
}

func (c *Client) execMany(ctx context.Context, r *request, n int) ([]Value, error) {
	if c.cl != nil {
		return c.cl.execMany(ctx, r, n)
	}
	return c.execManyLocal(ctx, r, n)
}

// execBlocking runs a command that may block the server-side connection on a
// dedicated connection, so the shared one keeps flowing.
func (c *Client) execBlocking(ctx context.Context, r *request) (Value, error) {
	if c.cl != nil {
		return c.cl.execBlocking(ctx, r)
	}
	return c.execBlockingLocal(ctx, r)
}

func (c *Client) execBlockingLocal(ctx context.Context, r *request) (Value, error) {
	cn, err := c.acquire(ctx)
	if err != nil {
		putReq(r)
		return Value{}, err
	}
	v, err := cn.exec(ctx, r)
	if err != nil && !isServerErr(err) {
		cn.close() // the reply may still be in flight: never reuse this connection
	} else {
		c.release(cn)
	}
	return v, err
}

func isServerErr(err error) bool {
	var e *Error
	return errors.As(err, &e)
}

func (c *Client) acquire(ctx context.Context) (*conn, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	c.idleMu.Lock()
	for len(c.idle) > 0 {
		cn := c.idle[len(c.idle)-1]
		c.idle = c.idle[:len(c.idle)-1]
		if !cn.dead.Load() {
			c.idleMu.Unlock()
			return cn, nil
		}
	}
	c.idleMu.Unlock()
	return c.dial(ctx, nil)
}

func (c *Client) release(cn *conn) {
	if cn.dead.Load() || c.closed.Load() {
		cn.close()
		return
	}
	c.idleMu.Lock()
	if len(c.idle) < c.opts.MaxIdleDedicated {
		c.idle = append(c.idle, cn)
		cn = nil
	}
	c.idleMu.Unlock()
	if cn != nil {
		cn.close()
	}
}

// Do sends any command and returns the raw reply, for commands that have no typed
// method. Arguments may be string, []byte, int, int32, int64, uint, uint32, uint64,
// float32, float64 or bool; nil is sent as an empty string. Bools are sent as "1" and
// "0", floats in plain decimal ("+inf" and "-inf" for infinities). Any other type
// returns an error before anything is sent. A server error is returned as *Error and the
// Value then has Kind KindError.
//
//	v, err := c.Do(ctx, "OBJECT", "ENCODING", "mykey")
//	s := v.Str()
func (c *Client) Do(ctx context.Context, args ...any) (Value, error) {
	v, err := c.cmds.Do(ctx, args...)
	if c.follow {
		c.trackMulti(args, v, err)
	}
	return v, err
}

// Stats are transport counters of the shared connection. Because frames that queue up
// while a write is in flight leave in one write, Writes is usually smaller than the
// number of commands sent. In cluster mode the counters add up over all node
// connections.
type Stats struct {
	// Writes is the number of socket write calls made.
	Writes uint64
	// BytesWritten is the number of bytes written to the socket.
	BytesWritten uint64
}

// Stats returns the transport counters of the current shared connection. They restart
// from zero when the connection is replaced. It returns the zero value before the first
// command.
func (c *Client) Stats() Stats {
	if c.cl != nil {
		return c.cl.stats()
	}
	cn := c.cur.Load()
	if cn == nil {
		return Stats{}
	}
	return Stats{Writes: cn.writes.Load(), BytesWritten: cn.bytesWritten.Load()}
}

// Conn is a dedicated connection taken from a Client with Client.Conn. Use it for
// commands that change connection state or must run on one connection: SELECT for one
// caller, WATCH, CLIENT TRACKING, or a series of commands that must not interleave with
// other goroutines. All typed command methods and Pipeline are available. A Conn is not
// safe for concurrent use; always Close it, which returns the connection to the client.
//
// In cluster mode a Conn is taken from the shard that owns the first key it uses and
// every later key must hash to the same slot, otherwise the call fails with
// ErrCrossSlot. Use Client.ConnFor to choose the shard up front.
type Conn struct {
	cmds
	c     *Client
	cn    *conn
	dirty bool

	// cluster mode: the connection is taken from the node owning the first key it sees
	// and every later key must hash to the same slot.
	cl         *cluster
	slot       int  // bound slot, -1 when not known yet
	deferMulti bool // MULTI sent before the node was known; it goes out with the first keyed command
	closed     bool
}

// Conn takes a dedicated connection (see the Conn type). It opens a new connection when
// no idle one is available. In cluster mode the connection is taken lazily, from the
// shard of the first key. Close the returned Conn when done.
//
//	d, err := c.Conn(ctx)
//	if err != nil {
//	    return err
//	}
//	defer d.Close()
//	d.Select(ctx, 2)
func (c *Client) Conn(ctx context.Context) (*Conn, error) {
	if c.cl != nil {
		t, err := c.cl.topo(ctx)
		if err != nil {
			return nil, err
		}
		if t != nil {
			d := &Conn{cl: c.cl, slot: -1}
			d.cmds.ex = d
			return d, nil
		}
	}
	cn, err := c.acquire(ctx)
	if err != nil {
		return nil, err
	}
	d := &Conn{c: c, cn: cn}
	d.cmds.ex = d
	return d, nil
}

// Close returns the connection to the client for reuse. If Select, Hello or another
// command changed the database or protocol of the connection (it is marked dirty), the
// connection is closed instead of being reused. Further use of the Conn returns
// ErrClosed. It always returns nil.
func (d *Conn) Close() error {
	d.closed = true
	if d.cn == nil {
		return nil
	}
	if d.dirty {
		d.cn.close()
	} else {
		d.c.release(d.cn)
	}
	d.cn = nil
	return nil
}

func (d *Conn) exec(ctx context.Context, r *request) (Value, error) {
	if d.cl != nil {
		if v, done, err := d.bind(ctx, r, 1); done {
			return v, err
		}
	}
	if d.cn == nil {
		putReq(r)
		return Value{}, ErrClosed
	}
	return d.cn.exec(ctx, r)
}

func (d *Conn) execMany(ctx context.Context, r *request, n int) ([]Value, error) {
	if d.cl != nil {
		if _, done, err := d.bind(ctx, r, n); done {
			return nil, err
		}
	}
	if d.cn == nil {
		putReq(r)
		return nil, ErrClosed
	}
	return d.cn.execMany(ctx, r, n)
}

func (d *Conn) execBlocking(ctx context.Context, r *request) (Value, error) { return d.exec(ctx, r) }

func (d *Conn) selected(db int64) { d.dirty = true }
func (c *Client) selected(db int64) {
	c.db.Store(db)
	c.idleMu.Lock()
	idle := c.idle
	c.idle = nil
	c.idleMu.Unlock()
	for _, cn := range idle {
		cn.close()
	}
}
