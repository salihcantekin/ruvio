// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// liveCluster connects with cluster routing to RUVIO_CLUSTER_ADDR (a 3-shard, per-shard
// port server) and empties every node, or skips.
func liveCluster(t testing.TB) (*Client, context.Context) {
	t.Helper()
	addr := os.Getenv("RUVIO_CLUSTER_ADDR")
	if addr == "" {
		t.Skip("RUVIO_CLUSTER_ADDR not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	c, err := Connect(ctx, Options{Addr: addr, Cluster: true})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if !c.IsCluster() {
		t.Fatal("server is not in cluster mode")
	}
	noErr(t, c.FlushDB(ctx))
	t.Cleanup(func() { c.Close(); cancel() })
	return c, ctx
}

// nodeSizes counts keys per node by asking every node directly.
func nodeSizes(t testing.TB, ctx context.Context, c *Client) []int64 {
	t.Helper()
	tp := c.cl.t.Load()
	out := make([]int64, len(tp.nodes))
	for i, n := range tp.nodes {
		out[i] = must[int64](t)(intOf(n.execLocal(ctx, newCmd(1, cDBSIZE))))
	}
	return out
}

// tagFor finds a hashtag whose slot is owned by node index i.
func tagFor(c *Client, i int) string {
	tp := c.cl.t.Load()
	for n := 0; ; n++ {
		tag := fmt.Sprintf("tg%d", n)
		if int(tp.owner[KeySlot(tag)]) == i {
			return tag
		}
	}
}

func TestClusterLiveTopologyAndSlots(t *testing.T) {
	c, ctx := liveCluster(t)
	if n := c.Nodes(); len(n) != 3 {
		t.Fatalf("nodes %v", n)
	}
	for _, k := range []string{"foo", "bar", "{user1}.x", "a{}b", "héllo", "k:12345"} {
		eq(t, must[int64](t)(c.ClusterKeySlot(ctx, k)), int64(KeySlot(k)))
	}
	eq(t, KeySlot("foo"), 12182)
	// the CLUSTER introspection methods decode on a real cluster
	sl := must[Value](t)(c.ClusterSlots(ctx))
	eq(t, len(sl.Array), 3)
	if nodes := must[string](t)(c.ClusterNodes(ctx)); len(nodes) == 0 {
		t.Fatal("empty CLUSTER NODES")
	}
	if info := must[string](t)(c.ClusterInfo(ctx)); len(info) == 0 {
		t.Fatal("empty CLUSTER INFO")
	}
	if id := must[string](t)(c.ClusterMyID(ctx)); id == "" {
		t.Fatal("empty CLUSTER MYID")
	}
	if sh := must[Value](t)(c.ClusterShards(ctx)); len(sh.Array) == 0 {
		t.Fatal("empty CLUSTER SHARDS")
	}
}

func TestClusterLiveKeysSpreadOverAllShards(t *testing.T) {
	c, ctx := liveCluster(t)
	const n = 300
	for i := 0; i < n; i++ {
		noErr(t, c.Set(ctx, fmt.Sprintf("spread:%d", i), fmt.Sprint(i)))
	}
	sizes := nodeSizes(t, ctx, c)
	var sum int64
	for i, s := range sizes {
		if s == 0 {
			t.Fatalf("node %d got no keys: %v", i, sizes)
		}
		sum += s
	}
	eq(t, sum, int64(n))
	eq(t, must[int64](t)(c.DBSize(ctx)), int64(n)) // fan-out sum
	for i := 0; i < n; i++ {
		v, ok, err := c.Get(ctx, fmt.Sprintf("spread:%d", i))
		if err != nil || !ok || v != fmt.Sprint(i) {
			t.Fatalf("spread:%d = %q %v %v", i, v, ok, err)
		}
	}
	ks := must[[]string](t)(c.Keys(ctx, "spread:*"))
	eq(t, len(ks), n)
}

func TestClusterLiveCrossSlotMultiKey(t *testing.T) {
	c, ctx := liveCluster(t)
	const n = 120
	keys := make([]string, n)
	var kv []string
	for i := range keys {
		keys[i] = fmt.Sprintf("multi:%d", i)
		kv = append(kv, keys[i], fmt.Sprintf("val-%d", i))
	}
	noErr(t, c.MSet(ctx, kv...))
	sz := nodeSizes(t, ctx, c)
	for _, s := range sz {
		if s == 0 {
			t.Fatalf("MSET did not reach every node: %v", sz)
		}
	}
	// MGET in a shuffled order with missing keys mixed in
	rng := rand.New(rand.NewSource(7))
	ask := append([]string(nil), keys...)
	ask = append(ask, "multi:missing1", "multi:missing2")
	rng.Shuffle(len(ask), func(i, j int) { ask[i], ask[j] = ask[j], ask[i] })
	got := must[[]NullString](t)(c.MGet(ctx, ask...))
	eq(t, len(got), len(ask))
	for i, k := range ask {
		var want NullString
		if k != "multi:missing1" && k != "multi:missing2" {
			want = NullString{Val: "val-" + k[len("multi:"):], Valid: true}
		}
		if got[i] != want {
			t.Fatalf("MGET position %d (%s) = %+v, want %+v", i, k, got[i], want)
		}
	}
	// EXISTS and TOUCH count across nodes, duplicates included
	eq(t, must[int64](t)(c.Exists(ctx, ask...)), int64(n))
	eq(t, must[int64](t)(c.Touch(ctx, keys[:50]...)), int64(50))
	eq(t, must[int64](t)(c.Exists(ctx, keys[0], keys[0], "multi:missing1")), int64(2))
	eq(t, must[int64](t)(c.Unlink(ctx, keys[:40]...)), int64(40))
	eq(t, must[int64](t)(c.Del(ctx, ask...)), int64(n-40))
	eq(t, must[int64](t)(c.DBSize(ctx)), int64(0))
}

func TestClusterLiveCrossSlotOthersFailFast(t *testing.T) {
	c, ctx := liveCluster(t)
	a, b := "{p}one", "{q}two"
	noErr(t, c.Set(ctx, a, "1"))
	noErr(t, c.Set(ctx, b, "2"))
	if KeySlot(a) == KeySlot(b) {
		t.Fatal("test keys must differ in slot")
	}
	_, err := c.SUnion(ctx, a, b)
	if !errors.Is(err, ErrCrossSlot) {
		t.Fatalf("SUNION: %v", err)
	}
	_, err = c.SUnionStore(ctx, "{p}dst", a, b)
	if !errors.Is(err, ErrCrossSlot) {
		t.Fatalf("SUNIONSTORE: %v", err)
	}
	if err = c.Rename(ctx, a, b); !errors.Is(err, ErrCrossSlot) {
		t.Fatalf("RENAME: %v", err)
	}
	_, err = c.BitOp(ctx, "AND", "{p}d", a, b)
	if !errors.Is(err, ErrCrossSlot) {
		t.Fatalf("BITOP: %v", err)
	}
	_, err = c.MSetNX(ctx, a, "x", b, "y")
	if !errors.Is(err, ErrCrossSlot) {
		t.Fatalf("MSETNX: %v", err)
	}
	// nothing was sent: the data is untouched
	eq(t, must[string](t)(strOf(c.Get(ctx, a))), "1")
	eq(t, must[string](t)(strOf(c.Get(ctx, b))), "2")
	eq(t, must[int64](t)(c.DBSize(ctx)), int64(2))
}

func intOf(v Value, err error) (int64, error) { return v.Int, err }

func strOf(v string, ok bool, err error) (string, error) { return v, err }

func TestClusterLivePipelineOrder(t *testing.T) {
	c, ctx := liveCluster(t)
	const n = 400
	vals, err := c.Pipelined(ctx, func(p *Pipeline) {
		for i := 0; i < n; i++ {
			p.Set(fmt.Sprintf("pl:%d", i), fmt.Sprint(i))
			if i%50 == 0 {
				p.Do("PING")
			}
		}
		for i := 0; i < n; i++ {
			p.Get(fmt.Sprintf("pl:%d", i))
			p.Incr(fmt.Sprintf("ctr:%d", i%7))
		}
	})
	noErr(t, err)
	pos := 0
	for i := 0; i < n; i++ {
		eq(t, vals[pos].Str(), "OK")
		pos++
		if i%50 == 0 {
			eq(t, vals[pos].Str(), "PONG")
			pos++
		}
	}
	ctr := map[int]int64{}
	for i := 0; i < n; i++ {
		eq(t, vals[pos].Str(), fmt.Sprint(i))
		ctr[i%7]++
		eq(t, vals[pos+1].Int, ctr[i%7])
		pos += 2
	}
	eq(t, pos, len(vals))
	for _, s := range nodeSizes(t, ctx, c) {
		if s == 0 {
			t.Fatal("pipeline did not reach every node")
		}
	}
}

func TestClusterLivePipelineSplitsCrossSlot(t *testing.T) {
	c, ctx := liveCluster(t)
	var keys []string
	var kv []any
	for i := 0; i < 30; i++ {
		keys = append(keys, fmt.Sprintf("ps:%d", i))
		kv = append(kv, keys[i], fmt.Sprint(i))
	}
	vals, err := c.Pipelined(ctx, func(p *Pipeline) {
		p.Do(append([]any{"MSET"}, kv...)...)
		p.Incr("ps:ctr")
		p.Do(append([]any{"MGET", "ps:missing"}, toAny(keys)...)...)
		p.Do(append([]any{"EXISTS"}, toAny(keys)...)...)
		p.Del(keys[:10]...)
		p.Do(append([]any{"UNLINK"}, toAny(keys[10:20])...)...)
		p.Do(append([]any{"TOUCH"}, toAny(keys)...)...)
		p.Get(keys[25])
	})
	noErr(t, err)
	eq(t, len(vals), 8)
	eq(t, vals[0].Str(), "OK")
	eq(t, vals[1].Int, int64(1))
	eq(t, len(vals[2].Array), len(keys)+1)
	eq(t, vals[2].Array[0].Kind, KindNil)
	for i := range keys {
		eq(t, vals[2].Array[i+1].Str(), fmt.Sprint(i))
	}
	eq(t, vals[3].Int, int64(30))
	eq(t, vals[4].Int, int64(10))
	eq(t, vals[5].Int, int64(10))
	eq(t, vals[6].Int, int64(10))
	eq(t, vals[7].Str(), "25")
	eq(t, must[int64](t)(c.DBSize(ctx)), int64(11))
	// with a stale table every part is redirected and still lands at its index
	staleTable(c)
	vals, err = c.Pipelined(ctx, func(p *Pipeline) {
		p.Do(append([]any{"MGET"}, toAny(keys[20:])...)...)
		p.Get("ps:ctr")
	})
	noErr(t, err)
	for i, v := range vals[0].Array {
		eq(t, v.Str(), fmt.Sprint(20+i))
	}
	eq(t, vals[1].Str(), "1")
}

// staleTable rotates the owner of every slot so each key first goes to the wrong node.
func staleTable(c *Client) {
	cl := c.cl
	cl.mu.Lock()
	defer cl.mu.Unlock()
	t := *cl.t.Load()
	nn := uint16(len(t.nodes))
	for i := range t.owner {
		t.owner[i] = (t.owner[i] + 1) % nn
	}
	cl.t.Store(&t)
}

func TestClusterLiveMovedRecovery(t *testing.T) {
	c, ctx := liveCluster(t)
	for i := 0; i < 30; i++ {
		noErr(t, c.Set(ctx, fmt.Sprintf("mv:%d", i), fmt.Sprint(i)))
	}
	slot, other := KeySlot("mv:0"), KeySlot("mv:1")
	truth, otherTruth := c.cl.t.Load().owner[slot], c.cl.t.Load().owner[other]
	staleTable(c)
	if c.cl.t.Load().owner[slot] == truth {
		t.Fatal("test setup: table not stale")
	}
	// Ruvio forwards a misrouted key to its owner instead of answering MOVED, so a stale
	// table still gets the right value (MOVED handling itself is covered by the fake-server
	// unit tests); the table is not patched because nothing told the client it was wrong.
	_, _ = truth, otherTruth
	eq(t, must[string](t)(strOf(c.Get(ctx, "mv:0"))), "0")
	for i := 0; i < 30; i++ {
		eq(t, must[string](t)(strOf(c.Get(ctx, fmt.Sprintf("mv:%d", i)))), fmt.Sprint(i))
	}
	// stale table again: writes, split MGET, pipeline and DEL all recover
	staleTable(c)
	noErr(t, c.Set(ctx, "mv:w", "w"))
	staleTable(c)
	var keys []string
	for i := 0; i < 30; i++ {
		keys = append(keys, fmt.Sprintf("mv:%d", i))
	}
	got := must[[]NullString](t)(c.MGet(ctx, keys...))
	for i := range keys {
		eq(t, got[i], NullString{Val: fmt.Sprint(i), Valid: true})
	}
	staleTable(c)
	vals, err := c.Pipelined(ctx, func(p *Pipeline) {
		for _, k := range keys {
			p.Get(k)
		}
	})
	noErr(t, err)
	for i := range keys {
		eq(t, vals[i].Str(), fmt.Sprint(i))
	}
	staleTable(c)
	eq(t, must[int64](t)(c.Del(ctx, keys...)), int64(30))
	// an explicit refresh restores the true table
	staleTable(c)
	noErr(t, c.RefreshSlots(ctx))
	eq(t, must[string](t)(strOf(c.Get(ctx, "mv:w"))), "w")
}

func TestClusterLiveTransactions(t *testing.T) {
	c, ctx := liveCluster(t)
	for i := 0; i < 3; i++ { // one tag per node
		tag := tagFor(c, i)
		k1, k2 := "{"+tag+"}a", "{"+tag+"}b"
		res, err := c.Transaction(ctx, func(p *Pipeline) {
			p.Set(k1, "1")
			p.Incr(k2)
			p.Incr(k2)
		})
		noErr(t, err)
		eq(t, len(res), 3)
		eq(t, res[2].Int, int64(2))
		eq(t, must[string](t)(strOf(c.Get(ctx, k1))), "1")

		// WATCH + EXEC, explicit
		tx, err := c.Tx(ctx)
		noErr(t, err)
		noErr(t, tx.Watch(ctx, k1, k2))
		res, err = tx.Exec(ctx, func(p *Pipeline) { p.Incr(k1); p.Incr(k2) })
		noErr(t, err)
		eq(t, res[1].Int, int64(3))
		noErr(t, tx.Close())

		// a watched key changed by another connection aborts the transaction
		tx, err = c.Tx(ctx)
		noErr(t, err)
		noErr(t, tx.Watch(ctx, k1))
		must[string](t)(strOf(c.Get(ctx, k1)))
		if _, err := c.Incr(ctx, k1); err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(ctx, func(p *Pipeline) { p.Set(k1, "lost") })
		if !errors.Is(err, ErrTxAborted) {
			t.Fatalf("want ErrTxAborted, got %v", err)
		}
		noErr(t, tx.Close())

		// Multi / Queue / ExecQueued with the node learned from the first key
		tx, err = c.Tx(ctx)
		noErr(t, err)
		noErr(t, tx.Multi(ctx))
		noErr(t, tx.Queue(ctx, "SET", k1, "q"))
		noErr(t, tx.Queue(ctx, "INCR", k2))
		res, err = tx.ExecQueued(ctx)
		noErr(t, err)
		eq(t, len(res), 2)
		noErr(t, tx.Close())
		eq(t, must[string](t)(strOf(c.Get(ctx, k1))), "q")
	}
}

func TestClusterLiveTransactionCrossSlotFails(t *testing.T) {
	c, ctx := liveCluster(t)
	a, b := "{xa}k", "{xb}k"
	_, err := c.Transaction(ctx, func(p *Pipeline) { p.Set(a, "1"); p.Set(b, "2") })
	if !errors.Is(err, ErrCrossSlot) {
		t.Fatalf("Transaction: %v", err)
	}
	tx, err := c.Tx(ctx)
	noErr(t, err)
	err = tx.Watch(ctx, a, b)
	if !errors.Is(err, ErrCrossSlot) {
		t.Fatalf("Watch: %v", err)
	}
	noErr(t, tx.Watch(ctx, a))
	if _, err = tx.Exec(ctx, func(p *Pipeline) { p.Set(b, "2") }); !errors.Is(err, ErrCrossSlot) {
		t.Fatalf("Exec on another slot than the watch: %v", err)
	}
	noErr(t, tx.Close())
	eq(t, must[int64](t)(c.Exists(ctx, a, b)), int64(0))
	// a pipeline carrying MULTI across slots
	_, err = c.Pipelined(ctx, func(p *Pipeline) { p.Do("MULTI"); p.Set(a, "1"); p.Set(b, "2"); p.Do("EXEC") })
	if !errors.Is(err, ErrCrossSlot) {
		t.Fatalf("pipeline MULTI: %v", err)
	}
	eq(t, must[int64](t)(c.Exists(ctx, a, b)), int64(0))
}

func TestClusterLiveHelpersWorkUnchanged(t *testing.T) {
	c, ctx := liveCluster(t)
	for i := 0; i < 3; i++ {
		tag := "{" + tagFor(c, i) + "}"
		// lock
		l, ok, err := c.AcquireLock(ctx, tag+"lock", 5*time.Second)
		noErr(t, err)
		if !ok {
			t.Fatal("lock not acquired")
		}
		_, ok, _ = c.AcquireLock(ctx, tag+"lock", 5*time.Second)
		if ok {
			t.Fatal("lock acquired twice")
		}
		ok, err = l.Refresh(ctx, 5*time.Second)
		noErr(t, err)
		eq(t, ok, true)
		ok, err = l.Unlock(ctx)
		noErr(t, err)
		eq(t, ok, true)
		noErr(t, c.WithLock(ctx, tag+"lock", time.Second, func(context.Context) error { return nil }))
		// lease + permit
		noErr(t, c.WithLease(ctx, tag+"lease", time.Second, func(_ context.Context, tok int64) error {
			if tok <= 0 {
				return errors.New("no fence token")
			}
			return nil
		}))
		p, ok, err := c.AcquirePermit(ctx, tag+"sem", 2, time.Second)
		noErr(t, err)
		eq(t, ok, true)
		_, err = p.Release(ctx)
		noErr(t, err)
		// rate limit
		var allowed int
		for j := 0; j < 5; j++ {
			ok, _, err := c.RateLimit(ctx, tag+"rl", 3, time.Minute)
			noErr(t, err)
			if ok {
				allowed++
			}
		}
		eq(t, allowed, 3)
		// getOrCreate (stampede-safe fill)
		calls := 0
		for j := 0; j < 2; j++ {
			v, err := c.GetOrCreate(ctx, tag+"goc", time.Minute, func(context.Context) (string, error) { calls++; return "made", nil })
			noErr(t, err)
			eq(t, v, "made")
		}
		eq(t, calls, 1)
		// idempotency
		runs := 0
		for j := 0; j < 2; j++ {
			out, err := c.Idempotent(ctx, tag+"idem", "fp", time.Minute, func(context.Context) ([]byte, error) { runs++; return []byte("r"), nil })
			noErr(t, err)
			eq(t, string(out), "r")
		}
		eq(t, runs, 1)
		// capped list and bloom
		for j := 0; j < 5; j++ {
			_, err = c.PushRecent(ctx, tag+"recent", 3, fmt.Sprint(j))
			noErr(t, err)
		}
		eq(t, len(must[[]string](t)(c.Recent(ctx, tag+"recent", 10))), 3)
		seen := must[[]bool](t)(c.SeenBefore(ctx, tag+"bf", "x", "y"))
		eq(t, seen, []bool{false, false})
		seen = must[[]bool](t)(c.SeenBefore(ctx, tag+"bf", "x"))
		eq(t, seen, []bool{true})
		// typed JSON
		type doc struct{ N int }
		noErr(t, c.JSONSetValue(ctx, tag+"doc", "$", doc{N: 4}))
		var d doc
		ok, err = c.JSONGetValue(ctx, tag+"doc", "$", &d)
		noErr(t, err)
		eq(t, ok, true)
		eq(t, d.N, 4)
	}
}

func TestClusterLiveKeyPositionsOnEveryNode(t *testing.T) {
	c, ctx := liveCluster(t)
	for i := 0; i < 3; i++ {
		g := "{" + tagFor(c, i) + "}"
		do := func(args ...any) Value {
			t.Helper()
			v, err := c.Do(ctx, args...)
			if err != nil {
				t.Fatalf("node %d %v: %v", i, args[0], err)
			}
			return v
		}
		do("SADD", g+"s1", "a", "b")
		do("SADD", g+"s2", "b", "c")
		eq(t, do("SINTERCARD", 2, g+"s1", g+"s2").Int, int64(1))
		eq(t, len(do("SUNION", g+"s1", g+"s2").Array), 3)
		do("SUNIONSTORE", g+"sd", g+"s1", g+"s2")
		do("SMOVE", g+"s1", g+"s2", "a")
		do("ZADD", g+"z1", 1, "a", 2, "b")
		do("ZADD", g+"z2", 1, "b")
		eq(t, do("ZUNIONSTORE", g+"zd", 2, g+"z1", g+"z2").Int, int64(2))
		eq(t, do("ZINTERSTORE", g+"zd2", 2, g+"z1", g+"z2").Int, int64(1))
		eq(t, len(do("ZUNION", 2, g+"z1", g+"z2").Array), 2)
		do("RPUSH", g+"l1", "x", "y")
		do("RPUSH", g+"l2", "z")
		do("LMOVE", g+"l1", g+"l2", "LEFT", "RIGHT")
		do("RPOPLPUSH", g+"l1", g+"l2")
		do("RPUSH", g+"l3", "p")
		eq(t, do("LMPOP", 2, g+"l3", g+"l4", "LEFT").Array[0].Str(), g+"l3")
		do("SET", g+"b1", "foobar")
		do("SET", g+"b2", "abcdef")
		eq(t, do("BITOP", "AND", g+"bd", g+"b1", g+"b2").Int, int64(6))
		do("PFADD", g+"h1", "a", "b")
		do("PFADD", g+"h2", "c")
		do("PFMERGE", g+"hd", g+"h1", g+"h2")
		eq(t, do("PFCOUNT", g+"h1", g+"h2").Int, int64(3))
		eq(t, do("EVAL", "return ruvio.call('SET', KEYS[1], ARGV[1])", 1, g+"ev", "1").Str(), "OK")
		do("COPY", g+"ev", g+"ev2")
		do("RENAME", g+"ev2", g+"ev3")
		do("RENAMENX", g+"ev3", g+"ev4")
		eq(t, do("OBJECT", "ENCODING", g+"ev").Str() != "", true)
		eq(t, do("MEMORY", "USAGE", g+"ev").Int > 0, true)
		do("XADD", g+"st", "*", "f", "v")
		do("XGROUP", "CREATE", g+"st", "grp", "0")
		eq(t, do("XINFO", "GROUPS", g+"st").Kind != KindNil, true)
		rd := do("XREADGROUP", "GROUP", "grp", "c1", "COUNT", 1, "STREAMS", g+"st", ">")
		eq(t, rd.Kind != KindNil, true)
		eq(t, len(do("XREAD", "STREAMS", g+"st", "0").Array), 1)
		do("GEOADD", g+"geo", 13.36, 38.11, "palermo")
		eq(t, do("GEOSEARCHSTORE", g+"geod", g+"geo", "FROMLONLAT", 13.36, 38.11, "BYRADIUS", 10, "km").Int, int64(1))
		do("JSON.SET", g+"j1", "$", `{"a":1}`)
		do("JSON.SET", g+"j2", "$", `{"a":2}`)
		eq(t, len(do("JSON.MGET", g+"j1", g+"j2", "$.a").Array), 2)
		// blocking list pop: data is there, so it returns at once
		kv, ok, err := c.BLPop(ctx, time.Second, g+"l2", g+"l5")
		noErr(t, err)
		eq(t, ok, true)
		eq(t, kv.Key, g+"l2")
		// keyless commands
		eq(t, do("PING").Str(), "PONG")
		eq(t, do("ECHO", "hi").Str(), "hi")
		eq(t, do("CLUSTER", "KEYSLOT", g+"x").Int, int64(KeySlot(g+"x")))
	}
}

func TestClusterLiveBlockingOnOwningNode(t *testing.T) {
	c, ctx := liveCluster(t)
	for i := 0; i < 3; i++ {
		key := "{" + tagFor(c, i) + "}q"
		done := make(chan KeyValue, 1)
		go func() {
			kv, ok, err := c.BLPop(ctx, 5*time.Second, key)
			if err != nil || !ok {
				done <- KeyValue{Key: "error"}
				return
			}
			done <- kv
		}()
		time.Sleep(50 * time.Millisecond)
		// the shared connection is not parked by the blocked call
		noErr(t, c.Set(ctx, key+"other", "1"))
		_, err := c.RPush(ctx, key, "wake")
		noErr(t, err)
		select {
		case kv := <-done:
			eq(t, kv, KeyValue{Key: key, Value: "wake"})
		case <-time.After(5 * time.Second):
			t.Fatal("BLPOP did not return")
		}
		_, ok, err := c.BLPop(ctx, 100*time.Millisecond, key)
		noErr(t, err)
		eq(t, ok, false)
	}
}

func TestClusterLiveScanWalksEveryNode(t *testing.T) {
	c, ctx := liveCluster(t)
	want := map[string]bool{}
	for i := 0; i < 90; i++ {
		k := fmt.Sprintf("scan:%d", i)
		want[k] = true
		noErr(t, c.Set(ctx, k, "x"))
	}
	got := map[string]int{}
	it := c.ScanIter(ScanOptions{Count: 10})
	for it.Next(ctx) {
		got[it.Val()]++
	}
	noErr(t, it.Err())
	eq(t, len(got), len(want))
	for k, n := range got {
		if !want[k] || n != 1 {
			t.Fatalf("key %s seen %d times", k, n)
		}
	}
}

func TestClusterLivePubSub(t *testing.T) {
	c, ctx := liveCluster(t)
	ps, err := c.PubSub(ctx, 64)
	noErr(t, err)
	defer ps.Close()
	noErr(t, ps.Subscribe(ctx, "news"))
	// PUBLISH goes to the same (seed) node as SUBSCRIBE
	n, err := c.Publish(ctx, "news", "hello")
	noErr(t, err)
	eq(t, n, int64(1))
	// shard channels on each node
	var chans []string
	for i := 0; i < 3; i++ {
		chans = append(chans, "{"+tagFor(c, i)+"}sch")
	}
	noErr(t, ps.SSubscribe(ctx, chans...))
	for _, ch := range chans {
		n, err := c.SPublish(ctx, ch, "m:"+ch)
		noErr(t, err)
		eq(t, n, int64(1))
	}
	got := map[string]string{}
	deadline := time.After(5 * time.Second)
	for len(got) < 4 {
		select {
		case m := <-ps.Channel():
			got[m.Channel] = m.Payload
		case <-deadline:
			t.Fatalf("got only %v", got)
		}
	}
	eq(t, got["news"], "hello")
	for _, ch := range chans {
		eq(t, got[ch], "m:"+ch)
	}
	noErr(t, ps.SUnsubscribe(ctx))
	n, err = c.SPublish(ctx, chans[0], "again")
	noErr(t, err)
	eq(t, n, int64(0))
}

func TestClusterLiveScriptsReachEveryNode(t *testing.T) {
	c, ctx := liveCluster(t)
	sha := must[string](t)(c.ScriptLoad(ctx, "return ruvio.call('INCR', KEYS[1])"))
	for i := 0; i < 3; i++ {
		key := "{" + tagFor(c, i) + "}ctr"
		v, err := c.EvalSHA(ctx, sha, []string{key})
		noErr(t, err)
		eq(t, v.Int, int64(1))
	}
}

func TestClusterLiveDedicatedConn(t *testing.T) {
	c, ctx := liveCluster(t)
	for i := 0; i < 3; i++ {
		key := "{" + tagFor(c, i) + "}dc"
		d, err := c.ConnFor(ctx, key)
		noErr(t, err)
		noErr(t, d.Set(ctx, key, "v"))
		if _, _, err := d.Get(ctx, "{"+tagFor(c, (i+1)%3)+"}dc"); !errors.Is(err, ErrCrossSlot) {
			t.Fatalf("other node's key on a bound connection: %v", err)
		}
		noErr(t, d.Close())
	}
}

func TestClusterLiveConcurrentMixed(t *testing.T) {
	c, ctx := liveCluster(t)
	const workers, per = 24, 150
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				k := fmt.Sprintf("cc:%d:%d", w, i%10)
				if _, err := c.Incr(ctx, k); err != nil {
					errs <- err
					return
				}
				if i%25 == 0 {
					ks := []string{k, fmt.Sprintf("cc:%d:%d", w, (i+3)%10), fmt.Sprintf("cc:x%d", i%4)}
					if _, err := c.MGet(ctx, ks...); err != nil {
						errs <- err
						return
					}
				}
				if i == 70 {
					staleTable(c) // heal under load
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var keys []string
	for w := 0; w < workers; w++ {
		for j := 0; j < 10; j++ {
			keys = append(keys, fmt.Sprintf("cc:%d:%d", w, j))
		}
	}
	vals := must[[]NullString](t)(c.MGet(ctx, keys...))
	var total int64
	for _, v := range vals {
		var n int64
		fmt.Sscan(v.Val, &n)
		total += n
	}
	eq(t, total, int64(workers*per))
	sz := nodeSizes(t, ctx, c)
	sort.Slice(sz, func(i, j int) bool { return sz[i] < sz[j] })
	if sz[0] == 0 {
		t.Fatal("a node got no keys")
	}
}

// The same options against a plain server: automatic fallback, nothing changes.
func TestStandaloneFallbackLive(t *testing.T) {
	addr := os.Getenv("RUVIO_TEST_ADDR")
	if addr == "" {
		t.Skip("RUVIO_TEST_ADDR not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, err := Connect(ctx, Options{Addr: addr, DB: testDB, Cluster: true})
	noErr(t, err)
	defer c.Close()
	noErr(t, c.FlushDB(ctx))
	if c.IsCluster() || c.Nodes() != nil {
		t.Fatal("a standalone server must not look like a cluster")
	}
	noErr(t, c.MSet(ctx, "a", "1", "b", "2", "c", "3"))
	got := must[[]NullString](t)(c.MGet(ctx, "c", "a", "zz", "b"))
	eq(t, got, []NullString{{"3", true}, {"1", true}, {}, {"2", true}})
	eq(t, must[int64](t)(c.Del(ctx, "a", "b", "zz")), int64(2))
	vals, err := c.Pipelined(ctx, func(p *Pipeline) { p.Set("p", "1"); p.Get("p"); p.Incr("n") })
	noErr(t, err)
	eq(t, vals[1].Str(), "1")
	res, err := c.Transaction(ctx, func(p *Pipeline) { p.Set("{t}1", "1"); p.Set("{t}2", "2") })
	noErr(t, err)
	eq(t, len(res), 2)
	// cross-slot multi-key commands are plain server behaviour in standalone mode
	_, err = c.SAdd(ctx, "x1", "a")
	noErr(t, err)
	_, err = c.SAdd(ctx, "x2", "b")
	noErr(t, err)
	un := must[[]string](t)(c.SUnion(ctx, "x1", "x2"))
	eq(t, len(un), 2)
	l, ok, err := c.AcquireLock(ctx, "lk", time.Second)
	noErr(t, err)
	eq(t, ok, true)
	_, err = l.Unlock(ctx)
	noErr(t, err)
	n := must[int64](t)(c.DBSize(ctx))
	if n < 4 {
		t.Fatalf("DBSIZE %d", n)
	}
	ps, err := c.PubSub(ctx, 8)
	noErr(t, err)
	noErr(t, ps.SSubscribe(ctx, "sc"))
	noErr(t, ps.Close())
}

func TestLiveClusterMoveSlotsStatusAndRefusal(t *testing.T) {
	c, ctx := liveCluster(t)
	status := must[map[string]string](t)(c.MoveSlotsStatus(ctx))
	if _, ok := status["state"]; !ok {
		t.Fatalf("status has no state: %v", status)
	}
	// Slot 0 is on shard 0: nothing to move, and the server says why.
	err := c.MoveSlots(ctx, 0, 10, 0)
	var server *Error
	if !errors.As(err, &server) || !strings.Contains(err.Error(), "already on that shard") {
		t.Fatalf("expected a refusal, got %v", err)
	}
}
