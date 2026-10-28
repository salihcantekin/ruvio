// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// okOrServerErr accepts a reply or a typed server error: both prove the command was
// encoded, sent and answered (used where the outcome depends on server config).
func okOrServerErr(t *testing.T, name string, err error) {
	t.Helper()
	var se *Error
	if err != nil && !errors.As(err, &se) {
		t.Fatalf("%s: transport error %v", name, err)
	}
	if err != nil {
		t.Logf("%s: server answered %q", name, err)
	}
}

func TestLiveConnectionAndServer(t *testing.T) {
	c, ctx := live(t)
	noErr(t, c.Ping(ctx))
	eq(t, must[string](t)(c.Echo(ctx, "hi")), "hi")
	h, err := c.Hello(ctx, 2)
	noErr(t, err)
	eq(t, h["server"].Str(), "ruvio")
	okOrServerErr(t, "HELLO AUTH", func() error { _, err := c.HelloAuth(ctx, 2, "default", "nopass"); return err }())
	okOrServerErr(t, "AUTH", c.Auth(ctx, "", "nopass"))

	noErr(t, c.ClientSetName(ctx, "go-test"))
	eq(t, must[string](t)(c.ClientGetName(ctx)), "go-test")
	id := must[int64](t)(c.ClientID(ctx))
	if !strings.Contains(must[string](t)(c.ClientList(ctx)), "go-test") {
		t.Fatal("client list")
	}
	if !strings.Contains(must[string](t)(c.ClientList(ctx, id)), "go-test") {
		t.Fatal("client list id")
	}
	if !strings.Contains(must[string](t)(c.ClientInfo(ctx)), "go-test") {
		t.Fatal("client info")
	}
	eq(t, must[int64](t)(c.ClientKill(ctx, ClientKillOptions{ID: 999999999})), int64(0))
	okOrServerErr(t, "CLIENT KILL ADDR", func() error {
		_, err := c.ClientKill(ctx, ClientKillOptions{Addr: "10.255.255.1:1"})
		return err
	}())

	if !strings.Contains(must[string](t)(c.Info(ctx, "server")), "ruvio") {
		t.Log("INFO server does not mention ruvio")
	}
	eq(t, must[int64](t)(c.Wait(ctx, 0, 10*time.Millisecond)), int64(0))
	if len(must[map[string]string](t)(c.ConfigGet(ctx, "*"))) == 0 {
		t.Fatal("config get")
	}
	if err := c.ConfigSet(ctx, "slowlog-max-len", "128"); err != nil {
		t.Fatal(err)
	}
	eq(t, must[map[string]string](t)(c.ConfigGet(ctx, "slowlog-max-len"))["slowlog-max-len"], "128")
	if err := c.ConfigSet(ctx, "shards", "2"); err == nil {
		t.Fatal("CONFIG SET shards should be refused")
	}
	if role, err := c.Role(ctx); err != nil || len(role.Array) == 0 || string(role.Array[0].Bytes) != "master" {
		t.Fatalf("ROLE answered %+v, %v", role, err)
	}
	// The live server keeps no WAL, so it cannot replicate: both forms are refused unchanged.
	if err := c.ReplicaOf(ctx, "127.0.0.1", 1); err == nil {
		t.Fatal("REPLICAOF should be refused without a WAL")
	}
	if err := c.ReplicaOfNoOne(ctx, false); err == nil {
		t.Fatal("REPLICAOF NO ONE should be refused without a WAL")
	}
	// Without replication the server refuses CLIENT DURABILITY.
	if err := c.ClientDurability(ctx, 1, 0); err == nil || !strings.Contains(err.Error(), "needs replication") {
		t.Fatalf("CLIENT DURABILITY answered %v, want the needs-replication error", err)
	}
	okOrServerErr(t, "SAVE", c.Save(ctx))
	okOrServerErr(t, "BGSAVE", func() error { _, err := c.BgSave(ctx); return err }())
	cmd := must[Value](t)(c.Command(ctx))
	if len(cmd.Array) < 200 {
		t.Fatalf("COMMAND returned %d entries", len(cmd.Array))
	}
	if n := must[int64](t)(c.CommandCount(ctx)); n < 200 {
		t.Fatalf("COMMAND COUNT %d", n)
	}
	eq(t, len(must[Value](t)(c.CommandInfo(ctx, "get", "set")).Array), 2)
	_, err = c.CommandDocs(ctx, "get")
	noErr(t, err)
	_, err = c.SlowLogGet(ctx, 5)
	noErr(t, err)
	_, err = c.SlowLogGet(ctx, -1)
	noErr(t, err)
	_, err = c.SlowLogLen(ctx)
	noErr(t, err)
	noErr(t, c.SlowLogReset(ctx))
	_, err = c.LatencyLatest(ctx)
	noErr(t, err)
}

func TestLiveACL(t *testing.T) {
	c, ctx := live(t)
	eq(t, must[string](t)(c.ACLWhoAmI(ctx)), "default")
	noErr(t, c.ACLSetUser(ctx, "gotest", "on", ">secret", "+get", "+ping", "+acl", "~*"))
	users := must[[]string](t)(c.ACLUsers(ctx))
	if !strings.Contains(strings.Join(users, ","), "gotest") {
		t.Fatalf("users %v", users)
	}
	if len(must[[]string](t)(c.ACLList(ctx))) == 0 {
		t.Fatal("acl list")
	}
	u, err := c.ACLGetUser(ctx, "gotest")
	noErr(t, err)
	if len(u.Array) == 0 {
		t.Fatal("acl getuser")
	}
	if len(must[[]string](t)(c.ACLCat(ctx))) == 0 {
		t.Fatal("acl cat")
	}
	okOrServerErr(t, "ACL CAT string", func() error { _, err := c.ACLCat(ctx, "string"); return err }())
	// SAVE before LOAD: LOAD replaces the live directory from security.acl_file.
	okOrServerErr(t, "ACL SAVE", c.ACLSave(ctx))
	okOrServerErr(t, "ACL LOAD", c.ACLLoad(ctx))

	// AUTH as the new user on a separate client
	u2, err := Connect(ctx, Options{Addr: os.Getenv("RUVIO_TEST_ADDR"), Username: "gotest", Password: "secret"})
	noErr(t, err)
	defer u2.Close()
	eq(t, must[string](t)(u2.ACLWhoAmI(ctx)), "gotest")
	if err := u2.Set(ctx, "x", "1"); err == nil {
		t.Fatal("ACL should refuse SET")
	}
}

func TestLiveClusterCommands(t *testing.T) {
	c, ctx := live(t)
	slot := must[int64](t)(c.ClusterKeySlot(ctx, "foo"))
	eq(t, slot, int64(KeySlot("foo")))
	eq(t, must[int64](t)(c.ClusterKeySlot(ctx, "{u1}.a")), int64(KeySlot("{u1}.b")))
	_, err := c.ClusterSlots(ctx)
	okOrServerErr(t, "CLUSTER SLOTS", err)
	_, err = c.ClusterNodes(ctx)
	okOrServerErr(t, "CLUSTER NODES", err)
	_, err = c.ClusterShards(ctx)
	okOrServerErr(t, "CLUSTER SHARDS", err)
	_, err = c.ClusterInfo(ctx)
	okOrServerErr(t, "CLUSTER INFO", err)
	_, err = c.ClusterMyID(ctx)
	okOrServerErr(t, "CLUSTER MYID", err)
	okOrServerErr(t, "READONLY", c.ReadOnly(ctx))
	okOrServerErr(t, "READWRITE", c.ReadWrite(ctx))
}

func TestLiveScripting(t *testing.T) {
	c, ctx := live(t)
	// Ruvio's Lua API is ruvio.call (redis.call is an alias).
	v, err := c.Eval(ctx, "ruvio.call('SET', KEYS[1], ARGV[1]); return ruvio.call('GET', KEYS[1])", []string{"ek"}, "val")
	noErr(t, err)
	eq(t, v.Str(), "val")
	sha := must[string](t)(c.ScriptLoad(ctx, "return ARGV[1] + 1"))
	v, err = c.EvalSHA(ctx, sha, nil, 41)
	noErr(t, err)
	eq(t, v.Int, int64(42))
	eq(t, must[[]bool](t)(c.ScriptExists(ctx, sha, "0000000000000000000000000000000000000000")), []bool{true, false})
	noErr(t, c.ScriptFlush(ctx))
	var se *Error
	if _, err := c.EvalSHA(ctx, sha, nil); !errors.As(err, &se) || se.Prefix() != "NOSCRIPT" {
		t.Fatalf("want NOSCRIPT, got %v", err)
	}
	for i := 0; i < 2; i++ {
		v, err = c.EvalCached(ctx, "return ARGV[1]", nil, "again")
		noErr(t, err)
		eq(t, v.Str(), "again")
	}
	if err := c.ScriptKill(ctx); !errors.As(err, &se) || se.Prefix() != "NOTBUSY" {
		t.Fatalf("want NOTBUSY, got %v", err)
	}
	eq(t, must[string](t)(c.FunctionLoad(ctx, "gofn", "return KEYS[1] .. ARGV[1]")), "gofn")
	if !strings.Contains(strings.Join(must[[]string](t)(c.FunctionList(ctx)), ","), "gofn") {
		t.Fatal("function list")
	}
	v, err = c.FCall(ctx, "gofn", []string{"k"}, "!")
	noErr(t, err)
	eq(t, v.Str(), "k!")
	eq(t, must[bool](t)(c.FunctionDelete(ctx, "gofn")), true)
	eq(t, must[bool](t)(c.FunctionDelete(ctx, "gofn")), false)
}

func TestLivePubSub(t *testing.T) {
	c, ctx := live(t)
	ps, err := c.PubSub(ctx, 0)
	noErr(t, err)
	defer ps.Close()
	noErr(t, ps.Ping(ctx))
	noErr(t, ps.Subscribe(ctx, "ch1", "ch2"))
	noErr(t, ps.PSubscribe(ctx, "news.*"))
	noErr(t, ps.SSubscribe(ctx, "sh1"))
	noErr(t, ps.Ping(ctx))
	eq(t, must[int64](t)(c.Publish(ctx, "ch1", "hello")), int64(1))
	c.Publish(ctx, "news.today", "p")
	c.SPublish(ctx, "sh1", "s")
	want := []Message{{Kind: "message", Channel: "ch1", Payload: "hello"},
		{Kind: "pmessage", Pattern: "news.*", Channel: "news.today", Payload: "p"},
		{Kind: "smessage", Channel: "sh1", Payload: "s"}}
	for _, w := range want {
		select {
		case m := <-ps.Channel():
			eq(t, m, w)
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for %v", w)
		}
	}
	noErr(t, ps.Unsubscribe(ctx, "ch1"))
	noErr(t, ps.Unsubscribe(ctx))
	noErr(t, ps.PUnsubscribe(ctx))
	noErr(t, ps.SUnsubscribe(ctx, "sh1"))
	eq(t, must[int64](t)(c.Publish(ctx, "ch2", "gone")), int64(0))
	ps.Close()
	if _, ok := <-ps.Channel(); ok {
		t.Fatal("channel should close")
	}
}

func TestLiveClientTracking(t *testing.T) {
	addr := os.Getenv("RUVIO_TEST_ADDR")
	if addr == "" {
		t.Skip("RUVIO_TEST_ADDR not set")
	}
	_, ctx := live(t)
	got := make(chan Value, 4)
	c3, err := Connect(ctx, Options{Addr: addr, DB: testDB, Protocol: 3, OnPush: func(v Value) { got <- v }})
	noErr(t, err)
	defer c3.Close()
	noErr(t, c3.ClientTracking(ctx, true))
	noErr(t, c3.Set(ctx, "tracked", "1"))
	_, _, err = c3.Get(ctx, "tracked")
	noErr(t, err)
	other, err := Connect(ctx, Options{Addr: addr, DB: testDB})
	noErr(t, err)
	defer other.Close()
	noErr(t, other.Set(ctx, "tracked", "2"))
	select {
	case v := <-got:
		if !strings.Contains(v.String(), "invalidate") || !strings.Contains(v.String(), "tracked") {
			t.Fatalf("push %v", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no invalidation push")
	}
	// RESP3 replies still decode through the typed methods
	h, err := c3.HGetAll(ctx, "nope")
	noErr(t, err)
	eq(t, len(h), 0)
	c3.HSet(ctx, "hh", "a", "1")
	eq(t, must[map[string]string](t)(c3.HGetAll(ctx, "hh")), map[string]string{"a": "1"})
	noErr(t, c3.ClientTracking(ctx, false))
}

func TestLivePipelineAndTx(t *testing.T) {
	c, ctx := live(t)
	p := c.Pipeline()
	p.Set("a", "1")
	p.SetPX("b", "2", 60000)
	p.Incr("n")
	p.IncrBy("n", 4)
	p.Get("a")
	p.Do("LPUSH", "a", "x") // WRONGTYPE stays in place
	p.HSet("h", "f", "v")
	p.HGet("h", "f")
	p.LPush("l", "1")
	p.RPush("l", "2")
	p.SAdd("s", "m")
	p.ZAdd("z", Z{"m", 1})
	p.PExpire("a", 60000)
	p.Publish("nobody", "x")
	p.DoBytes([]byte("ECHO"), []byte("bin"))
	p.Del("b")
	eq(t, p.Len(), 16)
	vals, err := p.Exec(ctx)
	var se *Error
	if !errors.As(err, &se) || se.Prefix() != "WRONGTYPE" {
		t.Fatalf("want first error WRONGTYPE, got %v", err)
	}
	eq(t, len(vals), 16)
	eq(t, vals[3].Int, int64(5))
	eq(t, vals[4].Str(), "1")
	eq(t, vals[5].Kind, KindError)
	eq(t, vals[7].Str(), "v")
	eq(t, vals[14].Str(), "bin")
	vals, err = c.Pipelined(ctx, func(p *Pipeline) {
		for i := 0; i < 100; i++ {
			p.Incr("cnt")
		}
	})
	noErr(t, err)
	eq(t, vals[99].Int, int64(100))
	p.Do("SET", struct{}{})
	if _, err := p.Exec(ctx); err == nil {
		t.Fatal("bad argument must fail")
	}

	res, err := c.Transaction(ctx, func(p *Pipeline) {
		p.Set("tx", "1")
		p.Incr("tx")
		p.Get("tx")
	})
	noErr(t, err)
	eq(t, res[2].Str(), "2")

	// WATCH conflict aborts EXEC
	tx, err := c.Tx(ctx)
	noErr(t, err)
	noErr(t, tx.Watch(ctx, "tx"))
	cur, _, _ := tx.Get(ctx, "tx")
	eq(t, cur, "2")
	noErr(t, c.Set(ctx, "tx", "changed"))
	_, err = tx.Exec(ctx, func(p *Pipeline) { p.Set("tx", "mine") })
	if !errors.Is(err, ErrTxAborted) {
		t.Fatalf("want aborted, got %v", err)
	}
	noErr(t, tx.Watch(ctx, "tx"))
	noErr(t, tx.Unwatch(ctx))
	noErr(t, tx.Multi(ctx))
	noErr(t, tx.Queue(ctx, "SET", "tx", "q"))
	noErr(t, tx.Discard(ctx))
	noErr(t, tx.Multi(ctx))
	noErr(t, tx.Queue(ctx, "SET", "tx", "q"))
	noErr(t, tx.Queue(ctx, "GET", "tx"))
	res, err = tx.ExecQueued(ctx)
	noErr(t, err)
	eq(t, res[1].Str(), "q")
	noErr(t, tx.Close())
}

func TestLiveHelpers(t *testing.T) {
	c, ctx := live(t)
	// lock
	l, ok, err := c.AcquireLock(ctx, "lock", time.Minute)
	noErr(t, err)
	eq(t, ok, true)
	_, ok, _ = c.AcquireLock(ctx, "lock", time.Minute)
	eq(t, ok, false)
	eq(t, must[bool](t)(l.Refresh(ctx, 2*time.Minute)), true)
	// someone else's lock is never released by us
	other := &Lock{c: &c.cmds, Key: "lock", Token: "not-mine"}
	eq(t, must[bool](t)(other.Unlock(ctx)), false)
	eq(t, must[bool](t)(other.Refresh(ctx, time.Minute)), false)
	eq(t, must[bool](t)(l.Unlock(ctx)), true)
	eq(t, must[bool](t)(l.Unlock(ctx)), false)
	ran := false
	noErr(t, c.WithLock(ctx, "lock", time.Minute, func(ctx context.Context) error {
		ran = true
		if err := c.WithLock(ctx, "lock", time.Minute, func(context.Context) error { return nil }); !errors.Is(err, ErrNotAcquired) {
			t.Errorf("nested lock: %v", err)
		}
		return nil
	}))
	eq(t, ran, true)
	eq(t, must[int64](t)(c.Exists(ctx, "lock")), int64(0))
	wl, err := c.AcquireLockWait(ctx, "lock", time.Minute, 5*time.Millisecond)
	noErr(t, err)
	wl.Unlock(ctx)

	// lease + semaphore
	lh, ok, err := c.AcquireLease(ctx, "lease", time.Minute)
	noErr(t, err)
	eq(t, ok, true)
	eq(t, must[bool](t)(lh.Renew(ctx, 2*time.Minute)), true)
	eq(t, must[bool](t)(c.Renew(ctx, "lease", lh.Token+100, time.Minute)), false)
	eq(t, must[bool](t)(lh.Release(ctx)), true)
	eq(t, must[bool](t)(lh.Renew(ctx, time.Minute)), false)
	noErr(t, c.WithLease(ctx, "lease", time.Minute, func(ctx context.Context, token int64) error {
		if token <= lh.Token {
			t.Errorf("fencing token did not grow: %d <= %d", token, lh.Token)
		}
		return nil
	}))
	p1, ok1, _ := c.AcquirePermit(ctx, "sem", 1, time.Minute)
	_, ok2, _ := c.AcquirePermit(ctx, "sem", 1, time.Minute)
	eq(t, []any{ok1, ok2}, []any{true, false})
	if err := c.WithPermit(ctx, "sem", 1, time.Minute, func(context.Context) error { return nil }); !errors.Is(err, ErrNotAcquired) {
		t.Fatalf("permit: %v", err)
	}
	eq(t, must[bool](t)(p1.Renew(ctx, 2*time.Minute)), true)
	eq(t, must[bool](t)(p1.Release(ctx)), true)
	eq(t, must[bool](t)(p1.Renew(ctx, time.Minute)), false)
	noErr(t, c.WithPermit(ctx, "sem", 1, time.Minute, func(context.Context) error { return nil }))

	// rate limit
	a1, r1, _ := c.RateLimit(ctx, "rl", 2, time.Minute)
	a2, r2, _ := c.RateLimit(ctx, "rl", 2, time.Minute)
	a3, r3, err := c.RateLimit(ctx, "rl", 2, time.Minute)
	noErr(t, err)
	eq(t, []any{a1, r1, a2, r2, a3, r3}, []any{true, int64(1), true, int64(0), false, int64(0)})

	// cache-aside with stampede protection: 20 callers, one factory run
	var calls atomic.Int32
	errs := make(chan error, 20)
	vals := make(chan string, 20)
	for i := 0; i < 20; i++ {
		go func() {
			v, err := c.GetOrCreate(ctx, "cache", time.Minute, func(context.Context) (string, error) {
				calls.Add(1)
				time.Sleep(30 * time.Millisecond)
				return "computed", nil
			})
			errs <- err
			vals <- v
		}()
	}
	for i := 0; i < 20; i++ {
		noErr(t, <-errs)
		eq(t, <-vals, "computed")
	}
	eq(t, calls.Load(), int32(1))
	type cfg struct{ N int }
	got, err := GetOrCreateJSON(ctx, c, "cachej", time.Minute, func(context.Context) (cfg, error) { return cfg{7}, nil })
	noErr(t, err)
	eq(t, got, cfg{7})
	got, err = GetOrCreateJSON(ctx, c, "cachej", time.Minute, func(context.Context) (cfg, error) { return cfg{8}, nil })
	noErr(t, err)
	eq(t, got, cfg{7})
	if _, err := c.GetOrCreate(ctx, "cachefail", time.Minute, func(context.Context) (string, error) {
		return "", errors.New("boom")
	}, GetOrCreateOptions{FillTimeout: 50 * time.Millisecond}); err == nil {
		t.Fatal("factory error must propagate")
	}

	// idempotency
	runs := 0
	fn := func(context.Context) ([]byte, error) { runs++; return []byte("done"), nil }
	r, err := c.Idempotent(ctx, "idem", "fp", time.Minute, fn)
	noErr(t, err)
	eq(t, string(r), "done")
	r, err = c.Idempotent(ctx, "idem", "fp", time.Minute, fn)
	noErr(t, err)
	eq(t, []any{string(r), runs}, []any{"done", 1})
	if _, err := c.Idempotent(ctx, "idem", "other", time.Minute, fn); !errors.Is(err, ErrIdemConflict) {
		t.Fatalf("want conflict, got %v", err)
	}
	c.IdemBegin(ctx, "idem2", "fp", "someone", time.Minute)
	if _, err := c.Idempotent(ctx, "idem2", "fp", time.Minute, fn); !errors.Is(err, ErrIdemPending) {
		t.Fatalf("want pending, got %v", err)
	}

	// recent items
	for i := 0; i < 5; i++ {
		c.PushRecent(ctx, "recent", 3, string(rune('a'+i)))
	}
	eq(t, must[[]string](t)(c.Recent(ctx, "recent", 10)), []string{"e", "d", "c"})
	eq(t, must[[]string](t)(c.Recent(ctx, "recent", 2)), []string{"e", "d"})
}
