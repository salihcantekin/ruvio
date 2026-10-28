// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// A server that accepts and never answers: a half-open peer.
func silentServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { c.Close() })
		}
	}()
	return ln.Addr().String()
}

func TestCommandTimeoutBoundsACallWithoutDeadline(t *testing.T) {
	client := NewClient(Options{Addr: silentServer(t), CommandTimeout: 150 * time.Millisecond})
	defer client.Close()

	start := time.Now()
	_, _, err := client.Get(context.Background(), "k")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("took %v", elapsed)
	}
}

func TestCommandTimeoutDefaultsToThirtySeconds(t *testing.T) {
	client := NewClient(Options{})
	defer client.Close()
	if client.opts.CommandTimeout != 30*time.Second {
		t.Fatalf("default CommandTimeout = %v", client.opts.CommandTimeout)
	}
	off := NewClient(Options{CommandTimeout: -1})
	defer off.Close()
	if off.opts.CommandTimeout > 0 {
		t.Fatalf("a negative CommandTimeout must stay off, got %v", off.opts.CommandTimeout)
	}
}

// A server that answers every read with a nil array after delay: BLPOP on an empty list.
func slowNilServer(t *testing.T, delay time.Duration) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { c.Close() })
			go func() {
				buf := make([]byte, 512)
				for {
					if _, err := c.Read(buf); err != nil {
						return
					}
					time.Sleep(delay)
					if _, err := c.Write([]byte("*-1\r\n")); err != nil {
						return
					}
				}
			}()
		}
	}()
	return ln.Addr().String()
}

func TestCommandTimeoutAddsABlockingCommandsOwnWait(t *testing.T) {
	client := NewClient(Options{Addr: slowNilServer(t, time.Second), CommandTimeout: 200 * time.Millisecond})
	defer client.Close()

	start := time.Now()
	_, ok, err := client.BLPop(context.Background(), time.Second, "k")
	if err != nil || ok {
		t.Fatalf("BLPOP k 1: ok=%v err=%v", ok, err)
	}
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("took %v", elapsed)
	}
}

func enc(args ...string) []byte {
	b := appendArrayHeader(nil, len(args))
	for _, a := range args {
		b = appendBulkString(b, a)
	}
	return b
}

func TestCommandWaitTable(t *testing.T) {
	ms := func(n int) time.Duration { return time.Duration(n) * time.Millisecond }
	cases := []struct {
		args []string
		want time.Duration
	}{
		{[]string{"GET", "k"}, 0},
		{[]string{"SET", "k", "v"}, 0},
		{[]string{"blpop", "a", "b", "1"}, ms(1000)},
		{[]string{"BLPOP", "k", "0"}, waitForever},
		{[]string{"BLPOP", "k", "0.5"}, ms(500)},
		{[]string{"BLPOP", "k", "soon"}, waitForever},
		{[]string{"BLPOP", "k", "-1"}, waitForever},
		{[]string{"BRPOP", "k", "2"}, ms(2000)},
		{[]string{"BZPOPMIN", "k", "1.5"}, ms(1500)},
		{[]string{"BZPOPMAX", "k", "3"}, ms(3000)},
		{[]string{"BRPOPLPUSH", "a", "b", "4"}, ms(4000)},
		{[]string{"BLMOVE", "a", "b", "LEFT", "RIGHT", "0.25"}, ms(250)},
		{[]string{"BLMPOP", "2", "2", "a", "b", "LEFT"}, ms(2000)},
		{[]string{"BZMPOP", "0", "1", "z", "MIN"}, waitForever},
		{[]string{"XREAD", "STREAMS", "s", "$"}, 0},
		{[]string{"XREAD", "COUNT", "1", "BLOCK", "700", "STREAMS", "s", "$"}, ms(700)},
		{[]string{"XREAD", "BLOCK", "0", "STREAMS", "s", "$"}, waitForever},
		{[]string{"XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"}, 0},
		{[]string{"XREADGROUP", "GROUP", "BLOCK", "c", "STREAMS", "s", ">"}, 0},
		{[]string{"XREADGROUP", "GROUP", "g", "c", "BLOCK", "50", "STREAMS", "s", ">"}, ms(50)},
		{[]string{"WAIT", "1", "100"}, ms(100)},
		{[]string{"WAIT", "1", "0"}, waitForever},
		{[]string{"WAITAOF", "1", "0", "200"}, ms(200)},
		{[]string{"FILLGET", "k", "1000"}, 0},
		{[]string{"FILLGET", "k", "1000", "WAIT", "300"}, ms(300)},
		{[]string{"FILLGET", "k", "1000", "WAIT", "0"}, waitForever},
		{[]string{"MONITOR"}, waitForever},
		{[]string{"SUBSCRIBE", "news"}, waitForever},
		{[]string{"PSUBSCRIBE", "n*"}, waitForever},
		{[]string{"SSUBSCRIBE", "news"}, waitForever},
		{[]string{"PUBLISH", "news", "x"}, 0},
		{[]string{"BLPOP"}, waitForever},
	}
	for _, tc := range cases {
		if got := requestWait(enc(tc.args...)); got != tc.want {
			t.Errorf("%v: got %v, want %v", tc.args, got, tc.want)
		}
	}
	// A pipeline takes its widest wait; any wait-forever wins.
	pipe := append(append(enc("GET", "k"), enc("BLPOP", "k", "2")...), enc("XREAD", "BLOCK", "100", "STREAMS", "s", "$")...)
	if got := requestWait(pipe); got != ms(2000) {
		t.Errorf("pipeline: got %v", got)
	}
	if got := requestWait(append(enc("BLPOP", "k", "2"), enc("BLPOP", "k", "0")...)); got != waitForever {
		t.Errorf("pipeline with 0: got %v", got)
	}
	if got := requestWait([]byte("garbage")); got != 0 {
		t.Errorf("garbage: got %v", got)
	}
}

func TestCommandWaitDoesNotAllocate(t *testing.T) {
	get := enc("GET", "some:key")
	blpop := enc("BLPOP", "k", "1.5")
	if n := testing.AllocsPerRun(100, func() { requestWait(get); requestWait(blpop) }); n != 0 {
		t.Fatalf("allocs per run: %v", n)
	}
}

// A server that answers its first two commands late and every later one at once.
func lateServer(t *testing.T, late time.Duration) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { c.Close() })
			go func() {
				p := parser{}
				buf := make([]byte, 4096)
				var pending []byte
				seen := 0
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					pending = append(pending, buf[:n]...)
					for {
						_, used, done, err := p.next(pending)
						if err != nil || !done {
							break
						}
						pending = pending[used:]
						seen++
						reply := "$2\r\nok\r\n"
						if seen <= 2 {
							time.Sleep(late)
							reply = "$4\r\nlate\r\n"
						}
						if _, err := c.Write([]byte(reply)); err != nil {
							return
						}
					}
				}
			}()
		}
	}()
	return ln.Addr().String()
}

// Expired calls, whether the caller read its own reply or waited for the reader, fail with
// DeadlineExceeded; their late replies are dropped and the connection stays in step.
func TestCommandTimeoutDropsLateRepliesAndKeepsTheConnection(t *testing.T) {
	client := NewClient(Options{Addr: lateServer(t, 300*time.Millisecond), CommandTimeout: 100 * time.Millisecond})
	defer client.Close()
	ctx := context.Background()

	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, _, err := client.Get(ctx, "k")
			errs <- err
		}()
	}
	for i := 0; i < 2; i++ {
		if err := <-errs; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("want DeadlineExceeded, got %v", err)
		}
	}
	time.Sleep(800 * time.Millisecond) // both late replies arrive and are dropped
	for i := 0; i < 3; i++ {
		v, ok, err := client.Get(ctx, "k")
		if err != nil || !ok || v != "ok" {
			t.Fatalf("after the timeouts: %q %v %v", v, ok, err)
		}
	}
	p := client.Pipeline()
	p.Get("a")
	p.Get("b")
	pl, err := p.Exec(ctx)
	if err != nil || len(pl) != 2 || pl[0].Str() != "ok" {
		t.Fatalf("pipeline: %v %v", pl, err)
	}
}
