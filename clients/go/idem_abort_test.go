// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// idemFake answers BEGIN with "acquired" and ABORT with the given integer reply.
func idemFake(t *testing.T, abortReply string) *fakeNode {
	return newFakeNode(t, func(f *fakeNode, args []string) string {
		switch strings.ToUpper(args[0]) {
		case "PING":
			return "+PONG\r\n"
		case "IDEM":
			switch strings.ToUpper(args[2]) {
			case "BEGIN":
				return "*1\r\n" + bulk("acquired")
			case "ABORT":
				return abortReply
			}
		}
		return ":1\r\n"
	})
}

func TestIdemAbortEncodesAndMapsTheReply(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		reply string
		want  bool
	}{{":1\r\n", true}, {":0\r\n", false}} {
		f := idemFake(t, tc.reply)
		c, err := Connect(ctx, Options{Addr: f.addr})
		if err != nil {
			t.Fatal(err)
		}
		got, err := c.IdemAbort(ctx, "k", "fp", "me")
		c.Close()
		if err != nil || got != tc.want {
			t.Fatalf("reply %q: got %v, %v want %v", tc.reply, got, err, tc.want)
		}
		if cmds := f.commands(); cmds[len(cmds)-1] != "IDEM k ABORT fp me" {
			t.Fatalf("sent %v", cmds)
		}
	}
}

func TestIdempotentAbortsOnlyWhenAsked(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("boom")
	fail := func(context.Context) ([]byte, error) { return nil, boom }
	for _, abort := range []bool{false, true} {
		f := idemFake(t, ":1\r\n")
		c, err := Connect(ctx, Options{Addr: f.addr})
		if err != nil {
			t.Fatal(err)
		}
		var opts []IdemOption
		if abort {
			opts = append(opts, WithAbortOnError())
		}
		_, err = c.Idempotent(ctx, "k", "fp", time.Minute, fail, opts...)
		c.Close()
		if !errors.Is(err, boom) {
			t.Fatalf("abort=%v: got %v, want the work's error", abort, err)
		}
		sentAbort := false
		for _, line := range f.commands() {
			if strings.Contains(line, " ABORT ") {
				sentAbort = true
			}
		}
		if sentAbort != abort {
			t.Fatalf("abort=%v: sent ABORT = %v (%v)", abort, sentAbort, f.commands())
		}
	}
}

func TestIdempotentAbortFailureKeepsTheWorkError(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("boom")
	f := idemFake(t, "-ERR INPUT bad\r\n")
	c, err := Connect(ctx, Options{Addr: f.addr})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.Idempotent(ctx, "k", "fp", time.Minute,
		func(context.Context) ([]byte, error) { return nil, boom }, WithAbortOnError())
	if !errors.Is(err, boom) {
		t.Fatalf("got %v, want the work's error", err)
	}
}
