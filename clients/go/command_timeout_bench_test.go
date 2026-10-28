// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// The cost of Options.CommandTimeout on calls whose context has no deadline (the case it
// applies to): off (-1) against the 30 s default.
//
//	RUVIO_TEST_ADDR=127.0.0.1:6379 go test -run '^$' -bench CommandTimeout -benchmem
func benchTimeoutClient(b *testing.B, timeout time.Duration) *Client {
	b.Helper()
	addr := os.Getenv("RUVIO_TEST_ADDR")
	if addr == "" {
		b.Skip("RUVIO_TEST_ADDR not set")
	}
	c := NewClient(Options{Addr: addr, DB: testDB, CommandTimeout: timeout})
	b.Cleanup(func() { c.Close() })
	if err := c.Set(context.Background(), "bench:timeout", strings.Repeat("v", 64)); err != nil {
		b.Fatal(err)
	}
	return c
}

func BenchmarkCommandTimeout(b *testing.B) {
	for _, mode := range []struct {
		name    string
		timeout time.Duration
	}{{"off", -1}, {"default", 0}} {
		ctx := context.Background()
		b.Run("GetSerial/"+mode.name, func(b *testing.B) {
			c := benchTimeoutClient(b, mode.timeout)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := c.Get(ctx, "bench:timeout"); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("SetSerial/"+mode.name, func(b *testing.B) {
			c := benchTimeoutClient(b, mode.timeout)
			v := strings.Repeat("v", 64)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := c.Set(ctx, "bench:timeout", v); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("GetParallel/"+mode.name, func(b *testing.B) {
			c := benchTimeoutClient(b, mode.timeout)
			b.ReportAllocs()
			b.SetParallelism(16)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, _, err := c.Get(ctx, "bench:timeout"); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
		b.Run("PipelineGet16/"+mode.name, func(b *testing.B) {
			c := benchTimeoutClient(b, mode.timeout)
			b.ReportAllocs()
			b.ResetTimer()
			p := c.Pipeline()
			for i := 0; i < b.N; i += 16 {
				for k := 0; k < 16; k++ {
					p.Get("bench:timeout")
				}
				if _, err := p.Exec(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
