// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

// Command faultprobe is the probe for scripts/client_faults.py: it writes fault:<name>:<i> = i
// with durability replicas 1, reads each key back, and keeps going through a failover until a
// line arrives on stdin. Environment: RUVIO_SEEDS, RUVIO_PROBE_NAME and, for TLS, RUVIO_PROBE_CA
// (PEM path). Output, one line each: ready, ack <i>, err <i> <text>, bad <i> <value>, done.
//
//	go build -o /tmp/faultprobe ./internal/faultprobe
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	ruvio "github.com/salihcantekin/ruvio/clients/go"
)

func main() {
	name := os.Getenv("RUVIO_PROBE_NAME")
	opts := ruvio.Options{Seeds: strings.Split(os.Getenv("RUVIO_SEEDS"), ","), DurabilityReplicas: 1}
	if ca := os.Getenv("RUVIO_PROBE_CA"); ca != "" {
		pem, err := os.ReadFile(ca)
		if err != nil {
			fmt.Fprintln(os.Stderr, "ca:", err)
			os.Exit(1)
		}
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(pem)
		opts.TLS = &tls.Config{RootCAs: pool}
	}
	ctx := context.Background()
	c, err := ruvio.Connect(ctx, opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, "connect:", err)
		os.Exit(1)
	}
	defer c.Close()

	var stop atomic.Bool
	go func() {
		buf := make([]byte, 1)
		os.Stdin.Read(buf)
		stop.Store(true)
	}()

	fmt.Println("ready")
	for i := 1; !stop.Load(); {
		key := "fault:" + name + ":" + strconv.Itoa(i)
		if err := c.Set(ctx, key, strconv.Itoa(i)); err != nil {
			fmt.Println("err", i, strings.Fields(err.Error())[0])
			time.Sleep(20 * time.Millisecond)
			continue
		}
		got, _, err := c.Get(ctx, key)
		if err != nil {
			fmt.Println("err", i, strings.Fields(err.Error())[0])
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if got != strconv.Itoa(i) {
			fmt.Println("bad", i, got)
		}
		fmt.Println("ack", i)
		i++
	}
	fmt.Println("done")
}
