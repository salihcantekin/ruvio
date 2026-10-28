// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

// Command seedprobe is the probe for scripts/seed_failover.py: it connects with the seeds in
// RUVIO_SEEDS, reads seed:name, waits for a line on stdin (the first server is stopped by then)
// and reads it again.
//
//	go build -o /tmp/seedprobe ./internal/seedprobe
//	scripts/seed_failover.py target/debug/ruvio -- /tmp/seedprobe
package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	ruvio "github.com/salihcantekin/ruvio/clients/go"
)

func main() {
	seeds := strings.Split(os.Getenv("RUVIO_SEEDS"), ",")
	ctx := context.Background()
	c, err := ruvio.Connect(ctx, ruvio.Options{Seeds: seeds})
	if err != nil {
		fmt.Fprintln(os.Stderr, "connect:", err)
		os.Exit(1)
	}
	defer c.Close()

	first, _, err := c.Get(ctx, "seed:name")
	if err != nil {
		fmt.Fprintln(os.Stderr, "first GET:", err)
		os.Exit(1)
	}
	fmt.Println("first", first)

	bufio.NewReader(os.Stdin).ReadString('\n')

	for i := 0; i < 50; i++ {
		if value, _, err := c.Get(ctx, "seed:name"); err == nil {
			fmt.Println("after", value)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	os.Exit(1)
}
