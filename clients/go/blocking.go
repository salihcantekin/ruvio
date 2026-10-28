// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"bytes"
	"math"
	"strconv"
	"time"
)

// waitForever is the wait of a command that blocks with no bound: a 0 timeout, a
// connection that switches to push mode (SUBSCRIBE, MONITOR) or an argument that does
// not parse. Such a call gets no command timeout at all.
const waitForever time.Duration = -1

// defaultCommandTimeout is Options.CommandTimeout when it is left 0.
const defaultCommandTimeout = 30 * time.Second

// commandWait returns the wait the RESP-encoded command at the start of b carries on top
// of Options.CommandTimeout (0: not a blocking command, waitForever: no deadline), and the
// bytes after the command. A plain command costs a length and first-byte check and
// allocates nothing. Blocking waits, the same rule in every Ruvio client:
//
//	BLPOP, BRPOP, BZPOPMIN, BZPOPMAX, BRPOPLPUSH, BLMOVE   last argument, seconds
//	BLMPOP, BZMPOP                                         first argument, seconds
//	XREAD, XREADGROUP                                      BLOCK ms (absent: not blocking)
//	WAIT, WAITAOF                                          last argument, ms
//	FILLGET key fill_ms WAIT wait_ms                       wait_ms
func commandWait(b []byte) (time.Duration, []byte) {
	n, b, ok := respInt(b, '*')
	if !ok || n < 1 {
		return 0, nil
	}
	name, b, ok := respBulk(b)
	if !ok {
		return 0, nil
	}
	n--
	if !waitCandidate(name) {
		return 0, skipBulks(b, n)
	}
	return classifyWait(name, b, n)
}

// requestWait is the widest wait of the commands in b (one command, or a pipeline).
func requestWait(b []byte) time.Duration {
	var widest time.Duration
	for len(b) > 0 {
		var w time.Duration
		w, b = commandWait(b)
		if w == waitForever {
			return waitForever
		}
		if w > widest {
			widest = w
		}
	}
	return widest
}

// waitCandidate reports whether name has the length and first letter of a command
// classifyWait knows; GET, SET, PING, ... stop here.
func waitCandidate(name []byte) bool {
	if len(name) < 4 {
		return false
	}
	switch first := name[0] | 0x20; len(name) {
	case 4:
		return first == 'w'
	case 5:
		return first == 'b' || first == 'x'
	case 6, 8:
		return first == 'b'
	case 7:
		return first == 'w' || first == 'f' || first == 'm'
	case 9:
		return first == 's'
	case 10:
		return first == 'b' || first == 'x' || first == 'p' || first == 's'
	}
	return false
}

func classifyWait(name, b []byte, n int) (time.Duration, []byte) {
	is := func(known string) bool { return bytes.EqualFold(name, []byte(known)) }
	// Collect up to the argument we need, then skip the rest.
	switch {
	case is("BLPOP"), is("BRPOP"), is("BZPOPMIN"), is("BZPOPMAX"), is("BRPOPLPUSH"), is("BLMOVE"):
		var last []byte
		for i := 0; i < n; i++ {
			var ok bool
			if last, b, ok = respBulk(b); !ok {
				return waitForever, nil
			}
		}
		if n == 0 {
			return waitForever, b
		}
		return waitSeconds(last), b
	case is("BLMPOP"), is("BZMPOP"):
		if n == 0 {
			return waitForever, b
		}
		first, rest, ok := respBulk(b)
		if !ok {
			return waitForever, nil
		}
		return waitSeconds(first), skipBulks(rest, n-1)
	case is("XREAD"), is("XREADGROUP"):
		w := time.Duration(0)
		skip := 0
		if len(name) == 10 {
			skip = 3 // GROUP <group> <consumer>: names that may spell BLOCK
		}
		done := false
		for i := 0; i < n; i++ {
			arg, rest, ok := respBulk(b)
			if !ok {
				return waitForever, nil
			}
			b = rest
			if i < skip || done {
				continue
			}
			if bytes.EqualFold(arg, []byte("STREAMS")) {
				done = true
			} else if bytes.EqualFold(arg, []byte("BLOCK")) && i+1 < n {
				ms, rest, ok := respBulk(b)
				if !ok {
					return waitForever, nil
				}
				b = rest
				i++
				w = waitMillis(ms)
				done = true
			}
		}
		return w, b
	case is("WAIT"), is("WAITAOF"):
		var last []byte
		for i := 0; i < n; i++ {
			var ok bool
			if last, b, ok = respBulk(b); !ok {
				return waitForever, nil
			}
		}
		if n == 0 {
			return waitForever, b
		}
		return waitMillis(last), b
	case is("FILLGET"):
		w := time.Duration(0)
		var prev []byte
		for i := 0; i < n; i++ {
			arg, rest, ok := respBulk(b)
			if !ok {
				return waitForever, nil
			}
			b = rest
			if i == 3 && bytes.EqualFold(prev, []byte("WAIT")) {
				w = waitMillis(arg)
			}
			prev = arg
		}
		return w, b
	case is("MONITOR"), is("SUBSCRIBE"), is("PSUBSCRIBE"), is("SSUBSCRIBE"):
		return waitForever, skipBulks(b, n)
	}
	return 0, skipBulks(b, n)
}

// waitSeconds reads a timeout in seconds, possibly fractional ("0.5").
func waitSeconds(arg []byte) time.Duration {
	f, err := strconv.ParseFloat(b2s(arg), 64)
	if err != nil || !(f > 0) || f > math.MaxInt64/float64(time.Second) {
		return waitForever
	}
	return time.Duration(f * float64(time.Second))
}

// waitMillis reads a timeout in whole milliseconds.
func waitMillis(arg []byte) time.Duration {
	ms, err := strconv.ParseInt(b2s(arg), 10, 64)
	if err != nil || ms <= 0 || ms > math.MaxInt64/int64(time.Millisecond) {
		return waitForever
	}
	return time.Duration(ms) * time.Millisecond
}

// respInt reads "<prefix><digits>\r\n" at the start of b.
func respInt(b []byte, prefix byte) (int, []byte, bool) {
	if len(b) < 4 || b[0] != prefix {
		return 0, nil, false
	}
	n := 0
	i := 1
	for ; i < len(b) && b[i] >= '0' && b[i] <= '9'; i++ {
		n = n*10 + int(b[i]-'0')
		if n > 1<<30 {
			return 0, nil, false
		}
	}
	if i == 1 || i+1 >= len(b) || b[i] != '\r' || b[i+1] != '\n' {
		return 0, nil, false
	}
	return n, b[i+2:], true
}

// respBulk reads one "$<len>\r\n<data>\r\n" at the start of b.
func respBulk(b []byte) ([]byte, []byte, bool) {
	n, b, ok := respInt(b, '$')
	if !ok || len(b) < n+2 {
		return nil, nil, false
	}
	return b[:n], b[n+2:], true
}

func skipBulks(b []byte, n int) []byte {
	for i := 0; i < n; i++ {
		var ok bool
		if _, b, ok = respBulk(b); !ok {
			return nil
		}
	}
	return b
}
