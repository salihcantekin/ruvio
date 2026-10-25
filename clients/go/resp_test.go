// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"bytes"
	"errors"
	"math"
	"strings"
	"testing"
)

func TestEncodeTyped(t *testing.T) {
	r := newCmd(5, cSET).s("k").s("value").t(tPX).i(1500)
	want := "*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$4\r\n1500\r\n"
	if string(r.buf) != want {
		t.Fatalf("got %q", r.buf)
	}
	putReq(r)
	r = newCmd(4, cZADD).s("z").f(1.5).f(math.Inf(-1))
	if !strings.Contains(string(r.buf), "$3\r\n1.5\r\n$4\r\n-inf\r\n") {
		t.Fatalf("float encode %q", r.buf)
	}
	putReq(r)
	big := strings.Repeat("x", 12)
	r = newCmd(12, cDEL)
	for i := 0; i < 11; i++ {
		r.s(big)
	}
	if !strings.HasPrefix(string(r.buf), "*12\r\n$3\r\nDEL\r\n$12\r\n") {
		t.Fatalf("header %q", r.buf[:24])
	}
	putReq(r)
}

func TestAppendCommand(t *testing.T) {
	b, err := AppendCommand(nil, "SET", []byte{0, 255}, 42, int64(-7), uint64(9), 2.25, true)
	if err != nil {
		t.Fatal(err)
	}
	want := "*7\r\n$3\r\nSET\r\n$2\r\n\x00\xff\r\n$2\r\n42\r\n$2\r\n-7\r\n$1\r\n9\r\n$4\r\n2.25\r\n$1\r\n1\r\n"
	if string(b) != want {
		t.Fatalf("got %q", b)
	}
	if _, err := AppendCommand(nil, struct{}{}); err == nil {
		t.Fatal("expected unsupported type error")
	}
}

func TestParseScalars(t *testing.T) {
	cases := []struct {
		in   string
		kind Kind
		str  string
		n    int64
	}{
		{"+OK\r\n", KindSimple, "OK", 0},
		{"-ERR bad\r\n", KindError, "ERR bad", 0},
		{":-42\r\n", KindInt, "-42", -42},
		{"$5\r\nhello\r\n", KindBulk, "hello", 0},
		{"$0\r\n\r\n", KindBulk, "", 0},
		{"$-1\r\n", KindNil, "", 0},
		{"*-1\r\n", KindNil, "", 0},
		{"_\r\n", KindNil, "", 0},
		{"#t\r\n", KindBool, "1", 1},
		{",3.5\r\n", KindDouble, "3.5", 0},
		{"(12345678901234567890\r\n", KindBigNum, "12345678901234567890", 0},
		{"=7\r\ntxt:abc\r\n", KindVerbatim, "txt:abc", 0},
		{"!3\r\nERR\r\n", KindError, "ERR", 0},
	}
	for _, c := range cases {
		v, n, err := ParseReply([]byte(c.in))
		if err != nil || n != len(c.in) {
			t.Fatalf("%q: n=%d err=%v", c.in, n, err)
		}
		if v.Kind != c.kind || v.Str() != c.str || (c.kind == KindInt && v.Int != c.n) {
			t.Fatalf("%q: got %+v", c.in, v)
		}
	}
}

func TestParseAggregates(t *testing.T) {
	in := "*3\r\n:1\r\n*2\r\n$1\r\na\r\n$-1\r\n*0\r\n"
	v, n, err := ParseReply([]byte(in))
	if err != nil || n != len(in) {
		t.Fatal(err, n)
	}
	if v.String() != "[1 [a (nil)] []]" {
		t.Fatalf("got %s", v)
	}
	m, _, err := ParseReply([]byte("%2\r\n+a\r\n:1\r\n+b\r\n:2\r\n"))
	if err != nil || m.Kind != KindMap || len(m.Array) != 4 {
		t.Fatalf("map %v %v", m, err)
	}
	// attribute is skipped
	a, _, err := ParseReply([]byte("|1\r\n+k\r\n+v\r\n:5\r\n"))
	if err != nil || a.Kind != KindInt || a.Int != 5 {
		t.Fatalf("attr %v %v", a, err)
	}
	p, _, err := ParseReply([]byte(">2\r\n+invalidate\r\n*1\r\n$1\r\nk\r\n"))
	if err != nil || p.Kind != KindPush {
		t.Fatalf("push %v %v", p, err)
	}
}

// TestParseResumable feeds a reply one byte at a time, as the connection reader does
// with fragmented reads, and checks the parser resumes without reparsing.
func TestParseResumable(t *testing.T) {
	in := []byte("*2\r\n$11\r\nhello world\r\n*2\r\n:7\r\n+x\r\n:99\r\n")
	var p parser
	var buf []byte
	var got []Value
	r := 0
	for i := 0; i < len(in); i++ {
		buf = append(buf, in[i])
		for r < len(buf) {
			v, n, done, err := p.next(buf[r:])
			if err != nil {
				t.Fatal(err)
			}
			r += n
			if !done {
				break
			}
			got = append(got, v)
		}
	}
	if len(got) != 2 || got[0].String() != "[hello world [7 x]]" || got[1].Int != 99 {
		t.Fatalf("got %v", got)
	}
}

func TestParseErrors(t *testing.T) {
	for _, in := range []string{"?x\r\n", ":abc\r\n", "$3\r\nabcd\r\n", "+a\rb", "$67108865\r\n", "*1048577\r\n", "%1048577\r\n"} {
		_, _, err := ParseReply([]byte(in))
		if !errors.Is(err, ErrProtocol) {
			t.Fatalf("%q: want protocol error, got %v", in, err)
		}
	}
	if _, _, err := ParseReply([]byte("$10\r\nabc")); err != errIncomplete {
		t.Fatalf("want incomplete, got %v", err)
	}
}

func TestParseBigBulk(t *testing.T) {
	payload := bytes.Repeat([]byte("z"), 1<<20)
	in := append([]byte("$1048576\r\n"), payload...)
	in = append(in, '\r', '\n')
	v, _, err := ParseReply(in)
	if err != nil || !bytes.Equal(v.Bytes, payload) {
		t.Fatal("big bulk mismatch", err)
	}
}

func TestAllocations(t *testing.T) {
	var p parser
	intReply := []byte(":12345\r\n")
	if a := testing.AllocsPerRun(1000, func() { p.next(intReply) }); a != 0 {
		t.Fatalf("int reply allocs = %v, want 0", a)
	}
	okReply := []byte("+OK\r\n")
	if a := testing.AllocsPerRun(1000, func() { p.next(okReply) }); a > 1 {
		t.Fatalf("simple reply allocs = %v, want <= 1", a)
	}
	bulk := []byte("$5\r\nhello\r\n")
	if a := testing.AllocsPerRun(1000, func() { p.next(bulk) }); a != 1 {
		t.Fatalf("bulk reply allocs = %v, want 1 (the value)", a)
	}
	// The request comes from a sync.Pool, which the race detector empties at random.
	if raceEnabled {
		return
	}
	if a := testing.AllocsPerRun(1000, func() {
		r := newCmd(3, cSET).s("key:1").s("value").i(42)
		putReq(r)
	}); a != 0 {
		t.Fatalf("encode allocs = %v, want 0", a)
	}
}

func TestKeySlot(t *testing.T) {
	// Values from the Redis cluster spec / CLUSTER KEYSLOT.
	if s := KeySlot("foo"); s != 12182 {
		t.Fatalf("foo=%d", s)
	}
	if KeySlot("{user1000}.following") != KeySlot("{user1000}.followers") {
		t.Fatal("hashtag")
	}
	if s := KeySlot("123456789"); s != 0x31C3&16383 {
		t.Fatalf("crc check %d", s)
	}
}

func TestRing(t *testing.T) {
	var q ring
	next, want := 0, 0
	rs := make([]*request, 500)
	for i := range rs {
		rs[i] = &request{}
	}
	for next < len(rs) {
		for k := 0; k < 7 && next < len(rs); k++ { // push 7, pop 3: forces wrap and growth
			q.push(rs[next])
			next++
		}
		for k := 0; k < 3 && q.size > 0; k++ {
			if q.peek() != rs[want] {
				t.Fatalf("order at %d", want)
			}
			q.pop()
			want++
		}
	}
	for q.size > 0 {
		if q.peek() != rs[want] {
			t.Fatalf("order at %d", want)
		}
		q.pop()
		want++
	}
	if want != len(rs) || q.peek() != nil {
		t.Fatal("ring drained wrong")
	}
}
