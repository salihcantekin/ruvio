// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import "bytes"

// slotCount is the number of cluster hash slots.
const slotCount = 16384

var crcTable = func() (t [256]uint16) {
	for i := range t {
		crc := uint16(i) << 8
		for b := 0; b < 8; b++ {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
		t[i] = crc
	}
	return
}()

// hashSlot is CRC16-XMODEM of the key, or of the first non-empty {tag} in it, mod 16384.
func hashSlot(key []byte) int {
	if i := bytes.IndexByte(key, '{'); i >= 0 {
		if j := bytes.IndexByte(key[i+1:], '}'); j > 0 {
			key = key[i+1 : i+1+j]
		}
	}
	var crc uint16
	for _, b := range key {
		crc = crc<<8 ^ crcTable[byte(crc>>8)^b]
	}
	return int(crc) & (slotCount - 1)
}

// KeySlot returns the cluster hash slot (0 to 16383) of key: CRC16-XMODEM modulo 16384.
// If the key contains a non-empty {hashtag}, only the text inside the first braces is
// hashed.
//
//	ruvio.KeySlot("{user:1}:name") == ruvio.KeySlot("{user:1}:email") // true
func KeySlot(key string) int {
	if i := indexByte(key, '{'); i >= 0 {
		if j := indexByte(key[i+1:], '}'); j > 0 {
			key = key[i+1 : i+1+j]
		}
	}
	var crc uint16
	for k := 0; k < len(key); k++ {
		crc = crc<<8 ^ crcTable[byte(crc>>8)^key[k]]
	}
	return int(crc) & (slotCount - 1)
}

func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// fanKind marks commands that act on a whole cluster rather than one node.
type fanKind uint8

const (
	fanNone   fanKind = iota
	fanSum            // DBSIZE: replies are added
	fanFirst          // FLUSHDB, FLUSHALL, SCRIPT LOAD ...: sent to every node, first reply returned
	fanConcat         // KEYS: arrays are joined
	fanSub            // like fanFirst, only for subcommands LOAD/FLUSH/DELETE/RESTORE
)

// cmdSpec describes how the router treats one command.
type cmdSpec struct {
	keys  *commandKeyRule // where the keys are, from the server's command table
	split bool            // keyed multi-key command that is split per slot when its keys differ
	fan   fanKind
	scan  bool // SCAN: walks every node
}

// Server flag bits of a command (see scripts/gen_client_keys.py).
const (
	flagReadonly = 2
	flagBlocking = 16
)

// reads reports whether the command only reads and may go to a replica: flagged readonly
// by the server, and not blocking.
func (s cmdSpec) reads() bool {
	f := s.keys.flags
	return f&flagReadonly != 0 && f&flagBlocking == 0
}

var (
	// firstKey routes a command missing from the table by its first argument.
	firstKey = commandKeyRule{kind: ruleRange, a: 1, b: 1, c: 1}
	// allKeys routes SSUBSCRIBE and SUNSUBSCRIBE by every channel.
	allKeys = commandKeyRule{kind: ruleRange, a: 1, b: -1, c: 1}
)

// cmdSpecs is the server's command table (command_keys_gen.go) with the router's own
// choices on top: fan-out, split, SCAN and channel routing for sharded Pub/Sub.
var cmdSpecs = func() map[string]cmdSpec {
	m := make(map[string]cmdSpec, len(commandKeyRules))
	rules := make([]commandKeyRule, 0, len(commandKeyRules)) // never grows: the pointers stay valid
	for n, r := range commandKeyRules {
		rules = append(rules, r)
		m[n] = cmdSpec{keys: &rules[len(rules)-1]}
	}
	set := func(n string, s cmdSpec) {
		s.keys = m[n].keys
		m[n] = s
	}
	// scripts and functions: LOAD/FLUSH/DELETE must reach every node.
	set("SCRIPT", cmdSpec{fan: fanSub})
	set("FUNCTION", cmdSpec{fan: fanSub})
	set("DBSIZE", cmdSpec{fan: fanSum})
	set("FLUSHDB", cmdSpec{fan: fanFirst})
	set("FLUSHALL", cmdSpec{fan: fanFirst})
	set("KEYS", cmdSpec{fan: fanConcat})
	set("SCAN", cmdSpec{scan: true})
	// keyed multi-key commands that are split per slot
	for _, n := range []string{"DEL", "UNLINK", "EXISTS", "TOUCH", "MGET", "MSET"} {
		set(n, cmdSpec{split: true})
	}
	// sharded Pub/Sub goes to the node owning the channel's slot.
	m["SPUBLISH"] = cmdSpec{keys: &firstKey}
	m["SSUBSCRIBE"] = cmdSpec{keys: &allKeys}
	m["SUNSUBSCRIBE"] = cmdSpec{keys: &allKeys}
	return m
}()

// specOf looks a command name up case-insensitively without allocating.
func specOf(name []byte) (cmdSpec, bool) {
	var up [32]byte
	if len(name) > len(up) {
		return cmdSpec{keys: &firstKey}, false
	}
	for i, c := range name {
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		up[i] = c
	}
	s, ok := cmdSpecs[string(up[:len(name)])]
	if !ok {
		return cmdSpec{keys: &firstKey}, false
	}
	return s, true
}

// ---- reading an encoded command frame ---------------------------------------------------

// frameHeader reads "*N\r\n" at b[0:] and returns N and the offset of the first bulk.
func frameHeader(b []byte) (n, off int, ok bool) {
	if len(b) < 4 || b[0] != '*' {
		return 0, 0, false
	}
	i := 1
	for ; i < len(b) && b[i] != '\r'; i++ {
		if b[i] < '0' || b[i] > '9' || n > 1<<24 {
			return 0, 0, false
		}
		n = n*10 + int(b[i]-'0')
	}
	if i+1 >= len(b) || b[i+1] != '\n' {
		return 0, 0, false
	}
	return n, i + 2, true
}

// frameBulk reads one "$len\r\n...\r\n" at b[off:] and returns the payload and the next offset.
func frameBulk(b []byte, off int) (arg []byte, next int, ok bool) {
	if off >= len(b) || b[off] != '$' {
		return nil, 0, false
	}
	i, n := off+1, 0
	for ; i < len(b) && b[i] != '\r'; i++ {
		if b[i] < '0' || b[i] > '9' || n > 1<<30 {
			return nil, 0, false
		}
		n = n*10 + int(b[i]-'0')
	}
	start := i + 2
	end := start + n
	if end+2 > len(b) {
		return nil, 0, false
	}
	return b[start:end], end + 2, true
}

// frameArgs appends up to max arguments (all when max <= 0) of the frame at b[0:] to dst.
// It returns the number of arguments the frame declares and the offset just past the
// arguments that were read.
func frameArgs(b []byte, max int, dst [][]byte) (args [][]byte, n, end int, ok bool) {
	n, off, ok := frameHeader(b)
	if !ok {
		return dst, 0, 0, false
	}
	want := n
	if max > 0 && max < n {
		want = max
	}
	for i := 0; i < want; i++ {
		var a []byte
		if a, off, ok = frameBulk(b, off); !ok {
			return dst, n, off, false
		}
		dst = append(dst, a)
	}
	return dst, n, off, true
}

// frameEnd returns the offset just past the frame at b[0:].
func frameEnd(b []byte) (int, bool) {
	n, off, ok := frameHeader(b)
	if !ok {
		return 0, false
	}
	for i := 0; i < n; i++ {
		if _, off, ok = frameBulk(b, off); !ok {
			return 0, false
		}
	}
	return off, true
}

// ---- key extraction ----------------------------------------------------------------------------

func upperEq(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		if c != s[i] {
			return false
		}
	}
	return true
}

func atoiBytes(b []byte) (int, bool) {
	if len(b) == 0 || len(b) > 9 {
		return 0, false
	}
	n := 0
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}

// keysOf appends the keys of the command args (args[0] is the name) to dst, in argument
// order, as rule r places them. Keyless commands add nothing. The returned slices alias args.
func keysOf(r *commandKeyRule, args [][]byte, dst [][]byte) [][]byte {
	n := len(args)
	switch r.kind {
	case ruleRange:
		last := int(r.b)
		if last < 0 {
			last += n
		}
		for i := int(r.a); i <= last && i < n; i += int(r.c) {
			dst = append(dst, args[i])
		}
	case ruleNumKeys:
		at := int(r.a)
		if r.b > 0 && int(r.b) < n {
			dst = append(dst, args[r.b])
		}
		if at < n {
			if c, ok := atoiBytes(args[at]); ok {
				if c > n-at-1 {
					c = n - at - 1
				}
				dst = append(dst, args[at+1:at+1+c]...)
			}
		}
	case ruleKeywordHalf:
		for i := 1; i < n; i++ {
			if upperEq(args[i], r.keywords[0]) {
				rest := n - i - 1
				return append(dst, args[i+1:i+1+rest/2]...)
			}
		}
	case ruleKeyAndStore:
		first := int(r.a)
		if first < n {
			dst = append(dst, args[first])
		}
		for i := first + 1; i+1 < n; i++ {
			for _, w := range r.keywords {
				if upperEq(args[i], w) {
					i++
					dst = append(dst, args[i])
					break
				}
			}
		}
	}
	return dst
}

// CommandKeys returns the keys the router would use for a command, in argument order;
// args[0] is the command name. It returns nil for commands without keys or for an empty
// call. It is meant for tests and tooling.
//
//	ruvio.CommandKeys("MGET", "a", "b") // ["a" "b"]
func CommandKeys(args ...string) []string {
	bs := make([][]byte, len(args))
	for i, a := range args {
		bs[i] = []byte(a)
	}
	if len(bs) == 0 {
		return nil
	}
	spec, _ := specOf(bs[0])
	var out []string
	for _, k := range keysOf(spec.keys, bs, nil) {
		out = append(out, string(k))
	}
	return out
}
