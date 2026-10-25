// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"errors"
	"math"
	"strconv"
	"unsafe"
)

// Kind is the RESP type of a reply.
type Kind uint8

const (
	// KindNil is a null bulk, null array or RESP3 null.
	KindNil Kind = iota
	// KindSimple is a simple string such as +OK.
	KindSimple
	// KindError is an error reply; Bytes holds the message.
	KindError
	// KindInt is an integer reply; Int holds it.
	KindInt
	// KindBulk is a bulk string; Bytes holds it.
	KindBulk
	// KindArray is an array; Array holds the elements.
	KindArray
	// KindDouble is a RESP3 double.
	KindDouble // RESP3 ','
	// KindBool is a RESP3 boolean; Int is 0 or 1.
	KindBool // RESP3 '#'
	// KindMap is a RESP3 map, stored flat in Array.
	KindMap // RESP3 '%', stored flat as key, value, key, value...
	// KindSet is a RESP3 set.
	KindSet // RESP3 '~'
	// KindPush is a RESP3 out-of-band push (pub/sub, tracking).
	KindPush // RESP3 '>'
	// KindBigNum is a RESP3 big number; Bytes holds its digits.
	KindBigNum // RESP3 '('
	// KindVerbatim is a RESP3 verbatim string; Bytes holds it.
	KindVerbatim // RESP3 '='
)

// Value is one parsed server reply. Use Kind to see what it holds, or the conversion
// methods (Str, AsInt64, AsFloat64, Strings). Bytes owns its memory and is never a view
// of the connection buffer, so it is safe to keep.
//
// Kind says which field is set: Int for KindInt, KindBool (0 or 1) and KindDouble (IEEE
// bits; use AsFloat64); Bytes for KindSimple, KindError, KindBulk, KindBigNum and
// KindVerbatim; Array for KindArray, KindSet, KindPush and KindMap (flat: key, value,
// key, value ...).
type Value struct {
	// Kind says which of the fields below is set.
	Kind  Kind
	Int   int64   // KindInt, KindBool (0/1); KindDouble keeps its IEEE bits here (use AsFloat64)
	Bytes []byte  // KindSimple, KindError, KindBulk, KindBigNum, KindVerbatim
	Array []Value // KindArray, KindMap (flat), KindSet, KindPush
}

// IsNil reports whether the reply is a null: a null bulk string, a null array or a RESP3
// null.
func (v Value) IsNil() bool { return v.Kind == KindNil }

// Str returns the text of a scalar reply (simple, bulk, error, big number, verbatim,
// integer, boolean or double). It returns "" for a null or an aggregate. For byte kinds
// the string shares memory with Bytes and does not copy.
func (v Value) Str() string {
	switch v.Kind {
	case KindSimple, KindBulk, KindError, KindBigNum, KindVerbatim:
		return b2s(v.Bytes)
	case KindInt, KindBool:
		return strconv.FormatInt(v.Int, 10)
	case KindDouble:
		return strconv.FormatFloat(math.Float64frombits(uint64(v.Int)), 'g', -1, 64)
	}
	return ""
}

// AsInt64 converts an integer, boolean, double (truncated) or numeric string to int64.
// It returns *UnexpectedReplyError for other kinds, or a parse error if the text is not
// a number.
func (v Value) AsInt64() (int64, error) {
	switch v.Kind {
	case KindInt, KindBool:
		return v.Int, nil
	case KindBulk, KindSimple, KindBigNum:
		return parseInt(v.Bytes)
	case KindDouble:
		return int64(math.Float64frombits(uint64(v.Int))), nil
	}
	return 0, unexpected(v)
}

// AsFloat64 converts a double, integer or numeric string to float64. It returns
// *UnexpectedReplyError for other kinds.
func (v Value) AsFloat64() (float64, error) {
	switch v.Kind {
	case KindDouble:
		return math.Float64frombits(uint64(v.Int)), nil
	case KindInt:
		return float64(v.Int), nil
	case KindBulk, KindSimple:
		return parseFloat(v.Bytes)
	}
	return 0, unexpected(v)
}

// Strings converts an array, set or map reply to a slice of strings, one per element;
// null elements become "". A null reply gives (nil, nil). Other kinds return
// *UnexpectedReplyError.
func (v Value) Strings() ([]string, error) {
	if v.Kind == KindNil {
		return nil, nil
	}
	if !v.isAgg() {
		return nil, unexpected(v)
	}
	out := make([]string, len(v.Array))
	for i := range v.Array {
		out[i] = v.Array[i].Str()
	}
	return out, nil
}

func (v Value) isAgg() bool {
	return v.Kind == KindArray || v.Kind == KindSet || v.Kind == KindMap || v.Kind == KindPush
}

// String renders the value for debugging and logging. The format is not stable.
func (v Value) String() string {
	switch v.Kind {
	case KindNil:
		return "(nil)"
	case KindError:
		return "(error) " + b2s(v.Bytes)
	case KindArray, KindSet, KindMap, KindPush:
		b := []byte{'['}
		for i, e := range v.Array {
			if i > 0 {
				b = append(b, ' ')
			}
			b = append(b, e.String()...)
		}
		return string(append(b, ']'))
	}
	return v.Str()
}

// Error is a server error reply such as -ERR ..., -WRONGTYPE ... or -NOSCRIPT .... Typed
// methods and Do return it as *Error; test it with errors.As.
//
//	var e *ruvio.Error
//	if errors.As(err, &e) && e.Prefix() == "WRONGTYPE" { ... }
type Error struct {
	// Msg is the full server message, for example "WRONGTYPE Operation against a key
	// holding the wrong kind of value".
	Msg string
}

// Error returns the full server message, including its first word.
func (e *Error) Error() string { return e.Msg }

// Prefix returns the first word of the message (ERR, WRONGTYPE, NOSCRIPT, MOVED, BUSY
// ...), which is the error class.
func (e *Error) Prefix() string {
	for i := 0; i < len(e.Msg); i++ {
		if e.Msg[i] == ' ' {
			return e.Msg[:i]
		}
	}
	return e.Msg
}

// ErrProtocol is returned (usually wrapped) when the server sends a malformed reply. The
// connection is closed because the stream can no longer be trusted.
var ErrProtocol = errors.New("ruvio: protocol error")

var errIncomplete = errors.New("incomplete")

func unexpected(v Value) error {
	if v.Kind == KindError {
		return &Error{Msg: string(v.Bytes)}
	}
	return &UnexpectedReplyError{Value: v}
}

// UnexpectedReplyError is returned when a reply has a shape the typed method does not
// expect, for example an integer where an array was required. Value holds the offending
// reply. It usually means the server is not Ruvio or Redis compatible for that command.
type UnexpectedReplyError struct {
	// Value is the reply that did not have the expected shape.
	Value Value
}

func (e *UnexpectedReplyError) Error() string {
	return "ruvio: unexpected reply " + e.Value.String()
}

// b2s views bytes as a string without copying. Only used on bytes the client owns and
// never mutates afterwards.
func b2s(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

func parseInt(b []byte) (int64, error) {
	if len(b) == 0 || len(b) > 20 {
		return strconv.ParseInt(string(b), 10, 64)
	}
	neg := false
	i := 0
	if b[0] == '-' {
		neg = true
		i = 1
		if len(b) == 1 {
			return 0, ErrProtocol
		}
	}
	var n uint64
	for ; i < len(b); i++ {
		c := b[i] - '0'
		if c > 9 {
			return strconv.ParseInt(string(b), 10, 64)
		}
		n = n*10 + uint64(c)
	}
	if len(b) >= 19 { // possible overflow: defer to strconv for exact checks
		return strconv.ParseInt(string(b), 10, 64)
	}
	if neg {
		return -int64(n), nil
	}
	return int64(n), nil
}

func parseFloat(b []byte) (float64, error) {
	switch string(b) { // no allocation: compiler optimizes string(b) in switch
	case "inf", "+inf", "Inf", "+Inf":
		return math.Inf(1), nil
	case "-inf", "-Inf":
		return math.Inf(-1), nil
	}
	return strconv.ParseFloat(b2s(b), 64)
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

// preEnc returns the complete RESP bulk fragment for a fixed token: $3\r\nGET\r\n.
func preEnc(s string) []byte {
	b := make([]byte, 0, len(s)+8)
	b = append(b, '$')
	b = strconv.AppendInt(b, int64(len(s)), 10)
	b = append(b, '\r', '\n')
	b = append(b, s...)
	return append(b, '\r', '\n')
}

func appendArrayHeader(b []byte, n int) []byte {
	if n < 10 {
		return append(b, '*', byte('0'+n), '\r', '\n')
	}
	b = append(b, '*')
	b = strconv.AppendInt(b, int64(n), 10)
	return append(b, '\r', '\n')
}

func appendLen(b []byte, n int) []byte {
	if n < 10 {
		return append(b, '$', byte('0'+n), '\r', '\n')
	}
	b = append(b, '$')
	b = strconv.AppendInt(b, int64(n), 10)
	return append(b, '\r', '\n')
}

func appendBulkString(b []byte, s string) []byte {
	b = appendLen(b, len(s))
	b = append(b, s...)
	return append(b, '\r', '\n')
}

func appendBulkBytes(b []byte, s []byte) []byte {
	b = appendLen(b, len(s))
	b = append(b, s...)
	return append(b, '\r', '\n')
}

func appendBulkInt(b []byte, n int64) []byte {
	var tmp [20]byte
	d := strconv.AppendInt(tmp[:0], n, 10)
	b = appendLen(b, len(d))
	b = append(b, d...)
	return append(b, '\r', '\n')
}

func appendBulkUint(b []byte, n uint64) []byte {
	var tmp [20]byte
	d := strconv.AppendUint(tmp[:0], n, 10)
	b = appendLen(b, len(d))
	b = append(b, d...)
	return append(b, '\r', '\n')
}

func appendBulkFloat(b []byte, f float64) []byte {
	var tmp [32]byte
	var d []byte
	switch {
	case math.IsInf(f, 1):
		d = append(tmp[:0], "+inf"...)
	case math.IsInf(f, -1):
		d = append(tmp[:0], "-inf"...)
	default:
		d = strconv.AppendFloat(tmp[:0], f, 'f', -1, 64)
	}
	b = appendLen(b, len(d))
	b = append(b, d...)
	return append(b, '\r', '\n')
}

// appendArg encodes one argument of the generic Do API.
func appendArg(b []byte, a any) ([]byte, error) {
	switch v := a.(type) {
	case string:
		return appendBulkString(b, v), nil
	case []byte:
		return appendBulkBytes(b, v), nil
	case int:
		return appendBulkInt(b, int64(v)), nil
	case int64:
		return appendBulkInt(b, v), nil
	case int32:
		return appendBulkInt(b, int64(v)), nil
	case uint:
		return appendBulkUint(b, uint64(v)), nil
	case uint64:
		return appendBulkUint(b, v), nil
	case uint32:
		return appendBulkUint(b, uint64(v)), nil
	case float64:
		return appendBulkFloat(b, v), nil
	case float32:
		return appendBulkFloat(b, float64(v)), nil
	case bool:
		if v {
			return appendBulkString(b, "1"), nil
		}
		return appendBulkString(b, "0"), nil
	case nil:
		return appendBulkString(b, ""), nil
	}
	return b, errors.New("ruvio: unsupported argument type")
}

// AppendCommand appends args to b as one RESP command frame and returns the extended
// slice. Argument types are the same as for Do. It returns an error for an unsupported
// type. It is exported for tooling and benchmarks; the typed methods encode without
// boxing.
func AppendCommand(b []byte, args ...any) ([]byte, error) {
	b = appendArrayHeader(b, len(args))
	var err error
	for _, a := range args {
		if b, err = appendArg(b, a); err != nil {
			return b, err
		}
	}
	return b, nil
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

// maxBulk bounds a single bulk length the parser will accept (64 MiB, as in the other clients).
const maxBulk = 64 << 20

// maxArray bounds the element count of one array, set, push or map (1,048,576, as in the other clients).
const maxArray = 1 << 20

type frame struct {
	arr  []Value
	i    int
	kind Kind
}

// parser is a resumable RESP2/RESP3 parser. It consumes complete tokens from a byte
// slice; when a reply is split across reads it keeps the partial aggregate on its
// stack, so large replies parse in linear time.
type parser struct {
	stack []frame
	// arena backs the bulk strings of one aggregate reply (one allocation instead of
	// one per element); it is dropped when the reply completes.
	arena []byte
	// need is a hint: bytes required from the current position to finish the next token.
	need int
}

// readLine returns the index of '\r' for a line starting at p, or -1.
func readLine(buf []byte, p int) int {
	for i := p; i+1 < len(buf); i++ {
		if buf[i] == '\r' {
			if buf[i+1] != '\n' {
				return -2
			}
			return i
		}
	}
	return -1
}

func parseLineInt(b []byte) (int64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	neg := false
	i := 0
	if b[0] == '-' {
		neg = true
		i = 1
	}
	if i >= len(b) {
		return 0, false
	}
	var n int64
	for ; i < len(b); i++ {
		c := b[i] - '0'
		if c > 9 {
			return 0, false
		}
		n = n*10 + int64(c)
	}
	if neg {
		n = -n
	}
	return n, true
}

// next parses as many tokens as possible from buf. It returns the number of bytes
// consumed and done=true with the value when a whole top-level reply is complete.
func (p *parser) next(buf []byte) (v Value, consumed int, done bool, err error) {
	pos := 0
outer:
	for {
		if pos >= len(buf) {
			p.need = 1
			return Value{}, pos, false, nil
		}
		t := buf[pos]
		le := readLine(buf, pos+1)
		if le == -1 {
			p.need = 0
			return Value{}, pos, false, nil
		}
		if le == -2 {
			return Value{}, pos, false, ErrProtocol
		}
		line := buf[pos+1 : le]
		after := le + 2
		var val Value
		agg := -1
		aggKind := KindArray
		switch t {
		case '+':
			val = Value{Kind: KindSimple, Bytes: p.simple(line)}
		case '-':
			val = Value{Kind: KindError, Bytes: cloneBytes(line)}
		case ':':
			n, ok := parseLineInt(line)
			if !ok {
				return Value{}, pos, false, ErrProtocol
			}
			val = Value{Kind: KindInt, Int: n}
		case '$', '=', '!':
			n, ok := parseLineInt(line)
			if !ok || n > maxBulk {
				return Value{}, pos, false, ErrProtocol
			}
			if n < 0 {
				val = Value{Kind: KindNil}
				break
			}
			end := after + int(n)
			if end+2 > len(buf) {
				p.need = end + 2 - pos
				return Value{}, pos, false, nil
			}
			if buf[end] != '\r' || buf[end+1] != '\n' {
				return Value{}, pos, false, ErrProtocol
			}
			k := KindBulk
			if t == '=' {
				k = KindVerbatim
			} else if t == '!' {
				k = KindError
			}
			val = Value{Kind: k, Bytes: p.clone(buf[after:end], len(buf)-end)}
			after = end + 2
		case '*', '~', '>', '%':
			n, ok := parseLineInt(line)
			if !ok || n > maxArray {
				return Value{}, pos, false, ErrProtocol
			}
			if n < 0 {
				val = Value{Kind: KindNil}
				break
			}
			switch t {
			case '~':
				aggKind = KindSet
			case '>':
				aggKind = KindPush
			case '%':
				aggKind = KindMap
				n *= 2
			}
			agg = int(n)
		case '_':
			val = Value{Kind: KindNil}
		case '#':
			if len(line) != 1 {
				return Value{}, pos, false, ErrProtocol
			}
			var b int64
			if line[0] == 't' {
				b = 1
			}
			val = Value{Kind: KindBool, Int: b}
		case ',':
			f, e := parseFloat(line)
			if e != nil {
				return Value{}, pos, false, ErrProtocol
			}
			val = Value{Kind: KindDouble, Int: int64(math.Float64bits(f))}
		case '(':
			val = Value{Kind: KindBigNum, Bytes: cloneBytes(line)}
		case '|':
			// RESP3 attribute: parse and discard by treating it as a map frame that is dropped.
			n, ok := parseLineInt(line)
			if !ok || n < 0 {
				return Value{}, pos, false, ErrProtocol
			}
			p.stack = append(p.stack, frame{arr: make([]Value, n*2), kind: KindNil})
			pos = after
			if n == 0 {
				p.stack = p.stack[:len(p.stack)-1]
			}
			continue
		default:
			return Value{}, pos, false, ErrProtocol
		}
		pos = after
		if agg > 0 {
			p.stack = append(p.stack, frame{arr: make([]Value, agg), kind: aggKind})
			continue
		}
		if agg == 0 {
			val = Value{Kind: aggKind, Array: []Value{}}
		}
		// place the completed value into its parent(s)
		for {
			if len(p.stack) == 0 {
				p.need = 0
				p.arena = nil
				return val, pos, true, nil
			}
			top := &p.stack[len(p.stack)-1]
			top.arr[top.i] = val
			top.i++
			if top.i < len(top.arr) {
				break
			}
			f := *top
			p.stack[len(p.stack)-1] = frame{}
			p.stack = p.stack[:len(p.stack)-1]
			if f.kind == KindNil { // finished attribute: drop and keep parsing
				continue outer
			}
			val = Value{Kind: f.kind, Array: f.arr}
		}
	}
}

// Shared, read-only bytes for the most common simple replies (no allocation).
var (
	okBytes     = []byte("OK")
	queuedBytes = []byte("QUEUED")
	pongBytes   = []byte("PONG")
)

func (p *parser) simple(line []byte) []byte {
	switch string(line) {
	case "OK":
		return okBytes
	case "QUEUED":
		return queuedBytes
	case "PONG":
		return pongBytes
	}
	return cloneBytes(line)
}

// clone copies a bulk payload. Inside an aggregate it carves the copy from an arena
// sized by the bytes still buffered (capped at 64 KiB), so an array of small bulks
// costs about one allocation.
func (p *parser) clone(b []byte, rest int) []byte {
	if len(p.stack) == 0 || len(b) > 4096 {
		return cloneBytes(b)
	}
	if cap(p.arena)-len(p.arena) < len(b) {
		n := rest + len(b)
		if n > 64<<10 {
			n = 64 << 10
		}
		if n < 256 {
			n = 256
		}
		p.arena = make([]byte, 0, n)
	}
	start := len(p.arena)
	p.arena = append(p.arena, b...)
	return p.arena[start:len(p.arena):len(p.arena)]
}

func cloneBytes(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// ParseReply parses one complete reply from the start of b and returns it with the
// number of bytes used. It returns an error if b holds an incomplete or malformed reply.
// It is the standalone form of the connection parser, for tests and tooling.
func ParseReply(b []byte) (Value, int, error) {
	var p parser
	v, n, done, err := p.next(b)
	if err != nil {
		return Value{}, 0, err
	}
	if !done {
		return Value{}, 0, errIncomplete
	}
	return v, n, nil
}
