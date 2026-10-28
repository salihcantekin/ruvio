package ruvio

import "strconv"

// Typed command keywords. Each type is a string type, so an untyped string
// literal such as "km" or "LEFT" still compiles where the type is expected.
// The known values encode through pre-built RESP fragments (no per-call
// allocation); any other value is sent as given and the server decides.

var (
	kwM       = preEnc("m")
	kwKM      = preEnc("km")
	kwFT      = preEnc("ft")
	kwMI      = preEnc("mi")
	kwASC     = preEnc("ASC")
	kwDESC    = preEnc("DESC")
	kwLEFT    = preEnc("LEFT")
	kwRIGHT   = preEnc("RIGHT")
	kwBYTE    = preEnc("BYTE")
	kwAND     = preEnc("AND")
	kwOR      = preEnc("OR")
	kwXOR     = preEnc("XOR")
	kwNOT     = preEnc("NOT")
	kwSUM     = preEnc("SUM")
	kwMIN     = preEnc("MIN")
	kwSET     = preEnc("SET")
	kwINCRBY  = preEnc("INCRBY")
	kwOVERFLW = preEnc("OVERFLOW")
	kwWRAP    = preEnc("WRAP")
	kwSAT     = preEnc("SAT")
	kwFAIL    = preEnc("FAIL")

	kwScanTypes = map[ScanType][]byte{
		ScanString: preEnc("string"), ScanJSON: preEnc("json"), ScanList: preEnc("list"),
		ScanSet: preEnc("set"), ScanHash: preEnc("hash"), ScanZSet: preEnc("zset"),
		ScanBloomFilter: preEnc("bloomfilter"), ScanStream: preEnc("stream"),
		ScanLease: preEnc("lease"), ScanLimit: preEnc("limit"),
		ScanSemaphore: preEnc("semaphore"), ScanOnce: preEnc("once"),
	}
)

// GeoUnit is a GEO distance unit. Valid values: GeoMeters ("m"), GeoKilometers
// ("km"), GeoFeet ("ft"), GeoMiles ("mi"). The zero value means meters.
//
//	d, ok, err := c.GeoDist(ctx, "cities", "a", "b", ruvio.GeoKilometers)
type GeoUnit string

// GEO distance units.
const (
	GeoMeters     GeoUnit = "m"
	GeoKilometers GeoUnit = "km"
	GeoFeet       GeoUnit = "ft"
	GeoMiles      GeoUnit = "mi"
)

func (u GeoUnit) token() []byte {
	switch u {
	case "m", "":
		return kwM
	case "km":
		return kwKM
	case "ft":
		return kwFT
	case "mi":
		return kwMI
	}
	return nil
}

// ExpireCondition is the optional condition of EXPIRE, PEXPIRE, EXPIREAT and PEXPIREAT.
// Valid values: ExpireNx ("NX", set only when the key has no deadline), ExpireXx ("XX",
// only when it has one), ExpireGt ("GT", only when the new deadline is later) and
// ExpireLt ("LT", only when it is earlier). A key without a deadline counts as having an
// infinite one. The zero value means no condition.
//
//	ok, err := c.ExpireIf(ctx, "session:1", time.Hour, ruvio.ExpireGt)
type ExpireCondition string

// EXPIRE conditions.
const (
	ExpireNx ExpireCondition = "NX"
	ExpireXx ExpireCondition = "XX"
	ExpireGt ExpireCondition = "GT"
	ExpireLt ExpireCondition = "LT"
)

// n is the number of arguments the condition adds.
func (e ExpireCondition) n() int {
	if e == "" {
		return 0
	}
	return 1
}

// append writes the condition token, if any. Unknown values are sent as given and the
// server decides.
func (e ExpireCondition) append(r *request) *request {
	switch e {
	case "":
	case ExpireNx:
		r.t(tNX)
	case ExpireXx:
		r.t(tXX)
	case ExpireGt:
		r.t(tGT)
	case ExpireLt:
		r.t(tLT)
	default:
		r.s(string(e))
	}
	return r
}

// GeoOrder is a GEO result order. Valid values: GeoAsc ("ASC"), GeoDesc ("DESC");
// the zero value leaves the order unspecified.
//
//	q := ruvio.GeoSearchQuery{Radius: 5, Unit: ruvio.GeoKilometers, Sort: ruvio.GeoAsc}
type GeoOrder string

// GEO result orders.
const (
	GeoAsc  GeoOrder = "ASC"
	GeoDesc GeoOrder = "DESC"
)

func (o GeoOrder) token() []byte {
	switch o {
	case "ASC":
		return kwASC
	case "DESC":
		return kwDESC
	}
	return nil
}

// ListSide names the end of a list: ListLeft ("LEFT") or ListRight ("RIGHT").
// Used by LMove, BLMove, LMPop and BLMPop.
//
//	v, ok, err := c.LMove(ctx, "src", "dst", ruvio.ListLeft, ruvio.ListRight)
type ListSide string

// List ends.
const (
	ListLeft  ListSide = "LEFT"
	ListRight ListSide = "RIGHT"
)

func (s ListSide) token() []byte {
	switch s {
	case "LEFT":
		return kwLEFT
	case "RIGHT":
		return kwRIGHT
	}
	return nil
}

// ListPosition is the LINSERT position: InsertBefore ("BEFORE") or InsertAfter ("AFTER").
//
//	n, err := c.LInsertAt(ctx, "k", ruvio.InsertBefore, "pivot", "value")
type ListPosition string

// LINSERT positions.
const (
	InsertBefore ListPosition = "BEFORE"
	InsertAfter  ListPosition = "AFTER"
)

func (p ListPosition) token() []byte {
	switch p {
	case "BEFORE":
		return tBEFORE
	case "AFTER":
		return tAFTER
	}
	return nil
}

// ScanType filters SCAN by key type. Valid values: ScanString ("string"), ScanJSON
// ("json"), ScanList ("list"), ScanSet ("set"), ScanHash ("hash"), ScanZSet ("zset"),
// ScanBloomFilter ("bloomfilter"), ScanStream ("stream"), ScanLease ("lease"),
// ScanLimit ("limit"), ScanSemaphore ("semaphore"), ScanOnce ("once").
//
//	c.Scan(ctx, "0", ruvio.ScanOptions{Type: ruvio.ScanHash})
type ScanType string

// SCAN key types.
const (
	ScanString      ScanType = "string"
	ScanJSON        ScanType = "json"
	ScanList        ScanType = "list"
	ScanSet         ScanType = "set"
	ScanHash        ScanType = "hash"
	ScanZSet        ScanType = "zset"
	ScanBloomFilter ScanType = "bloomfilter"
	ScanStream      ScanType = "stream"
	ScanLease       ScanType = "lease"
	ScanLimit       ScanType = "limit"
	ScanSemaphore   ScanType = "semaphore"
	ScanOnce        ScanType = "once"
)

func (t ScanType) token() []byte { return kwScanTypes[t] }

// BitOperation is the BITOP operation: BitAnd, BitOr, BitXor or BitNot.
//
//	n, err := c.BitOp(ctx, ruvio.BitAnd, "dest", "a", "b")
type BitOperation string

// BITOP operations.
const (
	BitAnd BitOperation = "AND"
	BitOr  BitOperation = "OR"
	BitXor BitOperation = "XOR"
	BitNot BitOperation = "NOT"
)

func (o BitOperation) token() []byte {
	switch o {
	case "AND":
		return kwAND
	case "OR":
		return kwOR
	case "XOR":
		return kwXOR
	case "NOT":
		return kwNOT
	}
	return nil
}

// BitUnit is the BITCOUNT range unit: BitUnitByte ("BYTE") or BitUnitBit ("BIT").
//
//	n, err := c.BitCountUnit(ctx, "k", 0, 15, ruvio.BitUnitBit)
type BitUnit string

// BITCOUNT range units.
const (
	BitUnitByte BitUnit = "BYTE"
	BitUnitBit  BitUnit = "BIT"
)

func (u BitUnit) token() []byte {
	switch u {
	case "BYTE":
		return kwBYTE
	case "BIT":
		return tBIT
	}
	return nil
}

// Aggregate is the ZUNION/ZINTER score combiner: AggSum, AggMin or AggMax; the
// zero value leaves the server default (SUM).
//
//	z := ruvio.ZStore{Keys: []string{"a", "b"}, Aggregate: ruvio.AggMax}
type Aggregate string

// Score aggregation modes.
const (
	AggSum Aggregate = "SUM"
	AggMin Aggregate = "MIN"
	AggMax Aggregate = "MAX"
)

func (a Aggregate) token() []byte {
	switch a {
	case "SUM":
		return kwSUM
	case "MIN":
		return kwMIN
	case "MAX":
		return tMAX
	}
	return nil
}

// BitFieldOpName is a BITFIELD sub-command: BitFieldGet, BitFieldSet,
// BitFieldIncrBy or BitFieldOverflow (lower case is accepted too).
//
//	ruvio.BitFieldOp{Op: ruvio.BitFieldSet, Type: ruvio.BitU8, Offset: "0", Value: 7}
type BitFieldOpName string

// BITFIELD sub-commands.
const (
	BitFieldGet      BitFieldOpName = "GET"
	BitFieldSet      BitFieldOpName = "SET"
	BitFieldIncrBy   BitFieldOpName = "INCRBY"
	BitFieldOverflow BitFieldOpName = "OVERFLOW"
)

func (o BitFieldOpName) token() []byte {
	switch o {
	case "GET", "get":
		return tGET
	case "SET", "set":
		return kwSET
	case "INCRBY", "incrby":
		return kwINCRBY
	case "OVERFLOW", "overflow":
		return kwOVERFLW
	}
	return nil
}

// BITFIELD OVERFLOW modes, used as BitFieldOp.Type when Op is BitFieldOverflow.
const (
	BitOverflowWrap = "WRAP"
	BitOverflowSat  = "SAT"
	BitOverflowFail = "FAIL"
)

func overflowToken(s string) []byte {
	switch s {
	case "WRAP":
		return kwWRAP
	case "SAT":
		return kwSAT
	case "FAIL":
		return kwFAIL
	}
	return nil
}

// Common BITFIELD field types.
const (
	BitI8  = "i8"
	BitI16 = "i16"
	BitI32 = "i32"
	BitI64 = "i64"
	BitU8  = "u8"
	BitU16 = "u16"
	BitU32 = "u32"
)

// BitSigned returns the BITFIELD type for a signed field of the given width (1..64).
//
//	ruvio.BitFieldOp{Op: ruvio.BitFieldGet, Type: ruvio.BitSigned(5), Offset: "#1"}
func BitSigned(bits int) string { return "i" + strconv.Itoa(bits) }

// BitUnsigned returns the BITFIELD type for an unsigned field of the given width (1..63).
func BitUnsigned(bits int) string { return "u" + strconv.Itoa(bits) }

// kw appends tok when known, else the raw value.
func (r *request) kw(tok []byte, raw string) *request {
	if tok != nil {
		return r.t(tok)
	}
	return r.s(raw)
}

var (
	kwEQ = preEnc("=")

	kwInfoSections = map[InfoSection][]byte{
		InfoServer: preEnc("server"), InfoStats: preEnc("stats"), InfoBuffers: preEnc("buffers"),
		InfoMemory: preEnc("memory"), InfoMemoryBreakdown: preEnc("memorybreakdown"),
		InfoKeyspace: preEnc("keyspace"), InfoSnapshot: preEnc("snapshot"),
		InfoCluster: preEnc("cluster"), InfoReplication: preEnc("replication"),
		InfoCommandCounters: preEnc("commandcounters"), InfoLatencyStats: preEnc("latencystats"),
		InfoLogs: preEnc("logs"), InfoSlabs: preEnc("slabs"), InfoAll: preEnc("all"),
		InfoDefault: preEnc("default"), InfoPrometheus: preEnc("prometheus"),
	}

	kwClientTypes = map[ClientType][]byte{
		ClientNormal: preEnc("normal"), ClientMaster: preEnc("master"),
		ClientReplica: preEnc("replica"), ClientPubSub: preEnc("pubsub"),
	}
)

// TrimStrategy is the stream trim criterion: TrimMaxLen ("MAXLEN") keeps at most N
// entries, TrimMinID ("MINID") drops entries with an id below the threshold.
//
//	t := ruvio.StreamTrim{Strategy: ruvio.TrimMinID, Threshold: "1700000000000-0"}
type TrimStrategy string

// Stream trim strategies.
const (
	TrimMaxLen TrimStrategy = "MAXLEN"
	TrimMinID  TrimStrategy = "MINID"
)

func (s TrimStrategy) token() []byte {
	switch s {
	case "MAXLEN":
		return tMAXLEN
	case "MINID":
		return tMINID
	}
	return nil
}

// TrimMode is the optional trim modifier: TrimExact ("=") or TrimApproximate ("~").
// The zero value sends no modifier. Ruvio trims exactly in both modes. The server does
// not accept LIMIT.
//
//	t := ruvio.TrimMaxLen.Of(1000).WithMode(ruvio.TrimApproximate)
type TrimMode string

// Stream trim modes.
const (
	TrimExact       TrimMode = "="
	TrimApproximate TrimMode = "~"
)

func (m TrimMode) token() []byte {
	switch m {
	case "=":
		return kwEQ
	case "~":
		return tTILDE
	}
	return nil
}

// StreamTrim is one trim option shared by XADD (XAddArgs.Trim) and XTRIM (XTrimBy):
// strategy MAXLEN or MINID, an optional mode "=" or "~", and the threshold (an entry
// count for MAXLEN, a "ms-seq" id for MINID).
//
//	id, _, err := c.XAdd(ctx, ruvio.XAddArgs{
//	    Stream: "events", Trim: ruvio.TrimMaxLen.Of(10000).WithMode(ruvio.TrimExact),
//	    Values: []string{"k", "v"},
//	})
type StreamTrim struct {
	// Strategy is TrimMaxLen or TrimMinID.
	Strategy TrimStrategy
	// Mode is TrimExact, TrimApproximate or empty (no modifier).
	Mode TrimMode
	// Threshold is the maximum entry count (MAXLEN) or the smallest id to keep (MINID).
	Threshold string
}

// Of returns a StreamTrim with this strategy and a numeric threshold (MAXLEN count).
//
//	t := ruvio.TrimMaxLen.Of(500)
func (s TrimStrategy) Of(n int64) StreamTrim {
	return StreamTrim{Strategy: s, Threshold: strconv.FormatInt(n, 10)}
}

// OfID returns a StreamTrim with this strategy and an id threshold (MINID).
//
//	t := ruvio.TrimMinID.OfID("1700000000000-0")
func (s TrimStrategy) OfID(id string) StreamTrim {
	return StreamTrim{Strategy: s, Threshold: id}
}

// WithMode returns a copy of t with the given mode.
func (t StreamTrim) WithMode(m TrimMode) StreamTrim {
	t.Mode = m
	return t
}

// size is the number of arguments the trim adds to a command.
func (t StreamTrim) size() int {
	if t.Mode != "" {
		return 3
	}
	return 2
}

func (r *request) trim(t StreamTrim) *request {
	r.kw(t.Strategy.token(), string(t.Strategy))
	if t.Mode != "" {
		r.kw(t.Mode.token(), string(t.Mode))
	}
	return r.s(t.Threshold)
}

// InfoSection names an INFO section. Valid values: InfoServer, InfoStats, InfoBuffers,
// InfoMemory, InfoMemoryBreakdown, InfoKeyspace, InfoSnapshot, InfoCluster,
// InfoReplication, InfoCommandCounters, InfoLatencyStats, InfoLogs, InfoSlabs, plus
// InfoAll, InfoDefault and InfoPrometheus (Prometheus text format). The server returns
// an empty reply for an unknown name.
//
//	text, err := c.InfoOf(ctx, ruvio.InfoMemory)
type InfoSection string

// INFO sections.
const (
	InfoServer          InfoSection = "server"
	InfoStats           InfoSection = "stats"
	InfoBuffers         InfoSection = "buffers"
	InfoMemory          InfoSection = "memory"
	InfoMemoryBreakdown InfoSection = "memorybreakdown"
	InfoKeyspace        InfoSection = "keyspace"
	InfoSnapshot        InfoSection = "snapshot"
	InfoCluster         InfoSection = "cluster"
	InfoReplication     InfoSection = "replication"
	InfoCommandCounters InfoSection = "commandcounters"
	InfoLatencyStats    InfoSection = "latencystats"
	InfoLogs            InfoSection = "logs"
	InfoSlabs           InfoSection = "slabs"
	InfoAll             InfoSection = "all"
	InfoDefault         InfoSection = "default"
	InfoPrometheus      InfoSection = "prometheus"
)

func (s InfoSection) token() []byte { return kwInfoSections[s] }

// ClientType filters CLIENT LIST TYPE. Valid values: ClientNormal ("normal"),
// ClientMaster ("master"), ClientReplica ("replica"), ClientPubSub ("pubsub").
//
//	text, err := c.ClientListType(ctx, ruvio.ClientPubSub)
type ClientType string

// CLIENT LIST connection types.
const (
	ClientNormal  ClientType = "normal"
	ClientMaster  ClientType = "master"
	ClientReplica ClientType = "replica"
	ClientPubSub  ClientType = "pubsub"
)

func (t ClientType) token() []byte { return kwClientTypes[t] }

// Stream id constants for XADD, XGROUP, XREAD, XREADGROUP and the XRANGE family.
const (
	// StreamIDNew asks XADD to generate the id ("*").
	StreamIDNew = "*"
	// StreamIDLastDelivered is the last id of the stream at call time ("$").
	StreamIDLastDelivered = "$"
	// StreamIDNewMessages reads only messages never delivered to the group (">").
	StreamIDNewMessages = ">"
	// StreamIDMin is the lowest possible id for range bounds ("-").
	StreamIDMin = "-"
	// StreamIDMax is the highest possible id for range bounds ("+").
	StreamIDMax = "+"
)

// Sorted-set score and lex bounds, accepted wherever a method takes a min/max string.
const (
	// ScoreNegInf is the lowest score bound ("-inf").
	ScoreNegInf = "-inf"
	// ScorePosInf is the highest score bound ("+inf").
	ScorePosInf = "+inf"
	// LexMin is the lowest lex bound ("-").
	LexMin = "-"
	// LexMax is the highest lex bound ("+").
	LexMax = "+"
)

// ScoreExclusive returns the exclusive score bound "(x".
//
//	n, err := c.ZCount(ctx, "board", ruvio.ScoreExclusive(5), ruvio.ScorePosInf)
func ScoreExclusive(x float64) string {
	return string(strconv.AppendFloat(append(make([]byte, 0, 24), '('), x, 'f', -1, 64))
}

// LexInclusive returns the inclusive lex bound "[member".
//
//	ruvio.LexInclusive("a") // "[a"
func LexInclusive(member string) string { return "[" + member }

// LexExclusive returns the exclusive lex bound "(member".
//
//	ruvio.LexExclusive("a") // "(a"
func LexExclusive(member string) string { return "(" + member }
