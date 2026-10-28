// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"time"
)

// XMessage is one stream entry: ID is the entry id ("1700000000000-0": millisecond time,
// then a sequence number) and Values holds the fields as field, value, field, value ...
// Use Map for field lookup.
type XMessage struct {
	// ID is the entry id, "<ms>-<seq>".
	ID string
	// Values holds the fields as a flat list: name, value, name, value ... (see Map).
	Values []string
}

// Map returns the entry's fields as a map. If a field name is repeated, the last value
// wins.
func (m XMessage) Map() map[string]string {
	out := make(map[string]string, len(m.Values)/2)
	for i := 0; i+1 < len(m.Values); i += 2 {
		out[m.Values[i]] = m.Values[i+1]
	}
	return out
}

// XStream is the entries read from one stream by XRead: the stream key and its messages.
type XStream struct {
	// Stream is the stream key.
	Stream string
	// Messages are the entries read, oldest first.
	Messages []XMessage
}

func toXMessages(v Value) ([]XMessage, error) {
	if v.Kind == KindNil {
		return nil, nil
	}
	if !v.isAgg() {
		return nil, unexpected(v)
	}
	// Tolerate a single unwrapped entry [id, [fields]]: Ruvio 0.3.0 answers a blocked
	// XREAD that wakes up in this shape.
	if len(v.Array) == 2 && v.Array[0].Kind == KindBulk && v.Array[1].isAgg() {
		vals, err := v.Array[1].Strings()
		return []XMessage{{ID: v.Array[0].Str(), Values: vals}}, err
	}
	out := make([]XMessage, 0, len(v.Array))
	for _, e := range v.Array {
		if e.Kind == KindNil || len(e.Array) < 2 {
			if len(e.Array) == 1 {
				out = append(out, XMessage{ID: e.Array[0].Str()})
			} else if e.Kind == KindBulk {
				out = append(out, XMessage{ID: e.Str()})
			}
			continue
		}
		vals, err := e.Array[1].Strings()
		if err != nil {
			return nil, err
		}
		out = append(out, XMessage{ID: e.Array[0].Str(), Values: vals})
	}
	return out, nil
}

func toXStreams(v Value) ([]XStream, error) {
	if v.Kind == KindNil {
		return nil, nil
	}
	if !v.isAgg() {
		return nil, unexpected(v)
	}
	var out []XStream
	if v.Kind == KindMap { // RESP3
		for i := 0; i+1 < len(v.Array); i += 2 {
			msgs, err := toXMessages(v.Array[i+1])
			if err != nil {
				return nil, err
			}
			out = append(out, XStream{v.Array[i].Str(), msgs})
		}
		return out, nil
	}
	for _, s := range v.Array {
		if len(s.Array) != 2 {
			return nil, unexpected(v)
		}
		msgs, err := toXMessages(s.Array[1])
		if err != nil {
			return nil, err
		}
		out = append(out, XStream{s.Array[0].Str(), msgs})
	}
	return out, nil
}

// XAddArgs are the arguments of XAdd.
//
// Stream is the key. ID is "*" (the default when empty: the server generates ms-seq),
// "ms-*" (fixed millisecond part, generated sequence) or an explicit "ms-seq"; an
// explicit id must be greater than the last one or the server returns "ERR The ID
// specified in XADD is equal or smaller than the target stream top item". NoMkStream
// does not create a missing stream (XAdd then reports ok == false). MaxLen > 0 trims the
// stream to about that many entries after the add; MinID drops entries with a smaller
// id; if both are set MaxLen wins. Approx sends "~", but Ruvio always trims exactly.
// Delay (Ruvio) keeps the new entry hidden from XReadGroup for that long (milliseconds).
// TTL, when positive, sets a deadline on the stream only if this call creates it (EX for
// whole seconds, otherwise PX); an existing stream keeps its deadline, and a negative TTL
// returns ErrInvalidTTL.
// Values are the entry fields, field, value, ... in pairs, at least one pair.
type XAddArgs struct {
	// Stream is the stream key.
	Stream string
	// ID is the entry id; empty (the default) lets the server generate one ("*"). An explicit id must be greater than the last one.
	ID string
	// NoMkStream makes XAdd do nothing when the stream does not exist, instead of creating it.
	NoMkStream bool
	// MaxLen caps the stream length by trimming the oldest entries; 0 or less disables it. Takes precedence over MinID.
	MaxLen int64
	// MinID trims entries with an id lower than this; used only when MaxLen is not set.
	MinID string
	// Approx makes the trim approximate ("~"), which is cheaper; the stream may keep a few more entries than the limit.
	Approx bool
	// Trim, when set, replaces MaxLen, MinID and Approx: strategy, "=" or "~" mode and threshold in one typed option (see StreamTrim).
	Trim *StreamTrim
	// Delay hides the entry from consumer-group delivery until the duration has passed (Ruvio extension, sent as DELAY in milliseconds). 0 means deliver at once.
	Delay time.Duration
	// TTL sets a deadline on the stream when this call creates it (sent as EX for whole seconds, otherwise PX); an existing stream keeps its deadline. 0 means none.
	TTL time.Duration
	// Values are the entry fields as name, value, name, value ...; the count must be even and at least 2.
	Values []string
}

// XAdd appends an entry to a stream and returns its id (XADD). A missing stream is
// created unless a.NoMkStream is set, in which case ok is false and the id is empty. It
// wakes one blocked XReadGroup consumer and any blocked XRead without removing the
// entry. An odd number of a.Values, a bad id, or a key of another type (WRONGTYPE) is a
// server error.
//
//	id, _, err := c.XAdd(ctx, ruvio.XAddArgs{
//	    Stream: "events", MaxLen: 10000,
//	    Values: []string{"type", "signup", "user", "42"},
//	})
func (c *cmds) XAdd(ctx context.Context, a XAddArgs) (string, bool, error) {
	ttok, tv, err := optTTL(a.TTL)
	if err != nil {
		return "", false, err
	}
	n := 3 + len(a.Values)
	if ttok != nil {
		n += 2
	}
	if a.Delay > 0 {
		n += 2
	}
	if a.NoMkStream {
		n++
	}
	if a.Trim != nil {
		n += a.Trim.size()
	} else if a.MaxLen > 0 || a.MinID != "" {
		n += 2
		if a.Approx {
			n++
		}
	}
	r := newCmd(n, cXADD).s(a.Stream)
	if ttok != nil {
		r.t(ttok).i(tv)
	}
	if a.Delay > 0 {
		r.t(tDELAY).i(ms(a.Delay))
	}
	if a.NoMkStream {
		r.t(tNOMKSTREAM)
	}
	if a.Trim != nil {
		r.trim(*a.Trim)
	} else if a.MaxLen > 0 || a.MinID != "" {
		if a.MaxLen > 0 {
			r.t(tMAXLEN)
		} else {
			r.t(tMINID)
		}
		if a.Approx {
			r.t(tTILDE)
		}
		if a.MaxLen > 0 {
			r.i(a.MaxLen)
		} else {
			r.s(a.MinID)
		}
	}
	id := a.ID
	if id == "" {
		id = "*"
	}
	r.s(id).ss(a.Values)
	return c.strNil(ctx, r)
}

// XLen returns the number of entries in the stream (XLEN), 0 for a missing key.
func (c *cmds) XLen(ctx context.Context, stream string) (int64, error) {
	return c.intR(ctx, newCmd(2, cXLEN).s(stream))
}

func xrangeReq(name []byte, stream, a, b string, count int64) *request {
	if count > 0 {
		return newCmd(6, name).s(stream).s(a).s(b).t(tCOUNT).i(count)
	}
	return newCmd(4, name).s(stream).s(a).s(b)
}

// XRange returns the entries with ids from start to end, both inclusive, oldest first
// (XRANGE). Use "-" for the lowest id and "+" for the highest; ids are "ms-seq"
// (exclusive "(" bounds are not supported). count > 0 limits the number of entries, 0
// returns all. A missing stream gives an empty result.
//
//	msgs, err := c.XRange(ctx, "events", "-", "+", 100)
func (c *cmds) XRange(ctx context.Context, stream, start, end string, count int64) ([]XMessage, error) {
	v, err := c.value(ctx, xrangeReq(cXRANGE, stream, start, end, count))
	if err != nil {
		return nil, err
	}
	return toXMessages(v)
}

// XRevRange returns the entries from end down to start, newest first (XREVRANGE). Note
// the argument order: the higher bound comes first. count > 0 limits the result.
func (c *cmds) XRevRange(ctx context.Context, stream, end, start string, count int64) ([]XMessage, error) {
	v, err := c.value(ctx, xrangeReq(cXREVRANGE, stream, end, start, count))
	if err != nil {
		return nil, err
	}
	return toXMessages(v)
}

// BlockForever, used as XReadArgs.Block or XReadGroupArgs.Block, sends BLOCK 0: wait
// until an entry arrives (or ctx ends or the server shuts down).
const BlockForever time.Duration = -1

func blockMs(d time.Duration) int64 {
	if d < 0 {
		return 0
	}
	return ms(d)
}

// XReadArgs are the arguments of XRead. Streams and IDs pair up by position (same
// length). Each id is "$" (only entries added after the call starts), "0" (from the
// beginning) or the "ms-seq" of the last entry already seen; entries with a greater id
// are returned. Count > 0 limits entries per stream. Block == 0 does not block; Block >
// 0 waits up to that long (milliseconds, rounded up) for an entry; BlockForever waits
// until one arrives. A blocking read runs on a dedicated connection. All streams must
// share a hash slot in cluster mode.
type XReadArgs struct {
	// Streams are the stream keys to read.
	Streams []string
	// IDs are the last ids already seen, one per stream and in the same order; "$" means only entries added after the call starts (useful with Block).
	IDs []string
	// Count limits the entries returned per stream; 0 or less means no limit.
	Count int64
	// Block waits up to this long for new entries. 0 does not block; a positive value waits that long and returns nil on timeout. The wait uses a dedicated connection.
	Block time.Duration
}

// XRead reads entries from one or more streams (XREAD). It returns one XStream per
// stream that has new entries, or a nil slice when nothing arrived (including when a
// blocking read timed out). The reader's position is yours to keep: pass the last id you
// processed next time. For a consumer group use XReadGroup.
//
//	ss, err := c.XRead(ctx, ruvio.XReadArgs{
//	    Streams: []string{"events"}, IDs: []string{"$"},
//	    Block: 5 * time.Second, Count: 10,
//	})
func (c *cmds) XRead(ctx context.Context, a XReadArgs) ([]XStream, error) {
	n := 2 + len(a.Streams) + len(a.IDs)
	if a.Count > 0 {
		n += 2
	}
	if a.Block != 0 {
		n += 2
	}
	r := newCmd(n, cXREAD)
	if a.Count > 0 {
		r.t(tCOUNT).i(a.Count)
	}
	if a.Block != 0 {
		r.t(tBLOCK).i(blockMs(a.Block))
	}
	r.t(tSTREAMS).ss(a.Streams).ss(a.IDs)
	var v Value
	var err error
	if a.Block != 0 {
		v, err = c.ex.execBlocking(ctx, r)
	} else {
		v, err = c.value(ctx, r)
	}
	if err != nil {
		return nil, err
	}
	return toXStreams(v)
}

// XDel deletes entries by id (XDEL; ids are "ms-seq") and returns how many existed.
// Deleting does not change the stream's last generated id.
func (c *cmds) XDel(ctx context.Context, stream string, ids ...string) (int64, error) {
	return c.intR(ctx, newCmd(2+len(ids), cXDEL).s(stream).ss(ids))
}

// XSetID moves the stream's last generated id forward (XSETID) without storing an entry,
// so later XAdd ids with "*" are greater than it. id has the form "ms-seq".
func (c *cmds) XSetID(ctx context.Context, stream, id string) error {
	return c.ok(ctx, newCmd(3, cXSETID).s(stream).s(id))
}

// XTrim trims the stream and returns how many entries were removed (XTRIM). strategy is
// "MAXLEN" (threshold is the maximum number of entries to keep) or "MINID" (threshold is
// the smallest id to keep). approx sends "~", but Ruvio trims exactly either way. The
// LIMIT option is not offered by the server.
func (c *cmds) XTrim(ctx context.Context, stream, strategy, threshold string, approx bool) (int64, error) {
	if approx {
		return c.intR(ctx, newCmd(5, cXTRIM).s(stream).s(strategy).t(tTILDE).s(threshold))
	}
	return c.intR(ctx, newCmd(4, cXTRIM).s(stream).s(strategy).s(threshold))
}

// XTrimBy trims the stream with a typed option (XTRIM) and returns how many entries
// were removed. Mode may be "=" or "~" or empty; the server does not accept LIMIT.
//
//	n, err := c.XTrimBy(ctx, "events", ruvio.TrimMaxLen.Of(1000).WithMode(ruvio.TrimApproximate))
func (c *cmds) XTrimBy(ctx context.Context, stream string, t StreamTrim) (int64, error) {
	return c.intR(ctx, newCmd(2+t.size(), cXTRIM).s(stream).trim(t))
}

// XGroupCreate creates a consumer group on a stream (XGROUP CREATE stream group id). id
// is where the group starts reading: "$" for new entries only, "0" for everything, or an
// explicit "ms-seq". With mkStream the stream is created if missing; otherwise a missing
// stream is a server error. A group that already exists returns "BUSYGROUP Consumer
// Group name already exists".
func (c *cmds) XGroupCreate(ctx context.Context, stream, group, id string, mkStream bool) error {
	if mkStream {
		return c.ok(ctx, newCmd(6, cXGROUP).t(tCREATE).s(stream).s(group).s(id).t(tMKSTREAM))
	}
	return c.ok(ctx, newCmd(5, cXGROUP).t(tCREATE).s(stream).s(group).s(id))
}

// XGroupCreateConsumer registers a consumer in a group with no pending entries (XGROUP
// CREATECONSUMER) and returns true if it was new. XReadGroup also creates consumers on
// first use. A missing stream or group is a server error.
func (c *cmds) XGroupCreateConsumer(ctx context.Context, stream, group, consumer string) (bool, error) {
	return c.boolR(ctx, newCmd(5, cXGROUP).t(tCREATECONSUMER).s(stream).s(group).s(consumer))
}

// XGroupDestroy removes a group and its pending entries (XGROUP DESTROY) and returns
// true if the group existed.
func (c *cmds) XGroupDestroy(ctx context.Context, stream, group string) (bool, error) {
	return c.boolR(ctx, newCmd(4, cXGROUP).t(tDESTROY).s(stream).s(group))
}

// XGroupSetID moves the group's read cursor (XGROUP SETID) to id ("$", "0" or "ms-seq").
// Entries after it are delivered again by XReadGroup.
func (c *cmds) XGroupSetID(ctx context.Context, stream, group, id string) error {
	return c.ok(ctx, newCmd(5, cXGROUP).t(tSETID).s(stream).s(group).s(id))
}

// XGroupDelConsumer removes a consumer from a group (XGROUP DELCONSUMER) and returns how
// many pending entries it held; those entries are dropped, so claim them first if they
// matter.
func (c *cmds) XGroupDelConsumer(ctx context.Context, stream, group, consumer string) (int64, error) {
	return c.intR(ctx, newCmd(5, cXGROUP).t(tDELCONSUMER).s(stream).s(group).s(consumer))
}

// XReadGroupArgs are the arguments of XReadGroup. It reads one Stream as Consumer of
// Group and always asks for new entries (id ">"). Count > 0 limits the batch. Block
// works as in XReadArgs. NoAck delivers entries without adding them to the pending list.
type XReadGroupArgs struct {
	// Group is the consumer group name.
	Group string
	// Consumer is the consumer name inside the group; it is created on first use.
	Consumer string
	// Stream is the stream key to read.
	Stream string
	// Count limits the entries returned; 0 or less means no limit.
	Count int64
	// Block waits up to this long for new entries; 0 does not block. The wait uses a dedicated connection.
	Block time.Duration
	// NoAck delivers entries without adding them to the pending list, so they cannot be acknowledged or claimed.
	NoAck bool
}

// XReadGroup delivers new entries of a stream to a consumer (XREADGROUP GROUP group
// consumer ... STREAMS stream >). Each entry goes to one consumer of the group and,
// unless NoAck is set, stays in the pending list until XAck. It returns an empty slice
// when nothing is available or a blocking read timed out. A missing stream or group
// returns "NOGROUP No such key or consumer group". Entries added with a Delay are not
// delivered before it passes. To re-read your own unacknowledged entries use XPendingExt
// and XClaim. Blocking calls run on a dedicated connection and wake the oldest waiting
// consumer first.
func (c *cmds) XReadGroup(ctx context.Context, a XReadGroupArgs) ([]XMessage, error) {
	n := 7
	if a.Count > 0 {
		n += 2
	}
	if a.Block != 0 {
		n += 2
	}
	if a.NoAck {
		n++
	}
	r := newCmd(n, cXREADGROUP).t(tGROUP).s(a.Group).s(a.Consumer)
	if a.Count > 0 {
		r.t(tCOUNT).i(a.Count)
	}
	if a.Block != 0 {
		r.t(tBLOCK).i(blockMs(a.Block))
	}
	if a.NoAck {
		r.t(tNOACK)
	}
	r.t(tSTREAMS).s(a.Stream).s(">")
	var v Value
	var err error
	if a.Block != 0 {
		v, err = c.ex.execBlocking(ctx, r)
	} else {
		v, err = c.value(ctx, r)
	}
	if err != nil {
		return nil, err
	}
	ss, err := toXStreams(v)
	if err != nil || len(ss) == 0 {
		return nil, err
	}
	return ss[0].Messages, nil
}

// XAck acknowledges entries for a group (XACK), removing them from the pending list, and
// returns how many were pending.
func (c *cmds) XAck(ctx context.Context, stream, group string, ids ...string) (int64, error) {
	return c.intR(ctx, newCmd(3+len(ids), cXACK).s(stream).s(group).ss(ids))
}

// XPendingSummary is the short XPENDING reply: Count pending entries, the Lowest and
// Highest pending ids (empty when Count is 0) and Consumers, the pending count per
// consumer name.
type XPendingSummary struct {
	// Count is the number of pending entries in the group.
	Count int64
	// Lowest is the smallest pending entry id.
	Lowest string
	// Highest is the largest pending entry id.
	Highest string
	// Consumers maps each consumer name to its number of pending entries.
	Consumers map[string]int64
}

// XPending returns the pending-entries summary of a group (XPENDING stream group). A
// missing stream or group returns NOGROUP.
func (c *cmds) XPending(ctx context.Context, stream, group string) (XPendingSummary, error) {
	v, err := c.value(ctx, newCmd(3, cXPENDING).s(stream).s(group))
	if err != nil {
		return XPendingSummary{}, err
	}
	if len(v.Array) != 4 {
		return XPendingSummary{}, unexpected(v)
	}
	s := XPendingSummary{Count: v.Array[0].Int, Lowest: v.Array[1].Str(), Highest: v.Array[2].Str(),
		Consumers: map[string]int64{}}
	for _, e := range v.Array[3].Array {
		if len(e.Array) == 2 {
			n, _ := e.Array[1].AsInt64()
			s.Consumers[e.Array[0].Str()] = n
		}
	}
	return s, nil
}

// XPendingExtArgs are the arguments of XPendingExt. Start and End are inclusive id
// bounds ("-" and "+" for all); Count is the maximum number of rows and is required.
// Idle > 0 keeps only entries idle at least that long (milliseconds). Consumer, when
// set, keeps one consumer's entries.
type XPendingExtArgs struct {
	// Stream is the stream key.
	Stream string
	// Group is the consumer group name.
	Group string
	// Idle keeps only entries idle for at least this long; 0 means no filter.
	Idle time.Duration
	// Start is the lowest id to include; "-" means the smallest.
	Start string
	// End is the highest id to include; "+" means the largest.
	End string
	// Count is the maximum number of entries to return.
	Count int64
	// Consumer keeps only the entries held by this consumer; empty means all.
	Consumer string
}

// XPendingEntry is one row of the extended XPENDING: the entry ID, the Consumer that
// holds it, how long it has been idle (Idle) and how many times it was delivered
// (Deliveries).
type XPendingEntry struct {
	// ID is the entry id.
	ID string
	// Consumer is the consumer that holds the entry.
	Consumer string
	// Idle is the time since the entry was last delivered.
	Idle time.Duration
	// Deliveries is how many times the entry was delivered.
	Deliveries int64
}

// XPendingExt lists pending entries of a group (XPENDING stream group [IDLE ms] start
// end count [consumer]). Use it to find entries a crashed consumer never acknowledged,
// then XClaim or XAutoClaim them.
func (c *cmds) XPendingExt(ctx context.Context, a XPendingExtArgs) ([]XPendingEntry, error) {
	n := 6
	if a.Idle > 0 {
		n += 2
	}
	if a.Consumer != "" {
		n++
	}
	r := newCmd(n, cXPENDING).s(a.Stream).s(a.Group)
	if a.Idle > 0 {
		r.t(tIDLE).i(ms(a.Idle))
	}
	r.s(a.Start).s(a.End).i(a.Count)
	if a.Consumer != "" {
		r.s(a.Consumer)
	}
	v, err := c.value(ctx, r)
	if err != nil {
		return nil, err
	}
	out := make([]XPendingEntry, 0, len(v.Array))
	for _, e := range v.Array {
		if len(e.Array) != 4 {
			return nil, unexpected(v)
		}
		out = append(out, XPendingEntry{e.Array[0].Str(), e.Array[1].Str(),
			time.Duration(e.Array[2].Int) * time.Millisecond, e.Array[3].Int})
	}
	return out, nil
}

// XAutoClaimArgs are the arguments of XAutoClaim. Entries pending for at least MinIdle
// are moved to Consumer, scanning ids from Start ("0-0" to begin). Count > 0 limits the
// batch (the server uses 100 when it is not given).
type XAutoClaimArgs struct {
	// Stream is the stream key.
	Stream string
	// Group is the consumer group name.
	Group string
	// Consumer is the consumer that takes ownership.
	Consumer string
	// MinIdle is the minimum idle time of an entry to be claimed.
	MinIdle time.Duration
	// Start is the id to scan from; "0-0" starts at the beginning, later calls pass the cursor returned by the previous one.
	Start string
	// Count is the maximum number of entries to claim; 0 or less uses the server default.
	Count int64
}

func xautoclaimReq(a *XAutoClaimArgs, justID bool) *request {
	n := 6
	if a.Count > 0 {
		n += 2
	}
	if justID {
		n++
	}
	r := newCmd(n, cXAUTOCLAIM).s(a.Stream).s(a.Group).s(a.Consumer).i(ms(a.MinIdle)).s(a.Start)
	if a.Count > 0 {
		r.t(tCOUNT).i(a.Count)
	}
	if justID {
		r.t(tJUSTID)
	}
	return r
}

// XAutoClaim moves idle pending entries to a consumer (XAUTOCLAIM). It returns the id to
// pass as Start next time ("0-0" when the scan finished), the claimed entries and the
// ids that were dropped from the pending list because the entry was deleted. The server
// examines a bounded number of pending entries per call, so call it in a loop until the
// returned id is "0-0".
func (c *cmds) XAutoClaim(ctx context.Context, a XAutoClaimArgs) (string, []XMessage, []string, error) {
	v, err := c.value(ctx, xautoclaimReq(&a, false))
	if err != nil {
		return "", nil, nil, err
	}
	if len(v.Array) < 2 {
		return "", nil, nil, unexpected(v)
	}
	msgs, err := toXMessages(v.Array[1])
	var deleted []string
	if len(v.Array) > 2 {
		deleted, _ = v.Array[2].Strings()
	}
	return v.Array[0].Str(), msgs, deleted, err
}

// XAutoClaimJustID is XAutoClaim with JUSTID: it returns the next start id and only the
// ids of the claimed entries, without their fields,.
func (c *cmds) XAutoClaimJustID(ctx context.Context, a XAutoClaimArgs) (string, []string, error) {
	v, err := c.value(ctx, xautoclaimReq(&a, true))
	if err != nil {
		return "", nil, err
	}
	if len(v.Array) < 2 {
		return "", nil, unexpected(v)
	}
	ids, err := v.Array[1].Strings()
	return v.Array[0].Str(), ids, err
}

// XClaimArgs are the arguments of XClaim. Entries in IDs that have been pending at least
// MinIdle are given to Consumer. Idle sets their idle time and Time sets their
// last-delivery time as an absolute time (do not set both). RetryCount sets the delivery
// counter. Force claims ids that are not pending. LastID moves the group's last
// delivered id.
type XClaimArgs struct {
	// Stream is the stream key.
	Stream string
	// Group is the consumer group name.
	Group string
	// Consumer is the consumer that takes ownership.
	Consumer string
	// MinIdle skips entries idle for less than this long.
	MinIdle time.Duration
	// IDs are the entry ids to claim.
	IDs []string
	// Idle sets the entry idle time after the claim; 0 leaves it unset.
	Idle time.Duration
	// Time sets the last-delivery time to this instant; the zero time leaves it unset.
	Time time.Time
	// RetryCount sets the delivery counter; 0 or less leaves it unset.
	RetryCount int64
	// Force claims ids that are not in the pending list.
	Force bool
	// LastID sets the group last-delivered id; empty leaves it unset.
	LastID string
}

func xclaimReq(a *XClaimArgs, justID bool) *request {
	n := 5 + len(a.IDs)
	if a.Idle > 0 {
		n += 2
	}
	if !a.Time.IsZero() {
		n += 2
	}
	if a.RetryCount > 0 {
		n += 2
	}
	if a.Force {
		n++
	}
	if justID {
		n++
	}
	if a.LastID != "" {
		n += 2
	}
	r := newCmd(n, cXCLAIM).s(a.Stream).s(a.Group).s(a.Consumer).i(ms(a.MinIdle)).ss(a.IDs)
	if a.Idle > 0 {
		r.t(tIDLE).i(ms(a.Idle))
	}
	if !a.Time.IsZero() {
		r.t(tTIME).i(a.Time.UnixMilli())
	}
	if a.RetryCount > 0 {
		r.t(tRETRYCOUNT).i(a.RetryCount)
	}
	if a.Force {
		r.t(tFORCE)
	}
	if justID {
		r.t(tJUSTID)
	}
	if a.LastID != "" {
		r.t(tLASTID).s(a.LastID)
	}
	return r
}

// XClaim takes ownership of pending entries (XCLAIM) and returns the claimed entries
// with their fields. Entries that are not pending, or not idle long enough, are skipped.
func (c *cmds) XClaim(ctx context.Context, a XClaimArgs) ([]XMessage, error) {
	v, err := c.value(ctx, xclaimReq(&a, false))
	if err != nil {
		return nil, err
	}
	return toXMessages(v)
}

// XClaimJustID is XClaim with JUSTID: it returns only the ids of the claimed entries.
func (c *cmds) XClaimJustID(ctx context.Context, a XClaimArgs) ([]string, error) {
	return c.strs(ctx, xclaimReq(&a, true))
}

// XInfoStream returns XINFO STREAM as a map of field name to raw Value (length,
// last-generated-id, groups, first-entry and so on). A missing key returns a server
// error.
func (c *cmds) XInfoStream(ctx context.Context, stream string) (map[string]Value, error) {
	v, err := c.value(ctx, newCmd(3, cXINFO).t(tSTREAM).s(stream))
	if err != nil {
		return nil, err
	}
	return toValueMap(v)
}

func toMapList(v Value) ([]map[string]string, error) {
	if !v.isAgg() {
		return nil, unexpected(v)
	}
	out := make([]map[string]string, 0, len(v.Array))
	for _, e := range v.Array {
		m, err := toStrMap(e)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

// XInfoGroups returns one map per consumer group of the stream (XINFO GROUPS), with
// fields such as name, consumers, pending and last-delivered-id as strings.
func (c *cmds) XInfoGroups(ctx context.Context, stream string) ([]map[string]string, error) {
	v, err := c.value(ctx, newCmd(3, cXINFO).t(tGROUPS).s(stream))
	if err != nil {
		return nil, err
	}
	return toMapList(v)
}

// XInfoConsumers returns one map per consumer of the group (XINFO CONSUMERS), with
// fields such as name, pending and idle as strings. A missing group returns NOGROUP.
func (c *cmds) XInfoConsumers(ctx context.Context, stream, group string) ([]map[string]string, error) {
	v, err := c.value(ctx, newCmd(4, cXINFO).t(tCONSUMERS).s(stream).s(group))
	if err != nil {
		return nil, err
	}
	return toMapList(v)
}
