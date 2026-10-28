// Copyright (c) 2026 Salih Cantekin. MIT License; see LICENSE.

package ruvio

import (
	"context"
	"time"
)

// first unwraps a single-element array reply (RedisJSON "$" paths) to its element.
func first(v Value) Value {
	if v.Kind == KindArray && len(v.Array) == 1 {
		return v.Array[0]
	}
	return v
}

func (c *cmds) jsonInt(ctx context.Context, r *request) (int64, bool, error) {
	v, err := c.value(ctx, r)
	if err != nil {
		return 0, false, err
	}
	v = first(v)
	if v.Kind == KindNil {
		return 0, false, nil
	}
	n, err := v.AsInt64()
	return n, err == nil, err
}

func pathReq(name []byte, key, path string) *request {
	if path == "" {
		return newCmd(2, name).s(key)
	}
	return newCmd(3, name).s(key).s(path)
}

// JSONGet returns the JSON text stored at key, or at path (JSON.GET key [path]). Without
// a path it returns the whole document ("$"). ok is false when the key or the path does
// not exist. The reply is compact JSON text (not RedisJSON's array wrapper for "$"
// paths). Ruvio accepts at most one path, so passing two or more paths returns a server
// error. A key that holds another type returns WRONGTYPE; so do string commands on a
// JSON key.
//
// Paths are limited to "$" (root), ".field" and "[N]" segments such as
// "$.users[0].name"; there are no wildcards or filters. A path may also start with "."
// or "[" (the legacy form).
//
//	s, ok, err := c.JSONGet(ctx, "user:1", "$.name") // `"Ada"`
func (c *cmds) JSONGet(ctx context.Context, key string, paths ...string) (string, bool, error) {
	return c.strNil(ctx, newCmd(2+len(paths), cJSON_GET).s(key).ss(paths))
}

// JSONSet writes the JSON text json at path (JSON.SET key path json). json must be valid
// JSON, otherwise "ERR invalid JSON value". A missing key can only be created at path
// "$". A final object field is created if missing; intermediate objects and array slots
// are not. A key of another type returns WRONGTYPE. The key's TTL is kept. Documents are
// limited to 16 MiB.
//
// When the target cannot be reached (for example a missing intermediate path) the server
// answers null and this method still returns nil, so it cannot tell that nothing was
// written; use JSONSetMode if you need to know. For Go values use JSONSetValue.
func (c *cmds) JSONSet(ctx context.Context, key, path, json string) error {
	return c.ok(ctx, newCmd(4, cJSON_SET).s(key).s(path).s(json))
}

// JSONSetMode is JSONSet with a condition: mode "NX" writes only when the path does not
// exist and "XX" only when it does. ok is false when the write was skipped (condition
// not met or target unreachable).
func (c *cmds) JSONSetMode(ctx context.Context, key, path, json, mode string) (bool, error) {
	v, err := c.value(ctx, newCmd(5, cJSON_SET).s(key).s(path).s(json).s(mode))
	return err == nil && v.Kind != KindNil, err
}

// JSONSetWithTTL is JSONSet that also sets a deadline when it creates the key
// (JSON.SET key path json EX|PX). EX is sent for whole seconds, PX otherwise; an
// existing key keeps its deadline. A ttl of zero or less returns ErrInvalidTTL.
func (c *cmds) JSONSetWithTTL(ctx context.Context, key, path, json string, ttl time.Duration) error {
	tok, v, err := ttlArg(ttl)
	if err != nil {
		return err
	}
	return c.ok(ctx, newCmd(6, cJSON_SET).s(key).s(path).s(json).t(tok).i(v))
}

// JSONSetModeWithTTL is JSONSetMode with a create-time deadline (JSON.SET key path json
// NX|XX EX|PX). ok is false when the write was skipped. A ttl of zero or less returns
// ErrInvalidTTL.
func (c *cmds) JSONSetModeWithTTL(ctx context.Context, key, path, json, mode string, ttl time.Duration) (bool, error) {
	tok, v, err := ttlArg(ttl)
	if err != nil {
		return false, err
	}
	r, err := c.value(ctx, newCmd(7, cJSON_SET).s(key).s(path).s(json).s(mode).t(tok).i(v))
	return err == nil && r.Kind != KindNil, err
}

// JSONDel removes the value at path (JSON.DEL key [path]); an empty path removes the
// whole key. It returns 1 when something was removed and 0 for a missing key or path.
func (c *cmds) JSONDel(ctx context.Context, key, path string) (int64, error) {
	return c.intR(ctx, pathReq(cJSON_DEL, key, path))
}

// JSONForget is the JSON.FORGET alias of JSONDel.
func (c *cmds) JSONForget(ctx context.Context, key, path string) (int64, error) {
	return c.intR(ctx, pathReq(cJSON_FORGET, key, path))
}

// JSONMGet reads the same path from several keys (JSON.MGET key ... path), in key order.
// An entry is not Valid for a missing key, a key that is not JSON, or a missing path.
// Note the argument order: path first, then keys. All keys must share a hash slot
// (ErrCrossSlot in cluster mode otherwise).
func (c *cmds) JSONMGet(ctx context.Context, path string, keys ...string) ([]NullString, error) {
	return c.nullStrs(ctx, newCmd(2+len(keys), cJSON_MGET).ss(keys).s(path))
}

// JSONMerge applies an RFC 7396 merge patch (JSON.MERGE key path patch): object members
// merge recursively, null members delete fields, and arrays and scalars replace the
// target. For example merging {"a":null} into {"a":1} gives {}. A missing key can only
// be created at "$". It is not RFC 6902 JSON Patch.
func (c *cmds) JSONMerge(ctx context.Context, key, path, patch string) error {
	return c.ok(ctx, newCmd(4, cJSON_MERGE).s(key).s(path).s(patch))
}

// JMerge is the JMERGE alias of JSONMerge.
func (c *cmds) JMerge(ctx context.Context, key, path, patch string) error {
	return c.ok(ctx, newCmd(4, cJMERGE).s(key).s(path).s(patch))
}

// JSONType returns the JSON type name at path (JSON.TYPE): "object", "array", "string",
// "integer", "number", "boolean" or "null". ok is false when the key or path does not
// exist.
func (c *cmds) JSONType(ctx context.Context, key, path string) (string, bool, error) {
	v, err := c.value(ctx, pathReq(cJSON_TYPE, key, path))
	if err != nil {
		return "", false, err
	}
	v = first(v)
	return v.Str(), v.Kind != KindNil, nil
}

// JSONStrLen returns the byte length of the JSON string at path (JSON.STRLEN). ok is
// false when the key or path is missing; a value of another JSON type returns WRONGTYPE.
func (c *cmds) JSONStrLen(ctx context.Context, key, path string) (int64, bool, error) {
	return c.jsonInt(ctx, pathReq(cJSON_STRLEN, key, path))
}

// JSONArrLen returns the length of the array at path (JSON.ARRLEN); ok is false when the
// key or path is missing, WRONGTYPE for a non-array.
func (c *cmds) JSONArrLen(ctx context.Context, key, path string) (int64, bool, error) {
	return c.jsonInt(ctx, pathReq(cJSON_ARRLEN, key, path))
}

// JSONObjLen returns the number of members of the object at path (JSON.OBJLEN); ok is
// false when missing.
func (c *cmds) JSONObjLen(ctx context.Context, key, path string) (int64, bool, error) {
	return c.jsonInt(ctx, pathReq(cJSON_OBJLEN, key, path))
}

// JSONObjKeys returns the member names of the object at path, in document order
// (JSON.OBJKEYS). A missing key or path gives an empty result.
func (c *cmds) JSONObjKeys(ctx context.Context, key, path string) ([]string, error) {
	return c.strs(ctx, pathReq(cJSON_OBJKEYS, key, path))
}

// JSONNumIncrBy adds delta to the number at path and returns the new value as JSON text
// (JSON.NUMINCRBY). The path must hold a number. The key's TTL is kept.
func (c *cmds) JSONNumIncrBy(ctx context.Context, key, path string, delta float64) (string, error) {
	v, err := c.value(ctx, newCmd(4, cJSON_NUMINCRBY).s(key).s(path).f(delta))
	return first(v).Str(), err
}

// JSONNumMultBy multiplies the number at path by factor and returns the new value as
// JSON text (JSON.NUMMULTBY).
func (c *cmds) JSONNumMultBy(ctx context.Context, key, path string, factor float64) (string, error) {
	v, err := c.value(ctx, newCmd(4, cJSON_NUMMULTBY).s(key).s(path).f(factor))
	return first(v).Str(), err
}

// JSONToggle flips the boolean at path and returns the new value (JSON.TOGGLE). A
// non-boolean returns WRONGTYPE.
func (c *cmds) JSONToggle(ctx context.Context, key, path string) (bool, error) {
	n, _, err := c.jsonInt(ctx, newCmd(3, cJSON_TOGGLE).s(key).s(path))
	return n == 1, err
}

// JSONArrAppend appends JSON values (each a JSON text such as `1` or `"x"`) to the array
// at path and returns the new length (JSON.ARRAPPEND). The path must hold an array. The
// cost grows with the array length; for append-heavy accumulation a list (RPush) is
// cheaper.
func (c *cmds) JSONArrAppend(ctx context.Context, key, path string, values ...string) (int64, error) {
	n, _, err := c.jsonInt(ctx, newCmd(3+len(values), cJSON_ARRAPPEND).s(key).s(path).ss(values))
	return n, err
}

// JSONArrInsert inserts JSON values before index in the array at path and returns the
// new length (JSON.ARRINSERT). A negative index counts from the end.
func (c *cmds) JSONArrInsert(ctx context.Context, key, path string, index int64, values ...string) (int64, error) {
	n, _, err := c.jsonInt(ctx, newCmd(4+len(values), cJSON_ARRINSERT).s(key).s(path).i(index).ss(values))
	return n, err
}

// JSONArrPop removes the element at index (-1 is the last) from the array at path and
// returns it as JSON text (JSON.ARRPOP). ok is false for a missing key, path or an empty
// array.
func (c *cmds) JSONArrPop(ctx context.Context, key, path string, index int64) (string, bool, error) {
	v, err := c.value(ctx, newCmd(4, cJSON_ARRPOP).s(key).s(path).i(index))
	if err != nil {
		return "", false, err
	}
	v = first(v)
	return v.Str(), v.Kind != KindNil, nil
}

// JSONArrTrim keeps only the elements from start to stop, both inclusive, and returns
// the new length (JSON.ARRTRIM).
func (c *cmds) JSONArrTrim(ctx context.Context, key, path string, start, stop int64) (int64, error) {
	n, _, err := c.jsonInt(ctx, newCmd(5, cJSON_ARRTRIM).s(key).s(path).i(start).i(stop))
	return n, err
}

// JSONArrIndex returns the index of the first element equal to the JSON scalar
// (JSON.ARRINDEX key path scalar [start [stop]]), or -1 when absent. scalar is JSON
// text, so a string needs quotes: `"x"`. startStop gives an optional start and end index
// (one or two values).
func (c *cmds) JSONArrIndex(ctx context.Context, key, path, scalar string, startStop ...int64) (int64, error) {
	r := newCmd(4+len(startStop), cJSON_ARRINDEX).s(key).s(path).s(scalar)
	for _, x := range startStop {
		r.i(x)
	}
	n, _, err := c.jsonInt(ctx, r)
	return n, err
}

// JSONClear empties arrays and objects and sets numbers to 0 at path (JSON.CLEAR) and
// returns how many values it cleared.
func (c *cmds) JSONClear(ctx context.Context, key, path string) (int64, error) {
	n, _, err := c.jsonInt(ctx, pathReq(cJSON_CLEAR, key, path))
	return n, err
}

// JSONStrAppend appends a JSON string literal (with quotes, such as `"abc"`) to the
// string at path and returns the new length (JSON.STRAPPEND).
func (c *cmds) JSONStrAppend(ctx context.Context, key, path, jsonString string) (int64, error) {
	n, _, err := c.jsonInt(ctx, newCmd(4, cJSON_STRAPPEND).s(key).s(path).s(jsonString))
	return n, err
}

// JSONIndexCreate creates an exact-match index on one scalar field of the JSON documents
// whose key starts with prefix (Ruvio JSON.INDEX CREATE name PREFIX prefix PATH path
// [TYPE t]). path is a field path such as "$.status". typ is "string" (the default when
// empty), "number" or "bool". The index is built in the background (JSONFind answers
// "index is building" until it is ready), follows every later write, delete and expiry,
// lives in memory (rebuilt at startup) and works for database 0 only. name is at most 64
// bytes. The definition occupies a reserved key that shows up in Keys and DBSize.
func (c *cmds) JSONIndexCreate(ctx context.Context, name, prefix, path, typ string) error {
	if typ == "" {
		return c.ok(ctx, newCmd(7, cJSON_INDEX).t(tCREATE).s(name).t(tPREFIX).s(prefix).t(tPATH).s(path))
	}
	return c.ok(ctx, newCmd(9, cJSON_INDEX).t(tCREATE).s(name).t(tPREFIX).s(prefix).t(tPATH).s(path).t(tTYPE).s(typ))
}

// JSONIndexDrop deletes an index definition (JSON.INDEX DROP name). The documents are
// not touched.
func (c *cmds) JSONIndexDrop(ctx context.Context, name string) error {
	return c.ok(ctx, newCmd(3, cJSON_INDEX).t(tDROP).s(name))
}

// JSONIndexList returns the index definitions as a raw Value (JSON.INDEX LIST), one line
// per shard.
func (c *cmds) JSONIndexList(ctx context.Context) (Value, error) {
	return c.value(ctx, newCmd(2, cJSON_INDEX).t(tLIST))
}

// JSONFind returns the keys of documents whose indexed field equals value (Ruvio
// JSON.FIND index VALUE value [COUNT n] [AFTER key]), in ascending key order. count
// limits the page (0 uses the server default of 100; the server maximum is 1000). To
// page, pass the last key of the previous page as after ("" starts from the beginning).
func (c *cmds) JSONFind(ctx context.Context, index, value string, count int64, after string) ([]string, error) {
	n := 4
	if count > 0 {
		n += 2
	}
	if after != "" {
		n += 2
	}
	r := newCmd(n, cJSON_FIND).s(index).t(tVALUE).s(value)
	if count > 0 {
		r.t(tCOUNT).i(count)
	}
	if after != "" {
		r.t(tAFTER).s(after)
	}
	return c.strs(ctx, r)
}

// ---- Bloom filters ---------------------------------------------------------------

// BFReserve creates a Bloom filter (BF.RESERVE key error capacity [EXPANSION n |
// NONSCALING]). errorRate is the target false-positive probability, strictly between 0
// and 1 (for example 0.01); capacity is the number of items the first layer is sized for
// (> 0). By default the filter scales: when a layer is full a larger one is added
// (expansion times bigger, default 2, 0 omits the option) with half the error rate. With
// nonScaling the filter keeps one layer and adds fail with "ERR non scaling filter is
// full" once it is full; nonScaling and expansion are mutually exclusive (nonScaling
// wins). Reserving an existing key returns "ERR item exists"; invalid numbers return
// "ERR invalid Bloom configuration". A layer larger than 64 MiB is refused.
func (c *cmds) BFReserve(ctx context.Context, key string, errorRate float64, capacity, expansion int64, nonScaling bool) error {
	switch {
	case nonScaling:
		return c.ok(ctx, newCmd(5, cBF_RESERVE).s(key).f(errorRate).i(capacity).t(tNONSCALING))
	case expansion > 0:
		return c.ok(ctx, newCmd(6, cBF_RESERVE).s(key).f(errorRate).i(capacity).t(tEXPANSION).i(expansion))
	}
	return c.ok(ctx, newCmd(4, cBF_RESERVE).s(key).f(errorRate).i(capacity))
}

// BFAdd adds an item and returns true when it was new, false when it was probably
// present (BF.ADD). A missing key is created with error rate 0.01 and capacity 100.
func (c *cmds) BFAdd(ctx context.Context, key, item string) (bool, error) {
	return c.boolR(ctx, newCmd(3, cBF_ADD).s(key).s(item))
}

// BFMAdd adds items in one round trip and returns, per item, whether it was new
// (BF.MADD). A missing key is created like BFAdd does.
func (c *cmds) BFMAdd(ctx context.Context, key string, items ...string) ([]bool, error) {
	return c.bools(ctx, newCmd(2+len(items), cBF_MADD).s(key).ss(items))
}

// BFExists reports whether the item is possibly in the filter (BF.EXISTS). A false
// answer is certain; a true answer can be a false positive at the configured rate. A
// missing key gives false.
func (c *cmds) BFExists(ctx context.Context, key, item string) (bool, error) {
	return c.boolR(ctx, newCmd(3, cBF_EXISTS).s(key).s(item))
}

// BFMExists tests several items in one round trip (BF.MEXISTS); all false for a missing
// key.
func (c *cmds) BFMExists(ctx context.Context, key string, items ...string) ([]bool, error) {
	return c.bools(ctx, newCmd(2+len(items), cBF_MEXISTS).s(key).ss(items))
}

// BFInsertOptions are the creation options of BFInsert; they apply only when the filter
// does not exist yet. Zero values are omitted. Capacity and Error size the new filter
// (server defaults 100 and 0.01), Expansion sets the growth factor, NonScaling keeps a
// single layer, NoCreate makes a missing filter an error ("ERR not found") instead of
// creating it. TTL, when positive, sets a deadline on the filter when this call creates
// it (EX for whole seconds, otherwise PX); an existing filter keeps its deadline, and a
// negative TTL returns ErrInvalidTTL.
type BFInsertOptions struct {
	TTL        time.Duration // deadline to set when the call creates the filter
	Capacity   int64         // items the new filter is sized for
	Error      float64       // target false-positive probability, 0 < Error < 1
	Expansion  int64         // growth factor of added layers
	NoCreate   bool          // fail instead of creating a missing filter
	NonScaling bool          // keep a single layer
}

// BFInsert adds items (BF.INSERT key [options] ITEMS item ...), creating the filter from
// o when it is missing, and returns per item whether it was new. At least one item is
// required.
func (c *cmds) BFInsert(ctx context.Context, key string, o BFInsertOptions, items ...string) ([]bool, error) {
	ttok, tv, err := optTTL(o.TTL)
	if err != nil {
		return nil, err
	}
	n := 3 + len(items)
	if ttok != nil {
		n += 2
	}
	if o.Capacity > 0 {
		n += 2
	}
	if o.Error > 0 {
		n += 2
	}
	if o.Expansion > 0 {
		n += 2
	}
	if o.NoCreate {
		n++
	}
	if o.NonScaling {
		n++
	}
	r := newCmd(n, cBF_INSERT).s(key)
	if ttok != nil {
		r.t(ttok).i(tv)
	}
	if o.Capacity > 0 {
		r.t(tCAPACITY).i(o.Capacity)
	}
	if o.Error > 0 {
		r.t(tERROR).f(o.Error)
	}
	if o.Expansion > 0 {
		r.t(tEXPANSION).i(o.Expansion)
	}
	if o.NoCreate {
		r.t(tNOCREATE)
	}
	if o.NonScaling {
		r.t(tNONSCALING)
	}
	r.t(tITEMS).ss(items)
	return c.bools(ctx, r)
}

// BFInfo returns the filter's properties as a map of name to Value: "Capacity", "Size"
// (bytes), "Number of filters" (layers), "Number of items inserted" and "Expansion
// rate". A missing key returns "ERR not found".
func (c *cmds) BFInfo(ctx context.Context, key string) (map[string]Value, error) {
	v, err := c.value(ctx, newCmd(2, cBF_INFO).s(key))
	if err != nil {
		return nil, err
	}
	return toValueMap(v)
}

// BFCard returns how many items were added (BF.CARD), 0 for a missing key.
func (c *cmds) BFCard(ctx context.Context, key string) (int64, error) {
	return c.intR(ctx, newCmd(2, cBF_CARD).s(key))
}
