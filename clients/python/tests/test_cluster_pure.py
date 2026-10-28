# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Pure cluster-routing functions (no I/O): key extraction corner cases, plans, pipeline grouping,
split/merge of multi-key commands, SCAN cursor encoding and topology/redirect parsing."""

from __future__ import annotations

import pytest

import ruvio
from ruvio import _cluster as cl
from ruvio._resp import encode


def A(*parts):
    return [p if isinstance(p, bytes) else str(p).encode() for p in parts]


def keys_of(*parts):
    args = A(*parts)
    return cl.command_keys(args[0].upper(), args)


# --------------------------------------------------------------------------- key_slot


def test_key_slot_accepts_any_bytes_like_and_edge_tags():
    assert cl.key_slot(bytearray(b"foo")) == cl.key_slot("foo") == 12182
    assert cl.key_slot(memoryview(b"foo")) == 12182
    assert cl.key_slot("") == 0
    assert cl.key_slot("{}") == cl.key_slot("{}") and cl.key_slot("{}x") != cl.key_slot("x")  # empty tag: hash it all
    assert cl.key_slot("a{b") == cl.key_slot("a{b")  # unclosed tag: whole key
    assert cl.key_slot("{a}{b}") == cl.key_slot("a")
    assert cl.key_slot("x{ab}y") == cl.key_slot("ab")
    assert all(0 <= cl.key_slot(f"k{i}") < cl.SLOT_COUNT for i in range(200))


# --------------------------------------------------------------------------- command_keys


def test_counted_key_lists_tolerate_bad_counts():
    assert keys_of("EVAL", "script", 2, "a", "b", "arg") == [b"a", b"b"]
    assert keys_of("EVAL", "script", 0, "arg") == []
    assert keys_of("EVAL", "script", "notanumber", "a") == []  # the server will reject it; route anywhere
    assert keys_of("EVAL", "script") == []  # numkeys missing
    assert keys_of("EVALSHA", "sha", 5, "a", "b") == [b"a", b"b"]  # count larger than what was sent
    assert keys_of("LMPOP", 2, "a", "b", "LEFT") == [b"a", b"b"]
    assert keys_of("LMPOP", "x", "a") == []
    assert keys_of("BLMPOP", 1, 1, "a", "LEFT") == [b"a"]
    assert keys_of("ZUNIONSTORE", "dst", 2, "a", "b") == [b"dst", b"a", b"b"]
    assert keys_of("ZUNIONSTORE", "dst") == [b"dst"]
    assert keys_of("FCALL", "fn", 1, "k") == [b"k"]


def test_stream_bitop_geo_and_list_layouts():
    assert keys_of("XREAD", "COUNT", 1, "BLOCK", 5, "STREAMS", "a", "b", "0", "0") == [b"a", b"b"]
    assert keys_of("XREADGROUP", "GROUP", "g", "c", "streams", "a", "$") == [b"a"]
    assert keys_of("XREAD", "COUNT", 1) == []  # no STREAMS keyword
    assert keys_of("BITOP", "AND", "dst", "s1", "s2") == [b"dst", b"s1", b"s2"]
    assert keys_of("GEORADIUS", "k", 1, 2, 3, "m", "STORE", "d", "STOREDIST", "e") == [b"k", b"d", b"e"]
    assert keys_of("GEORADIUSBYMEMBER", "k", "m", 3, "km") == [b"k"]
    assert keys_of("GEORADIUS", "k", 1, 2, 3, "m", "STORE") == [b"k"]  # STORE without a value: ignored
    assert keys_of("BLPOP", "a", "b", 0) == [b"a", b"b"]
    assert keys_of("BLPOP", "a") == []  # only the timeout
    assert keys_of("JSON.MGET", "a", "b", "$.x") == [b"a", b"b"]
    assert keys_of("OBJECT", "ENCODING", "k") == [b"k"]
    assert keys_of("OBJECT") == []
    assert keys_of("MSET", "a", 1, "b", 2) == [b"a", b"b"]
    assert keys_of("MSET", "a", 1, "b") == [b"a", b"b"]  # odd tail: still a key
    assert keys_of("RENAME", "a", "b") == [b"a", b"b"]
    assert keys_of("RENAME", "a") == [b"a"]
    assert keys_of("DEL") == [] and keys_of("MGET") == []
    assert keys_of("FOO", "x", "y") == [b"x"]  # unknown commands: argument 1 is the key
    assert keys_of("FOO") == []
    for keyless in ("PING", "INFO", "PUBLISH", "SCAN", "DBSIZE", "KEYS"):
        assert keys_of(keyless, "x") == []


# --------------------------------------------------------------------------- plan


def test_plan_for_conditional_fan_out_and_special_commands():
    assert cl.plan(b"CLIENT", A("CLIENT", "SETNAME", "n")) == (cl.PLAN_FAN, cl.FAN_FIRST)
    assert cl.plan(b"CLIENT", A("CLIENT", "setname", "n")) == (cl.PLAN_FAN, cl.FAN_FIRST)
    assert cl.plan(b"CLIENT", A("CLIENT", "ID")) == (cl.PLAN_SLOT, None)
    assert cl.plan(b"CLIENT", A("CLIENT")) == (cl.PLAN_SLOT, None)
    assert cl.plan(b"SCRIPT", A("SCRIPT", "LOAD", "return 1")) == (cl.PLAN_FAN, cl.FAN_FIRST)
    assert cl.plan(b"SCRIPT", A("SCRIPT", "EXISTS", "x")) == (cl.PLAN_SLOT, None)
    assert cl.plan(b"FUNCTION", A("FUNCTION", "FLUSH")) == (cl.PLAN_FAN, cl.FAN_FIRST)
    assert cl.plan(b"FUNCTION", A("FUNCTION", "LIST")) == (cl.PLAN_SLOT, None)
    assert cl.plan(b"DBSIZE", A("DBSIZE")) == (cl.PLAN_FAN, cl.FAN_SUM)
    assert cl.plan(b"KEYS", A("KEYS", "*")) == (cl.PLAN_FAN, cl.FAN_CAT)
    assert cl.plan(b"SCAN", A("SCAN", 0)) == (cl.PLAN_SCAN,)
    assert cl.plan(b"PUBLISH", A("PUBLISH", "c", "m")) == (cl.PLAN_SLOT, 0)
    assert cl.plan(b"PING", A("PING")) == (cl.PLAN_SLOT, None)
    assert cl.plan(b"UNWATCH", A("UNWATCH")) == (cl.PLAN_SLOT, None)
    assert cl.plan(b"GET", A("GET", "k")) == (cl.PLAN_SLOT, cl.key_slot("k"))
    assert cl.plan(b"FOO", A("FOO")) == (cl.PLAN_SLOT, None)
    assert cl.plan(b"MGET", A("MGET")) == (cl.PLAN_SLOT, None)  # no keys: let the server complain
    for txn in (b"MULTI", b"EXEC", b"DISCARD"):
        with pytest.raises(ruvio.RuvioError, match="pipeline"):
            cl.plan(txn, A(txn))
    same = cl.plan(b"MGET", A("MGET", "{t}a", "{t}b"))
    assert same == (cl.PLAN_SLOT, cl.key_slot("t"))


def test_plan_splits_only_the_splittable_commands():
    a, b = "a", "b"
    assert cl.key_slot(a) != cl.key_slot(b)
    for name in (b"MGET", b"DEL", b"UNLINK", b"EXISTS", b"TOUCH"):
        kind, keys = cl.plan(name, A(name, a, b))
        assert kind == cl.PLAN_SPLIT and keys == [b"a", b"b"]
    assert cl.plan(b"MSET", A("MSET", a, 1, b, 2))[0] == cl.PLAN_SPLIT
    for name, args in ((b"MSETNX", A("MSETNX", a, 1, b, 2)), (b"SUNION", A("SUNION", a, b)), (b"RENAME", A("RENAME", a, b))):
        with pytest.raises(ruvio.CrossSlotError, match="nothing was sent") as ei:
            cl.plan(name, args)
        assert name.decode() in str(ei.value)


# --------------------------------------------------------------------------- pipelines


def test_pipeline_slot_rules():
    assert cl.pipeline_slot(encode("PING", ())) is None
    assert cl.pipeline_slot(encode("GET", ("k",))) == cl.key_slot("k")
    assert cl.pipeline_slot(encode("get", ("k",))) == cl.key_slot("k")
    assert cl.pipeline_slot(encode("FOO", ())) is None
    assert cl.pipeline_slot(encode("MGET", ("{t}a", "{t}b"))) == cl.key_slot("t")
    assert cl.pipeline_slot(encode("PUBLISH", ("c", "m"))) == 0
    with pytest.raises(ruvio.CrossSlotError, match="transaction must use keys in one slot"):
        cl.pipeline_slot(encode("MGET", ("a", "b")))
    for name in ("DBSIZE", "KEYS", "FLUSHDB"):
        with pytest.raises(ruvio.RuvioError, match="cannot be pipelined"):
            cl.pipeline_slot(encode(name, ()))
    with pytest.raises(ruvio.RuvioError):
        cl.pipeline_slot(encode("SCAN", (0,)))


def test_pipeline_groups_by_node_with_keyless_on_the_seed():
    table = [0] * 8192 + [1] * 8192
    lo = next(k for k in (f"k{i}" for i in range(99)) if cl.key_slot(k) < 8192)
    hi = next(k for k in (f"k{i}" for i in range(99)) if cl.key_slot(k) >= 8192)
    frames = [encode("SET", (lo, 1)), encode("PING", ()), encode("GET", (hi,)), encode("get", (lo,)), encode("MGET", ("{t}a", "{t}b"))]
    groups, subs = cl.pipeline_groups(frames, table)
    assert subs is None  # nothing was split
    assert groups[-1][0] == [1]  # keyless
    tag_node = table[cl.key_slot("t")]
    assert groups[0][0][:2] == [0, 3]
    assert groups[1][0] == ([2, 4] if tag_node == 1 else [2])
    assert tag_node in groups and 4 in groups[tag_node][0]
    assert [f for f in groups[0][1][:2]] == [frames[0], frames[3]]
    assert cl.pipeline_groups([], table) == ({}, None)
    for bad in (encode("SINTER", ("a", "b")), encode("RENAME", ("a", "b"))):
        with pytest.raises(ruvio.CrossSlotError):
            cl.pipeline_groups([encode("PING", ()), bad], table)
    for fan in (encode("DBSIZE", ()), encode("KEYS", ("*",)), encode("SCAN", (0,))):
        with pytest.raises(ruvio.RuvioError, match="cannot be pipelined"):
            cl.pipeline_groups([fan], table)


def test_pipeline_groups_split_cross_slot_commands_per_slot():
    table = [0] * 8192 + [1] * 8192
    a, b, c = "a", "b", "{a}c"  # a and {a}c share a slot, b is elsewhere
    assert cl.key_slot(a) != cl.key_slot(b) and cl.key_slot(a) == cl.key_slot(c)
    frames = [encode("GET", (a,)), encode("MGET", (b, a, c)), encode("PING", ()), encode("MSET", (a, 1, b, 2))]
    groups, subs = cl.pipeline_groups(frames, table)
    n = len(frames)
    assert [s[0] for s in subs] == [1, 1, 3, 3]  # pipeline index of each sub-command
    assert cl.split_frame(subs[0][4]) == [b"MGET", b"b"] and subs[0][3] == [0]
    assert cl.split_frame(subs[1][4]) == [b"MGET", b"a", b"{a}c"] and subs[1][3] == [1, 2]
    assert cl.split_frame(subs[2][4]) == [b"MSET", b"a", b"1"] and cl.split_frame(subs[3][4]) == [b"MSET", b"b", b"2"]
    # every sub-command sits in its slot's node group, at position n + its sub index
    for ni, (pos, fs) in groups.items():
        for p, f in zip(pos, fs):
            if p >= n:
                assert subs[p - n][4] == f and table[cl.key_slot(cl.split_frame(f)[1])] == ni
            else:
                assert frames[p] == f
    # the split commands themselves are not sent; their sub-commands are
    assert sorted(p for pos, _ in groups.values() for p in pos) == [0, 2] + list(range(n, n + len(subs)))
    # replies come back per position; merging puts one reply at each command's index
    replies = ["va", None, "PONG", None] + [["vb"], ["va", "vc"], "OK", "OK"]
    assert cl.merge_pipeline(replies, n, subs) == ["va", ["vb", "va", "vc"], "PONG", "OK"]  # MGET key order
    counts = cl.pipeline_groups([encode("DEL", (a, b, c)), encode("EXISTS", (b, a))], table)[1]
    assert cl.merge_pipeline([None, None, 1, 2, 1, 1], 2, counts) == [3, 2]
    err = ruvio.ResponseError("ERR nope")
    assert cl.merge_pipeline([None, None, 1, err, 1, 1], 2, counts) == [err, 2]  # the error stays at its index


# --------------------------------------------------------------------------- split / merge


def test_split_groups_pairs_for_mset_and_positions():
    a, b, c = "a", "b", "a2"
    sa, sb = cl.key_slot(a), cl.key_slot(b)
    assert sa != sb
    args = A("MSET", a, 1, b, 2, a, 3)
    out = cl.split_groups(b"MSET", args, cl.command_keys(b"MSET", args), lambda slot: slot == sa)
    by_node = {node: subs for node, subs in out.items()}
    assert set(by_node) == {True, False}
    (pos_a, frame_a), = by_node[True]
    assert pos_a == [0, 2]
    assert cl.split_frame(frame_a) == [b"MSET", b"a", b"1", b"a", b"3"]
    (pos_b, frame_b), = by_node[False]
    assert pos_b == [1] and cl.split_frame(frame_b) == [b"MSET", b"b", b"2"]
    args = A("DEL", a, b, a)
    out = cl.split_groups(b"DEL", args, args[1:], lambda slot: 0)
    assert [pos for pos, _ in out[0]] == [[0, 2], [1]] or [pos for pos, _ in out[0]] == [[1], [0, 2]]


def test_merge_split_by_command_and_first_error_wins():
    assert cl.merge_split(b"MGET", 4, [([0, 2], ["a", "c"]), ([1], ["b"])]) == ["a", "b", "c", None]
    assert cl.merge_split(b"MSET", 2, [([0], "OK"), ([1], "OK")]) == "OK"
    assert cl.merge_split(b"DEL", 3, [([0], 2), ([1], 1)]) == 3
    assert cl.merge_split(b"EXISTS", 3, []) == 0
    e1, e2 = ruvio.ResponseError("ERR one"), ruvio.ResponseError("ERR two")
    assert cl.merge_split(b"MGET", 2, [([0], "a"), ([1], e1)]) is e1
    assert cl.merge_split(b"DEL", 2, [([0], e1), ([1], e2)]) is e1


# --------------------------------------------------------------------------- SCAN cursor


def test_scan_cursor_encoding_edge_cases():
    assert cl.scan_prepare(A("SCAN", 0), 1) == (0, encode("SCAN", ("0",)))
    node, frame = cl.scan_prepare(A("SCAN", 11, "COUNT", 5), 4)
    assert node == 3 and cl.split_frame(frame) == [b"SCAN", b"2", b"COUNT", b"5"]
    for bad in (A("SCAN"), A("SCAN", "x"), A("SCAN", -1), A("SCAN", "1.5"), A("SCAN", "")):
        with pytest.raises(ruvio.ResponseError, match="invalid cursor"):
            cl.scan_prepare(bad, 3)
    # replies: bytes cursors stay bytes, str stay str, odd replies pass through
    assert cl.scan_finish([b"4", [b"k"]], 1, 3) == [b"13", [b"k"]]
    assert cl.scan_finish(["0", ["k"]], 2, 3) == ["0", ["k"]]
    assert cl.scan_finish(["0", []], 0, 1) == ["0", []]  # single node: done immediately
    assert cl.scan_finish("OK", 0, 3) == "OK"
    assert cl.scan_finish(["0"], 0, 3) == ["0"]
    assert cl.scan_finish(None, 0, 3) is None
    assert cl.scan_finish(("2", ["k"]), 0, 2) == ["4", ["k"]]
    # a full round trip through every node returns to 0 exactly once
    cursor, visited = 0, []
    pages = {(0, "0"): "7", (0, "7"): "0", (1, "0"): "0", (2, "0"): "3", (2, "3"): "0"}
    for _ in range(20):
        node, frame = cl.scan_prepare(A("SCAN", cursor), 3)
        sc = cl.split_frame(frame)[1].decode()
        visited.append(node)
        cursor = int(cl.scan_finish([pages[(node, sc)], []], node, 3)[0])
        if cursor == 0:
            break
    assert visited == [0, 0, 1, 2, 2] and cursor == 0


# --------------------------------------------------------------------------- topology


def test_parse_slots_clamping_hosts_and_malformed_rows():
    t = cl.parse_slots([[-5, 10, ["h1", 1]], [11, 99999, [b"h2", 2, "id", []]]], "seed")
    assert t.nodes == [("h1", 1), ("h2", 2)]
    assert t.slots[0] == 0 and t.slots[10] == 0 and t.slots[11] == 1 and t.slots[16383] == 1
    assert len(t.slots) == cl.SLOT_COUNT
    for host in ("", "0.0.0.0", "::", b"::"):
        assert cl.parse_slots([[0, 5, [host, 9]]], "seedhost").nodes == [("seedhost", 9)]
    # one node serving two ranges is one node
    t = cl.parse_slots([[0, 5, ["h", 1]], [6, 9, ["h", 1]], [10, 20, ["g", 2]]], "s")
    assert t.nodes == [("h", 1), ("g", 2)] and t.slots[7] == 0
    # holes stay -1
    t = cl.parse_slots([[0, 5, ["h", 1]], [100, 105, ["g", 2]]], "s")
    assert t.slots[50] == -1 and t.slots[101] == 1
    for bad in ("x", 5, {}, [[]], [[0, 5]], [[0, 5, []]], [["a", 5, ["h", 1]]], [[0, 5, ["h", "port"]]], [None]):
        assert cl.parse_slots(bad, "s") is None, bad
    assert cl.parse_slots([[10, 5, ["h", 1]]], "s").slots == [-1] * cl.SLOT_COUNT  # inverted range: no slots


def test_parse_moved_variants():
    assert cl.parse_moved(ruvio.MovedError("MOVED 12 h:1")) == (12, "h", 1)
    assert cl.parse_moved(ruvio.MovedError("MOVED 12 h:1 extra")) == (12, "h", 1)
    assert cl.parse_moved(ruvio.MovedError("MOVED 12 :7")) == (12, "", 7)
    assert cl.parse_moved(ruvio.MovedError("ASK 12 h:1")) is None
    assert cl.parse_ask(ruvio.MovedError("ASK 12 h:1")) == (12, "h", 1)
    assert cl.parse_ask(ruvio.MovedError("MOVED 12 h:1")) is None
    assert cl.is_tryagain(ruvio.ResponseError("TRYAGAIN try later")) and not cl.is_tryagain(ruvio.MovedError("ASK 1 h:1"))
    assert not cl.is_tryagain("TRYAGAIN") and not cl.is_tryagain(1)
    for bad in ("MOVED", "MOVED 12", "MOVED x h:1", "MOVED 12 h:y", "MOVED 12 hostonly", "MOVEDx 1 h:1"):
        assert cl.parse_moved(ruvio.MovedError(bad)) is None, bad
    assert cl.parse_moved(Exception()) is None  # no message at all
    assert cl.parse_moved(Exception(5)) is None  # non-string message
    assert cl.parse_moved(ValueError("MOVED 1 h:2")) == (1, "h", 2)  # any exception with a MOVED message
