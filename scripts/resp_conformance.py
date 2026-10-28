#!/usr/bin/env python3
"""Small RESP2 conformance smoke for the evented server."""

import argparse
import socket

# Re-exported: several scripts import these names from this module.
from _ruvio import encode, read_response, request, wait_ready  # noqa: F401


def run(host: str, port: int):
    wait_ready(port, host=host)
    # The timeout only stops a lost reply from hanging the check.
    connection = socket.create_connection((host, port), timeout=10)
    with connection:
        stream = connection.makefile("rwb", buffering=0)
        assert request(stream, b"PING") == (b"+", b"PONG")
        assert request(stream, b"SET", b"\x00key", b"\xffvalue") == (b"+", b"OK")
        assert request(stream, b"GET", b"\x00key") == (b"$", b"\xffvalue")
        assert request(stream, b"SET", b"counter", b"41") == (b"+", b"OK")
        assert request(stream, b"INCR", b"counter") == (b":", 42)
        assert request(stream, b"DECR", b"counter") == (b":", 41)
        assert request(stream, b"DEL", b"\x00key", b"counter") == (b":", 2)
        assert request(stream, b"LPUSH", b"list", b"a", b"b") == (b":", 2)
        assert request(stream, b"RPUSH", b"list", b"c") == (b":", 3)
        assert request(stream, b"LLEN", b"list") == (b":", 3)
        assert request(stream, b"LINDEX", b"list", b"-1") == (b"$", b"c")
        assert request(stream, b"LRANGE", b"list", b"0", b"-1") == (
            b"*",
            [(b"$", b"b"), (b"$", b"a"), (b"$", b"c")],
        )
        assert request(stream, b"RPOP", b"list") == (b"$", b"c")
        assert request(stream, b"LTRIM", b"list", b"0", b"0") == (b"+", b"OK")
        assert request(stream, b"LPOP", b"list") == (b"$", b"b")
        assert request(stream, b"BLPOP", b"missing-blpop", b"0.01") == (b"*", None)
        assert request(stream, b"BRPOP", b"missing-brpop", b"0.01") == (b"*", None)
        assert request(stream, b"SADD", b"set", b"a", b"b") == (b":", 2)
        assert request(stream, b"SISMEMBER", b"set", b"a") == (b":", 1)
        assert request(stream, b"SCARD", b"set") == (b":", 2)
        assert request(stream, b"SMEMBERS", b"set") == (b"*", [(b"$", b"a"), (b"$", b"b")])
        assert request(stream, b"SADD", b"other", b"b", b"c") == (b":", 2)
        assert request(stream, b"SINTER", b"set", b"other") == (b"*", [(b"$", b"b")])
        assert request(stream, b"SUNION", b"set", b"other") == (
            b"*",
            [(b"$", b"a"), (b"$", b"b"), (b"$", b"c")],
        )
        assert request(stream, b"SDIFF", b"set", b"other") == (b"*", [(b"$", b"a")])
        assert request(stream, b"SINTERSTORE", b"both", b"set", b"other") == (b":", 1)
        assert request(stream, b"SUNIONSTORE", b"unioned", b"set", b"other") == (b":", 3)
        assert request(stream, b"SDIFFSTORE", b"diffed", b"set", b"other") == (b":", 1)
        assert request(stream, b"SMOVE", b"set", b"other", b"a") == (b":", 1)
        assert request(stream, b"SADD", b"pop", b"only") == (b":", 1)
        assert request(stream, b"SPOP", b"pop") == (b"$", b"only")
        assert request(stream, b"SPOP", b"pop", b"1") == (b"*", [])
        assert request(stream, b"HSET", b"user", b"name", b"ada", b"city", b"london") == (b":", 2)
        assert request(stream, b"HMSET", b"legacy", b"a", b"1", b"b", b"2") == (b"+", b"OK")
        assert request(stream, b"HMGET", b"legacy", b"a", b"b") == (b"*", [(b"$", b"1"), (b"$", b"2")])
        assert request(stream, b"HGET", b"user", b"name") == (b"$", b"ada")
        assert request(stream, b"HLEN", b"user") == (b":", 2)
        assert request(stream, b"HEXISTS", b"user", b"city") == (b":", 1)
        kind, items = request(stream, b"HGETALL", b"user")
        pairs = sorted(zip(items[0::2], items[1::2]))
        assert kind == b"*" and pairs == [((b"$", b"city"), (b"$", b"london")), ((b"$", b"name"), (b"$", b"ada"))]
        assert request(stream, b"HMGET", b"user", b"city", b"missing") == (
            b"*",
            [(b"$", b"london"), (b"$", None)],
        )
        kind, items = request(stream, b"HKEYS", b"user")
        assert kind == b"*" and sorted(items) == [(b"$", b"city"), (b"$", b"name")]
        kind, items = request(stream, b"HVALS", b"user")
        assert kind == b"*" and sorted(items) == [(b"$", b"ada"), (b"$", b"london")]
        assert request(stream, b"HDEL", b"user", b"city") == (b":", 1)
        assert request(stream, b"HINCRBY", b"user", b"visits", b"2") == (b":", 2)
        assert request(stream, b"HGET", b"user", b"visits") == (b"$", b"2")
        assert request(stream, b"ZADD", b"sorted", b"1", b"member", b"2", b"other") == (b":", 2)
        assert request(stream, b"ZADD", b"sorted", b"NX", b"9", b"member") == (b":", 0)
        assert request(stream, b"ZADD", b"sorted", b"XX", b"8", b"absent") == (b":", 0)
        assert request(stream, b"ZCARD", b"sorted") == (b":", 2)
        assert request(stream, b"ZSCORE", b"sorted", b"member") == (b"$", b"1")
        assert request(stream, b"ZRANK", b"sorted", b"other") == (b":", 1)
        assert request(stream, b"ZREVRANK", b"sorted", b"member") == (b":", 1)
        assert request(stream, b"ZREVRANGE", b"sorted", b"0", b"0") == (b"*", [(b"$", b"other")])
        assert request(stream, b"ZRANGE", b"sorted", b"0", b"-1", b"WITHSCORES") == (
            b"*",
            [(b"$", b"member"), (b"$", b"1"), (b"$", b"other"), (b"$", b"2")],
        )
        assert request(stream, b"ZRANGEBYSCORE", b"sorted", b"(1", b"+inf") == (
            b"*",
            [(b"$", b"other")],
        )
        assert request(stream, b"ZINCRBY", b"sorted", b"0.5", b"member") == (b"$", b"1.5")
        assert request(stream, b"ZREM", b"sorted", b"other") == (b":", 1)
        assert request(stream, b"ZADD", b"pop", b"1", b"a", b"2", b"b", b"3", b"c", b"4", b"d") == (b":", 4)
        assert request(stream, b"ZPOPMIN", b"pop") == (b"*", [(b"$", b"a"), (b"$", b"1")])
        assert request(stream, b"ZPOPMAX", b"pop", b"1") == (b"*", [(b"$", b"d"), (b"$", b"4")])
        assert request(stream, b"BZPOPMIN", b"pop", b"0.1") == (
            b"*",
            [(b"$", b"pop"), (b"$", b"b"), (b"$", b"2")],
        )
        assert request(stream, b"BZPOPMAX", b"pop", b"0.1") == (
            b"*",
            [(b"$", b"pop"), (b"$", b"c"), (b"$", b"3")],
        )
        assert request(stream, b"BZPOPMIN", b"pop", b"0.05") == (b"*", None)
        assert request(stream, b"ZADD", b"rm", b"1", b"a", b"2", b"b", b"3", b"c", b"0", b"x") == (b":", 4)
        assert request(stream, b"ZREMRANGEBYRANK", b"rm", b"0", b"0") == (b":", 1)
        assert request(stream, b"ZREMRANGEBYSCORE", b"rm", b"(1", b"2") == (b":", 1)
        assert request(stream, b"ZREMRANGEBYLEX", b"rm", b"-", b"+") == (b":", 2)
        assert request(stream, b"HSET", b"ttl-hash", b"a", b"1", b"b", b"2") == (b":", 2)
        assert request(stream, b"HEXPIRE", b"ttl-hash", b"100", b"FIELDS", b"2", b"a", b"x") == (
            b"*",
            [(b":", 1), (b":", -2)],
        )
        assert request(stream, b"HPEXPIRE", b"ttl-hash", b"200000", b"GT", b"FIELDS", b"1", b"a") == (
            b"*",
            [(b":", 1)],
        )
        prefix, ttls = request(stream, b"HTTL", b"ttl-hash", b"FIELDS", b"2", b"a", b"b")
        assert prefix == b"*" and 0 < ttls[0][1] <= 200 and ttls[1] == (b":", -1)
        prefix, pttls = request(stream, b"HPTTL", b"ttl-hash", b"FIELDS", b"1", b"a")
        assert prefix == b"*" and 0 < pttls[0][1] <= 200_000
        assert request(stream, b"HEXPIREAT", b"ttl-hash", b"4102444800", b"FIELDS", b"1", b"b") == (
            b"*",
            [(b":", 1)],
        )
        assert request(stream, b"HEXPIRETIME", b"ttl-hash", b"FIELDS", b"1", b"b") == (
            b"*",
            [(b":", 4102444800)],
        )
        assert request(stream, b"HPEXPIRETIME", b"ttl-hash", b"FIELDS", b"1", b"b") == (
            b"*",
            [(b":", 4102444800000)],
        )
        assert request(stream, b"HPERSIST", b"ttl-hash", b"FIELDS", b"2", b"a", b"b") == (
            b"*",
            [(b":", 1), (b":", 1)],
        )
        assert request(stream, b"HPEXPIREAT", b"ttl-hash", b"1", b"FIELDS", b"1", b"a") == (
            b"*",
            [(b":", 2)],
        )
        assert request(stream, b"HGETALL", b"ttl-hash") == (b"*", [(b"$", b"b"), (b"$", b"2")])
        assert request(stream, b"BF.RESERVE", b"reserved-bloom", b"0.01", b"100") == (b"+", b"OK")
        assert request(stream, b"BF.ADD", b"bloom", b"item") == (b":", 1)
        assert request(stream, b"BF.EXISTS", b"bloom", b"item") == (b":", 1)
        assert request(stream, b"SET", b"ttl-key", b"value", b"EX", b"60") == (b"+", b"OK")
        prefix, ttl = request(stream, b"TTL", b"ttl-key")
        assert prefix == b":" and 0 < ttl <= 60
        assert request(stream, b"SET", b"ttl-key", b"kept", b"KEEPTTL") == (b"+", b"OK")
        assert request(stream, b"SET", b"nx-key", b"a", b"NX") == (b"+", b"OK")
        assert request(stream, b"SET", b"nx-key", b"b", b"NX") == (b"$", None)
        assert request(stream, b"SET", b"nx-key", b"c", b"GET") == (b"$", b"a")
        assert request(stream, b"GET", b"nx-key") == (b"$", b"c")
        assert request(stream, b"MSET", b"ma", b"1", b"mb", b"2") == (b"+", b"OK")
        assert request(stream, b"MGET", b"ma", b"missing-mget", b"mb") == (
            b"*",
            [(b"$", b"1"), (b"$", None), (b"$", b"2")],
        )
        assert request(stream, b"GETSET", b"ma", b"3") == (b"$", b"1")
        assert request(stream, b"APPEND", b"ma", b"0") == (b":", 2)
        assert request(stream, b"STRLEN", b"ma") == (b":", 2)
        assert request(stream, b"STRLEN", b"missing-strlen") == (b":", 0)
        assert request(stream, b"INCRBY", b"by", b"5") == (b":", 5)
        assert request(stream, b"INCRBY", b"cap", b"3", b"MAX", b"4") == (b":", 3)
        assert request(stream, b"INCRBY", b"cap", b"2", b"MAX", b"4") == (b"$", None)
        prefix, permit = request(stream, b"SEMAPHORE", b"sem", b"1", b"60000")
        assert prefix == b":" and permit > 0
        assert request(stream, b"SEMAPHORE", b"sem", b"1", b"60000") == (b"$", None)
        assert request(stream, b"RELEASE", b"sem", str(permit).encode()) == (b":", 1)
        assert request(stream, b"EXISTS", b"sem") == (b":", 0)
        assert request(stream, b"DECRBY", b"by", b"2") == (b":", 3)
        assert request(stream, b"EXISTS", b"by", b"missing-exists") == (b":", 1)
        assert request(stream, b"TYPE", b"by") == (b"+", b"string")
        assert request(stream, b"TYPE", b"missing-type") == (b"+", b"none")
        assert request(stream, b"RENAME", b"by", b"renamed-by") == (b"+", b"OK")
        assert request(stream, b"UNLINK", b"renamed-by") == (b":", 1)
        prefix, dbsize = request(stream, b"DBSIZE")
        assert prefix == b":" and dbsize >= 1
        prefix, scan = request(stream, b"SCAN", b"0", b"COUNT", b"100")
        assert prefix == b"*" and scan[0] == (b"$", b"0") and scan[1][0] == b"*"
        assert request(stream, b"EXPIRE", b"ttl-key", b"60") == (b":", 1)
        prefix, ttl = request(stream, b"TTL", b"ttl-key")
        assert prefix == b":" and 0 < ttl <= 60
        assert request(stream, b"PERSIST", b"ttl-key") == (b":", 1)
        assert request(stream, b"TTL", b"ttl-key") == (b":", -1)
        assert request(stream, b"PEXPIRE", b"ttl-key", b"60000") == (b":", 1)
        prefix, pttl = request(stream, b"PTTL", b"ttl-key")
        assert prefix == b":" and 0 < pttl <= 60_000
        assert request(stream, b"PERSIST", b"ttl-key") == (b":", 1)
        assert request(stream, b"EXPIREAT", b"ttl-key", b"4102444800") == (b":", 1)
        assert request(stream, b"PERSIST", b"ttl-key") == (b":", 1)
        assert request(stream, b"PEXPIREAT", b"ttl-key", b"4102444800000") == (b":", 1)
        assert request(stream, b"PERSIST", b"ttl-key") == (b":", 1)
        assert request(stream, b"MULTI") == (b"+", b"OK")
        assert request(stream, b"SET", b"{t}x", b"1") == (b"+", b"QUEUED")
        assert request(stream, b"INCR", b"{t}i") == (b"+", b"QUEUED")
        assert request(stream, b"EXEC") == (b"*", [(b"+", b"OK"), (b":", 1)])
        assert request(stream, b"WATCH", b"{t}x") == (b"+", b"OK")
        assert request(stream, b"SET", b"{t}x", b"x") == (b"+", b"OK")
        assert request(stream, b"MULTI") == (b"+", b"OK")
        assert request(stream, b"GET", b"{t}x") == (b"+", b"QUEUED")
        assert request(stream, b"EXEC") == (b"*", None)
        assert request(stream, b"UNWATCH") == (b"+", b"OK")
        assert request(stream, b"MULTI") == (b"+", b"OK")
        assert request(stream, b"SET", b"{t}discard", b"1") == (b"+", b"QUEUED")
        assert request(stream, b"DISCARD") == (b"+", b"OK")
        assert request(stream, b"GET", b"{t}discard") == (b"$", None)
        assert request(
            stream,
            b"EVAL",
            b"return ruvio.call('SET', KEYS[1], ARGV[1])",
            b"1",
            b"{t}s",
            b"lua",
        ) == (b"+", b"OK")
        assert request(stream, b"GET", b"{t}s") == (b"$", b"lua")
        sha = request(stream, b"SCRIPT", b"LOAD", b"return ruvio.call('GET', KEYS[1])")  # SCRIPT LOAD
        assert sha[0] == b"$" and len(sha[1]) == 40
        assert request(stream, b"EVALSHA", sha[1], b"1", b"{t}s") == (b"$", b"lua")
        assert request(stream, b"SCRIPT", b"EXISTS", sha[1]) == (b"*", [(b":", 1)])  # SCRIPT EXISTS
        assert request(stream, b"SCRIPT", b"FLUSH") == (b"+", b"OK")  # SCRIPT FLUSH
        assert request(stream, b"EVALSHA", sha[1], b"1", b"{t}s")[0] == b"-"
        assert request(stream, b"SCRIPT", b"KILL")[0] == b"-"  # SCRIPT KILL
        prefix, info = request(stream, b"INFO")
        assert prefix == b"$" and b"ruvio_" in info
        config = request(stream, b"CONFIG", b"GET", b"durability")  # CONFIG GET
        assert config[0] == b"*" and (b"$", b"durability") in config[1]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    args = parser.parse_args()
    run(args.host, args.port)
