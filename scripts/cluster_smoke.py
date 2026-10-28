#!/usr/bin/env python3
import argparse
import binascii
import hashlib
import socket
import sys

from _ruvio import wait_ready


class RespError(Exception):
    pass


def encode(*arguments: str) -> bytes:
    frame = bytearray(f"*{len(arguments)}\r\n", "ascii")
    for argument in arguments:
        payload = argument.encode()
        frame.extend(f"${len(payload)}\r\n".encode())
        frame.extend(payload)
        frame.extend(b"\r\n")
    return bytes(frame)


def read_line(stream) -> bytes:
    line = stream.readline()
    if not line.endswith(b"\r\n"):
        raise RuntimeError("truncated RESP line")
    return line[:-2]


def decode(stream):
    prefix = stream.read(1)
    if prefix == b"+":
        return read_line(stream)
    if prefix == b"-":
        raise RespError(read_line(stream).decode())
    if prefix == b":":
        return int(read_line(stream))
    if prefix == b"$":
        length = int(read_line(stream))
        if length == -1:
            return None
        payload = stream.read(length)
        if stream.read(2) != b"\r\n":
            raise RuntimeError("truncated bulk string")
        return payload
    if prefix == b"*":
        return [decode(stream) for _ in range(int(read_line(stream)))]
    raise RuntimeError(f"unexpected RESP prefix: {prefix!r}")


def command(port: int, *arguments: str):
    # The timeout only stops a lost reply from hanging the check.
    with socket.create_connection(("127.0.0.1", port), timeout=10) as client:
        client.sendall(encode(*arguments))
        return decode(client.makefile("rb"))


def slot(key: str) -> int:
    encoded = key.encode()
    opening = encoded.find(b"{")
    if opening >= 0:
        closing = encoded.find(b"}", opening + 1)
        if closing > opening + 1:
            encoded = encoded[opening + 1 : closing]
    return binascii.crc_hqx(encoded, 0) % 16384


def owner_port(key: str, left: int, right: int) -> int:
    return left if slot(key) < 8192 else right


def main(left: int = 6379, right: int = 6380) -> None:
    wait_ready(left)
    wait_ready(right)

    slots = command(left, "CLUSTER", "SLOTS")  # CLUSTER SLOTS
    assert [(entry[0], entry[1]) for entry in slots] == [(0, 8191), (8192, 16383)]
    nodes = command(left, "CLUSTER", "NODES")  # CLUSTER NODES
    assert f"127.0.0.1:{left}".encode() in nodes
    assert f"127.0.0.1:{right}".encode() in nodes
    assert b"myself,master" in nodes
    shards = command(left, "CLUSTER", "SHARDS")  # CLUSTER SHARDS
    assert [shard[1] for shard in shards] == [[0, 8191], [8192, 16383]]
    assert dict(zip(shards[1][3][0][::2], shards[1][3][0][1::2]))[b"port"] == right
    cluster_info = command(right, "CLUSTER", "INFO")  # CLUSTER INFO
    assert b"cluster_state:ok" in cluster_info
    assert b"cluster_known_nodes:2" in cluster_info
    # A node id is the SHA-1 of the address the shard is announced on.
    assert command(right, "CLUSTER", "MYID") == hashlib.sha1(f"127.0.0.1:{right}".encode()).hexdigest().encode()  # CLUSTER MYID
    assert command(left, "CLUSTER", "KEYSLOT", "{left}:value") == slot("{left}:value")  # CLUSTER KEYSLOT
    assert command(left, "READONLY") == b"OK"
    assert command(left, "READWRITE") == b"OK"

    keys = ["{left}:value", "{right}:value"]
    assert {owner_port(key, left, right) for key in keys} == {left, right}
    for key in keys:
        port = owner_port(key, left, right)
        assert command(port, "SET", key, "stored") == b"OK"
        assert command(port, "GET", key) == b"stored"
        # The other shard forwards a key it does not own to its owner instead of answering MOVED.
        wrong_port = right if port == left else left
        assert command(wrong_port, "GET", key) == b"stored", "wrong shard did not forward"

    try:
        command(left, "DEL", "{left}:a", "{right}:b")
    except RespError as error:
        assert error.args[0].startswith("CROSSSLOT")
    else:
        raise AssertionError("cross-slot DEL was accepted")

    info = command(left, "INFO")
    assert b"ruvio_shard_id:0" in info
    assert b"ruvio_shard_count:2" in info
    assert b"cluster_enabled:1" in info
    assert b"ruvio_backend:evented" in info

    print("cluster smoke test passed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-port", type=int, default=6379)
    args = parser.parse_args()
    try:
        main(args.base_port, args.base_port + 1)
    except Exception as error:
        print(f"cluster smoke test failed: {error}", file=sys.stderr)
        raise