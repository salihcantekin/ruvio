# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Fails when a command in docs/compatibility-matrix.json has no typed method.

The "python" field of clients/testdata/command_methods.json maps each matrix command to the
method that sends it. Methods marked with ``PubSub.`` live on the Pub/Sub session classes. The test also checks that every mapped
method exists on both the sync and the asyncio client, and that the live tests call it.
"""

from __future__ import annotations

import json
import os
import re

import ruvio
import ruvio.asyncio

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
MATRIX = os.path.join(ROOT, "docs", "compatibility-matrix.json")
TESTS = os.path.dirname(__file__)

METHODS = os.path.join(os.path.dirname(__file__), "..", "..", "testdata", "command_methods.json")


def _command_methods():
    """Matrix command -> the "python" field of command_methods.json, as a list of method names."""
    with open(METHODS, encoding="utf-8") as f:
        commands = json.load(f)["commands"]
    out = {}
    for name, entry in commands.items():
        value = entry.get("python")
        out[name] = [value] if isinstance(value, str) else list(value or [])
    return out


COMMANDS = _command_methods()

# Sub-forms and options with their own methods (also checked for existence and live use).
EXTRA = [
    "command_count", "command_info", "command_docs", "slowlog_len", "slowlog_reset", "idem_complete", "idem_abort",
    "changes_start", "object_idletime", "object_freq", "json_index_drop", "json_index_list", "xpending_range",
    "rate_limit", "limit_sliding", "limit_tokens", "limit_sliding_with_retry", "limit_tokens_with_retry", "seen_before", "push_recent", "json_get_obj", "json_set_obj", "json_mget_obj",
    "execute_command", "lock", "lease_scope", "semaphore_scope", "get_or_create", "idempotent",
    "scan_iter", "hscan_iter", "sscan_iter", "zscan_iter", "pipeline", "transaction", "pubsub", "dedicated",
]


def _matrix():
    with open(MATRIX, encoding="utf-8") as f:
        return [c["name"] for c in json.load(f)["commands"]]


def _live_sources():
    text = ""
    for name in os.listdir(TESTS):
        if name.startswith("test_") and name.endswith(".py") and name not in ("test_coverage.py", "test_resp.py"):
            with open(os.path.join(TESTS, name), encoding="utf-8") as f:
                text += f.read()
    return text


def _resolve(cls, pubsub_cls, method):
    if method.startswith("PubSub."):
        return getattr(pubsub_cls, method.split(".", 1)[1], None)
    return getattr(cls, method, None)


def test_every_matrix_command_has_a_typed_method():
    names = _matrix()
    missing = [n for n in names if not COMMANDS.get(n)]
    assert not missing, f"no typed method mapped for: {missing}"
    for cls, ps in ((ruvio.Ruvio, ruvio.PubSub), (ruvio.asyncio.Ruvio, ruvio.asyncio.PubSub)):
        absent = [f"{n} -> {m}" for n, ms in COMMANDS.items() for m in ms if not callable(_resolve(cls, ps, m))]
        assert not absent, f"{cls.__module__}: methods missing: {absent}"
    stale = sorted(set(COMMANDS) - set(names))
    assert not stale, f"mapped commands not in the matrix: {stale}"
    print(f"coverage: {sum(1 for n in names if n in COMMANDS)}/{len(names)}")


def test_extra_methods_exist():
    for cls in (ruvio.Ruvio, ruvio.asyncio.Ruvio):
        absent = [m for m in EXTRA if not callable(getattr(cls, m, None))]
        assert not absent, f"{cls.__name__}: {absent}"


def test_every_typed_method_is_exercised_by_live_tests():
    src = _live_sources()
    unused = []
    for m in [m for ms in COMMANDS.values() for m in ms] + EXTRA:
        name = m.split(".", 1)[-1]
        if not re.search(r"\b" + re.escape(name) + r"\(", src):
            unused.append(m)
    assert not unused, f"typed methods never called by a live test: {unused}"
