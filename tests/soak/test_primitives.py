#!/usr/bin/env python3
"""Unit tests of the primitives check (`ruvio_soak/primitives_check.py` and the Go checker in
`tests/soak/porcupine`) on histories with known faults. No server; the Go part needs a Go
toolchain and the Porcupine module (the PR gate has both).

    python3 tests/soak/test_primitives.py"""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ruvio_soak import primitives_check as check  # noqa: E402

MS = 1_000_000


class Recorder:
    def __init__(self):
        self.entries, self.t, self.n = [], 0, 0

    def op(self, p, f, k, v, outcome="ok", result=None, profile="durable", gap=10 * MS):
        self.n += 1
        self.t += gap
        base = {"p": p, "i": self.n, "f": f, "k": k, "pr": profile}
        self.entries.append(dict(base, type="invoke", v=v, t=self.t))
        if outcome:
            self.t += gap
            self.entries.append(dict(base, type=outcome, v=result, t=self.t))

    def write(self) -> Path:
        handle = tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False)
        handle.write("\n".join(json.dumps(e) for e in self.entries) + "\n")
        handle.close()
        return Path(handle.name)


def run(recorder: Recorder) -> dict:
    return check.check_history(recorder.write())


class Locks(unittest.TestCase):
    def clean(self, r: Recorder, key="lock", ttl=30_000):
        r.op("a", "lease", key, [1, ttl], result=11)
        r.op("a", "release", key, 11, result=1)
        r.op("b", "lease", key, [1, ttl], result=12)
        r.op("b", "release", key, 12, result=1)

    def test_a_clean_history_passes(self):
        r = Recorder()
        self.clean(r)
        result = run(r)
        self.assertEqual(result["verdict"], "ok", result["problems"])
        self.assertTrue(result["linearizability"]["ran"], result["linearizability"])
        self.assertEqual(result["linearizability"]["keys"], 1)

    def test_a_refused_grant_while_held_and_an_unknown_release_pass(self):
        r = Recorder()
        r.op("a", "lease", "lock", [1, 30_000], result=11)
        r.op("b", "lease", "lock", [1, 30_000], result=None)
        r.op("a", "release", "lock", 11, outcome="info")
        r.op("b", "lease", "lock", [1, 30_000], result=12)
        result = run(r)
        self.assertEqual(result["verdict"], "ok", result["problems"])

    def test_two_holders_of_a_lease_are_found(self):
        r = Recorder()
        r.op("a", "lease", "lock", [1, 30_000], result=11)
        r.op("b", "lease", "lock", [1, 30_000], result=12)
        r.op("a", "release", "lock", 11, result=1)
        r.op("b", "release", "lock", 12, result=1)
        result = run(r)
        self.assertEqual(result["verdict"], "fail")
        self.assertEqual(result["counts"]["durable"]["double_holder"], 1)
        self.assertEqual(result["linearizability"]["illegal"], ["lock"])

    def test_a_smaller_token_is_found(self):
        r = Recorder()
        r.op("a", "lease", "lock", [1, 30_000], result=20)
        r.op("a", "release", "lock", 20, result=1)
        r.op("b", "lease", "lock", [1, 30_000], result=19)
        r.op("b", "release", "lock", 19, result=1)
        result = run(r)
        self.assertEqual(result["verdict"], "fail")
        self.assertEqual(result["counts"]["durable"]["token_regression"], 1)
        self.assertEqual(result["linearizability"]["illegal"], ["lock"])

    def test_the_same_token_twice_is_found(self):
        r = Recorder()
        self.clean(r)
        r.op("c", "lease", "lock", [1, 30_000], result=12)
        result = run(r)
        self.assertEqual(result["counts"]["durable"]["duplicate_token"], 1)

    def test_a_semaphore_over_its_maximum_is_found(self):
        r = Recorder()
        for worker, token in (("a", 1), ("b", 2), ("c", 3)):
            r.op(worker, "semaphore", "sem", [2, 30_000], result=token)
        result = run(r)
        self.assertEqual(result["counts"]["durable"]["double_holder"], 1)
        self.assertEqual(result["linearizability"]["illegal"], ["sem"])

    def test_a_semaphore_within_its_maximum_passes(self):
        r = Recorder()
        r.op("a", "semaphore", "sem", [2, 30_000], result=1)
        r.op("b", "semaphore", "sem", [2, 30_000], result=2)
        r.op("c", "semaphore", "sem", [2, 30_000], result=None)
        r.op("a", "release", "sem", 1, result=1)
        r.op("c", "semaphore", "sem", [2, 30_000], result=3)
        result = run(r)
        self.assertEqual(result["verdict"], "ok", result["problems"])

    def test_a_double_holder_in_default_is_only_a_warning(self):
        r = Recorder()
        r.op("a", "lease", "lock", [1, 30_000], result=11, profile="default")
        r.op("b", "lease", "lock", [1, 30_000], result=12, profile="default")
        result = run(r)
        self.assertEqual(result["counts"]["default"]["double_holder"], 1)
        self.assertEqual(result["verdict"], "fail")  # linearizability has no profile and fails


class Transfers(unittest.TestCase):
    def setup(self, profile="durable"):
        r = Recorder()
        r.op("init", "init", "{g}", {"{g}:a": 100, "{g}:b": 100}, result={"{g}:a": 100, "{g}:b": 100}, profile=profile)
        r.op("w", "transfer", "{g}", ["{g}:a", "{g}:b", 30], result=[70, 130], profile=profile)
        return r

    def test_a_conserved_sum_passes(self):
        r = self.setup()
        r.op("f", "final", "{g}:a", None, result=70)
        r.op("f", "final", "{g}:b", None, result=130)
        result = check.check_file(r.write())
        self.assertEqual(result["verdict"], "ok", result["problems"])

    def test_a_changed_sum_is_found(self):
        r = self.setup()
        r.op("f", "final", "{g}:a", None, result=70)
        r.op("f", "final", "{g}:b", None, result=131)
        result = check.check_file(r.write())
        self.assertEqual(result["counts"]["durable"]["sum_mismatch"], 1)
        self.assertEqual(result["verdict"], "fail")

    def test_an_unknown_transfer_may_or_may_not_have_applied(self):
        r = self.setup()
        r.op("w", "transfer", "{g}", ["{g}:a", "{g}:b", 10], outcome="info")
        r.op("f", "final", "{g}:a", None, result=60)
        r.op("f", "final", "{g}:b", None, result=140)
        self.assertEqual(check.check_file(r.write())["verdict"], "ok")

    def test_a_lost_acknowledged_transfer_with_the_sum_kept_is_found(self):
        r = self.setup(profile="default")
        r.op("f", "final", "{g}:a", None, result=100, profile="default")
        r.op("f", "final", "{g}:b", None, result=100, profile="default")
        result = check.check_file(r.write())
        self.assertEqual(result["counts"]["default"]["lost"], 1)
        self.assertEqual(result["counts"]["default"]["excess"], 1)
        self.assertEqual(result["verdict"], "warn")

    def test_a_negative_balance_is_found(self):
        r = Recorder()
        r.op("init", "init", "{g}", {"{g}:a": 10, "{g}:b": 10}, result={"{g}:a": 10, "{g}:b": 10})
        r.op("f", "final", "{g}:a", None, result=-5)
        r.op("f", "final", "{g}:b", None, result=25)
        self.assertEqual(check.check_file(r.write())["counts"]["durable"]["negative"], 1)


class Idem(unittest.TestCase):
    key = "soak:t:durable:idem:7"

    def test_one_effect_passes(self):
        r = Recorder()
        r.op("a", "begin", self.key, ["fp-7", "a-1"], result=["acquired"])
        r.op("b", "begin", self.key, ["fp-7", "b-1"], result=["pending"])
        r.op("a", "effect", self.key, None, result=1)
        r.op("a", "complete", self.key, ["fp-7", "a-1", "done-7"], result=1)
        r.op("b", "begin", self.key, ["fp-7", "b-2"], result=["completed", "done-7"])
        r.op("f", "final", self.key, None, result=1)
        result = check.check_file(r.write())
        self.assertEqual(result["verdict"], "ok", result["problems"])

    def test_two_acquires_and_two_effects_are_found(self):
        r = Recorder()
        r.op("a", "begin", self.key, ["fp-7", "a-1"], result=["acquired"])
        r.op("b", "begin", self.key, ["fp-7", "b-1"], result=["acquired"])
        r.op("a", "effect", self.key, None, result=1)
        r.op("b", "effect", self.key, None, result=2)
        r.op("f", "final", self.key, None, result=2)
        result = check.check_file(r.write())
        self.assertEqual(result["counts"]["durable"]["double_acquire"], 1)
        self.assertEqual(result["counts"]["durable"]["double_effect"], 1)

    def test_a_wrong_replay_is_found(self):
        r = Recorder()
        r.op("b", "begin", self.key, ["fp-7", "b-1"], result=["completed", "done-8"])
        r.op("f", "final", self.key, None, result=0)
        self.assertEqual(check.check_file(r.write())["counts"]["durable"]["bad_replay"], 1)

    def test_a_lost_effect_is_found(self):
        r = Recorder()
        r.op("a", "begin", self.key, ["fp-7", "a-1"], result=["acquired"])
        r.op("a", "effect", self.key, None, result=1)
        r.op("f", "final", self.key, None, result=0)
        self.assertEqual(check.check_file(r.write())["counts"]["durable"]["lost_effect"], 1)


if __name__ == "__main__":
    unittest.main()
