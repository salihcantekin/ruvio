#!/usr/bin/env python3
"""Unit tests of the soak tool: the history checker on histories with known faults, config
rendering, the ssh backend's commands, and the blob upload request. No server, no network
beyond 127.0.0.1; about a second.

    python3 tests/soak/test_soak.py"""

from __future__ import annotations

import gzip
import http.server
import json
import signal
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from pathlib import Path

from ruvio_soak import history
from ruvio_soak.backends import SshBackend, parse_proc_status
from ruvio_soak.cluster import Cluster
from ruvio_soak.resp import key_slot
from ruvio_soak.runner import Runner
from ruvio_soak.status import BlobUploader, Status


class Recorder:
    """Builds a history with increasing timestamps."""

    def __init__(self):
        self.entries, self.t, self.op = [], 0, 0

    def op_(self, p, f, k, v, outcome, profile="durable", result=None):
        self.op += 1
        self.t += 10
        self.entries.append({"p": p, "i": self.op, "type": "invoke", "f": f, "k": k, "v": v, "pr": profile, "t": self.t})
        if outcome is not None:
            self.t += 10
            self.entries.append(
                {"p": p, "i": self.op, "type": outcome, "f": f, "k": k, "pr": profile, "t": self.t,
                 "v": v if result is None else result}
            )

    def final(self, k, value, profile="durable"):
        self.op_("final", "final", k, None, "ok", profile, result=value)


class HistoryChecks(unittest.TestCase):
    def test_a_clean_history_passes(self):
        r = Recorder()
        for v in (1, 2, 3):
            r.op_("w", "append", "l", v, "ok")
            r.op_("c", "incr", "c", 1, "ok")
            r.op_("rw", "write", "r", v, "ok")
            r.op_("rr", "read", "r", None, "ok", result=v)
        r.final("l", [1, 2, 3])
        r.final("c", 3)
        r.final("r", 3)
        result = history.check(r.entries)
        self.assertEqual(result["verdict"], "ok", result)

    def test_a_lost_acknowledged_append_fails_durable_and_warns_default(self):
        for profile, verdict in (("durable", "fail"), ("default", "warn")):
            r = Recorder()
            for v in (1, 2, 3):
                r.op_("w", "append", "l", v, "ok", profile)
            r.final("l", [1, 3], profile)
            result = history.check(r.entries)
            self.assertEqual(result["verdict"], verdict)
            self.assertEqual(result["profiles"][profile]["lost"], 1)

    def test_unknown_writes_may_be_present_or_missing(self):
        r = Recorder()
        r.op_("w", "append", "l", 1, "ok")
        r.op_("w", "append", "l", 2, "info")
        r.op_("w", "append", "l", 3, None)  # no completion: the controller stopped
        r.op_("w", "append", "l", 4, "ok")
        r.final("l", [1, 3, 4])
        self.assertEqual(history.check(r.entries)["verdict"], "ok")

    def test_a_value_reported_as_not_applied_is_a_phantom(self):
        r = Recorder()
        r.op_("w", "append", "l", 1, "ok")
        r.op_("w", "append", "l", 2, "fail")
        r.final("l", [1, 2])
        result = history.check(r.entries)
        self.assertEqual(result["verdict"], "fail")
        self.assertEqual(result["profiles"]["durable"]["phantom"], 1)

    def test_duplicates_and_order(self):
        r = Recorder()
        for v in (1, 2, 3):
            r.op_("w", "append", "l", v, "ok", "default")
        r.final("l", [1, 3, 2, 3], "default")
        stats = history.check(r.entries)["profiles"]["default"]
        self.assertEqual(stats["duplicate"], 1)
        self.assertEqual(stats["order"], 1)

    def test_counter_bounds(self):
        r = Recorder()
        r.op_("c", "incr", "c", 1, "ok")
        r.op_("c", "incr", "c", 1, "info")
        r.op_("c", "incr", "c", 1, "ok")
        r.final("c", 3)
        self.assertEqual(history.check(r.entries)["verdict"], "ok")
        r.entries.pop(); r.entries.pop()
        r.final("c", 4)
        self.assertEqual(history.check(r.entries)["profiles"]["durable"]["excess"], 1)
        r.entries.pop(); r.entries.pop()
        r.final("c", 1)
        self.assertEqual(history.check(r.entries)["profiles"]["durable"]["lost"], 1)

    def test_stale_and_backwards_reads(self):
        r = Recorder()
        r.op_("rw", "write", "r", 1, "ok", "default")
        r.op_("rw", "write", "r", 2, "ok", "default")
        r.op_("rr", "read", "r", None, "ok", "default", result=1)  # starts after 2 was acknowledged
        r.op_("rr2", "read", "r", None, "ok", "default", result=2)
        r.op_("rr2", "read", "r", None, "ok", "default", result=1)
        r.final("r", 2, "default")
        stats = history.check(r.entries)["profiles"]["default"]
        self.assertEqual(stats["stale_read"], 2)
        self.assertEqual(stats["backwards_read"], 1)

    def test_a_long_register_history_is_checked_in_seconds(self):
        # Every read used to scan all earlier writes of its key; a 3-hour run never finished.
        r = Recorder()
        for v in range(1, 30001):
            r.op_("rw", "write", "r", v, "ok", "default")
            r.op_("rr", "read", "r", None, "ok", "default", result=v)
        r.op_("rr2", "read", "r", None, "ok", "default", result=29000)
        r.final("r", 30000, "default")
        started = time.monotonic()
        stats = history.check(r.entries)["profiles"]["default"]
        self.assertLess(time.monotonic() - started, 10)
        self.assertEqual(stats["stale_read"], 1)
        self.assertEqual(stats["ok"], 60001)

    def test_a_read_concurrent_with_a_write_may_return_either(self):
        r = Recorder()
        r.op_("rw", "write", "r", 1, "ok")
        r.entries.append({"p": "rw", "i": 90, "type": "invoke", "f": "write", "k": "r", "v": 2, "pr": "durable", "t": 100})
        r.entries.append({"p": "rr", "i": 91, "type": "invoke", "f": "read", "k": "r", "v": None, "pr": "durable", "t": 101})
        r.entries.append({"p": "rr", "i": 91, "type": "ok", "f": "read", "k": "r", "v": 1, "pr": "durable", "t": 103})
        r.entries.append({"p": "rw", "i": 90, "type": "ok", "f": "write", "k": "r", "v": 2, "pr": "durable", "t": 102})
        r.t = 200
        r.final("r", 2)
        self.assertEqual(history.check(r.entries)["verdict"], "ok")

    def test_an_unreadable_key_fails(self):
        r = Recorder()
        r.op_("w", "append", "l", 1, "ok")
        r.op_("final", "final", "l", None, "fail")
        self.assertEqual(history.check(r.entries)["profiles"]["durable"]["unread"], 1)

    def test_a_cut_last_line_is_ignored(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "h.jsonl"
            path.write_text('{"p":"w","i":1,"type":"invoke","f":"incr","k":"c","v":1,"pr":"default","t":1}\n{"p":"w","i"')
            self.assertEqual(len(history.load(path)), 1)


class Rendering(unittest.TestCase):
    def test_azure_ports_and_roles(self):
        cluster = Cluster(hosts=["10.0.0.1", "10.0.0.2", "10.0.0.3"]).with_new_secrets()
        self.assertEqual(cluster.address(2, "g2"), "10.0.0.3:7100")
        env = cluster.node_env(1, "g1", "/d", "/t")
        self.assertEqual(env["RUVIO_REPLICATION_ROLE"], "replica")
        self.assertEqual(env["RUVIO_REPLICATION_PRIMARY"], "10.0.0.1:27000")
        self.assertEqual(cluster.node_env(1, "g2", "/d", "/t")["RUVIO_REPLICATION_ROLE"], "primary")
        config = cluster.coordinator_toml(0, "/c")
        self.assertIn('peers = ["1@10.0.0.1:26400", "2@10.0.0.2:26400", "3@10.0.0.3:26400"]', config)
        self.assertIn(f'auth = "{cluster.password}"', config)
        self.assertNotIn(cluster.password, json.dumps(cluster.public()))

    def test_local_hosts_do_not_share_ports(self):
        cluster = Cluster(hosts=["127.0.0.1"] * 3, local=True)
        addresses = [cluster.address(h, r) for h, r in cluster.processes()]
        addresses += [cluster.replication_address(h, g.name) for h in range(3) for g in cluster.groups]
        self.assertEqual(len(addresses), len(set(addresses)))

    def test_key_slot_matches_redis(self):
        self.assertEqual(key_slot("foo"), 12182)
        self.assertEqual(key_slot("{user1000}.following"), key_slot("user1000"))


class SshCommands(unittest.TestCase):
    def test_faults_and_watchdogs(self):
        calls = []
        cluster = Cluster(hosts=["10.0.0.1", "10.0.0.2", "10.0.0.3"]).with_new_secrets()
        backend = SshBackend(cluster, "/bin/true", "soak", run=lambda a, c, stdin=None: calls.append((a, c, stdin)) or "")
        backend.signal(1, "g1", signal.SIGKILL)
        backend.signal(0, "coordinator", signal.SIGSTOP)
        backend.isolate(2, 300)
        backend.heal(2)
        self.assertEqual(calls[0][:2], ("10.0.0.2", "sudo systemctl kill -s KILL ruvio@g1"))
        self.assertEqual(calls[1][1], "sudo systemctl kill -s STOP ruvio-coordinator")
        # The watchdog is armed before the rule goes in.
        self.assertIn("--on-active=300", calls[2][1])
        self.assertEqual(calls[3][1], "sudo nft -f /dev/stdin")
        self.assertIn(b"ip saddr { 10.0.0.1, 10.0.0.2 } drop", calls[3][2])
        self.assertIn("nft delete table inet ruvio_soak", calls[4][1])

    def test_deploy_never_puts_secrets_on_a_command_line(self):
        calls = []
        cluster = Cluster(hosts=["10.0.0.1", "10.0.0.2", "10.0.0.3"]).with_new_secrets()
        with tempfile.NamedTemporaryFile() as binary:
            backend = SshBackend(cluster, binary.name, "soak", run=lambda a, c, stdin=None: calls.append((a, c, stdin)) or "")
            backend.deploy()
        self.assertTrue(any(cluster.password.encode() in (stdin or b"") for _, _, stdin in calls))
        self.assertFalse(any(cluster.password in command or cluster.secret in command for _, command, _ in calls))


class ProcessMemory(unittest.TestCase):
    def test_proc_status_memory_lines(self):
        text = "Name:\truvio\nVmHWM:\t  2048 kB\nVmRSS:\t  1024 kB\nRssAnon:\t   900 kB\nRssFile:\t   124 kB\nThreads:\t9\nVmFlags: x\n"
        self.assertEqual(parse_proc_status(text),
                         {"rss_peak_kib": 2048, "rss_kib": 1024, "rss_anon_kib": 900, "rss_file_kib": 124, "threads": 9})
        self.assertEqual(parse_proc_status(""), {})


class BlobUpload(unittest.TestCase):
    def test_put_request_and_redaction(self):
        received = []

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_PUT(self):
                length = int(self.headers["Content-Length"])
                received.append((self.path, self.headers["x-ms-blob-type"], self.rfile.read(length)))
                self.send_response(201 if "fail" not in self.path else 403)
                self.end_headers()

            def log_message(self, *args):
                pass

        server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            base = f"http://127.0.0.1:{server.server_port}/soak?sv=2022&sig=SECRET"
            uploader = BlobUploader(base, "run-1")
            with tempfile.TemporaryDirectory() as directory:
                status = Status(Path(directory), "run-1", {"name": "p"}, "local", uploader)
                status.problem("something", severity="warn")
                status.save()
                saved = json.loads((Path(directory) / "status.json").read_text())
            self.assertEqual(saved["verdict"], "warn")
            self.assertEqual([path for path, _, _ in received], ["/soak/run-1/status.json?sv=2022&sig=SECRET", "/soak/current/status.json?sv=2022&sig=SECRET"])
            self.assertEqual(received[0][1], "BlockBlob")
            self.assertFalse(uploader.put("fail.json", b"{}"))
            self.assertNotIn("SECRET", uploader.last_error)
        finally:
            server.shutdown()
            server.server_close()

    def test_run_files_upload_gzipped(self):
        received = {}

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_PUT(self):
                received[self.path.split("?")[0]] = self.rfile.read(int(self.headers["Content-Length"]))
                self.send_response(201)
                self.end_headers()

            def log_message(self, *args):
                pass

        server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            uploader = BlobUploader(f"http://127.0.0.1:{server.server_port}/soak?sig=x", "run-1")
            with tempfile.TemporaryDirectory() as directory:
                lines = b"".join(b'{"n":%d}\n' % n for n in range(300000))
                (Path(directory) / "history.jsonl").write_bytes(lines)
                (Path(directory) / "check.json").write_bytes(b"{}")
                Runner.upload(SimpleNamespace(uploader=uploader, directory=Path(directory)),
                              "history.jsonl", "check.json", "missing.jsonl")
            self.assertEqual(sorted(received), ["/soak/run-1/check.json", "/soak/run-1/history.jsonl.gz"])
            self.assertEqual(gzip.decompress(received["/soak/run-1/history.jsonl.gz"]), lines)
        finally:
            server.shutdown()
            server.server_close()

    def test_a_url_without_sas_is_refused(self):
        with self.assertRaises(ValueError):
            BlobUploader("https://account.blob.core.windows.net/soak", "x")


if __name__ == "__main__":
    unittest.main()
