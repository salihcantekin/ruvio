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

from ruvio_soak import history, latency, workload
from ruvio_soak.backends import SshBackend, parse_host_stats, parse_proc_status
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

    def test_a_counter_loss_is_timed_where_the_replies_went_back(self):
        r = Recorder()
        for reply in (1, 2, 3):
            r.op_("c", "incr", "c", 1, "ok", "default", result=1)
            r.entries[-1]["r"] = reply
        lost_at = r.t + 10  # the invoke time of the next increment
        for reply in (3, 4, 5):  # the increment that returned 3 was lost with a primary
            r.op_("c", "incr", "c", 1, "ok", "default", result=1)
            r.entries[-1]["r"] = reply
        r.final("c", 5, "default")
        result = history.check(r.entries)
        self.assertEqual(result["profiles"]["default"]["lost"], 1)
        (_, count, t, _), = result["findings"]
        self.assertEqual((count, t), ("lost", lost_at))

    def test_a_counter_loss_without_replies_falls_back_to_the_newest_increments(self):
        r = Recorder()
        for _ in range(3):
            r.op_("c", "incr", "c", 1, "ok", "default")
        last_end = r.t
        r.final("c", 2, "default")
        (_, _, t, _), = history.check(r.entries)["findings"]
        self.assertEqual(t, last_end)

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
                runner = SimpleNamespace(uploader=uploader, directory=Path(directory))
                runner.upload_file = lambda path, name: Runner.upload_file(runner, path, name)
                runner.note_upload = lambda name, ok: Runner.note_upload(runner, name, ok)
                Runner.upload(runner, "history.jsonl", "check.json", "missing.jsonl")
            self.assertEqual(sorted(received), ["/soak/run-1/check.json", "/soak/run-1/history.jsonl.gz"])
            self.assertEqual(gzip.decompress(received["/soak/run-1/history.jsonl.gz"]), lines)
        finally:
            server.shutdown()
            server.server_close()

    def test_a_url_without_sas_is_refused(self):
        with self.assertRaises(ValueError):
            BlobUploader("https://account.blob.core.windows.net/soak", "x")



class LongRunPieces(unittest.TestCase):
    """The parts a 24-hour run needs: bucketed checks, fault attribution, latency histograms,
    history segments, bounded lists, host stats and the lease settings."""

    def mixed_history(self):
        r = Recorder()
        for n in range(1, 40):
            r.op_("w", "append", f"list{n % 7}", n, "ok", profile="default")
            r.op_("c", "incr", f"counter{n % 5}", 1, "ok", profile="durable")
        for k in range(7):
            r.final(f"list{k}", [n for n in range(1, 40) if n % 7 == k and n != 8], profile="default")  # 8 lost
        for k in range(5):
            r.final(f"counter{k}", sum(1 for n in range(1, 40) if n % 5 == k), profile="durable")
        return r

    def test_bucketed_check_matches_a_single_pass(self):
        r = self.mixed_history()
        with tempfile.TemporaryDirectory() as directory:
            half = len(r.entries) // 2
            paths = [Path(directory) / "a.jsonl", Path(directory) / "b.jsonl"]
            for path, part in zip(paths, (r.entries[:half], r.entries[half:])):
                path.write_text("".join(json.dumps(e, separators=(",", ":")) + "\n" for e in part))
            single = history.check_files(paths, buckets=1)
            bucketed = history.check_files(paths, buckets=4, work=Path(directory))
        self.assertEqual(single["profiles"], bucketed["profiles"])
        self.assertEqual(bucketed["profiles"]["default"]["lost"], 1)
        self.assertEqual(single["verdict"], bucketed["verdict"])

    def test_findings_are_tied_to_fault_windows(self):
        r = self.mixed_history()
        result = history.check(r.entries)
        (_, _, t, _), = result["findings"]
        inside = [{"kind": "kill-primary", "start": t - 5, "end": t + 5}]
        outside = [{"kind": "kill-primary", "start": t + 100, "end": t + 200}]
        self.assertEqual(history.attribute(result, inside)["default"]["lost"]["by_fault"], {"kill-primary": 1})
        self.assertEqual(history.attribute(result, outside)["default"]["lost"]["unexplained"], 1)

    def test_fault_windows_from_events(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            path.write_text("".join(json.dumps(e) + "\n" for e in [
                {"event": "fault", "kind": "netem", "mono": 1000},
                {"event": "healed", "kind": "netem", "mono": 2000},
                {"event": "fault", "kind": "kill-host", "mono": 5000},
            ]))
            windows = history.fault_windows(path, grace_s=0.000001)
        self.assertEqual(windows[0], {"kind": "netem", "start": 1000, "end": 3000})
        self.assertIsNone(windows[1]["end"])

    def test_latency_percentiles_never_understate(self):
        for micros in (1, 3, 17, 999, 1000, 123456, 5_000_000):
            self.assertGreaterEqual(latency.bucket_upper(latency.bucket(micros)), micros)
            self.assertLess(latency.bucket_upper(latency.bucket(micros)), micros * 1.07 + 1)
        histogram = {}
        for micros in range(1, 1001):
            index = latency.bucket(micros)
            histogram[index] = histogram.get(index, 0) + 1
        self.assertAlmostEqual(latency.percentile(histogram, 0.5), 0.5, delta=0.03)
        self.assertAlmostEqual(latency.percentile(histogram, 0.99), 0.99, delta=0.05)

    def test_latency_minutes_are_written(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = latency.Recorder(Path(directory) / "latency.jsonl")
            recorder.add("default:incr", "ok", 0.002)
            recorder.add("default:incr", "info", 0.5)
            recorder.close()
            entry = json.loads((Path(directory) / "latency.jsonl").read_text())
        self.assertEqual(entry["counts"], {"ok": 1, "info": 1})
        self.assertAlmostEqual(entry["p99_ms"], 2.0, delta=0.15)

    def test_history_segments_and_written_keys(self):
        with tempfile.TemporaryDirectory() as directory:
            h = workload.History(Path(directory))
            h.add({"p": "w", "i": 1, "type": "invoke", "f": "incr", "k": "a", "pr": "default", "v": 1})
            closed = h.rotate()
            h.add({"p": "w", "i": 1, "type": "ok", "f": "incr", "k": "a", "pr": "default", "v": 1})
            h.close()
            self.assertEqual([p.name for p in h.paths()], ["history-0000.jsonl", "history-0001.jsonl"])
            self.assertEqual(closed.name, "history-0000.jsonl")
            self.assertEqual(h.written, {"a": ("incr", "default")})
            result = history.check_files(h.paths())
        self.assertEqual(result["operations"], 1)

    def test_lists_move_to_a_new_key_at_their_cap(self):
        worker = workload.AppendWorker.__new__(workload.AppendWorker)
        sent = []
        worker.keys, worker.base, worker.generation, worker.next, worker.max_len = ["l"], ["l"], [0], {"l": 1}, 3
        worker.record = lambda f, k, v, call: sent.append((k, v))
        for _ in range(5):
            worker.step()
        self.assertEqual(sent, [("l", 1), ("l", 2), ("l", 3), ("l:1", 1), ("l:1", 2)])

    def test_host_stats_are_parsed_per_unit(self):
        text = ("== ruvio@g1\nVmRSS:\t 2048 kB\nRssAnon:\t 1024 kB\nThreads:\t9\nCpuTicks: 77\nFds: 31\n"
                "DataBytes: 4096\nWalBytes: 1024\nCheckpointBytes: 2048\n== host\nLoad: 0.25\nDiskAvail: 10\n"
                "ClockOffsetS: -0.000004\n")
        processes, host = parse_host_stats(text, {"ruvio@g1": "g1"})
        self.assertEqual(processes["g1"], {"rss_kib": 2048, "rss_anon_kib": 1024, "threads": 9, "cpu_ticks": 77,
                                           "fds": 31, "data_dir_bytes": 4096, "wal_bytes": 1024, "checkpoint_bytes": 2048})
        self.assertEqual(host, {"load1": 0.25, "disk_avail_bytes": 10, "clock_offset_s": -0.000004})
        _, host = parse_host_stats("== host\nCpuStealTicks: 5\nIoFullUs: 1200\nDiskFlushes: 7\nDirtyKib: 64\n", {})
        self.assertEqual(host, {"cpu_steal_ticks": 5, "io_full_us": 1200, "disk_flushes": 7, "dirty_kib": 64})

    def test_a_host_that_waited_on_its_disk_is_listed_as_a_stall(self):
        from ruvio_soak import report

        def sample(t, host, io_full_us, steal):
            return {"t": t, "host": host, "io_full_us": io_full_us, "cpu_steal_ticks": steal, "clk_tck": 100,
                    "disk_writes": t * 100}
        hosts = [sample(0, "host0", 0, 0), sample(10, "host0", 1_000_000, 10), sample(20, "host0", 9_000_000, 10),
                 sample(0, "host1", 0, 0), sample(10, "host1", 0, 800), sample(0, "controller", 0, 0)]
        run = report.Run.__new__(report.Run)
        run.hosts = hosts
        stalls = report._host_stalls(run)
        self.assertEqual([(s["host"], s["t"]) for s in stalls], [("host1", 10), ("host0", 20)])
        self.assertEqual(stalls[1]["io_full_share"], 0.8)
        self.assertEqual(stalls[0]["steal_share"], 0.8)
        self.assertEqual(stalls[1]["disk_writes_per_s"], 100.0)

    def test_lease_and_snapshot_settings_reach_the_nodes(self):
        cluster = Cluster(hosts=["10.0.0.1", "10.0.0.2", "10.0.0.3"], primary_lease_ms=2000, snapshot_interval_ms=60000)
        env = cluster.node_env(1, "g1", "/d", "/t")
        self.assertEqual(env["RUVIO_PRIMARY_LEASE"], "yes")
        self.assertEqual(env["RUVIO_PRIMARY_LEASE_COORDINATORS"], "1,2,3")
        self.assertEqual(env["RUVIO_SNAPSHOT_INTERVAL_MS"], "60000")
        self.assertEqual(cluster.node_env(1, "g1", "/d", "/t", as_primary=True)["RUVIO_REPLICATION_ROLE"], "primary")
        self.assertNotIn("RUVIO_PRIMARY_LEASE", Cluster(hosts=["a", "b", "c"]).node_env(0, "g1", "/d", "/t"))

    def test_the_coordinator_partition_cuts_only_the_raft_port(self):
        commands = []
        backend = SshBackend(Cluster(hosts=["10.0.0.1", "10.0.0.2", "10.0.0.3"]), "ruvio", "soak",
                             run=lambda address, command, stdin=None: commands.append((address, command, stdin)) or "")
        backend.partition_coordinator(0, 60)
        rules = commands[-1][2].decode()
        self.assertIn("tcp dport 26400 drop", rules)
        self.assertIn("{ 10.0.0.2, 10.0.0.3 }", rules)
        self.assertTrue(any("--on-active=60" in c for _, c, _ in commands))


    def report_run(self, directory, metrics, findings=(), events=None):
        """A run directory: load 1-10000 s, one kill-primary fault at 5000-5060 s healed, and
        the given metrics and findings (finding times are wall seconds, written as monotonic
        nanoseconds with the events' clock offset of zero)."""
        events = events or [
            {"event": "load-start", "t": 1, "mono": 10**9},
            {"event": "fault", "kind": "kill-primary", "t": 5000, "mono": 5000 * 10**9},
            {"event": "healed", "kind": "kill-primary", "t": 5060, "recovery_s": 4, "mono": 5060 * 10**9},
            {"event": "load-stop", "t": 10000, "mono": 10000 * 10**9},
        ]
        (directory / "plan.json").write_text("{}")
        (directory / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in events))
        (directory / "metrics.jsonl").write_text("".join(json.dumps(m) + "\n" for m in metrics))
        (directory / "findings.jsonl").write_text(
            "".join(json.dumps([p, c, int(t * 10**9), k]) + "\n" for p, c, t, k in findings))
        (directory / "check.json").write_text(json.dumps({"profiles": {
            "durable": {}, "default": {"lost": len(findings)}}, "attribution": {}}))

    def test_a_failover_outside_every_fault_fails_and_explains_its_losses(self):
        from ruvio_soak import report

        metrics = [{"t": t, "process": "host0-coordinator", "failovers_total": str(n),
                    "last_failover_unix_ms": str(ms)}
                   for t, n, ms in ((100, 0, 0), (2000, 1, 1_995_000), (5100, 2, 5_003_000))]
        with tempfile.TemporaryDirectory() as directory:
            self.report_run(Path(directory), metrics, [
                ("default", "lost", 1990, "k1"),  # just before the failover at 1995 s
                ("default", "lost", 5010, "k2"),  # inside the fault
                ("default", "lost", 8000, "k3"),  # neither
            ])
            criteria = {c["id"]: c for c in report.write(Path(directory))["criteria"]}
        self.assertEqual(criteria[8]["status"], "fail")
        self.assertEqual(len(criteria[8]["value"]["outside_faults"]), 1)
        default = criteria[2]["details"]["default"]
        self.assertEqual(default["by_uninjected_failover"], {"lost": 1})
        self.assertEqual(default["unexplained"], {"lost": 1})

    def test_memory_and_checkpoints_are_judged_against_the_data(self):
        from ruvio_soak import report

        # The data doubles between the quiet windows; anonymous memory and the checkpoint
        # double with it, the WAL stays flat: no leak, no growth beyond the data.
        def sample(t, scale):
            return {"t": t, "process": "host0-g1", "rss_anon_kib": 200_000 * scale,
                    "charged_memory_bytes": str(100_000 * 1024 * scale), "wal_bytes": 300 << 20,
                    "checkpoint_bytes": (200 << 20) * scale}
        metrics = [sample(t, 1) for t in range(3700, 5000, 60)] + [sample(t, 2) for t in range(8700, 10000, 60)]
        with tempfile.TemporaryDirectory() as directory:
            self.report_run(Path(directory), metrics)
            run = report.Run(Path(directory), settle_s=3600)
            run.hosts = [{"disk_avail_bytes": 90, "disk_size_bytes": 100}]
            memory, disk = report._memory(run), report._disk(run)
        self.assertEqual(memory["status"], "pass", memory)
        self.assertEqual(disk["status"], "pass", disk)

    def test_hot_tags_land_in_the_moved_range_and_differ(self):
        from ruvio_soak import workload
        from ruvio_soak.resp import key_slot

        tags = [workload.hot_tag(["4096-4607"], n) for n in range(6)]
        self.assertEqual(len(set(tags)), 6)
        self.assertTrue(all(4096 <= key_slot("{" + t + "}") <= 4607 for t in tags))
        self.assertEqual(workload.hot_tag(["4096-4607", "9000-9100"], 1) != workload.hot_tag(["4096-4607"], 1), True)
        self.assertTrue(9000 <= key_slot("{" + workload.hot_tag(["4096-4607", "9000-9100"], 1) + "}") <= 9100)

    def test_a_move_with_failures_after_five_seconds_fails_criterion_4(self):
        from ruvio_soak import report

        def run_with(fails):
            events = [
                {"event": "load-start", "t": 1, "mono": 10**9},
                {"event": "fault", "kind": "slot-churn", "t": 3000, "down_s": 0, "mono": 3000 * 10**9},
                {"event": "slot-move", "kind": "slot-churn", "t": 3005, "s": 1.2, "fails_after_5s": fails, "mono": 3005 * 10**9},
                {"event": "healed", "kind": "slot-churn", "t": 3020, "recovery_s": 20, "mono": 3020 * 10**9},
                {"event": "load-stop", "t": 10000, "mono": 10000 * 10**9},
            ]
            with tempfile.TemporaryDirectory() as directory:
                self.report_run(Path(directory), [], events=events)
                return {c["id"]: c for c in report.write(Path(directory))["criteria"]}

        self.assertEqual(run_with(0)[4]["status"], "pass")
        self.assertEqual(run_with(3)[4]["status"], "fail")
        self.assertEqual(run_with(0)[5]["value"]["move_max_s"], 1.2)

    def test_recovery_is_counted_from_the_end_of_the_fault(self):
        from ruvio_soak import report

        # A 30 s fault healed 33 s after it began recovered in 3 s; a slot move counts whole.
        events = [
            {"event": "load-start", "t": 1, "mono": 10**9},
            {"event": "fault", "kind": "kill-host", "t": 3000, "down_s": 30, "mono": 3000 * 10**9},
            {"event": "healed", "kind": "kill-host", "t": 3033, "recovery_s": 33, "mono": 3033 * 10**9},
            {"event": "fault", "kind": "migrate-slots", "t": 5000, "down_s": 0, "mono": 5000 * 10**9},
            {"event": "healed", "kind": "migrate-slots", "t": 5013, "recovery_s": 13, "mono": 5013 * 10**9},
            {"event": "fault", "kind": "kill-primary", "t": 7000, "down_s": 10, "mono": 7000 * 10**9},
            {"event": "healed", "kind": "kill-primary", "t": 7026, "recovery_s": 26, "mono": 7026 * 10**9},
            {"event": "load-stop", "t": 10000, "mono": 10000 * 10**9},
        ]
        with tempfile.TemporaryDirectory() as directory:
            self.report_run(Path(directory), [], events=events)
            criteria = {c["id"]: c for c in report.write(Path(directory))["criteria"]}
        self.assertEqual(criteria[5]["status"], "fail")  # kill-primary took 16 s after its fault
        self.assertEqual(criteria[5]["value"]["max_s"], 16)
        self.assertEqual(criteria[5]["value"]["move_max_s"], 13)
        self.assertEqual(criteria[5]["details"]["kill-host"]["max_s"], 3)

    def test_throughput_is_compared_only_within_one_primary_placement(self):
        from ruvio_soak import report

        # Quiet minutes 2-49 with g2 on host1 at 300 ops, 51-99 on host0 at 360 ops: each
        # placement is steady, so the 18 % step between them is not a failure.
        def roles(t, g2):
            return [{"t": t, "process": "host0-g1", "replication_role": "primary"},
                    {"t": t, "process": g2, "replication_role": "primary"}]
        metrics = [m for t in range(0, 10000, 60) for m in roles(t, "host1-g2" if t < 5000 else "host0-g2")]
        with tempfile.TemporaryDirectory() as directory:
            self.report_run(Path(directory), metrics, events=[
                {"event": "load-start", "t": 1, "mono": 10**9},
                {"event": "load-stop", "t": 10000, "mono": 10000 * 10**9},
            ])
            (Path(directory) / "background-latency-0.jsonl").write_text("".join(
                json.dumps({"t": t, "series": "bg:get", "counts": {"ok": 300 if t < 5000 else 360}}) + "\n"
                for t in range(0, 10000, 60)))
            run = report.Run(Path(directory), settle_s=100)
            criteria = {c["id"]: c for c in report.write(Path(directory))["criteria"]}
        # No fault: the first and the last third of the load after the settle time.
        self.assertEqual(run.quiet_a, (101, 101 + (10000 - 101) / 3))
        self.assertEqual(run.quiet_b, (10000 - (10000 - 101) / 3, 10000))
        self.assertEqual(criteria[11]["status"], "pass", criteria[11])
        self.assertEqual(criteria[11]["value"]["placements"], 2)

    def test_a_quarter_of_the_minutes_may_dip_in_the_throughput(self):
        from ruvio_soak import report

        steady = [100, 101, 99, 100, 102, 98, 100, 100]
        self.assertTrue(report._throughput_minutes(steady)["steady"])
        one_dip = [100, 101, 99, 90, 102, 98, 100, 100]
        judged = report._throughput_minutes(one_dip)
        self.assertTrue(judged["steady"], judged)
        self.assertEqual(judged["dips"], [0.9])
        # Two dips in eight minutes, a dip to -20 % and a +-6 % spread are within the limits.
        self.assertTrue(report._throughput_minutes([100, 90, 99, 90, 102, 98, 100, 100])["steady"])
        self.assertTrue(report._throughput_minutes([100, 101, 99, 80, 102, 98, 100, 100])["steady"])
        self.assertTrue(report._throughput_minutes([100, 112, 99, 100, 102, 98, 100, 100])["steady"])
        # Three dips in eight minutes, a dip below -25 % or a +-10 % spread fail.
        self.assertFalse(report._throughput_minutes([100, 90, 99, 90, 91, 98, 100, 100])["steady"])
        self.assertFalse(report._throughput_minutes([100, 101, 99, 70, 102, 98, 100, 100])["steady"])
        self.assertFalse(report._throughput_minutes([100, 120, 99, 100, 102, 98, 100, 100])["steady"])

    def stall_hosts(self, directory, io_full_us_at):
        """hosts.jsonl samples every 60 s from 0 to 9960 s on one node host; `io_full_us_at`
        maps a sample time to the cumulative io_full_us (else 0)."""
        (directory / "hosts.jsonl").write_text("".join(
            json.dumps({"t": t, "host": "host0", "io_full_us": io_full_us_at(t)}) + "\n"
            for t in range(0, 10000, 60)))

    def test_failures_in_a_host_stall_minute_are_not_unexplained(self):
        from ruvio_soak import report

        # A stall sample at t=6000 (60 % of its 60 s interval) widens to 5910-6030.
        def io(t):
            return 0 if t < 6000 else 36_000_000
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            events = [{"event": "load-start", "t": 1, "mono": 10**9},
                      {"event": "load-stop", "t": 10000, "mono": 10000 * 10**9}]
            self.report_run(directory, [], events=events)
            self.stall_hosts(directory, io)

            def series(timeouts_at, ok=1000):
                rows = []
                for t in range(60, 10000, 60):
                    counts = {"ok": ok}
                    if t in timeouts_at:
                        counts["timeout"] = 10
                    rows.append({"t": t, "series": "durable:set", "counts": counts})
                return "".join(json.dumps(r) + "\n" for r in rows)
            (directory / "latency.jsonl").write_text(series({5940}))
            first = {c["id"]: c for c in report.write(directory)["criteria"]}[7]
            # Ten timeouts in a quiet minute: 10 > max(5, 0.16) fails.
            (directory / "latency.jsonl").write_text(series({3000}))
            second = {c["id"]: c for c in report.write(directory)["criteria"]}[7]
            # Four timeouts outside a stall are within the allowance of 5.
            rows = [json.loads(l) for l in series({3000}).splitlines()]
            for r in rows:
                r["counts"]["timeout"] = 4 if "timeout" in r["counts"] else 0
                if not r["counts"]["timeout"]:
                    del r["counts"]["timeout"]
            (directory / "latency.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
            third = {c["id"]: c for c in report.write(directory)["criteria"]}[7]
            # Past 5 the allowance is one per million ok ops: 20 failures among 300 M ok ops pass.
            big = [{"t": t, "series": "durable:set", "counts": {"ok": 300_000_000 if t == 60 else 1000,
                                                                "timeout": 20 if t == 3000 else 0}}
                   for t in range(60, 10000, 60)]
            for r in big:
                if not r["counts"]["timeout"]:
                    del r["counts"]["timeout"]
            (directory / "latency.jsonl").write_text("".join(json.dumps(r) + "\n" for r in big))
            fourth = {c["id"]: c for c in report.write(directory)["criteria"]}[7]
        self.assertEqual(first["status"], "pass", first)
        self.assertEqual(first["value"], {"unexplained": 0, "during_host_stall": 10})
        self.assertEqual(first["details"]["during_host_stall"], {"durable:set:timeout": 10})
        self.assertEqual(second["status"], "fail", second)
        self.assertEqual(second["value"], {"unexplained": 10, "during_host_stall": 0})
        self.assertEqual(third["status"], "pass", third)
        self.assertEqual(fourth["status"], "pass", fourth)

    def test_stall_minutes_are_left_out_of_the_throughput_groups(self):
        from ruvio_soak import report

        # A flat 300 ops per minute with one minute at 150 (during a stall) in each of two
        # runs: left out, the placement is steady; counted, the dip would be -50 %.
        def io(t):
            return 0 if t < 6000 else 36_000_000
        metrics =[{"t": t, "process": "host0-g1", "replication_role": "primary"} for t in range(0, 10000, 60)]
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            self.report_run(directory, metrics, events=[
                {"event": "load-start", "t": 1, "mono": 10**9},
                {"event": "load-stop", "t": 10000, "mono": 10000 * 10**9}])
            self.stall_hosts(directory, io)
            (directory / "background-latency-0.jsonl").write_text("".join(
                json.dumps({"t": t, "series": "bg:get", "counts": {"ok": 150 if t == 5940 else 300}}) + "\n"
                for t in range(0, 10000, 60)))
            criteria = {c["id"]: c for c in report.write(directory)["criteria"]}
        self.assertEqual(criteria[11]["status"], "pass", criteria[11])
        self.assertEqual(criteria[11]["details"][0]["dips"], [])

    def test_latency_step_between_1_2_and_1_5_passes_and_is_listed(self):
        from ruvio_soak import report

        def hist(ms):
            return {str(latency.bucket(ms * 1000)): 1000}
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            self.report_run(directory, [], events=[
                {"event": "load-start", "t": 1, "mono": 10**9},
                {"event": "load-stop", "t": 10000, "mono": 10000 * 10**9}])
            rows = [{"t": t, "series": "durable:set", "counts": {"ok": 1000},
                     "hist": hist(10.0 if t < 7000 else 13.0)} for t in range(0, 10000, 60)]
            (directory / "latency.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
            criteria = {c["id"]: c for c in report.write(directory)["criteria"]}
        self.assertEqual(criteria[9]["status"], "pass", criteria[9])
        self.assertEqual(criteria[9]["details"]["series_over_1_2"], ["durable:set"])

    def test_a_coordinator_with_a_late_leader_change_is_not_judged(self):
        from ruvio_soak import report

        # host0 becomes leader at t=9000: 45 min later is 11700, past the end of the run, so
        # nothing is left and it is not judged; host1 grows the whole time and fails.
        def sample(t, process, leader, anon):
            return {"t": t, "process": process, "is_leader": leader, "rss_anon_kib": anon}
        metrics = []
        for t in range(0, 10000, 60):
            metrics.append(sample(t, "host0-coordinator", "1" if t >= 9000 else "0",
                                  9216 if t >= 9000 else 3072))
            metrics.append(sample(t, "host1-coordinator", "0", 3072 + t))
        with tempfile.TemporaryDirectory() as directory:
            self.report_run(Path(directory), metrics, events=[
                {"event": "load-start", "t": 1, "mono": 10**9},
                {"event": "load-stop", "t": 10000, "mono": 10000 * 10**9},
            ])
            criteria = {c["id"]: c for c in report.write(Path(directory))["criteria"]}
        self.assertEqual(criteria[13]["details"]["host0-coordinator"]["status"], "not_judged")
        self.assertIn("leader changed at 02:30", criteria[13]["details"]["host0-coordinator"]["reason"])
        self.assertEqual(criteria[13]["value"]["host1-coordinator"], "fail", criteria[13]["details"])
        self.assertEqual(criteria[13]["status"], "fail")

    def test_a_coordinator_is_judged_only_since_its_last_leadership_change(self):
        from ruvio_soak import report

        # host0 becomes leader at t=5040 and its memory steps from 3 to 9 MiB, then stays flat;
        # host1 never changes and grows the same way: only host1 fails.
        def sample(t, process, leader, anon):
            return {"t": t, "process": process, "is_leader": leader, "rss_anon_kib": anon}
        metrics = []
        for t in range(0, 10000, 60):
            metrics.append(sample(t, "host0-coordinator", "1" if t >= 5000 else "0",
                                  9216 if t >= 5000 else 3072))
            metrics.append(sample(t, "host1-coordinator", "0", 9216 if t >= 5000 else 3072))
        with tempfile.TemporaryDirectory() as directory:
            self.report_run(Path(directory), metrics, events=[
                {"event": "load-start", "t": 1, "mono": 10**9},
                {"event": "load-stop", "t": 10000, "mono": 10000 * 10**9},
            ])
            criteria = {c["id"]: c for c in report.write(Path(directory))["criteria"]}
        self.assertEqual(criteria[13]["value"], {"host0-coordinator": "pass", "host1-coordinator": "fail"},
                         criteria[13]["details"])
        self.assertEqual(criteria[13]["details"]["host0-coordinator"]["leader_changed_at"], 5040)

    def test_a_report_without_data_is_never_a_pass(self):
        from ruvio_soak import report

        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / "plan.json").write_text("{}")
            result = report.write(Path(directory))
            self.assertTrue((Path(directory) / "report.md").exists())
        self.assertEqual(result["verdict"], "incomplete")
        self.assertEqual({r["status"] for r in result["criteria"]}, {"not_measured"})
        self.assertEqual(len(result["criteria"]), 17)


    def env_hosts(self, directory, stalled, steal_at=()):
        """hosts.jsonl: one node host sampled every 10 s from 0 to 9990 s; each time in
        `stalled` is a sample interval in which every task waited on the disk the whole time."""
        io, rows = 0, []
        for t in range(0, 10000, 10):
            if t in stalled:
                io += 10_000_000
            rows.append({"t": t, "host": "host0", "io_full_us": io, "clk_tck": 100,
                         "cpu_steal_ticks": 5 if t in steal_at else 0})
        (directory / "hosts.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))

    def environment_run(self, stalled=(), steal_at=(), preflight=None, lost=False):
        from ruvio_soak import report

        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            self.report_run(directory, [], events=[{"event": "load-start", "t": 1, "mono": 10**9},
                                                   {"event": "load-stop", "t": 10000, "mono": 10000 * 10**9}])
            self.env_hosts(directory, set(stalled), set(steal_at))
            if preflight is not None:
                (directory / "preflight.json").write_text(json.dumps(preflight))
            if lost:
                (directory / "check.json").write_text(json.dumps({"profiles": {"durable": {"lost": 1}}, "attribution": {}}))
            return report.write(directory)

    def recovery_report(self, bad_minute=None, extra_stall=None):
        """One 10 s host stall ending at 5000 s; latency and background minutes around it."""
        from ruvio_soak import report

        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            self.report_run(directory, [], events=[{"event": "load-start", "t": 1, "mono": 10**9},
                                                   {"event": "load-stop", "t": 10000, "mono": 10000 * 10**9}])
            self.env_hosts(directory, {5000} | set(extra_stall or ()))
            fast, slow = {str(latency.bucket(1000)): 100}, {str(latency.bucket(50_000)): 100}
            (directory / "latency.jsonl").write_text("".join(json.dumps(
                {"t": t, "series": "durable:set", "counts": {"ok": 100}, "hist": slow if t == bad_minute else fast}) + "\n"
                for t in range(60, 10000, 60)))
            (directory / "background-latency-0.jsonl").write_text("".join(json.dumps(
                {"t": t, "series": "bg:get", "counts": {"ok": 400 if t == bad_minute else 1000}}) + "\n"
                for t in range(60, 10000, 60)))
            return {c["id"]: c for c in report.write(directory, settle_s=100)["criteria"]}[19]

    def test_a_stall_followed_by_a_normal_minute_passes_the_recovery_criterion(self):
        criterion = self.recovery_report()
        self.assertEqual(criterion["status"], "pass", criterion)
        self.assertEqual(criterion["value"]["judged"], 1)

    def test_slow_latency_or_low_throughput_after_a_stall_fails_it(self):
        criterion = self.recovery_report(bad_minute=5040)
        self.assertEqual(criterion["status"], "fail", criterion)
        self.assertEqual(criterion["value"]["slow"], 1)
        self.assertEqual(criterion["details"][0]["min_minute_share"], 0.4)

    def test_two_stalls_close_together_are_not_judged(self):
        # The slow minute 5040 follows the first stall, but the second one starts 50 s after it:
        # only the second is judged (its own minutes are normal).
        criterion = self.recovery_report(bad_minute=5040, extra_stall={5060})
        self.assertEqual((criterion["status"], criterion["value"]["stalls"], criterion["value"]["judged"]), ("pass", 2, 1), criterion)

    def test_a_run_without_a_stall_passes_it_and_an_old_run_has_no_such_criterion(self):
        from ruvio_soak import report

        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            self.report_run(directory, [], events=[{"event": "load-start", "t": 1, "mono": 10**9},
                                                   {"event": "load-stop", "t": 10000, "mono": 10000 * 10**9}])
            self.assertNotIn(19, {c["id"] for c in report.write(directory)["criteria"]})
            self.env_hosts(directory, set())
            self.assertEqual({c["id"]: c for c in report.write(directory)["criteria"]}[19]["status"], "pass")

    def test_the_lock_and_transfer_check_becomes_criterion_18_only_for_plans_that_run_it(self):
        from ruvio_soak import report

        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            self.report_run(directory, [])
            self.assertNotIn(18, {c["id"] for c in report.write(directory)["criteria"]})
            (directory / "plan.json").write_text(json.dumps({"load": {"primitives": {"leases": 1}}}))
            ids = lambda: {c["id"]: c for c in report.write(directory)["criteria"]}[18]["status"]
            self.assertEqual(ids(), "not_measured")                       # no file: not run
            checked = {"verdict": "ok", "problems": [], "linearizability": {"ran": True, "illegal": [], "unknown": []}}
            (directory / "check-primitives.json").write_text(json.dumps(checked))
            self.assertEqual(ids(), "pass")
            (directory / "check-primitives.json").write_text(json.dumps({**checked, "linearizability": {"ran": False, "reason": "no go toolchain"}}))
            self.assertEqual(ids(), "not_measured")                       # never a pass without the checker
            (directory / "check-primitives.json").write_text(json.dumps(
                {**checked, "verdict": "fail", "problems": ["x"], "linearizability": {"ran": True, "illegal": ["lease:a"], "unknown": []}}))
            self.assertEqual(ids(), "fail")

    def test_a_quiet_run_is_not_an_invalid_environment(self):
        result = self.environment_run(stalled=range(100, 200, 20))
        self.assertFalse(result["environment"]["invalid"], result["environment"])
        self.assertEqual(result["environment"]["stalls"], 5)
        self.assertEqual(result["environment"]["longest_stall_s"], 10.0)
        self.assertNotEqual(result["verdict"], "invalid_environment")

    def test_too_much_stalled_time_makes_the_run_invalid(self):
        # 130 separate 10 s stalls: 13 % of a 10,000 s load, none of them long.
        result = self.environment_run(stalled=range(20, 2620, 20))
        env = result["environment"]
        self.assertEqual((env["stalls"], env["longest_stall_s"]), (130, 10.0))
        self.assertTrue(env["invalid"])
        self.assertEqual(result["verdict"], "invalid_environment")
        self.assertIn("13.0%", env["reasons"][0])

    def test_one_long_stall_makes_the_run_invalid(self):
        # Seven consecutive stalled samples are one 70 s stall.
        result = self.environment_run(stalled=range(500, 570, 10))
        env = result["environment"]
        self.assertEqual((env["stalls"], env["longest_stall_s"]), (1, 70.0))
        self.assertEqual(result["verdict"], "invalid_environment")

    def test_steal_and_a_failed_preflight_make_the_run_invalid(self):
        self.assertEqual(self.environment_run(steal_at=[300])["verdict"], "invalid_environment")
        failed = self.environment_run(preflight={"10.0.0.2": {"ok": False}, "10.0.0.1": {"ok": True}})
        self.assertEqual(failed["verdict"], "invalid_environment")
        self.assertEqual(failed["environment"]["preflight"], "fail")
        passed = self.environment_run(preflight={"10.0.0.1": {"ok": True}})
        self.assertEqual(passed["environment"]["preflight"], "pass")
        self.assertFalse(passed["environment"]["invalid"])

    def test_a_lost_write_is_never_put_down_to_the_environment(self):
        result = self.environment_run(stalled=range(500, 570, 10), lost=True)
        self.assertTrue(result["environment"]["invalid"])
        self.assertEqual(result["verdict"], "fail")


class Preflight(unittest.TestCase):
    def test_measure_counts_syncs_and_removes_its_file(self):
        from ruvio_soak import preflight

        now = [0.0]

        def clock():
            now[0] += 0.01   # each sync takes 20 ms: two clock reads
            return now[0]
        with tempfile.TemporaryDirectory() as directory:
            result = preflight.measure(directory, 1.0, clock=clock, sync=lambda fd: None)
            self.assertEqual(list(Path(directory).iterdir()), [])
        self.assertGreater(result["syncs"], 10)
        self.assertEqual(result["stalls_over_1s"], 0)
        self.assertAlmostEqual(result["p50_ms"], 10.0, delta=1)

    def test_a_slow_disk_and_a_stall_fail_the_check(self):
        from ruvio_soak import preflight

        good = {"syncs": 10, "seconds": 1, "iops": 900.0, "p50_ms": 1, "p99_ms": 2, "max_ms": 9, "stalls_over_1s": 0}
        self.assertTrue(preflight.judge(good, 1000)["ok"])          # 90 % of the target
        self.assertTrue(preflight.judge(good, None)["ok"])          # no target: only stalls fail
        slow = preflight.judge({**good, "iops": 700.0}, 1000)
        self.assertFalse(slow["ok"])
        self.assertIn("under 80%", slow["problems"][0])
        stalled = preflight.judge({**good, "stalls_over_1s": 1, "max_ms": 2500}, 1000)
        self.assertFalse(stalled["ok"])
        self.assertFalse(preflight.judge({**good, "syncs": 0, "iops": 0.0}, None)["ok"])

    def test_every_node_is_checked_over_ssh_with_the_script_on_stdin(self):
        from unittest import mock

        from ruvio_soak import cli

        calls = []

        class Fake:
            def __init__(self, cluster, binary, user, key=None):
                pass

            def run(self, address, command, stdin=None, timeout=None):
                calls.append((address, command, stdin))
                if address == "10.0.0.3":
                    raise RuntimeError("ssh 10.0.0.3: exited 255")
                return json.dumps({"ok": True, "iops": 900.0}) + "\n"
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(cli, "SshBackend", Fake):
            options = SimpleNamespace(hosts="10.0.0.1,10.0.0.2,10.0.0.3", dir="/var/lib/ruvio", seconds=30.0,
                                      target_iops=800.0, ssh_user="soak", ssh_key=None, out=directory)
            code = cli.preflight_command(options)
            written = json.loads((Path(directory) / "preflight.json").read_text())
        self.assertEqual(code, 1)
        self.assertEqual(sorted(c[0] for c in calls), ["10.0.0.1", "10.0.0.2", "10.0.0.3"])
        self.assertEqual(calls[0][1], "sudo python3 - --dir /var/lib/ruvio --seconds 30 --target-iops 800")
        self.assertIn(b"def measure", calls[0][2])
        self.assertTrue(written["10.0.0.1"]["ok"])
        self.assertFalse(written["10.0.0.3"]["ok"])

class AzurePlans(unittest.TestCase):
    def plan(self, name):
        return json.loads((Path(__file__).parent / "plans" / name).read_text())

    def test_the_long_azure_plans_run_the_slot_churn_and_the_lock_check(self):
        for name in ("azure-24h.json", "azure-1h-tools.json"):
            plan = self.plan(name)
            kinds = {s["kind"] for s in plan["faults"]["sequence"]}
            self.assertTrue({"slot-churn", "rebalance-slots", "migrate-slots"} <= kinds, name)
            self.assertTrue({"slot-churn", "rebalance-slots"} <= set(plan["faults"]["kinds"]), name)
            self.assertTrue(plan["load"]["primitives"], name)
            Cluster.from_json({**plan["cluster"], "hosts": ["10.0.0.1", "10.0.0.2", "10.0.0.3"]})

    def test_the_controller_has_a_go_toolchain_for_the_lock_check(self):
        text = (Path(__file__).parent / "azure" / "cloud-init" / "controller.yaml").read_text()
        self.assertIn("  - golang-go\n", text)


class PreflightOverSsh(unittest.TestCase):
    """The preflight script as the ssh path sends it: its source on the stdin of `python3 -`."""

    def local_backend(self, seen):
        import subprocess

        class Local:
            def __init__(self, cluster, binary, user, key=None):
                pass

            def run(self, address, command, stdin=None, timeout=None):
                seen.append(timeout)
                self_command = command.replace("sudo ", "", 1).split()
                result = subprocess.run(["python3", "-I", *self_command[1:]], input=stdin, capture_output=True, timeout=60)
                if result.returncode:
                    raise RuntimeError(result.stderr.decode()[-300:])
                return result.stdout.decode()
        return Local

    def test_the_script_runs_from_stdin_and_the_ssh_timeout_covers_its_length(self):
        from unittest import mock

        from ruvio_soak import cli

        seen = []
        with tempfile.TemporaryDirectory() as data, tempfile.TemporaryDirectory() as out, \
                mock.patch.object(cli, "SshBackend", self.local_backend(seen)):
            options = SimpleNamespace(hosts="a,b", dir=data, seconds=1.0, target_iops=None, ssh_user="soak", ssh_key=None, out=out)
            code = cli.preflight_command(options)
            written = json.loads((Path(out) / "preflight.json").read_text())
            self.assertEqual(list(Path(data).iterdir()), [])
        self.assertEqual(code, 0)
        self.assertTrue(all(r["ok"] and r["syncs"] > 0 for r in written.values()), written)
        self.assertEqual(seen, [91.0, 91.0])   # seconds + 90: the 120 s default cut a 120 s check off

    def test_a_real_ssh_timeout_becomes_a_failed_node_not_a_crash(self):
        import subprocess
        from unittest import mock

        backend = SshBackend(Cluster(hosts=["10.0.0.1"]), "", "soak")
        with mock.patch.object(subprocess, "run", side_effect=subprocess.TimeoutExpired("ssh", 5)):
            with self.assertRaises(RuntimeError) as caught:
                backend.run("10.0.0.1", "sudo python3 - --dir /x", timeout=5)
        self.assertIn("did not finish in 5 s", str(caught.exception))

    def test_a_failed_preflight_file_stops_a_run_before_it_starts(self):
        from ruvio_soak import cli

        with tempfile.TemporaryDirectory() as directory:
            bad = Path(directory) / "preflight.json"
            bad.write_text(json.dumps({"10.0.0.1": {"ok": True}, "10.0.0.2": {"ok": False, "problems": ["slow"]}}))
            options = SimpleNamespace(plan=str(Path(directory) / "none.json"), duration=None, preflight=str(bad), out=directory)
            plan = Path(directory) / "none.json"
            plan.write_text("{}")
            self.assertEqual(cli.run(options), 2)
            self.assertEqual([p.name for p in Path(directory).iterdir() if p.is_dir()], [])


class AzureMonitorExport(unittest.TestCase):
    VM = {"name": "node0", "id": "/subscriptions/s/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/node0",
          "properties": {"hardwareProfile": {"vmSize": "Standard_D4ads_v6"}}}

    def fakes(self, offered, fail_metrics=False):
        calls = []

        def imds(path, **params):
            return ({"resourceGroupName": "rg", "subscriptionId": "s", "name": "ctl"} if path == "instance/compute"
                    else {"access_token": "tok"})

        def get(url, token):
            calls.append(url)
            assert token == "tok"
            if "virtualMachines?" in url:
                return {"value": [self.VM]}
            if "metricDefinitions" in url:
                return {"value": [{"name": {"value": n}} for n in offered]}
            if fail_metrics:
                raise OSError("boom")
            return {"value": [{"name": {"value": "Data Disk IOPS Consumed Percentage"}, "timeseries": [{"data": [
                {"timeStamp": "2026-11-28T10:00:00Z", "average": 90.0, "maximum": 100.0},
                {"timeStamp": "2026-11-28T10:01:00Z", "average": 50.0, "maximum": 60.0}]}]}]}
        return get, imds, calls

    def test_metrics_are_read_by_name_and_a_missing_name_is_listed(self):
        from ruvio_soak import azure_monitor

        get, imds, calls = self.fakes(["Data Disk IOPS Consumed Percentage", "Percentage CPU"])
        document = azure_monitor.export("RG", "2026-11-28T10:00:00Z", "2026-11-28T11:00:00Z", get=get, imds=imds, log=lambda *a: None)
        vm = document["vms"]["node0"]
        self.assertEqual(vm["summary"]["Data Disk IOPS Consumed Percentage"],
                         {"points": 2, "avg": 70.0, "max": 100.0, "last_point": "2026-11-28T10:01:00Z"})
        self.assertIn("Data Disk Queue Depth", vm["missing"])
        self.assertNotIn("Percentage CPU", vm["missing"])
        query = calls[-1]
        self.assertIn("metricnames=Percentage%20CPU%2CData%20Disk%20IOPS", query.replace("+", "%20"))
        self.assertIn("timespan=2026-11-28T10%3A00%3A00Z%2F2026-11-28T11%3A00%3A00Z", query)

    def test_a_failed_read_is_recorded_not_raised_and_another_group_is_refused(self):
        from ruvio_soak import azure_monitor

        get, imds, _ = self.fakes(["Percentage CPU"], fail_metrics=True)
        document = azure_monitor.export("rg", "a", "b", get=get, imds=imds, log=lambda *a: None)
        self.assertIn("OSError", document["vms"]["node0"]["error"])
        with tempfile.TemporaryDirectory() as directory:
            self.assertIsNone(azure_monitor.export_to(directory, "other-group", "a", "b", get=get, imds=imds))
            self.assertEqual(list(Path(directory).iterdir()), [])
            self.assertIsNotNone(azure_monitor.export_to(directory, "rg", "a", "b", get=get, imds=imds, log=lambda *a: None))
            self.assertTrue((Path(directory) / "azure-monitor.json").exists())


class LocalDiskRig(unittest.TestCase):
    def test_the_freeze_plan_has_no_faults_and_the_azure_cluster_settings(self):
        import local_disk

        plan = local_disk.freeze_plan(420)
        self.assertEqual(plan["faults"]["kinds"], [])
        self.assertEqual(plan["cluster"]["down_after_ms"], 3000)
        Cluster.from_json({**plan["cluster"], "hosts": ["127.0.0.1"] * 3, "local": True})
        self.assertEqual(plan["duration_s"], 420)

    def test_a_machine_without_root_skips(self):
        from unittest import mock

        import local_disk

        with mock.patch.object(local_disk.os, "geteuid", return_value=1000):
            self.assertEqual(local_disk.main(["kill9", "--binary", "/bin/true"]), 2)


if __name__ == "__main__":
    unittest.main()
