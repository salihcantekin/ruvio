"""Command line: `run`, `check`, `render`. See tests/soak/README.md."""

from __future__ import annotations

import argparse
import hashlib
import json
import secrets
import sys
import tempfile
import time
from pathlib import Path

from . import history
from .backends import LocalBackend, SshBackend
from .cluster import Cluster, save
from .runner import Runner
from .status import BlobUploader

DEFAULT_SAS_FILE = Path.home() / ".ruvio-soak" / "blob-sas-url"


def _cluster(options, plan: dict) -> Cluster:
    overrides = dict(plan.get("cluster", {}))
    if options.backend == "local":
        overrides.update(hosts=["127.0.0.1"] * 3, local=True)
    else:
        overrides.update(hosts=options.hosts.split(","), local=False)
    return Cluster.from_json(overrides).with_new_secrets()


def run(options) -> int:
    plan = json.loads(Path(options.plan).read_text())
    if options.duration is not None:
        plan["duration_s"] = options.duration
    run_id = time.strftime("%Y%m%d-%H%M%S-", time.gmtime()) + secrets.token_hex(2)
    directory = Path(options.out) / run_id
    directory.mkdir(parents=True)
    cluster = _cluster(options, plan)
    save(cluster, directory / "cluster.json")
    (directory / "plan.json").write_text(json.dumps(plan, indent=2))
    if options.backend == "local":
        backend = LocalBackend(cluster, options.binary, directory / "nodes")
    else:
        backend = SshBackend(cluster, options.binary, options.ssh_user, options.ssh_key)
    uploader = BlobUploader.from_file(Path(options.blob_sas_file) if options.blob_sas_file else DEFAULT_SAS_FILE, run_id)
    print(f"run {run_id} in {directory} ({backend.name}); status upload {'on' if uploader else 'off'}", flush=True)
    runner = Runner(plan, cluster, backend, directory, run_id, uploader)
    runner.status.data["binary_sha256"] = hashlib.sha256(Path(options.binary).read_bytes()).hexdigest()
    status = runner.run()
    print(json.dumps({k: status[k] for k in ("verdict", "problems", "faults", "operations")}, indent=2))
    failed = status["verdict"] == "fail" or (options.strict and status["verdict"] == "warn")
    return 1 if failed else 0


def check(options) -> int:
    result = history.check_file(Path(options.history))
    print(json.dumps(result, indent=2))
    return 0 if result["verdict"] != "fail" else 1


def render(options) -> int:
    """Prints the configs one host would get (secrets replaced), to review them."""
    plan = json.loads(Path(options.plan).read_text()) if options.plan else {}
    cluster = _cluster(options, plan)
    cluster.password, cluster.secret = "<password>", "<secret>"
    host = options.host
    print(f"# /etc/ruvio/topology.toml\n{cluster.topology_toml()}")
    print(f"# /etc/ruvio/coordinator.toml\n{cluster.coordinator_toml(host, '/var/lib/ruvio/coordinator')}")
    for g in cluster.groups:
        env = cluster.node_env(host, g.name, f"/var/lib/ruvio/{g.name}", "/etc/ruvio/topology.toml")
        print(f"# /etc/ruvio/{g.name}.env\n" + "".join(f"{k}={v}\n" for k, v in sorted(env.items())))
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="ruvio-soak", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    def backend_options(p):
        p.add_argument("--backend", choices=["local", "ssh"], default="local")
        p.add_argument("--hosts", default="10.42.1.11,10.42.1.12,10.42.1.13", help="ssh: the node VMs' private addresses")

    p = commands.add_parser("run", help="run a plan")
    p.add_argument("plan")
    p.add_argument("--binary", required=True, help="the ruvio binary to run (copied to the VMs with ssh)")
    backend_options(p)
    p.add_argument("--ssh-user", default="soak")
    p.add_argument("--ssh-key")
    p.add_argument("--out", default=str(Path(tempfile.gettempdir()) / "ruvio-soak-runs"))
    p.add_argument("--duration", type=float, help="override the plan's duration_s")
    p.add_argument("--blob-sas-file", help=f"file holding a container SAS URL (default {DEFAULT_SAS_FILE})")
    p.add_argument("--strict", action="store_true", help="exit 1 on warn too")
    p.set_defaults(handler=run)

    p = commands.add_parser("check", help="check a history file")
    p.add_argument("history")
    p.set_defaults(handler=check)

    p = commands.add_parser("render", help="print one host's configs")
    p.add_argument("--plan")
    p.add_argument("--host", type=int, default=0)
    backend_options(p)
    p.set_defaults(handler=render)

    options = parser.parse_args(argv)
    return options.handler(options)


if __name__ == "__main__":
    sys.exit(main())
