"""Where the processes run and how faults reach them.

`LocalBackend` starts every process on this machine; it is how the tool itself is tested.
`SshBackend` drives systemd units on the VMs that cloud-init prepared (`ruvio@g1`, `ruvio@g2`,
`ruvio-coordinator`) and injects network faults with nftables and netem. Each network fault is
also scheduled for removal on the host itself (`systemd-run --on-active`), so a fault never
outlives a controller that died."""

from __future__ import annotations

import os
import shlex
import signal
import subprocess
from pathlib import Path

from .cluster import COORDINATOR, Cluster, env_file

NETWORK_FAULTS = ("isolate-host", "netem")


PROC_FIELDS = ("VmRSS", "VmHWM", "RssAnon", "RssFile", "RssShmem", "VmData", "VmSwap", "Threads")


def parse_proc_status(text: str) -> dict[str, int]:
    """The memory lines of `/proc/<pid>/status` (kB, `Threads` a count) as `rss_kib`,
    `rss_anon_kib`, ...: anonymous memory that keeps growing while the server's own count of
    its data stays flat points at the allocator or a leak, not at the data."""
    names = {"VmRSS": "rss_kib", "VmHWM": "rss_peak_kib", "RssAnon": "rss_anon_kib", "RssFile": "rss_file_kib",
             "RssShmem": "rss_shmem_kib", "VmData": "data_kib", "VmSwap": "swap_kib", "Threads": "threads"}
    values = {}
    for line in text.splitlines():
        field, _, rest = line.partition(":")
        if field in names and rest.split():
            try:
                values[names[field]] = int(rest.split()[0])
            except ValueError:
                pass
    return values


class Unsupported(Exception):
    """The backend cannot inject this fault."""


class LocalBackend:
    name = "local"

    def __init__(self, cluster: Cluster, binary: str, work: Path):
        self.cluster, self.binary, self.work = cluster, binary, work
        self.processes: dict[tuple[int, str], subprocess.Popen] = {}

    def supports(self, fault: str) -> bool:
        return fault not in NETWORK_FAULTS

    def deploy(self) -> None:
        self.work.mkdir(parents=True, exist_ok=True)
        (self.work / "topology.toml").write_text(self.cluster.topology_toml())
        for host in range(len(self.cluster.hosts)):
            directory = self.work / f"host{host}"
            directory.mkdir(exist_ok=True)
            (directory / "coordinator.toml").write_text(
                self.cluster.coordinator_toml(host, str(directory / "coordinator"))
            )
            (directory / "coordinator.toml").chmod(0o600)

    def _command(self, host: int, role: str, follow: str | None):
        directory = self.work / f"host{host}"
        if role == COORDINATOR:
            return [self.binary, "coordinator", "--config", str(directory / "coordinator.toml")], None
        env = self.cluster.node_env(host, role, str(directory / role), str(self.work / "topology.toml"), follow)
        return [self.binary], dict(os.environ, **env)

    def start(self, host: int, role: str, follow: str | None = None) -> None:
        """`follow`: start a data node as a replica of this replication address instead of in
        its configured role."""
        if self.running(host, role):
            return
        command, env = self._command(host, role, follow)
        log = open(self.work / f"host{host}-{role}.log", "ab")
        # The server looks for ruvio.toml in its working directory (and may write one there).
        self.processes[(host, role)] = subprocess.Popen(
            command, env=env, cwd=self.work / f"host{host}", stdout=log, stderr=subprocess.STDOUT, start_new_session=True
        )
        log.close()

    def signal(self, host: int, role: str, sig: int) -> None:
        process = self.processes.get((host, role))
        if process is not None and process.poll() is None:
            process.send_signal(sig)
            if sig == signal.SIGKILL:
                process.wait()

    def running(self, host: int, role: str) -> bool:
        process = self.processes.get((host, role))
        return process is not None and process.poll() is None

    def memory(self, host: int, role: str) -> dict[str, int] | None:
        process = self.processes.get((host, role))
        if process is None or process.poll() is not None:
            return None
        try:
            return parse_proc_status(Path(f"/proc/{process.pid}/status").read_text()) or None
        except OSError:
            pass
        try:
            out = subprocess.run(["ps", "-o", "rss=", "-p", str(process.pid)], capture_output=True, text=True)
            return {"rss_kib": int(out.stdout.strip())}
        except (OSError, ValueError):
            return None

    def isolate(self, host: int, seconds: int) -> None:
        raise Unsupported("isolate-host needs the ssh backend")

    def netem(self, host: int, spec: str, seconds: int) -> None:
        raise Unsupported("netem needs the ssh backend")

    def heal(self, host: int) -> None:
        pass

    def stop_all(self) -> None:
        for process in self.processes.values():
            if process.poll() is None:
                process.send_signal(signal.SIGCONT)
                process.send_signal(signal.SIGTERM)
        for process in self.processes.values():
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def unit(role: str) -> str:
    return "ruvio-coordinator" if role == COORDINATOR else f"ruvio@{role}"


class SshBackend:
    """`run(host_address, command)` runs a shell command on a VM and returns its stdout; tests
    pass a fake. Commands use sudo, which the VM's admin user has without a password."""

    name = "ssh"
    NFT_TABLE = "ruvio_soak"

    def __init__(self, cluster: Cluster, binary: str, user: str, key: str | None = None, run=None):
        self.cluster, self.binary, self.user, self.key = cluster, binary, user, key
        self.run = run or self._ssh
        self.interface = "eth0"
        self.fault_id = 0

    def _ssh_base(self, address: str) -> list[str]:
        command = ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new", "-o", "ConnectTimeout=5"]
        if self.key:
            command += ["-i", self.key]
        return command + [f"{self.user}@{address}"]

    def _ssh(self, address: str, command: str, stdin: bytes | None = None) -> str:
        result = subprocess.run(
            self._ssh_base(address) + [command], input=stdin, capture_output=True, timeout=120
        )
        if result.returncode != 0:
            raise RuntimeError(f"ssh {address}: {command.split()[0:3]} exited {result.returncode}: {result.stderr[-300:]!r}")
        return result.stdout.decode(errors="replace")

    def supports(self, fault: str) -> bool:
        return True

    def _write(self, address: str, path: str, data: bytes, mode: str = "0640") -> None:
        self.run(
            address,
            f"sudo install -D -m {mode} -o root -g ruvio /dev/stdin {shlex.quote(path)}",
            stdin=data,
        )

    def deploy(self) -> None:
        """Copies the binary and every config; stops whatever ran before and wipes the data
        directories, so each run starts from empty nodes."""
        binary = Path(self.binary).read_bytes()
        topology = self.cluster.topology_toml().encode()
        for host, address in enumerate(self.cluster.hosts):
            self.run(address, "sudo systemctl stop 'ruvio@*' ruvio-coordinator || true")
            self.run(address, "sudo rm -rf /var/lib/ruvio/g* /var/lib/ruvio/coordinator")
            self.run(address, "sudo install -m 0755 /dev/stdin /usr/local/bin/ruvio", stdin=binary)
            self._write(address, "/etc/ruvio/topology.toml", topology)
            self._write(
                address,
                "/etc/ruvio/coordinator.toml",
                self.cluster.coordinator_toml(host, "/var/lib/ruvio/coordinator").encode(),
            )
            for g in self.cluster.groups:
                self._write_env(host, g.name, None)
            self.run(address, "sudo systemctl daemon-reload")

    def _write_env(self, host: int, group: str, follow: str | None) -> None:
        env = self.cluster.node_env(host, group, f"/var/lib/ruvio/{group}", "/etc/ruvio/topology.toml", follow)
        self._write(self.cluster.hosts[host], f"/etc/ruvio/{group}.env", env_file(env).encode())

    def start(self, host: int, role: str, follow: str | None = None) -> None:
        if role != COORDINATOR:
            self._write_env(host, role, follow)
        self.run(self.cluster.hosts[host], f"sudo systemctl start {unit(role)}")

    def signal(self, host: int, role: str, sig: int) -> None:
        name = signal.Signals(sig).name.removeprefix("SIG")
        self.run(self.cluster.hosts[host], f"sudo systemctl kill -s {name} {unit(role)}")

    def running(self, host: int, role: str) -> bool:
        try:
            out = self.run(self.cluster.hosts[host], f"systemctl is-active {unit(role)} || true")
        except RuntimeError:
            return False
        return out.strip() == "active"

    def memory(self, host: int, role: str) -> dict[str, int] | None:
        fields = "|".join(PROC_FIELDS)
        try:
            out = self.run(
                self.cluster.hosts[host],
                f"pid=$(systemctl show -p MainPID --value {unit(role)}); "
                f"[ \"$pid\" != 0 ] && grep -E '^({fields}):' /proc/$pid/status || true",
            )
        except RuntimeError:
            return None
        return parse_proc_status(out) or None

    def _watchdog(self, address: str, seconds: int, command: str) -> None:
        self.fault_id += 1
        self.run(
            address,
            f"sudo systemd-run --quiet --unit ruvio-soak-heal-{os.getpid()}-{self.fault_id} "
            f"--on-active={int(seconds)} /bin/sh -c {shlex.quote(command)}",
        )

    def isolate(self, host: int, seconds: int) -> None:
        """Drops traffic between `host` and the other cluster hosts (the controller still
        reaches it). Removed by `heal`, or on the host after `seconds` at the latest."""
        address = self.cluster.hosts[host]
        peers = ", ".join(a for i, a in enumerate(self.cluster.hosts) if i != host)
        rules = (
            f"table inet {self.NFT_TABLE} {{\n"
            f"  chain input {{ type filter hook input priority -10; ip saddr {{ {peers} }} drop; }}\n"
            f"  chain output {{ type filter hook output priority -10; ip daddr {{ {peers} }} drop; }}\n"
            "}\n"
        )
        self._watchdog(address, seconds, f"nft delete table inet {self.NFT_TABLE}")
        self.run(address, "sudo nft -f /dev/stdin", stdin=rules.encode())

    def netem(self, host: int, spec: str, seconds: int) -> None:
        """`spec` is a netem argument list such as `delay 100ms 20ms loss 2%`."""
        address = self.cluster.hosts[host]
        self._watchdog(address, seconds, f"tc qdisc del dev {self.interface} root")
        self.run(address, f"sudo tc qdisc replace dev {self.interface} root netem {spec}")

    def heal(self, host: int) -> None:
        self.run(
            self.cluster.hosts[host],
            f"sudo nft delete table inet {self.NFT_TABLE} 2>/dev/null; "
            f"sudo tc qdisc del dev {self.interface} root 2>/dev/null; true",
        )

    def stop_all(self) -> None:
        for host, address in enumerate(self.cluster.hosts):
            try:
                self.heal(host)
                self.run(address, "sudo systemctl kill -s CONT 'ruvio@*' ruvio-coordinator 2>/dev/null; sudo systemctl stop 'ruvio@*' ruvio-coordinator")
            except RuntimeError:
                pass
