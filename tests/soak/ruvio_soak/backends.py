"""Where the processes run and how faults reach them.

`LocalBackend` starts every process on this machine; it is how the tool itself is tested.
`SshBackend` drives systemd units on the VMs that cloud-init prepared (`ruvio@g1`, `ruvio@g2`,
`ruvio-coordinator`) and injects network faults with nftables and netem. Each network fault is
also scheduled for removal on the host itself (`systemd-run --on-active`), so a fault never
outlives a controller that died."""

from __future__ import annotations

import os
import shlex
import shutil
import signal
import subprocess
import time
from pathlib import Path

from .cluster import COORDINATOR, Cluster, env_file

NETWORK_FAULTS = ("isolate-host", "netem", "partition-coordinator-leader")

# One shell snippet per process: its /proc status lines, CPU ticks, open descriptors and the size
# of its data directory, so a sample costs one SSH round trip per host.
HOST_SCRIPT = r"""
for u in {units}; do
  pid=$(systemctl show -p MainPID --value "$u" 2>/dev/null); dir="/var/lib/ruvio/${{u#ruvio@}}"
  [ "$u" = ruvio-coordinator ] && dir=/var/lib/ruvio/coordinator
  echo "== $u"
  if [ -n "$pid" ] && [ "$pid" != 0 ]; then
    grep -E '^(VmRSS|VmHWM|RssAnon|RssFile|RssShmem|VmData|VmSwap|Threads|voluntary_ctxt_switches|nonvoluntary_ctxt_switches):' /proc/$pid/status
    echo "CpuTicks: $(awk '{{print $14+$15}}' /proc/$pid/stat)"
    echo "Fds: $(sudo ls /proc/$pid/fd 2>/dev/null | wc -l)"
  fi
  echo "DataBytes: $(sudo du -sb "$dir" 2>/dev/null | cut -f1)"
  echo "WalBytes: $(sudo find "$dir" -path '*/wal/*' -type f -printf '%s\n' 2>/dev/null | awk '{{s+=$1}} END {{print s+0}}')"
  echo "CheckpointBytes: $(sudo find "$dir" -path '*/checkpoints/*' -type f -printf '%s\n' 2>/dev/null | awk '{{s+=$1}} END {{print s+0}}')"
done
echo "== host"
echo "Load: $(cut -d' ' -f1 /proc/loadavg)"
awk '/^MemAvailable:/ {{print "MemAvailableKib: " $2}}' /proc/meminfo
df -B1 --output=avail,size /var/lib/ruvio | tail -1 | awk '{{print "DiskAvail: " $1; print "DiskSize: " $2}}'
awk '/eth0:/ {{print "NetRx: " $2; print "NetTx: " $10}}' /proc/net/dev
echo "ClockOffsetS: $(chronyc -c tracking 2>/dev/null | cut -d, -f5)"
echo "ClkTck: $(getconf CLK_TCK)"
awk '/^cpu / {{print "CpuIowaitTicks: " $6; print "CpuStealTicks: " $9}}' /proc/stat
awk '/^some/ {{split($5, a, "="); print "IoSomeUs: " a[2]}} /^full/ {{split($5, a, "="); print "IoFullUs: " a[2]}}' /proc/pressure/io 2>/dev/null
awk '/^Dirty:/ {{print "DirtyKib: " $2}} /^Writeback:/ {{print "WritebackKib: " $2}}' /proc/meminfo
src=$(findmnt -no SOURCE /var/lib/ruvio 2>/dev/null)
dev=$(lsblk -no PKNAME "$src" 2>/dev/null | head -1); [ -z "$dev" ] && [ -n "$src" ] && dev=$(basename "$(readlink -f "$src")")
[ -n "$dev" ] && awk -v d="$dev" '$3 == d {{print "DiskReads: " $4; print "DiskWrites: " $8; print "DiskWriteSectors: " $10;
  print "DiskInFlight: " $12; print "DiskBusyMs: " $13; print "DiskQueueMs: " $14; print "DiskFlushes: " $19;
  print "DiskFlushMs: " $20}}' /proc/diskstats
"""

STAT_NAMES = {"CpuTicks": "cpu_ticks", "Fds": "fds", "DataBytes": "data_dir_bytes", "WalBytes": "wal_bytes", "CheckpointBytes": "checkpoint_bytes",
              "voluntary_ctxt_switches": "ctx_voluntary", "nonvoluntary_ctxt_switches": "ctx_involuntary"}
HOST_NAMES = {"Load": "load1", "MemAvailableKib": "mem_available_kib", "DiskAvail": "disk_avail_bytes",
              "DiskSize": "disk_size_bytes", "NetRx": "net_rx_bytes", "NetTx": "net_tx_bytes",
              "ClockOffsetS": "clock_offset_s", "ClkTck": "clk_tck",
              # Cumulative counters: a stall shorter than the sample interval still shows as a
              # jump in the time the disk was busy or tasks waited for it.
              "CpuIowaitTicks": "cpu_iowait_ticks", "CpuStealTicks": "cpu_steal_ticks",
              "IoSomeUs": "io_some_us", "IoFullUs": "io_full_us", "DirtyKib": "dirty_kib",
              "WritebackKib": "writeback_kib", "DiskReads": "disk_reads", "DiskWrites": "disk_writes",
              "DiskWriteSectors": "disk_write_sectors", "DiskInFlight": "disk_in_flight",
              "DiskBusyMs": "disk_busy_ms", "DiskQueueMs": "disk_queue_ms", "DiskFlushes": "disk_flushes",
              "DiskFlushMs": "disk_flush_ms"}


def parse_host_stats(text: str, roles: dict[str, str]) -> tuple[dict[str, dict], dict]:
    """Splits `HOST_SCRIPT` output into {role: stats} and the host's own stats. `roles` maps a
    unit name to its role."""
    processes: dict[str, dict] = {}
    status_lines: dict[str, list[str]] = {}
    host: dict = {}
    current, role = None, None
    for line in text.splitlines():
        if line.startswith("== "):
            name = line[3:].strip()
            if name == "host":
                current, role = host, None
            else:
                role = roles.get(name, name)
                current = processes.setdefault(role, {})
                status_lines.setdefault(role, [])
            continue
        if current is None:
            continue
        field, _, rest = line.partition(":")
        value = rest.strip().split()[0] if rest.strip() else ""
        names = HOST_NAMES if current is host else STAT_NAMES
        if field in names and value:
            try:
                current[names[field]] = float(value) if "." in value else int(value)
            except ValueError:
                pass
        elif role is not None:
            status_lines[role].append(line)
    for role, lines in status_lines.items():
        processes[role].update(parse_proc_status("\n".join(lines)))
    return processes, host


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



def _tree_size(directory: Path, keep=lambda path: True) -> int:
    total = 0
    if directory.exists():
        for path in directory.rglob("*"):
            try:
                if path.is_file() and keep(path):
                    total += path.stat().st_size
            except OSError:
                pass
    return total


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

    def _command(self, host: int, role: str, follow: str | None, as_primary: bool = False):
        directory = self.work / f"host{host}"
        if role == COORDINATOR:
            return [self.binary, "coordinator", "--config", str(directory / "coordinator.toml")], None
        env = self.cluster.node_env(host, role, str(directory / role), str(self.work / "topology.toml"), follow, as_primary)
        return [self.binary], dict(os.environ, **env)

    def start(self, host: int, role: str, follow: str | None = None, as_primary: bool = False) -> None:
        """`follow`: start a data node as a replica of this replication address instead of in
        its configured role; `as_primary`: start it as the primary."""
        if self.running(host, role):
            return
        command, env = self._command(host, role, follow, as_primary)
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

    def stats(self, host: int) -> tuple[dict[str, dict], dict]:
        """CPU ticks, descriptors and data size of each process of `host`, and the machine's
        own load, memory and disk (shared by every local host)."""
        processes = {}
        for role in self.cluster.roles():
            process = self.processes.get((host, role))
            directory = self.work / f"host{host}" / role
            stats: dict = {"data_dir_bytes": _tree_size(directory),
                           "wal_bytes": _tree_size(directory, lambda p: "wal" in p.parts),
                           "checkpoint_bytes": _tree_size(directory, lambda p: "checkpoints" in p.parts)}
            if process is not None and process.poll() is None:
                try:
                    fields = Path(f"/proc/{process.pid}/stat").read_text().rsplit(")", 1)[1].split()
                    stats["cpu_ticks"] = int(fields[11]) + int(fields[12])
                    stats["fds"] = len(os.listdir(f"/proc/{process.pid}/fd"))
                except (OSError, ValueError, IndexError):
                    pass
            processes[role] = stats
        machine: dict = {"clk_tck": os.sysconf("SC_CLK_TCK")}
        try:
            machine["load1"] = os.getloadavg()[0]
            usage = shutil.disk_usage(self.work)
            machine["disk_avail_bytes"], machine["disk_size_bytes"] = usage.free, usage.total
            for line in Path("/proc/meminfo").read_text().splitlines():
                if line.startswith("MemAvailable:"):
                    machine["mem_available_kib"] = int(line.split()[1])
        except OSError:
            pass
        return processes, machine

    def logs(self, host: int, role: str, since: float) -> str:
        """The process's output since `since` (epoch seconds): locally the whole log file is
        read again and the caller keeps the offset, so `since` is ignored."""
        path = self.work / f"host{host}-{role}.log"
        try:
            return path.read_text(errors="replace")
        except OSError:
            return ""

    def isolate(self, host: int, seconds: int) -> None:
        raise Unsupported("isolate-host needs the ssh backend")

    def partition_coordinator(self, host: int, seconds: int) -> None:
        raise Unsupported("partition-coordinator-leader needs the ssh backend")

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

    def _write_env(self, host: int, group: str, follow: str | None, as_primary: bool = False) -> None:
        env = self.cluster.node_env(host, group, f"/var/lib/ruvio/{group}", "/etc/ruvio/topology.toml", follow, as_primary)
        self._write(self.cluster.hosts[host], f"/etc/ruvio/{group}.env", env_file(env).encode())

    def start(self, host: int, role: str, follow: str | None = None, as_primary: bool = False) -> None:
        if role != COORDINATOR:
            self._write_env(host, role, follow, as_primary)
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

    def stats(self, host: int) -> tuple[dict[str, dict], dict]:
        units = {unit(role): role for role in self.cluster.roles()}
        try:
            out = self.run(self.cluster.hosts[host], HOST_SCRIPT.format(units=" ".join(units)))
        except RuntimeError:
            return {}, {}
        return parse_host_stats(out, units)

    def logs(self, host: int, role: str, since: float) -> str:
        stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(since))
        try:
            return self.run(
                self.cluster.hosts[host],
                f"sudo journalctl -q -o short-iso-precise --utc -u {unit(role)} --since {shlex.quote(stamp + ' UTC')} --no-pager",
            )
        except RuntimeError as error:
            return f"(journalctl failed: {error})\n"

    def partition_coordinator(self, host: int, seconds: int) -> None:
        """Cuts the coordinator of `host` from the other coordinators only (the Raft port); the
        data nodes and the controller still reach it, and it still reaches the data nodes."""
        address = self.cluster.hosts[host]
        peers = ", ".join(a for i, a in enumerate(self.cluster.hosts) if i != host)
        port = self.cluster.coordinator_port
        rules = (
            f"table inet {self.NFT_TABLE} {{\n"
            f"  chain input {{ type filter hook input priority -10; ip saddr {{ {peers} }} tcp dport {port} drop; "
            f"ip saddr {{ {peers} }} tcp sport {port} drop; }}\n"
            f"  chain output {{ type filter hook output priority -10; ip daddr {{ {peers} }} tcp dport {port} drop; "
            f"ip daddr {{ {peers} }} tcp sport {port} drop; }}\n"
            "}\n"
        )
        self._watchdog(address, seconds, f"nft delete table inet {self.NFT_TABLE}")
        self.run(address, "sudo nft -f /dev/stdin", stdin=rules.encode())

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
