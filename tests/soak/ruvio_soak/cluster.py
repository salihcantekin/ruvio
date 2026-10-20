"""The cluster under test: hosts, two replica groups and three coordinators, and the files each
process reads. One description renders the configs for a local run (every host on 127.0.0.1,
ports apart) and for an Azure run (one VM per host, the same ports on each).

Each host runs one coordinator and one member of every group. Group `g1` starts with its primary
on the first host, `g2` on the second, so one host failure never takes both primaries."""

from __future__ import annotations

import json
import secrets
from dataclasses import dataclass, field
from pathlib import Path

COORDINATOR = "coordinator"


@dataclass(frozen=True)
class Group:
    name: str
    port: int  # client port of shard 0 on every host (plus the host offset when local)
    replication_port: int
    slots: str
    durability: str  # a WAL-backed profile: everysec or always
    first_primary: int  # index of the host whose member starts as primary
    shards: int = 1


@dataclass
class Cluster:
    hosts: list[str]  # addresses the other hosts and the clients use
    local: bool = False
    coordinator_port: int = 26400
    groups: list[Group] = field(
        default_factory=lambda: [
            Group("g1", 7000, 27000, "0-8191", "always", 0),
            Group("g2", 7100, 27100, "8192-16383", "everysec", 1),
        ]
    )
    ping_interval_ms: int = 500
    down_after_ms: int = 3000
    primary_lease_ms: int = 0
    password: str = ""
    secret: str = ""

    @classmethod
    def from_json(cls, data: dict) -> "Cluster":
        groups = [Group(**g) for g in data.pop("groups")] if "groups" in data else None
        cluster = cls(**data)
        if groups:
            cluster.groups = groups
        return cluster

    def with_new_secrets(self) -> "Cluster":
        """Fresh AUTH password and coordinator secret; they live only in the run directory."""
        self.password = secrets.token_hex(24)
        self.secret = secrets.token_hex(24)
        return self

    def public(self) -> dict:
        """The description without secrets, for logs and status files."""
        return {
            "hosts": self.hosts,
            "local": self.local,
            "coordinator_port": self.coordinator_port,
            "groups": [g.__dict__ for g in self.groups],
            "ping_interval_ms": self.ping_interval_ms,
            "down_after_ms": self.down_after_ms,
            "primary_lease_ms": self.primary_lease_ms,
        }

    # Locally every host shares 127.0.0.1, so host i's ports are shifted by 10 * i. A group
    # with more than 10 shards would overlap the next host.
    def _offset(self, host: int) -> int:
        return 10 * host if self.local else 0

    def group(self, name: str) -> Group:
        return next(g for g in self.groups if g.name == name)

    def roles(self) -> list[str]:
        return [COORDINATOR] + [g.name for g in self.groups]

    def processes(self) -> list[tuple[int, str]]:
        return [(host, role) for host in range(len(self.hosts)) for role in self.roles()]

    def address(self, host: int, role: str) -> str:
        if role == COORDINATOR:
            return f"{self.hosts[host]}:{self.coordinator_port + self._offset(host)}"
        return f"{self.hosts[host]}:{self.group(role).port + self._offset(host)}"

    def replication_address(self, host: int, group: str) -> str:
        return f"{self.hosts[host]}:{self.group(group).replication_port + self._offset(host)}"

    def coordinators(self) -> list[str]:
        return [self.address(host, COORDINATOR) for host in range(len(self.hosts))]

    def host_of(self, role: str, address: str) -> int | None:
        for host in range(len(self.hosts)):
            if self.address(host, role) == address:
                return host
        return None

    def seeds(self) -> list[str]:
        return [self.address(host, g.name) for g in self.groups for host in range(len(self.hosts))]

    def topology_toml(self) -> str:
        """The `cluster_topology` file every data node starts from (version 1, each group named
        by the address of its first primary)."""
        text = "version = 1\n"
        for g in self.groups:
            text += (
                f'\n[[groups]]\nname = "{g.name}"\naddress = "{self.address(g.first_primary, g.name)}"\n'
                f'shards = {g.shards}\nslots = ["{g.slots}"]\n'
            )
        return text

    def coordinator_toml(self, host: int, data_dir: str) -> str:
        peers = ", ".join(
            f'"{index + 1}@{self.address(index, COORDINATOR)}"' for index in range(len(self.hosts))
        )
        text = (
            f"id = {host + 1}\n"
            f'listen = "{self.address(host, COORDINATOR)}"\n'
            f'data_dir = "{data_dir}"\n'
            f"peers = [{peers}]\n"
            f'secret = "{self.secret}"\n'
            f"ping_interval_ms = {self.ping_interval_ms}\n"
            f"down_after_ms = {self.down_after_ms}\n"
            f"primary_lease_ms = {self.primary_lease_ms}\n"
            f'auth = "{self.password}"\n'
        )
        for g in self.groups:
            members = ", ".join(
                f'{{ address = "{self.address(h, g.name)}", replication = "{self.replication_address(h, g.name)}" }}'
                for h in range(len(self.hosts))
            )
            text += f'\n[[groups]]\nname = "{g.name}"\nshards = {g.shards}\nslots = ["{g.slots}"]\nmembers = [{members}]\n'
        return text

    def node_env(
        self, host: int, group: str, data_dir: str, topology_path: str, follow: str | None = None
    ) -> dict[str, str]:
        """Environment of one data node. The role is the starting role only; after a failover
        the coordinators repoint the members. `follow` (a replication address) starts the node
        as a replica of that member instead."""
        g = self.group(group)
        env = {
            "RUVIO_ADDR": self.address(host, group),
            "RUVIO_SHARDS": str(g.shards),
            "RUVIO_DURABILITY": g.durability,
            "RUVIO_DATA_DIR": data_dir,
            "RUVIO_REQUIREPASS": self.password,
            "RUVIO_REPLICATION_LISTEN": self.replication_address(host, group),
            "RUVIO_CLUSTER_TOPOLOGY": topology_path,
            "RUVIO_CLUSTER_GROUP": group,
        }
        if follow is None and host == g.first_primary:
            env["RUVIO_REPLICATION_ROLE"] = "primary"
        else:
            env["RUVIO_REPLICATION_ROLE"] = "replica"
            env["RUVIO_REPLICATION_PRIMARY"] = follow or self.replication_address(g.first_primary, group)
        return env


def env_file(env: dict[str, str]) -> str:
    """A systemd `EnvironmentFile`."""
    return "".join(f"{key}={value}\n" for key, value in sorted(env.items()))


def save(cluster: Cluster, path: Path) -> None:
    """Writes the cluster with its secrets, readable by the owner only."""
    data = cluster.public()
    data["password"], data["secret"] = cluster.password, cluster.secret
    path.write_text(json.dumps(data, indent=2))
    path.chmod(0o600)


def load(path: Path) -> Cluster:
    return Cluster.from_json(json.loads(path.read_text()))
