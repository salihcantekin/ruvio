//! `coordinator.toml`: who the coordinators are, which replication groups they watch and how
//! quickly they act.

use std::{fmt, net::SocketAddr, path::PathBuf, time::Duration};

use serde::{Deserialize, Serialize};

/// A `host:port` the coordinator dials. The host may be a name (a container or DNS name); it
/// is resolved again on every connection, so a node that moves keeps working.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
}

impl Endpoint {
    pub fn parse(text: &str) -> Result<Self, String> {
        let (host, port) = text
            .rsplit_once(':')
            .ok_or_else(|| format!("{text:?} must be host:port"))?;
        let host = host.trim_start_matches('[').trim_end_matches(']');

        if host.is_empty() {
            return Err(format!("{text:?} has no host"));
        }

        Ok(Self {
            host: host.to_owned(),
            port: port
                .parse()
                .map_err(|_| format!("{text:?} has no valid port"))?,
        })
    }

    /// The same host, `offset` ports up: shard `i` of a data node.
    pub fn plus(&self, offset: u16) -> Self {
        Self {
            host: self.host.clone(),
            port: self.port + offset,
        }
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(formatter, "[{}]:{}", self.host, self.port)
        } else {
            write!(formatter, "{}:{}", self.host, self.port)
        }
    }
}

impl Serialize for Endpoint {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Endpoint {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;

        Self::parse(&text).map_err(serde::de::Error::custom)
    }
}

/// One data node of a replication group.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Member {
    /// Its RESP address (shard 0's port in per-shard mode), the one clients use.
    pub address: Endpoint,
    /// Its replication base address (`replication.listen`), the one `REPLICAOF` names.
    pub replication: Endpoint,
}

/// A primary and its replicas, failed over together.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Group {
    pub name: String,
    /// The first member is the primary when the coordinators start for the first time.
    pub members: Vec<Member>,
    /// Multi-node cluster: the group's shard count and the hash slots it owns
    /// (`"first-last"` or one slot; none for a group that joins empty). Either every group
    /// has `shards` or none does.
    #[serde(default)]
    pub shards: Option<usize>,
    #[serde(default)]
    pub slots: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    id: u64,
    listen: SocketAddr,
    data_dir: PathBuf,
    /// `id@host:port` of every coordinator, this one included.
    peers: Vec<String>,
    #[serde(default = "default_ping_interval_ms")]
    ping_interval_ms: u64,
    #[serde(default = "default_down_after_ms")]
    down_after_ms: u64,
    /// Promote the best replica even when none holds every record a `CLIENT DURABILITY`
    /// reply was released for; those records are lost.
    #[serde(default)]
    unclean_failover: bool,
    /// Lease each coordinator grants the agreed primary on every ping (`RUVIO LEASE`); 0, the
    /// default, grants none. Nodes with `primary-lease` on take writes only under it.
    #[serde(default)]
    primary_lease_ms: u64,
    /// How much faster or slower than real time, in percent, any coordinator's or data
    /// node's clock may run; below 100.
    #[serde(default = "default_lease_drift_percent")]
    primary_lease_drift_percent: u64,
    /// Password (or `username password`) the coordinator logs in to data nodes with.
    #[serde(default)]
    auth: Option<String>,
    /// Shared by every coordinator: required on coordinator-to-coordinator calls and, with
    /// `AUTH`, from clients of the Sentinel API. Without it anyone who reaches the port can
    /// read the topology and take part in the coordinators' votes.
    #[serde(default)]
    secret: Option<String>,
    /// TLS on the coordinator port (Raft and the Sentinel API), and on the calls to the other
    /// coordinators, which must all have it. The pair is also the client certificate shown
    /// to data nodes that ask for one.
    #[serde(default)]
    tls_cert: Option<PathBuf>,
    #[serde(default)]
    tls_key: Option<PathBuf>,
    /// The CA that vouches for the other coordinators' certificates and, with `node_tls`, the
    /// data nodes'.
    #[serde(default)]
    tls_ca: Option<PathBuf>,
    /// Dial data nodes with TLS (their `tls_cert`), checked against `tls_ca`.
    #[serde(default)]
    node_tls: bool,
    #[serde(default)]
    groups: Vec<Group>,
    /// Joining a running set of coordinators: wait to be added with `COORDINATOR ADD` instead
    /// of proposing `peers` as the first membership.
    #[serde(default)]
    join: bool,
}

fn default_ping_interval_ms() -> u64 {
    500
}

fn default_down_after_ms() -> u64 {
    3000
}

fn default_lease_drift_percent() -> u64 {
    10
}

/// How often a data node checks its lease (its maintenance tick).
const NODE_TICK: Duration = Duration::from_millis(100);

/// How long after its last grant a coordinator counts a primary's lease as ended: the lease
/// stretched by the clock drift, plus the node's check interval and as much again as margin.
/// A failover waits until a majority of coordinators passed this.
///
/// Each clock runs at most `drift_percent` (d) faster or slower than real time. A node whose
/// clock runs slow holds the lease for up to `lease / (1 - d)` of real time, and a coordinator
/// whose clock runs fast measures that as `lease * (1 + d) / (1 - d)`. `drift_percent` must be
/// below 100.
pub fn lease_safe_interval(lease: Duration, drift_percent: u64) -> Duration {
    let stretched = lease.as_millis() as u64 * (100 + drift_percent);

    Duration::from_millis(stretched.div_ceil(100 - drift_percent)) + NODE_TICK * 2
}

#[derive(Clone, Debug)]
pub struct CoordinatorConfig {
    pub id: u64,
    pub listen: SocketAddr,
    pub data_dir: PathBuf,
    pub peers: Vec<(u64, Endpoint)>,
    pub ping_interval: Duration,
    pub down_after: Duration,
    pub unclean_failover: bool,
    /// The lease granted to primaries, and how long after a grant it is counted as ended
    /// ([`lease_safe_interval`]); `None` when off.
    pub primary_lease: Option<(Duration, Duration)>,
    pub auth: Option<String>,
    pub secret: Option<String>,
    pub tls: TlsSettings,
    pub groups: Vec<Group>,
    /// Waits to be added to running coordinators instead of starting a new set.
    pub join: bool,
}

/// The coordinator's TLS files; everything off by default.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TlsSettings {
    /// The certificate and key of the coordinator port; `None` serves and calls the other
    /// coordinators in clear text.
    pub pair: Option<(PathBuf, PathBuf)>,
    pub ca: Option<PathBuf>,
    pub node_tls: bool,
}

impl TlsSettings {
    fn parse(
        cert: Option<PathBuf>,
        key: Option<PathBuf>,
        ca: Option<PathBuf>,
        node_tls: bool,
    ) -> Result<Self, String> {
        let set = |path: Option<PathBuf>| path.filter(|path| !path.as_os_str().is_empty());
        let pair = match (set(cert), set(key)) {
            (Some(cert), Some(key)) => Some((cert, key)),
            (None, None) => None,
            _ => return Err("tls_cert and tls_key must be set together".into()),
        };
        let ca = set(ca);

        if ca.is_none() && (pair.is_some() || node_tls) {
            return Err(
                "tls_cert and node_tls need tls_ca, the CA that vouches for the other coordinators and the data nodes".into(),
            );
        }
        if ca.is_some() && pair.is_none() && !node_tls {
            return Err("tls_ca needs tls_cert and tls_key, or node_tls".into());
        }

        Ok(Self { pair, ca, node_tls })
    }
}

impl CoordinatorConfig {
    pub fn parse(text: &str) -> Result<Self, String> {
        let file: FileConfig = toml::from_str(text).map_err(|error| error.to_string())?;
        let mut peers = Vec::with_capacity(file.peers.len());

        for peer in &file.peers {
            let (id, address) = peer
                .split_once('@')
                .ok_or_else(|| format!("peer {peer:?} must be id@host:port"))?;
            let id = id
                .parse()
                .map_err(|_| format!("peer {peer:?} has no numeric id"))?;
            let address = Endpoint::parse(address)?;

            if peers.iter().any(|(known, _)| *known == id) {
                return Err(format!("peer id {id} is listed twice"));
            }
            peers.push((id, address));
        }

        if !peers.iter().any(|(id, _)| *id == file.id) {
            return Err(format!(
                "peers must include this coordinator (id {})",
                file.id
            ));
        }
        if file.ping_interval_ms == 0 || file.down_after_ms < file.ping_interval_ms {
            return Err("down_after_ms must be at least ping_interval_ms, which is above 0".into());
        }
        let primary_lease = if file.primary_lease_ms == 0 {
            None
        } else {
            let lease = Duration::from_millis(file.primary_lease_ms);

            // Checked before the safe interval is worked out, which they keep from overflowing.
            if lease > crate::primary_lease::MAX_LEASE {
                return Err("primary_lease_ms must be at most 60000".into());
            }
            if file.primary_lease_drift_percent >= 100 {
                return Err("primary_lease_drift_percent must be below 100".into());
            }
            if peers.len() > crate::primary_lease::MAX_VOTERS as usize {
                return Err(format!(
                    "at most {} coordinators can grant a primary lease",
                    crate::primary_lease::MAX_VOTERS
                ));
            }
            // A node closes its gate `GATE_MARGIN` before the lease ends, so a renewal every
            // ping must leave more than that, with room for the round trip.
            let floor =
                file.ping_interval_ms
                    .saturating_mul(3)
                    .max(file.ping_interval_ms.saturating_add(
                        crate::primary_lease::GATE_MARGIN.as_millis() as u64 + 200,
                    ));

            if file.primary_lease_ms < floor {
                return Err(format!(
                    "primary_lease_ms must be at least {floor}: three times ping_interval_ms, and ping_interval_ms plus 500"
                ));
            }
            let safe = lease_safe_interval(lease, file.primary_lease_drift_percent);

            if safe.as_millis() as u64 > file.down_after_ms {
                return Err(format!(
                    "down_after_ms must be at least {} with this primary_lease_ms: the lease plus {}% drift and 200 ms",
                    safe.as_millis(),
                    file.primary_lease_drift_percent
                ));
            }
            Some((lease, safe))
        };
        for group in &file.groups {
            if group.members.len() < 2 {
                return Err(format!(
                    "group {:?} needs a primary and a replica",
                    group.name
                ));
            }
            if file
                .groups
                .iter()
                .filter(|other| other.name == group.name)
                .count()
                > 1
            {
                return Err(format!("group {:?} is listed twice", group.name));
            }
        }

        let clustered = file
            .groups
            .iter()
            .filter(|group| group.shards.is_some())
            .count();

        if file
            .groups
            .iter()
            .any(|group| group.shards.is_none() && !group.slots.is_empty())
        {
            return Err("a group with slots needs shards".into());
        }
        if clustered != 0 {
            if clustered != file.groups.len() {
                return Err("with shards, every group needs shards".into());
            }
            // The map the nodes will receive, checked as they check it.
            let text = cluster_map_text(&file.groups, 1, first_member);

            crate::topology::Topology::parse(
                &text,
                &file.groups[0].name,
                file.groups[0].shards.unwrap_or(1),
            )?;
        }

        Ok(Self {
            id: file.id,
            listen: file.listen,
            data_dir: file.data_dir,
            peers,
            ping_interval: Duration::from_millis(file.ping_interval_ms),
            down_after: Duration::from_millis(file.down_after_ms),
            unclean_failover: file.unclean_failover,
            primary_lease,
            auth: file.auth.filter(|auth| !auth.is_empty()),
            secret: std::env::var("RUVIO_COORDINATOR_SECRET")
                .ok()
                .or(file.secret)
                .filter(|secret| !secret.is_empty()),
            tls: TlsSettings::parse(file.tls_cert, file.tls_key, file.tls_ca, file.node_tls)?,
            groups: file.groups,
            join: file.join,
        })
    }

    /// How many of the coordinators in `peers` make a majority; once running, the Raft
    /// membership counts instead.
    pub fn quorum(&self) -> usize {
        self.peers.len() / 2 + 1
    }

    pub fn group(&self, name: &str) -> Option<&Group> {
        self.groups.iter().find(|group| group.name == name)
    }

    /// The cluster map the data nodes get with `RUVIO TOPOLOGY SET`, and its version: each
    /// group named by its agreed primary, with the ranges `moved` between groups. The
    /// version is 1 plus the sum of the groups' epochs plus the moves, so every failover and
    /// every move raises it. `None` without shards, or before every group has an agreed
    /// primary.
    pub fn cluster_map(
        &self,
        primaries: &super::raft::Primaries,
        moved: &[super::raft::MovedRange],
    ) -> Option<(u64, String)> {
        self.groups.first()?.shards?;
        let mut version = 1 + moved.len() as u64;

        for group in &self.groups {
            version += primaries.get(&group.name)?.epoch;
        }
        let primary = |group: &Group| {
            primaries
                .get(&group.name)
                .map_or_else(String::new, |primary| primary.member.clone())
        };
        let text = if moved.is_empty() {
            cluster_map_text(&self.groups, version, primary)
        } else {
            placed_map_text(&self.groups, version, primary, &self.owners(moved))
        };

        Some((version, text))
    }

    /// Every slot's group (an index into `groups`) and shard after the moves `moved`. The
    /// configured slots are split over each group's shards as its nodes split them.
    pub fn owners(&self, moved: &[super::raft::MovedRange]) -> Vec<(usize, usize)> {
        let base = cluster_map_text(&self.groups, 1, first_member);
        let first = &self.groups[0];
        let mut owners = vec![(0, 0); crate::routing::HASH_SLOT_COUNT];

        // Checked when the configuration was read: the groups cover every slot once.
        if let Ok(topology) =
            crate::topology::Topology::parse(&base, &first.name, first.shards.unwrap_or(1))
        {
            for (index, group) in topology.groups.iter().enumerate() {
                for &(first, last) in &group.ranges {
                    for slot in first..=last {
                        owners[usize::from(slot)] = (index, group.shard_of(slot).unwrap_or(0));
                    }
                }
            }
        }
        for range in moved {
            if let Some(index) = self
                .groups
                .iter()
                .position(|group| group.name == range.group)
            {
                for slot in range.first..=range.last {
                    owners[usize::from(slot)] = (index, range.shard);
                }
            }
        }

        owners
    }
}

/// The map once ranges moved between groups: every range names its shard (`"a-b@s"`), so
/// a node keeps each slot on the shard that holds its data whatever else moved.
fn placed_map_text(
    groups: &[Group],
    version: u64,
    primary: impl Fn(&Group) -> String,
    owners: &[(usize, usize)],
) -> String {
    let mut text = format!("version = {version}\n");

    for (index, group) in groups.iter().enumerate() {
        let address = primary(group);
        let mut runs: Vec<(usize, usize, usize)> = Vec::new();

        for (slot, &(owner, shard)) in owners.iter().enumerate() {
            if owner != index {
                continue;
            }
            match runs.last_mut() {
                Some((_, last, known)) if *last + 1 == slot && *known == shard => *last = slot,
                _ => runs.push((slot, slot, shard)),
            }
        }
        let slots: Vec<String> = runs
            .iter()
            .map(|(first, last, shard)| format!("\"{first}-{last}@{shard}\""))
            .collect();

        text.push_str(&format!(
            "\n[[groups]]\nname = {:?}\naddress = {:?}\nshards = {}\nslots = [{}]\n",
            group.name,
            address,
            group.shards.unwrap_or(1),
            slots.join(", ")
        ));
    }

    text
}

/// The map in the format of a node's `cluster_topology` file.
/// The first member's client address: what a map names before any primary is agreed.
fn first_member(group: &Group) -> String {
    group
        .members
        .first()
        .map_or_else(String::new, |member| member.address.to_string())
}

fn cluster_map_text(groups: &[Group], version: u64, primary: impl Fn(&Group) -> String) -> String {
    let mut text = format!("version = {version}\n");

    for group in groups {
        let address = primary(group);
        let slots: Vec<String> = group
            .slots
            .iter()
            .map(|range| format!("{range:?}"))
            .collect();

        text.push_str(&format!(
            "\n[[groups]]\nname = {:?}\naddress = {:?}\nshards = {}\nslots = [{}]\n",
            group.name,
            address,
            group.shards.unwrap_or(1),
            slots.join(", ")
        ));
    }

    text
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
id = 2
listen = "127.0.0.1:26401"
data_dir = "/tmp/c2"
peers = ["1@127.0.0.1:26400", "2@127.0.0.1:26401", "3@127.0.0.1:26402"]
down_after_ms = 1000

[[groups]]
name = "main"
members = [
  { address = "127.0.0.1:6379", replication = "127.0.0.1:26379" },
  { address = "127.0.0.1:6380", replication = "127.0.0.1:26380" },
]
"#;

    #[test]
    fn parses_peers_groups_and_defaults() {
        let config = CoordinatorConfig::parse(SAMPLE).unwrap();

        assert_eq!(config.id, 2);
        assert_eq!(config.peers.len(), 3);
        assert_eq!(config.quorum(), 2);
        assert_eq!(config.ping_interval, Duration::from_millis(500));
        assert_eq!(config.down_after, Duration::from_millis(1000));
        assert!(!config.unclean_failover);
        assert_eq!(config.primary_lease, None);
        assert_eq!(config.group("main").unwrap().members.len(), 2);
        assert!(config.group("other").is_none());
    }

    #[test]
    fn tls_settings_come_in_complete_sets() {
        let with = |extra: &str| {
            CoordinatorConfig::parse(&SAMPLE.replacen(
                "down_after_ms = 1000",
                &format!("down_after_ms = 1000\n{extra}"),
                1,
            ))
        };

        assert_eq!(with("").unwrap().tls, TlsSettings::default());
        let full =
            with("tls_cert = \"c.pem\"\ntls_key = \"k.pem\"\ntls_ca = \"ca.pem\"\nnode_tls = true")
                .unwrap()
                .tls;

        assert_eq!(full.pair, Some(("c.pem".into(), "k.pem".into())));
        assert_eq!(full.ca, Some("ca.pem".into()));
        assert!(full.node_tls);
        let nodes_only = with("tls_ca = \"ca.pem\"\nnode_tls = true").unwrap().tls;

        assert_eq!(nodes_only.pair, None);
        assert!(with("tls_cert = \"c.pem\"\ntls_ca = \"ca.pem\"").is_err());
        assert!(with("tls_cert = \"c.pem\"\ntls_key = \"k.pem\"").is_err());
        assert!(with("node_tls = true").is_err());
        assert!(with("tls_ca = \"ca.pem\"").is_err());
    }

    #[test]
    fn the_binary_install_example_parses() {
        let config =
            CoordinatorConfig::parse(include_str!("../../deploy/coordinator.toml")).unwrap();

        assert_eq!(
            (config.id, config.peers.len(), config.groups.len()),
            (1, 3, 1)
        );
    }

    #[test]
    fn the_compose_example_configs_parse() {
        for (id, text) in [
            (
                1,
                include_str!("../../deploy/coordinator/coordinator-1.toml"),
            ),
            (
                2,
                include_str!("../../deploy/coordinator/coordinator-2.toml"),
            ),
            (
                3,
                include_str!("../../deploy/coordinator/coordinator-3.toml"),
            ),
        ] {
            let config = CoordinatorConfig::parse(text).unwrap();

            assert_eq!(config.id, id);
            assert_eq!(config.groups[0].members[0].address.host, "ruvio-1");
        }
    }

    #[test]
    fn endpoints_take_names_and_addresses() {
        let named = Endpoint::parse("ruvio-1:6379").unwrap();

        assert_eq!((named.host.as_str(), named.port), ("ruvio-1", 6379));
        assert_eq!(named.plus(2).to_string(), "ruvio-1:6381");
        assert_eq!(
            Endpoint::parse("[::1]:6379").unwrap().to_string(),
            "[::1]:6379"
        );
        assert!(Endpoint::parse("6379").is_err());
        assert!(Endpoint::parse(":6379").is_err());
    }

    #[test]
    fn a_cluster_map_names_each_group_by_its_agreed_primary() {
        let text = format!(
            "{SAMPLE}shards = 2\nslots = [\"0-8191\"]\n\n[[groups]]\nname = \"b\"\nshards = 1\nslots = [\"8192-16383\"]\nmembers = [\n  {{ address = \"10.0.0.2:7000\", replication = \"10.0.0.2:27000\" }},\n  {{ address = \"10.0.0.3:7000\", replication = \"10.0.0.3:27000\" }},\n]\n"
        );
        let config = CoordinatorConfig::parse(&text).unwrap();
        let mut primaries = super::super::raft::Primaries::new();

        primaries.insert(
            "main".into(),
            super::super::raft::Primary {
                member: "127.0.0.1:6379".into(),
                epoch: 1,
            },
        );
        assert!(
            config.cluster_map(&primaries, &[]).is_none(),
            "b has no primary yet"
        );
        primaries.insert(
            "b".into(),
            super::super::raft::Primary {
                member: "10.0.0.3:7000".into(),
                epoch: 3,
            },
        );

        let (version, map) = config.cluster_map(&primaries, &[]).unwrap();
        let topology = crate::topology::Topology::parse(&map, "b", 1).unwrap();

        assert_eq!((version, topology.version), (5, 5));
        assert_eq!(topology.groups[1].host, "10.0.0.3");
        assert_eq!(topology.groups[0].port, 6379);

        // A moved range raises the version and names its shard; the rest of each group
        // stays on the shards it had.
        let moved = [super::super::raft::MovedRange {
            first: 100,
            last: 199,
            group: "b".into(),
            shard: 0,
        }];
        let (version, map) = config.cluster_map(&primaries, &moved).unwrap();
        let before = crate::topology::Topology::parse(
            &config.cluster_map(&primaries, &[]).unwrap().1,
            "main",
            2,
        )
        .unwrap();
        let after = crate::topology::Topology::parse(&map, "main", 2).unwrap();

        assert_eq!(version, 6);
        assert!(map.contains("\"100-199@0\""), "{map}");
        assert!(
            map.contains("\"0-99@0\", \"200-4095@0\", \"4096-8191@1\""),
            "{map}"
        );
        for slot in (0..100).chain(200..16384) {
            assert_eq!(
                before.groups[0].shard_of(slot),
                after.groups[0].shard_of(slot),
                "slot {slot}"
            );
        }
        assert_eq!(after.groups[1].shard_of(150), Some(0));
        assert!(
            CoordinatorConfig::parse(&text.replace("shards = 1\n", ""))
                .unwrap_err()
                .contains("needs shards")
        );
        // A group may join with no slots, as long as the others cover every slot.
        let empty = text
            .replace("[\"0-8191\"]", "[\"0-16383\"]")
            .replace("[\"8192-16383\"]", "[]");

        assert!(CoordinatorConfig::parse(&empty).is_ok());
        assert!(
            CoordinatorConfig::parse(&text.replace("slots = [\"8192-16383\"]\n", ""))
                .unwrap_err()
                .contains("slot 8192")
        );
        assert!(
            CoordinatorConfig::parse(&text.replace("8192-16383", "8192-16000"))
                .unwrap_err()
                .contains("slot 16001")
        );
    }

    #[test]
    fn a_primary_lease_must_end_before_a_failover_starts() {
        // 2000 ms stretched by 110/90 is 2444.4, rounded up, plus two node ticks.
        assert_eq!(
            lease_safe_interval(Duration::from_millis(2000), 10),
            Duration::from_millis(2645)
        );
        assert_eq!(
            lease_safe_interval(Duration::from_millis(1000), 0),
            Duration::from_millis(1200)
        );
        // The largest drift and lease do not overflow.
        assert_eq!(
            lease_safe_interval(Duration::from_millis(60_000), 99),
            Duration::from_millis(11_940_200)
        );
        let lease = |text: &str| {
            CoordinatorConfig::parse(&SAMPLE.replace("down_after_ms = 1000", text))
                .map(|config| config.primary_lease)
        };

        assert_eq!(
            lease("down_after_ms = 3000\nprimary_lease_ms = 2000").unwrap(),
            Some((Duration::from_millis(2000), Duration::from_millis(2645)))
        );
        for (text, reason) in [
            (
                "down_after_ms = 2600\nprimary_lease_ms = 2000",
                "at least 2645",
            ),
            (
                "down_after_ms = 3000\nprimary_lease_ms = 1000",
                "at least 1500",
            ),
            (
                "down_after_ms = 3000\nprimary_lease_ms = 2000\nprimary_lease_drift_percent = 100",
                "below 100",
            ),
            (
                "down_after_ms = 90000\nprimary_lease_ms = 61000",
                "at most 60000",
            ),
            // Out-of-range values are refused, never overflowed.
            (
                "down_after_ms = 3000\nprimary_lease_ms = 9223372036854775807",
                "at most 60000",
            ),
            (
                "down_after_ms = 3000\nprimary_lease_ms = 2000\nprimary_lease_drift_percent = 9223372036854775807",
                "below 100",
            ),
        ] {
            let error = lease(text).unwrap_err();

            assert!(error.contains(reason), "{text}: {error}");
        }
    }

    #[test]
    fn a_primary_lease_takes_at_most_the_largest_coordinator_count_a_grant_may_name() {
        let peers = |count: usize| {
            let list: Vec<String> = (1..=count)
                .map(|id| format!("\"{id}@127.0.0.1:{}\"", 20_000 + id))
                .collect();

            SAMPLE
                .replace(
                    "\"1@127.0.0.1:26400\", \"2@127.0.0.1:26401\", \"3@127.0.0.1:26402\"",
                    &list.join(", "),
                )
                .replace(
                    "down_after_ms = 1000",
                    "down_after_ms = 3000\nprimary_lease_ms = 2000",
                )
        };
        let most = crate::primary_lease::MAX_VOTERS as usize;

        assert!(CoordinatorConfig::parse(&peers(most)).is_ok());
        assert!(
            CoordinatorConfig::parse(&peers(most + 1))
                .unwrap_err()
                .contains("at most 255 coordinators")
        );
        // Without the lease the count is not limited here.
        assert!(
            CoordinatorConfig::parse(&peers(most + 1).replace("primary_lease_ms = 2000", ""))
                .is_ok()
        );
    }

    #[test]
    fn refuses_inconsistent_settings() {
        for (from, to, reason) in [
            ("id = 2", "id = 9", "include this coordinator"),
            (
                "down_after_ms = 1000",
                "down_after_ms = 100",
                "at least ping_interval_ms",
            ),
            (
                "\"1@127.0.0.1:26400\"",
                "\"x@127.0.0.1:26400\"",
                "numeric id",
            ),
            (
                "\"3@127.0.0.1:26402\"",
                "\"2@127.0.0.1:26402\"",
                "listed twice",
            ),
        ] {
            let error = CoordinatorConfig::parse(&SAMPLE.replace(from, to)).unwrap_err();

            assert!(error.contains(reason), "{error}");
        }
    }
}
