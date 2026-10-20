//! `coordinator.toml`: who the coordinators are, which replication groups they watch and how
//! quickly they act.

use std::{fmt, net::SocketAddr, path::PathBuf, time::Duration};

use serde::Deserialize;

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

impl<'de> Deserialize<'de> for Endpoint {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;

        Self::parse(&text).map_err(serde::de::Error::custom)
    }
}

/// One data node of a replication group.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
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
    /// Password (or `username password`) the coordinator logs in to data nodes with.
    #[serde(default)]
    auth: Option<String>,
    /// Shared by every coordinator: required on coordinator-to-coordinator calls and, with
    /// `AUTH`, from clients of the Sentinel API. Without it anyone who reaches the port can
    /// read the topology and take part in the coordinators' votes.
    #[serde(default)]
    secret: Option<String>,
    #[serde(default)]
    groups: Vec<Group>,
}

fn default_ping_interval_ms() -> u64 {
    500
}

fn default_down_after_ms() -> u64 {
    3000
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
    pub auth: Option<String>,
    pub secret: Option<String>,
    pub groups: Vec<Group>,
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
            let text = cluster_map_text(&file.groups, 1, |_| 0);

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
            auth: file.auth.filter(|auth| !auth.is_empty()),
            secret: std::env::var("RUVIO_COORDINATOR_SECRET")
                .ok()
                .or(file.secret)
                .filter(|secret| !secret.is_empty()),
            groups: file.groups,
        })
    }

    /// How many coordinators make a majority.
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
                .map_or(0, |primary| primary.member)
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
        let base = cluster_map_text(&self.groups, 1, |_| 0);
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
    primary: impl Fn(&Group) -> usize,
    owners: &[(usize, usize)],
) -> String {
    let mut text = format!("version = {version}\n");

    for (index, group) in groups.iter().enumerate() {
        let address = group
            .members
            .get(primary(group))
            .map_or_else(String::new, |member| member.address.to_string());
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
fn cluster_map_text(groups: &[Group], version: u64, primary: impl Fn(&Group) -> usize) -> String {
    let mut text = format!("version = {version}\n");

    for group in groups {
        let address = group
            .members
            .get(primary(group))
            .map_or_else(String::new, |member| member.address.to_string());
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
        assert_eq!(config.group("main").unwrap().members.len(), 2);
        assert!(config.group("other").is_none());
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
                member: 0,
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
                member: 1,
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
