//! A cluster of several nodes: which group of nodes owns each hash slot.
//!
//! A group is one primary and its replicas (they have the same shard count). The topology
//! names every group and the slot ranges it owns; this node serves the slots of its own
//! group and answers `MOVED` for the others. Without a topology nothing here is read.

use std::{
    fmt,
    path::Path,
    sync::{Arc, RwLock},
};

use serde::Deserialize;

use crate::routing::{HASH_SLOT_COUNT, SlotTable};

/// The newest map a node received (`RUVIO TOPOLOGY SET`), in its data directory. At
/// start the newer of this file and `cluster_topology` is used.
pub const TOPOLOGY_FILE: &str = "TOPOLOGY";

/// One group of the cluster, as clients reach its primary.
#[derive(Clone, Debug)]
pub struct TopologyGroup {
    pub name: String,
    /// Client host of the group's primary.
    pub host: String,
    /// Client port of the primary's shard 0; shard `i` listens on this port plus `i`.
    pub port: u16,
    pub shards: usize,
    /// Slot ranges the group owns, as `(first, last)`, in slot order.
    pub ranges: Vec<(u16, u16)>,
    /// For each of `ranges`, the shard that serves it when the map names one
    /// (`"first-last@shard"`, written by the coordinators once a range has moved between
    /// groups); the others are split over the shards as [`TopologyGroup::shard_of`] says.
    pub placed_on: Vec<Option<u16>>,
    /// Its index in the map.
    index: u16,
    /// For every slot of the map, the owning group's index and the shard of that group that
    /// serves it (see [`TopologyGroup::shard_of`]): one table per map, shared by its groups.
    placed: std::sync::Arc<[(u16, u16)]>,
}

/// Equal when the group is described the same way; its placement follows from that.
impl PartialEq for TopologyGroup {
    fn eq(&self, other: &Self) -> bool {
        (
            &self.name,
            &self.host,
            self.port,
            self.shards,
            &self.ranges,
            &self.placed_on,
        ) == (
            &other.name,
            &other.host,
            other.port,
            other.shards,
            &other.ranges,
            &other.placed_on,
        )
    }
}

impl Eq for TopologyGroup {}

impl TopologyGroup {
    /// The shard of this group that serves `slot`: the shard a range names, or else the
    /// group's other slots, in order, split into `shards` equal runs, as the shard formula
    /// splits all 16384 (a group that owns every slot places them exactly as the formula
    /// does). `None` when the group does not own it.
    pub fn shard_of(&self, slot: u16) -> Option<usize> {
        let (group, shard) = *self.placed.get(usize::from(slot))?;

        (group == self.index).then_some(usize::from(shard))
    }

    /// Writes this group's slots into `placed`.
    fn place(&self, placed: &mut [(u16, u16)]) {
        let ranked = || {
            self.ranges
                .iter()
                .zip(&self.placed_on)
                .filter(|(_, shard)| shard.is_none())
                .map(|(range, _)| *range)
        };
        let total: usize = ranked()
            .map(|(first, last)| usize::from(last - first) + 1)
            .sum();
        let mut rank = 0;

        for (first, last) in ranked() {
            for slot in first..=last {
                placed[usize::from(slot)] = (self.index, (rank * self.shards / total) as u16);
                rank += 1;
            }
        }
        for (&(first, last), shard) in self.ranges.iter().zip(&self.placed_on) {
            if let Some(shard) = *shard {
                for slot in first..=last {
                    placed[usize::from(slot)] = (self.index, shard);
                }
            }
        }
    }

    /// The group's ranges as a map file writes them, each with the shard that serves it.
    pub fn placed_ranges(&self) -> Vec<(u16, u16, u16)> {
        let mut out: Vec<(u16, u16, u16)> = Vec::new();

        for &(first, last) in &self.ranges {
            for slot in first..=last {
                let shard = self.shard_of(slot).unwrap_or(0) as u16;

                match out.last_mut() {
                    Some(run) if run.1 + 1 == slot && run.2 == shard => run.1 = slot,
                    _ => out.push((slot, slot, shard)),
                }
            }
        }
        out
    }
}

/// The whole cluster map, checked when it is loaded: every slot has exactly one group.
#[derive(Clone, PartialEq, Eq)]
pub struct Topology {
    /// Increases with every change of the map; reported as `cluster_current_epoch`.
    pub version: u64,
    pub groups: Vec<TopologyGroup>,
    /// Index of this node's group in `groups`.
    pub local: usize,
}

impl fmt::Debug for Topology {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Topology")
            .field("version", &self.version)
            .field("groups", &self.groups.len())
            .field("local", &self.local)
            .finish()
    }
}

/// The map every shard of a node reads, replaced whole when a newer one arrives
/// (`RUVIO TOPOLOGY SET`). Read on redirects and `CLUSTER` replies only, never per command.
#[derive(Clone)]
pub struct SharedTopology(Arc<RwLock<Arc<Topology>>>);

impl SharedTopology {
    pub fn new(topology: Topology) -> Self {
        Self(Arc::new(RwLock::new(Arc::new(topology))))
    }

    pub fn get(&self) -> Arc<Topology> {
        Arc::clone(&self.0.read().unwrap_or_else(|error| error.into_inner()))
    }

    pub fn replace(&self, topology: Topology) {
        *self.0.write().unwrap_or_else(|error| error.into_inner()) = Arc::new(topology);
    }
}

impl PartialEq for SharedTopology {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for SharedTopology {}

impl fmt::Debug for SharedTopology {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(formatter)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TopologyFile {
    version: u64,
    groups: Vec<GroupFile>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GroupFile {
    name: String,
    address: String,
    shards: usize,
    slots: Vec<String>,
}

impl Topology {
    /// Reads and checks a topology file; `group` names this node's group.
    pub fn load(path: &Path, group: &str, local_shards: usize) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|error| format!("cannot read topology {}: {error}", path.display()))?;

        Self::parse(&text, group, local_shards)
            .map_err(|error| format!("topology {}: {error}", path.display()))
    }

    /// The newer of the configured map and the one saved in `data_dir` by an earlier
    /// `RUVIO TOPOLOGY SET`, and whether it is the saved one (the coordinators').
    pub fn load_newest(
        path: &Path,
        data_dir: &Path,
        group: &str,
        local_shards: usize,
    ) -> Result<(Self, bool), String> {
        let configured = Self::load(path, group, local_shards)?;
        let saved = data_dir.join(TOPOLOGY_FILE);

        if !saved.exists() {
            return Ok((configured, false));
        }
        let saved = Self::load(&saved, group, local_shards)?;

        Ok(if saved.version > configured.version {
            tracing::warn!(
                saved = saved.version,
                configured = configured.version,
                "using the newer cluster map received from the coordinators ({TOPOLOGY_FILE} in \
                 the data directory) instead of cluster_topology"
            );
            (saved, true)
        } else {
            (configured, false)
        })
    }

    pub fn parse(text: &str, group: &str, local_shards: usize) -> Result<Self, String> {
        let file: TopologyFile = toml::from_str(text).map_err(|error| error.to_string())?;
        let mut owner: Vec<Option<usize>> = vec![None; HASH_SLOT_COUNT];
        let mut groups = Vec::with_capacity(file.groups.len());

        if file.groups.is_empty() {
            return Err("no groups".into());
        }
        // Group indices are `u16`, and a node's slot table holds `local_shards + groups`.
        if local_shards + file.groups.len() > crate::routing::FROZEN_BASE {
            return Err("too many groups".into());
        }

        for (index, entry) in file.groups.into_iter().enumerate() {
            if groups
                .iter()
                .any(|known: &TopologyGroup| known.name == entry.name)
            {
                return Err(format!("group {:?} is listed twice", entry.name));
            }
            if entry.shards == 0 || entry.shards > 1024 {
                return Err(format!("group {:?}: shards must be 1-1024", entry.name));
            }
            let (host, port) = entry
                .address
                .rsplit_once(':')
                .and_then(|(host, port)| Some((host, port.parse::<u16>().ok()?)))
                .filter(|(host, _)| !host.is_empty())
                .ok_or_else(|| format!("group {:?}: address must be host:port", entry.name))?;

            if usize::from(port) + entry.shards - 1 > usize::from(u16::MAX) {
                return Err(format!("group {:?}: ports past 65535", entry.name));
            }
            let mut ranges = Vec::with_capacity(entry.slots.len());

            for text in &entry.slots {
                let (first, last, shard) = parse_range(text)
                    .ok_or_else(|| format!("group {:?}: bad slot range {text:?}", entry.name))?;

                if shard.is_some_and(|shard| usize::from(shard) >= entry.shards) {
                    return Err(format!(
                        "group {:?}: slot range {text:?} names a shard it does not have",
                        entry.name
                    ));
                }

                for slot in first..=last {
                    if let Some(other) = owner[usize::from(slot)].replace(index) {
                        return Err(format!(
                            "slot {slot} is given to two groups ({} and {:?})",
                            groups
                                .get(other)
                                .map_or(entry.name.as_str(), |known: &TopologyGroup| &known.name),
                            entry.name
                        ));
                    }
                }
                ranges.push(((first, last), shard));
            }
            ranges.sort_unstable();
            let (ranges, placed_on): (Vec<_>, Vec<_>) = ranges.into_iter().unzip();

            groups.push(TopologyGroup {
                name: entry.name,
                host: host
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .to_owned(),
                port,
                shards: entry.shards,
                ranges,
                placed_on,
                index: index as u16,
                placed: std::sync::Arc::from(Vec::new()),
            });
        }
        let mut placed = vec![(0, 0); HASH_SLOT_COUNT];

        for group in &groups {
            group.place(&mut placed);
        }
        let placed: std::sync::Arc<[(u16, u16)]> = placed.into();

        for group in &mut groups {
            group.placed = std::sync::Arc::clone(&placed);
        }

        if let Some(slot) = owner.iter().position(Option::is_none) {
            return Err(format!("slot {slot} has no group"));
        }
        let local = groups
            .iter()
            .position(|known| known.name == group)
            .ok_or_else(|| format!("this node's group {group:?} is not in the topology"))?;

        if groups[local].shards != local_shards {
            return Err(format!(
                "group {group:?} lists {} shards, this node runs {local_shards}",
                groups[local].shards
            ));
        }
        if local_shards + groups.len() > usize::from(u16::MAX) {
            return Err("too many groups".into());
        }

        Ok(Self {
            version: file.version,
            groups,
            local,
        })
    }

    /// The slot table this map gives: this group's slots on its shards by
    /// [`TopologyGroup::shard_of`], every other group's on that group's remote owner entry
    /// (`local_shards + group index`, see [`Topology::remote`]).
    pub fn placement(&self, local_shards: usize) -> Vec<u16> {
        let mut owners = vec![0; HASH_SLOT_COUNT];

        for (index, group) in self.groups.iter().enumerate() {
            for &(first, last) in &group.ranges {
                for slot in first..=last {
                    owners[usize::from(slot)] = if index == self.local {
                        group.shard_of(slot).unwrap_or(0) as u16
                    } else {
                        (local_shards + index) as u16
                    };
                }
            }
        }

        owners
    }

    /// Fills the slot table with [`Topology::placement`].
    pub fn apply(&self, table: &SlotTable, local_shards: usize) {
        for (slot, owner) in self.placement(local_shards).into_iter().enumerate() {
            table.assign(slot as u16, usize::from(owner));
        }
    }

    /// Whether `slot` belongs to this node's group.
    pub fn owns(&self, slot: u16) -> bool {
        self.groups[self.local]
            .ranges
            .iter()
            .any(|&(first, last)| (first..=last).contains(&slot))
    }

    /// The client address of the shard that serves `slot`, for a table entry `owner` at or
    /// above `local_shards` (another group). A node that moved slots between its own shards
    /// forwards the key to the shard that holds it.
    pub fn remote(&self, slot: u16, owner: usize, local_shards: usize) -> Option<(&str, u16)> {
        let group = self.groups.get(owner.checked_sub(local_shards)?)?;
        let shard = group.shard_of(slot)?;

        Some((&group.host, group.port + shard as u16))
    }
}

/// `first-last`, `slot`, or either with `@shard`.
fn parse_range(text: &str) -> Option<(u16, u16, Option<u16>)> {
    let (text, shard) = match text.split_once('@') {
        Some((range, shard)) => (range, Some(shard.trim().parse().ok()?)),
        None => (text, None),
    };
    let (first, last) = match text.split_once('-') {
        Some((first, last)) => (first.trim().parse().ok()?, last.trim().parse().ok()?),
        None => {
            let slot: u16 = text.trim().parse().ok()?;
            (slot, slot)
        }
    };

    (first <= last && usize::from(last) < HASH_SLOT_COUNT).then_some((first, last, shard))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TWO: &str = r#"
        version = 3

        [[groups]]
        name = "a"
        address = "10.0.0.1:7000"
        shards = 2
        slots = ["0-8191"]

        [[groups]]
        name = "b"
        address = "10.0.0.2:7100"
        shards = 3
        slots = ["8192-16000", "16001-16383"]
    "#;

    #[test]
    fn remote_slots_point_at_the_owning_shard_of_the_other_group() {
        let topology = Topology::parse(TWO, "a", 2).unwrap();
        let table = SlotTable::from_formula(2);

        topology.apply(&table, 2);

        assert_eq!(topology.version, 3);
        assert!(topology.owns(0) && topology.owns(8191) && !topology.owns(8192));
        // Group a's slots are split over its two shards.
        assert_eq!(table.owner(0), 0);
        assert_eq!(table.owner(4095), 0);
        assert_eq!(table.owner(4096), 1);
        assert_eq!(table.owner(8191), 1);
        // Remote slots name group "b" (index 1) past the local shards.
        assert_eq!(table.owner(8192), 3);
        assert_eq!(table.owner(16383), 3);
        // Group b's 8192 slots over three shards: 2731 / 2731 / 2730.
        assert_eq!(topology.remote(8192, 3, 2), Some(("10.0.0.2", 7100)));
        assert_eq!(topology.remote(8192 + 2731, 3, 2), Some(("10.0.0.2", 7101)));
        assert_eq!(topology.remote(16383, 3, 2), Some(("10.0.0.2", 7102)));
        assert_eq!(topology.remote(0, 1, 2), None);
    }

    #[test]
    fn a_group_that_owns_every_slot_places_them_as_the_formula_does() {
        for shards in [1, 2, 3, 7, 16] {
            let text = format!(
                "version = 1\n[[groups]]\nname = \"a\"\naddress = \"h:1\"\nshards = {shards}\nslots = [\"0-16383\"]\n"
            );
            let topology = Topology::parse(&text, "a", shards).unwrap();
            let table = SlotTable::from_formula(shards);

            topology.apply(&table, shards);
            assert!(table == SlotTable::from_formula(shards), "{shards} shards");
        }
    }

    #[test]
    fn a_range_that_names_a_shard_stays_there_and_the_rest_keep_their_split() {
        // Group a kept 0-8191 on two shards, then gained 8192-9000 on shard 0.
        let text = TWO
            .replace(
                "[\"0-8191\"]",
                "[\"0-4095@0\", \"4096-8191@1\", \"8192-9000@0\"]",
            )
            .replace("[\"8192-16000\", \"16001-16383\"]", "[\"9001-16383\"]");
        let topology = Topology::parse(&text, "a", 2).unwrap();
        let a = &topology.groups[0];

        assert_eq!(a.shard_of(0), Some(0));
        assert_eq!(a.shard_of(4096), Some(1));
        assert_eq!(a.shard_of(8191), Some(1));
        assert_eq!(a.shard_of(8192), Some(0));
        assert_eq!(a.shard_of(9001), None);
        assert_eq!(
            a.placed_ranges(),
            vec![(0, 4095, 0), (4096, 8191, 1), (8192, 9000, 0)]
        );
        // Unnamed ranges keep the rank split.
        assert_eq!(
            Topology::parse(TWO, "a", 2).unwrap().groups[0].placed_ranges(),
            vec![(0, 4095, 0), (4096, 8191, 1)]
        );
        assert!(
            Topology::parse(&text.replace("@1", "@2"), "a", 2)
                .unwrap_err()
                .contains("names a shard")
        );
    }

    #[test]
    fn refuses_maps_with_gaps_overlaps_or_a_wrong_local_group() {
        let gap = TWO.replace("16001-16383", "16001-16382");
        let overlap = TWO.replace("8192-16000", "8191-16000");

        assert!(
            Topology::parse(&gap, "a", 2)
                .unwrap_err()
                .contains("slot 16383")
        );
        assert!(
            Topology::parse(&overlap, "a", 2)
                .unwrap_err()
                .contains("two groups")
        );
        assert!(
            Topology::parse(TWO, "c", 2)
                .unwrap_err()
                .contains("not in the topology")
        );
        assert!(Topology::parse(TWO, "a", 4).unwrap_err().contains("shards"));
        assert!(Topology::parse(&TWO.replace("10.0.0.2:7100", "nope"), "a", 2).is_err());
    }
}
