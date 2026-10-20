//! `RUVIO TOPOLOGY`: the coordinator keeps a multi-node node's cluster map current.

use std::io::{self, Write};

use super::ShardState;
use crate::{
    engine::{Response, write_response},
    topology::{TOPOLOGY_FILE, Topology},
};

/// `RUVIO TOPOLOGY VERSION` (`map` is `None`) or `RUVIO TOPOLOGY SET map`.
pub(super) fn command(
    state: &ShardState,
    map: Option<&[u8]>,
    output: &mut impl Write,
) -> io::Result<()> {
    let current = state
        .forwarding
        .as_ref()
        .and_then(|forwarding| forwarding.fabric.topology());
    let Some(map) = map else {
        let version = current.map_or(0, |shared| shared.get().version);

        return write_response(output, Response::Integer(version as i64));
    };

    match install(state, map) {
        Ok(()) => output.write_all(b"+OK\r\n"),
        Err(message) => {
            output.write_all(b"-")?;
            // A parse error may span lines; an error reply is one line.
            output.write_all(message.replace(['\r', '\n'], " ").as_bytes())?;
            output.write_all(b"\r\n")
        }
    }
}

fn install(state: &ShardState, map: &[u8]) -> Result<(), String> {
    // One install at a time per process: two shards checking the version and writing the
    // file at once could otherwise leave the older map on disk or in memory.
    static INSTALLING: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _installing = INSTALLING.lock().unwrap_or_else(|error| error.into_inner());

    const UNMAPPED: &str = "ERR RUVIO TOPOLOGY SET needs a node started with cluster_topology";
    let forwarding = state.forwarding.as_ref().ok_or(UNMAPPED)?;
    let current = forwarding
        .fabric
        .topology()
        .map(|shared| shared.get())
        .ok_or(UNMAPPED)?;
    let config = &state.published_config;
    let text = std::str::from_utf8(map).map_err(|_| "ERR RUVIO TOPOLOGY map is not UTF-8")?;
    let next = Topology::parse(text, &config.cluster_group, config.shards)
        .map_err(|error| format!("ERR RUVIO TOPOLOGY {error}"))?;

    if next.version < current.version {
        return Err(format!(
            "ERR RUVIO TOPOLOGY version {} is older than {}",
            next.version, current.version
        ));
    }
    if next.version == current.version {
        return if next == *current {
            Ok(())
        } else {
            Err(format!(
                "ERR RUVIO TOPOLOGY version {} is already in use with another map",
                next.version
            ))
        };
    }
    // Data does not move between groups yet: only the other groups' addresses may change.
    if next.groups[next.local].ranges != current.groups[current.local].ranges {
        return Err(
            "ERR RUVIO TOPOLOGY would change this group's slots; moving slots between \
                    groups is not supported yet"
                .into(),
        );
    }

    // Kept even without persistence: a restart must not fall back to an older map.
    std::fs::create_dir_all(&config.persistence.data_dir)
        .and_then(|()| {
            crate::replication::write_durably(&config.persistence.data_dir.join(TOPOLOGY_FILE), map)
        })
        .map_err(|error| format!("ERR RUVIO TOPOLOGY cannot save the map: {error}"))?;
    tracing::info!(version = next.version, "installed cluster topology");
    forwarding.fabric.set_topology(next);

    Ok(())
}
