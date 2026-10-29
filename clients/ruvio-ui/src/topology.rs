//! The cluster as `CLUSTER SHARDS` describes it: one entry per shard, with every slot range it
//! owns and the nodes that serve it, primary first.

use ruvio_client::RespValue;
use serde_json::{Value, json};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Member {
    pub host: String,
    pub port: u16,
    pub primary: bool,
    /// The sequence a replica acknowledged; 0 for a primary.
    pub offset: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Shard {
    pub ranges: Vec<(u16, u16)>,
    /// Primary first, then its replicas.
    pub members: Vec<Member>,
}

impl Shard {
    pub fn primary(&self) -> Option<&Member> {
        self.members
            .iter()
            .find(|member| member.primary)
            .or(self.members.first())
    }

    fn primary_address(&self) -> Option<(&str, u16)> {
        self.primary()
            .map(|member| (member.host.as_str(), member.port))
    }
}

/// Parses a `CLUSTER SHARDS` reply. Shards without a node are left out; `None` when the reply
/// is not a list of shards.
pub fn parse_shards(value: &RespValue) -> Option<Vec<Shard>> {
    let RespValue::Array(rows) = value else {
        return None;
    };
    let mut shards = Vec::new();

    for row in rows {
        let RespValue::Array(fields) = row else {
            return None;
        };
        let mut ranges = Vec::new();
        let mut members = Vec::new();

        for pair in fields.chunks(2) {
            let [name, value] = pair else { break };

            match (text_of(name).as_deref(), value) {
                (Some("slots"), RespValue::Array(bounds)) => {
                    for bound in bounds.chunks(2) {
                        if let [start, end] = bound
                            && let (Some(start), Some(end)) = (integer_of(start), integer_of(end))
                            && let (Ok(start), Ok(end)) = (u16::try_from(start), u16::try_from(end))
                        {
                            ranges.push((start, end));
                        }
                    }
                }
                (Some("nodes"), RespValue::Array(nodes)) => {
                    members.extend(nodes.iter().filter_map(member_of));
                }
                _ => {}
            }
        }

        if !members.is_empty() {
            shards.push(Shard { ranges, members });
        }
    }

    Some(shards)
}

fn member_of(node: &RespValue) -> Option<Member> {
    let RespValue::Array(fields) = node else {
        return None;
    };
    let mut host = None;
    let mut endpoint = None;
    let mut port = None;
    let mut primary = true;
    let mut offset = 0;

    for pair in fields.chunks(2) {
        let [name, value] = pair else { break };

        match text_of(name).as_deref() {
            Some("ip") => host = text_of(value),
            Some("endpoint") => endpoint = text_of(value),
            Some("port") => port = integer_of(value).and_then(|port| u16::try_from(port).ok()),
            Some("role") => primary = text_of(value).as_deref() == Some("master"),
            Some("replication-offset") => offset = integer_of(value).unwrap_or(0),
            _ => {}
        }
    }

    Some(Member {
        host: endpoint.filter(|name| !name.is_empty()).or(host)?,
        port: port?,
        primary,
        offset,
    })
}

/// Shards listed by their primary alone: another group's shards (a node does not know their
/// replicas) or shards without replicas. Their primaries are asked for their own view.
pub fn lone_primaries(shards: &[Shard]) -> Vec<(String, u16)> {
    let mut addresses: Vec<(String, u16)> = Vec::new();

    for shard in shards.iter().filter(|shard| shard.members.len() == 1) {
        if let Some((host, port)) = shard.primary_address()
            && !addresses
                .iter()
                .any(|(known, known_port)| known == host && *known_port == port)
        {
            addresses.push((host.to_owned(), port));
        }
    }

    addresses
}

/// Takes the members of `answer`'s shards (the view of another group's node, which lists its
/// own replicas) for the shards of `shards` that have the same primary and fewer members.
pub fn merge_members(shards: &mut [Shard], answer: &[Shard]) {
    for shard in shards.iter_mut() {
        let Some(primary) = shard.primary_address() else {
            continue;
        };

        if let Some(better) = answer.iter().find(|other| {
            other.primary_address() == Some(primary) && other.members.len() > shard.members.len()
        }) {
            shard.members = better.members.clone();
        }
    }
}

pub fn is_loopback_host(host: &str) -> bool {
    let host = host.trim_matches(['[', ']']);

    host.eq_ignore_ascii_case("localhost") || host == "::1" || host.starts_with("127.")
}

/// A server that does not set `advertised_host` names its nodes `127.0.0.1`, which points back
/// at this process when the server is elsewhere (another machine, a host the UI container
/// reaches by name). Such nodes are reached through the address the user signed in with.
pub fn reroute_loopback(shards: &mut [Shard], entry_host: &str) -> Option<String> {
    if is_loopback_host(entry_host) {
        return None;
    }

    let mut changed = false;

    for member in shards.iter_mut().flat_map(|shard| shard.members.iter_mut()) {
        if is_loopback_host(&member.host) {
            member.host = entry_host.to_owned();
            changed = true;
        }
    }

    changed.then(|| {
        format!(
            "The server names its shards 127.0.0.1, so they are reached through {entry_host}. Set RUVIO_ADVERTISE_HOST on the server to a name clients can reach to make this permanent."
        )
    })
}

/// Orders the shards by their first slot, so a shard keeps its number (and a group its color)
/// whichever node the user signed in to. Shards without slots go last.
pub fn sort_by_slots(shards: &mut [Shard]) {
    shards.sort_by_key(|shard| {
        shard
            .ranges
            .iter()
            .map(|(start, _)| *start)
            .min()
            .unwrap_or(u16::MAX)
    });
}

/// The shards as the browser reads them: the primary's address, every range, the slot count
/// and the replicas.
pub fn shards_json(shards: &[Shard]) -> Vec<Value> {
    shards
        .iter()
        .enumerate()
        .filter_map(|(index, shard)| {
            let primary = shard.primary()?;
            let slots: u32 = shard
                .ranges
                .iter()
                .map(|(start, end)| u32::from(end.saturating_sub(*start)) + 1)
                .sum();

            Some(json!({
                "id": index,
                "host": primary.host,
                "port": primary.port,
                "ranges": shard.ranges.iter().map(|(start, end)| json!([start, end])).collect::<Vec<_>>(),
                "slots": slots,
                "replicas": shard
                    .members
                    .iter()
                    .filter(|member| !std::ptr::eq(*member, primary))
                    .map(|member| json!({"host": member.host, "port": member.port, "offset": member.offset}))
                    .collect::<Vec<_>>(),
            }))
        })
        .collect()
}

pub fn integer_of(value: &RespValue) -> Option<u64> {
    match value {
        RespValue::Integer(value) => u64::try_from(*value).ok(),
        RespValue::Bulk(bytes) => std::str::from_utf8(bytes).ok()?.parse().ok(),
        RespValue::Simple(text) => text.parse().ok(),
        _ => None,
    }
}

pub fn text_of(value: &RespValue) -> Option<String> {
    match value {
        RespValue::Bulk(bytes) => String::from_utf8(bytes.clone()).ok(),
        RespValue::Simple(text) => Some(text.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(text: &str) -> RespValue {
        RespValue::Bulk(text.as_bytes().to_vec())
    }

    fn node(host: &str, port: i64, role: &str, offset: i64) -> RespValue {
        RespValue::Array(vec![
            bulk("id"),
            bulk("abc"),
            bulk("port"),
            RespValue::Integer(port),
            bulk("ip"),
            bulk(host),
            bulk("endpoint"),
            bulk(host),
            bulk("role"),
            bulk(role),
            bulk("replication-offset"),
            RespValue::Integer(offset),
            bulk("health"),
            bulk("online"),
        ])
    }

    fn shard(slots: &[i64], nodes: Vec<RespValue>) -> RespValue {
        RespValue::Array(vec![
            bulk("slots"),
            RespValue::Array(slots.iter().map(|slot| RespValue::Integer(*slot)).collect()),
            bulk("nodes"),
            RespValue::Array(nodes),
        ])
    }

    fn member(host: &str, port: u16, primary: bool, offset: u64) -> Member {
        Member {
            host: host.into(),
            port,
            primary,
            offset,
        }
    }

    #[test]
    fn a_shard_with_several_ranges_is_one_shard_with_its_replicas() {
        let reply = RespValue::Array(vec![
            shard(
                &[0, 99, 200, 8191],
                vec![
                    node("a", 7000, "master", 0),
                    node("a2", 7000, "replica", 42),
                ],
            ),
            shard(&[100, 199, 8192, 16383], vec![node("a", 7001, "master", 0)]),
        ]);
        let shards = parse_shards(&reply).unwrap();

        assert_eq!(shards.len(), 2);
        assert_eq!(shards[0].ranges, vec![(0, 99), (200, 8191)]);
        assert_eq!(
            shards[0].members,
            vec![member("a", 7000, true, 0), member("a2", 7000, false, 42)]
        );

        let json = shards_json(&shards);

        assert_eq!(json[0]["slots"], 100 + 7992);
        assert_eq!(json[0]["replicas"][0]["offset"], 42);
        assert_eq!(json[1]["ranges"], json!([[100, 199], [8192, 16383]]));
        assert_eq!(json[1]["replicas"], json!([]));
    }

    #[test]
    fn shards_are_numbered_by_their_first_slot() {
        let shard = |first, port| Shard {
            ranges: vec![(first, first)],
            members: vec![member("a", port, true, 0)],
        };
        let mut shards = vec![
            shard(8192, 7120),
            Shard {
                ranges: vec![],
                members: vec![member("e", 1, true, 0)],
            },
            shard(0, 7100),
        ];

        sort_by_slots(&mut shards);

        assert_eq!(
            shards
                .iter()
                .map(|shard| shard.members[0].port)
                .collect::<Vec<_>>(),
            vec![7100, 7120, 1]
        );
    }

    #[test]
    fn a_reply_that_is_not_a_shard_list_is_none() {
        assert!(parse_shards(&RespValue::Error("ERR cluster mode is disabled".into())).is_none());
        assert_eq!(parse_shards(&RespValue::Array(vec![])), Some(vec![]));
    }

    #[test]
    fn another_groups_replicas_come_from_its_own_primary() {
        let mut ours = vec![
            Shard {
                ranges: vec![(0, 8191)],
                members: vec![member("a", 7000, true, 0), member("a2", 7000, false, 5)],
            },
            Shard {
                ranges: vec![(8192, 16383)],
                members: vec![member("b", 7000, true, 0)],
            },
        ];

        assert_eq!(lone_primaries(&ours), vec![("b".to_owned(), 7000)]);

        let theirs = vec![
            Shard {
                ranges: vec![(0, 8191)],
                members: vec![member("a", 7000, true, 0)],
            },
            Shard {
                ranges: vec![(8192, 16383)],
                members: vec![member("b", 7000, true, 0), member("b2", 7000, false, 9)],
            },
        ];

        merge_members(&mut ours, &theirs);

        assert_eq!(ours[0].members.len(), 2, "our own replicas are kept");
        assert_eq!(ours[1].members[1], member("b2", 7000, false, 9));
    }

    #[test]
    fn loopback_nodes_are_reached_through_the_entry_host() {
        let mut shards = vec![Shard {
            ranges: vec![(0, 16383)],
            members: vec![
                member("127.0.0.1", 6379, true, 0),
                member("localhost", 6390, false, 0),
                member("10.0.0.9", 6391, false, 0),
            ],
        }];
        let note = reroute_loopback(&mut shards, "ruvio");

        assert!(note.is_some_and(|text| text.contains("through ruvio")));
        assert_eq!(shards[0].members[0].host, "ruvio");
        assert_eq!(shards[0].members[1].host, "ruvio");
        assert_eq!(shards[0].members[2].host, "10.0.0.9");

        let mut local = vec![Shard {
            ranges: vec![(0, 16383)],
            members: vec![member("127.0.0.1", 6379, true, 0)],
        }];

        assert!(reroute_loopback(&mut local, "127.0.0.1").is_none());
        assert!(reroute_loopback(&mut local, "localhost").is_none());
        assert_eq!(local[0].members[0].host, "127.0.0.1");
    }
}
