//! Cluster routing against an in-process fake cluster: no Ruvio server needed.
//!
//! `Cluster` runs N fake shard nodes on loopback. Each node keeps a small in-memory store,
//! answers `CLUSTER SLOTS` from a slot table the test can change, and replies `-MOVED` for
//! keys it does not own, so redirects, stale maps and dead nodes are all reproducible.

use std::collections::{HashMap, VecDeque};
use std::io::{BufReader, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use ruvio_client::__bench::parse;
use ruvio_client::{Client, ClientOptions, Error, RespValue, hash_slot};

type Reply = Vec<u8>;

fn simple(text: &str) -> Reply {
    format!("+{text}\r\n").into_bytes()
}

fn error(text: &str) -> Reply {
    format!("-{text}\r\n").into_bytes()
}

fn int(value: i64) -> Reply {
    format!(":{value}\r\n").into_bytes()
}

fn bulk(text: &str) -> Reply {
    format!("${}\r\n{text}\r\n", text.len()).into_bytes()
}

fn null() -> Reply {
    b"$-1\r\n".to_vec()
}

fn array(items: Vec<Reply>) -> Reply {
    let mut out = format!("*{}\r\n", items.len()).into_bytes();

    for item in items {
        out.extend(item);
    }

    out
}

#[derive(Clone, Debug, PartialEq)]
enum Val {
    Str(String),
    List(Vec<String>),
    Set(Vec<String>),
    Hash(Vec<(String, String)>),
    Zset(Vec<(String, String)>),
    Stream(Vec<(String, Vec<(String, String)>)>),
}

#[derive(Clone, Debug)]
struct Entry {
    val: Val,
    ttl: i64,
}

struct State {
    ports: Vec<u16>,
    owners: Vec<usize>,
    stores: Vec<HashMap<String, Entry>>,
    log: Vec<(usize, Vec<String>)>,
    script: HashMap<String, VecDeque<Reply>>,
    hang_up: HashMap<String, usize>,
    dead: Vec<bool>,
    conns: Vec<Vec<TcpStream>>,
}

fn stream_number(id: &str) -> u64 {
    id.split('-')
        .next()
        .and_then(|n| n.parse().ok())
        .unwrap_or(0)
}

impl State {
    fn slots_reply(&self) -> Reply {
        let mut runs: Vec<Reply> = Vec::new();
        let mut start = 0_usize;

        for slot in 1..=self.owners.len() {
            if slot == self.owners.len() || self.owners[slot] != self.owners[start] {
                runs.push(array(vec![
                    int(start as i64),
                    int(slot as i64 - 1),
                    array(vec![
                        bulk("127.0.0.1"),
                        int(i64::from(self.ports[self.owners[start]])),
                    ]),
                ]));
                start = slot;
            }
        }

        array(runs)
    }

    fn keys_of(cmd: &[String]) -> Vec<&str> {
        let name = cmd[0].to_ascii_uppercase();

        match name.as_str() {
            "GET" | "SET" | "TYPE" | "PTTL" | "PEXPIRE" | "RPUSH" | "LRANGE" | "SADD"
            | "SMEMBERS" | "HSET" | "HSCAN" | "ZADD" | "ZRANGE" | "XADD" | "XRANGE" | "RENAME" => {
                cmd.get(1).map(String::as_str).into_iter().collect()
            }
            "DEL" | "EXISTS" | "MGET" | "TOUCH" | "UNLINK" | "WATCH" => {
                cmd[1..].iter().map(String::as_str).collect()
            }
            "MSET" => cmd[1..].iter().step_by(2).map(String::as_str).collect(),
            _ => Vec::new(),
        }
    }

    fn moved_check(&self, node: usize, cmd: &[String]) -> Option<Reply> {
        for key in Self::keys_of(cmd) {
            let slot = hash_slot(key);
            let owner = self.owners[usize::from(slot)];

            if owner != node {
                return Some(error(&format!(
                    "MOVED {slot} 127.0.0.1:{}",
                    self.ports[owner]
                )));
            }
        }

        None
    }

    fn take_hang_up(&mut self, node: usize, name: &str) -> bool {
        for key in [format!("{node}:{name}"), name.to_owned()] {
            if let Some(left) = self.hang_up.get_mut(&key) {
                if *left > 0 {
                    *left -= 1;

                    return true;
                }
            }
        }

        false
    }

    fn take_script(&mut self, node: usize, name: &str) -> Option<Reply> {
        for key in [format!("{node}:{name}"), name.to_owned()] {
            if let Some(queue) = self.script.get_mut(&key) {
                if let Some(reply) = queue.pop_front() {
                    return Some(reply);
                }
            }
        }

        None
    }

    fn dispatch(
        &mut self,
        node: usize,
        cmd: &[String],
        queue: &mut Option<Vec<Vec<String>>>,
    ) -> Option<Reply> {
        let name = cmd[0].to_ascii_uppercase();

        self.log.push((node, cmd.to_vec()));

        if self.take_hang_up(node, &name) {
            return None;
        }

        if let Some(reply) = self.take_script(node, &name) {
            return Some(reply);
        }

        if let Some(reply) = self.moved_check(node, cmd) {
            return Some(reply);
        }

        if queue.is_some() {
            match name.as_str() {
                "EXEC" => {
                    let commands = queue.take().unwrap();
                    let replies = commands.iter().map(|c| self.run(node, c)).collect();

                    return Some(array(replies));
                }
                "DISCARD" => {
                    *queue = None;

                    return Some(simple("OK"));
                }
                _ => {
                    queue.as_mut().unwrap().push(cmd.to_vec());

                    return Some(simple("QUEUED"));
                }
            }
        }

        if name == "MULTI" {
            *queue = Some(Vec::new());

            return Some(simple("OK"));
        }

        Some(self.run(node, cmd))
    }

    fn run(&mut self, node: usize, cmd: &[String]) -> Reply {
        let name = cmd[0].to_ascii_uppercase();
        let arg = |index: usize| cmd.get(index).cloned().unwrap_or_default();

        match name.as_str() {
            "PING" => simple("PONG"),
            "ECHO" => bulk(&arg(1)),
            "CLUSTER" => self.slots_reply(),
            "GET" => match self.stores[node].get(&arg(1)) {
                None => null(),
                Some(Entry {
                    val: Val::Str(text),
                    ..
                }) => bulk(text),
                Some(_) => error("WRONGTYPE wrong kind of value"),
            },
            "SET" => {
                self.stores[node].insert(
                    arg(1),
                    Entry {
                        val: Val::Str(arg(2)),
                        ttl: -1,
                    },
                );

                simple("OK")
            }
            "MSET" => {
                for pair in cmd[1..].chunks(2) {
                    self.stores[node].insert(
                        pair[0].clone(),
                        Entry {
                            val: Val::Str(pair.get(1).cloned().unwrap_or_default()),
                            ttl: -1,
                        },
                    );
                }

                simple("OK")
            }
            "DEL" | "UNLINK" => int(cmd[1..]
                .iter()
                .filter(|k| self.stores[node].remove(*k).is_some())
                .count() as i64),
            "EXISTS" | "TOUCH" => int(cmd[1..]
                .iter()
                .filter(|k| self.stores[node].contains_key(*k))
                .count() as i64),
            "MGET" => array(
                cmd[1..]
                    .iter()
                    .map(|k| match self.stores[node].get(k) {
                        Some(Entry {
                            val: Val::Str(text),
                            ..
                        }) => bulk(text),
                        _ => null(),
                    })
                    .collect(),
            ),
            "TYPE" => match self.stores[node].get(&arg(1)).map(|e| &e.val) {
                None => simple("none"),
                Some(Val::Str(_)) => simple("string"),
                Some(Val::List(_)) => simple("list"),
                Some(Val::Set(_)) => simple("set"),
                Some(Val::Hash(_)) => simple("hash"),
                Some(Val::Zset(_)) => simple("zset"),
                Some(Val::Stream(_)) => simple("stream"),
            },
            "PTTL" => match self.stores[node].get(&arg(1)) {
                None => int(-2),
                Some(entry) => int(entry.ttl),
            },
            "PEXPIRE" => match self.stores[node].get_mut(&arg(1)) {
                None => int(0),
                Some(entry) => {
                    entry.ttl = arg(2).parse().unwrap_or(-1);

                    int(1)
                }
            },
            "RPUSH" | "SADD" => {
                let set = name == "SADD";
                let entry = self.stores[node].entry(arg(1)).or_insert(Entry {
                    val: if set {
                        Val::Set(Vec::new())
                    } else {
                        Val::List(Vec::new())
                    },
                    ttl: -1,
                });
                let (Val::List(items) | Val::Set(items)) = &mut entry.val else {
                    return error("WRONGTYPE");
                };

                items.extend(cmd[2..].iter().cloned());

                int(items.len() as i64)
            }
            "LRANGE" => match self.stores[node].get(&arg(1)).map(|e| &e.val) {
                Some(Val::List(items)) => {
                    let from: usize = arg(2).parse().unwrap_or(0);
                    let to: usize = arg(3).parse().unwrap_or(0);
                    let end = (to + 1).min(items.len());

                    array(
                        items
                            .get(from..end.max(from))
                            .unwrap_or(&[])
                            .iter()
                            .map(|s| bulk(s))
                            .collect(),
                    )
                }
                _ => array(Vec::new()),
            },
            "SMEMBERS" => match self.stores[node].get(&arg(1)).map(|e| &e.val) {
                Some(Val::Set(items)) => array(items.iter().map(|s| bulk(s)).collect()),
                _ => array(Vec::new()),
            },
            "HSET" => {
                let entry = self.stores[node].entry(arg(1)).or_insert(Entry {
                    val: Val::Hash(Vec::new()),
                    ttl: -1,
                });
                let Val::Hash(pairs) = &mut entry.val else {
                    return error("WRONGTYPE");
                };

                for pair in cmd[2..].chunks(2) {
                    pairs.push((pair[0].clone(), pair.get(1).cloned().unwrap_or_default()));
                }

                int((cmd.len() as i64 - 2) / 2)
            }
            "HSCAN" => match self.stores[node].get(&arg(1)).map(|e| &e.val) {
                Some(Val::Hash(pairs)) => {
                    let from: usize = arg(2).parse().unwrap_or(0);
                    let count: usize = arg(4).parse().unwrap_or(10);
                    let end = (from + count).min(pairs.len());
                    let next = if end >= pairs.len() { 0 } else { end };
                    let flat = pairs[from..end]
                        .iter()
                        .flat_map(|(f, v)| [bulk(f), bulk(v)])
                        .collect();

                    array(vec![bulk(&next.to_string()), array(flat)])
                }
                _ => array(vec![bulk("0"), array(Vec::new())]),
            },
            "ZADD" => {
                let entry = self.stores[node].entry(arg(1)).or_insert(Entry {
                    val: Val::Zset(Vec::new()),
                    ttl: -1,
                });
                let Val::Zset(members) = &mut entry.val else {
                    return error("WRONGTYPE");
                };

                for pair in cmd[2..].chunks(2) {
                    members.push((pair[1].clone(), pair[0].clone()));
                }

                int((cmd.len() as i64 - 2) / 2)
            }
            "ZRANGE" => match self.stores[node].get(&arg(1)).map(|e| &e.val) {
                Some(Val::Zset(members)) => {
                    let from: usize = arg(2).parse().unwrap_or(0);
                    let to: usize = arg(3).parse().unwrap_or(0);
                    let flat: Vec<Reply> = members
                        .iter()
                        .flat_map(|(m, s)| [bulk(m), bulk(s)])
                        .collect();
                    let end = (to + 1).min(flat.len());

                    array(flat.get(from..end.max(from)).unwrap_or(&[]).to_vec())
                }
                _ => array(Vec::new()),
            },
            "XADD" => {
                let entry = self.stores[node].entry(arg(1)).or_insert(Entry {
                    val: Val::Stream(Vec::new()),
                    ttl: -1,
                });
                let Val::Stream(entries) = &mut entry.val else {
                    return error("WRONGTYPE");
                };
                let fields = cmd[3..]
                    .chunks(2)
                    .map(|p| (p[0].clone(), p.get(1).cloned().unwrap_or_default()))
                    .collect();

                entries.push((arg(2), fields));

                bulk(&arg(2))
            }
            "XRANGE" => match self.stores[node].get(&arg(1)).map(|e| &e.val) {
                Some(Val::Stream(entries)) => {
                    let start = arg(2);
                    let count: usize = arg(5).parse().unwrap_or(usize::MAX);
                    let page = entries
                        .iter()
                        .filter(|(id, _)| {
                            start == "-" || stream_number(id) >= stream_number(&start)
                        })
                        .take(count)
                        .map(|(id, fields)| {
                            array(vec![
                                bulk(id),
                                array(
                                    fields
                                        .iter()
                                        .flat_map(|(f, v)| [bulk(f), bulk(v)])
                                        .collect(),
                                ),
                            ])
                        })
                        .collect();

                    array(page)
                }
                _ => array(Vec::new()),
            },
            "RENAME" => match self.stores[node].remove(&arg(1)) {
                Some(entry) => {
                    self.stores[node].insert(arg(2), entry);

                    simple("OK")
                }
                None => error("ERR no such key"),
            },
            "SCAN" => {
                let mut keys: Vec<&String> = self.stores[node].keys().collect();

                keys.sort();

                let from: usize = arg(1).parse().unwrap_or(0);
                let end = (from + 2).min(keys.len());
                let next = if end >= keys.len() { 0 } else { end };
                let page = keys[from.min(end)..end].iter().map(|k| bulk(k)).collect();

                array(vec![bulk(&next.to_string()), array(page)])
            }
            "DBSIZE" => int(self.stores[node].len() as i64),
            "FLUSHDB" | "FLUSHALL" => {
                self.stores[node].clear();

                simple("OK")
            }
            "SUBSCRIBE" | "SSUBSCRIBE" | "PSUBSCRIBE" => {
                let mut out = Vec::new();

                for (index, channel) in cmd[1..].iter().enumerate() {
                    out.extend(array(vec![
                        bulk(&name.to_ascii_lowercase()),
                        bulk(channel),
                        int(index as i64 + 1),
                    ]));
                }

                out
            }
            "PUBLISH" | "SPUBLISH" => int(0),
            _ => simple("OK"),
        }
    }
}

fn serve(state: Arc<Mutex<State>>, node: usize, stream: TcpStream) {
    let mut reader = BufReader::new(stream.try_clone().expect("clone"));
    let mut writer = stream;
    let mut queue = None;

    loop {
        let Ok(RespValue::Array(items)) = parse(&mut reader) else {
            return;
        };
        let command: Vec<String> = items
            .into_iter()
            .map(|item| item.into_string().unwrap().unwrap())
            .collect();
        let reply = state.lock().unwrap().dispatch(node, &command, &mut queue);

        match reply {
            None => {
                let _ = writer.shutdown(Shutdown::Both);

                return;
            }
            Some(bytes) => {
                if writer.write_all(&bytes).is_err() {
                    return;
                }
            }
        }
    }
}

fn accept_loop(state: Arc<Mutex<State>>, node: usize, listener: TcpListener) {
    listener.set_nonblocking(true).unwrap();

    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(false).unwrap();

                {
                    // Checked under the lock kill() takes, so no socket outlives a kill.
                    let mut state = state.lock().unwrap();

                    if state.dead[node] {
                        return;
                    }

                    state.conns[node].push(stream.try_clone().unwrap());
                }

                let state = Arc::clone(&state);

                thread::spawn(move || serve(state, node, stream));
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if state.lock().unwrap().dead[node] {
                    return;
                }

                thread::sleep(Duration::from_millis(2));
            }
            Err(_) => return,
        }
    }
}

struct Cluster {
    state: Arc<Mutex<State>>,
    accepts: Mutex<Vec<Option<JoinHandle<()>>>>,
}

impl Cluster {
    fn start(nodes: usize) -> Self {
        let listeners: Vec<TcpListener> = (0..nodes)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let ports = listeners
            .iter()
            .map(|l| l.local_addr().unwrap().port())
            .collect();
        let owners = (0..16_384_usize)
            .map(|slot| slot * nodes / 16_384)
            .collect();
        let state = Arc::new(Mutex::new(State {
            ports,
            owners,
            stores: vec![HashMap::new(); nodes],
            log: Vec::new(),
            script: HashMap::new(),
            hang_up: HashMap::new(),
            dead: vec![false; nodes],
            conns: (0..nodes).map(|_| Vec::new()).collect(),
        }));

        let accepts = listeners
            .into_iter()
            .enumerate()
            .map(|(node, listener)| {
                let state = Arc::clone(&state);

                Some(thread::spawn(move || accept_loop(state, node, listener)))
            })
            .collect();

        Self {
            state,
            accepts: Mutex::new(accepts),
        }
    }

    fn port(&self, node: usize) -> u16 {
        self.state.lock().unwrap().ports[node]
    }

    fn options(&self) -> ClientOptions {
        ClientOptions {
            host: "127.0.0.1".to_owned(),
            port: self.port(0),
            connect_timeout: Duration::from_secs(1),
            max_reconnect_attempts: 2,
            reconnect_base_delay: Duration::ZERO,
            reconnect_max_delay: Duration::ZERO,
            ..ClientOptions::default()
        }
    }

    fn client(&self) -> Client {
        let mut client = Client::connect_with(self.options()).unwrap();

        // No test may hang on a reply that never comes.
        client
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();

        client
    }

    fn owner(&self, slot: u16) -> usize {
        self.state.lock().unwrap().owners[usize::from(slot)]
    }

    /// A key whose slot `node` owns right now.
    fn key_on(&self, node: usize, tag: &str) -> String {
        (0..)
            .map(|i| format!("{tag}{i}"))
            .find(|key| self.owner(hash_slot(key)) == node)
            .unwrap()
    }

    fn log(&self) -> Vec<(usize, Vec<String>)> {
        self.state.lock().unwrap().log.clone()
    }

    fn clear_log(&self) {
        self.state.lock().unwrap().log.clear();
    }

    /// Commands the nodes received, by upper-case name.
    fn seen(&self, name: &str) -> Vec<(usize, Vec<String>)> {
        self.log()
            .into_iter()
            .filter(|(_, cmd)| cmd[0].eq_ignore_ascii_case(name))
            .collect()
    }

    fn put(&self, node: usize, key: &str, val: Val, ttl: i64) {
        self.state.lock().unwrap().stores[node].insert(key.to_owned(), Entry { val, ttl });
    }

    fn peek(&self, node: usize, key: &str) -> Option<Val> {
        self.state.lock().unwrap().stores[node]
            .get(key)
            .map(|e| e.val.clone())
    }

    fn move_slots(&self, from: u16, to: u16, node: usize) {
        let mut state = self.state.lock().unwrap();

        for slot in from..=to {
            state.owners[usize::from(slot)] = node;
        }
    }

    /// Queues `times` copies of `reply` for the command `name` (optionally `"node:NAME"`).
    fn script(&self, name: &str, reply: &[u8], times: usize) {
        let mut state = self.state.lock().unwrap();
        let queue = state.script.entry(name.to_owned()).or_default();

        for _ in 0..times {
            queue.push_back(reply.to_vec());
        }
    }

    /// Closes the connection, without replying, the next `times` times `name` arrives.
    fn hang_up(&self, name: &str, times: usize) {
        self.state
            .lock()
            .unwrap()
            .hang_up
            .insert(name.to_owned(), times);
    }

    /// Stops the node: open sockets close, and new connections are refused once this returns.
    fn kill(&self, node: usize) {
        let mut state = self.state.lock().unwrap();

        state.dead[node] = true;

        for stream in state.conns[node].drain(..) {
            let _ = stream.shutdown(Shutdown::Both);
        }

        drop(state);

        // The accept loop drops the listener when it returns.
        if let Some(accept) = self.accepts.lock().unwrap()[node].take() {
            accept.join().unwrap();
        }
    }

    fn push(&self, node: usize, bytes: &[u8]) {
        let state = self.state.lock().unwrap();

        for mut stream in state.conns[node].iter() {
            stream.write_all(bytes).unwrap();
        }
    }
}

fn text(value: &str) -> RespValue {
    RespValue::Bulk(value.as_bytes().to_vec())
}

fn server_error(result: Result<RespValue, Error>) -> String {
    match result {
        Err(Error::Server(message)) => message,
        Err(other) => panic!("expected a server error, got {other:?}"),
        Ok(value) => panic!("expected a server error, got {value:?}"),
    }
}

fn protocol_error<T>(result: Result<T, Error>) -> String {
    match result {
        Err(Error::Protocol(message)) => message,
        Err(other) => panic!("expected a protocol error, got {other:?}"),
        Ok(_) => panic!("expected a protocol error, got Ok"),
    }
}

fn many(client: &mut Client, commands: Vec<Vec<&str>>) -> Result<Vec<RespValue>, Error> {
    let refs: Vec<&[&str]> = commands.iter().map(Vec::as_slice).collect();

    client.execute_many(&refs)
}

// ---------------------------------------------------------------------------------------------
// Discovery
// ---------------------------------------------------------------------------------------------

#[test]
fn keys_are_sent_to_the_node_that_owns_their_slot() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();

    assert!(client.is_cluster());
    assert_eq!(client.shard_count(), 3);

    for index in 0..40 {
        let key = format!("route{index}");
        let owner = cluster.owner(hash_slot(&key));

        assert!(client.set(&key, "v").unwrap());
        assert_eq!(client.get_string(&key).unwrap().as_deref(), Some("v"));
        assert_eq!(cluster.peek(owner, &key), Some(Val::Str("v".into())));
    }

    assert_eq!(client.get("never-set").unwrap(), None);
}

#[test]
fn discovery_rejects_malformed_slot_maps() {
    for reply in [
        // Gap: slots 101..16383 are not owned.
        &b"*1\r\n*3\r\n:0\r\n:100\r\n*2\r\n$9\r\n127.0.0.1\r\n:1\r\n"[..],
        // An empty array is not a topology either.
        b"*0\r\n",
    ] {
        let cluster = Cluster::start(1);

        cluster.script("CLUSTER", reply, 1);

        let message = protocol_error(Client::connect_with(cluster.options()));

        assert_eq!(message, "invalid CLUSTER SLOTS topology");
    }
}

#[test]
fn a_disabled_cluster_error_makes_a_standalone_client() {
    let cluster = Cluster::start(1);

    cluster.script(
        "CLUSTER",
        b"-ERR This instance has cluster support disabled\r\n",
        1,
    );

    let mut client = Client::connect_with(cluster.options()).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    assert!(!client.is_cluster());
    assert_eq!(client.shard_count(), 1);
    assert_eq!(client.ping().unwrap(), "PONG");
}

#[test]
fn select_is_refused_when_the_server_is_a_cluster() {
    let cluster = Cluster::start(2);
    let options = ClientOptions {
        database: 2,
        ..cluster.options()
    };

    assert_eq!(
        protocol_error(Client::connect_with(options)),
        "SELECT is not supported in cluster mode"
    );
}

// ---------------------------------------------------------------------------------------------
// Multi-key splitting
// ---------------------------------------------------------------------------------------------

#[test]
fn mset_mget_del_exists_touch_unlink_split_across_shards() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let (a, b, c) = (
        cluster.key_on(0, "a"),
        cluster.key_on(1, "b"),
        cluster.key_on(2, "c"),
    );
    let missing = cluster.key_on(2, "missing");

    assert_eq!(
        client
            .execute(&["MSET", &a, "1", &b, "2", &c, "3"])
            .unwrap(),
        RespValue::Simple("OK".into())
    );
    assert_eq!(cluster.peek(0, &a), Some(Val::Str("1".into())));
    assert_eq!(cluster.peek(1, &b), Some(Val::Str("2".into())));
    assert_eq!(cluster.peek(2, &c), Some(Val::Str("3".into())));

    assert_eq!(
        client.execute(&["MGET", &c, &a, &missing, &b]).unwrap(),
        RespValue::Array(vec![text("3"), text("1"), RespValue::Null, text("2")])
    );
    assert_eq!(
        client.execute(&["EXISTS", &a, &a, &b]).unwrap(),
        RespValue::Integer(3)
    );
    assert_eq!(
        client.execute(&["TOUCH", &a, &b, &c, &missing]).unwrap(),
        RespValue::Integer(3)
    );
    assert_eq!(
        client.execute(&["UNLINK", &a]).unwrap(),
        RespValue::Integer(1)
    );
    assert_eq!(
        client.execute(&["DEL", &b, &c, &missing]).unwrap(),
        RespValue::Integer(2)
    );
    assert_eq!(client.del(&[&a, &b]).unwrap(), 0);
}

#[test]
fn keys_sharing_a_hash_tag_are_sent_as_one_command() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();

    client.execute(&["MSET", "{t}x", "1", "{t}y", "2"]).unwrap();
    cluster.clear_log();

    assert_eq!(
        client.execute(&["MGET", "{t}x", "{t}y"]).unwrap(),
        RespValue::Array(vec![text("1"), text("2")])
    );
    assert_eq!(cluster.seen("MGET").len(), 1);
}

#[test]
fn mget_keeps_null_for_a_part_whose_reply_is_not_an_array() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let (a, b) = (cluster.key_on(0, "a"), cluster.key_on(1, "b"));

    cluster.script("MGET", b":5\r\n", 2);

    assert_eq!(
        client.execute(&["MGET", &a, &b]).unwrap(),
        RespValue::Array(vec![RespValue::Null, RespValue::Null])
    );
}

#[test]
fn an_error_from_one_part_of_a_split_command_is_returned() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let (a, b) = (cluster.key_on(0, "a"), cluster.key_on(1, "b"));

    cluster.script("DEL", b"-ERR boom\r\n", 1);

    assert_eq!(server_error(client.execute(&["DEL", &a, &b])), "ERR boom");
}

#[test]
fn a_moved_reply_to_one_part_of_a_split_command_is_followed() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let (a, b) = (cluster.key_on(0, "a"), cluster.key_on(1, "b"));
    let slot = hash_slot(&b);

    cluster.put(0, &a, Val::Str("1".into()), -1);
    cluster.put(2, &b, Val::Str("2".into()), -1);
    cluster.move_slots(slot, slot, 2);

    assert_eq!(
        client.execute(&["DEL", &a, &b]).unwrap(),
        RespValue::Integer(2)
    );
    assert_eq!(cluster.peek(2, &b), None);
}

// ---------------------------------------------------------------------------------------------
// Every-node commands
// ---------------------------------------------------------------------------------------------

#[test]
fn dbsize_adds_up_and_flush_reaches_every_shard() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();

    for (node, count) in [(0, 2), (1, 3), (2, 4)] {
        for index in 0..count {
            cluster.put(node, &format!("k{node}-{index}"), Val::Str("v".into()), -1);
        }
    }

    assert_eq!(client.execute(&["DBSIZE"]).unwrap(), RespValue::Integer(9));
    assert_eq!(client.dbsize().unwrap(), 9);
    assert_eq!(
        client.execute(&["FLUSHDB"]).unwrap(),
        RespValue::Simple("OK".into())
    );
    assert_eq!(client.dbsize().unwrap(), 0);

    cluster.put(1, "left", Val::Str("v".into()), -1);
    client.execute(&["FLUSHALL"]).unwrap();
    assert_eq!(cluster.peek(1, "left"), None);
}

#[test]
fn an_error_from_one_shard_stops_an_every_node_command() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();

    cluster.script("1:DBSIZE", b"-ERR busy\r\n", 1);
    cluster.clear_log();

    assert_eq!(server_error(client.execute(&["DBSIZE"])), "ERR busy");
    // Node 2 is never asked once node 1 fails.
    assert_eq!(cluster.seen("DBSIZE").len(), 2);
}

#[test]
fn script_and_function_admin_commands_spread_only_when_they_change_state() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();

    cluster.clear_log();
    client.execute(&["script", "flush"]).unwrap();
    assert_eq!(cluster.seen("SCRIPT").len(), 3);

    cluster.clear_log();
    client.execute(&["SCRIPT", "LOAD", "return 1"]).unwrap();
    assert_eq!(cluster.seen("SCRIPT").len(), 3);

    cluster.clear_log();
    client.execute(&["SCRIPT", "EXISTS", "abc"]).unwrap();
    assert_eq!(
        cluster.seen("SCRIPT"),
        vec![(0, vec!["SCRIPT".into(), "EXISTS".into(), "abc".into()])]
    );

    cluster.clear_log();
    client.execute(&["FUNCTION", "DELETE", "lib"]).unwrap();
    client.execute(&["FUNCTION", "LOAD", "code"]).unwrap();
    client.execute(&["FUNCTION", "LIST"]).unwrap();
    // DELETE spread to three nodes, LOAD to three, LIST to the seed only.
    assert_eq!(cluster.seen("FUNCTION").len(), 7);
}

#[test]
fn publish_goes_to_the_owner_of_slot_zero() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();

    cluster.clear_log();
    client.execute(&["PUBLISH", "chan", "hi"]).unwrap();

    assert_eq!(cluster.seen("PUBLISH")[0].0, cluster.owner(0));
}

#[test]
fn keyless_commands_go_to_the_seed() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();

    cluster.clear_log();
    assert_eq!(client.ping().unwrap(), "PONG");
    client.execute(&["INFO"]).unwrap();
    client.execute(&["TIME"]).unwrap();

    assert!(cluster.log().iter().all(|(node, _)| *node == 0));
}

// ---------------------------------------------------------------------------------------------
// SCAN
// ---------------------------------------------------------------------------------------------

#[test]
fn scan_walks_every_shard_with_a_combined_cursor() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let mut expected = Vec::new();

    for node in 0..3 {
        for index in 0..5 {
            let key = format!("s{node}-{index}");

            cluster.put(node, &key, Val::Str("v".into()), -1);
            expected.push(key);
        }
    }

    let mut seen = Vec::new();
    let mut cursor = "0".to_owned();

    loop {
        let reply = client.execute(&["SCAN", &cursor, "COUNT", "2"]).unwrap();
        let items = reply.as_array().unwrap();

        cursor = items[0].as_string().unwrap().unwrap();
        for key in items[1].as_array().unwrap() {
            seen.push(key.as_string().unwrap().unwrap());
        }

        if cursor == "0" {
            break;
        }
    }

    seen.sort();
    expected.sort();
    assert_eq!(seen, expected);
}

#[test]
fn scan_cursor_selects_the_shard_and_forwards_the_rest() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();

    cluster.clear_log();
    // Cursor 4 is shard 4 % 3 = 1, with server cursor 4 / 3 = 1.
    client.execute(&["SCAN", "4", "MATCH", "x*"]).unwrap();

    assert_eq!(
        cluster.seen("SCAN"),
        vec![(
            1,
            vec!["SCAN".into(), "1".into(), "MATCH".into(), "x*".into()]
        )]
    );
}

#[test]
fn scan_rejects_a_missing_or_unparseable_cursor() {
    let cluster = Cluster::start(2);
    let mut client = cluster.client();

    assert_eq!(
        protocol_error(client.execute(&["SCAN"])),
        "ERR invalid cursor"
    );
    assert_eq!(
        protocol_error(client.execute(&["SCAN", "abc"])),
        "ERR invalid cursor"
    );
    assert_eq!(
        protocol_error(client.execute(&["SCAN", "-1"])),
        "ERR invalid cursor"
    );
}

#[test]
fn scan_passes_through_replies_it_cannot_rewrite() {
    let cluster = Cluster::start(2);
    let mut client = cluster.client();

    cluster.script("SCAN", b"+ODD\r\n", 1);
    assert_eq!(
        client.execute(&["SCAN", "0"]).unwrap(),
        RespValue::Simple("ODD".into())
    );

    cluster.script("SCAN", b"*1\r\n$1\r\n0\r\n", 1);
    assert_eq!(
        client.execute(&["SCAN", "0"]).unwrap(),
        RespValue::Array(vec![text("0")])
    );
}

#[test]
fn scan_survives_odd_server_cursors_and_reports_overflow() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();

    // A non-numeric or null server cursor counts as finished: move on to shard 1.
    cluster.script("SCAN", b"*2\r\n$3\r\nabc\r\n*0\r\n", 1);
    assert_eq!(
        client.execute(&["SCAN", "0"]).unwrap(),
        RespValue::Array(vec![text("1"), RespValue::Array(vec![])])
    );
    cluster.script("SCAN", b"*2\r\n$-1\r\n*0\r\n", 1);
    assert_eq!(
        client.execute(&["SCAN", "1"]).unwrap(),
        RespValue::Array(vec![text("2"), RespValue::Array(vec![])])
    );
    // The last shard finishing wraps to 0.
    cluster.script("SCAN", b"*2\r\n$1\r\n0\r\n*0\r\n", 1);
    assert_eq!(
        client.execute(&["SCAN", "2"]).unwrap(),
        RespValue::Array(vec![text("0"), RespValue::Array(vec![])])
    );
    // A server cursor too large to fold into the combined cursor.
    cluster.script("SCAN", b"*2\r\n$20\r\n18446744073709551615\r\n*0\r\n", 1);
    assert_eq!(
        protocol_error(client.execute(&["SCAN", "0"])),
        "scan cursor overflow"
    );
}

// ---------------------------------------------------------------------------------------------
// MOVED handling
// ---------------------------------------------------------------------------------------------

#[test]
fn a_moved_to_a_known_node_points_that_slot_without_reloading_the_map() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let key = cluster.key_on(1, "mv");
    let slot = hash_slot(&key);
    let neighbour = (0..)
        .map(|i| format!("nb{i}"))
        .find(|k| cluster.owner(hash_slot(k)) == 1 && hash_slot(k) != slot)
        .unwrap();

    cluster.put(2, &key, Val::Str("new-home".into()), -1);
    cluster.move_slots(slot, slot, 2);
    cluster.clear_log();

    assert_eq!(
        client.get_string(&key).unwrap().as_deref(),
        Some("new-home")
    );
    assert_eq!(cluster.seen("GET").len(), 2);
    assert!(cluster.seen("CLUSTER").is_empty());

    // That slot now goes straight to the new owner; the rest of the map is unchanged.
    cluster.clear_log();
    assert_eq!(
        client.get_string(&key).unwrap().as_deref(),
        Some("new-home")
    );
    client.get(&neighbour).unwrap();
    assert_eq!(
        cluster.seen("GET"),
        vec![
            (2, vec!["GET".into(), key]),
            (1, vec!["GET".into(), neighbour])
        ]
    );
}

#[test]
fn a_moved_to_an_unknown_node_reloads_the_whole_map() {
    let cluster = Cluster::start(3);

    // The client starts with a map in which node 2 owns nothing.
    cluster.move_slots(10_923, 16_383, 1);

    let mut client = cluster.client();

    assert_eq!(client.shard_count(), 2);

    let key = cluster.key_on(1, "far");
    let other = (0..)
        .map(|i| format!("other{i}"))
        .find(|k| cluster.owner(hash_slot(k)) == 1 && hash_slot(k) != hash_slot(&key))
        .unwrap();

    for name in [&key, &other] {
        let slot = hash_slot(name);

        cluster.put(2, name, Val::Str("v".into()), -1);
        cluster.move_slots(slot, slot, 2);
    }

    cluster.clear_log();
    assert_eq!(client.get_string(&key).unwrap().as_deref(), Some("v"));
    assert_eq!(cluster.seen("CLUSTER").len(), 1);
    assert_eq!(client.shard_count(), 3);

    // The reloaded map already knows the other moved slot.
    cluster.clear_log();
    assert_eq!(client.get_string(&other).unwrap().as_deref(), Some("v"));
    assert_eq!(cluster.seen("GET"), vec![(2, vec!["GET".into(), other])]);
}

#[test]
fn a_second_moved_for_the_same_command_reloads_the_whole_map() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let key = cluster.key_on(2, "twice");
    let moved = format!(
        "-MOVED {} 127.0.0.1:{}\r\n",
        hash_slot(&key),
        cluster.port(2)
    );

    cluster.put(2, &key, Val::Str("v".into()), -1);
    cluster.script("GET", moved.as_bytes(), 1);
    cluster.clear_log();
    // One MOVED: that slot only.
    assert_eq!(client.get_string(&key).unwrap().as_deref(), Some("v"));
    assert!(cluster.seen("CLUSTER").is_empty());

    cluster.script("GET", moved.as_bytes(), 2);
    cluster.clear_log();
    // The second MOVED of one command reloads the map once.
    assert_eq!(client.get_string(&key).unwrap().as_deref(), Some("v"));
    assert_eq!(cluster.seen("GET").len(), 3);
    assert_eq!(cluster.seen("CLUSTER").len(), 1);
}

#[test]
fn after_a_reload_the_slot_still_points_at_the_node_the_moved_named() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let key = cluster.key_on(2, "named");
    let slot = hash_slot(&key);
    // The map keeps saying node 2; the redirects name node 1.
    let moved = format!("-MOVED {slot} 127.0.0.1:{}\r\n", cluster.port(1));

    // Node 1 answers the key as if it owned it, while CLUSTER SLOTS keeps naming node 2.
    cluster.script("2:GET", moved.as_bytes(), 1);
    cluster.script("1:GET", moved.as_bytes(), 1);
    cluster.script("1:GET", &bulk("on-one"), 2);
    cluster.clear_log();

    assert_eq!(client.get_string(&key).unwrap().as_deref(), Some("on-one"));
    assert_eq!(cluster.seen("CLUSTER").len(), 1);
    // Node 2 first, then node 1 twice: the retry after the reload still went to node 1.
    let nodes: Vec<usize> = cluster.seen("GET").into_iter().map(|(n, _)| n).collect();

    assert_eq!(nodes, [2, 1, 1]);

    // The slot keeps pointing at node 1 for the next command.
    cluster.clear_log();
    assert_eq!(client.get_string(&key).unwrap().as_deref(), Some("on-one"));
    assert_eq!(cluster.seen("GET"), vec![(1, vec!["GET".into(), key])]);
}

#[test]
fn a_moved_to_a_node_missing_from_the_reloaded_map_is_still_followed() {
    let cluster = Cluster::start(3);

    // Node 2 owns nothing, before and after the reload.
    cluster.move_slots(10_923, 16_383, 1);

    let mut client = cluster.client();
    let key = cluster.key_on(1, "ghost");
    let moved = format!(
        "-MOVED {} 127.0.0.1:{}\r\n",
        hash_slot(&key),
        cluster.port(2)
    );

    // Node 2 answers the key although CLUSTER SLOTS never lists it.
    cluster.script("1:GET", moved.as_bytes(), 1);
    cluster.script("2:GET", &bulk("v"), 2);
    cluster.clear_log();

    assert_eq!(client.get_string(&key).unwrap().as_deref(), Some("v"));
    assert_eq!(cluster.seen("CLUSTER").len(), 1);

    cluster.clear_log();
    assert_eq!(client.get_string(&key).unwrap().as_deref(), Some("v"));
    assert_eq!(cluster.seen("GET"), vec![(2, vec!["GET".into(), key])]);
}

#[test]
fn up_to_three_redirects_are_followed_and_the_fourth_is_returned() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let port = cluster.port(2);
    let moved = format!("-MOVED 1 127.0.0.1:{port}\r\n");

    let key = cluster.key_on(2, "rd");

    cluster.put(2, &key, Val::Str("v".into()), -1);
    cluster.script("GET", moved.as_bytes(), 3);
    cluster.clear_log();
    // Three scripted MOVED replies, then the real answer: the loop allows exactly that.
    assert_eq!(client.get_string(&key).unwrap().as_deref(), Some("v"));
    assert_eq!(cluster.seen("GET").len(), 4);

    cluster.script("GET", moved.as_bytes(), 4);
    cluster.clear_log();
    let message = server_error(client.execute(&["GET", &key]));

    assert!(message.starts_with("MOVED 1 127.0.0.1:"), "{message}");
    assert_eq!(cluster.seen("GET").len(), 4);
    // The first redirect points the slot; the second and third reload the map.
    assert_eq!(cluster.seen("CLUSTER").len(), 2);
}

#[test]
fn an_ask_sends_asking_and_the_command_to_the_named_node_and_keeps_the_map() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let key = cluster.key_on(0, "ask");
    let ask = format!("-ASK {} 127.0.0.1:{}\r\n", hash_slot(&key), cluster.port(1));

    cluster.put(0, &key, Val::Str("on-a".into()), -1);
    cluster.script("0:GET", ask.as_bytes(), 1);
    // Node 1 does not own the slot; the scripted reply stands for the migrating import.
    cluster.script("1:GET", &bulk("on-b"), 1);
    cluster.clear_log();

    assert_eq!(client.get_string(&key).unwrap().as_deref(), Some("on-b"));
    assert!(cluster.seen("CLUSTER").is_empty());

    let on_b: Vec<Vec<String>> = cluster
        .log()
        .into_iter()
        .filter(|(node, _)| *node == 1)
        .map(|(_, command)| command)
        .collect();

    assert_eq!(
        on_b,
        vec![vec!["ASKING".to_owned()], vec!["GET".into(), key.clone()]]
    );

    // The slot still belongs to node 0.
    cluster.clear_log();
    assert_eq!(client.get_string(&key).unwrap().as_deref(), Some("on-a"));
    assert_eq!(cluster.seen("GET"), vec![(0, vec!["GET".into(), key])]);
    assert!(cluster.seen("ASKING").is_empty());
}

#[test]
fn an_ask_to_a_node_the_client_does_not_know_yet_is_followed() {
    let cluster = Cluster::start(3);

    // Node 2 owns nothing, so the map never lists it.
    cluster.move_slots(10_923, 16_383, 1);

    let mut client = cluster.client();
    let key = cluster.key_on(1, "ghostask");
    let ask = format!("-ASK {} 127.0.0.1:{}\r\n", hash_slot(&key), cluster.port(2));

    cluster.put(1, &key, Val::Str("on-owner".into()), -1);
    cluster.script("1:GET", ask.as_bytes(), 1);
    cluster.script("2:GET", &bulk("on-importer"), 1);
    cluster.clear_log();

    assert_eq!(
        client.get_string(&key).unwrap().as_deref(),
        Some("on-importer")
    );
    assert_eq!(cluster.seen("ASKING"), vec![(2, vec!["ASKING".to_owned()])]);

    cluster.clear_log();
    assert_eq!(
        client.get_string(&key).unwrap().as_deref(),
        Some("on-owner")
    );
    assert_eq!(cluster.seen("GET"), vec![(1, vec!["GET".into(), key])]);
}

#[test]
fn a_tryagain_is_retried_after_a_short_sleep() {
    let cluster = Cluster::start(2);
    let mut client = cluster.client();
    let key = cluster.key_on(1, "retry");

    cluster.put(1, &key, Val::Str("v".into()), -1);
    cluster.script("GET", b"-TRYAGAIN slot is being handed over\r\n", 1);
    cluster.clear_log();

    let started = Instant::now();

    assert_eq!(client.get_string(&key).unwrap().as_deref(), Some("v"));
    assert!(started.elapsed() >= Duration::from_millis(5));
    assert_eq!(
        cluster.seen("GET"),
        vec![(1, vec!["GET".into(), key.clone()]); 2]
    );
}

#[test]
fn the_eighth_tryagain_in_a_row_is_returned() {
    let cluster = Cluster::start(2);
    let mut client = cluster.client();
    let key = cluster.key_on(1, "giveup");

    cluster.script("GET", b"-TRYAGAIN slot is being handed over\r\n", 8);
    cluster.clear_log();

    let started = Instant::now();
    let message = server_error(client.execute(&["GET", &key]));

    assert!(message.starts_with("TRYAGAIN"), "{message}");
    // Sleeps of 5, 10, 20, 40, 80, 160 and 320 ms came before the seven retries.
    assert!(started.elapsed() >= Duration::from_millis(635));
    assert_eq!(cluster.seen("GET").len(), 8);
}

#[test]
fn a_pipeline_of_refused_commands_waits_for_one_tryagain_budget() {
    let cluster = Cluster::start(2);
    let mut client = cluster.client();
    let key = cluster.key_on(1, "storm");
    let commands: Vec<Vec<&str>> = (0..100).map(|_| vec!["GET", key.as_str()]).collect();
    let borrowed: Vec<&[&str]> = commands.iter().map(Vec::as_slice).collect();

    // The slot stays frozen: every send of any command is refused.
    cluster.script("GET", b"-TRYAGAIN slot is being handed over\r\n", 1_000);

    let started = Instant::now();
    let replies = client.execute_many(&borrowed).unwrap();
    let elapsed = started.elapsed();

    assert_eq!(replies.len(), 100);
    for reply in &replies {
        let RespValue::Error(text) = reply else {
            panic!("expected TRYAGAIN, got {reply:?}");
        };

        assert!(text.starts_with("TRYAGAIN"), "{text}");
    }
    // One budget (635 ms), not 100 of them; the bound leaves room for a slow machine.
    assert!(elapsed >= Duration::from_millis(635), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(5), "{elapsed:?}");
}

#[test]
fn ask_and_moved_share_one_budget_of_three_redirects() {
    let cluster = Cluster::start(2);
    let mut client = cluster.client();
    let key = cluster.key_on(0, "budget");
    let slot = hash_slot(&key);
    let port = cluster.port(1);

    cluster.script(
        "GET",
        format!("-ASK {slot} 127.0.0.1:{port}\r\n").as_bytes(),
        1,
    );
    cluster.script(
        "GET",
        format!("-MOVED {slot} 127.0.0.1:{port}\r\n").as_bytes(),
        1,
    );
    cluster.script(
        "GET",
        format!("-ASK {slot} 127.0.0.1:{port}\r\n").as_bytes(),
        2,
    );
    cluster.clear_log();

    let message = server_error(client.execute(&["GET", &key]));

    assert!(message.starts_with("ASK "), "{message}");
    assert_eq!(cluster.seen("GET").len(), 4);
    assert_eq!(cluster.seen("ASKING").len(), 2);
}

#[test]
fn malformed_or_foreign_redirects_are_returned_as_errors() {
    let cluster = Cluster::start(2);
    let mut client = cluster.client();

    for reply in [
        &b"-MOVED 5 hostwithoutport\r\n"[..],
        b"-MOVED\r\n",
        b"-MOVED 5 host:notaport\r\n",
        b"-ASK 5 hostwithoutport\r\n",
        b"-ASK 16384 127.0.0.1:1\r\n",
        b"-ERR something else\r\n",
    ] {
        cluster.script("GET", reply, 1);
        cluster.clear_log();

        let message = server_error(client.execute(&["GET", "k"]));

        assert_eq!(message, String::from_utf8_lossy(&reply[1..reply.len() - 2]));
        assert_eq!(cluster.seen("GET").len(), 1, "no retry for {message}");
    }
}

#[test]
fn moved_to_an_unreachable_node_is_an_io_error() {
    let cluster = Cluster::start(2);
    let mut client = cluster.client();
    let dead = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let dead_port = dead.local_addr().unwrap().port();

    drop(dead);
    cluster.script(
        "GET",
        format!("-MOVED 1 127.0.0.1:{dead_port}\r\n").as_bytes(),
        1,
    );

    assert!(matches!(client.execute(&["GET", "k"]), Err(Error::Io(_))));
}

// ---------------------------------------------------------------------------------------------
// Binary-safe commands
// ---------------------------------------------------------------------------------------------

#[test]
fn execute_binary_routes_by_key_and_sends_keyless_commands_to_the_seed() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let key = cluster.key_on(2, "bin");

    cluster.clear_log();
    let set: [&[u8]; 3] = [b"SET", key.as_bytes(), b"v"];

    client.execute_binary(&set).unwrap();
    assert_eq!(cluster.peek(2, &key), Some(Val::Str("v".into())));

    // Keyless: INFO has a second argument that is not a key.
    client.execute_binary(&[b"INFO", b"server"]).unwrap();
    client.execute_binary(&[b"PING".as_slice()]).unwrap();
    assert_eq!(cluster.seen("INFO")[0].0, 0);
    assert_eq!(cluster.seen("PING")[0].0, 0);
}

#[test]
fn execute_binary_follows_moved_and_stops_after_three_redirects() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let key = cluster.key_on(0, "binmv");
    let slot = hash_slot(&key);

    cluster.put(1, &key, Val::Str("there".into()), -1);
    cluster.move_slots(slot, slot, 1);

    let get: [&[u8]; 2] = [b"GET", key.as_bytes()];

    assert_eq!(client.execute_binary(&get).unwrap(), text("there"));

    let moved = format!("-MOVED {slot} 127.0.0.1:{}\r\n", cluster.port(1));

    cluster.script("GET", moved.as_bytes(), 4);
    cluster.clear_log();
    let result = client.execute_binary(&get);

    assert!(
        matches!(result, Err(Error::Server(ref m)) if m.starts_with("MOVED")),
        "{result:?}"
    );
    assert_eq!(cluster.seen("GET").len(), 4);
}

#[test]
fn execute_binary_rejects_an_empty_command() {
    let cluster = Cluster::start(2);
    let mut client = cluster.client();

    assert!(
        protocol_error(client.execute_binary(&[] as &[&[u8]])).contains("at least one argument")
    );
    // The socket is still usable.
    assert_eq!(client.ping().unwrap(), "PONG");
}

// ---------------------------------------------------------------------------------------------
// Pipelines
// ---------------------------------------------------------------------------------------------

#[test]
fn a_pipeline_is_grouped_by_shard_and_replies_keep_command_order() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let (a, b, c) = (
        cluster.key_on(0, "a"),
        cluster.key_on(1, "b"),
        cluster.key_on(2, "c"),
    );

    cluster.clear_log();
    let replies = many(
        &mut client,
        vec![
            vec!["SET", a.as_str(), "1"],
            vec!["SET", b.as_str(), "2"],
            vec!["SET", c.as_str(), "3"],
            vec!["GET", c.as_str()],
            vec!["PING"],
            vec!["GET", a.as_str()],
            vec!["GET", "no-such"],
        ],
    )
    .unwrap();

    assert_eq!(
        replies,
        vec![
            RespValue::Simple("OK".into()),
            RespValue::Simple("OK".into()),
            RespValue::Simple("OK".into()),
            text("3"),
            RespValue::Simple("PONG".into()),
            text("1"),
            RespValue::Null,
        ]
    );
    assert!(client.execute_many(&[]).is_err());
    let _ = &b;
}

#[test]
fn a_node_dropping_inside_a_pipeline_leaves_the_other_nodes_in_step() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let (a, b, c) = (
        cluster.key_on(0, "a"),
        cluster.key_on(1, "b"),
        cluster.key_on(2, "c"),
    );

    cluster.hang_up("1:SET", 1);
    // Every node is written before any is read; the first node read fails, and the others'
    // replies are still read so their sockets do not answer the next command with this one's.
    assert!(matches!(
        many(
            &mut client,
            vec![
                vec!["SET", b.as_str(), "2"],
                vec!["SET", a.as_str(), "1"],
                vec!["SET", c.as_str(), "3"],
            ],
        ),
        Err(Error::Io(_))
    ));
    assert_eq!(
        many(
            &mut client,
            vec![
                vec!["GET", a.as_str()],
                vec!["GET", c.as_str()],
                vec!["SET", b.as_str(), "4"],
                vec!["GET", b.as_str()],
            ],
        )
        .unwrap(),
        vec![
            text("1"),
            text("3"),
            RespValue::Simple("OK".into()),
            text("4"),
        ]
    );
}

#[test]
fn a_pipeline_follows_moved_for_a_single_command() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let (a, b) = (cluster.key_on(0, "a"), cluster.key_on(1, "b"));
    let slot = hash_slot(&b);

    cluster.put(0, &a, Val::Str("1".into()), -1);
    cluster.put(2, &b, Val::Str("2".into()), -1);
    cluster.move_slots(slot, slot, 2);

    assert_eq!(
        many(
            &mut client,
            vec![vec!["GET", a.as_str()], vec!["GET", b.as_str()]]
        )
        .unwrap(),
        vec![text("1"), text("2")]
    );
}

#[test]
fn a_pipeline_splits_cross_slot_split_commands_like_the_plain_command() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let (a, b, c) = (
        cluster.key_on(0, "a"),
        cluster.key_on(1, "b"),
        cluster.key_on(2, "c"),
    );
    let missing = cluster.key_on(2, "missing");

    cluster.clear_log();
    let replies = many(
        &mut client,
        vec![
            vec!["SET", a.as_str(), "1"],
            vec!["MSET", b.as_str(), "2", c.as_str(), "3"],
            vec!["MGET", c.as_str(), a.as_str(), missing.as_str(), b.as_str()],
            vec!["DEL", a.as_str(), b.as_str()],
            vec!["EXISTS", a.as_str(), c.as_str(), c.as_str()],
            vec!["TOUCH", c.as_str(), missing.as_str()],
            vec!["UNLINK", c.as_str(), a.as_str()],
            vec!["GET", c.as_str()],
            vec!["MSET", a.as_str(), "1", b.as_str()],
        ],
    )
    .unwrap();

    assert_eq!(
        replies,
        vec![
            RespValue::Simple("OK".into()),
            RespValue::Simple("OK".into()),
            RespValue::Array(vec![text("3"), text("1"), RespValue::Null, text("2")]),
            RespValue::Integer(2),
            // Each node runs its commands in pipeline order: the DEL came first.
            RespValue::Integer(2),
            RespValue::Integer(1),
            RespValue::Integer(1),
            RespValue::Null,
            RespValue::Error("ERR wrong number of arguments for 'mset' command".into()),
        ]
    );

    // Every command that reached a node carries keys of one slot only.
    assert!(!cluster.log().is_empty());
    for (_, command) in cluster.log() {
        let keys: Vec<&String> = match command[0].as_str() {
            "MSET" => command[1..].iter().step_by(2).collect(),
            "SET" | "GET" => command[1..2].iter().collect(),
            _ => command[1..].iter().collect(),
        };

        assert!(
            keys.iter().all(|key| hash_slot(key) == hash_slot(keys[0])),
            "{command:?}"
        );
    }

    assert!(cluster.seen("MGET").len() >= 3);
}

#[test]
fn a_pipeline_refuses_other_cross_slot_commands_at_their_index_only() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let (a, c) = (cluster.key_on(0, "a"), cluster.key_on(2, "c"));

    cluster.clear_log();
    let replies = many(
        &mut client,
        vec![
            vec!["SET", a.as_str(), "1"],
            vec!["COPY", a.as_str(), c.as_str()],
            vec!["RENAME", a.as_str(), c.as_str()],
            vec!["GET", a.as_str()],
        ],
    )
    .unwrap();

    assert_eq!(
        replies,
        vec![
            RespValue::Simple("OK".into()),
            RespValue::Error(CROSSSLOT.into()),
            RespValue::Error(CROSSSLOT.into()),
            text("1"),
        ]
    );
    assert!(cluster.seen("COPY").is_empty());
    assert!(cluster.seen("RENAME").is_empty());
}

#[test]
fn a_moved_part_of_a_split_command_in_a_pipeline_is_followed() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let (a, b) = (cluster.key_on(0, "a"), cluster.key_on(1, "b"));
    let slot = hash_slot(&b);

    cluster.put(0, &a, Val::Str("1".into()), -1);
    cluster.put(2, &b, Val::Str("2".into()), -1);
    cluster.move_slots(slot, slot, 2);

    assert_eq!(
        many(
            &mut client,
            vec![
                vec!["MGET", a.as_str(), b.as_str()],
                vec!["EXISTS", b.as_str(), a.as_str()]
            ]
        )
        .unwrap(),
        vec![
            RespValue::Array(vec![text("1"), text("2")]),
            RespValue::Integer(2)
        ]
    );
}

#[test]
fn a_pipeline_holding_a_transaction_goes_to_one_connection() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let key = cluster.key_on(2, "tx");

    cluster.clear_log();
    let replies = many(
        &mut client,
        vec![vec!["MULTI"], vec!["SET", key.as_str(), "v"], vec!["EXEC"]],
    )
    .unwrap();

    assert_eq!(
        replies,
        vec![
            RespValue::Simple("OK".into()),
            RespValue::Simple("QUEUED".into()),
            RespValue::Array(vec![RespValue::Simple("OK".into())]),
        ]
    );
    assert!(cluster.log().iter().all(|(node, _)| *node == 2));
    assert_eq!(cluster.seen("MULTI")[0].0, 2);

    // No keyed command: the seed gets it.
    cluster.clear_log();
    many(&mut client, vec![vec!["MULTI"], vec!["EXEC"]]).unwrap();
    assert_eq!(cluster.seen("MULTI")[0].0, 0);
}

// ---------------------------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------------------------

#[test]
fn multi_exec_runs_on_the_node_that_owns_the_first_key() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let key = cluster.key_on(1, "t");

    cluster.clear_log();
    client.multi().unwrap();
    // MULTI alone sends nothing: it is held until a keyed command shows the shard.
    assert!(cluster.seen("MULTI").is_empty());
    assert_eq!(
        client.execute(&["SET", &key, "v"]).unwrap(),
        RespValue::Simple("QUEUED".into())
    );
    assert_eq!(
        client.execute(&["GET", &key]).unwrap(),
        RespValue::Simple("QUEUED".into())
    );
    assert_eq!(
        client.execute(&["EXEC"]).unwrap(),
        RespValue::Array(vec![RespValue::Simple("OK".into()), text("v")])
    );
    assert_eq!(cluster.seen("MULTI")[0].0, 1);

    // The transaction is over: a plain command routes normally again.
    assert_eq!(client.get_string(&key).unwrap().as_deref(), Some("v"));
}

#[test]
fn keyless_commands_in_a_transaction_are_held_back_and_sent_with_it() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let key = cluster.key_on(2, "t");

    client.execute(&["MULTI"]).unwrap();
    cluster.clear_log();
    assert_eq!(
        client.execute(&["ECHO", "x"]).unwrap(),
        RespValue::Simple("QUEUED".into())
    );
    assert!(cluster.log().is_empty());
    client.execute(&["SET", &key, "v"]).unwrap();

    let names: Vec<String> = cluster
        .log()
        .into_iter()
        .map(|(_, cmd)| cmd[0].clone())
        .collect();

    assert_eq!(names, ["MULTI", "ECHO", "SET"]);
    assert_eq!(
        client.execute(&["EXEC"]).unwrap(),
        RespValue::Array(vec![text("x"), RespValue::Simple("OK".into())])
    );
}

#[test]
fn exec_with_only_keyless_commands_runs_on_the_seed() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();

    client.execute(&["MULTI"]).unwrap();
    client.execute(&["ECHO", "x"]).unwrap();
    cluster.clear_log();

    assert_eq!(
        client.execute(&["EXEC"]).unwrap(),
        RespValue::Array(vec![text("x")])
    );
    assert!(cluster.log().iter().all(|(node, _)| *node == 0));

    // An empty transaction is fine too.
    client.execute(&["MULTI"]).unwrap();
    assert_eq!(client.execute(&["EXEC"]).unwrap(), RespValue::Array(vec![]));
}

#[test]
fn transaction_misuse_is_reported_without_touching_the_server() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();

    client.execute(&["MULTI"]).unwrap();
    cluster.clear_log();
    assert_eq!(
        server_error(client.execute(&["MULTI"])),
        "ERR MULTI calls can not be nested"
    );
    assert_eq!(
        server_error(client.execute(&["WATCH", "k"])),
        "ERR WATCH inside MULTI is not allowed"
    );
    assert!(cluster.log().is_empty());

    // DISCARD before any keyed command only drops the held state.
    client.execute(&["ECHO", "x"]).unwrap();
    assert_eq!(
        client.execute(&["DISCARD"]).unwrap(),
        RespValue::Simple("OK".into())
    );
    assert!(cluster.log().is_empty());
    assert_eq!(client.ping().unwrap(), "PONG");
}

#[test]
fn discard_after_the_transaction_started_is_forwarded_and_unpins() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let key = cluster.key_on(1, "t");

    client.execute(&["MULTI"]).unwrap();
    client.execute(&["SET", &key, "v"]).unwrap();
    assert_eq!(
        client.execute(&["DISCARD"]).unwrap(),
        RespValue::Simple("OK".into())
    );
    assert_eq!(cluster.peek(1, &key), None);

    cluster.clear_log();
    client.ping().unwrap();
    assert_eq!(cluster.seen("PING")[0].0, 0);
}

#[test]
fn a_failing_multi_on_the_shard_is_returned_and_clears_the_queue() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let key = cluster.key_on(1, "t");

    client.execute(&["MULTI"]).unwrap();
    cluster.script("MULTI", b"-ERR no multi today\r\n", 1);
    assert_eq!(
        server_error(client.execute(&["SET", &key, "v"])),
        "ERR no multi today"
    );
    // The router is back to normal: the next command is routed, not queued.
    client.set(&key, "again").unwrap();
    assert_eq!(cluster.peek(1, &key), Some(Val::Str("again".into())));
}

#[test]
fn watch_pins_the_node_and_exec_releases_it() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let (a, b) = (cluster.key_on(1, "w"), cluster.key_on(1, "x"));

    client.watch(&[&a, &b]).unwrap();
    assert_eq!(cluster.seen("WATCH")[0].0, 1);

    cluster.clear_log();
    client.execute(&["MULTI"]).unwrap();
    assert_eq!(
        client.execute(&["SET", &a, "v"]).unwrap(),
        RespValue::Simple("QUEUED".into())
    );
    assert_eq!(
        client.execute(&["EXEC"]).unwrap(),
        RespValue::Array(vec![RespValue::Simple("OK".into())])
    );
    // Everything while pinned went to the watched node.
    assert!(cluster.log().iter().all(|(node, _)| *node == 1));

    cluster.clear_log();
    client.ping().unwrap();
    assert_eq!(cluster.seen("PING")[0].0, 0);
}

#[test]
fn unwatch_releases_a_pin_but_not_inside_multi() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let key = cluster.key_on(2, "w");

    client.watch(&[&key]).unwrap();
    client.execute(&["UNWATCH"]).unwrap();
    cluster.clear_log();
    client.ping().unwrap();
    assert_eq!(cluster.seen("PING")[0].0, 0);

    // UNWATCH queued inside a MULTI leaves the pin in place.
    client.watch(&[&key]).unwrap();
    client.execute(&["MULTI"]).unwrap();
    client.execute(&["UNWATCH"]).unwrap();
    cluster.clear_log();
    client.ping().unwrap();
    assert_eq!(cluster.seen("PING")[0].0, 2);
    client.execute(&["DISCARD"]).unwrap();
    cluster.clear_log();
    client.ping().unwrap();
    assert_eq!(cluster.seen("PING")[0].0, 0);
}

#[test]
fn a_failed_watch_or_multi_does_not_pin_or_enter_multi() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let key = cluster.key_on(1, "w");

    cluster.script("WATCH", b"-ERR cannot watch\r\n", 1);
    assert_eq!(
        server_error(client.execute(&["WATCH", &key])),
        "ERR cannot watch"
    );
    cluster.clear_log();
    client.ping().unwrap();
    assert_eq!(cluster.seen("PING")[0].0, 0);

    client.watch(&[&key]).unwrap();
    cluster.script("MULTI", b"-ERR nope\r\n", 1);
    assert_eq!(server_error(client.execute(&["MULTI"])), "ERR nope");
    // Not in MULTI, so UNWATCH still releases the pin.
    client.execute(&["UNWATCH"]).unwrap();
    cluster.clear_log();
    client.ping().unwrap();
    assert_eq!(cluster.seen("PING")[0].0, 0);
}

// ---------------------------------------------------------------------------------------------
// Cross-slot commands that are not split
// ---------------------------------------------------------------------------------------------

const CROSSSLOT: &str = "CROSSSLOT Keys in request do not hash to the same slot";

#[test]
fn rename_to_the_same_slot_is_a_plain_rename() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();

    cluster.put(
        cluster.owner(hash_slot("{s}a")),
        "{s}a",
        Val::Str("v".into()),
        -1,
    );
    cluster.clear_log();
    client.rename("{s}a", "{s}b").unwrap();
    assert_eq!(cluster.log().len(), 1);
    assert_eq!(cluster.seen("RENAME").len(), 1);
}

#[test]
fn rename_and_renamenx_across_slots_are_refused_before_sending() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let (src, dst) = (cluster.key_on(0, "src"), cluster.key_on(2, "dst"));

    cluster.put(0, &src, Val::Str("v".into()), -1);
    cluster.clear_log();

    assert!(matches!(client.rename(&src, &dst), Err(Error::Server(m)) if m == CROSSSLOT));
    assert!(matches!(client.rename_nx(&src, &dst), Err(Error::Server(m)) if m == CROSSSLOT));
    assert_eq!(
        server_error(client.execute(&["renamenx", &src, &dst])),
        CROSSSLOT
    );
    // Nothing reached any node and the source is untouched.
    assert!(cluster.log().is_empty(), "{:?}", cluster.log());
    assert_eq!(cluster.peek(0, &src), Some(Val::Str("v".into())));
    assert_eq!(cluster.peek(2, &dst), None);
}

#[test]
fn other_multi_key_commands_across_slots_are_refused_before_sending() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let (a, b) = (cluster.key_on(0, "a"), cluster.key_on(2, "b"));
    // Two slots on the same node are still two slots.
    let (x, y) = (cluster.key_on(1, "x"), cluster.key_on(1, "y"));

    assert_ne!(hash_slot(&x), hash_slot(&y));
    cluster.clear_log();

    for command in [
        vec!["COPY", &a, &b],
        vec!["LMOVE", &a, &b, "LEFT", "RIGHT"],
        vec!["SMOVE", &a, &b, "m"],
        vec!["SINTERSTORE", &a, &x],
        vec!["BITOP", "AND", &a, &a, &b],
        vec!["EVAL", "return 1", "2", &x, &y],
        vec!["SUNION", &x, &y],
    ] {
        assert_eq!(
            server_error(client.execute(&command)),
            CROSSSLOT,
            "{command:?}"
        );
    }

    assert!(cluster.log().is_empty(), "{:?}", cluster.log());

    // The same commands in one slot are sent, to the owner of that slot.
    let tagged = cluster.key_on(2, "{t}");
    let tag = &tagged[..3];
    let other = format!("{tag}other");

    client.execute(&["COPY", &tagged, &other]).unwrap();
    client
        .execute(&["EVAL", "return 1", "2", &tagged, &other])
        .unwrap();
    assert_eq!(cluster.seen("COPY")[0].0, cluster.owner(hash_slot(tag)));
    assert_eq!(cluster.seen("EVAL").len(), 1);
    // A command with no keys still goes to the seed.
    client.execute(&["EVAL", "return 1", "0"]).unwrap();
    assert_eq!(cluster.seen("EVAL").len(), 2);
}

// ---------------------------------------------------------------------------------------------
// Connection loss, reconnect and hooks
// ---------------------------------------------------------------------------------------------

#[test]
fn a_dropped_node_connection_is_reopened_by_the_next_command_and_hooks_fire() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let key = cluster.key_on(1, "drop");
    let lost = Arc::new(AtomicUsize::new(0));
    let restored = Arc::new(AtomicUsize::new(0));
    let notices = Arc::new(Mutex::new(Vec::new()));

    {
        let lost = Arc::clone(&lost);
        let notices = Arc::clone(&notices);

        client.on_connection_lost(move |notice| {
            lost.fetch_add(1, Ordering::SeqCst);
            notices.lock().unwrap().push(notice);
        });
    }
    {
        let restored = Arc::clone(&restored);

        client.on_connection_restored(move |_| {
            restored.fetch_add(1, Ordering::SeqCst);
        });
    }

    client.set(&key, "v").unwrap();
    cluster.hang_up("GET", 1);
    // The command that finds the drop fails; it is not replayed.
    assert!(matches!(client.get(&key), Err(Error::Io(_))));
    assert_eq!(lost.load(Ordering::SeqCst), 0);

    assert_eq!(client.get_string(&key).unwrap().as_deref(), Some("v"));
    assert_eq!(lost.load(Ordering::SeqCst), 1);
    assert_eq!(restored.load(Ordering::SeqCst), 1);

    let notices = notices.lock().unwrap();

    assert_eq!(notices[0].port, cluster.port(1));
    assert!(!notices[0].subscriber);
}

#[test]
fn with_reconnect_off_a_dropped_node_stays_closed_until_reconnect_is_called() {
    let cluster = Cluster::start(3);
    let mut client = Client::connect_with(ClientOptions {
        reconnect: false,
        ..cluster.options()
    })
    .unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let key = cluster.key_on(1, "drop");

    cluster.hang_up("GET", 1);
    assert!(matches!(client.get(&key), Err(Error::Io(_))));

    // Every try for that shard fails the same way and does not reach the server.
    cluster.clear_log();
    for _ in 0..2 {
        let result = client.get(&key);

        assert!(
            matches!(result, Err(Error::Io(ref e)) if e.kind() == std::io::ErrorKind::BrokenPipe),
            "{result:?}"
        );
    }
    assert!(cluster.seen("GET").is_empty());

    client.reconnect().unwrap();
    assert_eq!(client.get(&key).unwrap(), None);
}

#[test]
fn reconnect_on_a_healthy_cluster_client_is_a_no_op() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();

    client.reconnect().unwrap();
    // It opens the nodes it had not talked to yet.
    assert_eq!(client.ping().unwrap(), "PONG");
}

#[test]
fn a_custom_reconnect_delay_decides_how_often_a_dead_node_is_tried() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let calls = Arc::new(AtomicUsize::new(0));

    {
        let calls = Arc::clone(&calls);

        client.set_reconnect_delay(move |attempt| {
            calls.fetch_add(1, Ordering::SeqCst);
            (attempt < 3).then_some(Duration::ZERO)
        });
    }

    cluster.kill(2);

    // Node 2 was never contacted, so reconnecting has to open it: refused three times.
    assert!(matches!(client.reconnect(), Err(Error::Io(_))));
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[test]
fn a_dead_owner_is_replaced_from_the_refreshed_slot_map() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let key = cluster.key_on(1, "dead");

    client.set(&key, "v").unwrap();
    cluster.kill(1);
    cluster.move_slots(0, 16_383, 0);
    cluster.put(0, &key, Val::Str("v0".into()), -1);

    // The first command meets the dead socket; the next finds the slot map has changed.
    assert!(client.get(&key).is_err());
    assert_eq!(client.get_string(&key).unwrap().as_deref(), Some("v0"));
}

#[test]
fn with_reconnect_off_a_dead_owner_triggers_a_refresh_at_once() {
    let cluster = Cluster::start(3);
    let mut client = Client::connect_with(ClientOptions {
        reconnect: false,
        ..cluster.options()
    })
    .unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let key = cluster.key_on(1, "dead");

    client.set(&key, "v").unwrap();
    cluster.kill(1);
    cluster.move_slots(0, 16_383, 0);
    cluster.put(0, &key, Val::Str("v0".into()), -1);

    assert!(client.get(&key).is_err());
    assert_eq!(client.get_string(&key).unwrap().as_deref(), Some("v0"));
}

#[test]
fn a_dead_owner_that_stays_in_the_slot_map_reports_the_connection_error() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let key = cluster.key_on(2, "dead");

    client.set(&key, "v").unwrap();
    cluster.kill(2);

    assert!(client.get(&key).is_err());
    // Refresh succeeds from a live node, but the owner is still the dead one.
    assert!(matches!(client.get(&key), Err(Error::Io(_))));
}

#[test]
fn if_no_node_can_describe_the_cluster_the_original_error_is_kept() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let key = cluster.key_on(2, "dead");

    client.set(&key, "v").unwrap();
    cluster.kill(2);
    assert!(client.get(&key).is_err());

    // Every live node now answers CLUSTER SLOTS with something that is not a topology.
    cluster.script("CLUSTER", b"+nonsense\r\n", 10);

    assert!(matches!(client.get(&key), Err(Error::Io(_))));
}

#[test]
fn a_moved_redirect_with_a_broken_topology_source_fails() {
    let cluster = Cluster::start(2);
    let mut client = cluster.client();
    let key = cluster.key_on(0, "k");
    let moved = format!(
        "-MOVED {} 127.0.0.1:{}\r\n",
        hash_slot(&key),
        cluster.port(1)
    );

    // The second MOVED of the command reloads the map, from sources that send nonsense.
    cluster.script("GET", moved.as_bytes(), 2);
    cluster.script("CLUSTER", b"*0\r\n", 5);

    assert_eq!(
        protocol_error(client.get(&key)),
        "invalid CLUSTER SLOTS topology"
    );
}

#[test]
fn refresh_fails_when_the_seed_connection_has_dropped_mid_redirect() {
    let cluster = Cluster::start(2);
    let mut client = cluster.client();
    let key = cluster.key_on(1, "k");
    let moved = format!(
        "-MOVED {} 127.0.0.1:{}\r\n",
        hash_slot(&key),
        cluster.port(0)
    );

    client.get(&key).unwrap();
    // The second MOVED reloads the map. Both open sockets hang up on CLUSTER SLOTS, and so do
    // the two nodes the reload then opens again, so no source is left.
    cluster.script("GET", moved.as_bytes(), 2);
    cluster.hang_up("CLUSTER", 4);

    assert!(matches!(client.get(&key), Err(Error::Io(_))));
}

// ---------------------------------------------------------------------------------------------
// Pub/Sub on a cluster
// ---------------------------------------------------------------------------------------------

#[test]
fn sharded_subscribe_connects_to_the_shard_that_owns_the_channel() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let channel = cluster.key_on(2, "chan");

    cluster.clear_log();
    let confirmations = client.ssubscribe(&[&channel]).unwrap();

    assert_eq!(confirmations.len(), 1);
    assert_eq!(cluster.seen("SSUBSCRIBE")[0].0, 2);

    // A plain SUBSCRIBE rides the shard that owns slot 0.
    let mut other = cluster.client();

    cluster.clear_log();
    assert_eq!(other.subscribe(&["news", "sport"]).unwrap().len(), 2);
    assert_eq!(cluster.seen("SUBSCRIBE")[0].0, cluster.owner(0));
}

#[test]
fn read_message_on_a_cluster_client_reads_the_seed_and_survives_a_timeout() {
    let cluster = Cluster::start(2);
    let mut client = cluster.client();

    client
        .set_read_timeout(Some(Duration::from_millis(60)))
        .unwrap();
    assert!(
        matches!(client.read_message(), Err(Error::Io(ref e)) if matches!(
            e.kind(),
            std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
        ))
    );

    // A timeout while only waiting leaves the socket usable.
    cluster.push(0, b"*3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$2\r\nhi\r\n");
    assert_eq!(
        client.read_message().unwrap(),
        RespValue::Array(vec![text("message"), text("ch"), text("hi")])
    );

    client
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    assert_eq!(client.ping().unwrap(), "PONG");
}

// Regressions

#[test]
fn a_broken_seed_connection_is_reopened_for_keyless_commands() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();

    cluster.hang_up("PING", 1);
    assert!(
        client.execute(&["PING"]).is_err(),
        "the call that hits the dropped socket fails"
    );
    assert_eq!(
        client.execute(&["PING"]).unwrap(),
        RespValue::Simple("PONG".into()),
        "the next keyless command must open the seed again instead of failing for good"
    );
    assert_eq!(
        client.execute_binary(&[b"PING".as_slice()]).unwrap(),
        RespValue::Simple("PONG".into())
    );
}

#[test]
fn a_cross_slot_mset_with_a_dangling_key_is_an_error_not_a_panic() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client();
    let (a, b) = (cluster.key_on(0, "a"), cluster.key_on(1, "b"));

    assert!(matches!(
        client.execute(&["MSET", &a, "1", &b]),
        Err(Error::Server(message)) if message.contains("wrong number of arguments")
    ));
    assert!(cluster.peek(0, &a).is_none(), "nothing may be written");
    assert_eq!(
        client.execute(&["PING"]).unwrap(),
        RespValue::Simple("PONG".into())
    );
}
