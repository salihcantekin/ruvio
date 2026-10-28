use std::net::{IpAddr, SocketAddr};

use std::sync::{Arc, Mutex};

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::args::{Args, Num};
use crate::cluster::{ClusterRouter, Connection, Discovery, throw_error};

use crate::error::Error;
use crate::tls::TlsOptions;
use crate::value::RespValue;

/// Condition for [`Client::set_with`] and the other `SET` variants.
///
/// Maps to the `NX` / `XX` flags of `SET`. When the condition is not met the server writes
/// nothing and replies with a null bulk; the client reports that as `false` (or, for
/// [`Client::set_and_get`], still returns the previous value).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SetMode {
    /// Write the key whether or not it exists (no flag sent). This is the default.
    #[default]
    Always,
    /// Write only when the key does not exist (`NX`). Use it to create a value once.
    IfNotExists,
    /// Write only when the key already exists (`XX`). Use it to overwrite without creating.
    IfExists,
}

/// Everything [`Client::connect_with`] needs to open a connection.
///
/// Start from [`ClientOptions::default`] (loopback, port 6379, no password, database 0) and
/// override what you need with struct-update syntax.
///
/// # Examples
///
/// ```no_run
/// # use ruvio_client::*;
/// # use std::time::Duration;
/// # fn run(client: &mut Client) -> Result<(), Error> {
/// let options = ClientOptions {
///     host: "cache.internal".to_owned(),
///     port: 6379,
///     password: Some("secret".to_owned()),
///     connect_timeout: Duration::from_secs(2),
///     ..ClientOptions::default()
/// };
/// let mut client = Client::connect_with(options)?;
/// client.ping()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ClientOptions {
    /// Server host name or IP address. Must not be empty. Defaults to `127.0.0.1`.
    pub host: String,
    /// Server RESP port. Defaults to 6379. On a sharded server with `port_mode = "per-shard"`
    /// this is the seed port (any shard); the other shards are found with `CLUSTER SLOTS`.
    pub port: u16,
    /// Seed addresses as `(host, port)` pairs. When not empty they are used instead of
    /// [`Self::host`] and [`Self::port`]: connecting tries them in order and keeps the first one
    /// that answers, and after a dropped connection the reconnect starts with the next seed. In
    /// cluster mode `CLUSTER SLOTS` comes from the first reachable seed, and the seeds are asked
    /// again when no known node answers a slot map reload. Defaults to empty.
    pub seeds: Vec<(String, u16)>,
    /// ACL user name. When set together with [`Self::password`] the client sends
    /// `AUTH user password`; with only a password it sends the one-argument `AUTH password`.
    /// Defaults to `None`.
    pub username: Option<String>,
    /// Password sent as `AUTH` right after connecting, before `SELECT` and cluster discovery.
    /// Also used again on every automatic reconnect. A wrong password makes
    /// [`Client::connect_with`] fail with [`Error::Server`]. Defaults to `None` (no `AUTH`).
    pub password: Option<String>,
    /// Logical database number, `0` up to the server's `databases` setting minus one (16
    /// databases by default). When not 0 the client sends `SELECT` after `AUTH`, and again after
    /// every reconnect. Only valid against a single-shard server: with a routed (multi-shard)
    /// connection a non-zero value makes [`Client::connect_with`] fail with [`Error::Protocol`].
    /// Defaults to 0.
    pub database: u32,
    /// Upper bound for each TCP connect attempt (not for reads). Defaults to 5 seconds. A
    /// timeout surfaces as [`Error::Io`].
    pub connect_timeout: Duration,
    /// Read timeout applied to every command socket right after connecting (and after a
    /// reconnect), as [`Client::set_read_timeout`] does later. Defaults to 30 seconds; `None`
    /// waits for a reply forever, so a half-open peer blocks a call until the socket drops.
    /// A blocking command gets its own wait on top (`BLPOP k 5` may take 5 s plus this), and
    /// one that waits forever (`BLPOP k 0`) has no limit; Pub/Sub sockets have none either.
    /// A timeout surfaces as [`Error::Io`].
    pub command_timeout: Option<Duration>,
    /// When true (the default) connecting sends `CLUSTER SLOTS`. If the server returns a slot
    /// map with more than one node the client becomes a router that sends each key to its
    /// owning shard; a server that has cluster support off (or a single shard) keeps one plain
    /// socket. Set it to false to talk to exactly the host and port given, for example to
    /// inspect one shard on purpose; then `dbsize`, `scan`, `keys` and `flushdb` act on that
    /// shard only.
    pub discover_cluster: bool,
    /// When true (the default) a socket that is found broken is opened again before the next
    /// command, restoring `AUTH`, `SELECT` and active subscriptions. The command that
    /// discovered the drop still fails with [`Error::Io`] and is not re-sent, because it may
    /// already have been applied. Set it to false to handle drops yourself with
    /// [`Client::reconnect`].
    pub reconnect: bool,
    /// How many connect attempts one automatic reconnect makes before giving up with the last
    /// error. 1 means a single try. Defaults to 8. Ignored when [`Client::set_reconnect_delay`]
    /// is used.
    pub max_reconnect_attempts: u32,
    /// Wait after the first failed reconnect attempt; it doubles with each further failure up
    /// to [`Self::reconnect_max_delay`]. Defaults to 100 ms.
    pub reconnect_base_delay: Duration,
    /// Ceiling for the doubling reconnect wait. Defaults to 2 seconds.
    pub reconnect_max_delay: Duration,
    /// Where a cluster client sends read-only commands; see [`ReadPolicy`]. Ignored by a
    /// standalone connection. Defaults to [`ReadPolicy::Primary`].
    pub read_policy: ReadPolicy,
    /// When set, every connection this client opens (the first one, reconnects, per-shard and
    /// replica sockets) sends `CLIENT DURABILITY REPLICAS n [TIMEOUT ms]` right after `AUTH`, and
    /// the connect fails with [`Error::Server`] when the server refuses it (for example on a
    /// server without replication). Replies are then held until that many replicas have the
    /// writes on disk. Defaults to `None` (nothing is sent).
    pub durability: Option<Durability>,
    /// When set, every connection this client opens (including reconnects and per-shard and
    /// replica sockets) is wrapped in TLS, and the server's certificate is verified; see
    /// [`TlsOptions`]. Needs the `tls` cargo feature: without it the connect fails with
    /// [`Error::Protocol`]. Defaults to `None` (plain TCP).
    pub tls: Option<TlsOptions>,
    /// When true (the default) a standalone client finds the primary again after a failover:
    /// on a reconnect with several seeds (or a [`Self::sentinel_master`]) it asks the seeds
    /// `ROLE` and connects to the one that answers `master`, and on a `READONLY` reply to a
    /// command it does the same and sends that command once more to the new primary (`READONLY`
    /// means it was not applied). A replica's reported primary is followed. Nothing is sent
    /// while the connection is healthy. Ignored in cluster mode. Set it to false to get the
    /// `READONLY` error and the old reconnect order back.
    pub follow_primary: bool,
    /// When set, [`Self::seeds`] (or `host`/`port`) are coordinator or Sentinel addresses, not
    /// data nodes: the primary of this master name is asked with
    /// `SENTINEL get-master-addr-by-name <name>`, checked with `ROLE`, and connected to, at
    /// connect time, on every reconnect and after a `READONLY` reply. The same `AUTH`
    /// credentials are sent to the coordinators and the data node. Defaults to `None`.
    pub sentinel_master: Option<String>,
}

/// The replicas a connection waits for before its replies are released; see
/// [`ClientOptions::durability`] and [`Client::client_durability`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Durability {
    /// How many replicas must have each write on disk. 0 turns the wait off.
    pub replicas: u32,
    /// How long the server waits for them, in milliseconds. `None` keeps the server default
    /// (5000); `TIMEOUT` is then not sent.
    pub timeout_ms: Option<u32>,
}

/// Where a cluster client sends read-only commands (those the server flags `readonly`, such as
/// `GET`, `HGETALL` or `ZRANGE`). Replicas are found in `CLUSTER SLOTS`, which lists each
/// shard's replicas after its primary.
///
/// Replication is asynchronous: a read from a replica may miss recent writes, including the
/// caller's own. Writes, transactions, blocking commands, scripts not flagged `readonly`,
/// Pub/Sub, and any pipeline that holds a write always go to the primaries.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ReadPolicy {
    /// Every command goes to the slot's primary, as before replicas were listed.
    #[default]
    Primary,
    /// Reads go to one replica of the slot's shard when one is known and reachable, otherwise
    /// to the primary. A replica that cannot be opened, or answers `LOADING`, is skipped until
    /// the slot map is reloaded.
    PreferReplica,
    /// Reads go to a replica of the slot's shard; with none known or reachable they fail with
    /// [`Error::Protocol`].
    Replica,
}

impl ClientOptions {
    /// The addresses to connect to, in order: [`Self::seeds`] when set, otherwise `host:port`.
    pub(crate) fn addresses(&self) -> Vec<(String, u16)> {
        if self.seeds.is_empty() {
            vec![(self.host.clone(), self.port)]
        } else {
            self.seeds.clone()
        }
    }
}

/// Opens the first address of `addresses` that can be reached, trying them in order from
/// `start` and wrapping around. Only a connect failure (refused, timed out) moves on to the next
/// address; a refused `AUTH` is returned at once. Returns the connection and its address index.
pub(crate) fn open_first(
    addresses: &[(String, u16)],
    start: usize,
    options: &ClientOptions,
) -> Result<(Connection, usize), Error> {
    let mut last_error = None;

    for step in 0..addresses.len() {
        let index = (start + step) % addresses.len();
        let (host, port) = &addresses[index];

        match Connection::open(host, *port, options) {
            Ok(connection) => return Ok((connection, index)),
            Err(error @ Error::Io(_)) => last_error = Some(error),
            Err(error) => return Err(error),
        }
    }

    Err(last_error.unwrap_or_else(|| Error::Protocol("no address to connect to".to_owned())))
}

/// What `ROLE` said: `master`, or `slave` with the address the replica reports for its primary
/// (for Ruvio that is the replication address, which is not always a client address).
enum Role {
    Master,
    Replica(Option<(String, u16)>),
}

fn read_role(reply: &RespValue) -> Option<Role> {
    let items = reply.as_array()?;
    let kind = items.first()?.as_string().ok()??;

    if kind.eq_ignore_ascii_case("master") {
        return Some(Role::Master);
    }

    if !kind.eq_ignore_ascii_case("slave") {
        return None;
    }

    let host = items
        .get(1)
        .and_then(|item| item.as_string().ok().flatten())
        .filter(|host| !host.is_empty());
    let port = items
        .get(2)
        .and_then(RespValue::as_integer)
        .and_then(|port| u16::try_from(port).ok())
        .filter(|port| *port != 0);

    Some(Role::Replica(host.zip(port)))
}

/// The options of a probe socket: no durability, and a reply wait of at most the connect
/// timeout, so a hung seed costs one bounded wait and never the 30 s command timeout.
fn probe_options(options: &ClientOptions) -> ClientOptions {
    let mut probe = options.clone();

    probe.durability = None;
    probe.command_timeout = Some(
        options
            .command_timeout
            .map_or(options.connect_timeout, |wait| {
                wait.min(options.connect_timeout)
            }),
    );

    probe
}

/// One bounded question to one address. `None` when it cannot be reached or does not answer;
/// a refused `AUTH` is an error, as in [`open_first`].
fn ask(
    address: &(String, u16),
    options: &ClientOptions,
    arguments: &[&str],
) -> Result<Option<RespValue>, Error> {
    let mut connection = match Connection::open(&address.0, address.1, options) {
        Ok(connection) => connection,
        Err(Error::Io(_)) => return Ok(None),
        Err(error) => return Err(error),
    };

    match connection.send(arguments) {
        Ok(RespValue::Error(_)) | Err(Error::Io(_)) => Ok(None),
        Ok(reply) => Ok(Some(reply)),
        Err(error) => Err(error),
    }
}

fn ask_role(address: &(String, u16), probe: &ClientOptions) -> Result<Option<Role>, Error> {
    Ok(ask(address, probe, &["ROLE"])?.and_then(|reply| read_role(&reply)))
}

/// The address `SENTINEL get-master-addr-by-name` names, when the coordinator answers one.
fn ask_sentinel(
    address: &(String, u16),
    name: &str,
    probe: &ClientOptions,
) -> Result<Option<(String, u16)>, Error> {
    let Some(reply) = ask(
        address,
        probe,
        &["SENTINEL", "get-master-addr-by-name", name],
    )?
    else {
        return Ok(None);
    };
    let Some([host, port]) = reply.as_array() else {
        return Ok(None);
    };
    let host = host
        .as_string()
        .ok()
        .flatten()
        .filter(|host| !host.is_empty());
    let port = port
        .as_string()
        .ok()
        .flatten()
        .and_then(|port| port.parse::<u16>().ok());

    Ok(host.zip(port))
}

/// Finds the address that answers `ROLE` with `master`, asking `seeds` in order from `start`.
/// A replica's reported primary is asked after the seeds, once per address. With
/// [`ClientOptions::sentinel_master`] the seeds are coordinators: each names a primary, and the
/// first that `ROLE` confirms wins. `avoid` is an address that must not be taken (the one that
/// just answered `READONLY`) while it still says `master`: a replaced primary does. Once it
/// answers `ROLE` as a replica it is cleared, so a later round takes it when it is promoted.
/// One round, every question bounded by [`probe_options`]; `None` when no address is
/// confirmed.
pub(crate) fn find_primary(
    seeds: &[(String, u16)],
    start: usize,
    avoid: &mut Option<(String, u16)>,
    options: &ClientOptions,
) -> Result<Option<(String, u16)>, Error> {
    let probe = probe_options(options);
    let mut queue: std::collections::VecDeque<(String, u16)> = (0..seeds.len())
        .map(|step| seeds[(start + step) % seeds.len()].clone())
        .collect();

    if let Some(name) = &options.sentinel_master {
        for seed in queue {
            if let Some(address) = ask_sentinel(&seed, name, &probe)? {
                match ask_role(&address, &probe)? {
                    Some(Role::Master) if avoid.as_ref() != Some(&address) => {
                        return Ok(Some(address));
                    }
                    Some(Role::Replica(_)) if avoid.as_ref() == Some(&address) => *avoid = None,
                    _ => {}
                }
            }
        }

        return Ok(None);
    }

    let mut seen: Vec<(String, u16)> = queue.iter().cloned().collect();

    while let Some(address) = queue.pop_front() {
        match ask_role(&address, &probe)? {
            Some(Role::Master) if avoid.as_ref() != Some(&address) => return Ok(Some(address)),
            Some(Role::Replica(reported)) => {
                if avoid.as_ref() == Some(&address) {
                    *avoid = None;
                }

                if let Some(reported) = reported.filter(|reported| !seen.contains(reported)) {
                    seen.push(reported.clone());
                    queue.push_back(reported);
                }
            }
            _ => {}
        }
    }

    Ok(None)
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_owned(),
            port: 6379,
            seeds: Vec::new(),
            username: None,
            password: None,
            database: 0,
            connect_timeout: Duration::from_secs(5),
            command_timeout: Some(Duration::from_secs(30)),
            discover_cluster: true,
            reconnect: true,
            max_reconnect_attempts: 8,
            reconnect_base_delay: Duration::from_millis(100),
            reconnect_max_delay: Duration::from_secs(2),
            read_policy: ReadPolicy::Primary,
            durability: None,
            tls: None,
            follow_primary: true,
            sentinel_master: None,
        }
    }
}

/// The shared slot for an `on_connection_lost` / `on_connection_restored` callback.
pub(crate) type NoticeHook = Arc<Mutex<Option<Arc<dyn Fn(ConnectionNotice) + Send + Sync>>>>;

/// Describes a socket that dropped or came back; passed to the callbacks registered with
/// [`Client::on_connection_lost`] and [`Client::on_connection_restored`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionNotice {
    /// Host the socket was connected to.
    pub host: String,
    /// Port the socket was connected to.
    pub port: u16,
    /// True when the socket is the dedicated Pub/Sub connection, false for a command socket.
    pub subscriber: bool,
}

/// A blocking Ruvio client. Create one with [`Client::connect`] or [`Client::connect_with`].
///
/// Against a single server it is one TCP connection. Against a sharded server it opens one
/// connection per shard (lazily) and sends each command to the shard that owns its key; see
/// the crate-level docs for the cluster rules. All command methods are in this one type and
/// take `&mut self`, so share a client between threads behind a `Mutex`, or give each thread
/// its own client. A command waits up to [`ClientOptions::command_timeout`] (30 s by
/// default, plus a blocking command's own wait) for its reply; change it with
/// [`Client::set_read_timeout`].
///
/// Subscribe commands open a second TCP socket automatically, so `GET`, `SET`, `PING` and
/// `PUBLISH` stay on the command socket while messages arrive on the other one.
///
/// The client speaks RESP2 and does not send `CLIENT SETINFO` or `HELLO`.
///
/// # Examples
///
/// ```no_run
/// # use ruvio_client::*;
/// # use std::time::Duration;
/// # fn run(client: &mut Client) -> Result<(), Error> {
/// let mut client = Client::connect("127.0.0.1", 6379)?;
/// client.set("user:1:name", "Ada")?;
/// let name = client.get_string("user:1:name")?;
/// assert_eq!(name.as_deref(), Some("Ada"));
/// # Ok(())
/// # }
/// ```
pub struct Client {
    transport: Transport,
    database: u32,
    options: ClientOptions,
    dedicated_subscriber: bool,
    pubsub: Option<Box<Client>>,
    on_lost: NoticeHook,
    on_restored: NoticeHook,
    reconnect_delay: Option<Arc<dyn Fn(u32) -> Option<Duration> + Send + Sync>>,
    subscriptions: Vec<Vec<String>>,
    read_timeout: Option<Option<Duration>>,
    /// The addresses from [`ClientOptions::addresses`] and the one the socket is connected to.
    addresses: Vec<(String, u16)>,
    current: usize,
    /// True between a successful `MULTI` and `EXEC` / `DISCARD` sent through this client; a
    /// refused command inside it is never sent again.
    in_multi: bool,
}

enum Transport {
    Single(Connection),
    Cluster(Box<ClusterRouter>),
}

impl Client {
    /// Opens a connection to `host:port` on database 0 with default options.
    ///
    /// Sends `CLUSTER SLOTS` so a sharded server is routed automatically.
    ///
    /// # Arguments
    /// * `host` - host name or IP address.
    /// * `port` - RESP port, 6379 by default on the server.
    ///
    /// # Returns
    /// A connected [`Client`].
    ///
    /// # Errors
    /// [`Error::Io`] when the connection cannot be opened within 5 seconds; [`Error::Protocol`]
    /// when `host` is empty.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let mut client = Client::connect("127.0.0.1", 6379)?;
    /// assert_eq!(client.ping()?, "PONG");
    /// # Ok(())
    /// # }
    /// ```
    pub fn connect(host: impl Into<String>, port: u16) -> Result<Self, Error> {
        Self::connect_database(host, port, 0)
    }

    /// Opens a connection bound to one logical database.
    ///
    /// Sends `SELECT` when `database` is not 0. The returned client is the database handle:
    /// every command on it runs in `database`, and the choice is restored after a reconnect.
    ///
    /// # Arguments
    /// * `host`, `port` - as for [`Client::connect`].
    /// * `database` - index from 0 to the server's `databases` setting minus one (16 databases
    ///   by default). Only a single-shard server has more than database 0.
    ///
    /// # Errors
    /// [`Error::Server`] when the index is out of range (`ERR DB index is out of range`);
    /// [`Error::Protocol`] against a sharded server when `database` is not 0; [`Error::Io`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let mut sessions = Client::connect_database("127.0.0.1", 6379, 2)?;
    /// sessions.set("token", "abc")?; // lives in database 2 only
    /// # Ok(())
    /// # }
    /// ```
    pub fn connect_database(
        host: impl Into<String>,
        port: u16,
        database: u32,
    ) -> Result<Self, Error> {
        Self::connect_with(ClientOptions {
            host: host.into(),
            port,
            database,
            ..ClientOptions::default()
        })
    }

    /// Like [`Client::connect_database`] but takes an [`IpAddr`](std::net::IpAddr), so no name
    /// lookup happens.
    ///
    /// # Arguments
    /// * `ip` - server address.
    /// * `port` - RESP port.
    /// * `database` - logical database, 0 for the default.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let ip = "127.0.0.1".parse().unwrap();
    /// let mut client = Client::connect_ip(ip, 6379, 0)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn connect_ip(ip: IpAddr, port: u16, database: u32) -> Result<Self, Error> {
        Self::connect_addr(SocketAddr::new(ip, port), database)
    }

    /// Like [`Client::connect_database`] but takes a ready [`SocketAddr`](std::net::SocketAddr).
    ///
    /// # Arguments
    /// * `address` - server IP and port.
    /// * `database` - logical database, 0 for the default.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let address = "127.0.0.1:6379".parse().unwrap();
    /// let mut client = Client::connect_addr(address, 0)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn connect_addr(address: SocketAddr, database: u32) -> Result<Self, Error> {
        Self::connect_database(address.ip().to_string(), address.port(), database)
    }

    /// Opens a connection with full control over [`ClientOptions`].
    ///
    /// The steps are: TCP connect (bounded by `connect_timeout`), `AUTH` when a password is set,
    /// `CLUSTER SLOTS` when `discover_cluster` is on, and `SELECT` when `database` is not 0.
    ///
    /// # Errors
    /// * [`Error::Protocol`] - empty host, or a non-zero `database` against a sharded server.
    /// * [`Error::Server`] - `AUTH` or `SELECT` refused (wrong password, database out of range).
    /// * [`Error::Io`] - connect failed or timed out.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let mut client = Client::connect_with(ClientOptions {
    ///     host: "127.0.0.1".to_owned(),
    ///     password: Some("secret".to_owned()),
    ///     database: 1,
    ///     ..ClientOptions::default()
    /// })?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn connect_with(options: ClientOptions) -> Result<Self, Error> {
        let mut addresses = options.addresses();

        if addresses.iter().any(|(host, _)| host.is_empty()) {
            return Err(Error::Protocol("host is required".to_owned()));
        }

        if let Some(name) = &options.sentinel_master {
            let primary = find_primary(&addresses, 0, &mut None, &options)?.ok_or_else(|| {
                Error::Protocol(format!("no coordinator names a primary for '{name}'"))
            })?;

            addresses = vec![primary];
        }

        let (seed, current) = open_first(&addresses, 0, &options)?;

        if !options.discover_cluster {
            return finish_standalone(seed, options, addresses, current);
        }

        match ClusterRouter::discover(seed, options.clone(), addresses.clone())? {
            Discovery::Cluster(_) if options.database != 0 => Err(Error::Protocol(
                "SELECT is not supported in cluster mode".to_owned(),
            )),
            Discovery::Cluster(mut router) => {
                let on_lost = Arc::new(Mutex::new(None));
                let on_restored = Arc::new(Mutex::new(None));

                router.share_hooks(Arc::clone(&on_lost), Arc::clone(&on_restored));

                Ok(Self {
                    transport: Transport::Cluster(router),
                    database: 0,
                    options,
                    dedicated_subscriber: false,
                    pubsub: None,
                    read_timeout: None,
                    on_lost,
                    on_restored,
                    reconnect_delay: None,
                    subscriptions: Vec::new(),
                    addresses,
                    current,
                    in_multi: false,
                })
            }

            Discovery::Standalone(seed) => finish_standalone(seed, options, addresses, current),
        }
    }

    /// True when the server reported a multi-shard slot map and commands are routed by hash
    /// slot. False for a standalone server or when `discover_cluster` is off.
    pub fn is_cluster(&self) -> bool {
        matches!(self.transport, Transport::Cluster(_))
    }

    /// How many shards the client routes to: the number of nodes in the slot map, or 1 for a
    /// single server. Does not send anything.
    pub fn shard_count(&self) -> usize {
        match &self.transport {
            Transport::Single(_) => 1,
            Transport::Cluster(router) => router.node_count(),
        }
    }

    /// Sends `PING` and returns the reply, normally `"PONG"`. Use it as a health check.
    ///
    /// # Errors
    /// [`Error::Io`] when the server is unreachable, [`Error::Server`] when it refuses the
    /// command (for example `NOAUTH`).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// assert_eq!(client.ping()?, "PONG");
    /// # Ok(())
    /// # }
    /// ```
    pub fn ping(&mut self) -> Result<String, Error> {
        read_ping(self.run(["PING"])?)
    }

    /// Sends `PING message` and returns the message back (a bulk echo). Handy to check that
    /// replies are matched to the right requests.
    ///
    /// # Arguments
    /// * `message` - any text.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// assert_eq!(client.ping_message("hi")?, "hi");
    /// # Ok(())
    /// # }
    /// ```
    pub fn ping_message(&mut self, message: &str) -> Result<String, Error> {
        read_ping(self.run(["PING", message])?)
    }

    /// The logical database this client last selected (0 unless set by `connect_database`,
    /// [`ClientOptions::database`], [`Client::select`] or [`Client::into_database`]). Does not
    /// contact the server.
    pub fn database(&self) -> u32 {
        self.database
    }

    /// Opens a second socket dedicated to Pub/Sub and returns it as a new [`Client`].
    ///
    /// You rarely need this: `subscribe`, `psubscribe` and `ssubscribe` on a normal client open
    /// that socket on their own. Use it when you want the subscriber in its own variable, for
    /// example to read messages on another thread. On a sharded server the new socket connects
    /// to the seed node only; classic `PUBLISH` reaches subscribers on every shard.
    ///
    /// # Errors
    /// [`Error::Io`] when the connection cannot be opened; [`Error::Server`] when `AUTH`
    /// fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let mut subscriber = client.subscriber()?;
    /// subscriber.subscribe(&["news"])?;
    /// let message = subscriber.read_message()?; // blocks until a push arrives
    /// # Ok(())
    /// # }
    /// ```
    pub fn subscriber(&self) -> Result<Self, Error> {
        self.open_pubsub(&["SUBSCRIBE"])
    }

    /// Opens a Pub/Sub socket for sharded channels (`SSUBSCRIBE`).
    ///
    /// On a sharded server the socket connects to the shard that owns `channel`'s hash slot, so
    /// `ssubscribe` of channels in that slot (use one hash tag, e.g. `{room1}:a`, `{room1}:b`)
    /// works there. A plain [`Client::subscriber`] reaches one shard only, and `SSUBSCRIBE` on
    /// the wrong shard is redirected by the server, not forwarded.
    ///
    /// # Arguments
    /// * `channel` - any channel name whose slot you want to listen on.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let mut subscriber = client.sharded_subscriber("{room1}:events")?;
    /// subscriber.ssubscribe(&["{room1}:events"])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn sharded_subscriber(&self, channel: &str) -> Result<Self, Error> {
        self.open_pubsub(&["SSUBSCRIBE", channel])
    }

    /// Sends `SELECT` to switch this connection to another logical database.
    ///
    /// # Arguments
    /// * `database` - index from 0 to the server's `databases` setting minus one (16 by
    ///   default). Remembered for reconnects. Only a single-shard server has more than one
    ///   database; a sharded server only has database 0.
    ///
    /// # Errors
    /// [`Error::Server`] if the index is out of range or the server runs several shards.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.select(3)?;
    /// assert_eq!(client.database(), 3);
    /// # Ok(())
    /// # }
    /// ```
    pub fn select(&mut self, database: u32) -> Result<(), Error> {
        let index = Num::uint(u64::from(database));

        self.run(["SELECT", index.as_str()])?;
        self.database = database;

        Ok(())
    }

    /// Sends `SELECT` and returns the same connection, so it can be chained after `connect`.
    ///
    /// This is not a second TCP socket: every later command on the client uses the new
    /// database.
    ///
    /// # Errors
    /// As for [`Client::select`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let mut client = Client::connect("127.0.0.1", 6379)?.into_database(2)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn into_database(mut self, database: u32) -> Result<Self, Error> {
        self.select(database)?;

        Ok(self)
    }

    /// Sets how long a reply read may block, on every socket of this client (including the
    /// Pub/Sub socket). `None` waits forever. Command sockets start with
    /// [`ClientOptions::command_timeout`] (30 s by default); Pub/Sub sockets start with none.
    ///
    /// A timeout while nothing has arrived yet is harmless: [`Error::Io`] with kind
    /// `WouldBlock`/`TimedOut`, and the connection stays usable (this is how you poll
    /// [`Client::read_message`]). A timeout in the middle of a command reply leaves the reply
    /// unread, so drop the client in that case. A blocking command such as `blpop` gets its
    /// own wait on top of this timeout, and none at all when it waits forever (`0`).
    ///
    /// # Arguments
    /// * `timeout` - `Some(duration)` (must be non-zero) or `None`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.set_read_timeout(Some(Duration::from_secs(3)))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<(), Error> {
        self.read_timeout = Some(timeout);
        // Sockets opened later (reconnects, new shards) start with it too.
        self.options.command_timeout = timeout;

        match &mut self.transport {
            Transport::Single(connection) => connection.set_read_timeout(timeout)?,
            Transport::Cluster(router) => router.set_read_timeout(timeout)?,
        }

        if let Some(pubsub) = &mut self.pubsub {
            pubsub.set_read_timeout(timeout)?;
        }

        Ok(())
    }

    /// Sends `GET` and returns the raw bytes of a string value.
    ///
    /// # Arguments
    /// * `key` - the key to read.
    ///
    /// # Returns
    /// `Some(bytes)` with the stored bytes (possibly empty), or `None` when the key is missing
    /// or expired. Use [`Client::get_string`] for text.
    ///
    /// # Errors
    /// [`Error::Server`] `WRONGTYPE` when the key holds a list, hash, set or other non-string
    /// type; [`Error::Io`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.set("k", "v")?;
    /// assert_eq!(client.get("k")?, Some(b"v".to_vec()));
    /// assert_eq!(client.get("missing")?, None);
    /// # Ok(())
    /// # }
    /// ```
    pub fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        match self.execute(&["GET", key])? {
            RespValue::Null => Ok(None),
            RespValue::Bulk(bytes) => Ok(Some(bytes)),
            other => Err(Error::Protocol(format!("GET returned {other:?}"))),
        }
    }

    /// Sends `GET` and decodes the value as UTF-8 text.
    ///
    /// # Returns
    /// `Some(text)` or `None` when the key is missing.
    ///
    /// # Errors
    /// [`Error::Protocol`] when the value is not valid UTF-8 (use [`Client::get`] for binary
    /// data); [`Error::Server`] `WRONGTYPE` for a non-string key.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.set("greeting", "hello")?;
    /// assert_eq!(client.get_string("greeting")?.as_deref(), Some("hello"));
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_string(&mut self, key: &str) -> Result<Option<String>, Error> {
        match self.get(key)? {
            None => Ok(None),
            Some(bytes) => RespValue::Bulk(bytes).into_string(),
        }
    }

    /// Sends `SET key value` with no expiry and no condition. Replaces any existing value of
    /// any type, and clears any TTL.
    ///
    /// # Returns
    /// `true` when written. (Always true here; the `bool` is shared with the conditional
    /// variants such as [`Client::set_with`].) Use [`Client::set_bytes`] for non-UTF-8 values.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.set("name", "Ada")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn set(&mut self, key: &str, value: &str) -> Result<bool, Error> {
        self.set_with(key, value, None, SetMode::Always)
    }

    /// Sends `SET` with an optional expiry and an optional `NX`/`XX` condition.
    ///
    /// # Arguments
    /// * `key`, `value` - key and string value.
    /// * `expiry` - `Some(ttl)` sends `PX` with the TTL in whole milliseconds (sub-millisecond
    ///   parts are dropped); `None` stores without expiry and removes any previous TTL.
    /// * `mode` - see [`SetMode`].
    ///
    /// # Returns
    /// `true` when the value was written; `false` when `NX`/`XX` skipped it.
    ///
    /// # Errors
    /// [`Error::Server`] if the TTL is zero or too large for the server.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// // Create the key once, expiring in 30 seconds.
    /// let created = client.set_with("lock", "me", Some(Duration::from_secs(30)), SetMode::IfNotExists)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_with(
        &mut self,
        key: &str,
        value: &str,
        expiry: Option<Duration>,
        mode: SetMode,
    ) -> Result<bool, Error> {
        let mut digits = [0_u8; 40];

        match expiry {
            Some(ttl) => self.set_fixed(
                key,
                value,
                Some(("PX", decimal(duration_millis(ttl), &mut digits))),
                mode,
            ),
            None => self.set_fixed(key, value, None, mode),
        }
    }

    /// Sends `SET ... PXAT`: the key expires at an absolute time.
    ///
    /// # Arguments
    /// * `key`, `value`, `mode` - as for [`Client::set_with`].
    /// * `expires_at` - absolute [`SystemTime`](std::time::SystemTime); sent as Unix
    ///   milliseconds. A time in the past makes the key expire immediately.
    ///
    /// # Returns
    /// `true` when written, `false` when `mode` skipped the write.
    ///
    /// # Errors
    /// [`Error::Protocol`] when `expires_at` is before the Unix epoch.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// use std::time::SystemTime;
    /// let deadline = SystemTime::now() + Duration::from_secs(3600);
    /// client.set_expires_at("promo", "on", deadline, SetMode::Always)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_expires_at(
        &mut self,
        key: &str,
        value: &str,
        expires_at: SystemTime,
        mode: SetMode,
    ) -> Result<bool, Error> {
        let mut digits = [0_u8; 40];

        self.set_fixed(
            key,
            value,
            Some(("PXAT", decimal(unix_millis(expires_at)?, &mut digits))),
            mode,
        )
    }

    /// Sends `SET ... KEEPTTL`: overwrites the value but keeps the key's current expiry.
    ///
    /// # Arguments
    /// * `mode` - [`SetMode`] condition.
    ///
    /// # Returns
    /// `true` when written, `false` when `mode` skipped the write.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.set_keep_ttl("session", "new-data", SetMode::IfExists)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_keep_ttl(&mut self, key: &str, value: &str, mode: SetMode) -> Result<bool, Error> {
        self.set_fixed(key, value, Some(("KEEPTTL", "")), mode)
    }

    /// `SET` built on the stack: no argument vector or owned strings. A `("KEEPTTL", "")` expiry
    /// sends the keyword alone.
    fn set_fixed(
        &mut self,
        key: &str,
        value: &str,
        expiry: Option<(&str, &str)>,
        mode: SetMode,
    ) -> Result<bool, Error> {
        let mut arguments = ["SET", key, value, "", "", ""];
        let mut count = 3;

        if let Some((keyword, amount)) = expiry {
            arguments[count] = keyword;
            count += 1;

            if !amount.is_empty() {
                arguments[count] = amount;
                count += 1;
            }
        }

        match mode {
            SetMode::Always => {}
            SetMode::IfNotExists => {
                arguments[count] = "NX";
                count += 1;
            }
            SetMode::IfExists => {
                arguments[count] = "XX";
                count += 1;
            }
        }

        Ok(!matches!(
            self.execute(&arguments[..count])?,
            RespValue::Null
        ))
    }

    /// Sends `SET ... GET`: writes a value and returns the previous one in one step.
    ///
    /// # Arguments
    /// * `key`, `value`, `expiry`, `mode` - as for [`Client::set_with`].
    ///
    /// # Returns
    /// The previous string, or `None` when the key did not exist. With `NX`/`XX` the previous
    /// value is returned even if the write was skipped, so you cannot tell from the result alone
    /// whether the write happened.
    ///
    /// # Errors
    /// [`Error::Server`] `WRONGTYPE` when the existing value is not a string.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let old = client.set_and_get("config", "v2", None, SetMode::Always)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_and_get(
        &mut self,
        key: &str,
        value: &str,
        expiry: Option<Duration>,
        mode: SetMode,
    ) -> Result<Option<String>, Error> {
        let millis = expiry.map(|ttl| Num::wide(duration_millis(ttl)));
        let mut arguments = ["SET", key, value, "", "", "", ""];
        let mut count = 3;

        if let Some(millis) = &millis {
            arguments[3] = "PX";
            arguments[4] = millis.as_str();
            count = 5;
        }

        if let Some(flag) = mode_flag(mode) {
            arguments[count] = flag;
            count += 1;
        }

        arguments[count] = "GET";
        self.bulk_or_null(&arguments[..=count])
    }

    /// Sends `SET key value IFEQ expected [PX ms]`, a Ruvio compare-and-set: write only when
    /// the current string equals `expected`. Redis has no equivalent.
    ///
    /// # Arguments
    /// * `key`, `value` - key and the new value.
    /// * `expected` - the value the key must hold now (exact byte comparison).
    /// * `ttl` - `Some(ttl)` sets a new expiry in milliseconds; `None` clears any TTL (as a
    ///   plain `SET` does). See [`Client::set_if_eq_keep_ttl`] to keep it.
    ///
    /// # Returns
    /// `true` when swapped; `false` when the key is missing or holds a different value.
    ///
    /// # Errors
    /// [`Error::Server`] `WRONGTYPE` for a non-string key.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// // Release a lock only if we still own it.
    /// let swapped = client.set_if_eq("owner", "node-2", "node-1", None)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_if_eq(
        &mut self,
        key: &str,
        value: &str,
        expected: &str,
        ttl: Option<Duration>,
    ) -> Result<bool, Error> {
        let millis = ttl.map(|ttl| Num::wide(duration_millis(ttl)));

        let reply = match &millis {
            Some(millis) => {
                self.execute(&["SET", key, value, "IFEQ", expected, "PX", millis.as_str()])?
            }
            None => self.execute(&["SET", key, value, "IFEQ", expected])?,
        };

        Ok(!reply.is_null())
    }

    /// Sends `SET key value IFEQ expected KEEPTTL`: compare-and-set that leaves the key's TTL
    /// untouched. Arguments and results are as for [`Client::set_if_eq`].
    pub fn set_if_eq_keep_ttl(
        &mut self,
        key: &str,
        value: &str,
        expected: &str,
    ) -> Result<bool, Error> {
        Ok(!self
            .execute(&["SET", key, value, "IFEQ", expected, "KEEPTTL"])?
            .is_null())
    }

    /// Sends `SET key value IFGT [PX ms]` (Ruvio): writes when the key is missing or `value` is
    /// greater than the stored integer. Use it to keep a watermark, version or highest bid.
    ///
    /// # Arguments
    /// * `key`, `value` - key and the new number.
    /// * `ttl` - `Some(ttl)` sets a new expiry in milliseconds; `None` clears any TTL.
    ///
    /// # Returns
    /// `true` when written; `false` when the stored number is greater or equal.
    ///
    /// # Errors
    /// [`Error::Server`] `WRONGTYPE` for a non-string key, or an error when the stored string is
    /// not an integer.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let advanced = client.set_if_greater("watermark", 1_700_000_000, None)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_if_greater(
        &mut self,
        key: &str,
        value: i64,
        ttl: Option<Duration>,
    ) -> Result<bool, Error> {
        self.set_if_compare(key, value, "IFGT", ttl)
    }

    /// Sends `SET key value IFLT [PX ms]` (Ruvio): like [`Client::set_if_greater`] but writes
    /// when `value` is smaller than the stored integer (lowest price, earliest deadline).
    pub fn set_if_less(
        &mut self,
        key: &str,
        value: i64,
        ttl: Option<Duration>,
    ) -> Result<bool, Error> {
        self.set_if_compare(key, value, "IFLT", ttl)
    }

    fn set_if_compare(
        &mut self,
        key: &str,
        value: i64,
        condition: &str,
        ttl: Option<Duration>,
    ) -> Result<bool, Error> {
        let number = Num::int(value);
        let millis = ttl.map(|ttl| Num::wide(duration_millis(ttl)));

        let reply = match &millis {
            Some(millis) => self.execute(&[
                "SET",
                key,
                number.as_str(),
                condition,
                "PX",
                millis.as_str(),
            ])?,
            None => self.execute(&["SET", key, number.as_str(), condition])?,
        };

        Ok(!reply.is_null())
    }

    /// Sends `SET key value EX seconds`.
    ///
    /// # Arguments
    /// * `seconds` - lifetime in whole seconds; must be greater than 0.
    ///
    /// # Returns
    /// `true` when written.
    ///
    /// # Errors
    /// [`Error::Server`] if `seconds` is 0 or overflows. See also [`Client::setex`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.set_ex("otp", "493021", 60)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_ex(&mut self, key: &str, value: &str, seconds: u64) -> Result<bool, Error> {
        let seconds = Num::uint(seconds);

        self.set_fixed(key, value, Some(("EX", seconds.as_str())), SetMode::Always)
    }

    /// Sends `SET key value EXAT unix_seconds`: expire at an absolute time.
    ///
    /// # Arguments
    /// * `unix_seconds` - Unix timestamp in seconds. A past time deletes the key at once.
    /// * `mode` - [`SetMode`] condition.
    ///
    /// # Returns
    /// `true` when written, `false` when `mode` skipped the write.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.set_exat("report", "ready", 1_900_000_000, SetMode::Always)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_exat(
        &mut self,
        key: &str,
        value: &str,
        unix_seconds: u64,
        mode: SetMode,
    ) -> Result<bool, Error> {
        let seconds = Num::uint(unix_seconds);

        self.set_fixed(key, value, Some(("EXAT", seconds.as_str())), mode)
    }

    /// Binary-safe `SET key value [PX ms]` for values that are not valid UTF-8 (images,
    /// encrypted blobs, serialized structures). Read back with [`Client::get`].
    ///
    /// # Arguments
    /// * `value` - raw bytes.
    /// * `ttl` - `Some` sets the expiry in milliseconds; `None` stores without expiry and clears
    ///   any TTL.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.set_bytes("blob", &[0, 159, 146, 150], None)?;
    /// assert_eq!(client.get("blob")?, Some(vec![0, 159, 146, 150]));
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_bytes(
        &mut self,
        key: &str,
        value: &[u8],
        ttl: Option<Duration>,
    ) -> Result<(), Error> {
        let millis = ttl.map(|ttl| Num::wide(duration_millis(ttl)));

        match &millis {
            Some(millis) => self.execute_binary(&[
                b"SET",
                key.as_bytes(),
                value,
                b"PX",
                millis.as_str().as_bytes(),
            ])?,
            None => self.execute_binary(&[b"SET", key.as_bytes(), value])?,
        };

        Ok(())
    }

    /// Sends `INCR`: adds 1 to the integer stored at `key` and returns the new value. A missing
    /// key counts as 0, so the first call returns 1.
    ///
    /// # Errors
    /// [`Error::Server`] when the value is not an integer (`ERR value is not an integer or out
    /// of range`), would overflow `i64`, or the key holds another type (`WRONGTYPE`). A value
    /// written with `SETV` is also refused.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// assert_eq!(client.incr("visits")?, 1);
    /// assert_eq!(client.incr("visits")?, 2);
    /// # Ok(())
    /// # }
    /// ```
    pub fn incr(&mut self, key: &str) -> Result<i64, Error> {
        self.integer(["INCR", key])
    }

    /// Sets a time to live on an existing key.
    ///
    /// Sends `EXPIRE` when `ttl` is a whole number of seconds, otherwise `PEXPIRE` with
    /// milliseconds.
    ///
    /// # Arguments
    /// * `ttl` - time to live from now. Zero makes the key expire right away.
    ///
    /// # Returns
    /// `true` when the TTL was set; `false` when the key does not exist.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.set("cache:1", "data")?;
    /// assert!(client.expire("cache:1", Duration::from_secs(60))?);
    /// # Ok(())
    /// # }
    /// ```
    pub fn expire(&mut self, key: &str, ttl: Duration) -> Result<bool, Error> {
        if ttl.subsec_nanos() == 0 {
            let seconds = Num::uint(duration_secs(ttl));

            return self.flag(["EXPIRE", key, seconds.as_str()]);
        }

        let millis = Num::wide(duration_millis(ttl));

        self.flag(["PEXPIRE", key, millis.as_str()])
    }

    /// Sends `DEL` for one key. Returns 1 if the key existed and was removed, 0 otherwise. Any
    /// type of value is deleted.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.set("tmp", "x")?;
    /// assert_eq!(client.del_key("tmp")?, 1);
    /// # Ok(())
    /// # }
    /// ```
    pub fn del_key(&mut self, key: &str) -> Result<i64, Error> {
        self.del(&[key])
    }

    /// Sends `DEL` for several keys and returns how many existed and were removed.
    ///
    /// On a sharded server keys in different slots are deleted per shard and the counts summed;
    /// this is **not atomic**.
    ///
    /// # Errors
    /// [`Error::Protocol`] when `keys` is empty (nothing is sent).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let removed = client.del(&["a", "b", "c"])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn del(&mut self, keys: &[&str]) -> Result<i64, Error> {
        if keys.is_empty() {
            return Err(Error::Protocol("DEL needs at least one key".to_owned()));
        }

        self.integer(join("DEL", keys))
    }

    /// Reads the next reply without sending a command. Use it after a subscribe to receive
    /// Pub/Sub pushes.
    ///
    /// # Returns
    /// A [`RespValue::Array`]. For `SUBSCRIBE`: `["message", channel, payload]`; for
    /// `PSUBSCRIBE`: `["pmessage", pattern, channel, payload]`; for `SSUBSCRIBE`:
    /// `["smessage", channel, payload]`. Subscribe confirmations arrive as `["subscribe",
    /// channel, count]`. Prefer [`Client::next_message`], which skips confirmations and returns a
    /// [`PubSubMessage`](crate::PubSubMessage).
    ///
    /// Blocks until a message arrives unless [`Client::set_read_timeout`] is set; a timeout
    /// returns [`Error::Io`] and the client stays usable.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let mut subscriber = client.subscriber()?;
    /// subscriber.subscribe(&["news"])?;
    /// let push = subscriber.read_message()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn read_message(&mut self) -> Result<RespValue, Error> {
        if let Some(pubsub) = &mut self.pubsub {
            return pubsub.read_message();
        }

        self.ensure_open()?;

        let reply = match &mut self.transport {
            Transport::Single(connection) => connection.read()?,
            Transport::Cluster(router) => router.read_message()?,
        };

        throw_error(reply)
    }

    /// Registers a callback called when a command or Pub/Sub socket drops. Replaces any earlier
    /// one. The callback runs on the thread that noticed the drop, so keep it short and do not
    /// call the client from inside it.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// client.on_connection_lost(|notice| {
    ///     eprintln!("lost {}:{} (subscriber: {})", notice.host, notice.port, notice.subscriber);
    /// });
    /// # Ok(())
    /// # }
    /// ```
    pub fn on_connection_lost<F>(&mut self, hook: F)
    where
        F: Fn(ConnectionNotice) + Send + Sync + 'static,
    {
        *self.on_lost.lock().expect("connection hook") = Some(Arc::new(hook));
    }

    /// Registers a callback called after a dropped socket has been opened again (including
    /// `AUTH`, `SELECT` and re-subscribing). Same rules as [`Client::on_connection_lost`].
    pub fn on_connection_restored<F>(&mut self, hook: F)
    where
        F: Fn(ConnectionNotice) + Send + Sync + 'static,
    {
        *self.on_restored.lock().expect("connection hook") = Some(Arc::new(hook));
    }

    /// Replaces the built-in exponential wait between reconnect attempts.
    ///
    /// # Arguments
    /// * `delay` - called with the attempt number (1 after the first failure). Return
    ///   `Some(wait)` to sleep and try again, or `None` to give up and return the connect
    ///   error. When set, [`ClientOptions::max_reconnect_attempts`] is not applied.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// // Retry up to 5 times, waiting 200 ms * attempt.
    /// client.set_reconnect_delay(|attempt| {
    ///     (attempt <= 5).then(|| Duration::from_millis(200 * u64::from(attempt)))
    /// });
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_reconnect_delay<F>(&mut self, delay: F)
    where
        F: Fn(u32) -> Option<Duration> + Send + Sync + 'static,
    {
        self.reconnect_delay = Some(Arc::new(delay));

        if let Transport::Cluster(router) = &mut self.transport {
            router.set_reconnect_delay(Arc::clone(self.reconnect_delay.as_ref().unwrap()));
        }
    }

    /// Opens a dropped command socket again now. Does nothing when the connection is healthy.
    ///
    /// With [`ClientOptions::reconnect`] on (the default) the next command does this by itself;
    /// call it directly when you turned automatic reconnect off. `AUTH`, `SELECT` and active
    /// subscriptions are restored.
    ///
    /// # Errors
    /// [`Error::Io`] when the server still cannot be reached after the configured attempts.
    pub fn reconnect(&mut self) -> Result<(), Error> {
        if let Transport::Cluster(router) = &mut self.transport {
            return router.reconnect_broken();
        }

        if !self.transport_is_broken() {
            return Ok(());
        }

        self.reopen()
    }

    /// Sends one raw command and returns the reply, for commands that have no typed method.
    ///
    /// # Arguments
    /// * `arguments` - the command name followed by its arguments, e.g. `&["OBJECT",
    ///   "ENCODING", "k"]`. On a sharded server the key is found by the command's key rules and
    ///   routed; multi-key commands over several slots are split (`MGET`, `MSET`, `DEL`,
    ///   `EXISTS`, `UNLINK`, `TOUCH`) or rejected with `CROSSSLOT`.
    ///
    /// # Returns
    /// The [`RespValue`] reply. An error reply never comes back here; it becomes
    /// [`Error::Server`] and the connection stays open.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let reply = client.execute(&["OBJECT", "ENCODING", "mykey"])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn execute(&mut self, arguments: &[&str]) -> Result<RespValue, Error> {
        self.ensure_open()?;

        if is_subscription_command(arguments) && !self.dedicated_subscriber {
            return self.ensure_pubsub(arguments)?.execute(arguments);
        }

        let reply = match &mut self.transport {
            Transport::Single(connection) => connection.send(arguments)?,
            Transport::Cluster(router) => router.execute(arguments)?,
        };

        let reply = match throw_error(reply) {
            Err(Error::Server(message)) if is_readonly(&message) && self.follows() => {
                if !self.follow_after_readonly()? {
                    return Err(Error::Server(message));
                }

                return throw_error(self.send_single(arguments)?);
            }
            other => other?,
        };

        self.remember_subscription(arguments);

        if self.in_multi {
            if arguments.first().is_some_and(|name| {
                name.eq_ignore_ascii_case("EXEC") || name.eq_ignore_ascii_case("DISCARD")
            }) {
                self.in_multi = false;
            }
        } else if matches!(&reply, RespValue::Simple(ok) if ok == "OK")
            && arguments
                .first()
                .is_some_and(|name| name.len() == 5 && name.eq_ignore_ascii_case("MULTI"))
        {
            self.in_multi = true;
        }

        // MONITOR turns this socket into a push stream: drop the command timeout from it,
        // unless the caller set a read timeout to poll with.
        if self.read_timeout.is_none()
            && matches!(&reply, RespValue::Simple(ok) if ok == "OK")
            && arguments
                .first()
                .is_some_and(|name| name.len() == 7 && name.eq_ignore_ascii_case("MONITOR"))
        {
            match &mut self.transport {
                Transport::Single(connection) => connection.set_read_timeout(None)?,
                Transport::Cluster(router) => router.monitor_without_timeout()?,
            }
        }

        Ok(reply)
    }

    /// Binary-safe [`Client::execute`]: every argument is raw bytes, so values need not be
    /// UTF-8 (`RESTORE` payloads, images, encrypted blobs).
    ///
    /// On a sharded server the command is routed by its second argument; multi-key commands are
    /// not split on this path.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let reply = client.execute_binary(&[b"SET", b"k", &[0xff, 0x00, 0xfe]])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn execute_binary(&mut self, arguments: &[&[u8]]) -> Result<RespValue, Error> {
        self.ensure_open()?;

        let reply = match &mut self.transport {
            Transport::Single(connection) => connection.send_bytes(arguments)?,
            Transport::Cluster(router) => router.execute_binary(arguments)?,
        };

        match throw_error(reply) {
            Err(Error::Server(message)) if is_readonly(&message) && self.follows() => {
                if !self.follow_after_readonly()? {
                    return Err(Error::Server(message));
                }

                match &mut self.transport {
                    Transport::Single(connection) => throw_error(connection.send_bytes(arguments)?),
                    Transport::Cluster(_) => Err(Error::Server(message)),
                }
            }
            other => other,
        }
    }

    /// Pipelines commands: writes all of them, then reads one reply each, in one round trip.
    ///
    /// # Arguments
    /// * `commands` - each item is one command (name plus arguments). Must not be empty.
    ///
    /// # Returns
    /// One [`RespValue`] per command, in order. A failing command shows up as
    /// [`RespValue::Error`] at its index instead of failing the call, so check each reply. On a
    /// sharded server commands are grouped per shard and the replies still come back in the
    /// original order. A non-split multi-key command over several slots yields a `CROSSSLOT`
    /// error at its index and the rest still run.
    ///
    /// # Errors
    /// [`Error::Protocol`] for an empty list; [`Error::Io`] if the socket fails midway.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # use std::time::Duration;
    /// # fn run(client: &mut Client) -> Result<(), Error> {
    /// let replies = client.execute_many(&[&["SET", "a", "1"], &["INCR", "a"], &["GET", "a"]])?;
    /// assert_eq!(replies.len(), 3);
    /// # Ok(())
    /// # }
    /// ```
    pub fn execute_many(&mut self, commands: &[&[&str]]) -> Result<Vec<RespValue>, Error> {
        if commands.is_empty() {
            return Err(Error::Protocol(
                "a pipeline needs at least one command".to_owned(),
            ));
        }

        match &mut self.transport {
            Transport::Single(connection) => connection.send_many(commands),
            Transport::Cluster(router) => router.execute_many(commands),
        }
    }

    /// Writes one command and reads `replies` replies. A subscribe confirms each channel separately.
    pub(crate) fn run_replies<A: Args>(
        &mut self,
        arguments: A,
        replies: usize,
    ) -> Result<Vec<RespValue>, Error> {
        arguments.with_slice(|borrowed| self.run_replies_slice(borrowed, replies))
    }

    fn run_replies_slice(
        &mut self,
        borrowed: &[&str],
        replies: usize,
    ) -> Result<Vec<RespValue>, Error> {
        if is_subscription_command(borrowed) && !self.dedicated_subscriber {
            return self
                .ensure_pubsub(borrowed)?
                .run_replies_slice(borrowed, replies);
        }

        self.ensure_open()?;

        let values = match &mut self.transport {
            Transport::Single(connection) => connection.send_replies(borrowed, replies)?,
            Transport::Cluster(router) => router.run_replies(borrowed, replies)?,
        };

        if values.len() < replies {
            return Err(Error::Protocol("missing subscribe confirmation".to_owned()));
        }

        for value in &values {
            if let RespValue::Error(message) = value {
                return Err(Error::Server(message.clone()));
            }
        }

        self.remember_subscription(borrowed);

        Ok(values)
    }

    /// Sends one command built from any argument list; see [`Args`].
    #[inline]
    pub(crate) fn run<A: Args>(&mut self, arguments: A) -> Result<RespValue, Error> {
        arguments.with_slice(|borrowed| self.execute(borrowed))
    }

    /// Sends a command and hands the reply plus the command name (for error text) to `read`.
    #[inline]
    pub(crate) fn run_with<A: Args, T>(
        &mut self,
        arguments: A,
        read: impl FnOnce(&str, RespValue) -> Result<T, Error>,
    ) -> Result<T, Error> {
        arguments.with_slice(|borrowed| {
            let reply = self.execute(borrowed)?;

            read(borrowed.first().copied().unwrap_or_default(), reply)
        })
    }

    fn ensure_pubsub(&mut self, arguments: &[&str]) -> Result<&mut Client, Error> {
        if self.pubsub.is_none() {
            self.pubsub = Some(Box::new(self.open_pubsub(arguments)?));
        }

        Ok(self.pubsub.as_mut().unwrap())
    }
    fn open_pubsub(&self, arguments: &[&str]) -> Result<Client, Error> {
        let mut options = self.options.clone();

        options.discover_cluster = false;
        // A subscriber waits for pushes for as long as it takes; only an explicit
        // set_read_timeout (copied below) bounds its reads.
        options.command_timeout = None;

        if let Transport::Cluster(router) = &self.transport {
            let (host, port) = router.subscription_endpoint(arguments);

            options.host = host;
            options.port = port;
            options.seeds.clear();
        } else if self.addresses.len() > 1 && self.options.sentinel_master.is_none() {
            // The subscriber starts at the address the command socket is on.
            options.seeds = self.addresses.clone();
            options.seeds.rotate_left(self.current);
        }

        let mut client = Client::connect_with(options)?;

        client.dedicated_subscriber = true;
        client.on_lost = Arc::clone(&self.on_lost);
        client.on_restored = Arc::clone(&self.on_restored);
        client.reconnect_delay = self.reconnect_delay.clone();

        if let Some(timeout) = self.read_timeout {
            client.set_read_timeout(timeout)?;
        }

        Ok(client)
    }

    fn transport_is_broken(&self) -> bool {
        match &self.transport {
            Transport::Single(connection) => connection.is_broken(),
            Transport::Cluster(_) => false,
        }
    }

    // Only the socket is reopened; the command that hit the drop is not replayed, because its
    // outcome is unknown and it may be a write (clients/README.md limits C# retries to reads).
    fn ensure_open(&mut self) -> Result<(), Error> {
        let broken = self.transport_is_broken();

        if !broken {
            return Ok(());
        }

        if !self.options.reconnect {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "connection is closed",
            )));
        }

        self.reopen()
    }

    fn reopen(&mut self) -> Result<(), Error> {
        let (host, port) = self.addresses[self.current].clone();

        self.notify_lost(host, port);

        // Each attempt tries every address once, starting with the one after the address that
        // dropped, so a single address behaves as before and a moved primary is found. With
        // several seeds (or a coordinator name) the address that answers `ROLE` with `master`
        // comes first.
        let start = (self.current + 1) % self.addresses.len();
        let follow = self.follows();
        let mut attempt = 0_u32;

        let (opened, current) = loop {
            attempt += 1;

            match self.open_next(start, follow) {
                Ok(found) => break found,
                Err(error) => match self.next_delay(attempt) {
                    Some(delay) => std::thread::sleep(delay),
                    None => return Err(error),
                },
            }
        };

        self.settle(opened, current)
    }

    /// One reconnect attempt: the confirmed primary when `follow` is set and one is found, else
    /// the first address that answers.
    fn open_next(&mut self, start: usize, follow: bool) -> Result<(Connection, usize), Error> {
        if follow {
            let seeds = self.follow_seeds();

            if let Some(address) =
                find_primary(&seeds, start % seeds.len(), &mut None, &self.options)?
            {
                match Connection::open(&address.0, address.1, &self.options) {
                    Ok(connection) => return Ok((connection, self.locate(address))),
                    Err(Error::Io(_)) => {}
                    Err(error) => return Err(error),
                }
            }
        }

        open_first(&self.addresses, start, &self.options)
    }

    /// Restores `SELECT` and subscriptions on a freshly opened socket and makes it the
    /// connection.
    fn settle(&mut self, mut opened: Connection, current: usize) -> Result<(), Error> {
        if self.database != 0 {
            let index = self.database.to_string();

            throw_error(opened.send(&["SELECT", &index])?)?;
        }

        for command in &self.subscriptions {
            let borrowed: Vec<&str> = command.iter().map(String::as_str).collect();

            throw_error(opened.send(&borrowed)?)?;
        }

        self.transport = Transport::Single(opened);
        self.current = current;
        self.in_multi = false;

        let (host, port) = self.addresses[current].clone();

        self.notify_restored(host, port);

        Ok(())
    }

    /// True for a standalone client that has something to ask: several seeds or a coordinator
    /// name. A lone address behaves as it always did.
    fn follows(&self) -> bool {
        self.options.follow_primary
            && matches!(self.transport, Transport::Single(_))
            && (self.addresses.len() > 1 || self.options.sentinel_master.is_some())
    }

    /// Where the primary is asked for: the coordinators with a master name, else the seeds
    /// (plus any address found by an earlier follow).
    fn follow_seeds(&self) -> Vec<(String, u16)> {
        if self.options.sentinel_master.is_some() {
            self.options.addresses()
        } else {
            self.addresses.clone()
        }
    }

    /// The index of `address` in the connect list, which gains it when it is new (with a
    /// coordinator name the list is just the primary).
    fn locate(&mut self, address: (String, u16)) -> usize {
        if self.options.sentinel_master.is_some() {
            self.addresses = vec![address];

            return 0;
        }

        match self.addresses.iter().position(|known| *known == address) {
            Some(index) => index,
            None => {
                self.addresses.push(address);

                self.addresses.len() - 1
            }
        }
    }

    /// Moves the connection to the primary after a `READONLY` reply, asking again with the
    /// reconnect backoff until [`ClientOptions::max_reconnect_attempts`] rounds have passed.
    /// Returns true when the command may be sent again on the new socket: the refusal means it
    /// was not applied, except inside a transaction, whose earlier commands are lost with the
    /// old socket.
    #[cold]
    fn follow_after_readonly(&mut self) -> Result<bool, Error> {
        let retry = !self.in_multi;
        let refused = self.addresses[self.current].clone();
        // Kept across rounds: cleared once the refusing node says it is a replica, so its
        // promotion a moment later is taken instead of waiting out every round.
        let mut avoid = Some(refused.clone());
        let mut attempt = 0_u32;

        loop {
            attempt += 1;

            let seeds = self.follow_seeds();
            let start = if self.options.sentinel_master.is_some() {
                0
            } else {
                (self.current + 1) % seeds.len()
            };

            if let Some(address) = find_primary(&seeds, start, &mut avoid, &self.options)? {
                match Connection::open(&address.0, address.1, &self.options) {
                    Ok(opened) => {
                        self.notify_lost(refused.0.clone(), refused.1);

                        let index = self.locate(address);

                        self.settle(opened, index)?;

                        return Ok(retry);
                    }
                    Err(Error::Io(_)) => {}
                    Err(error) => return Err(error),
                }
            }

            match self.next_delay(attempt) {
                Some(delay) => std::thread::sleep(delay),
                None => return Ok(false),
            }
        }
    }

    fn send_single(&mut self, arguments: &[&str]) -> Result<RespValue, Error> {
        match &mut self.transport {
            Transport::Single(connection) => connection.send(arguments),
            Transport::Cluster(_) => Err(Error::Protocol(
                "a cluster client does not follow a primary".to_owned(),
            )),
        }
    }

    // Doubles per attempt and is capped at reconnect_max_delay, so a server that is still
    // restarting is not retried at a fixed rate.
    fn next_delay(&self, attempt: u32) -> Option<Duration> {
        if !self.options.reconnect {
            return None;
        }

        if let Some(delay) = &self.reconnect_delay {
            return delay(attempt);
        }

        if attempt >= self.options.max_reconnect_attempts {
            return None;
        }

        let multiplier = 2_u32.saturating_pow(attempt.saturating_sub(1));
        let delay = self.options.reconnect_base_delay.saturating_mul(multiplier);

        Some(delay.min(self.options.reconnect_max_delay))
    }

    fn notify_lost(&self, host: String, port: u16) {
        let notice = ConnectionNotice {
            host,
            port,
            subscriber: self.dedicated_subscriber,
        };

        if let Some(hook) = self.on_lost.lock().expect("connection hook").as_ref() {
            hook(notice);
        }
    }

    fn notify_restored(&self, host: String, port: u16) {
        let notice = ConnectionNotice {
            host,
            port,
            subscriber: self.dedicated_subscriber,
        };

        if let Some(hook) = self.on_restored.lock().expect("connection hook").as_ref() {
            hook(notice);
        }
    }

    fn remember_subscription(&mut self, arguments: &[&str]) {
        if !self.dedicated_subscriber || arguments.is_empty() {
            return;
        }

        let command = arguments[0].to_ascii_uppercase();
        let (subscribe, dropping) = match command.as_str() {
            "SUBSCRIBE" => ("SUBSCRIBE", false),
            "UNSUBSCRIBE" => ("SUBSCRIBE", true),
            "PSUBSCRIBE" => ("PSUBSCRIBE", false),
            "PUNSUBSCRIBE" => ("PSUBSCRIBE", true),
            "SSUBSCRIBE" => ("SSUBSCRIBE", false),
            "SUNSUBSCRIBE" => ("SSUBSCRIBE", true),
            _ => return,
        };

        if dropping && arguments.len() <= 1 {
            self.subscriptions
                .retain(|item| item.first().map(String::as_str) != Some(subscribe));

            return;
        }

        if dropping {
            for channel in &arguments[1..] {
                self.subscriptions.retain(|item| {
                    item.first().map(String::as_str) != Some(subscribe)
                        || item.get(1).map(String::as_str) != Some(*channel)
                });
            }

            return;
        }

        for channel in &arguments[1..] {
            let entry = vec![subscribe.to_owned(), (*channel).to_owned()];

            if !self.subscriptions.iter().any(|item| item == &entry) {
                self.subscriptions.push(entry);
            }
        }
    }

    pub(crate) fn integer<A: Args>(&mut self, arguments: A) -> Result<i64, Error> {
        self.run_with(arguments, |command, reply| match reply {
            RespValue::Integer(value) => Ok(value),
            other => Err(unexpected(command, &other)),
        })
    }

    pub(crate) fn flag<A: Args>(&mut self, arguments: A) -> Result<bool, Error> {
        Ok(self.integer(arguments)? > 0)
    }

    pub(crate) fn ok<A: Args>(&mut self, arguments: A) -> Result<(), Error> {
        self.run(arguments)?;

        Ok(())
    }

    pub(crate) fn text<A: Args>(&mut self, arguments: A) -> Result<String, Error> {
        self.run_with(arguments, |command, reply| {
            reply
                .into_string()?
                .ok_or_else(|| Error::Protocol(format!("{command} returned a null reply")))
        })
    }

    pub(crate) fn bulk_or_null<A: Args>(&mut self, arguments: A) -> Result<Option<String>, Error> {
        self.run(arguments)?.into_string()
    }

    pub(crate) fn bytes_or_null<A: Args>(
        &mut self,
        arguments: A,
    ) -> Result<Option<Vec<u8>>, Error> {
        self.run_with(arguments, |command, reply| match reply {
            RespValue::Null => Ok(None),
            RespValue::Bulk(bytes) => Ok(Some(bytes)),
            RespValue::Simple(text) => Ok(Some(text.into_bytes())),
            other => Err(unexpected(command, &other)),
        })
    }

    pub(crate) fn strings<A: Args>(&mut self, arguments: A) -> Result<Vec<String>, Error> {
        self.run_with(arguments, |command, reply| match reply {
            RespValue::Null => Ok(Vec::new()),
            RespValue::Array(items) => items
                .into_iter()
                .map(|item| {
                    item.into_string()?
                        .ok_or_else(|| Error::Protocol(format!("{command} returned a null bulk")))
                })
                .collect(),
            other => Err(unexpected(command, &other)),
        })
    }

    pub(crate) fn optional_strings<A: Args>(
        &mut self,
        arguments: A,
    ) -> Result<Vec<Option<String>>, Error> {
        self.run_with(arguments, |command, reply| match reply {
            RespValue::Null => Ok(Vec::new()),
            RespValue::Array(items) => items.into_iter().map(RespValue::into_string).collect(),
            other => Err(unexpected(command, &other)),
        })
    }

    pub(crate) fn optional_integer<A: Args>(&mut self, arguments: A) -> Result<Option<i64>, Error> {
        self.run_with(arguments, |command, reply| match reply {
            RespValue::Null => Ok(None),
            RespValue::Integer(value) => Ok(Some(value)),
            other => Err(unexpected(command, &other)),
        })
    }
}

fn read_ping(reply: RespValue) -> Result<String, Error> {
    if let Some(items) = reply.as_array() {
        if let Some(first) = items.first() {
            if first
                .as_string()?
                .is_some_and(|kind| kind.eq_ignore_ascii_case("pong"))
            {
                if items.len() > 1 {
                    return Ok(items[1].as_string()?.unwrap_or_default());
                }

                return Ok("PONG".to_owned());
            }
        }
    }

    reply
        .into_string()?
        .ok_or_else(|| Error::Protocol("PING returned a null reply".to_owned()))
}

fn finish_standalone(
    mut seed: Connection,
    options: ClientOptions,
    addresses: Vec<(String, u16)>,
    current: usize,
) -> Result<Client, Error> {
    if options.database != 0 {
        let index = options.database.to_string();

        throw_error(seed.send(&["SELECT", &index])?)?;
    }

    Ok(Client {
        transport: Transport::Single(seed),
        database: options.database,
        options,
        dedicated_subscriber: false,
        pubsub: None,
        read_timeout: None,
        on_lost: Arc::new(Mutex::new(None)),
        on_restored: Arc::new(Mutex::new(None)),
        reconnect_delay: None,
        subscriptions: Vec::new(),
        addresses,
        current,
        in_multi: false,
    })
}

/// A refused write: `READONLY` followed by a space or nothing.
fn is_readonly(message: &str) -> bool {
    message
        .strip_prefix("READONLY")
        .is_some_and(|rest| rest.is_empty() || rest.starts_with(' '))
}

fn is_subscription_command(arguments: &[&str]) -> bool {
    const NAMES: [&str; 6] = [
        "SUBSCRIBE",
        "UNSUBSCRIBE",
        "PSUBSCRIBE",
        "PUNSUBSCRIBE",
        "SSUBSCRIBE",
        "SUNSUBSCRIBE",
    ];

    arguments
        .first()
        .is_some_and(|name| NAMES.iter().any(|known| name.eq_ignore_ascii_case(known)))
}

pub(crate) fn unexpected(command: &str, reply: &RespValue) -> Error {
    Error::Protocol(format!("{command} returned {reply:?}"))
}

pub(crate) fn join(command: &str, arguments: &[&str]) -> Vec<String> {
    let mut values = Vec::with_capacity(arguments.len() + 1);

    values.push(command.to_owned());
    values.extend(arguments.iter().map(|argument| (*argument).to_owned()));

    values
}

fn mode_flag(mode: SetMode) -> Option<&'static str> {
    match mode {
        SetMode::Always => None,
        SetMode::IfNotExists => Some("NX"),
        SetMode::IfExists => Some("XX"),
    }
}

/// Decimal text of `value` in `buffer`, without allocating.
fn decimal(mut value: u128, buffer: &mut [u8; 40]) -> &str {
    let mut start = buffer.len();

    loop {
        start -= 1;
        buffer[start] = b'0' + (value % 10) as u8;
        value /= 10;

        if value == 0 {
            break;
        }
    }

    std::str::from_utf8(&buffer[start..]).expect("digits are ASCII")
}

pub(crate) fn unix_millis(time: SystemTime) -> Result<u128, Error> {
    time.duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis())
        .map_err(|_| Error::Protocol("time is before the Unix epoch".to_owned()))
}

pub(crate) fn duration_millis(ttl: Duration) -> u128 {
    let millis = ttl.as_millis();

    if ttl.subsec_nanos().is_multiple_of(1_000_000) {
        return millis;
    }

    millis.saturating_add(1)
}

pub(crate) fn duration_secs(ttl: Duration) -> u64 {
    let seconds = ttl.as_secs();

    if ttl.subsec_nanos() == 0 {
        return seconds;
    }

    seconds.saturating_add(1)
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};

    use std::net::{TcpListener, TcpStream};

    use std::thread;
    use std::time::Duration;

    use super::{Client, ClientOptions, Durability, Error, RespValue, read_ping};

    fn bound(stream: TcpStream) -> TcpStream {
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        stream
    }

    fn reply_standalone_cluster(server: &mut TcpStream) {
        let expected = b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n";
        let mut got = vec![0_u8; expected.len()];

        server.read_exact(&mut got).unwrap();
        assert_eq!(got, expected);
        server
            .write_all(b"-ERR cluster mode is disabled\r\n")
            .unwrap();
    }

    #[test]
    fn connect_asks_for_cluster_slots_then_is_quiet() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = thread::spawn(move || bound(crate::test_support::accept(&listener).0));
        let connecting = thread::spawn(move || Client::connect("127.0.0.1", port).unwrap());
        let mut server = accepted.join().unwrap();

        reply_standalone_cluster(&mut server);

        let mut client = connecting.join().unwrap();

        assert!(!client.is_cluster());
        thread::sleep(Duration::from_millis(50));
        server.set_nonblocking(true).unwrap();

        let mut peeked = [0_u8; 1];

        assert!(server.peek(&mut peeked).is_err());
        server.set_nonblocking(false).unwrap();

        let ping = thread::spawn(move || client.ping().unwrap());
        let mut got = [0_u8; 14];

        server.read_exact(&mut got).unwrap();

        assert_eq!(&got, b"*1\r\n$4\r\nPING\r\n");
        server.write_all(b"+PONG\r\n").unwrap();

        assert_eq!(ping.join().unwrap(), "PONG");
    }

    #[test]
    fn command_timeout_option_fails_a_call_whose_reply_never_comes() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = thread::spawn(move || bound(crate::test_support::accept(&listener).0));
        let mut client = Client::connect_with(ClientOptions {
            host: "127.0.0.1".to_owned(),
            port,
            discover_cluster: false,
            command_timeout: Some(Duration::from_millis(150)),
            ..ClientOptions::default()
        })
        .unwrap();
        let _server = accepted.join().unwrap();

        assert!(matches!(client.ping(), Err(crate::Error::Io(_))));
    }

    #[test]
    fn command_timeout_defaults_to_thirty_seconds() {
        assert_eq!(
            ClientOptions::default().command_timeout,
            Some(Duration::from_secs(30))
        );
    }

    #[test]
    fn a_blocking_command_waits_its_own_timeout_on_top_of_the_command_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let mut socket = bound(crate::test_support::accept(&listener).0);
            let mut got = [0_u8; 64];
            let _ = socket.read(&mut got).unwrap();
            // An empty list: the server answers when BLPOP's 1 s runs out.
            thread::sleep(Duration::from_millis(1000));
            socket.write_all(b"*-1\r\n").unwrap();
            // PING is never answered.
            let _ = socket.read(&mut got).unwrap();
            socket
        });
        let mut client = Client::connect_with(ClientOptions {
            host: "127.0.0.1".to_owned(),
            port,
            discover_cluster: false,
            command_timeout: Some(Duration::from_millis(200)),
            ..ClientOptions::default()
        })
        .unwrap();

        assert_eq!(client.blpop(&["k"], Duration::from_secs(1)).unwrap(), None);
        // The plain command timeout is back for the next command.
        let started = std::time::Instant::now();
        assert!(matches!(client.ping(), Err(crate::Error::Io(_))));
        assert!(started.elapsed() < Duration::from_secs(2));

        let _server = server.join().unwrap();
    }

    #[test]
    fn discover_cluster_off_sends_nothing_until_the_first_command() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = thread::spawn(move || bound(crate::test_support::accept(&listener).0));
        let mut client = Client::connect_with(ClientOptions {
            host: "127.0.0.1".to_owned(),
            port,
            discover_cluster: false,
            ..ClientOptions::default()
        })
        .unwrap();
        let mut server = accepted.join().unwrap();

        thread::sleep(Duration::from_millis(50));
        server.set_nonblocking(true).unwrap();

        let mut peeked = [0_u8; 1];

        assert!(server.peek(&mut peeked).is_err());
        server.set_nonblocking(false).unwrap();

        let ping = thread::spawn(move || client.ping().unwrap());
        let mut got = [0_u8; 14];

        server.read_exact(&mut got).unwrap();

        assert_eq!(&got, b"*1\r\n$4\r\nPING\r\n");
        server.write_all(b"+PONG\r\n").unwrap();

        assert_eq!(ping.join().unwrap(), "PONG");
    }

    #[test]
    fn a_server_error_leaves_the_next_command_usable() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = thread::spawn(move || bound(crate::test_support::accept(&listener).0));
        let connecting = thread::spawn(move || Client::connect("127.0.0.1", port).unwrap());
        let mut server = accepted.join().unwrap();

        reply_standalone_cluster(&mut server);

        let mut client = connecting.join().unwrap();
        let error = thread::spawn(move || {
            let error = client.get_string("missing").unwrap_err();
            let pong = client.ping().unwrap();

            (error.to_string(), pong)
        });

        read_some(&mut server);
        server.write_all(b"-ERR no such key\r\n").unwrap();
        read_some(&mut server);
        server.write_all(b"+PONG\r\n").unwrap();

        let (message, pong) = error.join().unwrap();

        assert_eq!(message, "ERR no such key");
        assert_eq!(pong, "PONG");
    }

    #[test]
    fn password_is_sent_as_auth() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = thread::spawn(move || bound(crate::test_support::accept(&listener).0));
        let connecting = thread::spawn(move || {
            Client::connect_with(ClientOptions {
                host: "127.0.0.1".to_owned(),
                port,
                password: Some("secret".to_owned()),
                ..ClientOptions::default()
            })
            .unwrap()
        });

        let mut server = accepted.join().unwrap();
        let expected = b"*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n";
        let mut got = vec![0_u8; expected.len()];

        server.read_exact(&mut got).unwrap();

        assert_eq!(got, expected);
        server.write_all(b"+OK\r\n").unwrap();
        reply_standalone_cluster(&mut server);
        connecting.join().unwrap();
    }

    #[test]
    fn durability_is_sent_after_auth_with_and_without_timeout() {
        let cases = [
            (
                Durability {
                    replicas: 2,
                    timeout_ms: None,
                },
                &b"*4\r\n$6\r\nCLIENT\r\n$10\r\nDURABILITY\r\n$8\r\nREPLICAS\r\n$1\r\n2\r\n"[..],
            ),
            (
                Durability {
                    replicas: 1,
                    timeout_ms: Some(250),
                },
                &b"*6\r\n$6\r\nCLIENT\r\n$10\r\nDURABILITY\r\n$8\r\nREPLICAS\r\n$1\r\n1\r\n$7\r\nTIMEOUT\r\n$3\r\n250\r\n"[..],
            ),
        ];

        for (durability, expected) in cases {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            let accepted = thread::spawn(move || bound(crate::test_support::accept(&listener).0));
            let connecting = thread::spawn(move || {
                Client::connect_with(ClientOptions {
                    host: "127.0.0.1".to_owned(),
                    port,
                    password: Some("secret".to_owned()),
                    durability: Some(durability),
                    ..ClientOptions::default()
                })
            });

            let mut server = accepted.join().unwrap();
            let auth = b"*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n";
            let mut got = vec![0_u8; auth.len()];

            server.read_exact(&mut got).unwrap();
            assert_eq!(got, auth);
            server.write_all(b"+OK\r\n").unwrap();
            got = vec![0_u8; expected.len()];
            server.read_exact(&mut got).unwrap();
            assert_eq!(got, expected);
            server
                .write_all(b"-ERR CLIENT DURABILITY needs replication on this server\r\n")
                .unwrap();

            let error = connecting.join().unwrap().err().unwrap();

            assert!(
                matches!(error, Error::Server(ref m) if m.contains("needs replication")),
                "{error:?}"
            );
        }
    }

    #[test]
    fn database_is_selected_after_auth() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = thread::spawn(move || bound(crate::test_support::accept(&listener).0));
        let connecting = thread::spawn(move || {
            Client::connect_with(ClientOptions {
                host: "127.0.0.1".to_owned(),
                port,
                password: Some("secret".to_owned()),
                database: 3,
                ..ClientOptions::default()
            })
            .unwrap()
        });

        let mut server = accepted.join().unwrap();
        let auth = b"*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n";
        let select = b"*2\r\n$6\r\nSELECT\r\n$1\r\n3\r\n";
        let mut got = vec![0_u8; auth.len()];

        server.read_exact(&mut got).unwrap();
        assert_eq!(got, auth);
        server.write_all(b"+OK\r\n").unwrap();
        reply_standalone_cluster(&mut server);
        got = vec![0_u8; select.len()];
        server.read_exact(&mut got).unwrap();
        assert_eq!(got, select);
        server.write_all(b"+OK\r\n").unwrap();
        assert_eq!(connecting.join().unwrap().database(), 3);
    }

    #[test]
    fn subscribe_opens_a_second_socket_and_leaves_ping_on_the_first() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let connecting = thread::spawn(move || Client::connect("127.0.0.1", port).unwrap());
        let mut command = bound(crate::test_support::accept(&listener).0);

        reply_standalone_cluster(&mut command);

        let mut client = connecting.join().unwrap();
        let working = thread::spawn(move || {
            let confirms = client.subscribe(&["news"]).unwrap();
            let pong = client.ping().unwrap();

            (confirms.len(), pong)
        });

        let mut subscriber = bound(crate::test_support::accept(&listener).0);
        let expected = b"*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nnews\r\n";
        let mut got = vec![0_u8; expected.len()];

        subscriber.read_exact(&mut got).unwrap();
        assert_eq!(got, expected);
        subscriber
            .write_all(b"*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n")
            .unwrap();

        let mut ping = [0_u8; 14];

        command.read_exact(&mut ping).unwrap();
        assert_eq!(&ping, b"*1\r\n$4\r\nPING\r\n");
        command.write_all(b"+PONG\r\n").unwrap();

        let (confirms, pong) = working.join().unwrap();

        assert_eq!(confirms, 1);
        assert_eq!(pong, "PONG");
    }

    #[test]
    fn explicit_subscriber_uses_one_extra_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let connecting = thread::spawn(move || Client::connect("127.0.0.1", port).unwrap());
        let mut command = bound(crate::test_support::accept(&listener).0);

        reply_standalone_cluster(&mut command);

        let client = connecting.join().unwrap();
        let working = thread::spawn(move || {
            let mut subscriber = client.subscriber().unwrap();

            subscriber.subscribe(&["news"]).unwrap();
        });

        let mut subscriber = bound(crate::test_support::accept(&listener).0);
        let expected = b"*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nnews\r\n";
        let mut got = vec![0_u8; expected.len()];

        subscriber.read_exact(&mut got).unwrap();
        assert_eq!(got, expected);
        subscriber
            .write_all(b"*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n")
            .unwrap();
        working.join().unwrap();
        listener.set_nonblocking(true).unwrap();

        assert!(listener.accept().is_err());
    }

    #[test]
    fn the_command_after_a_dropped_socket_reconnects() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let connecting = thread::spawn(move || {
            Client::connect_with(ClientOptions {
                host: "127.0.0.1".to_owned(),
                port,
                discover_cluster: false,
                reconnect_base_delay: std::time::Duration::ZERO,
                reconnect_max_delay: std::time::Duration::ZERO,
                ..ClientOptions::default()
            })
            .unwrap()
        });

        let accepted = bound(crate::test_support::accept(&listener).0);

        drop(accepted);

        let mut client = connecting.join().unwrap();
        let failed = thread::spawn(move || {
            assert!(client.ping().is_err());

            client.ping().unwrap()
        });

        let mut again = bound(crate::test_support::accept(&listener).0);
        let mut got = [0_u8; 14];

        again.read_exact(&mut got).unwrap();
        assert_eq!(&got, b"*1\r\n$4\r\nPING\r\n");
        again.write_all(b"+PONG\r\n").unwrap();

        assert_eq!(failed.join().unwrap(), "PONG");
    }

    fn closed_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();

        listener.local_addr().unwrap().port()
    }

    fn answer_ping(listener: &TcpListener) {
        let mut socket = bound(crate::test_support::accept(listener).0);
        let mut got = [0_u8; 14];

        socket.read_exact(&mut got).unwrap();
        assert_eq!(&got, b"*1\r\n$4\r\nPING\r\n");
        socket.write_all(b"+PONG\r\n").unwrap();
    }

    #[test]
    fn connect_skips_a_seed_that_cannot_be_reached() {
        let dead = closed_port();
        let live = TcpListener::bind("127.0.0.1:0").unwrap();
        let live_port = live.local_addr().unwrap().port();
        let connecting = thread::spawn(move || {
            let mut client = Client::connect_with(ClientOptions {
                seeds: vec![
                    ("127.0.0.1".to_owned(), dead),
                    ("127.0.0.1".to_owned(), live_port),
                ],
                discover_cluster: false,
                connect_timeout: Duration::from_millis(500),
                ..ClientOptions::default()
            })
            .unwrap();

            client.ping().unwrap()
        });

        answer_ping(&live);

        assert_eq!(connecting.join().unwrap(), "PONG");
    }

    #[test]
    fn a_dropped_connection_moves_on_to_the_next_seed() {
        let first = TcpListener::bind("127.0.0.1:0").unwrap();
        let second = TcpListener::bind("127.0.0.1:0").unwrap();
        let ports = [
            first.local_addr().unwrap().port(),
            second.local_addr().unwrap().port(),
        ];
        let connecting = thread::spawn(move || {
            Client::connect_with(ClientOptions {
                seeds: ports
                    .iter()
                    .map(|port| ("127.0.0.1".to_owned(), *port))
                    .collect(),
                discover_cluster: false,
                // The scripted seeds only answer PING; the rotation is what is checked.
                follow_primary: false,
                reconnect_base_delay: Duration::ZERO,
                reconnect_max_delay: Duration::ZERO,
                ..ClientOptions::default()
            })
            .unwrap()
        });

        drop(bound(crate::test_support::accept(&first).0));

        let mut client = connecting.join().unwrap();
        let restored = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = std::sync::Arc::clone(&restored);

        client.on_connection_restored(move |notice| seen.lock().unwrap().push(notice.port));

        let failed = thread::spawn(move || {
            assert!(client.ping().is_err());

            client.ping().unwrap()
        });

        // The first seed still accepts, but the reconnect must go to the second one.
        answer_ping(&second);

        assert_eq!(failed.join().unwrap(), "PONG");
        assert_eq!(*restored.lock().unwrap(), vec![ports[1]]);
    }

    #[test]
    fn an_empty_seed_host_is_refused() {
        let error = Client::connect_with(ClientOptions {
            seeds: vec![(String::new(), 6379)],
            ..ClientOptions::default()
        })
        .err()
        .unwrap();

        assert!(matches!(error, crate::Error::Protocol(_)));
    }

    #[test]
    fn ping_reads_a_pubsub_array() {
        let reply = RespValue::Array(vec![
            RespValue::Bulk(b"pong".to_vec()),
            RespValue::Bulk(b"hello".to_vec()),
        ]);

        assert_eq!(read_ping(reply).unwrap(), "hello");
    }

    fn read_some(stream: &mut TcpStream) {
        let mut buffer = [0_u8; 64];

        let read = stream.read(&mut buffer).unwrap();

        assert!(read > 0);
    }
}
