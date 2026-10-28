# Connection model

This page says who chooses what when an application talks to Ruvio: the
server, the client library, or the application. It covers servers, shards,
logical databases, and the clients (Rust and C# as examples). Replicas, failover and
clusters of several machines are on [Cluster and failover](CLUSTER.md).

## The three layers

| Layer | What it is | Who chooses |
| --- | --- | --- |
| Server address | One `host:port` of a running Ruvio process | The application's configuration (per environment) |
| Shard | One slot owner inside a Ruvio process, with its own port | The client, from one seed address |
| Logical database | A numbered key space (`SELECT 0..databases-1`) | The application, once, when it opens a connection |

## Servers

A Ruvio process is one server. The application is configured with the address
of the server it should use, for example from a connection string. Two
independent servers mean two clients, each opened with its own address.

A client moves to another process only within one deployment: to the new
primary of a replication group after a failover (when it was given several seed
addresses or the coordinators), and to the group that owns a key in a
multi-node cluster (`MOVED`, `TRYAGAIN` while a range moves). See [Cluster and failover](CLUSTER.md).

## Shards

`shards` (default 1) starts that many shard owners in one process. Shard `i`
listens on the bind port plus `i` and owns a range of the 16,384 hash slots.
A command for a key another shard owns is forwarded to that shard and
answered on the same connection; a multi-key command that spans slots gets
`CROSSSLOT`. The server does not send `MOVED` for keys.

The application does not pick a shard. It gives the client one address. On
connect, the client sends `CLUSTER SLOTS`, learns every shard, opens a socket
to each as needed, and sends every key to the shard that owns
`hash_slot(key)`. All six first-party clients do this. A single-shard server
answers as a standalone server and the client skips routing.

The application's only part is key design. Commands that touch several keys
need those keys in one slot, which a hash tag does: `{user:1}:profile` and
`{user:1}:cart` hash on `user:1`.

### Which shard holds a key

- `CLUSTER KEYSLOT key` returns the key's hash slot (CRC16 of the key, or of the part inside the first `{...}` hash tag, modulo 16,384).
- `CLUSTER SLOTS` lists every shard with its slot range, host and port. The shard that owns the slot is the one whose range contains it. With 2 shards, slots 0-8191 belong to the first and 8192-16383 to the second.
- Clients do this lookup themselves and send each key to its owner; the application normally never needs to know.
- Keys with the same hash tag always share a slot, so they share a shard: `{order:7}:items` and `{order:7}:total`.

### Commands that take several keys

`MGET`, `MSET`, `DEL`, `EXISTS`, `UNLINK`, `TOUCH`, `EVAL`, `EVALSHA`, `FCALL`, `MULTI`/`EXEC` and similar commands need all their keys in one slot, as in Redis Cluster. Otherwise the server answers `CROSSSLOT Keys in request do not hash to the same slot` and does nothing; keys that merely sit on one shard but in different slots are refused too. Put a hash tag in the keys that belong together. The first-party clients split `MGET`, `MSET`, `DEL`, `EXISTS`, `UNLINK` and `TOUCH` per slot for you (not atomic, see [CLIENT_BEHAVIOR.md](CLIENT_BEHAVIOR.md)); `EVAL` and the other commands are refused by the client before sending. A script may only use the keys it declared (`ruvio.call` on an undeclared key fails), and a script with no declared keys runs on the connection's shard.

## Logical databases

With `shards = 1`, the server has `databases` numbered key spaces (16 by
default, 1 to 256). A connection starts on database 0 and `SELECT n` changes
it for that connection only. `user:1` in database 0 and in database 1 are
different keys. See [LOGICAL_DATABASES.md](LOGICAL_DATABASES.md).

With `shards > 1` only database 0 exists, as in Redis Cluster. The clients
reject `database != 0` against a sharded server. So a server has either many
databases and one shard, or many shards and one database. It never has both.

## Clients

A client handle is bound to one database. The database is chosen when the
connection is opened, and the application does not name it on each command.

Rust (`clients/ruvio-client`):

```rust
// Server address comes from application config. Shards are discovered.
let mut cache = Client::connect("10.0.0.5", 6379)?;
cache.set("k", "v")?;

// A single-shard server with logical databases: one handle per database.
let mut sessions = Client::connect_database("10.0.0.5", 6379, 1)?;
```

`Client` is synchronous and takes `&mut self`. One handle is one command
connection (plus a second socket for Pub/Sub). `select` and `into_database`
switch the database of that same connection; they do not open another socket.

C# (`clients/csharp`):

```csharp
await using var connection = await RuvioConnection.ConnectAsync(
    new RuvioClientOptions { Host = "10.0.0.5", Port = 6379 });

IRuvioClient cache = await connection.GetDatabaseAsync(0);
IRuvioClient sessions = await connection.GetDatabaseAsync(1);
```

`RuvioConnection` owns the connection settings, reconnect, events, and
disposal. `GetDatabaseAsync(n)` returns one cached client per database, each
with its own command connection that selected `n` when it opened. Asking for
the same `n` again returns the same client. On a sharded server only `0` is
allowed. ASP.NET Core registration exposes the connection and the default
database client as singletons.

Both clients end up with the same rule: **one handle, one database**.

## What application code does

- Reads the server address (and a database number, if it uses one) from
  configuration.
- Opens the client or connection once at start-up and shares it.
- Uses key prefixes (`session:`, `cache:`) to separate kinds of data. This
  works on every server shape. Database numbers do not exist on sharded
  servers, do not isolate memory, ACL, or `FLUSHALL`, and are best kept to a
  few well-known roles on a single-shard server.
- Uses hash tags when several keys must be handled together on a sharded
  server.

It does not choose shards, and it does not send `SELECT` between commands.

## Reconnect

A dropped socket, including one shard's socket, is opened again before the
next command. Pass the database in the options when you open the client, so a
new socket is opened on that database. Do not rely on a later `select` to
survive a reconnect; this page does not claim that. See
[clients/README.md](../clients/README.md).

## Socket I/O stays on the shard thread

A connection's `recv`, parse, execute, format and `send` all run on its shard's thread. Moving the socket
calls to dedicated I/O threads measured slower on Linux and macOS, so there are none.
