// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.commands;

import io.github.salihcantekin.ruvio.Pipeline;
import io.github.salihcantekin.ruvio.RuvioAsync;
import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.RuvioServerException;
import io.github.salihcantekin.ruvio.RuvioPubSub;
import io.github.salihcantekin.ruvio.Transaction;
import io.github.salihcantekin.ruvio.args.ClientKillArgs;
import io.github.salihcantekin.ruvio.args.ClientType;
import io.github.salihcantekin.ruvio.args.InfoSection;
import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Cmd;
import io.github.salihcantekin.ruvio.internal.Reply;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Every typed Ruvio command as an asynchronous method. Implemented by {@link RuvioAsync} (the shared
 * multiplexed connection), {@link Pipeline} and {@link Transaction}. This class adds connection,
 * ACL, server, cluster and publish commands to the data-type families it inherits.
 *
 * @param <K> key type (also hash fields and stream field names)
 * @param <V> value type (also members, list elements and stream field values)
 */
public abstract class RuvioCommands<K, V> extends ScriptingCommands<K, V> {
    /**
     * Creates the command family with the given codecs.
     *
     * @param keys the key codec
     * @param values the value codec
     */
    protected RuvioCommands(RuvioCodec<K> keys, RuvioCodec<V> values) {
        super(keys, values);
    }

    @SuppressWarnings("unchecked")
    static final Reply<Map<String, Object>> TEXT_MAP = Reply.of((tree, q) -> Reply.fieldMap(Reply.text(tree)));

    @SuppressWarnings("unchecked")
    static final Reply<List<Object>> TEXT_LIST = Reply.of((tree, q) -> (List<Object>) Reply.text(Reply.list(tree)));

    // ---- connection ------------------------------------------------------------------------

    /**
     * Sends {@code RUVIO}: a Ruvio-only greeting.
     *
     * @return the text {@code PONG is taken}
     */
    public CompletableFuture<String> ruvio() {
        return c0(Cmd.RUVIO, Reply.STRING);
    }

    /**
     * Sends {@code RUVIO argument}. Only {@code OWNER} is accepted.
     *
     * @param argument {@code OWNER} (any case)
     * @return a one-line description of the shard rule: {@code the core that owns the slot does the work}
     * @throws RuvioServerException {@code ERR RUVIO expects zero arguments or OWNER} for any other word
     */
    public CompletableFuture<String> ruvio(String argument) {
        return send(args().w(Cmd.RUVIO).s(argument), Reply.STRING);
    }

    /**
     * Sends {@code RUVIO MOVESLOTS first-last shard}: moves the hash slots {@code first..last} to {@code shard} (counted from 0) of the same server, which keeps serving them meanwhile. Completes once the move has started; {@link #moveSlotsStatus()} follows it. One move runs at a time, and every slot of the range must be on one shard.
     *
     * @param first first hash slot of the range (0-16383)
     * @param last last hash slot of the range, inclusive
     * @param shard shard of the same server to move it to
     * @return a future that completes with no value once the move has started
     * @throws RuvioServerException when the range is already on that shard, spans shards, another move runs, the server has one shard, or replication is on
     */
    public CompletableFuture<Void> moveSlots(int first, int last, int shard) {
        return send(args().w(Cmd.RUVIO).s("MOVESLOTS").s(first + "-" + last).s(Integer.toString(shard)), Reply.OK);
    }

    /**
     * Sends {@code RUVIO MOVESLOTS STATUS}: the running or last slot move. {@code state} is {@code none}, {@code copying}, {@code cleaning}, {@code cancelling}, {@code done} or {@code cancelled}; the other names are {@code first}, {@code last}, {@code from}, {@code to}, {@code keys}, {@code rounds}, {@code elapsed_ms}, {@code freeze_ms} and {@code error}.
     *
     * @return name to value, as text
     */
    public CompletableFuture<Map<String, String>> moveSlotsStatus() {
        return send(args().w(Cmd.RUVIO).s("MOVESLOTS").s("STATUS"), Reply.STRING_MAP);
    }

    /**
     * Sends {@code PING}: checks that the server answers.
     *
     * <pre>{@code
     * String pong = client.ping();
     * }</pre>
     *
     * @return {@code PONG}
     */
    public CompletableFuture<String> ping() {
        return c0(Cmd.PING, Reply.STRING);
    }

    /**
     * Sends {@code PING message}.
     *
     * @param message the text to echo back
     * @return the same message
     */
    public CompletableFuture<V> ping(V message) {
        return send(args().w(Cmd.PING).v(message), r(Reply.VALUE));
    }

    /**
     * Sends {@code ECHO message}.
     *
     * @param message the value to echo back
     * @return the same message
     */
    public CompletableFuture<V> echo(V message) {
        return send(args().w(Cmd.ECHO).v(message), r(Reply.VALUE));
    }

    /**
     * Sends {@code HELLO protover}: chooses the protocol of this connection. The default is RESP2; {@code 3} enables RESP3 (null, boolean and map types, and push frames for {@code CLIENT TRACKING}, which go to the {@code pushListener} of the options). The client reads both. On the shared connection this changes the protocol for every caller.
     *
     * @param protocol {@code 2} or {@code 3}
     * @return the server properties (name, version, protocol, mode, role and so on) as a map
     * @throws RuvioServerException {@code NOPROTO} for any other protocol number (nothing changes)
     */
    public CompletableFuture<Map<String, Object>> hello(int protocol) {
        return send(args().w(Cmd.HELLO).l(protocol), TEXT_MAP);
    }

    /**
     * Sends {@code HELLO protover AUTH username password}: chooses the protocol and authenticates in one step.
     *
     * @param protocol {@code 2} or {@code 3}
     * @param username the ACL user name (use {@code default} for the default user)
     * @param password the password
     * @return the server properties as a map
     * @throws RuvioServerException {@code NOPROTO} for a bad protocol; {@code WRONGPASS} for bad credentials (the previous protocol stays)
     */
    public CompletableFuture<Map<String, Object>> hello(int protocol, String username, String password) {
        return send(args().w(Cmd.HELLO).l(protocol).s("AUTH").s(username).s(password), TEXT_MAP);
    }

    /**
     * Sends {@code AUTH password}: authenticates as the default user. The client already sends the password from {@code RuvioOptions} on every (re)connect; call this only to change identity later.
     *
     * @param password the password
     * @throws RuvioServerException {@code WRONGPASS} for a wrong password; {@code ERR} when the server has no password configured
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> auth(String password) {
        return send(args().w(Cmd.AUTH).s(password), Reply.OK);
    }

    /**
     * Sends {@code AUTH username password}: authenticates as an ACL user.
     *
     * @param username the ACL user name
     * @param password the password
     * @throws RuvioServerException {@code WRONGPASS} for bad credentials; {@code ERR max number of connections for this user reached} when the user's connection limit is full
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> auth(String username, String password) {
        return send(args().w(Cmd.AUTH).s(username).s(password), Reply.OK);
    }

    /**
     * Sends {@code SELECT db}: switches the logical database of the connection. On the shared client this changes the database for every caller, and the client selects it again after a reconnect. For one fixed database use {@code RuvioOptions.Builder#database}. Cluster mode allows only database 0.
     *
     * @param database the index, from 0 to {@code databases - 1} (16 by default)
     * @throws RuvioServerException {@code ERR DB index is out of range}
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> select(int database) {
        return send(args().w(Cmd.SELECT).l(database), Reply.OK);
    }

    /**
     * Sends {@code COMMAND}: describes every command the server knows (Redis 7 shape).
     *
     * @return one entry per command (name, arity, flags, key positions), with text decoded as strings
     */
    public CompletableFuture<List<Object>> command() {
        return c0(Cmd.COMMAND, TEXT_LIST);
    }

    /**
     * Sends {@code COMMAND COUNT}.
     *
     * @return how many commands the server supports
     */
    public CompletableFuture<Long> commandCount() {
        return send(args().w(Cmd.COMMAND).s("COUNT"), Reply.LONG);
    }

    /**
     * Sends {@code COMMAND INFO name [name ...]}.
     *
     * @param names command names such as {@code get}
     * @return one entry per name, in the shape of {@link #command()}; {@code null} for an unknown name
     */
    public CompletableFuture<List<Object>> commandInfo(String... names) {
        return send(args().w(Cmd.COMMAND).s("INFO").strings(names), TEXT_LIST);
    }

    /**
     * Sends {@code COMMAND DOCS [name ...]}.
     *
     * @param names command names; none returns all
     * @return the documentation tree as the server sends it
     */
    public CompletableFuture<Object> commandDocs(String... names) {
        return send(args().w(Cmd.COMMAND).s("DOCS").strings(names), Reply.TEXT);
    }

    /**
     * Sends {@code CLIENT GETNAME}.
     *
     * @return the connection name, or {@code null} when none is set
     */
    public CompletableFuture<String> clientGetname() {
        return send(args().w(Cmd.CLIENT).s("GETNAME"), Reply.STRING);
    }

    /**
     * Sends {@code CLIENT SETNAME name}. The name is per connection. The options' {@code clientName} sets it on every (re)connect.
     *
     * @param name a name without spaces
     * @throws RuvioServerException {@code ERR} when the name contains spaces or newlines
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> clientSetname(String name) {
        return send(args().w(Cmd.CLIENT).s("SETNAME").s(name), Reply.OK);
    }

    /**
     * Sends {@code CLIENT TRACKING ON|OFF}: server-assisted client caching. While on, the server remembers the keys this connection read with {@code GET}/{@code MGET} (also misses) and sends one {@code invalidate} push when such a key is written. It works on one shard only. Requires RESP3, so call {@link #hello(int)} with {@code 3} first and set a {@code pushListener} in the options to receive the frames. The options {@code REDIRECT}, {@code BCAST}, {@code PREFIX}, {@code OPTIN}, {@code OPTOUT} and {@code NOLOOP} are not supported.
     *
     * @param on {@code true} to start tracking, {@code false} to stop and forget the keys
     * @throws RuvioServerException {@code ERR} when the connection is not RESP3
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> clientTracking(boolean on) {
        return send(args().w(Cmd.CLIENT).s("TRACKING").s(on ? "ON" : "OFF"), Reply.OK);
    }

    /**
     * Sends {@code CLIENT DURABILITY REPLICAS n}. While set, the server holds this connection's replies until {@code replicas} replicas have every earlier write on disk. After the default 5000 ms the replies of the batch become {@code NOREPLICAS} errors and the writes stay applied. The options' {@code durability} sets it on every (re)connect.
     *
     * @param replicas the replicas that must hold each write; 0 turns it off
     * @throws RuvioServerException {@code ERR} on a server without replication or inside {@code MULTI}
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> clientDurability(long replicas) {
        return send(args().w(Cmd.CLIENT).s("DURABILITY").s("REPLICAS").l(replicas), Reply.OK);
    }

    /**
     * Sends {@code CLIENT DURABILITY REPLICAS n TIMEOUT ms}. Same as {@link #clientDurability(long)} with a longest wait.
     *
     * @param replicas the replicas that must hold each write; 0 turns it off
     * @param timeoutMillis the longest wait in milliseconds
     * @throws RuvioServerException {@code ERR} on a server without replication or inside {@code MULTI}
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> clientDurability(long replicas, long timeoutMillis) {
        return send(args().w(Cmd.CLIENT).s("DURABILITY").s("REPLICAS").l(replicas).s("TIMEOUT").l(timeoutMillis), Reply.OK);
    }

    /**
     * Sends {@code CLIENT ID}.
     *
     * @return the id the server gave this connection
     */
    public CompletableFuture<Long> clientId() {
        return send(args().w(Cmd.CLIENT).s("ID"), Reply.LONG);
    }

    /**
     * Sends {@code CLIENT LIST}.
     *
     * @return one text line per connection ({@code key=value} fields), lines separated by newlines
     */
    public CompletableFuture<String> clientList() {
        return send(args().w(Cmd.CLIENT).s("LIST"), Reply.STRING);
    }

    /**
     * Sends {@code CLIENT LIST TYPE type}. Ruvio checks the type but lists every connection.
     * Example: {@code client.clientList(ClientType.NORMAL)}.
     *
     * @param type the connection type
     * @return one text line per connection ({@code key=value} fields), lines separated by newlines
     */
    public CompletableFuture<String> clientList(ClientType type) {
        return send(args().w(Cmd.CLIENT).s("LIST").w(Cmd.TYPE).w(type.fragment()), Reply.STRING);
    }

    /**
     * Sends {@code CLIENT INFO}.
     *
     * @return one text line with the fields of this connection
     */
    public CompletableFuture<String> clientInfo() {
        return send(args().w(Cmd.CLIENT).s("INFO"), Reply.STRING);
    }

    /**
     * Sends {@code CLIENT KILL [ID id] [ADDR ip:port] [SKIPME yes|no]}: closes connections that match.
     *
     * <pre>{@code
     * long closed = client.clientKill(ClientKillArgs.id(42));
     * }</pre>
     *
     * @param filter the connection id and/or address to match; {@code skipMe(false)} also allows closing the caller's own connection
     * @return how many connections were closed
     * @throws RuvioServerException {@code ERR} for a syntax error
     */
    public CompletableFuture<Long> clientKill(ClientKillArgs filter) {
        Args a = args().w(Cmd.CLIENT).s("KILL");
        filter.write(a);
        return send(a, Reply.LONG);
    }

    /**
     * Sends the old form {@code CLIENT KILL addr}: closes one connection by address.
     *
     * @param addr the client address as {@code ip:port}
     * @throws RuvioServerException {@code ERR No such client} when nothing matches
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> clientKill(String addr) {
        return send(args().w(Cmd.CLIENT).s("KILL").s(addr), Reply.OK);
    }

    /**
     * Sends {@code SLOWLOG GET}: the slowest recent commands of the shard that answers.
     *
     * @return entries as lists (id, time, duration in microseconds, command arguments), newest first
     */
    public CompletableFuture<List<Object>> slowlogGet() {
        return send(args().w(Cmd.SLOWLOG).s("GET"), TEXT_LIST);
    }

    /**
     * Sends {@code SLOWLOG GET count}.
     *
     * @param count the most entries to return
     * @return the newest entries, as in {@link #slowlogGet()}
     */
    public CompletableFuture<List<Object>> slowlogGet(long count) {
        return send(args().w(Cmd.SLOWLOG).s("GET").l(count), TEXT_LIST);
    }

    /**
     * Sends {@code SLOWLOG LEN}.
     *
     * @return the number of entries kept
     */
    public CompletableFuture<Long> slowlogLen() {
        return send(args().w(Cmd.SLOWLOG).s("LEN"), Reply.LONG);
    }

    /**
     * Sends {@code SLOWLOG RESET}: empties the slow log.
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> slowlogReset() {
        return send(args().w(Cmd.SLOWLOG).s("RESET"), Reply.OK);
    }

    /**
     * Sends {@code LATENCY LATEST}. Ruvio keeps no latency events, so the answer is always empty.
     *
     * @return an empty list
     */
    public CompletableFuture<List<Object>> latencyLatest() {
        return send(args().w(Cmd.LATENCY).s("LATEST"), TEXT_LIST);
    }

    // ---- ACL -------------------------------------------------------------------------------

    /**
     * Sends {@code ACL WHOAMI}.
     *
     * @return the user name of this connection
     */
    public CompletableFuture<String> aclWhoami() {
        return send(args().w(Cmd.ACL).s("WHOAMI"), Reply.STRING);
    }

    /**
     * Sends {@code ACL USERS}.
     *
     * @return the names of all ACL users
     */
    public CompletableFuture<List<String>> aclUsers() {
        return send(args().w(Cmd.ACL).s("USERS"), Reply.STRING_LIST);
    }

    /**
     * Sends {@code ACL LIST}.
     *
     * @return one rule line per user; passwords are not shown
     */
    public CompletableFuture<List<String>> aclList() {
        return send(args().w(Cmd.ACL).s("LIST"), Reply.STRING_LIST);
    }

    /**
     * Sends {@code ACL GETUSER user}.
     *
     * @param user the user name
     * @return the user's flags, passwords (hashed), key patterns and command rules as a map; empty when the user does not exist
     * @throws RuvioServerException {@code NOPERM} when the caller may not run {@code ACL}
     */
    public CompletableFuture<Map<String, Object>> aclGetuser(String user) {
        return send(args().w(Cmd.ACL).s("GETUSER").s(user), TEXT_MAP);
    }

    /**
     * Sends {@code ACL CAT}.
     *
     * @return the command categories
     */
    public CompletableFuture<List<String>> aclCat() {
        return send(args().w(Cmd.ACL).s("CAT"), Reply.STRING_LIST);
    }

    /**
     * Sends {@code ACL CAT category}.
     *
     * @param category a category name such as {@code write}
     * @return the commands in that category
     * @throws RuvioServerException {@code ERR} for an unknown category
     */
    public CompletableFuture<List<String>> aclCat(String category) {
        return send(args().w(Cmd.ACL).s("CAT").s(category), Reply.STRING_LIST);
    }

    /**
     * Sends {@code ACL SETUSER user rule [rule ...]}: creates or changes a user. Refused while replication is on. An open connection keeps the rules from its last {@code AUTH}.
     *
     * <pre>{@code
     * client.aclSetuser("app", "on", ">s3cret", "~app:*", "+@read", "+@write");
     * }</pre>
     *
     * @param user the user name
     * @param rules rule tokens such as {@code on}, {@code >password}, {@code ~app:*} (key pattern), {@code +get}, {@code -flushall}, {@code +@read}
     * @throws RuvioServerException {@code ERR} for an unknown rule; {@code NOPERM}
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> aclSetuser(String user, String... rules) {
        return send(args().w(Cmd.ACL).s("SETUSER").s(user).strings(rules), Reply.OK);
    }

    /**
     * Sends {@code ACL LOAD}: replaces the live users with the ones in the ACL file.
     *
     * @throws RuvioServerException {@code ERR} when no ACL file is configured or it is invalid
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> aclLoad() {
        return send(args().w(Cmd.ACL).s("LOAD"), Reply.OK);
    }

    /**
     * Sends {@code ACL SAVE}: writes the live users to the ACL file (passwords stored as hashes).
     *
     * @throws RuvioServerException {@code ERR} when no ACL file is configured
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> aclSave() {
        return send(args().w(Cmd.ACL).s("SAVE"), Reply.OK);
    }

    // ---- server ----------------------------------------------------------------------------

    /**
     * Sends {@code INFO}: server statistics of the shard that answers, as {@code key:value} lines grouped under {@code # Server}, {@code # Stats}, {@code # Buffers}, {@code # Memory} and {@code # Keyspace}. With several shards each shard reports itself ({@code ruvio_shard_id}, {@code ruvio_shard_count}).
     *
     * @return the report text
     */
    public CompletableFuture<String> info() {
        return c0(Cmd.INFO, Reply.STRING);
    }

    /**
     * Sends {@code INFO section}. Only {@code prometheus} changes the output (Prometheus text format for the shard); other section names return the same report as {@link #info()}.
     *
     * @param section {@code prometheus} for metrics text
     * @return the report text
     */
    public CompletableFuture<String> info(String section) {
        return send(args().w(Cmd.INFO).s(section), Reply.STRING);
    }

    /**
     * Sends {@code INFO section} with a typed section name sent from pre-encoded bytes.
     * Example: {@code client.info(InfoSection.MEMORY)}.
     *
     * @param section the section; {@code InfoSection.PROMETHEUS} gives metrics text
     * @return the report text of that section (empty when the shard has no such block)
     */
    public CompletableFuture<String> info(InfoSection section) {
        return send(args().w(Cmd.INFO).w(section.fragment()), Reply.STRING);
    }

    /**
     * Sends {@code CONFIG GET pattern}: reads settings. The password ({@code requirepass}) is redacted.
     *
     * @param pattern a glob such as {@code max*} or {@code *}
     * @return setting name to value
     */
    public CompletableFuture<Map<String, String>> configGet(String pattern) {
        return send(args().w(Cmd.CONFIG).s("GET").s(pattern), Reply.STRING_MAP);
    }

    /**
     * Sends {@code CONFIG SET name value ...}: changes live settings, named as {@code CONFIG GET} shows them. Every shard of the server applies the change before it answers. All pairs apply or none: an unknown or restart-only name, or an invalid value, fails the call and changes nothing. Not written to the config file (see {@link #configRewrite()}) and not replicated.
     *
     * <pre>{@code
     * client.configSet(Map.of("maxmemory", "1073741824", "maxmemory-policy", "allkeys-lru"));
     * }</pre>
     *
     * @param settings setting names and values; iteration order is the send order
     * @throws RuvioServerException {@code ERR} for an unknown, restart-only or invalid setting
     * @return a future that completes with no value once every shard has applied the change
     */
    public CompletableFuture<Void> configSet(Map<String, String> settings) {
        Args a = args().w(Cmd.CONFIG).s("SET");
        for (Map.Entry<String, String> e : settings.entrySet()) {
            a.s(e.getKey()).s(e.getValue());
        }
        return send(a, Reply.OK);
    }

    /**
     * Sends {@code CONFIG REWRITE}: writes the live settings into the config file the server started from.
     *
     * @throws RuvioServerException {@code ERR} when the server runs without a config file
     * @return a future that completes with no value once the file is written
     */
    public CompletableFuture<Void> configRewrite() {
        return send(args().w(Cmd.CONFIG).s("REWRITE"), Reply.OK);
    }

    /**
     * Sends {@code REPLICAOF host port}: every shard of the server follows the primary at {@code host:port} (shard {@code i} dials {@code port + i}). Writes are refused at once. A shard continues from its own log when the new primary's history allows it, otherwise it takes a full copy. Lasts until the server restarts.
     *
     * @param host the primary's host
     * @param port the primary's base replication port
     * @return a future that completes with no value once every shard follows the primary
     */
    public CompletableFuture<Void> replicaOf(String host, int port) {
        return send(args().w(Cmd.REPLICAOF).s(host).s(Integer.toString(port)), Reply.OK);
    }

    /**
     * Sends {@code REPLICAOF NO ONE}: promotes a replica in place, one epoch above its old primary.
     *
     * @param force adds {@code FORCE}: promote even when a shard has not finished copying its primary, which may lose writes
     * @return a future that completes with no value once every shard takes writes
     */
    public CompletableFuture<Void> replicaOfNoOne(boolean force) {
        Args a = args().w(Cmd.REPLICAOF).s("NO").s("ONE");
        if (force) {
            a.s("FORCE");
        }
        return send(a, Reply.OK);
    }

    /**
     * Sends {@code ROLE} for the shard this connection is on.
     *
     * @return {@code ["master", sequence, []]} on a primary; {@code ["slave", host, port, "connect"|"connected", sequence]} on a replica
     */
    public CompletableFuture<List<Object>> role() {
        return send(args().w(Cmd.ROLE), TEXT_LIST);
    }

    /**
     * Sends {@code SAVE}: writes a snapshot now and waits for it.
     *
     * @throws RuvioServerException {@code ERR} when the server has no snapshot storage ({@code memory} profile) or the write fails
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> save() {
        return c0(Cmd.SAVE, Reply.OK);
    }

    /**
     * Sends {@code BGSAVE}: starts a snapshot in the background (in the server process, no fork).
     *
     * @return the status text {@code Background saving started}
     */
    public CompletableFuture<String> bgsave() {
        return c0(Cmd.BGSAVE, Reply.STRING);
    }

    /**
     * Sends {@code WAIT numreplicas timeout}: blocks until replicas have synced the shard's WAL sequence to disk. With {@code port_mode = "per-shard"} it covers the shard the connection reaches plus shards its writes were forwarded to. A write is never undone when the wait times out. Not allowed inside a transaction or a script.
     *
     * @param replicas how many replicas must catch up
     * @param timeoutMillis the longest wait in milliseconds; {@code 0} waits until shutdown
     * @return how many replicas reached the sequence
     */
    public CompletableFuture<Long> waitReplicas(long replicas, long timeoutMillis) {
        return send(args().w(Cmd.WAIT).l(replicas).l(timeoutMillis), Reply.LONG);
    }

    // ---- cluster ---------------------------------------------------------------------------

    /**
     * Sends {@code CLUSTER SLOTS}: the slot ranges and the node that serves each. The cluster client uses it for routing. Ruvio's topology is static: every shard of the process is a primary with no replicas.
     *
     * @return one entry per range (start slot, end slot, node host and port, node id)
     * @throws RuvioServerException {@code ERR cluster mode is disabled} on a server with one shard
     */
    public CompletableFuture<List<Object>> clusterSlots() {
        return send(args().w(Cmd.CLUSTER).s("SLOTS"), TEXT_LIST);
    }

    /**
     * Sends {@code CLUSTER NODES}.
     *
     * @return one text line per node
     * @throws RuvioServerException {@code ERR cluster mode is disabled} on a server with one shard
     */
    public CompletableFuture<String> clusterNodes() {
        return send(args().w(Cmd.CLUSTER).s("NODES"), Reply.STRING);
    }

    /**
     * Sends {@code CLUSTER SHARDS}.
     *
     * @return one entry per shard (slots and nodes)
     * @throws RuvioServerException {@code ERR cluster mode is disabled} on a server with one shard
     */
    public CompletableFuture<List<Object>> clusterShards() {
        return send(args().w(Cmd.CLUSTER).s("SHARDS"), TEXT_LIST);
    }

    /**
     * Sends {@code CLUSTER INFO}.
     *
     * @return {@code key:value} lines about the cluster state
     * @throws RuvioServerException {@code ERR cluster mode is disabled} on a server with one shard
     */
    public CompletableFuture<String> clusterInfo() {
        return send(args().w(Cmd.CLUSTER).s("INFO"), Reply.STRING);
    }

    /**
     * Sends {@code CLUSTER MYID}.
     *
     * @return the id of the node that answers
     * @throws RuvioServerException {@code ERR cluster mode is disabled} on a server with one shard
     */
    public CompletableFuture<String> clusterMyid() {
        return send(args().w(Cmd.CLUSTER).s("MYID"), Reply.STRING);
    }

    /**
     * Sends {@code CLUSTER KEYSLOT key}: the hash slot (0 to 16383) of a key, with {@code {tag}} support. Works on any server. To compute it without a round trip use {@link io.github.salihcantekin.ruvio.RuvioClient#hashSlot(String)}.
     *
     * @param key the key
     * @return the slot number
     */
    public CompletableFuture<Long> clusterKeyslot(K key) {
        return send(args().w(Cmd.CLUSTER).s("KEYSLOT").k(key), Reply.LONG);
    }

    /**
     * Sends {@code READONLY}. Every Ruvio shard is a primary, so this only answers OK.
     *
     * @throws RuvioServerException {@code ERR cluster mode is disabled} on a server with one shard
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> readonly() {
        return c0(Cmd.READONLY, Reply.OK);
    }

    /**
     * Sends {@code READWRITE}. Only answers OK.
     *
     * @throws RuvioServerException {@code ERR cluster mode is disabled} on a server with one shard
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> readwrite() {
        return c0(Cmd.READWRITE, Reply.OK);
    }

    // ---- publish (subscribing needs a RuvioPubSub connection) ------------------------------

    /**
     * Sends {@code PUBLISH channel message}: delivers a message to the subscribers of a channel. Subscribers on every shard of the server process get it, but the reply counts only receivers on the shard that took the command. Messages are not stored, not written to the log, and delivery to other shards is best effort. To receive, use {@link io.github.salihcantekin.ruvio.RuvioClient#pubSub}.
     *
     * @param channel the channel name
     * @param message the payload
     * @return the number of receivers on the shard that took the command
     * @throws RuvioServerException {@code ERR} when refused inside a transaction on a multi-shard server
     */
    public CompletableFuture<Long> publish(String channel, V message) {
        return send(args().w(Cmd.PUBLISH).s(channel).v(message), Reply.LONG);
    }

    /**
     * Sends {@code SPUBLISH shardchannel message}: like {@link #publish} for a sharded channel. The channel is routed by its hash slot, and only subscribers on the shard that owns the slot receive it. A server never forwards it: a wrong shard answers {@code MOVED}, which the cluster client follows.
     *
     * @param channel the sharded channel name
     * @param message the payload
     * @return the number of receivers on the owning shard
     */
    public CompletableFuture<Long> spublish(String channel, V message) {
        return send(args().w(Cmd.SPUBLISH).s(channel).v(message), Reply.LONG);
    }
}
