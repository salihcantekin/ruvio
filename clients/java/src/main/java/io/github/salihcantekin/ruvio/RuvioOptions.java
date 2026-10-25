// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import io.github.salihcantekin.ruvio.internal.Connection;

import io.github.salihcantekin.ruvio.internal.Address;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import javax.net.ssl.SSLContext;

/**
 * Connection settings. Immutable once built; create them with {@link #builder()} or {@link #of(String)}.
 *
 * <pre>{@code
 * RuvioOptions options = RuvioOptions.builder()
 *         .host("db.example.com").port(6379)
 *         .password("secret")
 *         .commandTimeoutMillis(2_000)
 *         .build();
 * }</pre>
 */
public final class RuvioOptions {
    /** Server host name or address (default {@code 127.0.0.1}). */
    public final String host;
    /** Server TCP port (default 6379). */
    public final int port;
    /**
     * Seed addresses as {@code host:port} strings (an IPv6 address as {@code [::1]:6379}), immutable. When not
     * empty they are used instead of {@link #host} and {@link #port}; empty by default.
     */
    public final List<String> seeds;
    /** ACL user name sent with {@code AUTH} on every (re)connect, or {@code null} to authenticate as the default user. */
    public final String username;
    /** Password sent with {@code AUTH} on every (re)connect, or {@code null} for no authentication. */
    public final String password;
    /** Logical database selected on every (re)connect (default 0). */
    public final int database;
    /** Connection name set with {@code CLIENT SETNAME} on every (re)connect, or {@code null}. */
    public final String clientName;
    /** Replicas that must hold each write before its reply, set with {@code CLIENT DURABILITY} on every (re)connect; 0 sends nothing. */
    public final int durabilityReplicas;
    /** Longest wait for those replicas in milliseconds; 0 leaves the server default. */
    public final int durabilityTimeoutMillis;
    /** Timeout for opening the socket, in milliseconds (default 5000). */
    public final int connectTimeoutMillis;
    /** How long blocking calls wait for a reply, in milliseconds (default 30000, plus a blocking command's own wait); 0 waits forever. */
    public final long commandTimeoutMillis;
    /** Maximum replies outstanding on one connection (default 16384). */
    public final int maxInFlight;
    /** Receiver of RESP3 push frames, or {@code null}. */
    public final Consumer<Object> pushListener;
    /** Whether cluster routing was requested. */
    public final boolean cluster;
    /** Where a cluster client sends read-only commands (default {@link ReadPolicy#PRIMARY}); ignored when standalone. */
    public final ReadPolicy readPolicy;
    /**
     * Whether a standalone client with several seeds (or a {@link #sentinelMaster}) finds the primary again after a
     * failover (default {@code true}).
     */
    public final boolean followPrimary;
    /** The master name asked of the coordinators in {@link #seeds}, or {@code null} (default) when the seeds are data nodes. */
    public final String sentinelMaster;
    /** The TLS context every connection uses, or {@code null} (default) for plain TCP; see {@link Builder#tls(SSLContext)}. */
    public final SSLContext sslContext;
    /** The name the server certificate must carry when it differs from the host, or {@code null} to use the host. */
    public final String tlsServerName;

    private RuvioOptions(Builder b) {
        this.host = b.host;
        this.port = b.port;
        this.seeds = Collections.unmodifiableList(new ArrayList<>(b.seeds));
        this.username = b.username;
        this.password = b.password;
        this.database = b.database;
        this.clientName = b.clientName;
        this.durabilityReplicas = b.durabilityReplicas;
        this.durabilityTimeoutMillis = b.durabilityTimeoutMillis;
        this.connectTimeoutMillis = b.connectTimeoutMillis;
        this.commandTimeoutMillis = b.commandTimeoutMillis;
        this.maxInFlight = b.maxInFlight;
        this.pushListener = b.pushListener;
        this.cluster = b.cluster;
        this.readPolicy = b.readPolicy;
        this.followPrimary = b.followPrimary;
        this.sentinelMaster = b.sentinelMaster;
        this.sslContext = b.sslContext;
        this.tlsServerName = b.tlsServerName;
    }

    /**
     * Starts a builder with the defaults: host {@code 127.0.0.1}, port 6379, no authentication,
     * database 0, 5 s connect timeout, no command timeout, 16384 outstanding replies, standalone, reads from primaries.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Parses {@code host:port}; with no colon the whole text is the host and the port stays 6379.
     * All other settings are the defaults.
     *
     * @param hostPort for example {@code "localhost:6380"}
     * @return the options
     * @throws NumberFormatException when the port is not a number
     */
    public static RuvioOptions of(String hostPort) {
        int colon = hostPort.lastIndexOf(':');
        if (colon < 0) {
            return builder().host(hostPort).build();
        }
        return builder().host(hostPort.substring(0, colon)).port(Integer.parseInt(hostPort.substring(colon + 1))).build();
    }

    /**
     * Returns the host.
     *
     * @return the server host
     */
    public String host() {
        return host;
    }

    /**
     * Returns the port.
     *
     * @return the server port
     */
    public int port() {
        return port;
    }

    /**
     * Returns the seed addresses.
     *
     * @return the {@code host:port} seeds; empty when only {@link #host()} and {@link #port()} are used
     */
    public List<String> seeds() {
        return seeds;
    }

    /**
     * Returns the database index.
     *
     * @return the logical database selected on connect
     */
    public int database() {
        return database;
    }

    /**
     * Tells whether cluster routing was requested. A standalone server still makes the client fall back
     * to plain standalone behaviour.
     *
     * @return {@code true} when cluster routing was requested
     */
    public boolean cluster() {
        return cluster;
    }

    /**
     * Copies these settings into a new builder, to change a few of them.
     *
     * @return a builder holding the same values
     */
    public Builder toBuilder() {
        Builder b = new Builder();
        b.host = host;
        b.port = port;
        b.seeds = new ArrayList<>(seeds);
        b.username = username;
        b.password = password;
        b.database = database;
        b.clientName = clientName;
        b.durabilityReplicas = durabilityReplicas;
        b.durabilityTimeoutMillis = durabilityTimeoutMillis;
        b.connectTimeoutMillis = connectTimeoutMillis;
        b.commandTimeoutMillis = commandTimeoutMillis;
        b.maxInFlight = maxInFlight;
        b.pushListener = pushListener;
        b.cluster = cluster;
        b.readPolicy = readPolicy;
        b.followPrimary = followPrimary;
        b.sentinelMaster = sentinelMaster;
        b.sslContext = sslContext;
        b.tlsServerName = tlsServerName;
        return b;
    }

    /** Builder for {@link RuvioOptions}. Not thread safe. */
    public static final class Builder {
        /** Creates a builder with the defaults; prefer {@link RuvioOptions#builder()}. */
        public Builder() {
        }

        private String host = "127.0.0.1";
        private int port = 6379;
        private List<String> seeds = new ArrayList<>();
        private String username;
        private String password;
        private int database;
        private String clientName;
        private int durabilityReplicas;
        private int durabilityTimeoutMillis;
        private int connectTimeoutMillis = 5_000;
        private long commandTimeoutMillis = 30_000;
        private int maxInFlight = 16_384;
        private Consumer<Object> pushListener;
        private boolean cluster;
        private ReadPolicy readPolicy = ReadPolicy.PRIMARY;
        private boolean followPrimary = true;
        private String sentinelMaster;
        private SSLContext sslContext;
        private String tlsServerName;

        /**
         * Sets the server host.
         *
         * @param host host name or address (default {@code 127.0.0.1})
         * @return this builder
         */
        public Builder host(String host) {
            this.host = host;
            return this;
        }

        /**
         * Sets the server port.
         *
         * @param port TCP port (default 6379)
         * @return this builder
         */
        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /**
         * Sets seed addresses, tried in order and used instead of {@link #host} and {@link #port}. Connecting
         * keeps the first seed that can be reached (one attempt each; a refused {@code AUTH} or {@code SELECT}
         * stops at once). After a dropped connection a standalone client reconnects to the next seed first,
         * trying every seed once per attempt. In cluster mode {@code CLUSTER SLOTS} comes from the first reachable
         * seed, and the seeds are asked again when no known node answers a slot map reload. Calling it with no
         * address clears the list.
         *
         * @param addresses {@code host:port} strings; an IPv6 address is written {@code [::1]:6379}
         * @return this builder
         * @throws IllegalArgumentException when an address has no host or the port is not a number from 1 to 65535
         */
        public Builder seeds(String... addresses) {
            return seeds(Arrays.asList(addresses));
        }

        /**
         * Sets seed addresses; see {@link #seeds(String...)}.
         *
         * @param addresses {@code host:port} strings; an IPv6 address is written {@code [::1]:6379}
         * @return this builder
         * @throws IllegalArgumentException when an address has no host or the port is not a number from 1 to 65535
         */
        public Builder seeds(List<String> addresses) {
            Address.parseAll(addresses);
            this.seeds = new ArrayList<>(addresses);
            return this;
        }

        /**
         * Sets the password sent with {@code AUTH password} on every (re)connect.
         *
         * @param password the password
         * @return this builder
         */
        public Builder password(String password) {
            this.password = password;
            return this;
        }

        /**
         * Sets the ACL user and password sent with {@code AUTH username password} on every (re)connect.
         *
         * @param username the ACL user name
         * @param password the password
         * @return this builder
         */
        public Builder credentials(String username, String password) {
            this.username = username;
            this.password = password;
            return this;
        }

        /**
         * Sets the logical database selected with {@code SELECT} on every (re)connect. Ignored in cluster mode.
         *
         * @param database the index, from 0 to {@code databases - 1} (default 0)
         * @return this builder
         */
        public Builder database(int database) {
            this.database = database;
            return this;
        }

        /**
         * Sets the connection name sent with {@code CLIENT SETNAME} on every (re)connect.
         *
         * @param name a name without spaces
         * @return this builder
         */
        public Builder clientName(String name) {
            this.clientName = name;
            return this;
        }

        /**
         * Makes the server hold every reply of each connection until {@code replicas} replicas have the writes on disk,
         * by sending {@code CLIENT DURABILITY REPLICAS n} on every (re)connect, also on the per-node connections of a cluster client.
         * Needs replication on the server; without it the connect fails.
         *
         * @param replicas the replicas that must hold each write; 0 sends nothing (default)
         * @return this builder
         */
        public Builder durability(int replicas) {
            return durability(replicas, 0);
        }

        /**
         * Same as {@link #durability(int)} with a longest wait: after that the replies of the batch become {@code NOREPLICAS} errors.
         *
         * @param replicas the replicas that must hold each write; 0 sends nothing
         * @param timeoutMillis the longest wait in milliseconds; 0 leaves the server default (5000)
         * @return this builder
         */
        public Builder durability(int replicas, int timeoutMillis) {
            this.durabilityReplicas = replicas;
            this.durabilityTimeoutMillis = timeoutMillis;
            return this;
        }

        /**
         * Sets the timeout for opening the socket.
         *
         * @param millis milliseconds (default 5000)
         * @return this builder
         */
        public Builder connectTimeoutMillis(int millis) {
            this.connectTimeoutMillis = millis;
            return this;
        }

        /**
         * How long a call waits for a reply; default 30000 (30 s), 0 waits forever. Asynchronous futures fail with the same exception after this long.
         * The command is not cancelled on the server when the wait times out; the call throws a
         * {@link RuvioException}. A blocking command gets its own wait on top ({@code blpop(5, k)} may
         * take 5 s plus this), and one that waits forever (timeout 0) has no limit.
         *
         * @param millis the wait in milliseconds; 0 waits forever
         * @return this builder
         */
        public Builder commandTimeoutMillis(long millis) {
            this.commandTimeoutMillis = millis;
            return this;
        }

        /**
         * Sets the maximum replies outstanding on one connection. More commands wait until replies arrive.
         *
         * @param count the limit, rounded up to a power of two (default 16384). A pipeline with more
         *        commands than this fails with a {@link RuvioException}
         * @return this builder
         */
        public Builder maxInFlight(int count) {
            this.maxInFlight = count;
            return this;
        }

        /**
         * Receives RESP3 push frames (for example CLIENT TRACKING invalidations) on the reader
         * thread. The frame is a List whose first element is the kind as a byte[] or String. It must not
         * block.
         *
         * @param listener the receiver
         * @return this builder
         */
        public Builder pushListener(Consumer<Object> listener) {
            this.pushListener = listener;
            return this;
        }

        /**
         * Routes every command to the node that owns its key's slot. On connect the seed
         * ({@link #host}/{@link #port}) is asked for {@code CLUSTER SLOTS}; a server that answers with an
         * error (standalone, "cluster mode is disabled") makes the client behave exactly like a plain
         * standalone client, so this can be switched on unconditionally. The server only has database 0
         * in cluster mode, so {@link #database} is ignored while routing is active.
         *
         * @param enabled {@code true} to route by key
         * @return this builder
         */
        public Builder cluster(boolean enabled) {
            this.cluster = enabled;
            return this;
        }

        /**
         * Sets where a cluster client sends read-only commands; see {@link ReadPolicy}. Reads with a key go to one
         * replica of the key's shard, picked at random whenever the slot map is loaded. Replication is asynchronous,
         * so a replica read may miss recent writes, including your own. Ignored by a standalone connection.
         *
         * @param policy the policy (default {@link ReadPolicy#PRIMARY})
         * @return this builder
         */
        public Builder readPolicy(ReadPolicy policy) {
            this.readPolicy = java.util.Objects.requireNonNull(policy, "policy");
            return this;
        }

        /**
         * Sets whether a standalone client finds the primary again after a failover. It applies with at least two
         * {@link #seeds} or a {@link #sentinelMaster}: on a reconnect the client asks the seeds {@code ROLE} and connects
         * to the node that answers {@code master} (a replica's reported primary is followed too), and when a command is
         * answered {@code READONLY} it does the same and sends that command once more to the new primary (nothing
         * was applied). Commands in a pipeline or a transaction are not sent again. Nothing is sent while the
         * connection is healthy. Ignored in cluster mode and with a single address. With {@code false} the
         * {@code READONLY} error is returned and the reconnect order is the plain seed order.
         *
         * @param enabled {@code false} to turn it off (default {@code true})
         * @return this builder
         */
        public Builder followPrimary(boolean enabled) {
            this.followPrimary = enabled;
            return this;
        }

        /**
         * Makes the {@link #seeds} (or host and port) coordinator addresses instead of data nodes: the primary of this
         * master name is asked with {@code SENTINEL get-master-addr-by-name name}, checked with {@code ROLE}, and
         * connected to at connect time, on every reconnect and after a {@code READONLY} reply. The same {@code AUTH}
         * credentials go to the coordinators and the data node. Connecting fails with a {@link RuvioException} when no
         * coordinator names a primary.
         *
         * @param masterName the master name, or {@code null} (default) for no coordinators
         * @return this builder
         */
        public Builder sentinelMaster(String masterName) {
            this.sentinelMaster = masterName;
            return this;
        }

        /**
         * Turns TLS on for every socket the client opens (first connect, reconnects, shards, replicas, Pub/Sub and
         * failover probes), trusting the JVM's default certificates. The server certificate chain and host name are
         * verified. Plain TCP is the default.
         *
         * @return this builder
         * @throws RuvioException when the JVM has no default TLS context
         */
        public Builder tls() {
            try {
                return tls(SSLContext.getDefault());
            } catch (NoSuchAlgorithmException e) {
                throw new RuvioException("no default TLS context: " + e.getMessage(), e);
            }
        }

        /**
         * Turns TLS on with your own context, for a private CA or a client certificate (see
         * {@link RuvioTls#context(java.nio.file.Path, java.nio.file.Path, java.nio.file.Path)}). The server certificate
         * chain is checked by the context's trust managers and the host name is always checked. The handshake is part of
         * the connect, so a refused certificate fails the connect.
         *
         * @param context the TLS context
         * @return this builder
         */
        public Builder tls(SSLContext context) {
            this.sslContext = Objects.requireNonNull(context, "context");
            return this;
        }

        /**
         * Turns TLS on and accepts any server certificate without checking it or the host name. This removes the
         * protection TLS gives against someone in the middle: use it for a throwaway test, never in production.
         *
         * @return this builder
         */
        public Builder tlsInsecure() {
            return tls(RuvioTls.insecureContext());
        }

        /**
         * The name the server certificate must carry, when it differs from the host you connect to (for example a
         * connection by IP address to a certificate issued for a DNS name). Used only with TLS; default the host.
         *
         * @param name the certificate name
         * @return this builder
         */
        public Builder tlsServerName(String name) {
            this.tlsServerName = name;
            return this;
        }

        /**
         * Builds the immutable options.
         *
         * @return the options
         */
        public RuvioOptions build() {
            return new RuvioOptions(this);
        }
    }
}
