// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.RuvioConnectionException;
import io.github.salihcantekin.ruvio.RuvioException;
import io.github.salihcantekin.ruvio.RuvioOptions;
import io.github.salihcantekin.ruvio.RuvioServerException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Owns the shared connection of a client and reopens it before the next command after a drop.
 * Commands in flight when the socket drops fail with {@link RuvioConnectionException}; nothing is
 * replayed automatically.
 */
public final class Link implements AutoCloseable {
    private static final AtomicInteger IDS = new AtomicInteger();

    /** The settings used for every (re)connect. */
    public final RuvioOptions options;
    private final String name;
    /** The addresses tried in order: the seeds, or the single host and port. */
    private final Address[] addresses;
    /** Index of the address the connection is (or was last) on; written under the lock or before publication. */
    private volatile int current;
    private volatile Connection connection;
    private volatile boolean closed;
    /** Send {@code READONLY} right after every (re)connect: this is a replica that serves reads. */
    private final boolean readOnly;
    /** The database selected on (re)connect; follows {@code SELECT}. */
    public volatile int database;

    /**
     * Creates a link and opens the socket.
     *
     * @param options the connection settings
     */
    public Link(RuvioOptions options) {
        this(options, false);
    }

    /**
     * Creates a link.
     *
     * @param options the connection settings
     * @param lazy {@code true} to open the socket on the first command (cluster nodes)
     */
    public Link(RuvioOptions options, boolean lazy) {
        this(options, lazy, addressesOf(options), false);
    }

    /**
     * Creates a lazily opened link to a replica: {@code READONLY} is sent right after every (re)connect.
     *
     * @param options the connection settings, naming the replica
     * @return the link
     */
    public static Link replica(RuvioOptions options) {
        return new Link(options, true, addressesOf(options), true);
    }

    private Link(RuvioOptions options, boolean lazy, Address[] addresses, boolean readOnly) {
        this.options = options;
        this.readOnly = readOnly;
        this.addresses = addresses;
        this.database = options.database;
        this.name = "ruvio-" + IDS.incrementAndGet();
        this.connection = lazy ? null : open(0);
    }

    private static Address[] addressesOf(RuvioOptions options) {
        if (options.seeds.isEmpty()) {
            return new Address[] {new Address(options.host, options.port)};
        }
        return Address.parseAll(options.seeds).toArray(new Address[0]);
    }

    /**
     * Opens a link to the first seed that can be reached, as a link that stays on that one address (a cluster
     * node must not move to a different node after a drop). With no seeds it is the plain link.
     *
     * @param options the connection settings
     * @return the link, connected
     * @throws RuvioConnectionException when no seed can be reached (the last error)
     */
    public static Link openSeed(RuvioOptions options) {
        if (options.seeds.isEmpty()) {
            return new Link(options);
        }
        RuvioConnectionException last = null;
        for (Address a : addressesOf(options)) {
            try {
                return new Link(options, false, new Address[] {a}, false);
            } catch (RuvioConnectionException e) {
                last = e;
            }
        }
        throw last;
    }

    /**
     * Returns the address the connection is on (the last one it was on when it dropped).
     *
     * @return the address
     */
    public Address address() {
        return addresses[current];
    }

    /**
     * Returns settings for a connection opened next to this one (a subscriber): the same settings, with the seeds
     * starting at the address this link is on. A link bound to one address returns settings that name only it.
     *
     * @return the settings
     */
    public RuvioOptions siblingOptions() {
        if (options.seeds.isEmpty()) {
            return options;
        }
        int n = addresses.length;
        int start = current;
        List<String> rotated = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            rotated.add(addresses[(start + i) % n].toString());
        }
        if (n == 1) {
            return options.toBuilder().seeds().host(addresses[0].host).port(addresses[0].port).build();
        }
        return options.toBuilder().seeds(rotated).build();
    }

    /**
     * Tells whether the shared socket is open (no reconnect attempt).
     *
     * @return {@code true} while it is open
     */
    public boolean isUp() {
        Connection c = connection;
        return c != null && c.isOpen();
    }

    /**
     * Returns the shared connection, reconnecting when it dropped.
     *
     * @return an open connection
     * @throws RuvioConnectionException when it cannot be opened or the link is closed
     */
    public Connection get() {
        Connection c = connection;
        if (c != null && c.isOpen()) {
            return c;
        }
        return reopen();
    }

    private synchronized Connection reopen() {
        if (closed) {
            throw new RuvioConnectionException("client is closed");
        }
        Connection c = connection;
        if (c != null && c.isOpen()) {
            return c;
        }
        // Each attempt tries every address once, starting with the one after the address that dropped.
        c = open(c == null ? 0 : (current + 1) % addresses.length);
        connection = c;
        return c;
    }

    private Connection open(int start) {
        return connectFrom(start, name, database, true);
    }

    /**
     * Opens the first address that accepts a connection, starting at {@code start} and wrapping around, one
     * attempt each. Only a failed connect moves on; a refused {@code AUTH} or {@code SELECT} is thrown at once.
     */
    private Connection connectFrom(int start, String threadName, int db, boolean remember) {
        RuvioConnectionException last = null;
        int n = addresses.length;
        for (int step = 0; step < n; step++) {
            int index = (start + step) % n;
            Address a = addresses[index];
            Connection c;
            try {
                c = new Connection(a.host, a.port, options.connectTimeoutMillis, options.maxInFlight, threadName);
            } catch (IOException e) {
                last = new RuvioConnectionException("cannot connect to " + a + ": " + e.getMessage(), e);
                continue;
            }
            if (remember) {
                c.setPushHandler(options.pushListener);
            }
            try {
                handshake(c, options, db);
                if (readOnly && remember) {
                    // Redis replicas serve reads only to connections that sent READONLY; Ruvio replicas serve them anyway.
                    await(new Raw(c).send(Reply.OK, "READONLY"), options.connectTimeoutMillis);
                }
            } catch (RuntimeException e) {
                c.close();
                throw e;
            }
            if (remember) {
                c.setCommandTimeout(options.commandTimeoutMillis);
                current = index;
            }
            return c;
        }
        throw last;
    }

    /**
     * Sends {@code AUTH}, {@code SELECT}, {@code CLIENT SETNAME} and {@code CLIENT DURABILITY}, pipelined in one write.
     *
     * @param c the connection
     * @param o the settings
     * @param database the database to select
     */
    public static void handshake(Connection c, RuvioOptions o, int database) {
        if (o.password == null && database == 0 && o.clientName == null && o.durabilityReplicas == 0) {
            return;
        }
        Raw raw = new Raw(c);
        Request<?> last = null;
        if (o.password != null) {
            last = o.username != null ? raw.send(Reply.OK, "AUTH", o.username, o.password) : raw.send(Reply.OK, "AUTH", o.password);
            await(last, o.connectTimeoutMillis);
        }
        if (database != 0) {
            last = raw.send(Reply.OK, "SELECT", (long) database);
        }
        if (o.clientName != null) {
            last = raw.send(Reply.OK, "CLIENT", "SETNAME", o.clientName);
        }
        if (o.durabilityReplicas != 0) {
            last = o.durabilityTimeoutMillis != 0
                    ? raw.send(Reply.OK, "CLIENT", "DURABILITY", "REPLICAS", (long) o.durabilityReplicas, "TIMEOUT", (long) o.durabilityTimeoutMillis)
                    : raw.send(Reply.OK, "CLIENT", "DURABILITY", "REPLICAS", (long) o.durabilityReplicas);
        }
        if (last != null) {
            await(last, o.connectTimeoutMillis);
        }
    }

    /**
     * Waits for a future.
     *
     * @param f the future
     * @param timeoutMillis the longest wait in milliseconds; 0 waits forever
     * @param <T> the result type
     * @return the result
     * @throws RuvioException when there is no reply in time or the wait was interrupted
     * @throws RuvioServerException when the server answered with an error
     * @throws RuvioConnectionException when the connection failed
     */
    public static <T> T await(java.util.concurrent.CompletableFuture<T> f, long timeoutMillis) {
        try {
            return timeoutMillis > 0 ? f.get(timeoutMillis, TimeUnit.MILLISECONDS) : f.get();
        } catch (ExecutionException e) {
            throw rethrow(e.getCause());
        } catch (TimeoutException e) {
            throw new RuvioException("no reply within " + timeoutMillis + " ms");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuvioException("interrupted", e);
        }
    }

    /**
     * Re-creates a failure on the calling thread so its stack trace shows the caller, keeping the reader-thread original as the cause.
     *
     * @param cause the original failure
     * @return the exception to throw
     */
    public static RuntimeException rethrow(Throwable cause) {
        if (cause instanceof RuvioServerException) {
            RuvioServerException copy = new RuvioServerException(cause.getMessage());
            copy.initCause(cause);
            return copy;
        }
        if (cause instanceof RuvioConnectionException) {
            return new RuvioConnectionException(cause.getMessage(), cause);
        }
        if (cause instanceof RuntimeException) {
            return (RuntimeException) cause;
        }
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        return new RuvioException(String.valueOf(cause), cause);
    }

    /**
     * Opens a separate connection with the same settings (transactions, pub/sub, dedicated clients).
     *
     * @param suffix a label added to the thread names
     * @return the new, authenticated connection
     * @throws RuvioConnectionException when it cannot be opened
     */
    public Connection openExtra(String suffix) {
        return connectFrom(current, name + "-" + suffix, database, false);
    }

    @Override
    public synchronized void close() {
        closed = true;
        Connection c = connection;
        if (c != null) {
            c.close();
        }
    }

    /** Untyped command sender over one connection, used for handshakes and internal commands. */
    public static final class Raw {
        private final Connection c;

        Raw(Connection c) {
            this.c = c;
        }

        <T> Request<T> send(Reply<T> reply, Object... arguments) {
            RespOut o = c.begin();
            try {
                o.header(arguments.length);
                for (Object a : arguments) {
                    o.arg(a);
                }
            } catch (RuntimeException e) {
                c.abort();
                throw e;
            }
            Request<T> r = new Request<>(reply, RuvioCodec.UTF8, RuvioCodec.UTF8);
            c.end(r);
            return r;
        }
    }
}
