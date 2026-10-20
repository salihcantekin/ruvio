// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.RuvioConnectionException;
import io.github.salihcantekin.ruvio.RuvioException;
import io.github.salihcantekin.ruvio.RuvioOptions;
import io.github.salihcantekin.ruvio.RuvioServerException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
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
    /** Rounds that look for the primary after a {@code READONLY} reply. */
    private static final int FOLLOW_ROUNDS = 8;
    /** The wait before the second round in milliseconds; it doubles up to {@link #followMaxMillis}. Package-private so tests can shorten it. */
    static volatile long followBaseMillis = 100;
    /** The longest wait between two rounds in milliseconds. */
    static volatile long followMaxMillis = 2_000;
    /** Runs the work that follows a {@code READONLY} reply off the reader threads, which must never wait for a reply. */
    private static final ExecutorService BACKGROUND = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 30, TimeUnit.SECONDS,
            new SynchronousQueue<>(), r -> {
                Thread t = new Thread(r, "ruvio-follow");
                t.setDaemon(true);
                return t;
            });
    /** Scratch buffers that keep each encoded command, so a refused one can be sent again; one per calling thread. */
    private static final ThreadLocal<Frame> FRAMES = ThreadLocal.withInitial(() -> new Frame(true));

    /** The settings used for every (re)connect. */
    public final RuvioOptions options;
    private final String name;
    /** The addresses tried in order: the seeds, or the single host and port; grows with a primary a replica reported. */
    private volatile Address[] addresses;
    /** The addresses as configured: the seeds or the single host and port (the coordinators with a master name). */
    private final Address[] origin;
    /** This standalone connection looks for the primary again after a failover. */
    private final boolean follow;
    /** Serializes the rounds that follow a {@code READONLY} reply. */
    private final Object followLock = new Object();
    /** Called after the connection moved to another node, to drop what was opened to the old one. */
    private volatile Runnable onMove;
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
        this(options, false, addressesOf(options), false, followsOf(options));
    }

    /**
     * Creates a link.
     *
     * @param options the connection settings
     * @param lazy {@code true} to open the socket on the first command (cluster nodes)
     */
    public Link(RuvioOptions options, boolean lazy) {
        this(options, lazy, addressesOf(options), false, false);
    }

    /**
     * Creates a lazily opened link to a replica: {@code READONLY} is sent right after every (re)connect.
     *
     * @param options the connection settings, naming the replica
     * @return the link
     */
    public static Link replica(RuvioOptions options) {
        return new Link(options, true, addressesOf(options), true, false);
    }

    @SuppressWarnings("this-escape") // the first connect may ask the coordinators
    private Link(RuvioOptions options, boolean lazy, Address[] addresses, boolean readOnly, boolean follow) {
        this.options = options;
        this.readOnly = readOnly;
        this.addresses = addresses;
        this.origin = addresses;
        this.follow = follow;
        this.database = options.database;
        this.name = "ruvio-" + IDS.incrementAndGet();
        if (follow && !lazy && options.sentinelMaster != null) {
            // With a master name the seeds are coordinators: the first connect goes to the primary they name.
            Address primary = findPrimary(origin, 0, null);
            if (primary == null) {
                throw new RuvioException("no coordinator names a primary for '" + options.sentinelMaster + "'");
            }
            this.addresses = new Address[] {primary};
        }
        this.connection = lazy ? null : open(0);
    }

    /** Following needs a standalone connection with something to ask: several seeds or a master name. */
    private static boolean followsOf(RuvioOptions options) {
        return options.followPrimary && (options.seeds.size() > 1 || options.sentinelMaster != null);
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
            return new Link(options, false, addressesOf(options), false, false);
        }
        RuvioConnectionException last = null;
        for (Address a : addressesOf(options)) {
            try {
                return new Link(options, false, new Address[] {a}, false, false);
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
        Address[] list = addresses;
        return list[Math.min(current, list.length - 1)];
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
        Address[] list = addresses;
        int n = list.length;
        int start = Math.min(current, n - 1);
        List<String> rotated = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            rotated.add(list[(start + i) % n].toString());
        }
        if (n == 1) {
            return options.toBuilder().seeds().host(list[0].host).port(list[0].port).build();
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
        // Each attempt tries every address once, starting with the one after the address that dropped. With
        // several seeds (or a master name) the node that answers ROLE with master comes first.
        int start = c == null ? 0 : (current + 1) % addresses.length;
        if (follow) {
            Connection primary = openPrimary(start);
            if (primary != null) {
                Connection old = connection;
                connection = primary;
                moved(old);
                return primary;
            }
        }
        c = open(start);
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
        Address[] list = addresses;
        int n = list.length;
        for (int step = 0; step < n; step++) {
            int index = (start + step) % n;
            Connection c;
            try {
                c = connectOne(list[index], threadName, db, remember);
            } catch (CannotConnect e) {
                last = e;
                continue;
            }
            if (remember) {
                current = index;
            }
            return c;
        }
        throw last;
    }

    /**
     * Opens one address: connect, then {@code AUTH}, {@code SELECT}, name and durability. A failed connect is a
     * {@link CannotConnect}; a refused {@code AUTH} or
     * {@code SELECT} is thrown as it is.
     */
    private Connection connectOne(Address a, String threadName, int db, boolean remember) {
        Connection c;
        try {
            c = new Connection(a.host, a.port, options.connectTimeoutMillis, options.maxInFlight, threadName);
        } catch (IOException e) {
            throw new CannotConnect("cannot connect to " + a + ": " + e.getMessage(), e);
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
        }
        return c;
    }

    /** A socket that could not be opened (as opposed to one that failed during the handshake). */
    private static final class CannotConnect extends RuvioConnectionException {
        private static final long serialVersionUID = 1L;

        CannotConnect(String message, Throwable cause) {
            super(message, cause);
        }
    }

    // ---- following a failover ---------------------------------------------------------------

    /**
     * Tells whether this link looks for the primary again after a failover: the option is on, the connection is
     * standalone and there are several seeds or a master name.
     *
     * @return {@code true} when it follows
     */
    public boolean follows() {
        return follow;
    }

    /**
     * Sets what runs after the connection moved to another node (the transaction pool drops its idle sockets).
     *
     * @param action the callback
     */
    public void onMove(Runnable action) {
        this.onMove = action;
    }

    /**
     * Returns the calling thread's scratch buffer for the next command of a following link; the command is kept
     * (without a copy) so that a refused one can be sent again.
     *
     * @return the buffer to encode into
     */
    public RespOut begin() {
        Frame f = FRAMES.get();
        f.reset();
        return f.out;
    }

    /**
     * Sends the command encoded in {@code f} on the shared connection, reconnecting first when it dropped.
     *
     * @param f the scratch frame {@link #begin()} returned
     * @param r the request awaiting the reply
     */
    public void send(Frame f, RoutedRequest<?> r) {
        byte[] buf = f.out.buf;
        int off = f.base;
        int len = f.out.pos - off;
        f.keep();
        r.setFrame(buf, off, len);
        r.link = this;
        Connection c = get();
        r.sentOn = c;
        RespOut o = c.begin();
        try {
            o.raw(buf, off, len);
        } catch (RuntimeException e) {
            c.abort();
            throw e;
        }
        c.end(r);
    }

    /**
     * Tells whether an error is a refused write: its text is {@code READONLY} alone or {@code READONLY} and a space.
     *
     * @param error the failure
     * @return {@code true} for a {@code READONLY} reply
     */
    public static boolean isReadonly(Throwable error) {
        if (!(error instanceof RuvioServerException)) {
            return false;
        }
        String m = error.getMessage();
        return m != null && m.startsWith("READONLY") && (m.length() == 8 || m.charAt(8) == ' ');
    }

    /**
     * A command was refused with {@code READONLY}: finds the primary (off the reader thread), moves the shared
     * connection to it and sends the command once more. The request fails with the original error when no
     * primary is found, and with the reply of the second send otherwise (a second refusal included).
     *
     * @param r the refused request
     * @param error the {@code READONLY} error
     */
    void followReadonly(RoutedRequest<?> r, Throwable error) {
        try {
            BACKGROUND.execute(() -> sendAgain(r, error));
        } catch (RejectedExecutionException e) {
            r.failFinal(error);
        }
    }

    /**
     * A transaction's connection was refused with {@code READONLY}: moves the shared connection to the primary
     * (off the calling thread) without sending anything again.
     *
     * @param refusedOn the connection that answered
     */
    public void followAfterRefusal(Connection refusedOn) {
        try {
            BACKGROUND.execute(() -> {
                try {
                    moveAfterReadonly(refusedOn);
                } catch (RuntimeException ignored) {
                    // the next command meets READONLY and tries again
                }
            });
        } catch (RejectedExecutionException ignored) {
            // shutting down
        }
    }

    private void sendAgain(RoutedRequest<?> r, Throwable error) {
        Connection target;
        try {
            target = moveAfterReadonly(r.sentOn);
        } catch (RuntimeException e) {
            r.failFinal(e); // a refused AUTH, as when connecting
            return;
        }
        if (target == null) {
            r.failFinal(error);
            return;
        }
        if (r.isDone()) {
            return;
        }
        r.retried = true;
        r.sentOn = target;
        try {
            RespOut o = target.begin();
            try {
                o.raw(r.frame, r.frameOff, r.frameLen);
            } catch (RuntimeException e) {
                target.abort();
                throw e;
            }
            target.end(r);
        } catch (RuntimeException e) {
            r.failFinal(e);
        }
    }

    /**
     * Runs rounds until a primary other than the one that refused is found, waiting between rounds, and makes
     * the shared connection a normal connection to it.
     *
     * @return the connection to use, or null when no round found a primary
     */
    private Connection moveAfterReadonly(Connection refusedOn) {
        synchronized (followLock) {
            Connection now = connection;
            if (now != null && now.isOpen() && (now.port != refusedOn.port || !now.host.equals(refusedOn.host))) {
                return now; // an earlier refusal already moved the connection
            }
            Address refused = new Address(refusedOn.host, refusedOn.port);
            boolean coordinators = options.sentinelMaster != null;
            long wait = followBaseMillis;
            for (int round = 0; round < FOLLOW_ROUNDS; round++) {
                if (closed) {
                    return null;
                }
                Address[] seeds = coordinators ? origin : addresses;
                int start = coordinators ? 0 : (current + 1) % seeds.length;
                Address primary = findPrimary(seeds, start, refused);
                if (primary != null) {
                    Connection c = null;
                    try {
                        c = connectOne(primary, name, database, true);
                    } catch (RuvioServerException e) {
                        throw e;
                    } catch (RuvioException e) {
                        // an I/O error: another round
                    }
                    if (c != null) {
                        Connection old;
                        synchronized (this) {
                            if (closed) {
                                c.close();
                                return null;
                            }
                            current = locate(primary);
                            old = connection;
                            connection = c;
                        }
                        moved(old);
                        return c;
                    }
                }
                if (round + 1 < FOLLOW_ROUNDS) {
                    try {
                        Thread.sleep(wait);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return null;
                    }
                    wait = Math.min(wait * 2, followMaxMillis);
                }
            }
            return null;
        }
    }

    /** After the shared connection moved: lets the old socket answer what it still owes, then closes it, and tells the pool. */
    private void moved(Connection old) {
        Runnable action = onMove;
        if (action != null) {
            action.run();
        }
        if (old == null) {
            return;
        }
        if (old.inFlight() == 0) {
            old.close();
            return;
        }
        // Other commands wait on the old socket: a refused one is sent again on the new socket when its reply arrives.
        long limit = Math.max(options.commandTimeoutMillis, options.connectTimeoutMillis);
        try {
            BACKGROUND.execute(() -> {
                long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(limit, 1_000));
                while (old.isOpen() && old.inFlight() > 0 && System.nanoTime() < deadline) {
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                old.close();
            });
        } catch (RejectedExecutionException e) {
            old.close();
        }
    }

    /** A connection to the primary found by one round, or null (no primary, or it could not be opened). */
    private Connection openPrimary(int start) {
        Address[] seeds = options.sentinelMaster != null ? origin : addresses;
        Address primary = findPrimary(seeds, start % seeds.length, null);
        if (primary == null) {
            return null;
        }
        Connection c;
        try {
            c = connectOne(primary, name, database, true);
        } catch (RuvioServerException e) {
            throw e;
        } catch (RuvioException e) {
            return null; // an I/O error: the first seed that answers, as without following
        }
        current = locate(primary);
        return c;
    }

    /** The index of {@code a} in the connect list, which gains it when it is new (with a master name the list is just the primary). */
    private int locate(Address a) {
        if (options.sentinelMaster != null) {
            current = 0;
            addresses = new Address[] {a};
            return 0;
        }
        Address[] list = addresses;
        for (int i = 0; i < list.length; i++) {
            if (list[i].equals(a)) {
                return i;
            }
        }
        Address[] bigger = Arrays.copyOf(list, list.length + 1);
        bigger[list.length] = a;
        addresses = bigger;
        return list.length;
    }

    /** What {@code ROLE} said: {@code master}, or {@code slave} with the address it reports for its primary (may be null). */
    private static final class Role {
        static final Role MASTER = new Role(true, null);
        final boolean master;
        final Address reported;

        Role(boolean master, Address reported) {
            this.master = master;
            this.reported = reported;
        }
    }

    private static String text(Object o) {
        if (o instanceof byte[]) {
            return new String((byte[]) o, StandardCharsets.UTF_8);
        }
        return o instanceof String ? (String) o : null;
    }

    private static int portOf(Object o) {
        long port = -1;
        if (o instanceof Long) {
            port = (Long) o;
        } else {
            String t = text(o);
            if (t != null) {
                try {
                    port = Long.parseLong(t.trim());
                } catch (NumberFormatException e) {
                    return 0;
                }
            }
        }
        return port >= 1 && port <= 65535 ? (int) port : 0;
    }

    private static Role roleOf(Object reply) {
        if (!(reply instanceof List) || ((List<?>) reply).isEmpty()) {
            return null;
        }
        List<?> items = (List<?>) reply;
        String kind = text(items.get(0));
        if (kind == null) {
            return null;
        }
        if (kind.equalsIgnoreCase("master")) {
            return Role.MASTER;
        }
        if (!kind.equalsIgnoreCase("slave")) {
            return null;
        }
        String host = items.size() > 1 ? text(items.get(1)) : null;
        int port = items.size() > 2 ? portOf(items.get(2)) : 0;
        return new Role(false, host != null && !host.isEmpty() && port != 0 ? new Address(host, port) : null);
    }

    /** The wait for the reply of a probe: the smaller of the connect and the command timeout (the connect timeout when that is off). */
    private long probeWait() {
        long connect = options.connectTimeoutMillis;
        long command = options.commandTimeoutMillis;
        return command > 0 ? Math.min(connect, command) : connect;
    }

    /**
     * One question to one address on a short-lived socket: connect, {@code AUTH} as configured, one command, one
     * reply, close. Returns null when there is no answer (cannot connect, times out, drops, error reply, nil).
     *
     * @throws RuvioServerException when {@code AUTH} is refused
     */
    private Object ask(Address a, Object... command) {
        Connection c;
        try {
            c = new Connection(a.host, a.port, options.connectTimeoutMillis, 16, name + "-probe");
        } catch (IOException e) {
            return null;
        }
        try {
            long wait = probeWait();
            Raw raw = new Raw(c);
            if (options.password != null) {
                try {
                    await(options.username != null ? raw.send(Reply.OK, "AUTH", options.username, options.password)
                            : raw.send(Reply.OK, "AUTH", options.password), wait);
                } catch (RuvioServerException e) {
                    throw e;
                } catch (RuvioException e) {
                    return null;
                }
            }
            try {
                return await(raw.send(Reply.RAW, command), wait);
            } catch (RuvioException e) {
                return null;
            }
        } finally {
            c.close();
        }
    }

    /**
     * One round: the address of a node that answered {@code ROLE} with {@code master}, asking {@code seeds} in order
     * from {@code start}. A replica's reported primary is asked after the seeds, once per address. With a master name
     * the seeds are coordinators: each names a primary and the first that {@code ROLE} confirms wins.
     *
     * @param seeds where to ask
     * @param start the first seed to ask
     * @param avoid an address that must not be taken (the one that just answered {@code READONLY}), or null
     * @return the primary, or null when no address is confirmed
     */
    private Address findPrimary(Address[] seeds, int start, Address avoid) {
        int n = seeds.length;
        if (options.sentinelMaster != null) {
            for (int i = 0; i < n; i++) {
                Address named = named(ask(seeds[(start + i) % n], "SENTINEL", "get-master-addr-by-name", options.sentinelMaster));
                if (named != null && !named.equals(avoid)) {
                    Role role = roleOf(ask(named, "ROLE"));
                    if (role != null && role.master) {
                        return named;
                    }
                }
            }
            return null;
        }
        ArrayDeque<Address> queue = new ArrayDeque<>();
        List<Address> seen = new ArrayList<>(n + 2);
        for (int i = 0; i < n; i++) {
            Address a = seeds[(start + i) % n];
            queue.add(a);
            seen.add(a);
        }
        Address a;
        while ((a = queue.pollFirst()) != null) {
            Role role = roleOf(ask(a, "ROLE"));
            if (role == null) {
                continue;
            }
            if (role.master) {
                if (!a.equals(avoid)) {
                    return a;
                }
            } else if (role.reported != null && !seen.contains(role.reported)) {
                seen.add(role.reported);
                queue.addLast(role.reported);
            }
        }
        return null;
    }

    /** The address of a {@code SENTINEL get-master-addr-by-name} reply: host and port, or null. */
    private static Address named(Object reply) {
        if (!(reply instanceof List) || ((List<?>) reply).size() != 2) {
            return null;
        }
        List<?> items = (List<?>) reply;
        String host = text(items.get(0));
        int port = portOf(items.get(1));
        return host == null || host.isEmpty() || port == 0 ? null : new Address(host, port);
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
