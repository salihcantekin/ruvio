// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import io.github.salihcantekin.ruvio.args.ScanArgs;
import io.github.salihcantekin.ruvio.args.SetOptions;
import io.github.salihcantekin.ruvio.commands.RuvioSyncCommands;
import io.github.salihcantekin.ruvio.internal.Blocking;
import io.github.salihcantekin.ruvio.internal.ClusterNode;
import io.github.salihcantekin.ruvio.internal.ClusterRouter;
import io.github.salihcantekin.ruvio.internal.HashSlot;
import io.github.salihcantekin.ruvio.internal.Link;
import io.github.salihcantekin.ruvio.internal.ScanIterator;
import io.github.salihcantekin.ruvio.internal.TxPool;
import io.github.salihcantekin.ruvio.model.FillResult;
import io.github.salihcantekin.ruvio.model.IdemResult;
import io.github.salihcantekin.ruvio.model.KeyValue;
import io.github.salihcantekin.ruvio.model.PubSubMessage;
import io.github.salihcantekin.ruvio.model.RateLimitResult;
import io.github.salihcantekin.ruvio.model.ScoredValue;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Ruvio client: every typed command as a blocking method (inherited from
 * {@link RuvioSyncCommands}), {@link #async()} for the CompletableFuture API, explicit
 * {@link #pipeline()}, {@link #multi()} transactions, {@link #pubSub(Consumer)} and the
 * convenience helpers (locks, leases, semaphores, rate limits, cache-aside, idempotency, scan
 * iterators, typed JSON, capped lists, Bloom "seen before").
 *
 * <p>All views of one client share one multiplexed socket: any number of threads may call it at
 * once and their commands are pipelined. Blocking server commands (BLPOP, XREAD BLOCK, ...) hold
 * that socket while they wait, so run them on {@link #dedicated()}.
 *
 * <p>Failures: {@link RuvioServerException} for an error reply from the server (for example
 * {@code WRONGTYPE}); {@link RuvioConnectionException} when the socket fails (the command may or may
 * not have run); {@link RuvioException} for a timeout ({@code commandTimeoutMillis}) or an interrupt.
 * Close the client when done; it is {@link AutoCloseable}.
 *
 * <pre>{@code
 * try (RuvioClient<String, String> client = RuvioClient.connect("127.0.0.1", 6379)) {
 *     client.set("greeting", "hello");
 *     String value = client.get("greeting");   // "hello"
 * }
 * }</pre>
 *
 * @param <K> key type (also hash fields and stream field names)
 * @param <V> value type (also members and stream field values)
 */
public final class RuvioClient<K, V> extends RuvioSyncCommands<K, V> implements AutoCloseable {
    private static final SecureRandom RANDOM = new SecureRandom();

    private final Link link;
    private final TxPool txPool;
    private final long timeout;
    /** Non-null when cluster routing is active. */
    private final ClusterRouter router;
    /** A view onto one node (see {@link #onNode}): closing it leaves the shared connection open. */
    private final boolean view;
    private volatile JsonCodec json = JsonCodec.PASSTHROUGH;

    private RuvioClient(Link link, TxPool txPool, RuvioAsync<K, V> async, ClusterRouter router, boolean view) {
        super(async);
        this.link = link;
        this.txPool = txPool;
        this.timeout = link.options.commandTimeoutMillis;
        this.router = router;
        this.view = view;
    }

    /**
     * Connects to a server with UTF-8 String keys and values and default options (no password, database
     * 0, 5 s connect timeout, no command timeout).
     *
     * <pre>{@code
     * RuvioClient<String, String> client = RuvioClient.connect("127.0.0.1", 6379);
     * }</pre>
     *
     * @param host the server host name or address
     * @param port the TCP port (Ruvio listens on 6379 by default)
     * @return a connected client
     * @throws RuvioConnectionException when the server cannot be reached within the connect timeout
     */
    public static RuvioClient<String, String> connect(String host, int port) {
        return connect(RuvioOptions.builder().host(host).port(port).build());
    }

    /**
     * Connects with UTF-8 String keys and values using the given options (password, database, timeouts,
     * cluster mode and so on).
     *
     * <pre>{@code
     * RuvioOptions options = RuvioOptions.builder().host("db.example.com").password("secret").build();
     * RuvioClient<String, String> client = RuvioClient.connect(options);
     * }</pre>
     *
     * @param options the connection settings
     * @return a connected client; with {@code cluster(true)} it routes every command to the shard that
     *         owns the key
     * @throws RuvioConnectionException when the server cannot be reached
     * @throws RuvioServerException when authentication or {@code SELECT} is refused
     */
    public static RuvioClient<String, String> connect(RuvioOptions options) {
        return connect(options, RuvioCodec.UTF8, RuvioCodec.UTF8);
    }

    /**
     * Connects with the given codecs, for example {@code RuvioCodec.UTF8, RuvioCodec.BYTES} for String
     * keys and binary values. In cluster mode the client asks the seed node for the slot map; if the
     * server is not cluster-enabled it silently falls back to a standalone connection.
     *
     * <pre>{@code
     * RuvioClient<String, byte[]> client =
     *         RuvioClient.connect(RuvioOptions.builder().build(), RuvioCodec.UTF8, RuvioCodec.BYTES);
     * }</pre>
     *
     * @param options the connection settings
     * @param keys the codec for keys, hash fields and stream field names
     * @param values the codec for values, members and stream field values
     * @param <K> the key type
     * @param <V> the value type
     * @return a connected client
     * @throws RuvioConnectionException when the server cannot be reached
     * @throws RuvioServerException when authentication or {@code SELECT} is refused
     */
    public static <K, V> RuvioClient<K, V> connect(RuvioOptions options, RuvioCodec<K> keys, RuvioCodec<V> values) {
        if (options.cluster) {
            // The server only has database 0 in cluster mode, so the seed connection never SELECTs.
            RuvioOptions seedOptions = options.database == 0 ? options : options.toBuilder().database(0).build();
            Link seed = Link.openSeed(seedOptions);
            ClusterRouter router;
            try {
                router = ClusterRouter.detect(seed, seedOptions);
            } catch (RuntimeException e) {
                seed.close();
                throw e;
            }
            if (router != null) {
                return new RuvioClient<>(seed, router.seed().txPool, new RuvioAsync<>(seed, router, keys, values), router, false);
            }
            seed.close(); // standalone server: reconnect below with the caller's database
        }
        Link link = new Link(options);
        return new RuvioClient<>(link, new TxPool(link), new RuvioAsync<>(link, keys, values), null, false);
    }

    /** The cluster router (null when standalone); package-private for tests. */
    ClusterRouter router() {
        return router;
    }

    /**
     * Tells whether commands are routed by key to a cluster's nodes.
     *
     * @return {@code true} for a cluster client, {@code false} for a plain standalone client
     */
    public boolean isCluster() {
        return router != null;
    }

    /**
     * Lists the cluster nodes, for use with {@link #onNode(String)}.
     *
     * @return {@code host:port} of every node that owns slots, in slot order; empty for a standalone client
     */
    public List<String> nodeAddresses() {
        List<String> out = new ArrayList<>();
        if (router != null) {
            for (ClusterNode n : router.masters()) {
                out.add(n.address);
            }
        }
        return out;
    }

    /**
     * A client that sends everything to one cluster node, whatever the key (for per-node commands such
     * as DBSIZE, FLUSHDB, KEYS, INFO or SCAN). Shares that node's connection; closing the view does
     * not close it. Standalone clients return themselves.
     *
     * <p>Ruvio shards are independent servers: {@code DBSIZE}, {@code KEYS}, {@code SCAN} and
     * {@code FLUSHDB} on a cluster client only reach the seed node, so use this view to ask each node.
     *
     * <pre>{@code
     * long total = 0;
     * for (String node : client.nodeAddresses()) {
     *     total += client.onNode(node).dbsize();
     * }
     * }</pre>
     *
     * @param hostPort a node address as returned by {@link #nodeAddresses()}
     * @return a view whose commands all go to that node
     * @throws IllegalArgumentException when the address is not a node of this cluster
     */
    public RuvioClient<K, V> onNode(String hostPort) {
        if (router == null) {
            return this;
        }
        ClusterNode n = router.node(hostPort);
        if (n == null) {
            throw new IllegalArgumentException("unknown cluster node " + hostPort);
        }
        return nodeView(n);
    }

    private RuvioClient<K, V> nodeView(ClusterNode n) {
        RuvioClient<K, V> c = new RuvioClient<>(n.link, n.txPool, new RuvioAsync<>(n.link, null, async.keyCodec(), async.valueCodec()), null, true);
        c.json = json;
        return c;
    }

    /**
     * Returns the asynchronous API on the same connection.
     *
     * @return the async facade; every command returns a {@link CompletableFuture}
     */
    public RuvioAsync<K, V> async() {
        return async;
    }

    /**
     * Returns a view with other codecs on the same connection. Closing any view closes the connection.
     *
     * @param keys the codec for keys
     * @param values the codec for values
     * @param <K2> the new key type
     * @param <V2> the new value type
     * @return a client with the same settings, JSON codec and connection
     */
    public <K2, V2> RuvioClient<K2, V2> withCodecs(RuvioCodec<K2> keys, RuvioCodec<V2> values) {
        RuvioClient<K2, V2> c = new RuvioClient<>(link, txPool, async.withCodecs(keys, values), router, view);
        c.json = json;
        return c;
    }

    /**
     * Returns a view with {@code byte[]} values (binary safe) on the same connection.
     *
     * @return a client whose values are raw bytes
     */
    public RuvioClient<K, byte[]> binaryValues() {
        return withCodecs(async.keyCodec(), RuvioCodec.BYTES);
    }

    /**
     * A new client with its own socket and the same settings, for blocking commands (BLPOP, BLMOVE,
     * XREAD BLOCK, FILLGET WAIT) that would otherwise hold the shared socket. Close it when done.
     * In cluster mode it is a cluster client of its own, so a blocking command runs on a connection
     * to the node that owns its key without holding the shared one.
     *
     * <pre>{@code
     * try (RuvioClient<String, String> blocking = client.dedicated()) {
     *     KeyValue<String, String> job = blocking.blpop(5, "jobs");
     * }
     * }</pre>
     *
     * @return a new, independent client; the caller must close it
     * @throws RuvioConnectionException when the new socket cannot be opened
     */
    public RuvioClient<K, V> dedicated() {
        RuvioOptions o = link.options.toBuilder().database(link.database).build();
        return connect(o, async.keyCodec(), async.valueCodec());
    }

    /**
     * Starts an explicit pipeline: commands are buffered and sent in one write by {@link Pipeline#flush()}.
     *
     * @return a new pipeline (not thread safe)
     */
    public Pipeline<K, V> pipeline() {
        return async.pipeline();
    }

    /**
     * Starts a {@code MULTI}/{@code EXEC} transaction on a dedicated (pooled) connection. {@code MULTI}
     * is sent lazily with the first queued command. In a cluster all keys must hash to one slot.
     *
     * <pre>{@code
     * try (Transaction<String, String> tx = client.multi()) {
     *     tx.set("a", "1");
     *     tx.incr("counter");
     *     tx.exec();
     * }
     * }</pre>
     *
     * @return a new transaction; close it (or call {@code exec}/{@code discard}) to return the connection
     */
    public Transaction<K, V> multi() {
        if (router != null) {
            return new Transaction<>(router, timeout, async.keyCodec(), async.valueCodec());
        }
        return new Transaction<>(txPool, timeout, async.keyCodec(), async.valueCodec());
    }

    /**
     * Opens a Pub/Sub subscriber on its own connection (or, in a cluster, one connection per node for
     * shard channels). Messages are delivered to {@code listener} on the subscriber's reader thread.
     *
     * <pre>{@code
     * try (RuvioPubSub sub = client.pubSub(msg -> System.out.println(msg.channel() + ": " + msg.payloadString()))) {
     *     sub.subscribe("news");
     *     client.publish("news", "hello");
     * }
     * }</pre>
     *
     * @param listener called for every message received, exact, pattern or shard; must not block
     * @return a new subscriber; close it when done
     * @throws RuvioConnectionException when the connection cannot be opened
     */
    public RuvioPubSub pubSub(Consumer<PubSubMessage> listener) {
        return new RuvioPubSub(link.siblingOptions(), listener, router);
    }

    /**
     * Sets the codec used by {@link #jsonSetObject}, {@link #jsonGetObject} and {@link #jsonMgetObjects}.
     * It applies to this client and to views created afterwards.
     *
     * @param codec the serializer (default {@link JsonCodec#PASSTHROUGH}, which accepts JSON text only)
     */
    public void setJsonCodec(JsonCodec codec) {
        this.json = codec;
    }

    @Override
    protected <T> T await(CompletableFuture<T> future) {
        if (timeout > 0) {
            // A blocking command waits its own time on top; one that waits forever has no limit.
            return Link.await(future, Blocking.timeoutFor(timeout, future));
        }
        try {
            return future.join();
        } catch (CompletionException e) {
            throw Link.rethrow(e.getCause());
        } catch (java.util.concurrent.CancellationException e) {
            throw new RuvioException("cancelled", e);
        }
    }

    /**
     * Closes the connection (and the transaction pool and cluster connections). A client returned by
     * {@link #onNode(String)} leaves the shared connection open. Commands still waiting fail with a
     * {@link RuvioConnectionException}.
     */
    @Override
    public void close() {
        if (view) {
            return;
        }
        txPool.close();
        link.close();
        if (router != null) {
            router.close();
        }
    }

    private static String newToken() {
        byte[] b = new byte[16];
        RANDOM.nextBytes(b);
        StringBuilder sb = new StringBuilder(32);
        for (byte x : b) {
            sb.append(Character.forDigit((x >> 4) & 0xF, 16)).append(Character.forDigit(x & 0xF, 16));
        }
        return sb.toString();
    }

    private static void backoff(int attempt) {
        long micros = Math.min(20_000, 500L << Math.min(attempt, 6));
        LockSupport.parkNanos(micros * 1000);
    }

    // ---- locks, leases, semaphores ---------------------------------------------------------

    /**
     * Tries once to take a distributed lock with {@code SET key token NX PX ttl}, using a random 128-bit
     * owner token. The lock expires by itself after the TTL, so a crashed owner never blocks others forever.
     *
     * <pre>{@code
     * try (RuvioLock<String> lock = client.tryLock("job:42", 30_000)) {
     *     if (lock != null) {
     *         // do the work
     *     }
     * }
     * }</pre>
     *
     * @param key the lock key
     * @param ttlMillis how long the lock lives if never released, in milliseconds (above 0)
     * @return the held lock, or {@code null} when someone else holds it
     * @throws RuvioServerException on a server error such as {@code WRONGTYPE} when the key holds another type
     */
    public RuvioLock<K> tryLock(K key, long ttlMillis) {
        RuvioAsync<K, String> c = async.withCodecs(async.keyCodec(), RuvioCodec.UTF8);
        String token = newToken();
        if (await(c.set(key, token, SetOptions.px(ttlMillis).ifAbsent()))) {
            return new RuvioLock<>(c, key, token, timeout);
        }
        return null;
    }

    /**
     * Retries {@link #tryLock} with a short backoff (up to 20 ms between attempts) for up to
     * {@code waitMillis}.
     *
     * @param key the lock key
     * @param ttlMillis how long the lock lives if never released, in milliseconds (above 0)
     * @param waitMillis how long to keep trying, in milliseconds; 0 tries once
     * @return the held lock, or {@code null} when it could not be taken in time
     */
    public RuvioLock<K> lock(K key, long ttlMillis, long waitMillis) {
        long deadline = System.nanoTime() + waitMillis * 1_000_000L;
        for (int attempt = 0; ; attempt++) {
            RuvioLock<K> lock = tryLock(key, ttlMillis);
            if (lock != null || System.nanoTime() >= deadline) {
                return lock;
            }
            backoff(attempt);
        }
    }

    /**
     * Takes a lease with {@code LEASE key ttl}: an exclusive hold with a growing fencing token. Pass
     * {@link RuvioToken#token()} to the protected resource so it can reject stale holders.
     *
     * <pre>{@code
     * try (RuvioToken<String> lease = client.tryLease("leader", 10_000)) {
     *     if (lease != null) {
     *         long fence = lease.token();
     *     }
     * }
     * }</pre>
     *
     * @param key the lease key
     * @param ttlMillis how long the lease lives, in milliseconds (above 0)
     * @return the held lease, or {@code null} while someone else holds it
     */
    public RuvioToken<K> tryLease(K key, long ttlMillis) {
        Long token = await(async.lease(key, ttlMillis));
        return token == null ? null : new RuvioToken<>(async, key, token, timeout);
    }

    /**
     * Takes one slot of a counting semaphore with {@code SEMAPHORE key max ttl}.
     *
     * @param key the semaphore key
     * @param max the number of slots (above 0)
     * @param ttlMillis how long a slot is held if never released, in milliseconds (above 0)
     * @return the held slot, or {@code null} when all {@code max} slots are taken
     */
    public RuvioToken<K> tryAcquirePermit(K key, long max, long ttlMillis) {
        Long token = await(async.semaphore(key, max, ttlMillis));
        return token == null ? null : new RuvioToken<>(async, key, token, timeout);
    }

    // ---- rate limit ------------------------------------------------------------------------

    /**
     * Counts one call against a fixed-window rate limit with {@code LIMIT key max window}.
     *
     * <pre>{@code
     * RateLimitResult r = client.rateLimit("api:user:7", 100, 60_000);
     * if (!r.allowed()) {
     *     // reject the request
     * }
     * }</pre>
     *
     * @param key the limiter key (one per user, API key and so on)
     * @param max the calls allowed per window (above 0)
     * @param windowMillis the window length in milliseconds (above 0)
     * @return whether the call fits, and how many calls remain in the window ({@code 0} when refused)
     */
    public RateLimitResult rateLimit(K key, long max, long windowMillis) {
        Long remaining = await(async.limit(key, max, windowMillis));
        return remaining == null ? new RateLimitResult(false, 0) : new RateLimitResult(true, remaining);
    }

    // ---- cache-aside -----------------------------------------------------------------------

    /**
     * Cache-aside with stampede protection on FILLGET/FILLSET: returns the cached value, or lets
     * exactly one caller run {@code factory} while the others poll plain FILLGET with a short
     * backoff (FILLGET WAIT would park the shared connection). {@code fillMillis} bounds how long
     * a filler may take before another caller takes over. The fill time is {@code ttlMillis} limited to
     * between 1 and 30 seconds.
     *
     * <pre>{@code
     * String profile = client.getOrCreate("profile:7", 60_000, () -> loadProfile(7));
     * }</pre>
     *
     * @param key the cache key
     * @param ttlMillis how long the value stays cached, in milliseconds
     * @param factory computes the value on a miss; it must not return {@code null}
     * @return the cached or newly computed value
     * @throws RuvioException when another caller held the fill and the value did not appear in time
     * @throws IllegalArgumentException when the factory returns {@code null}
     */
    public V getOrCreate(K key, long ttlMillis, Supplier<V> factory) {
        return getOrCreate(key, ttlMillis, 0, Math.max(1_000, Math.min(ttlMillis, 30_000)), factory);
    }

    /**
     * Like {@link #getOrCreate(Object, long, Supplier)} with stale-while-revalidate: a value is
     * kept {@code staleMillis} past its TTL; one caller refreshes it while others get the stale value.
     *
     * @param key the cache key
     * @param ttlMillis how long the value is fresh, in milliseconds
     * @param staleMillis how long past the TTL the old value may still be served; 0 disables it
     * @param fillMillis how long a filler may run before another caller may take over, in milliseconds
     * @param factory computes the value on a miss; it must not return {@code null}
     * @return the cached, stale or newly computed value
     * @throws RuvioException when the value did not appear within {@code fillMillis} plus one second
     * @throws IllegalArgumentException when the factory returns {@code null}
     */
    public V getOrCreate(K key, long ttlMillis, long staleMillis, long fillMillis, Supplier<V> factory) {
        long deadline = System.nanoTime() + (fillMillis + 1_000) * 1_000_000L;
        for (int attempt = 0; ; attempt++) {
            FillResult<V> r = await(async.fillget(key, fillMillis));
            switch (r.state()) {
                case HIT:
                    return r.value();
                case STALE:
                    if (r.token() == null) {
                        return r.value();
                    }
                    return fill(key, ttlMillis, staleMillis, r.token(), factory);
                case FILL:
                    return fill(key, ttlMillis, staleMillis, r.token(), factory);
                case BUSY:
                default:
                    if (System.nanoTime() >= deadline) {
                        throw new RuvioException("getOrCreate: value for key was not filled in time");
                    }
                    backoff(attempt);
            }
        }
    }

    private V fill(K key, long ttlMillis, long staleMillis, String token, Supplier<V> factory) {
        V value = factory.get();
        if (value == null) {
            throw new IllegalArgumentException("getOrCreate factory returned null");
        }
        if (staleMillis > 0) {
            await(async.fillset(key, value, ttlMillis, token, staleMillis));
        } else {
            await(async.fillset(key, value, ttlMillis, token));
        }
        return value;
    }

    // ---- idempotency -----------------------------------------------------------------------

    /**
     * Runs {@code action} at most once per (key, fingerprint) while the record lives: IDEM BEGIN
     * reserves the key; the winner runs the action and publishes its result with IDEM COMPLETE;
     * concurrent callers poll until the result is published. A different fingerprint on the same key
     * throws. Not exactly-once for external side effects if the winner crashes mid-way.
     *
     * <pre>{@code
     * String receipt = client.idempotent("pay:order-9", fingerprintOf(request), 86_400_000, 5_000,
     *         () -> charge(request));
     * }</pre>
     *
     * @param key the idempotency key (for example the client-supplied request id)
     * @param fingerprint a hash of the request; a different fingerprint on the same key is a conflict
     * @param ttlMillis how long the record lives, in milliseconds
     * @param waitMillis how long a concurrent caller polls for the winner's result, in milliseconds
     * @param action the work to run; its text result is stored and returned to repeat callers
     * @return the action's result, or the stored result of the first successful run
     * @throws RuvioException when the key is reserved for a different fingerprint, or another caller is
     *         still running the action after {@code waitMillis}
     */
    public String idempotent(K key, String fingerprint, long ttlMillis, long waitMillis, Supplier<String> action) {
        return idempotent(key, fingerprint, ttlMillis, waitMillis, action, false);
    }

    /**
     * Like {@link #idempotent(Object, String, long, long, Supplier)}; with {@code abortOnError} set, an
     * exception from {@code action} first sends {@code IDEM ABORT}, so a retry runs the action again at
     * once instead of finding the key pending until {@code ttlMillis}, and then rethrows the original
     * exception. A failed abort (for example Ruvio unreachable) is ignored and the record stays pending.
     * Use it only when a failed action did not take effect or is safe to run again.
     *
     * <pre>{@code
     * String receipt = client.idempotent("pay:order-9", fingerprintOf(request), 86_400_000, 5_000,
     *         () -> charge(request), true);
     * }</pre>
     *
     * @param key the idempotency key
     * @param fingerprint a hash of the request
     * @param ttlMillis how long the record lives, in milliseconds
     * @param waitMillis how long a concurrent caller polls for the winner's result, in milliseconds
     * @param action the work to run
     * @param abortOnError {@code true} to release the reservation when {@code action} throws; {@code false}
     *        (what the overload without this parameter uses) leaves it pending
     * @return the action's result, or the stored result of the first successful run
     * @throws RuvioException when the key is reserved for a different fingerprint, or another caller is
     *         still running the action after {@code waitMillis}
     */
    public String idempotent(K key, String fingerprint, long ttlMillis, long waitMillis, Supplier<String> action,
            boolean abortOnError) {
        String owner = newToken();
        long deadline = System.nanoTime() + waitMillis * 1_000_000L;
        for (int attempt = 0; ; attempt++) {
            IdemResult r = await(async.idemBegin(key, fingerprint, owner, ttlMillis));
            switch (r.state()) {
                case ACQUIRED: {
                    String result;
                    try {
                        result = action.get();
                    } catch (RuntimeException | Error e) {
                        if (abortOnError) {
                            try {
                                await(async.idemAbort(key, fingerprint, owner));
                            } catch (RuntimeException ignored) {
                                // the record stays pending until its retention; the action's error wins
                            }
                        }
                        throw e;
                    }
                    await(async.idemComplete(key, fingerprint, owner, result.getBytes(StandardCharsets.UTF_8)));
                    return result;
                }
                case COMPLETED:
                    return r.resultString();
                case CONFLICT:
                    throw new RuvioException("idempotency key is reserved for a different fingerprint");
                case PENDING:
                default:
                    if (System.nanoTime() >= deadline) {
                        throw new RuvioException("idempotent: another caller is still running the action");
                    }
                    backoff(attempt);
            }
        }
    }

    // ---- scan iterators --------------------------------------------------------------------

    /**
     * Lazily iterates every key matching {@code options} (null for all) with SCAN. In cluster mode
     * the iterator walks the nodes one after the other (each with its own cursor), so it sees every
     * node's keys; a single {@code scan(cursor)} call only covers the node it lands on.
     *
     * <pre>{@code
     * for (String key : client.scanIterable(ScanArgs.match("user:*").limit(500))) {
     *     System.out.println(key);
     * }
     * }</pre>
     *
     * @param options {@code MATCH}/{@code COUNT}/{@code TYPE} settings, or {@code null} for all keys
     * @return an iterable that sends one {@code SCAN} per page; each {@code iterator()} starts over.
     *         Keys added or removed during the walk may or may not appear, and a key can show up twice.
     */
    public Iterable<K> scanIterable(ScanArgs options) {
        if (router != null) {
            return () -> new NodeChain<>(router.masters(), n -> {
                RuvioClient<K, V> c = nodeView(n);
                return c.scanIterable(options).iterator();
            });
        }
        return ScanIterator.iterable(cursor -> await(options == null ? async.scan(cursor) : async.scan(cursor, options)));
    }

    /**
     * Same walk as {@link #scanIterable(ScanArgs)}, as a lazy stream.
     *
     * @param options {@code MATCH}/{@code COUNT}/{@code TYPE} settings, or {@code null} for all keys
     * @return a stream of keys; pages are fetched as the stream is consumed
     */
    public Stream<K> scanStream(ScanArgs options) {
        if (router != null) {
            return ScanIterator.streamOf(scanIterable(options));
        }
        return ScanIterator.stream(cursor -> await(options == null ? async.scan(cursor) : async.scan(cursor, options)));
    }

    /** Concatenates the iterators of several nodes, opening each one only when the previous is exhausted. */
    private static final class NodeChain<T> implements java.util.Iterator<T> {
        private final java.util.Iterator<ClusterNode> nodes;
        private final java.util.function.Function<ClusterNode, java.util.Iterator<T>> open;
        private java.util.Iterator<T> current = java.util.Collections.emptyIterator();

        NodeChain(List<ClusterNode> nodes, java.util.function.Function<ClusterNode, java.util.Iterator<T>> open) {
            this.nodes = nodes.iterator();
            this.open = open;
        }

        @Override
        public boolean hasNext() {
            while (!current.hasNext()) {
                if (!nodes.hasNext()) {
                    return false;
                }
                current = open.apply(nodes.next());
            }
            return true;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }
            return current.next();
        }
    }

    /**
     * Lazily walks the field/value pairs of a collection with {@code HSCAN}, one page per round trip.
     *
     * @param key the key of the collection
     * @param options {@code MATCH}/{@code COUNT} settings, or {@code null} for the defaults
     * @return an iterable of field/value pairs; each {@code iterator()} starts a new walk. A missing key gives an empty walk.
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds another type
     */
    public Iterable<KeyValue<K, V>> hscanIterable(K key, ScanArgs options) {
        return ScanIterator.iterable(cursor -> await(options == null ? async.hscan(key, cursor) : async.hscan(key, cursor, options)));
    }

    /**
     * Same walk as {@link #hscanIterable}, as a lazy stream.
     *
     * @param key the key of the collection
     * @param options {@code MATCH}/{@code COUNT} settings, or {@code null} for the defaults
     * @return a stream of field/value pairs; pages are fetched as the stream is consumed
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds another type
     */
    public Stream<KeyValue<K, V>> hscanStream(K key, ScanArgs options) {
        return ScanIterator.stream(cursor -> await(options == null ? async.hscan(key, cursor) : async.hscan(key, cursor, options)));
    }

    /**
     * Lazily walks the members of a collection with {@code SSCAN}, one page per round trip.
     *
     * @param key the key of the collection
     * @param options {@code MATCH}/{@code COUNT} settings, or {@code null} for the defaults
     * @return an iterable of members; each {@code iterator()} starts a new walk. A missing key gives an empty walk.
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds another type
     */
    public Iterable<V> sscanIterable(K key, ScanArgs options) {
        return ScanIterator.iterable(cursor -> await(options == null ? async.sscan(key, cursor) : async.sscan(key, cursor, options)));
    }

    /**
     * Same walk as {@link #sscanIterable}, as a lazy stream.
     *
     * @param key the key of the collection
     * @param options {@code MATCH}/{@code COUNT} settings, or {@code null} for the defaults
     * @return a stream of members; pages are fetched as the stream is consumed
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds another type
     */
    public Stream<V> sscanStream(K key, ScanArgs options) {
        return ScanIterator.stream(cursor -> await(options == null ? async.sscan(key, cursor) : async.sscan(key, cursor, options)));
    }

    /**
     * Lazily walks the members with scores of a collection with {@code ZSCAN}, one page per round trip.
     *
     * @param key the key of the collection
     * @param options {@code MATCH}/{@code COUNT} settings, or {@code null} for the defaults
     * @return an iterable of members with scores; each {@code iterator()} starts a new walk. A missing key gives an empty walk.
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds another type
     */
    public Iterable<ScoredValue<V>> zscanIterable(K key, ScanArgs options) {
        return ScanIterator.iterable(cursor -> await(options == null ? async.zscan(key, cursor) : async.zscan(key, cursor, options)));
    }

    /**
     * Same walk as {@link #zscanIterable}, as a lazy stream.
     *
     * @param key the key of the collection
     * @param options {@code MATCH}/{@code COUNT} settings, or {@code null} for the defaults
     * @return a stream of members with scores; pages are fetched as the stream is consumed
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds another type
     */
    public Stream<ScoredValue<V>> zscanStream(K key, ScanArgs options) {
        return ScanIterator.stream(cursor -> await(options == null ? async.zscan(key, cursor) : async.zscan(key, cursor, options)));
    }

    // ---- typed JSON ------------------------------------------------------------------------

    /**
     * Sends {@code JSON.SET key path json} with {@code value} serialized by the configured {@link JsonCodec}.
     *
     * <pre>{@code
     * client.setJsonCodec(myJacksonCodec);
     * client.jsonSetObject("user:7", "$", new User("Ada", 36));
     * }</pre>
     *
     * @param key the document key
     * @param path the JSON path; {@code $} for the whole document
     * @param value the object to store
     * @return {@code true} when the value was set
     * @throws IllegalArgumentException when the default passthrough codec is given something other than JSON text
     * @throws RuvioServerException on an invalid path or a missing parent
     */
    public boolean jsonSetObject(K key, String path, Object value) {
        return await(async.jsonSet(key, path, json.toJson(value)));
    }

    /**
     * Sends {@code JSON.GET key path} and deserializes the result with the configured {@link JsonCodec}.
     *
     * <pre>{@code
     * User user = client.jsonGetObject("user:7", "$", User.class);
     * }</pre>
     *
     * @param key the document key
     * @param path the JSON path
     * @param type the target class
     * @param <T> the target type
     * @return the value, or {@code null} when the key or path does not exist
     */
    public <T> T jsonGetObject(K key, String path, Class<T> type) {
        String text = await(async.jsonGet(key, path));
        return text == null ? null : json.fromJson(text, type);
    }

    /**
     * Sends {@code JSON.MGET key... path} and deserializes each document with the configured
     * {@link JsonCodec}.
     *
     * @param path the JSON path applied to every key
     * @param type the target class
     * @param keys the document keys
     * @param <T> the target type
     * @return one entry per key, in order; {@code null} for a missing key or path
     */
    @SafeVarargs
    public final <T> List<T> jsonMgetObjects(String path, Class<T> type, K... keys) {
        List<String> texts = await(async.jsonMget(path, keys));
        List<T> out = new ArrayList<>(texts.size());
        for (String t : texts) {
            out.add(t == null ? null : json.fromJson(t, type));
        }
        return out;
    }

    // ---- capped lists and Bloom --------------------------------------------------------------

    /**
     * Keeps a "recent items" list with {@code LPUSHCAP key cap value...}: pushes at the head and trims
     * the list to the newest {@code cap} items in one step.
     *
     * <pre>{@code
     * client.pushRecent("recent:user:7", 50, "page-a", "page-b");
     * }</pre>
     *
     * @param key the list key
     * @param cap the maximum list length (above 0)
     * @param values the items to push, in order (the last one ends up first)
     * @return the list length after trimming
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds another type
     */
    @SafeVarargs
    public final long pushRecent(K key, long cap, V... values) {
        return await(async.lpushcap(key, cap, values));
    }

    /**
     * Reads the newest items of a list built with {@link #pushRecent}.
     *
     * @param key the list key
     * @param count how many items to read (from the head, so the newest first)
     * @return up to {@code count} items; empty when the key is missing
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds another type
     */
    public List<V> recent(K key, long count) {
        return await(async.lrange(key, 0, count - 1));
    }

    /**
     * Bloom "seen before": BF.MADD each item and report, per item, whether it was (probably)
     * already present. False positives are possible; false negatives are not.
     *
     * <pre>{@code
     * boolean seen = client.seenBefore("events", eventId).get(0);
     * }</pre>
     *
     * @param key the Bloom filter key (created on first use)
     * @param items the items to check and add
     * @return one entry per item: {@code true} when it was probably already present, {@code false} when new
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds another type
     */
    @SafeVarargs
    public final List<Boolean> seenBefore(K key, V... items) {
        List<Boolean> added = await(async.bfMadd(key, items));
        List<Boolean> out = new ArrayList<>(added.size());
        for (Boolean a : added) {
            out.add(!a);
        }
        return out;
    }

    /**
     * Checks Bloom filter membership without adding, with {@code BF.MEXISTS}.
     *
     * @param key the Bloom filter key
     * @param items the items to check
     * @return one entry per item: {@code true} when it is probably present, {@code false} when it is
     *         definitely absent (all {@code false} for a missing key)
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds another type
     */
    @SafeVarargs
    public final List<Boolean> mightContain(K key, V... items) {
        return await(async.bfMexists(key, items));
    }

    /**
     * Computes the cluster hash slot of a key locally (CRC16 modulo 16384; only the part inside the
     * first {@code {hashtag}} counts when there is one).
     *
     * @param key the key bytes
     * @return the slot, from 0 to 16383
     */
    public static int hashSlot(byte[] key) {
        return HashSlot.of(key);
    }

    /**
     * Computes the cluster hash slot of a key given as text (UTF-8).
     *
     * @param key the key
     * @return the slot, from 0 to 16383
     */
    public static int hashSlot(String key) {
        return HashSlot.of(key.getBytes(StandardCharsets.UTF_8));
    }
}
