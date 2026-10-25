// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import io.github.salihcantekin.ruvio.internal.Address;
import io.github.salihcantekin.ruvio.internal.ClusterRouter;
import io.github.salihcantekin.ruvio.internal.Connection;
import io.github.salihcantekin.ruvio.internal.HashSlot;
import io.github.salihcantekin.ruvio.internal.Link;
import io.github.salihcantekin.ruvio.internal.Reply;
import io.github.salihcantekin.ruvio.internal.Request;
import io.github.salihcantekin.ruvio.internal.RespOut;
import io.github.salihcantekin.ruvio.internal.RespReader;
import io.github.salihcantekin.ruvio.model.PubSubMessage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Pub/Sub on its own connection. Messages are delivered to the listener on the reader thread, in
 * order; keep the listener fast. If the socket drops, the subscriber reconnects in the background
 * and subscribes again to everything it had (messages published meanwhile are lost). Get one from
 * {@link RuvioClient#pubSub(Consumer)}; publish with {@code client.publish(channel, message)} on a
 * normal connection (a subscriber connection can only subscribe).
 *
 * <p>The {@code *Async} methods return a future that completes when the server has confirmed every
 * channel or pattern; the others wait for that confirmation (up to {@code commandTimeoutMillis}, or
 * 10 seconds when none is set). While the socket is down they throw a {@link RuvioConnectionException}.
 * In a cluster, plain channels and patterns live on the seed node (a message published on another
 * node does not reach them); shard channels are subscribed on the node that owns their slot.
 *
 * <pre>{@code
 * try (RuvioPubSub sub = client.pubSub(msg -> System.out.println(msg))) {
 *     sub.subscribe("orders", "alerts");
 *     sub.psubscribe("user:*");
 *     Thread.sleep(60_000);
 * }
 * }</pre>
 */
public final class RuvioPubSub implements AutoCloseable {
    private static final byte[] SUBSCRIBE = RespOut.fragment("SUBSCRIBE");
    private static final byte[] UNSUBSCRIBE = RespOut.fragment("UNSUBSCRIBE");
    private static final byte[] PSUBSCRIBE = RespOut.fragment("PSUBSCRIBE");
    private static final byte[] PUNSUBSCRIBE = RespOut.fragment("PUNSUBSCRIBE");
    private static final byte[] SSUBSCRIBE = RespOut.fragment("SSUBSCRIBE");
    private static final byte[] SUNSUBSCRIBE = RespOut.fragment("SUNSUBSCRIBE");
    private static final byte[] PING = RespOut.fragment("PING");

    private final RuvioOptions options;
    private final Consumer<PubSubMessage> listener;
    private final Set<String> channels = new LinkedHashSet<>();
    private final Set<String> patterns = new LinkedHashSet<>();
    private final Set<String> shardChannels = new LinkedHashSet<>();
    private volatile SubConnection connection;
    private volatile boolean closed;
    // Cluster mode: SSUBSCRIBE goes to the node that owns the channel's slot, one subscriber per node.
    private final ClusterRouter router;
    /** True for the per-node subscribers of a cluster: SSUBSCRIBE must then carry one slot per command. */
    private final boolean slotGroups;
    /** The addresses tried in order: the seeds, or the single host and port. */
    private final Address[] addresses;
    /** Index of the address the subscriber connection is (or was last) on. */
    private volatile int opened;
    private final java.util.Map<String, RuvioPubSub> shardSubscribers = new java.util.LinkedHashMap<>();

    RuvioPubSub(RuvioOptions options, Consumer<PubSubMessage> listener) {
        this(options, listener, null, false);
    }

    RuvioPubSub(RuvioOptions options, Consumer<PubSubMessage> listener, ClusterRouter router) {
        this(options, listener, router, false);
    }

    private RuvioPubSub(RuvioOptions options, Consumer<PubSubMessage> listener, ClusterRouter router, boolean slotGroups) {
        this.options = options;
        this.listener = listener;
        this.router = router;
        this.slotGroups = slotGroups;
        this.addresses = options.seeds.isEmpty()
                ? new Address[] {new Address(options.host, options.port)}
                : Address.parseAll(options.seeds).toArray(new Address[0]);
        this.connection = open(0);
    }

    /**
     * Opens the first address that accepts a connection, starting at {@code start} and wrapping around, one
     * attempt each. A refused {@code AUTH} is thrown at once.
     */
    private SubConnection open(int start) {
        RuvioConnectionException last = null;
        int n = addresses.length;
        for (int step = 0; step < n; step++) {
            int index = (start + step) % n;
            Address a = addresses[index];
            SubConnection c;
            try {
                c = new SubConnection(this, a.host, a.port, options);
            } catch (IOException e) {
                last = new RuvioConnectionException("cannot connect to " + a + ": " + e.getMessage(), e);
                continue;
            }
            try {
                Link.handshake(c, options, 0);
            } catch (RuntimeException e) {
                c.close();
                throw e;
            }
            opened = index;
            return c;
        }
        throw last;
    }

    // ---- API -------------------------------------------------------------------------------

    /**
     * Sends {@code SUBSCRIBE channel [channel ...]}.
     *
     * @param channels the channel names (at least one)
     * @return a future that completes when the server confirmed every channel
     * @throws IllegalArgumentException when no channel is given
     * @throws RuvioConnectionException when the subscriber is closed or reconnecting
     */
    public CompletableFuture<Void> subscribeAsync(String... channels) {
        return send(SUBSCRIBE, "subscribe", this.channels, true, channels);
    }

    /**
     * Sends {@code UNSUBSCRIBE [channel ...]}.
     *
     * @param channels the channels to leave; none leaves every exact channel
     * @return a future that completes when the server confirmed it
     * @throws RuvioConnectionException when the subscriber is closed or reconnecting
     */
    public CompletableFuture<Void> unsubscribeAsync(String... channels) {
        return send(UNSUBSCRIBE, "unsubscribe", this.channels, false, channels);
    }

    /**
     * Sends {@code PSUBSCRIBE pattern [pattern ...]}. Patterns support {@code *}, {@code ?} and
     * {@code [abc]}; matching messages arrive with kind {@code PMESSAGE}.
     *
     * @param patterns the glob patterns (at least one)
     * @return a future that completes when the server confirmed every pattern
     * @throws IllegalArgumentException when no pattern is given
     * @throws RuvioConnectionException when the subscriber is closed or reconnecting
     */
    public CompletableFuture<Void> psubscribeAsync(String... patterns) {
        return send(PSUBSCRIBE, "psubscribe", this.patterns, true, patterns);
    }

    /**
     * Sends {@code PUNSUBSCRIBE [pattern ...]}.
     *
     * @param patterns the patterns to leave; none leaves every pattern
     * @return a future that completes when the server confirmed it
     * @throws RuvioConnectionException when the subscriber is closed or reconnecting
     */
    public CompletableFuture<Void> punsubscribeAsync(String... patterns) {
        return send(PUNSUBSCRIBE, "punsubscribe", this.patterns, false, patterns);
    }

    /**
     * SSUBSCRIBE. In cluster mode each channel is subscribed on the node that owns its slot (the
     * subscriber opens one extra connection per node it needs); plain SUBSCRIBE/PSUBSCRIBE stay on the
     * seed connection. Messages arrive with kind {@code SMESSAGE}.
     *
     * @param channels the shard channel names (at least one)
     * @return a future that completes when the server confirmed every channel
     * @throws IllegalArgumentException when no channel is given
     * @throws RuvioConnectionException when the subscriber is closed or reconnecting
     */
    public CompletableFuture<Void> ssubscribeAsync(String... channels) {
        if (router != null) {
            return shardFanOut(channels, true);
        }
        return send(SSUBSCRIBE, "ssubscribe", this.shardChannels, true, channels);
    }

    /**
     * Sends {@code SUNSUBSCRIBE [channel ...]}.
     *
     * @param channels the shard channels to leave; none leaves every shard channel
     * @return a future that completes when the server confirmed it
     * @throws RuvioConnectionException when the subscriber is closed or reconnecting
     */
    public CompletableFuture<Void> sunsubscribeAsync(String... channels) {
        if (router != null) {
            return shardFanOut(channels, false);
        }
        return send(SUNSUBSCRIBE, "sunsubscribe", this.shardChannels, false, channels);
    }

    private CompletableFuture<Void> shardFanOut(String[] channels, boolean add) {
        List<CompletableFuture<Void>> futures = new java.util.ArrayList<>();
        synchronized (shardSubscribers) {
            if (closed) {
                throw new RuvioConnectionException("subscriber is closed");
            }
            if (channels.length == 0) {
                if (add) {
                    throw new IllegalArgumentException("at least one channel is required");
                }
                for (RuvioPubSub sub : shardSubscribers.values()) {
                    futures.add(sub.sunsubscribeAsync());
                }
            } else {
                // The server wants one slot per (S)SUBSCRIBE: one command per slot, on that slot's node.
                java.util.Map<Integer, List<String>> perSlot = new java.util.LinkedHashMap<>();
                for (String ch : channels) {
                    perSlot.computeIfAbsent(HashSlot.of(ch.getBytes(StandardCharsets.UTF_8)), a -> new java.util.ArrayList<>()).add(ch);
                }
                for (java.util.Map.Entry<Integer, List<String>> e : perSlot.entrySet()) {
                    String address = router.owner(e.getKey()).address;
                    RuvioPubSub sub = shardSubscribers.get(address);
                    if (sub == null) {
                        if (!add) {
                            continue;
                        }
                        sub = new RuvioPubSub(router.node(address).link.siblingOptions(), listener, null, true);
                        shardSubscribers.put(address, sub);
                    }
                    String[] names = e.getValue().toArray(new String[0]);
                    futures.add(add ? sub.ssubscribeAsync(names) : sub.sunsubscribeAsync(names));
                }
            }
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]));
    }

    /**
     * Sends {@code SUBSCRIBE channel [channel ...]} and waits until the server confirmed every channel.
     *
     * @param channels the channel names (at least one)
     * @throws IllegalArgumentException when no channel is given
     * @throws RuvioConnectionException when the subscriber is closed or reconnecting
     * @throws RuvioException when the confirmation does not arrive in time
     */
    public void subscribe(String... channels) {
        await(subscribeAsync(channels));
    }

    /**
     * Sends {@code UNSUBSCRIBE [channel ...]} and waits for the confirmation.
     *
     * @param channels the channels to leave; none leaves every exact channel
     * @throws RuvioConnectionException when the subscriber is closed or reconnecting
     * @throws RuvioException when the confirmation does not arrive in time
     */
    public void unsubscribe(String... channels) {
        await(unsubscribeAsync(channels));
    }

    /**
     * Sends {@code PSUBSCRIBE pattern [pattern ...]} and waits for the confirmation.
     *
     * @param patterns the glob patterns such as {@code news.*} (at least one)
     * @throws IllegalArgumentException when no pattern is given
     * @throws RuvioConnectionException when the subscriber is closed or reconnecting
     * @throws RuvioException when the confirmation does not arrive in time
     */
    public void psubscribe(String... patterns) {
        await(psubscribeAsync(patterns));
    }

    /**
     * Sends {@code PUNSUBSCRIBE [pattern ...]} and waits for the confirmation.
     *
     * @param patterns the patterns to leave; none leaves every pattern
     * @throws RuvioConnectionException when the subscriber is closed or reconnecting
     * @throws RuvioException when the confirmation does not arrive in time
     */
    public void punsubscribe(String... patterns) {
        await(punsubscribeAsync(patterns));
    }

    /**
     * Sends {@code SSUBSCRIBE shardchannel [shardchannel ...]} and waits for the confirmation. See
     * {@link #ssubscribeAsync(String...)} for cluster behaviour.
     *
     * @param channels the shard channel names (at least one)
     * @throws IllegalArgumentException when no channel is given
     * @throws RuvioConnectionException when the subscriber is closed or reconnecting
     * @throws RuvioException when the confirmation does not arrive in time
     */
    public void ssubscribe(String... channels) {
        await(ssubscribeAsync(channels));
    }

    /**
     * Sends {@code SUNSUBSCRIBE [shardchannel ...]} and waits for the confirmation.
     *
     * @param channels the shard channels to leave; none leaves every shard channel
     * @throws RuvioConnectionException when the subscriber is closed or reconnecting
     * @throws RuvioException when the confirmation does not arrive in time
     */
    public void sunsubscribe(String... channels) {
        await(sunsubscribeAsync(channels));
    }

    /**
     * Sends {@code PING} on the subscriber connection and waits for the reply, to check that it is alive.
     *
     * @throws RuvioConnectionException when the subscriber is closed or reconnecting
     * @throws RuvioException when there is no reply in time
     */
    public void ping() {
        SubConnection c = live();
        RespOut o = c.begin();
        o.header(1);
        o.raw(PING);
        Request<Object> r = new Request<>(Reply.RAW, RuvioCodec.UTF8, RuvioCodec.UTF8);
        c.end(r);
        await(r);
    }

    /**
     * Returns the exact channels this subscriber is subscribed to (they are restored after a reconnect).
     *
     * @return a copy of the channel names
     */
    public synchronized Set<String> channels() {
        return new LinkedHashSet<>(channels);
    }

    /**
     * Returns the patterns this subscriber is subscribed to.
     *
     * @return a copy of the patterns
     */
    public synchronized Set<String> patterns() {
        return new LinkedHashSet<>(patterns);
    }

    /**
     * Returns the shard channels this subscriber is subscribed to (across all cluster nodes).
     *
     * @return a copy of the shard channel names
     */
    public synchronized Set<String> shardChannels() {
        Set<String> out = new LinkedHashSet<>(shardChannels);
        synchronized (shardSubscribers) {
            for (RuvioPubSub sub : shardSubscribers.values()) {
                out.addAll(sub.shardChannels());
            }
        }
        return out;
    }

    private void await(CompletableFuture<?> f) {
        Link.await(f, options.commandTimeoutMillis > 0 ? options.commandTimeoutMillis : 10_000);
    }

    private SubConnection live() {
        SubConnection c = connection;
        if (closed) {
            throw new RuvioConnectionException("subscriber is closed");
        }
        if (c == null || !c.isOpen()) {
            throw new RuvioConnectionException("subscriber connection is down (reconnecting)");
        }
        return c;
    }

    private CompletableFuture<Void> send(byte[] cmd, String kind, Set<String> set, boolean add, String[] names) {
        SubConnection c = live();
        int expected;
        synchronized (this) {
            if (add) {
                if (names.length == 0) {
                    throw new IllegalArgumentException("at least one channel is required");
                }
                for (String n : names) {
                    set.add(n);
                }
                expected = names.length;
            } else if (names.length == 0) {
                expected = Math.max(1, set.size());
                set.clear();
            } else {
                for (String n : names) {
                    set.remove(n);
                }
                expected = names.length;
            }
        }
        return c.subscribeCommand(cmd, kind, expected, names);
    }

    /** Closes the subscriber connection (and the per-node shard connections). It does not reconnect afterwards. */
    @Override
    public void close() {
        closed = true;
        SubConnection c = connection;
        if (c != null) {
            c.close();
        }
        synchronized (shardSubscribers) {
            for (RuvioPubSub sub : shardSubscribers.values()) {
                sub.close();
            }
        }
    }

    void deliver(PubSubMessage m) {
        try {
            listener.accept(m);
        } catch (RuntimeException ignored) {
            // a failing listener must not stop delivery
        }
    }

    void lost(SubConnection c) {
        if (closed || connection != c) {
            return;
        }
        Thread t = new Thread(() -> reconnectLoop(c), "ruvio-pubsub-reconnect");
        t.setDaemon(true);
        t.start();
    }

    private void reconnectLoop(SubConnection old) {
        long delay = 50;
        while (!closed) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                return;
            }
            try {
                // Each attempt tries every address once, starting with the one after the address that dropped.
                SubConnection c = open((opened + 1) % addresses.length);
                String[] ch;
                String[] pt;
                String[] sh;
                synchronized (this) {
                    ch = channels.toArray(new String[0]);
                    pt = patterns.toArray(new String[0]);
                    sh = shardChannels.toArray(new String[0]);
                }
                if (ch.length > 0) c.subscribeCommand(SUBSCRIBE, "subscribe", ch.length, ch);
                if (pt.length > 0) c.subscribeCommand(PSUBSCRIBE, "psubscribe", pt.length, pt);
                if (sh.length > 0) {
                    if (slotGroups) {
                        java.util.Map<Integer, List<String>> perSlot = new java.util.LinkedHashMap<>();
                        for (String name : sh) {
                            perSlot.computeIfAbsent(HashSlot.of(name.getBytes(StandardCharsets.UTF_8)), a -> new java.util.ArrayList<>()).add(name);
                        }
                        for (List<String> group : perSlot.values()) {
                            c.subscribeCommand(SSUBSCRIBE, "ssubscribe", group.size(), group.toArray(new String[0]));
                        }
                    } else {
                        c.subscribeCommand(SSUBSCRIBE, "ssubscribe", sh.length, sh);
                    }
                }
                if (closed) {
                    c.close();
                    return;
                }
                connection = c;
                return;
            } catch (RuntimeException e) {
                delay = Math.min(delay * 2, 2_000);
            }
        }
    }

    /** Waits for {@code remaining} confirmations of one kind. */
    static final class Ack extends Request<Void> {
        final String kind;
        int remaining;

        Ack(String kind, int remaining) {
            super(Reply.OK, RuvioCodec.UTF8, RuvioCodec.UTF8);
            this.kind = kind;
            this.remaining = remaining;
        }
    }

    static final class SubConnection extends Connection {
        private final RuvioPubSub owner;

        SubConnection(RuvioPubSub owner, String host, int port, RuvioOptions options) throws IOException {
            super(host, port, options.connectTimeoutMillis, 1024, "ruvio-pubsub", options.sslContext, options.tlsServerName);
            this.owner = owner;
        }

        CompletableFuture<Void> subscribeCommand(byte[] cmd, String kind, int expected, String[] names) {
            RespOut o = begin();
            try {
                o.header(1 + names.length);
                o.raw(cmd);
                for (String n : names) {
                    o.bulk(n);
                }
            } catch (RuntimeException e) {
                abort();
                throw e;
            }
            Ack ack = new Ack(kind, expected);
            end(ack);
            return ack;
        }

        @Override
        public void dispatch(RespReader r) {
            byte t = r.peek();
            if (t != '*' && t != '>') {
                super.dispatch(r);
                return;
            }
            int start = r.pos;
            Object tree = r.readObject();
            List<Object> items = tree instanceof List ? Reply.list(tree) : null;
            String kind = items != null && !items.isEmpty() ? Reply.str(items.get(0)) : null;
            if (kind != null) {
                kind = kind.toLowerCase(java.util.Locale.ROOT);
            }
            if ("message".equals(kind) && items.size() >= 3) {
                owner.deliver(new PubSubMessage(PubSubMessage.Kind.MESSAGE, null, Reply.str(items.get(1)), bytes(items.get(2))));
                return;
            }
            if ("smessage".equals(kind) && items.size() >= 3) {
                owner.deliver(new PubSubMessage(PubSubMessage.Kind.SMESSAGE, null, Reply.str(items.get(1)), bytes(items.get(2))));
                return;
            }
            if ("pmessage".equals(kind) && items.size() >= 4) {
                owner.deliver(new PubSubMessage(PubSubMessage.Kind.PMESSAGE, Reply.str(items.get(1)), Reply.str(items.get(2)), bytes(items.get(3))));
                return;
            }
            Request<?> head = peekRequest();
            if (kind != null && kind.endsWith("subscribe")) {
                if (head instanceof Ack && ((Ack) head).kind.equals(kind)) {
                    Ack ack = (Ack) head;
                    if (--ack.remaining <= 0) {
                        popRequest();
                        ack.deliver(null);
                    }
                }
                return; // confirmations beyond what was asked for are ignored
            }
            if (head == null) {
                return;
            }
            // Any other reply (PING's ["pong", ""]) answers the head request.
            r.pos = start;
            super.dispatch(r);
        }

        private static byte[] bytes(Object o) {
            if (o instanceof byte[]) {
                return (byte[]) o;
            }
            return o == null ? new byte[0] : o.toString().getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public void onClosed(Throwable cause) {
            owner.lost(this);
        }
    }
}
