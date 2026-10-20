// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import io.github.salihcantekin.ruvio.RuvioConnectionException;
import io.github.salihcantekin.ruvio.RuvioException;
import io.github.salihcantekin.ruvio.ReadPolicy;
import io.github.salihcantekin.ruvio.RuvioOptions;
import io.github.salihcantekin.ruvio.RuvioServerException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Routes encoded commands to the node that owns their key's slot.
 *
 * <p>The slot table (16384 entries) is built from {@code CLUSTER SLOTS}. Each node has its own lazily
 * opened multiplexed {@link Link}; a lost node connection only affects commands for that node and is
 * reopened by the next command. A {@code -MOVED} reply points that one slot at the named node and the
 * command is sent there, at most {@link #MAX_REDIRECTS} times per command; the whole table is re-read from
 * the server only when the named node is unknown or from the command's second MOVED on, and the retry still
 * goes to the named node.
 *
 * <p>With a {@link ReadPolicy} other than {@code PRIMARY}, reads with a key go to one replica of the key's shard
 * (listed after the primary in {@code CLUSTER SLOTS}, picked at random whenever the table loads). A replica that
 * cannot be opened or answers {@code LOADING} is skipped until the next load.
 */
public final class ClusterRouter implements AutoCloseable {
    /** The number of hash slots. */
    public static final int SLOTS = 16384;
    /** MOVED redirects followed per command (each to the node the reply names) before the error is returned. */
    public static final int MAX_REDIRECTS = 3;
    /** Sends again after {@code TRYAGAIN} per command (waiting 5, 10 and 20 ms) before the error is returned. */
    public static final int MAX_TRYAGAIN = 3;
    /** The wait before the first {@code TRYAGAIN} retry; it doubles for each further one. */
    private static final long TRYAGAIN_WAIT_MS = 5;
    private static final byte[] ASKING = "*1\r\n$6\r\nASKING\r\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

    /** Where a routed frame goes: straight onto a connection, or into a pipeline group. */
    public interface Sink {
        /**
         * Sends an encoded command.
         *
         * @param node the node that owns it
         * @param buf the array holding the command
         * @param off where the command starts
         * @param len the command length
         * @param request the request awaiting the reply
         */
        void send(ClusterNode node, byte[] buf, int off, int len, RoutedRequest<?> request);
    }

    /** Writes the frame to the node's shared connection right away. */
    public static final Sink DIRECT = (node, buf, off, len, request) -> {
        Connection c = node.link.get();
        RespOut o = c.begin();
        try {
            o.raw(buf, off, len);
        } catch (RuntimeException e) {
            c.abort();
            throw e;
        }
        c.end(request);
    };

    private static final ThreadLocal<Frame> FRAMES = ThreadLocal.withInitial(() -> new Frame(true));

    /** Runs MOVED follow-ups off the reader threads, which must never wait for a reply. */
    private static final ExecutorService BACKGROUND = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 30, TimeUnit.SECONDS,
            new SynchronousQueue<>(), r -> {
                Thread t = new Thread(r, "ruvio-cluster");
                t.setDaemon(true);
                return t;
            });

    private final RuvioOptions options;
    private volatile ClusterNode seed;
    /** The configured seed addresses, asked for the slot map when no known node answers. */
    private final Address[] seedAddresses;
    private final ConcurrentHashMap<String, ClusterNode> nodes = new ConcurrentHashMap<>();
    /** The replicas that serve reads; kept apart from {@link #nodes}, which only holds primaries and seeds. */
    private final ConcurrentHashMap<String, ClusterNode> replicas = new ConcurrentHashMap<>();
    /** Reads may go to replicas ({@code readPolicy} is not {@code PRIMARY}); checked once per command. */
    private final boolean replicaReads;
    private volatile ClusterNode[] table = new ClusterNode[SLOTS];
    private final AtomicInteger nextIndex = new AtomicInteger();
    private final Object refreshLock = new Object();
    private volatile boolean closed;

    private ClusterRouter(RuvioOptions options, ClusterNode seed) {
        this.options = options;
        this.replicaReads = options.readPolicy != ReadPolicy.PRIMARY;
        this.seedAddresses = Address.parseAll(options.seeds).toArray(new Address[0]);
        this.seed = seed;
        seed.index = 0;
        nodes.put(seed.address, seed);
    }

    /**
     * Asks the seed for {@code CLUSTER SLOTS}. When the server is not in cluster mode (it answers with an error) the caller keeps using the plain standalone path.
     *
     * @param seedLink the link to the seed node that answered (see {@link Link#openSeed})
     * @param options the client settings
     * @return the router, or null for a standalone server
     */
    public static ClusterRouter detect(Link seedLink, RuvioOptions options) {
        Address at = seedLink.address();
        ClusterRouter r = new ClusterRouter(options, new ClusterNode(at.host, at.port, seedLink));
        try {
            // Only the seed that answered is asked here: a standalone server must not send the client on to the other seeds.
            r.apply(r.querySlots(r.seed), r.seed);
        } catch (RuvioServerException e) {
            return null;
        }
        return r;
    }

    /**
     * Returns the seed node.
     *
     * @return the seed
     */
    public ClusterNode seed() {
        return seed;
    }

    /**
     * Returns the wait used for internal calls.
     *
     * @return {@code commandTimeoutMillis}, or 10000 when none is set
     */
    public long timeoutMillis() {
        return options.commandTimeoutMillis > 0 ? options.commandTimeoutMillis : 10_000;
    }

    // ---- topology --------------------------------------------------------------------------

    /**
     * Returns the node that owns a slot.
     *
     * @param slot the slot, 0 to 16383
     * @return the owner; any node when the table has none
     */
    public ClusterNode owner(int slot) {
        ClusterNode n = table[slot];
        return n != null ? n : anyNode();
    }

    /**
     * Picks a node for keyless commands: the seed while its connection is up, else any live node, else the seed.
     *
     * @return the node
     */
    public ClusterNode anyNode() {
        if (seed.link.isUp()) {
            return seed;
        }
        for (ClusterNode n : nodes.values()) {
            if (n.link.isUp()) {
                return n;
            }
        }
        return seed;
    }

    /**
     * Lists the distinct nodes that own slots, in slot order.
     *
     * @return the nodes
     */
    public List<ClusterNode> masters() {
        LinkedHashSet<ClusterNode> out = new LinkedHashSet<>();
        for (ClusterNode n : table) {
            if (n != null) {
                out.add(n);
            }
        }
        return new ArrayList<>(out);
    }

    /**
     * Finds a known node.
     *
     * @param address {@code host:port}
     * @return the node, or null
     */
    public ClusterNode node(String address) {
        return nodes.get(address);
    }

    /**
     * Returns the node that owns a key.
     *
     * @param key the key bytes
     * @return the owner
     */
    public ClusterNode ownerOfKey(byte[] key) {
        return owner(HashSlot.of(key));
    }

    private ClusterNode newNode(String host, int port) {
        RuvioOptions o = options.toBuilder().seeds().host(host).port(port).database(0).build();
        return new ClusterNode(host, port, new Link(o, true));
    }

    private ClusterNode addNode(String host, int port) {
        return nodes.computeIfAbsent(ClusterNode.address(host, port), a -> {
            ClusterNode n = newNode(host, port);
            n.index = nextIndex.incrementAndGet();
            return n;
        });
    }

    /**
     * Returns how many nodes are known.
     *
     * @return the node count
     */
    public int nodeCount() {
        return nodes.size();
    }

    /**
     * Re-reads {@code CLUSTER SLOTS} and swaps the slot table. Nodes with an open socket are asked first; when
     * none answers (the usual case after a node went away), every other known node and then every seed is opened
     * in turn, one connect attempt each, until one returns a slot map. A seed that cannot be opened is not kept.
     */
    public void refresh() {
        synchronized (refreshLock) {
            RuntimeException last = null;
            List<ClusterNode> candidates = new ArrayList<>(nodes.size());
            candidates.add(seed);
            for (ClusterNode n : nodes.values()) {
                if (n != seed) {
                    candidates.add(n);
                }
            }
            List<ClusterNode> down = null;
            for (ClusterNode n : candidates) {
                if (!n.link.isUp()) {
                    if (down == null) {
                        down = new ArrayList<>();
                    }
                    down.add(n);
                    continue;
                }
                try {
                    apply(querySlots(n), n);
                    return;
                } catch (RuvioException e) {
                    last = e;
                }
            }
            if (down != null) {
                for (ClusterNode n : down) {
                    try {
                        apply(querySlots(n), n);
                        return;
                    } catch (RuvioException e) {
                        last = e;
                    }
                }
            }
            java.util.HashSet<String> seen = new java.util.HashSet<>();
            for (Address a : seedAddresses) {
                String key = ClusterNode.address(a.host, a.port);
                if (nodes.containsKey(key) || !seen.add(key)) {
                    continue;
                }
                ClusterNode n = newNode(a.host, a.port);
                try {
                    n.link.get();
                } catch (RuvioException e) {
                    last = e;
                    n.close();
                    continue;
                }
                n.index = nextIndex.incrementAndGet();
                nodes.put(key, n);
                try {
                    apply(querySlots(n), n);
                    return;
                } catch (RuvioException e) {
                    last = e;
                }
            }
            throw last != null ? last : new RuvioException("no cluster node answered CLUSTER SLOTS");
        }
    }

    private Object querySlots(ClusterNode n) {
        Connection c = n.link.get();
        Request<Object> req = new Link.Raw(c).send(Reply.RAW, "CLUSTER", "SLOTS");
        return Link.await(req, timeoutMillis());
    }

    private void apply(Object tree, ClusterNode from) {
        List<Object> ranges = Reply.list(tree);
        ClusterNode[] t = new ClusterNode[SLOTS];
        int assigned = 0;
        Map<ClusterNode, List<Address>> listed = replicaReads ? new LinkedHashMap<>() : null;
        for (Object o : ranges) {
            List<Object> range = Reply.list(o);
            if (range.size() < 3) {
                continue;
            }
            int start = (int) (long) (Long) range.get(0);
            int end = (int) (long) (Long) range.get(1);
            List<Object> master = Reply.list(range.get(2));
            String host = Reply.str(master.get(0));
            int port = (int) (long) (Long) master.get(1);
            if (host == null || host.isEmpty()) {
                host = from.host;
            }
            ClusterNode n = nodes.get(ClusterNode.address(host, port));
            if (n == null) {
                n = addNode(host, port);
            }
            for (int s = Math.max(0, start); s <= end && s < SLOTS; s++) {
                t[s] = n;
                assigned++;
            }
            if (listed != null) {
                addReplicas(listed.computeIfAbsent(n, k -> new ArrayList<>()), range, from);
            }
        }
        if (assigned == 0) {
            throw new RuvioServerException("ERR CLUSTER SLOTS returned no slot ranges");
        }
        if (listed != null) {
            pickReaders(listed);
        }
        table = t;
    }

    /** Adds the replicas listed after the primary in a range; an entry that cannot be read is skipped. */
    private static void addReplicas(List<Address> into, List<Object> range, ClusterNode from) {
        for (int i = 3; i < range.size(); i++) {
            try {
                List<Object> entry = Reply.list(range.get(i));
                String host = Reply.str(entry.get(0));
                long port = (Long) entry.get(1);
                if (host == null || host.isEmpty()) {
                    host = from.host;
                }
                if (port < 1 || port > 65535) {
                    continue;
                }
                boolean known = false;
                for (Address a : into) {
                    known |= a.port == port && a.host.equals(host);
                }
                if (!known) {
                    into.add(new Address(host, (int) port));
                }
            } catch (RuntimeException ignored) {
                // a malformed replica entry does not fail the load
            }
        }
    }

    /**
     * Gives each primary one of its replicas for reads, kept until the next load. One random number per load, so
     * many clients spread over the replicas.
     */
    private void pickReaders(Map<ClusterNode, List<Address>> listed) {
        int pick = ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
        for (ClusterNode n : nodes.values()) {
            n.reader = null;
        }
        for (Map.Entry<ClusterNode, List<Address>> e : listed.entrySet()) {
            List<Address> list = e.getValue();
            if (list.isEmpty()) {
                continue;
            }
            Address a = list.get(pick % list.size());
            ClusterNode primary = e.getKey();
            ClusterNode replica = replicas.computeIfAbsent(ClusterNode.address(a.host, a.port), k -> {
                RuvioOptions o = options.toBuilder().seeds().host(a.host).port(a.port).database(0).build();
                return new ClusterNode(a.host, a.port, Link.replica(o));
            });
            replica.primary = primary;
            primary.reader = replica;
        }
    }

    // ---- routing ---------------------------------------------------------------------------

    /**
     * Returns the calling thread's scratch buffer for the next frame.
     *
     * @return the buffer to encode into
     */
    public RespOut begin() {
        Frame f = FRAMES.get();
        f.reset();
        return f.out;
    }

    /**
     * Routes the frame in {@code f} (already encoded) and hands it to {@code sink}.
     *
     * @param f the encoded frame
     * @param r the request awaiting the reply
     * @param sink where the frame goes
     */
    public void dispatch(Frame f, RoutedRequest<?> r, Sink sink) {
        dispatch(f, r, sink, false);
    }

    /**
     * Tells whether reads may go to replicas.
     *
     * @return {@code true} when the read policy is not {@code PRIMARY}
     */
    public boolean replicaReads() {
        return replicaReads;
    }

    /**
     * Routes a frame of a pipeline: reads are still sent to the primary's group, but marked in the request, because
     * the pipeline can only go to replicas once it is known that every command in it reads.
     *
     * @param f the encoded frame
     * @param r the request awaiting the reply
     * @param sink where the frame goes
     */
    public void dispatchQueued(Frame f, RoutedRequest<?> r, Sink sink) {
        dispatch(f, r, sink, true);
    }

    private void dispatch(Frame f, RoutedRequest<?> r, Sink sink, boolean queued) {
        f.parse();
        byte[] b = f.out.buf;
        ClusterKeys.Spec spec = ClusterKeys.spec(b, f.off[0], f.len[0]);
        if (replicaReads) {
            r.read = spec.read;
        }
        int slot = -1;
        if (spec.first) {
            if (f.argc >= 2) {
                slot = HashSlot.of(b, f.off[1], f.len[1]);
            }
        } else {
            int n = ClusterKeys.keys(spec, f);
            if (n > 0) {
                int[] k = f.keyIdx;
                slot = HashSlot.of(b, f.off[k[0]], f.len[k[0]]);
                for (int i = 1; i < n; i++) {
                    if (HashSlot.of(b, f.off[k[i]], f.len[k[i]]) != slot) {
                        crossSlot(spec, f, n, r, sink, queued);
                        return;
                    }
                }
            }
        }
        int off = f.base;
        int total = f.out.pos - off;
        r.router = this;
        byte[] buf = b;
        if (f.retains()) {
            f.keep();
        } else {
            buf = Arrays.copyOfRange(b, off, off + total);
            off = 0;
        }
        r.setFrame(buf, off, total);
        route(sink, slot, buf, off, total, r, queued);
    }

    /** Sends a frame to the owner of {@code slot} (any node when the slot is -1). */
    private void route(Sink sink, int slot, byte[] buf, int off, int len, RoutedRequest<?> r, boolean queued) {
        ClusterNode node = slot < 0 ? anyNode() : owner(slot);
        if (replicaReads && slot >= 0 && r.read) {
            if (queued) {
                r.replicaOk = true;
            } else {
                ClusterNode replica = readNode(node);
                if (replica != null) {
                    r.replica = replica;
                    node = replica;
                }
            }
        }
        try {
            sink.send(node, buf, off, len, r);
        } catch (RuvioConnectionException e) {
            sendAfterReload(sink, slot, buf, off, len, r, e);
        }
    }

    /**
     * The node could not be opened again: reload the slot map and send the command, which nothing was written
     * for, to the owner the new map names. A keyless command goes to the first node of the new map that opens.
     */
    private void sendAfterReload(Sink sink, int slot, byte[] buf, int off, int len, RoutedRequest<?> r, RuvioConnectionException error) {
        r.replica = null;
        if (closed) {
            throw error;
        }
        try {
            refresh();
        } catch (RuntimeException e) {
            throw error;
        }
        ClusterNode target = null;
        if (slot >= 0) {
            target = owner(slot);
        } else {
            for (ClusterNode n : masters()) {
                try {
                    n.link.get();
                    seed = n;
                    target = n;
                    break;
                } catch (RuvioConnectionException ignored) {
                    // try the next node of the new map
                }
            }
            if (target == null) {
                throw error;
            }
        }
        sink.send(target, buf, off, len, r);
    }


    /**
     * Computes the slot shared by the keys of the parsed frame.
     *
     * @param f the parsed frame
     * @return the slot, or -1 when the command has no key
     * @throws RuvioServerException {@code CROSSSLOT} when the keys do not share a slot
     */
    public static int commonSlot(Frame f) {
        f.parse();
        byte[] b = f.out.buf;
        ClusterKeys.Spec spec = ClusterKeys.spec(b, f.off[0], f.len[0]);
        int n = ClusterKeys.keys(spec, f);
        if (n == 0) {
            return -1;
        }
        int[] k = f.keyIdx;
        int slot = HashSlot.of(b, f.off[k[0]], f.len[k[0]]);
        for (int i = 1; i < n; i++) {
            if (HashSlot.of(b, f.off[k[i]], f.len[k[i]]) != slot) {
                throw crossSlotError();
            }
        }
        return slot;
    }

    /**
     * Builds the {@code CROSSSLOT} error the client raises itself.
     *
     * @return the exception
     */
    public static RuvioServerException crossSlotError() {
        return new RuvioServerException("CROSSSLOT Keys in request do not hash to the same slot (checked by the client, nothing was sent)");
    }

    private void crossSlot(ClusterKeys.Spec spec, Frame f, int n, RoutedRequest<?> r, Sink sink, boolean queued) {
        if (spec.split == ClusterKeys.NO_SPLIT) {
            r.failFinal(crossSlotError());
            return;
        }
        split(spec, f, n, r, sink, queued);
    }

    /**
     * MGET/MSET/DEL/UNLINK/EXISTS/TOUCH over several slots: one sub-command per slot (the server
     * requires one slot per command, even inside one shard), each sent to its owner. The sub-commands
     * run concurrently and the replies are re-assembled in the caller's order.
     */
    private void split(ClusterKeys.Spec spec, Frame f, int n, RoutedRequest<?> r, Sink sink, boolean queued) {
        boolean pairs = spec.pairs;
        if (pairs && (f.argc & 1) == 0) {
            r.failFinal(new RuvioServerException("ERR wrong number of arguments for '" + spec.name.toLowerCase() + "' command"));
            return;
        }
        byte[] b = f.out.buf;
        int[] k = Arrays.copyOf(f.keyIdx, n);
        Map<Integer, Integer> groupOf = new HashMap<>();
        int[] keyGroup = new int[n];
        int[] groupSlot = new int[n];
        int[] sizes = new int[n];
        int groups = 0;
        for (int i = 0; i < n; i++) {
            int slot = HashSlot.of(b, f.off[k[i]], f.len[k[i]]);
            Integer g = groupOf.get(slot);
            if (g == null) {
                g = groups++;
                groupOf.put(slot, g);
                groupSlot[g] = slot;
            }
            keyGroup[i] = g;
            sizes[g]++;
        }
        int[][] members = new int[groups][];
        for (int g = 0; g < groups; g++) {
            members[g] = new int[sizes[g]];
            sizes[g] = 0;
        }
        for (int i = 0; i < n; i++) {
            int g = keyGroup[i];
            members[g][sizes[g]++] = i;
        }
        SplitParent parent = new SplitParent(r, spec.split, n, members);
        RespOut tmp = new RespOut(256);
        for (int g = 0; g < groups; g++) {
            int[] m = members[g];
            tmp.reset();
            tmp.header(1 + m.length * (pairs ? 2 : 1));
            tmp.bulk(b, f.off[0], f.len[0]);
            for (int j : m) {
                int idx = k[j];
                tmp.bulk(b, f.off[idx], f.len[idx]);
                if (pairs) {
                    tmp.bulk(b, f.off[idx + 1], f.len[idx + 1]);
                }
            }
            SplitPart part = new SplitPart(parent, g, r);
            part.router = this;
            part.read = r.read;
            part.setFrame(Arrays.copyOf(tmp.buf, tmp.pos), 0, tmp.pos);
            try {
                route(sink, groupSlot[g], part.frame, 0, part.frameLen, part, queued);
            } catch (RuntimeException e) {
                parent.fail(e);
                return;
            }
        }
    }

    /** Collects the replies of the sub-commands of a split command and completes the caller's request. */
    public static final class SplitParent {
        private final RoutedRequest<?> target;
        private final int mode;
        private final int keys;
        private final int[][] members;
        private final Object[] results;
        private final AtomicInteger remaining;
        private final AtomicBoolean failed = new AtomicBoolean();

        SplitParent(RoutedRequest<?> target, int mode, int keys, int[][] members) {
            this.target = target;
            this.mode = mode;
            this.keys = keys;
            this.members = members;
            this.results = new Object[members.length];
            this.remaining = new AtomicInteger(members.length);
        }

        void part(int group, Object value) {
            results[group] = value;
            if (remaining.decrementAndGet() == 0 && !failed.get()) {
                try {
                    target.deliver(assemble());
                } catch (RuntimeException e) {
                    fail(e);
                }
            }
        }

        private Object assemble() {
            switch (mode) {
                case ClusterKeys.SPLIT_SUM: {
                    long sum = 0;
                    for (Object v : results) {
                        if (v instanceof Long) {
                            sum += (Long) v;
                        }
                    }
                    return sum;
                }
                case ClusterKeys.SPLIT_LIST: {
                    Object[] all = new Object[keys];
                    for (int g = 0; g < members.length; g++) {
                        List<?> part = (List<?>) results[g];
                        int[] m = members[g];
                        if (part == null || part.size() != m.length) {
                            throw new RuvioException("cluster split: unexpected reply size from a node");
                        }
                        for (int j = 0; j < m.length; j++) {
                            all[m[j]] = part.get(j);
                        }
                    }
                    return new ArrayList<>(Arrays.asList(all));
                }
                default:
                    return null;
            }
        }

        void fail(Throwable error) {
            if (failed.compareAndSet(false, true)) {
                target.failFinal(error);
            }
        }
    }

    /** One sub-command of a split command; it can follow a MOVED redirect like any routed request. */
    public static final class SplitPart extends RoutedRequest<Object> {
        private final SplitParent parent;
        private final int group;

        @SuppressWarnings("unchecked")
        SplitPart(SplitParent parent, int group, Request<?> like) {
            super((Reply<Object>) like.reply, like.keys, like.values);
            this.parent = parent;
            this.group = group;
        }

        @Override
        public void deliver(Object value) {
            parent.part(group, value);
        }

        @Override
        public void failFinal(Throwable error) {
            parent.fail(error);
        }
    }

    // ---- replica reads ---------------------------------------------------------------------

    /**
     * Returns the replica that serves the reads of a primary, with its connection open.
     *
     * @param primary the primary
     * @return the replica; null when the read should go to the primary (none known or reachable under
     *         {@code PREFER_REPLICA})
     * @throws RuvioException under {@code REPLICA}, when there is no replica to use ("no replica ..."), or
     *         the connection error when it cannot be opened
     */
    public ClusterNode readNode(ClusterNode primary) {
        ClusterNode replica = primary.reader;
        if (replica != null) {
            try {
                replica.link.get();
                return replica;
            } catch (RuvioException e) {
                // skipped until the next slot map load
                if (primary.reader == replica) {
                    primary.reader = null;
                }
                if (options.readPolicy == ReadPolicy.REPLICA) {
                    throw e;
                }
                return null;
            }
        }
        if (options.readPolicy == ReadPolicy.REPLICA) {
            throw new RuvioException("no replica is known for " + primary + " (read policy REPLICA)");
        }
        return null;
    }

    /**
     * Tells whether an error is a {@code LOADING} reply: the replica is still copying its primary.
     *
     * @param error the failure
     * @return {@code true} for a server error starting with {@code LOADING}
     */
    public static boolean isLoading(Throwable error) {
        if (!(error instanceof RuvioServerException)) {
            return false;
        }
        String m = error.getMessage();
        return m != null && (m.startsWith("LOADING") || m.startsWith("MASTERDOWN"));
    }

    /**
     * A replica answered {@code LOADING}: skip it until the next slot map load and, under {@code PREFER_REPLICA},
     * send the command to the primary instead. Under {@code REPLICA} the error is the command's result.
     *
     * @param r the request to send again
     * @param replica the replica that answered
     * @param loading the {@code LOADING} error
     */
    public void readAgain(RoutedRequest<?> r, ClusterNode replica, Throwable loading) {
        ClusterNode primary = replica.primary;
        if (primary != null && primary.reader == replica) {
            primary.reader = null;
        }
        r.replica = null;
        if (primary == null || options.readPolicy == ReadPolicy.REPLICA) {
            r.failFinal(loading);
            return;
        }
        try {
            BACKGROUND.execute(() -> {
                try {
                    DIRECT.send(primary, r.frame, r.frameOff, r.frameLen, r);
                } catch (RuntimeException e) {
                    r.failFinal(e);
                }
            });
        } catch (RejectedExecutionException e) {
            r.failFinal(loading);
        }
    }

    // ---- MOVED -----------------------------------------------------------------------------

    /**
     * Tells whether an error is a {@code MOVED} redirect.
     *
     * @param error the failure
     * @return {@code true} for a server error starting with {@code MOVED}
     */
    public static boolean isMoved(Throwable error) {
        if (!(error instanceof RuvioServerException)) {
            return false;
        }
        String m = error.getMessage();
        return m != null && m.startsWith("MOVED ");
    }

    /**
     * Follows a MOVED reply: patch/refresh the table, then send the saved frame to the new owner.
     *
     * @param r the request to re-send
     * @param moved the {@code MOVED} error
     */
    public void redirect(RoutedRequest<?> r, Throwable moved) {
        try {
            BACKGROUND.execute(() -> followMoved(r, moved));
        } catch (RejectedExecutionException e) {
            r.failFinal(moved);
        }
    }

    /**
     * Tells whether an error is an {@code ASK} redirect.
     *
     * @param error the failure
     * @return {@code true} for a server error starting with {@code ASK}
     */
    public static boolean isAsk(Throwable error) {
        if (!(error instanceof RuvioServerException)) {
            return false;
        }
        String m = error.getMessage();
        return m != null && m.startsWith("ASK ");
    }

    /**
     * Tells whether an error is a {@code TRYAGAIN} reply: nothing ran, the slot is being handed over.
     *
     * @param error the failure
     * @return {@code true} for a server error starting with {@code TRYAGAIN}
     */
    public static boolean isTryAgain(Throwable error) {
        if (!(error instanceof RuvioServerException)) {
            return false;
        }
        String m = error.getMessage();
        return m != null && m.startsWith("TRYAGAIN");
    }

    /**
     * Sends a command again after {@code TRYAGAIN}, routed by the current slot table, once the wait for this
     * retry has passed. The wait is scheduled, never slept, so a reader thread is not held.
     *
     * @param r the request to send again ({@code r.retries} already counts this retry)
     * @param tryAgain the {@code TRYAGAIN} error
     */
    public void retryLater(RoutedRequest<?> r, Throwable tryAgain) {
        try {
            CompletableFuture.delayedExecutor(TRYAGAIN_WAIT_MS << (r.retries - 1), TimeUnit.MILLISECONDS, BACKGROUND)
                    .execute(() -> resend(r, tryAgain));
        } catch (RejectedExecutionException e) {
            r.failFinal(tryAgain);
        }
    }

    private void resend(RoutedRequest<?> r, Throwable tryAgain) {
        try {
            r.replica = null;
            route(DIRECT, slotOf(r.frame, r.frameOff, r.frameLen), r.frame, r.frameOff, r.frameLen, r, false);
        } catch (RuntimeException e) {
            r.failFinal(e);
        }
    }

    /** The slot of an encoded single-slot command, or -1 when it has no key. */
    private static int slotOf(byte[] buf, int off, int len) {
        Frame f = new Frame();
        f.out.buf = Arrays.copyOfRange(buf, off, off + len);
        f.out.pos = len;
        f.parse();
        byte[] b = f.out.buf;
        ClusterKeys.Spec spec = ClusterKeys.spec(b, f.off[0], f.len[0]);
        if (spec.first) {
            return f.argc >= 2 ? HashSlot.of(b, f.off[1], f.len[1]) : -1;
        }
        int n = ClusterKeys.keys(spec, f);
        return n > 0 ? HashSlot.of(b, f.off[f.keyIdx[0]], f.len[f.keyIdx[0]]) : -1;
    }

    /** Stands in the reply queue for the {@code +OK} of {@code ASKING}; an error fails the command it precedes. */
    private static final class AskingRequest extends Request<Void> {
        private final RoutedRequest<?> command;

        AskingRequest(RoutedRequest<?> command) {
            super(Reply.OK, null, null);
            this.command = command;
        }

        @Override
        public void deliver(Object value) {
            // the +OK is discarded
        }

        @Override
        public void fail(Throwable error) {
            command.failFinal(error);
        }
    }

    private void followMoved(RoutedRequest<?> r, Throwable error) {
        try {
            String[] parts = error.getMessage().split(" ");
            if (parts.length < 3) {
                r.failFinal(error);
                return;
            }
            int slot = Integer.parseInt(parts[1]);
            String addr = parts[2];
            int colon = addr.lastIndexOf(':');
            String host = colon > 0 ? addr.substring(0, colon) : seed.host;
            int port = Integer.parseInt(addr.substring(colon + 1));
            r.replica = null;
            if (error.getMessage().startsWith("ASK ")) {
                // One-off: the slot table stays as it is.
                ClusterNode target = addNode(host, port);
                Connection c = target.link.get();
                RespOut o = c.begin();
                try {
                    o.raw(ASKING);
                    o.raw(r.frame, r.frameOff, r.frameLen);
                } catch (RuntimeException e) {
                    c.abort();
                    throw e;
                }
                c.endMany(new Request<?>[] {new AskingRequest(r), r}, 2);
                return;
            }
            ClusterNode target = onMoved(slot, host, port, r.redirects > 1);
            DIRECT.send(target, r.frame, r.frameOff, r.frameLen, r);
        } catch (RuntimeException e) {
            r.failFinal(e instanceof NumberFormatException ? error : e);
        }
    }

    /**
     * Points the slot at the named node and returns that node, which the retry always goes to. Re-reads the
     * whole table first when that node is unknown or {@code again} (the command's second MOVED or later).
     */
    private ClusterNode onMoved(int slot, String host, int port, boolean again) {
        ClusterNode n = nodes.get(ClusterNode.address(host, port));
        boolean unknown = n == null;
        if (unknown) {
            n = addNode(host, port);
        }
        if (slot >= 0 && slot < SLOTS) {
            table[slot] = n;
        }
        if (unknown || again) {
            try {
                refresh();
            } catch (RuntimeException ignored) {
                // keep the patched slot; the next MOVED tries again
            }
        }
        return n;
    }

    /**
     * Test hook: overwrites the owner of a slot, for example to simulate a stale table.
     *
     * @param slot the slot
     * @param node the new owner
     */
    public void forceSlot(int slot, ClusterNode node) {
        table[slot] = node;
    }

    @Override
    public void close() {
        closed = true;
        for (ClusterNode n : nodes.values()) {
            n.close();
        }
        for (ClusterNode n : replicas.values()) {
            n.close();
        }
    }

    /**
     * Tells whether the router was closed.
     *
     * @return {@code true} after {@link #close()}
     */
    public boolean isClosed() {
        return closed;
    }
}
