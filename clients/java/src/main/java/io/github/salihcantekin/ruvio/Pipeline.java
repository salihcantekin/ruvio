// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import io.github.salihcantekin.ruvio.commands.RuvioCommands;
import io.github.salihcantekin.ruvio.internal.ClusterNode;
import io.github.salihcantekin.ruvio.internal.ClusterRouter;
import io.github.salihcantekin.ruvio.internal.Connection;
import io.github.salihcantekin.ruvio.internal.Frame;
import io.github.salihcantekin.ruvio.internal.Link;
import io.github.salihcantekin.ruvio.internal.Request;
import io.github.salihcantekin.ruvio.internal.RespOut;
import io.github.salihcantekin.ruvio.internal.RoutedRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

/**
 * Explicit pipeline: typed commands are encoded into a private buffer and their futures complete
 * only after {@link #flush()} sends the whole batch in one write. Not thread safe; reuse after
 * flush is allowed. Do not wait on a pipeline future before flushing it.
 *
 * <p>All typed command methods (inherited, documented on the command families) only queue the
 * command and return a future. A pipeline is not a transaction: other callers' commands may run
 * between yours and one failing command does not stop the rest. In a cluster the commands are
 * grouped per node, one write each, so replies from different nodes are not ordered against each
 * other. A batch larger than {@code maxInFlight} fails with a {@link RuvioException}.
 *
 * <pre>{@code
 * Pipeline<String, String> pipe = client.pipeline();
 * CompletableFuture<Boolean> a = pipe.set("a", "1");
 * CompletableFuture<Long> n = pipe.incr("counter");
 * pipe.sync();                       // sends everything and waits
 * long counter = n.join();
 * }</pre>
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class Pipeline<K, V> extends RuvioCommands<K, V> {
    private final Link link;
    private final RespOut out = new RespOut(4096);
    private Request<?>[] pending = new Request<?>[16];
    private Request<?>[] sent = new Request<?>[16];
    private int count;
    private int mark;

    // Cluster mode: commands are routed when queued and grouped per node; each group is one write.
    private final ClusterRouter router;
    private final Frame frame;
    private Group[] byNode = new Group[4];
    private Group[] active = new Group[4];
    private int activeCount;
    private final ClusterRouter.Sink grouping;
    /** Under a replica read policy: every command queued since the last flush is a read. */
    private boolean allReads = true;

    Pipeline(Link link, ClusterRouter router, RuvioCodec<K> keys, RuvioCodec<V> values) {
        super(keys, values);
        this.link = link;
        this.router = router;
        if (router != null) {
            this.routed = true;
            this.frame = new Frame(true);
            this.grouping = this::group;
        } else {
            this.frame = null;
            this.grouping = null;
        }
    }

    @Override
    protected RespOut begin() {
        if (router != null) {
            frame.reset();
            return frame.out;
        }
        mark = out.pos;
        return out;
    }

    @Override
    protected void abort(RespOut o) {
        if (router == null) {
            o.pos = mark;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T> CompletableFuture<T> end(RespOut o, Request<T> request) {
        if (count == pending.length) {
            pending = Arrays.copyOf(pending, count << 1);
        }
        if (router != null) {
            RoutedRequest<T> routed = (RoutedRequest<T>) request;
            if (router.replicaReads()) {
                // Reads wait in the primary's group; the flush moves them to replicas if nothing in the batch writes.
                router.dispatchQueued(frame, routed, grouping);
                allReads &= routed.read;
            } else {
                router.dispatch(frame, routed, grouping);
            }
        }
        pending[count++] = request;
        return request;
    }

    private void group(ClusterNode node, byte[] buf, int off, int len, RoutedRequest<?> request) {
        int idx = node.index;
        if (idx >= byNode.length) {
            byNode = Arrays.copyOf(byNode, Math.max(idx + 1, byNode.length << 1));
        }
        Group g = byNode[idx];
        if (g == null) {
            g = new Group(node);
            byNode[idx] = g;
        } else if (g.node != node) {
            g = other(node);
        }
        if (g.n == 0) {
            if (activeCount == active.length) {
                active = Arrays.copyOf(active, activeCount << 1);
            }
            active[activeCount++] = g;
        }
        g.add(buf, off, len, request);
    }

    /** A node whose index clashes with another node's (nodes not created by the router): found by scanning. */
    private Group other(ClusterNode node) {
        for (int i = 0; i < activeCount; i++) {
            if (active[i].node == node) {
                return active[i];
            }
        }
        return new Group(node);
    }

    /** The commands of one flush that go to the same node: their frames and requests, in queue order. */
    private static final class Group {
        final ClusterNode node;
        final RespOut out = new RespOut(1024);
        Request<?>[] requests = new Request<?>[16];
        int n;

        Group(ClusterNode node) {
            this.node = node;
        }

        void add(byte[] buf, int off, int len, Request<?> r) {
            if (n == requests.length) {
                requests = Arrays.copyOf(requests, n << 1);
            }
            requests[n++] = r;
            out.raw(buf, off, len);
        }

        void reset() {
            Arrays.fill(requests, 0, n, null);
            n = 0;
            out.reset();
            if (out.buf.length > (1 << 20)) {
                out.buf = new byte[1024];
            }
        }
    }

    /**
     * Returns the number of commands buffered and not yet flushed.
     *
     * @return the queued command count
     */
    public int size() {
        return count;
    }

    /**
     * Sends every buffered command in one write (one write per node in a cluster). The futures complete
     * as the replies arrive; this call does not wait for them.
     *
     * @return the number of commands sent; 0 when nothing was queued
     * @throws RuvioConnectionException when the socket cannot be used; the affected futures fail with the
     *         same exception
     * @throws RuvioException when the batch is larger than {@code maxInFlight}
     */
    public int flush() {
        if (count == 0) {
            return 0;
        }
        if (router != null) {
            return flushCluster();
        }
        Connection c = link.get();
        RespOut o = c.begin();
        try {
            o.raw(out.buf, 0, out.pos);
        } catch (RuntimeException e) {
            c.abort();
            throw e;
        }
        int n = count;
        try {
            c.endMany(pending, n);
        } catch (RuntimeException e) {
            for (int i = 0; i < n; i++) {
                pending[i].fail(e);
            }
            throw e;
        } finally {
            Arrays.fill(pending, 0, n, null);
            count = 0;
            out.reset();
            if (out.buf.length > (1 << 20)) {
                out.buf = new byte[4096];
            }
        }
        return n;
    }

    private int flushCluster() {
        int n = count;
        RuntimeException first = null;
        boolean toReplicas = allReads && router.replicaReads();
        allReads = true;
        for (int gi = 0; gi < activeCount; gi++) {
            Group g = active[gi];
            try {
                if (!toReplicas || !sendToReplica(g)) {
                    write(g.node, g.out.buf, g.out.pos, g.requests, g.n);
                }
            } catch (RuntimeException e) {
                for (int i = 0; i < g.n; i++) {
                    g.requests[i].fail(e);
                }
                if (first == null) {
                    first = e;
                }
            } finally {
                g.reset();
            }
        }
        Arrays.fill(active, 0, activeCount, null);
        activeCount = 0;
        Arrays.fill(pending, 0, n, null);
        count = 0;
        if (first != null) {
            throw first;
        }
        return n;
    }

    private static void write(ClusterNode node, byte[] buf, int len, Request<?>[] requests, int n) {
        Connection c = node.link.get();
        RespOut o = c.begin();
        try {
            o.raw(buf, 0, len);
        } catch (RuntimeException e) {
            c.abort();
            throw e;
        }
        c.endMany(requests, n);
    }

    /**
     * Sends the keyed reads of a group to the primary's replica, the rest (keyless commands) to the primary.
     *
     * @return {@code false} when the group has no replica to use under {@code PREFER_REPLICA}: nothing was sent
     */
    private boolean sendToReplica(Group g) {
        int reads = 0;
        for (int i = 0; i < g.n; i++) {
            if (((RoutedRequest<?>) g.requests[i]).replicaOk) {
                reads++;
            }
        }
        if (reads == 0) {
            return false;
        }
        ClusterNode replica = router.readNode(g.node);
        if (replica == null) {
            return false;
        }
        if (reads == g.n) {
            for (int i = 0; i < g.n; i++) {
                ((RoutedRequest<?>) g.requests[i]).replica = replica;
            }
            write(replica, g.out.buf, g.out.pos, g.requests, g.n);
            return true;
        }
        RespOut onReplica = new RespOut(1024);
        RespOut onPrimary = new RespOut(1024);
        Request<?>[] toReplica = new Request<?>[reads];
        Request<?>[] toPrimary = new Request<?>[g.n - reads];
        int r = 0;
        int p = 0;
        for (int i = 0; i < g.n; i++) {
            RoutedRequest<?> q = (RoutedRequest<?>) g.requests[i];
            if (q.replicaOk) {
                q.replica = replica;
                onReplica.raw(q.frame, q.frameOff, q.frameLen);
                toReplica[r++] = q;
            } else {
                onPrimary.raw(q.frame, q.frameOff, q.frameLen);
                toPrimary[p++] = q;
            }
        }
        write(g.node, onPrimary.buf, onPrimary.pos, toPrimary, p);
        write(replica, onReplica.buf, onReplica.pos, toReplica, r);
        return true;
    }

    /**
     * Flushes and waits for every reply. This method does not throw for a failed command; read the
     * outcome from each command's own future ({@code join()} throws the error).
     *
     * @throws RuvioConnectionException when the flush itself fails
     * @throws RuvioException when the batch is larger than {@code maxInFlight}
     */
    public void sync() {
        int n = count;
        if (sent.length < n) {
            sent = new Request<?>[Math.max(n, sent.length << 1)];
        }
        System.arraycopy(pending, 0, sent, 0, n);
        try {
            flush();
        } catch (RuntimeException e) {
            Arrays.fill(sent, 0, n, null);
            throw e;
        }
        for (int i = 0; i < n; i++) {
            try {
                sent[i].join();
            } catch (RuntimeException ignored) {
                // reported through the command's own future
            }
            sent[i] = null;
        }
    }
}
