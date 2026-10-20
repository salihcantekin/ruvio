// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import io.github.salihcantekin.ruvio.commands.RuvioCommands;
import io.github.salihcantekin.ruvio.internal.ClusterRouter;
import io.github.salihcantekin.ruvio.internal.Link;
import io.github.salihcantekin.ruvio.internal.Request;
import io.github.salihcantekin.ruvio.internal.RespOut;
import io.github.salihcantekin.ruvio.internal.RoutedRequest;

import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous commands on the shared multiplexed connection. Thread safe; every method returns
 * immediately with a future completed by the reader thread in reply order. Get one from
 * {@link RuvioClient#async()}. Every command method is documented on the command family classes this
 * class inherits from (strings, keys, hashes, lists, sets, sorted sets, geo, streams, JSON, scripting
 * and Ruvio-specific commands).
 *
 * <p>Futures complete on the connection's reader thread: callbacks attached with
 * {@code thenApply}/{@code thenAccept} run there and must not block (use the {@code *Async}
 * variants with an executor for slow work).
 *
 * <p>Errors complete the future exceptionally: {@link RuvioServerException} for a server error reply,
 * {@link RuvioConnectionException} when the socket fails, {@link RuvioException} for a timeout.
 *
 * <pre>{@code
 * RuvioAsync<String, String> async = client.async();
 * async.set("k", "v").thenCompose(ok -> async.get("k")).thenAccept(System.out::println);
 * }</pre>
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class RuvioAsync<K, V> extends RuvioCommands<K, V> {
    /** The shared connection (the seed node's in cluster mode). */
    final Link link;
    /** Non-null in cluster mode: every command is routed by its key; {@code link} is then the seed's. */
    final ClusterRouter router;
    /** A standalone link that follows a failover: commands keep their encoded frame so a refused one can be sent again. */
    private final boolean follow;

    RuvioAsync(Link link, RuvioCodec<K> keys, RuvioCodec<V> values) {
        this(link, null, keys, values);
    }

    RuvioAsync(Link link, ClusterRouter router, RuvioCodec<K> keys, RuvioCodec<V> values) {
        super(keys, values);
        this.link = link;
        this.router = router;
        this.follow = router == null && link.follows();
        this.routed = router != null || follow;
    }

    @Override
    protected RespOut begin() {
        ClusterRouter r = router;
        if (r != null) {
            return r.begin();
        }
        return follow ? link.begin() : link.get().begin();
    }

    @Override
    protected void abort(RespOut out) {
        if (router == null && !follow) {
            out.owner.abort();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T> CompletableFuture<T> end(RespOut out, Request<T> request) {
        ClusterRouter r = router;
        if (follow) {
            link.send(out.frame, (RoutedRequest<T>) request);
        } else if (r == null) {
            out.owner.end(request);
        } else {
            r.dispatch(out.frame, (RoutedRequest<T>) request, ClusterRouter.DIRECT);
        }
        return request;
    }

    /**
     * Returns a view on the same connection that uses other codecs, for example String keys with
     * {@code byte[]} values.
     *
     * <pre>{@code
     * RuvioAsync<String, byte[]> raw = async.withCodecs(RuvioCodec.UTF8, RuvioCodec.BYTES);
     * }</pre>
     *
     * @param keys the codec for keys
     * @param values the codec for values
     * @param <K2> the new key type
     * @param <V2> the new value type
     * @return a new async facade; the connection, cluster routing and pipelines are shared
     */
    public <K2, V2> RuvioAsync<K2, V2> withCodecs(RuvioCodec<K2> keys, RuvioCodec<V2> values) {
        return new RuvioAsync<>(link, router, keys, values);
    }

    /**
     * Sends {@code SELECT db} on the shared connection. The client remembers the index and selects it
     * again after a reconnect.
     *
     * @param database the index, from 0 to {@code databases - 1} (16 by default)
     * @return a future that completes when the server accepted the switch
     */
    @Override
    public CompletableFuture<Void> select(int database) {
        CompletableFuture<Void> f = super.select(database);
        link.database = database;
        return f;
    }

    /**
     * Starts an explicit pipeline: commands are buffered locally and sent in one write by
     * {@link Pipeline#flush()}.
     *
     * @return a new pipeline (not thread safe) that uses this facade's codecs
     */
    public Pipeline<K, V> pipeline() {
        return new Pipeline<>(link, router, kc, vc);
    }
}
