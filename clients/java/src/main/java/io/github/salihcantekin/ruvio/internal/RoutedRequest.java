// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import io.github.salihcantekin.ruvio.RuvioCodec;

/**
 * A request sent by the cluster router. It keeps a copy of the encoded command so that a
 * {@code -MOVED} or {@code -ASK} reply can be followed (together at most {@link ClusterRouter#MAX_REDIRECTS} times) and a
 * {@code -TRYAGAIN} reply retried (at most {@link ClusterRouter#MAX_TRYAGAIN} times); any other failure
 * (including a lost connection) completes it as is: nothing is ever replayed after a transport failure.
 *
 * @param <T> the result type
 */
public class RoutedRequest<T> extends Request<T> {
    /** The router that sent this request. */
    public ClusterRouter router;
    /**
     * The command as sent, kept for MOVED retries: {@code frame[frameOff, frameOff + frameLen)}. Null for parents of split commands.
     */
    public byte[] frame;
    /** Start of the command in {@link #frame}. */
    public int frameOff;
    /** Length of the command in {@link #frame}. */
    public int frameLen;
    /** How many MOVED or ASK redirects this command has followed so far. */
    public byte redirects;
    /** How many times this command was sent again after a {@code TRYAGAIN} reply. */
    public byte retries;
    /** Set under a replica read policy: the command is a read (flagged readonly, not blocking). */
    public boolean read;
    /** Set while a pipeline holds the command: a keyed read, which may go to a replica when the whole pipeline reads. */
    public boolean replicaOk;
    /** The replica this command was sent to, or null; a {@code LOADING} reply from it sends the command on to the primary. */
    public ClusterNode replica;

    /**
     * Creates a request.
     *
     * @param reply the reply decoder
     * @param keys the key codec
     * @param values the value codec
     */
    public RoutedRequest(Reply<T> reply, RuvioCodec<?> keys, RuvioCodec<?> values) {
        super(reply, keys, values);
    }

    /**
     * Remembers the encoded command for redirects.
     *
     * @param buf the array holding the command
     * @param off where the command starts
     * @param len the command length
     */
    public void setFrame(byte[] buf, int off, int len) {
        frame = buf;
        frameOff = off;
        frameLen = len;
    }

    /**
     * Sends the command to the primary after a replica answered {@code LOADING}, follows a {@code -MOVED} or {@code -ASK}
     * reply or retries a {@code -TRYAGAIN} reply when the budgets allow, otherwise completes exceptionally.
     *
     * @param error the failure
     */
    @Override
    public void fail(Throwable error) {
        ClusterRouter r = router;
        ClusterNode from = replica;
        if (from != null && r != null && frame != null && ClusterRouter.isLoading(error)) {
            r.readAgain(this, from, error);
            return;
        }
        if (r != null && frame != null) {
            if (redirects < ClusterRouter.MAX_REDIRECTS && (ClusterRouter.isMoved(error) || ClusterRouter.isAsk(error))) {
                redirects++;
                r.redirect(this, error);
                return;
            }
            if (retries < ClusterRouter.MAX_TRYAGAIN && ClusterRouter.isTryAgain(error)) {
                retries++;
                r.retryLater(this, error);
                return;
            }
        }
        failFinal(error);
    }

    /**
     * Completes exceptionally without looking at redirects.
     *
     * @param error the failure
     */
    public void failFinal(Throwable error) {
        frame = null;
        completeExceptionally(error);
    }
}
