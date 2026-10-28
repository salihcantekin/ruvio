// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import io.github.salihcantekin.ruvio.RuvioCodec;

import java.util.concurrent.CompletableFuture;

/**
 * One pending reply. The future itself is the only per-command allocation of a typed call; the
 * decoder is a shared singleton that reads the reply straight from the connection's buffer.
 *
 * @param <T> the result type
 */
public class Request<T> extends CompletableFuture<T> {
    /** The decoder for this command's reply. */
    public final Reply<T> reply;
    /** The key codec used to decode keys in the reply. */
    public final RuvioCodec<?> keys;
    /** The value codec used to decode values in the reply. */
    public final RuvioCodec<?> values;
    /** When the connection's deadline check fails this request ({@code System.nanoTime} based); 0 means never. */
    long deadline;

    /**
     * Creates a request.
     *
     * @param reply the reply decoder
     * @param keys the key codec
     * @param values the value codec
     */
    public Request(Reply<T> reply, RuvioCodec<?> keys, RuvioCodec<?> values) {
        this.reply = reply;
        this.keys = keys;
        this.values = values;
    }

    /**
     * Stamps the deadline: the command timeout plus this command's own wait, none for one that waits forever.
     *
     * @param timeoutMillis the command timeout, greater than 0
     */
    final void arm(long timeoutMillis) {
        long ms = Blocking.timeoutFor(timeoutMillis, this);
        if (ms <= 0) {
            return;
        }
        long d = System.nanoTime() + Math.min(ms, 1_000_000_000_000L) * 1_000_000L;
        deadline = d == 0 ? 1 : d;
    }

    /**
     * Decodes this request's reply at the reader's position. May throw {@link RespReader#INCOMPLETE}.
     *
     * @param r the reader
     * @return the decoded value
     */
    public T decode(RespReader r) {
        return reply.decode(r, this);
    }

    /**
     * Completes with a decoded value; overridden by requests that route their result elsewhere.
     *
     * @param value the decoded value
     */
    @SuppressWarnings("unchecked")
    public void deliver(Object value) {
        complete((T) value);
    }

    /**
     * Completes exceptionally.
     *
     * @param error the failure
     */
    public void fail(Throwable error) {
        completeExceptionally(error);
    }
}
