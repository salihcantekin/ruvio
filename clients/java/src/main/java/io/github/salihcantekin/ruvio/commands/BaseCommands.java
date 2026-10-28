// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.commands;

import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.RuvioException;
import io.github.salihcantekin.ruvio.RuvioServerException;
import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Blocking;
import io.github.salihcantekin.ruvio.internal.Reply;
import io.github.salihcantekin.ruvio.internal.Request;
import io.github.salihcantekin.ruvio.internal.RespOut;
import io.github.salihcantekin.ruvio.internal.RoutedRequest;

import java.util.concurrent.CompletableFuture;

/**
 * Shared base of every command family. It holds the key and value codecs and the low-level encoders; the typed commands live in the subclasses ({@code StringCommands}, {@code HashCommands} and so on, chained up to {@link RuvioCommands}).
 *
 * <p>The same command methods exist on {@link io.github.salihcantekin.ruvio.RuvioAsync} (each returns a {@link CompletableFuture}), on {@link io.github.salihcantekin.ruvio.Pipeline} and {@link io.github.salihcantekin.ruvio.Transaction} (queued, answered on flush or exec), and, as blocking calls, on {@link io.github.salihcantekin.ruvio.RuvioClient}. The Javadoc of a method describes the value it produces; a future completes with that value, and an error reply completes it exceptionally with a {@link RuvioServerException}.
 *
 * @param <K> key type (also hash fields and stream field names)
 * @param <V> value type (also set and sorted-set members, list elements and stream field values)
 */
public abstract class BaseCommands<K, V> {
    /** The codec that encodes keys (and hash fields) and decodes keys in replies. */
    protected final RuvioCodec<K> kc;
    /** The codec that encodes values (and members, elements) and decodes values in replies. */
    protected final RuvioCodec<V> vc;

    /**
     * Creates a command family with the given codecs.
     *
     * @param keys the key codec
     * @param values the value codec
     */
    protected BaseCommands(RuvioCodec<K> keys, RuvioCodec<V> values) {
        this.kc = keys;
        this.vc = values;
    }

    /**
     * The codec used for keys and hash fields.
     *
     * @return the key codec
     */
    public final RuvioCodec<K> keyCodec() {
        return kc;
    }

    /**
     * The codec used for values, members and elements.
     *
     * @return the value codec
     */
    public final RuvioCodec<V> valueCodec() {
        return vc;
    }

    /**
     * Returns the buffer for one frame (locked for the shared connection). Internal: part of the extension point used by the connection, pipeline and transaction front-ends.
     *
     * @return the output buffer to encode into
     */
    protected abstract RespOut begin();

    /**
     * Rolls back a frame started with {@link #begin()} after an encoding failure. Internal.
     *
     * @param out the buffer returned by {@code begin}
     */
    protected abstract void abort(RespOut out);

    /**
     * Registers the request for the frame just written into {@code out} and releases the buffer. Internal.
     *
     * @param out the buffer returned by {@code begin}
     * @param request the pending reply
     * @param <T> the reply type
     * @return the future that completes with the decoded reply
     */
    protected abstract <T> CompletableFuture<T> end(RespOut out, Request<T> request);

    /** True when frames are routed by key (cluster mode); requests then carry what a redirect needs. */
    protected boolean routed;

    final <T> Request<T> req(Reply<T> reply) {
        if (routed) {
            return new RoutedRequest<>(reply, kc, vc);
        }
        return new Request<>(reply, kc, vc);
    }

    @SuppressWarnings("unchecked")
    static <T> Reply<T> r(Reply<?> reply) {
        return (Reply<T>) reply;
    }

    final Args args() {
        return new Args(kc, vc);
    }

    final RuntimeException fail(RespOut o, Throwable t) {
        abort(o);
        if (t instanceof RuntimeException) {
            return (RuntimeException) t;
        }
        if (t instanceof Error) {
            throw (Error) t;
        }
        return new RuvioException(t.toString(), t);
    }

    private static void check(Object o, String what) {
        if (o == null) {
            throw new IllegalArgumentException("null " + what);
        }
    }

    // ---- fixed shapes ----------------------------------------------------------------------

    /** CMD */
    final <T> CompletableFuture<T> c0(byte[] cmd, Reply<T> reply) {
        RespOut o = begin();
        o.header(1);
        o.raw(cmd);
        return end(o, req(reply));
    }

    /** CMD SUB */
    final <T> CompletableFuture<T> c00(byte[] cmd, byte[] sub, Reply<T> reply) {
        RespOut o = begin();
        o.header(2);
        o.raw(cmd);
        o.raw(sub);
        return end(o, req(reply));
    }

    /** CMD key */
    final <T> CompletableFuture<T> k(byte[] cmd, K key, Reply<T> reply) {
        check(key, "key");
        RespOut o = begin();
        try {
            o.header(2);
            o.raw(cmd);
            kc.write(o, key);
        } catch (Throwable t) {
            throw fail(o, t);
        }
        return end(o, req(reply));
    }

    /** CMD key value */
    final <T> CompletableFuture<T> kv(byte[] cmd, K key, V value, Reply<T> reply) {
        check(key, "key");
        check(value, "value");
        RespOut o = begin();
        try {
            o.header(3);
            o.raw(cmd);
            kc.write(o, key);
            vc.write(o, value);
        } catch (Throwable t) {
            throw fail(o, t);
        }
        return end(o, req(reply));
    }

    /** CMD key long */
    final <T> CompletableFuture<T> kl(byte[] cmd, K key, long n, Reply<T> reply) {
        check(key, "key");
        RespOut o = begin();
        try {
            o.header(3);
            o.raw(cmd);
            kc.write(o, key);
            o.bulk(n);
        } catch (Throwable t) {
            throw fail(o, t);
        }
        return end(o, req(reply));
    }

    /** CMD key long long */
    final <T> CompletableFuture<T> kll(byte[] cmd, K key, long a, long b, Reply<T> reply) {
        check(key, "key");
        RespOut o = begin();
        try {
            o.header(4);
            o.raw(cmd);
            kc.write(o, key);
            o.bulk(a);
            o.bulk(b);
        } catch (Throwable t) {
            throw fail(o, t);
        }
        return end(o, req(reply));
    }

    /** CMD key key */
    final <T> CompletableFuture<T> kk(byte[] cmd, K a, K b, Reply<T> reply) {
        check(a, "key");
        check(b, "key");
        RespOut o = begin();
        try {
            o.header(3);
            o.raw(cmd);
            kc.write(o, a);
            kc.write(o, b);
        } catch (Throwable t) {
            throw fail(o, t);
        }
        return end(o, req(reply));
    }

    /** CMD key field (field encoded with the key codec) */
    final <T> CompletableFuture<T> kf(byte[] cmd, K key, K field, Reply<T> reply) {
        return kk(cmd, key, field, reply);
    }

    /** CMD key field value */
    final <T> CompletableFuture<T> kfv(byte[] cmd, K key, K field, V value, Reply<T> reply) {
        check(key, "key");
        check(field, "field");
        check(value, "value");
        RespOut o = begin();
        try {
            o.header(4);
            o.raw(cmd);
            kc.write(o, key);
            kc.write(o, field);
            vc.write(o, value);
        } catch (Throwable t) {
            throw fail(o, t);
        }
        return end(o, req(reply));
    }

    /** CMD key long value */
    final <T> CompletableFuture<T> klv(byte[] cmd, K key, long n, V value, Reply<T> reply) {
        check(key, "key");
        check(value, "value");
        RespOut o = begin();
        try {
            o.header(4);
            o.raw(cmd);
            kc.write(o, key);
            o.bulk(n);
            vc.write(o, value);
        } catch (Throwable t) {
            throw fail(o, t);
        }
        return end(o, req(reply));
    }

    /** CMD key string */
    final <T> CompletableFuture<T> ks(byte[] cmd, K key, String s, Reply<T> reply) {
        check(key, "key");
        check(s, "argument");
        RespOut o = begin();
        try {
            o.header(3);
            o.raw(cmd);
            kc.write(o, key);
            o.bulk(s);
        } catch (Throwable t) {
            throw fail(o, t);
        }
        return end(o, req(reply));
    }

    /** CMD key... */
    @SafeVarargs
    final <T> CompletableFuture<T> keys(byte[] cmd, Reply<T> reply, K... keys) {
        if (keys.length == 0) {
            throw new IllegalArgumentException("at least one key is required");
        }
        for (K key : keys) {
            check(key, "key");
        }
        RespOut o = begin();
        try {
            o.header(1 + keys.length);
            o.raw(cmd);
            for (K key : keys) {
                kc.write(o, key);
            }
        } catch (Throwable t) {
            throw fail(o, t);
        }
        return end(o, req(reply));
    }

    /** CMD key value... */
    @SafeVarargs
    final <T> CompletableFuture<T> kvs(byte[] cmd, K key, Reply<T> reply, V... values) {
        check(key, "key");
        if (values.length == 0) {
            throw new IllegalArgumentException("at least one value is required");
        }
        for (V v : values) {
            check(v, "value");
        }
        RespOut o = begin();
        try {
            o.header(2 + values.length);
            o.raw(cmd);
            kc.write(o, key);
            for (V v : values) {
                vc.write(o, v);
            }
        } catch (Throwable t) {
            throw fail(o, t);
        }
        return end(o, req(reply));
    }

    /** CMD key field... (fields with the key codec) */
    @SafeVarargs
    final <T> CompletableFuture<T> kfs(byte[] cmd, K key, Reply<T> reply, K... fields) {
        check(key, "key");
        if (fields.length == 0) {
            throw new IllegalArgumentException("at least one field is required");
        }
        for (K f : fields) {
            check(f, "field");
        }
        RespOut o = begin();
        try {
            o.header(2 + fields.length);
            o.raw(cmd);
            kc.write(o, key);
            for (K f : fields) {
                kc.write(o, f);
            }
        } catch (Throwable t) {
            throw fail(o, t);
        }
        return end(o, req(reply));
    }

    /** Sends a frame built with {@link Args}. */
    final <T> CompletableFuture<T> send(Args a, Reply<T> reply) {
        // A blocking command's request carries its wait, which the synchronous facade adds to
        // commandTimeoutMillis; every other command gets the plain request.
        long wait = Blocking.waitMillis(a.out.buf, a.out.pos, a.count);
        Request<T> request = wait == 0 ? req(reply) : Blocking.request(routed, reply, kc, vc, wait);
        RespOut o = begin();
        try {
            o.header(a.count);
            o.raw(a.out.buf, 0, a.out.pos);
        } catch (Throwable t) {
            throw fail(o, t);
        }
        return end(o, request);
    }

    /**
     * Sends any command by name, for commands this client has no typed method for. Arguments may be {@code byte[]}, {@code String}, {@code Long}/{@code Integer}, {@code Double}, or anything with a useful {@code toString()}. In a cluster the key is found from the command table, so the command goes to the right node.
     *
     * <p>The reply is the generic tree: bulk strings are {@code byte[]}, status replies {@code String}, integers {@code Long}, arrays {@code List}, {@code nil} is {@code null}, and an error inside an array is a {@link RuvioServerException} element. A top-level error reply completes the future exceptionally.
     *
     * <pre>{@code
     * Object reply = client.execute("LPOS", "mylist", "x", "RANK", 2);
     * }</pre>
     *
     * @param arguments the command name followed by its arguments
     * @return the reply tree
     * @throws IllegalArgumentException when no argument is given
     * @throws RuvioServerException for a top-level error reply
     */
    public final CompletableFuture<Object> execute(Object... arguments) {
        if (arguments.length == 0) {
            throw new IllegalArgumentException("empty command");
        }
        Args a = args();
        for (Object x : arguments) {
            a.any(x);
        }
        return send(a, Reply.RAW);
    }

    /**
     * Like {@link #execute(Object...)} but bulk strings in the reply are decoded as UTF-8 {@code String}s instead of {@code byte[]}.
     *
     * @param arguments the command name followed by its arguments
     * @return the reply tree with text decoded
     * @throws IllegalArgumentException when no argument is given
     * @throws RuvioServerException for a top-level error reply
     */
    public final CompletableFuture<Object> executeText(Object... arguments) {
        if (arguments.length == 0) {
            throw new IllegalArgumentException("empty command");
        }
        Args a = args();
        for (Object x : arguments) {
            a.any(x);
        }
        return send(a, Reply.TEXT);
    }
}
