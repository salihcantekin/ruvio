// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.commands;

import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.RuvioException;
import io.github.salihcantekin.ruvio.RuvioServerException;
import io.github.salihcantekin.ruvio.args.SetOptions;
import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Cmd;
import io.github.salihcantekin.ruvio.internal.Reply;
import io.github.salihcantekin.ruvio.model.FillResult;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * String commands, including the Ruvio extensions SETV, TAKE, DELEX, GETEX, FILLGET/FILLSET.
 *
 * @param <K> key type (also hash fields and stream field names)
 * @param <V> value type (also members, list elements and stream field values)
 */
public abstract class StringCommands<K, V> extends BaseCommands<K, V> {
    /**
     * Creates the command family with the given codecs.
     *
     * @param keys the key codec
     * @param values the value codec
     */
    protected StringCommands(RuvioCodec<K> keys, RuvioCodec<V> values) {
        super(keys, values);
    }

    static final Reply<FillResult<Object>> FILL = Reply.of((tree, q) -> {
        List<Object> items = Reply.list(tree);
        String state = Reply.str(items.get(0));
        switch (state) {
            case "HIT":
                return new FillResult<>(FillResult.State.HIT, Reply.decodeWith(q.values, items.get(1)), null);
            case "FILL":
                return new FillResult<>(FillResult.State.FILL, null, Reply.str(items.get(1)));
            case "BUSY":
                return new FillResult<>(FillResult.State.BUSY, null, null);
            case "STALE":
                return new FillResult<>(FillResult.State.STALE, Reply.decodeWith(q.values, items.get(1)),
                        items.size() > 2 ? Reply.str(items.get(2)) : null);
            default:
                throw new RuvioException("unexpected FILLGET state " + state);
        }
    });

    /**
     * Sends {@code GET key}: reads a string value.
     *
     * <pre>{@code
     * String name = client.get("user:1:name"); // null when missing
     * }</pre>
     *
     * @param key the key to read
     * @return the value, or {@code null} when the key does not exist or has expired
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a list, set, hash, JSON or other non-string value
     */
    public CompletableFuture<V> get(K key) {
        return k(Cmd.GET, key, r(Reply.VALUE));
    }

    /**
     * Sends {@code SET key value}: stores a string, replacing any existing value of any type. An existing
     * TTL is cleared; use {@link #set(Object, Object, SetOptions)} with {@code keepTtl()} to keep it.
     *
     * <pre>{@code
     * client.set("user:1:name", "Ada");
     * }</pre>
     *
     * @param key the key to write
     * @param value the value to store
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> set(K key, V value) {
        return kv(Cmd.SET, key, value, Reply.OK);
    }

    /**
     * Sends {@code SET key value [EX|PX|EXAT|PXAT|KEEPTTL] [NX|XX]}: a string write with an optional
     * expiry and an optional condition.
     *
     * <pre>{@code
     * // 30 second session token, only if nobody else created it
     * boolean created = client.set("session:42", token, SetOptions.ex(30).ifAbsent());
     * }</pre>
     *
     * @param key the key to write
     * @param value the value to store
     * @param options expiry and condition, built with {@link SetOptions}; an empty options object is a plain {@code SET}
     * @return {@code true} when the value was written, {@code false} when {@code NX} (key already exists) or
     * {@code XX} (key missing) prevented the write
     * @throws RuvioServerException {@code ERR} for an expiry of zero or less, or a syntax error
     */
    public CompletableFuture<Boolean> set(K key, V value, SetOptions options) {
        Args a = args().w(Cmd.SET).k(key).v(value);
        options.write(a);
        return send(a, Reply.OK_BOOL);
    }

    /**
     * Sends {@code SET key value GET}: stores a string and returns the one it replaced. Any TTL is cleared.
     *
     * <pre>{@code
     * String old = client.setGet("config:mode", "fast");
     * }</pre>
     *
     * @param key the key to write
     * @param value the new value
     * @return the previous value, or {@code null} when the key did not exist
     * @throws RuvioServerException {@code WRONGTYPE} when the existing value is not a string (nothing is written)
     */
    public CompletableFuture<V> setGet(K key, V value) {
        return send(args().w(Cmd.SET).k(key).v(value).w(Cmd.GET), r(Reply.VALUE));
    }

    /**
     * Sends {@code SET key value [options] GET}: like {@link #setGet(Object, Object)} with an expiry and/or
     * a condition.
     *
     * @param key the key to write
     * @param value the new value
     * @param options expiry and condition, built with {@link SetOptions}
     * @return the previous value, or {@code null} when the key did not exist
     * @throws RuvioServerException {@code WRONGTYPE} when the existing value is not a string
     */
    public CompletableFuture<V> setGet(K key, V value, SetOptions options) {
        Args a = args().w(Cmd.SET).k(key).v(value);
        options.write(a);
        return send(a.w(Cmd.GET), r(Reply.VALUE));
    }

    /**
     * Sends the Ruvio command {@code SET key value IFEQ expected}: a compare-and-set on a string. The
     * write happens only when the current value is byte-for-byte equal to {@code expected}. Any TTL is cleared.
     *
     * <pre>{@code
     * boolean swapped = client.setIfEq("lock:report", "owner-b", "owner-a");
     * }</pre>
     *
     * @param key the key to write
     * @param value the new value
     * @param expected the value the key must currently hold
     * @return {@code true} when the value was written, {@code false} when the key is missing or holds a different value
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-string
     */
    public CompletableFuture<Boolean> setIfEq(K key, V value, V expected) {
        return send(args().w(Cmd.SET).k(key).v(value).w(Cmd.IFEQ).v(expected), Reply.OK_BOOL);
    }

    /**
     * Sends {@code SET key value IFEQ expected [EX|PX|EXAT|PXAT|KEEPTTL]}: like
     * {@link #setIfEq(Object, Object, Object)} with an expiry for the new value.
     *
     * @param key the key to write
     * @param value the new value
     * @param expected the value the key must currently hold
     * @param expiry the expiry to apply; only its expiry part is sent, any {@code NX}/{@code XX} condition is ignored
     * @return {@code true} when the value was written, {@code false} when the key is missing or holds a different value
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-string
     */
    public CompletableFuture<Boolean> setIfEq(K key, V value, V expected, SetOptions expiry) {
        Args a = args().w(Cmd.SET).k(key).v(value).w(Cmd.IFEQ).v(expected);
        expiry.writeExpiry(a);
        return send(a, Reply.OK_BOOL);
    }

    /**
     * Sends the Ruvio command {@code SET key value IFGT [EX|PX|EXAT|PXAT|KEEPTTL]}: writes when the key is missing or {@code value} is greater than the stored integer (a watermark, a version, the highest bid).
     * Any TTL is cleared; use the overload with {@code SetOptions} to set one.
     *
     * <pre>{@code
     * boolean advanced = client.setIfGreater("watermark", 1_700_000_000L);
     * }</pre>
     *
     * @param key the key to write
     * @param value the new number
     * @return {@code true} when the value was written, {@code false} when the stored number is greater or equal
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-string; an error when the stored string is not an integer
     */
    public CompletableFuture<Boolean> setIfGreater(K key, long value) {
        return send(args().w(Cmd.SET).k(key).l(value).w(Cmd.IFGT), Reply.OK_BOOL);
    }

    /**
     * Sends the Ruvio command {@code SET key value IFGT [EX|PX|EXAT|PXAT|KEEPTTL]}: writes when the key is missing or {@code value} is greater than the stored integer (a watermark, a version, the highest bid).
     *
     * @param key the key to write
     * @param value the new number
     * @param expiry the expiry to apply
     * @return {@code true} when written, {@code false} when refused
     */
    public CompletableFuture<Boolean> setIfGreater(K key, long value, SetOptions expiry) {
        Args a = args().w(Cmd.SET).k(key).l(value).w(Cmd.IFGT);
        expiry.writeExpiry(a);
        return send(a, Reply.OK_BOOL);
    }

    /**
     * Sends the Ruvio command {@code SET key value IFLT [EX|PX|EXAT|PXAT|KEEPTTL]}: writes when the key is missing or {@code value} is smaller than the stored integer (the lowest price, the earliest deadline).
     * Any TTL is cleared; use the overload with {@code SetOptions} to set one.
     *
     * <pre>{@code
     * boolean lowered = client.setIfLess("best:price", 4_99L);
     * }</pre>
     *
     * @param key the key to write
     * @param value the new number
     * @return {@code true} when the value was written, {@code false} when the stored number is smaller or equal
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-string; an error when the stored string is not an integer
     */
    public CompletableFuture<Boolean> setIfLess(K key, long value) {
        return send(args().w(Cmd.SET).k(key).l(value).w(Cmd.IFLT), Reply.OK_BOOL);
    }

    /**
     * Sends the Ruvio command {@code SET key value IFLT [EX|PX|EXAT|PXAT|KEEPTTL]}: writes when the key is missing or {@code value} is smaller than the stored integer (the lowest price, the earliest deadline).
     *
     * @param key the key to write
     * @param value the new number
     * @param expiry the expiry to apply
     * @return {@code true} when written, {@code false} when refused
     */
    public CompletableFuture<Boolean> setIfLess(K key, long value, SetOptions expiry) {
        Args a = args().w(Cmd.SET).k(key).l(value).w(Cmd.IFLT);
        expiry.writeExpiry(a);
        return send(a, Reply.OK_BOOL);
    }

    /**
     * Sends the Ruvio command {@code SETV key value}: starts a versioned string and returns its version.
     * It works on a missing key or a plain string (the key becomes versioned, version 1). Once a key is
     * versioned, later writes must use {@link #setv(Object, Object, long)} with the current version.
     *
     * <pre>{@code
     * long v = client.setv("doc:7", "draft"); // 1
     * }</pre>
     *
     * @param key the key to write
     * @param value the value to store
     * @return the new version, always {@code 1}
     * @throws RuvioServerException {@code ERR version mismatch} when the key is already versioned; {@code WRONGTYPE} when it holds a non-string
     */
    public CompletableFuture<Long> setv(K key, V value) {
        return kv(Cmd.SETV, key, value, Reply.LONG);
    }

    /**
     * Sends {@code SETV key value version}: an optimistic-concurrency write on a versioned string. It
     * succeeds only when {@code version} equals the key's current version, which then increases by one.
     * {@code TYPE} still reports {@code string}, and {@code INCR} refuses a versioned string.
     *
     * <pre>{@code
     * long v = client.setv("doc:7", "draft");        // 1
     * long v2 = client.setv("doc:7", "final", v);    // 2
     * }</pre>
     *
     * @param key the key to write
     * @param value the new value
     * @param version the version the caller last saw (returned by an earlier {@code setv})
     * @return the new version number
     * @throws RuvioServerException {@code ERR version mismatch} when the version differs or the key is missing (nothing changes); {@code WRONGTYPE} for a plain string or other type
     */
    public CompletableFuture<Long> setv(K key, V value, long version) {
        return send(args().w(Cmd.SETV).k(key).v(value).l(version), Reply.LONG);
    }

    /**
     * Sends the Ruvio command {@code TAKE key count}: atomically subtracts {@code count} from an integer
     * string, but only when the key holds at least that much. Use it for stock, credits or tokens.
     *
     * <pre>{@code
     * client.set("stock:sku1", "5");
     * Long left = client.take("stock:sku1", 2);  // 3
     * Long none = client.take("stock:sku1", 10); // null, stock stays 3
     * }</pre>
     *
     * @param key the counter key (a decimal integer string)
     * @param count the amount to take; must be greater than zero
     * @return the remainder after subtracting, or {@code null} when the key is missing or holds less than
     * {@code count} (nothing changes)
     * @throws RuvioServerException {@code ERR} when {@code count} is not positive or the value is not an integer; {@code WRONGTYPE} for a non-string
     */
    public CompletableFuture<Long> take(K key, long count) {
        return kl(Cmd.TAKE, key, count, Reply.LONG);
    }

    /**
     * Sends the Ruvio command {@code TRANSFER source destination amount}: moves {@code amount} from the
     * integer at {@code source} to the integer at {@code destination} in one step, only when {@code source}
     * holds at least that much. A missing {@code destination} starts at 0. Both writes are one WAL record.
     *
     * <pre>{@code
     * client.set("{u}:wallet", "50");
     * List<Long> after = client.transfer("{u}:wallet", "{u}:escrow", 30); // [20, 30]
     * List<Long> none = client.transfer("{u}:wallet", "{u}:escrow", 30);  // null, nothing changed
     * }</pre>
     *
     * @param source the key to subtract from (a decimal integer string)
     * @param destination the key to add to; in a cluster it must share a slot with {@code source}
     * (use a hash tag such as {@code {user:42}})
     * @param amount the amount to move; must be greater than zero
     * @return {@code [sourceAfter, destinationAfter]}, or {@code null} when {@code source} is missing or
     * short, or {@code destination} would overflow (nothing is written)
     * @throws RuvioServerException {@code ERR} when {@code amount} is not positive, the keys are the same or a
     * value is not an integer; {@code WRONGTYPE} for a non-string; {@code CROSSSLOT} when the keys are in different slots
     */
    public CompletableFuture<List<Long>> transfer(K source, K destination, long amount) {
        return send(args().w(Cmd.TRANSFER).k(source).k(destination).l(amount), Reply.LONG_PAIR);
    }

    /**
     * Sends the Ruvio command {@code TRANSFER source destination amount MAX max}: {@link #transfer(Object, Object, long)}
     * that also refuses when {@code destination} would end above {@code max}.
     *
     * @param source the key to subtract from (a decimal integer string)
     * @param destination the key to add to; in a cluster it must share a slot with {@code source}
     * @param amount the amount to move; must be greater than zero
     * @param max the highest value {@code destination} may reach
     * @return {@code [sourceAfter, destinationAfter]}, or {@code null} when nothing was moved
     * @throws RuvioServerException {@code ERR} when {@code amount} is not positive, the keys are the same or a
     * value is not an integer; {@code WRONGTYPE} for a non-string; {@code CROSSSLOT} when the keys are in different slots
     */
    public CompletableFuture<List<Long>> transfer(K source, K destination, long amount, long max) {
        return send(args().w(Cmd.TRANSFER).k(source).k(destination).l(amount).w(Cmd.MAX).l(max), Reply.LONG_PAIR);
    }

    /**
     * Sends the Ruvio command {@code DELEX key IFEQ expected}: deletes a string only when it equals
     * {@code expected}. The usual safe way to release a token-based lock.
     *
     * <pre>{@code
     * boolean released = client.delex("lock:report", myToken);
     * }</pre>
     *
     * @param key the key to delete
     * @param expected the value the key must currently hold
     * @return {@code true} when the key was deleted, {@code false} when it is missing or holds another value
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-string
     */
    public CompletableFuture<Boolean> delex(K key, V expected) {
        return send(args().w(Cmd.DELEX).k(key).w(Cmd.IFEQ).v(expected), Reply.BOOL);
    }

    /**
     * Sends the Ruvio command {@code GETEX key ttl_ms}: reads a string and sets its time to live in one step.
     *
     * <pre>{@code
     * String session = client.getex("session:42", 30_000); // slide the expiry to 30 s from now
     * }</pre>
     *
     * @param key the key to read
     * @param ttlMillis the new TTL in milliseconds; must be greater than zero
     * @return the value, or {@code null} when the key does not exist (no TTL is set then)
     * @throws RuvioServerException {@code ERR} when {@code ttlMillis} is not positive; {@code WRONGTYPE} for a non-string
     */
    public CompletableFuture<V> getex(K key, long ttlMillis) {
        return kl(Cmd.GETEX, key, ttlMillis, r(Reply.VALUE));
    }

    /**
     * Sends {@code MGET key [key ...]}: reads several strings at once.
     * In a cluster, keys in different slots are sent per slot and the replies merged in key order; the
     * read is then not a single atomic snapshot.
     *
     * <pre>{@code
     * List<String> values = client.mget("a", "b", "c");
     * }</pre>
     *
     * @param keys one or more keys
     * @return one element per key in the same order; an element is {@code null} when that key is missing or
     * does not hold a string (no {@code WRONGTYPE} error is raised)
     * @throws IllegalArgumentException when no key is given
     */
    @SafeVarargs
    public final CompletableFuture<List<V>> mget(K... keys) {
        return keys(Cmd.MGET, r(Reply.VALUE_LIST), keys);
    }

    /**
     * Sends {@code MSET key value [key value ...]}: writes several strings. Existing values of any type and
     * their TTLs are replaced.
     * In a cluster, pairs whose keys fall in different slots are sent per slot; that split is not atomic.
     *
     * <pre>{@code
     * client.mset(Map.of("a", "1", "b", "2"));
     * }</pre>
     *
     * @param pairs the keys and values to write; iteration order is the send order
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> mset(Map<K, V> pairs) {
        Args a = args().w(Cmd.MSET);
        for (Map.Entry<K, V> e : pairs.entrySet()) {
            a.k(e.getKey()).v(e.getValue());
        }
        return send(a, Reply.OK);
    }

    /**
     * Sends {@code MSETNX key value [key value ...]}: writes all pairs only when none of the keys exists.
     * All or nothing. In a cluster every key must hash to the same slot (use a {@code {tag}}), otherwise the
     * client fails with {@code CROSSSLOT} before sending.
     *
     * @param pairs the keys and values to write
     * @return {@code true} when every key was set, {@code false} (nothing written) when at least one key already existed
     * @throws RuvioServerException {@code CROSSSLOT} in a cluster when the keys span slots
     */
    public CompletableFuture<Boolean> msetnx(Map<K, V> pairs) {
        Args a = args().w(Cmd.MSETNX);
        for (Map.Entry<K, V> e : pairs.entrySet()) {
            a.k(e.getKey()).v(e.getValue());
        }
        return send(a, Reply.BOOL);
    }

    /**
     * Sends {@code GETSET key value}: stores a string and returns the previous one. The TTL is cleared.
     * Prefer {@link #setGet(Object, Object)}.
     *
     * @param key the key to write
     * @param value the new value
     * @return the previous value, or {@code null} when the key did not exist
     * @throws RuvioServerException {@code WRONGTYPE} when the existing value is not a string
     */
    public CompletableFuture<V> getset(K key, V value) {
        return kv(Cmd.GETSET, key, value, r(Reply.VALUE));
    }

    /**
     * Sends {@code APPEND key value}: adds bytes to the end of a string, creating it when missing. An
     * existing TTL is kept.
     *
     * @param key the key to extend
     * @param value the bytes to append
     * @return the length of the string in bytes after the append
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-string
     */
    public CompletableFuture<Long> append(K key, V value) {
        return kv(Cmd.APPEND, key, value, Reply.LONG);
    }

    /**
     * Sends {@code STRLEN key}.
     *
     * @param key the key to measure
     * @return the length of the string in bytes, or {@code 0} when the key does not exist
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-string
     */
    public CompletableFuture<Long> strlen(K key) {
        return k(Cmd.STRLEN, key, Reply.LONG);
    }

    /**
     * Sends {@code INCR key}: adds one to a decimal integer string. A missing key counts as 0; an existing TTL is kept.
     *
     * <pre>{@code
     * long hits = client.incr("page:hits");
     * }</pre>
     *
     * @param key the counter key
     * @return the value after the increment
     * @throws RuvioServerException {@code ERR} when the value is not a signed 64-bit integer or the result overflows (value unchanged); {@code WRONGTYPE} for a non-string
     */
    public CompletableFuture<Long> incr(K key) {
        return k(Cmd.INCR, key, Reply.LONG);
    }

    /**
     * Sends {@code INCR key EX|PX ttl}: like {@link #incr(Object)}, and a counter this call creates gets a time to live.
     * An existing key keeps its deadline.
     *
     * <pre>{{@code
     * long hits = client.incr("page:hits", Duration.ofMinutes(1));
     * }}</pre>
     *
     * @param key the counter key
     * @param ttl time to live of a newly created key: whole seconds are sent as {@code EX}, anything finer as {@code PX}
     * @return the value after the increment
     * @throws IllegalArgumentException when {@code ttl} is null, zero, negative or too large
     * @throws RuvioServerException {@code ERR} when the value is not a signed 64-bit integer or the result overflows; {@code WRONGTYPE} for a non-string
     */
    public CompletableFuture<Long> incr(K key, Duration ttl) {
        return send(args().w(Cmd.INCR).k(key).ttl(ttl), Reply.LONG);
    }

    /**
     * Sends {@code DECR key}: subtracts one from a decimal integer string. A missing key counts as 0; an existing TTL is kept.
     *
     * @param key the counter key
     * @return the value after the decrement
     * @throws RuvioServerException {@code ERR} when the value is not a signed 64-bit integer or the result overflows; {@code WRONGTYPE} for a non-string
     */
    public CompletableFuture<Long> decr(K key) {
        return k(Cmd.DECR, key, Reply.LONG);
    }

    /**
     * Sends {@code DECR key EX|PX ttl}: like {@link #decr(Object)}, and a counter this call creates gets a time to live.
     * An existing key keeps its deadline.
     *
     * @param key the counter key
     * @param ttl time to live of a newly created key: whole seconds are sent as {@code EX}, anything finer as {@code PX}
     * @return the value after the decrement
     * @throws IllegalArgumentException when {@code ttl} is null, zero, negative or too large
     * @throws RuvioServerException {@code ERR} when the value is not a signed 64-bit integer or the result overflows; {@code WRONGTYPE} for a non-string
     */
    public CompletableFuture<Long> decr(K key, Duration ttl) {
        return send(args().w(Cmd.DECR).k(key).ttl(ttl), Reply.LONG);
    }

    /**
     * Sends {@code INCRBY key delta}: adds {@code delta} (may be negative) to a decimal integer string. A
     * missing key counts as 0; an existing TTL is kept.
     *
     * @param key the counter key
     * @param delta the signed amount to add
     * @return the value after the addition
     * @throws RuvioServerException {@code ERR} when the value is not a signed 64-bit integer or the result overflows; {@code WRONGTYPE} for a non-string
     */
    public CompletableFuture<Long> incrby(K key, long delta) {
        return kl(Cmd.INCRBY, key, delta, Reply.LONG);
    }

    /**
     * Sends {@code INCRBY key delta EX|PX ttl}: like {@link #incrby(Object, long)}, and a counter this call creates gets a time
     * to live. An existing key keeps its deadline.
     *
     * @param key the counter key
     * @param delta the signed amount to add
     * @param ttl time to live of a newly created key: whole seconds are sent as {@code EX}, anything finer as {@code PX}
     * @return the value after the addition
     * @throws IllegalArgumentException when {@code ttl} is null, zero, negative or too large
     * @throws RuvioServerException {@code ERR} when the value is not a signed 64-bit integer or the result overflows; {@code WRONGTYPE} for a non-string
     */
    public CompletableFuture<Long> incrby(K key, long delta, Duration ttl) {
        return send(args().w(Cmd.INCRBY).k(key).l(delta).ttl(ttl), Reply.LONG);
    }

    /**
     * Sends the Ruvio command {@code INCRBY key delta MAX limit}: adds {@code delta} only when the result
     * stays at or below {@code limit}. Handy for quotas and bounded counters. A missing key counts as 0.
     *
     * <pre>{@code
     * Long used = client.incrbyMax("quota:user1", 1, 100); // null once 100 is reached
     * }</pre>
     *
     * @param key the counter key
     * @param delta the signed amount to add
     * @param limit the highest value the counter may reach
     * @return the value after the addition, or {@code null} when the result would pass {@code limit}
     * (nothing is written)
     * @throws RuvioServerException {@code ERR} when the value is not a signed 64-bit integer; {@code WRONGTYPE} for a non-string
     */
    public CompletableFuture<Long> incrbyMax(K key, long delta, long limit) {
        return send(args().w(Cmd.INCRBY).k(key).l(delta).w(Cmd.MAX).l(limit), Reply.LONG);
    }

    /**
     * Sends {@code INCRBY key delta EX|PX ttl MAX limit}: like {@link #incrbyMax(Object, long, long)}, and a counter this call
     * creates gets a time to live (a fixed-window quota in one command). An existing key keeps its deadline.
     *
     * <pre>{{@code
     * Long used = client.incrbyMax("quota:user1", 1, 100, Duration.ofMinutes(1)); // null once 100 is reached
     * }}</pre>
     *
     * @param key the counter key
     * @param delta the signed amount to add
     * @param limit the highest value the counter may reach
     * @param ttl time to live of a newly created key: whole seconds are sent as {@code EX}, anything finer as {@code PX}
     * @return the value after the addition, or {@code null} when the result would pass {@code limit} (nothing is written)
     * @throws IllegalArgumentException when {@code ttl} is null, zero, negative or too large
     * @throws RuvioServerException {@code ERR} when the value is not a signed 64-bit integer; {@code WRONGTYPE} for a non-string
     */
    public CompletableFuture<Long> incrbyMax(K key, long delta, long limit, Duration ttl) {
        return send(args().w(Cmd.INCRBY).k(key).l(delta).ttl(ttl).w(Cmd.MAX).l(limit), Reply.LONG);
    }

    /**
     * Sends {@code DECRBY key delta}: subtracts {@code delta} from a decimal integer string. A missing key counts as 0; an existing TTL is kept.
     *
     * @param key the counter key
     * @param delta the amount to subtract (a negative value adds)
     * @return the value after the subtraction
     * @throws RuvioServerException {@code ERR} when the value is not a signed 64-bit integer or the result overflows; {@code WRONGTYPE} for a non-string
     */
    public CompletableFuture<Long> decrby(K key, long delta) {
        return kl(Cmd.DECRBY, key, delta, Reply.LONG);
    }

    /**
     * Sends {@code DECRBY key delta EX|PX ttl}: like {@link #decrby(Object, long)}, and a counter this call creates gets a time
     * to live. An existing key keeps its deadline.
     *
     * @param key the counter key
     * @param delta the amount to subtract (a negative value adds)
     * @param ttl time to live of a newly created key: whole seconds are sent as {@code EX}, anything finer as {@code PX}
     * @return the value after the subtraction
     * @throws IllegalArgumentException when {@code ttl} is null, zero, negative or too large
     * @throws RuvioServerException {@code ERR} when the value is not a signed 64-bit integer or the result overflows; {@code WRONGTYPE} for a non-string
     */
    public CompletableFuture<Long> decrby(K key, long delta, Duration ttl) {
        return send(args().w(Cmd.DECRBY).k(key).l(delta).ttl(ttl), Reply.LONG);
    }

    /**
     * Sends {@code INCRBYFLOAT key delta}: adds a floating-point number to a numeric string. A missing key
     * counts as 0. Ruvio computes in double precision and stores 16 significant digits (Redis uses long double).
     *
     * @param key the key to change
     * @param delta the amount to add (may be negative)
     * @return the value after the addition
     * @throws RuvioServerException {@code ERR} when the value is not a number or the result is NaN or infinite; {@code WRONGTYPE} for a non-string
     */
    public CompletableFuture<Double> incrbyfloat(K key, double delta) {
        return send(args().w(Cmd.INCRBYFLOAT).k(key).d(delta), Reply.DOUBLE);
    }

    /**
     * Sends {@code INCRBYFLOAT key delta EX|PX ttl}: like {@link #incrbyfloat(Object, double)}, and a key this call creates gets a
     * time to live. An existing key keeps its deadline.
     *
     * @param key the key to change
     * @param delta the amount to add (may be negative)
     * @param ttl time to live of a newly created key: whole seconds are sent as {@code EX}, anything finer as {@code PX}
     * @return the value after the addition
     * @throws IllegalArgumentException when {@code ttl} is null, zero, negative or too large
     * @throws RuvioServerException {@code ERR} when the value is not a number or the result is NaN or infinite; {@code WRONGTYPE} for a non-string
     */
    public CompletableFuture<Double> incrbyfloat(K key, double delta, Duration ttl) {
        return send(args().w(Cmd.INCRBYFLOAT).k(key).d(delta).ttl(ttl), Reply.DOUBLE);
    }

    /**
     * Sends the Ruvio command {@code FILLGET key fill_ms}: cache read with stampede protection. When the
     * key is missing, exactly one caller is told to build the value; everyone else sees {@code BUSY}.
     * Answers by {@link FillResult#state()}:
     * <ul>
     * <li>{@code HIT}: the value is present and fresh, in {@link FillResult#value()}.</li>
     * <li>{@code FILL}: this caller owns the fill; compute the value and call {@code fillset} with
     * {@link FillResult#token()} before {@code fillMillis} pass.</li>
     * <li>{@code BUSY}: another caller is filling; retry shortly.</li>
     * <li>{@code STALE}: inside a stale-while-revalidate window the old value is returned; a non-null token means
     * this caller should refresh it.</li>
     * </ul>
     * The fill marker lives in server memory only and lapses after {@code fillMillis}, after which the next
     * caller becomes the filler. {@link io.github.salihcantekin.ruvio.RuvioClient#getOrCreate} wraps this whole loop.
     *
     * <pre>{@code
     * FillResult<String> r = client.fillget("page:home", 5_000);
     * if (r.state() == FillResult.State.FILL) {
     *     client.fillset("page:home", render(), 60_000, r.token());
     * }
     * }</pre>
     *
     * @param key the cache key
     * @param fillMillis how long a filler may take, in milliseconds
     * @return the state with value and/or token
     * @throws RuvioServerException {@code WRONGTYPE} for a non-string; {@code ERR too many pending fills} when the server's marker table (65,536) is full
     */
    public CompletableFuture<FillResult<V>> fillget(K key, long fillMillis) {
        return kl(Cmd.FILLGET, key, fillMillis, r(FILL));
    }

    /**
     * Sends {@code FILLGET key fill_ms WAIT wait_ms}: like {@link #fillget(Object, long)}, but a caller that
     * would get {@code BUSY} is held on the server until the filler writes, the marker lapses (the waiter then
     * becomes the filler) or {@code waitMillis} pass (then {@code BUSY}). While it waits, the whole shared
     * multiplexed connection is parked; call it on {@link io.github.salihcantekin.ruvio.RuvioClient#dedicated()}
     * or use client-side polling. Refused inside a transaction.
     *
     * @param key the cache key
     * @param fillMillis how long a filler may take, in milliseconds
     * @param waitMillis the longest a busy caller waits, in milliseconds
     * @return the state with value and/or token
     * @throws RuvioServerException {@code WRONGTYPE} for a non-string
     */
    public CompletableFuture<FillResult<V>> fillgetWait(K key, long fillMillis, long waitMillis) {
        return send(args().w(Cmd.FILLGET).k(key).l(fillMillis).w(Cmd.WAIT).l(waitMillis), r(FILL));
    }

    /**
     * Sends the Ruvio command {@code FILLSET key value ttl_ms token}: stores a value built after
     * {@code FILLGET} answered {@code FILL}.
     *
     * @param key the cache key
     * @param value the value to cache
     * @param ttlMillis how long the value lives, in milliseconds
     * @param token the token {@code fillget} returned
     * @return {@code true} when the token was still live and the value was written, {@code false} when it
     * was not (the marker lapsed or another caller took over); nothing is written then
     * @throws RuvioServerException {@code WRONGTYPE} for a non-string
     */
    public CompletableFuture<Boolean> fillset(K key, V value, long ttlMillis, String token) {
        return send(args().w(Cmd.FILLSET).k(key).v(value).l(ttlMillis).s(token), Reply.BOOL);
    }

    /**
     * Sends {@code FILLSET key value ttl_ms token SWR stale_ms}: like
     * {@link #fillset(Object, Object, long, String)}, and the value may still be served as {@code STALE} for
     * {@code staleMillis} after its TTL while one caller refreshes it (stale-while-revalidate).
     *
     * @param key the cache key
     * @param value the value to cache
     * @param ttlMillis how long the value is fresh, in milliseconds
     * @param token the token {@code fillget} returned
     * @param staleMillis extra time after expiry during which the old value may be served, in milliseconds
     * @return {@code true} when written, {@code false} when the token was no longer live
     * @throws RuvioServerException {@code WRONGTYPE} for a non-string
     */
    public CompletableFuture<Boolean> fillset(K key, V value, long ttlMillis, String token, long staleMillis) {
        return send(args().w(Cmd.FILLSET).k(key).v(value).l(ttlMillis).s(token).w(Cmd.SWR).l(staleMillis), Reply.BOOL);
    }

    /**
     * Sends {@code SETEX key seconds value}: stores a string with a TTL. Same as {@code SET key value EX seconds}.
     *
     * @param key the key to write
     * @param seconds the TTL in seconds; must be greater than zero
     * @param value the value to store
     * @throws RuvioServerException {@code ERR} when {@code seconds} is not positive
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> setex(K key, long seconds, V value) {
        return klv(Cmd.SETEX, key, seconds, value, Reply.OK);
    }

    /**
     * Sends {@code PSETEX key millis value}: stores a string with a TTL in milliseconds.
     *
     * @param key the key to write
     * @param millis the TTL in milliseconds; must be greater than zero
     * @param value the value to store
     * @throws RuvioServerException {@code ERR} when {@code millis} is not positive
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> psetex(K key, long millis, V value) {
        return klv(Cmd.PSETEX, key, millis, value, Reply.OK);
    }

    /**
     * Sends {@code SETNX key value}: stores a string only when the key does not exist.
     *
     * @param key the key to write
     * @param value the value to store
     * @return {@code true} when written, {@code false} when the key already existed
     */
    public CompletableFuture<Boolean> setnx(K key, V value) {
        return kv(Cmd.SETNX, key, value, Reply.BOOL);
    }

    /**
     * Sends {@code GETDEL key}: reads a string and deletes the key.
     *
     * @param key the key to read and remove
     * @return the value, or {@code null} when the key did not exist
     * @throws RuvioServerException {@code WRONGTYPE} for a non-string
     */
    public CompletableFuture<V> getdel(K key) {
        return k(Cmd.GETDEL, key, r(Reply.VALUE));
    }

    /**
     * Sends {@code GETRANGE key start end}: reads a slice of a string by byte offset.
     *
     * <pre>{@code
     * String tail = client.getrange("greeting", -5, -1);
     * }</pre>
     *
     * @param key the key to read
     * @param start the first byte offset, inclusive; negative counts from the end ({@code -1} is the last byte)
     * @param end the last byte offset, inclusive; negative counts from the end; clamped to the string length
     * @return the slice, or an empty value when the key is missing or the range is empty (never {@code null})
     * @throws RuvioServerException {@code WRONGTYPE} for a non-string
     */
    public CompletableFuture<V> getrange(K key, long start, long end) {
        return kll(Cmd.GETRANGE, key, start, end, r(Reply.VALUE));
    }

    /**
     * Sends {@code SUBSTR key start end}: the old name of {@link #getrange(Object, long, long)}, same behaviour.
     *
     * @param key the key to read
     * @param start the first byte offset, inclusive; negative counts from the end
     * @param end the last byte offset, inclusive; negative counts from the end
     * @return the slice, or an empty value when the key is missing or the range is empty
     * @throws RuvioServerException {@code WRONGTYPE} for a non-string
     */
    public CompletableFuture<V> substr(K key, long start, long end) {
        return kll(Cmd.SUBSTR, key, start, end, r(Reply.VALUE));
    }

    /**
     * Sends {@code SETRANGE key offset value}: overwrites part of a string starting at a byte offset,
     * padding with zero bytes if the string is shorter. A missing key is created. Strings are capped at 16 MiB.
     *
     * @param key the key to modify
     * @param offset the byte offset to start at; zero or greater
     * @param value the bytes to write
     * @return the length of the string after the write
     * @throws RuvioServerException {@code ERR} when the result would exceed 16 MiB; {@code WRONGTYPE} for a non-string
     */
    public CompletableFuture<Long> setrange(K key, long offset, V value) {
        return klv(Cmd.SETRANGE, key, offset, value, Reply.LONG);
    }
}
