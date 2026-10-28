// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.commands;

import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.RuvioServerException;
import io.github.salihcantekin.ruvio.args.ExpireCondition;
import io.github.salihcantekin.ruvio.args.ScanArgs;
import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Cmd;
import io.github.salihcantekin.ruvio.internal.Reply;
import io.github.salihcantekin.ruvio.model.KeyValue;
import io.github.salihcantekin.ruvio.model.ScanResult;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Hash commands, including per-field TTLs. Fields use the key codec, values the value codec.
 *
 * @param <K> key type (also hash fields and stream field names)
 * @param <V> value type (also members, list elements and stream field values)
 */
public abstract class HashCommands<K, V> extends KeyCommands<K, V> {
    /**
     * Creates the command family with the given codecs.
     *
     * @param keys the key codec
     * @param values the value codec
     */
    protected HashCommands(RuvioCodec<K> keys, RuvioCodec<V> values) {
        super(keys, values);
    }

    static final Reply<ScanResult<KeyValue<Object, Object>>> SCAN_FIELDS = Reply.of((tree, q) -> {
        List<Object> items = Reply.list(tree);
        List<Object> raw = Reply.list(items.get(1));
        List<KeyValue<Object, Object>> out = new ArrayList<>(raw.size() / 2);
        for (int i = 0; i + 1 < raw.size(); i += 2) {
            out.add(new KeyValue<>(Reply.decodeWith(q.keys, raw.get(i)), Reply.decodeWith(q.values, raw.get(i + 1))));
        }
        return new ScanResult<>(Reply.str(items.get(0)), out);
    });

    static final Reply<List<KeyValue<Object, Object>>> FIELD_PAIRS = Reply.of((tree, q) -> {
        List<Object> raw = Reply.list(tree);
        List<KeyValue<Object, Object>> out = new ArrayList<>(raw.size() / 2);
        if (!raw.isEmpty() && raw.get(0) instanceof List) {
            for (Object pair : raw) {
                List<Object> p = Reply.list(pair);
                out.add(new KeyValue<>(Reply.decodeWith(q.keys, p.get(0)), Reply.decodeWith(q.values, p.get(1))));
            }
            return out;
        }
        for (int i = 0; i + 1 < raw.size(); i += 2) {
            out.add(new KeyValue<>(Reply.decodeWith(q.keys, raw.get(i)), Reply.decodeWith(q.values, raw.get(i + 1))));
        }
        return out;
    });

    /**
     * Sends {@code HSET key field value}: sets one field of a hash, creating the hash when missing. An existing field is overwritten, and its field TTL is dropped.
     *
     * <pre>{@code
     * client.hset("user:1", "name", "Ada");
     * }</pre>
     *
     * @param key the hash key
     * @param field the field name (encoded with the key codec)
     * @param value the field value
     * @return {@code 1} when the field is new, {@code 0} when an existing field was overwritten
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-hash
     */
    public CompletableFuture<Long> hset(K key, K field, V value) {
        return kfv(Cmd.HSET, key, field, value, Reply.LONG);
    }

    /**
     * Sends {@code HSET key field value [field value ...]}: sets several fields in one command.
     *
     * <pre>{@code
     * client.hset("user:1", Map.of("name", "Ada", "city", "London"));
     * }</pre>
     *
     * @param key the hash key
     * @param fields the field names and values to set; must not be empty
     * @return how many fields were new (overwritten fields are not counted)
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-hash
     */
    public CompletableFuture<Long> hset(K key, Map<K, V> fields) {
        Args a = args().w(Cmd.HSET).k(key);
        for (Map.Entry<K, V> e : fields.entrySet()) {
            a.k(e.getKey()).v(e.getValue());
        }
        return send(a, Reply.LONG);
    }

    /**
     * Sends {@code HMSET key field value [field value ...]}: sets several fields like {@link #hset(Object, Map)} but
     * the server replies {@code OK} instead of the count of new fields. Redis marks {@code HMSET} deprecated; prefer
     * {@code hset} in new code.
     *
     * <pre>{@code
     * client.hmset("user:1", Map.of("name", "Ada", "city", "London"));
     * }</pre>
     *
     * @param key the hash key
     * @param fields the field names and values to set; must not be empty
     * @return a future that completes with no value once the server has acknowledged the command
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-hash
     */
    public CompletableFuture<Void> hmset(K key, Map<K, V> fields) {
        Args a = args().w(Cmd.HMSET).k(key);
        for (Map.Entry<K, V> e : fields.entrySet()) {
            a.k(e.getKey()).v(e.getValue());
        }
        return send(a, Reply.OK);
    }

    /**
     * Sends {@code HGET key field}.
     *
     * @param key the hash key
     * @param field the field to read
     * @return the value, or {@code null} when the key or the field does not exist
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-hash
     */
    public CompletableFuture<V> hget(K key, K field) {
        return kf(Cmd.HGET, key, field, r(Reply.VALUE));
    }

    /**
     * Sends {@code HDEL key field [field ...]}: removes fields. A hash left without fields disappears.
     *
     * @param key the hash key
     * @param fields one or more fields to remove
     * @return how many fields existed and were removed
     * @throws IllegalArgumentException when no field is given
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-hash
     */
    @SafeVarargs
    public final CompletableFuture<Long> hdel(K key, K... fields) {
        return kfs(Cmd.HDEL, key, Reply.LONG, fields);
    }

    /**
     * Sends {@code HLEN key}.
     *
     * @param key the hash key
     * @return the number of fields, {@code 0} when the key does not exist
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-hash
     */
    public CompletableFuture<Long> hlen(K key) {
        return k(Cmd.HLEN, key, Reply.LONG);
    }

    /**
     * Sends {@code HGETALL key}: reads every field and value. The order is the server's storage order (unspecified, as in Redis). Cost grows with the hash size; for big hashes page with {@link #hscan(Object, String)}.
     *
     * <pre>{@code
     * Map<String, String> user = client.hgetall("user:1");
     * }</pre>
     *
     * @param key the hash key
     * @return a map of field to value, empty when the key does not exist
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-hash
     */
    public CompletableFuture<Map<K, V>> hgetall(K key) {
        return k(Cmd.HGETALL, key, r(Reply.KV_MAP));
    }

    /**
     * Sends {@code HMGET key field [field ...]}: reads several fields at once.
     *
     * @param key the hash key
     * @param fields one or more fields
     * @return one element per field in the same order; {@code null} for a field (or a whole key) that does not exist
     * @throws IllegalArgumentException when no field is given
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-hash
     */
    @SafeVarargs
    public final CompletableFuture<List<V>> hmget(K key, K... fields) {
        return kfs(Cmd.HMGET, key, r(Reply.VALUE_LIST), fields);
    }

    /**
     * Sends {@code HEXISTS key field}.
     *
     * @param key the hash key
     * @param field the field to test
     * @return {@code true} when the field exists, {@code false} when the field or the key does not
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-hash
     */
    public CompletableFuture<Boolean> hexists(K key, K field) {
        return kf(Cmd.HEXISTS, key, field, Reply.BOOL);
    }

    /**
     * Sends {@code HKEYS key}: lists the field names, in unspecified order.
     *
     * @param key the hash key
     * @return the field names, empty when the key does not exist
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-hash
     */
    public CompletableFuture<List<K>> hkeys(K key) {
        return k(Cmd.HKEYS, key, r(Reply.KEY_LIST));
    }

    /**
     * Sends {@code HVALS key}: lists the values, in unspecified order.
     *
     * @param key the hash key
     * @return the values, empty when the key does not exist
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-hash
     */
    public CompletableFuture<List<V>> hvals(K key) {
        return k(Cmd.HVALS, key, r(Reply.VALUE_LIST));
    }

    /**
     * Sends {@code HINCRBY key field delta}: adds an integer to a field. A missing key or field counts as 0. The field's TTL is kept.
     *
     * @param key the hash key
     * @param field the field to change
     * @param delta the signed amount to add
     * @return the field value after the addition
     * @throws RuvioServerException {@code ERR} when the field is not a signed 64-bit integer or the result overflows; {@code WRONGTYPE} when the key holds a non-hash
     */
    public CompletableFuture<Long> hincrby(K key, K field, long delta) {
        return send(args().w(Cmd.HINCRBY).k(key).k(field).l(delta), Reply.LONG);
    }

    /**
     * Sends {@code HINCRBY key field delta EX|PX ttl}: like {@link #hincrby(Object, Object, long)}, and a hash this call creates
     * gets a time to live. An existing key keeps its deadline.
     *
     * @param key the hash key
     * @param field the field to change
     * @param delta the signed amount to add
     * @param ttl time to live of a newly created key: whole seconds are sent as {@code EX}, anything finer as {@code PX}
     * @return the field value after the addition
     * @throws IllegalArgumentException when {@code ttl} is null, zero, negative or too large
     * @throws RuvioServerException {@code ERR} when the field is not a signed 64-bit integer or the result overflows; {@code WRONGTYPE} when the key holds a non-hash
     */
    public CompletableFuture<Long> hincrby(K key, K field, long delta, Duration ttl) {
        return send(args().w(Cmd.HINCRBY).k(key).k(field).l(delta).ttl(ttl), Reply.LONG);
    }

    /**
     * Sends {@code HTRANSFER key from to amount}: moves {@code amount} between two integer fields of one hash
     * in one step, only when {@code from} holds at least that much. A missing {@code to} field starts at 0 and
     * field TTLs stay.
     *
     * @param key the hash key
     * @param from the field to subtract from
     * @param to the field to add to
     * @param amount the amount to move; must be greater than zero
     * @return {@code [fromAfter, toAfter]}, or {@code null} when the key or {@code from} is missing or short, or
     * {@code to} would overflow (nothing is written)
     * @throws RuvioServerException {@code ERR} when {@code amount} is not positive, the fields are the same or a
     * field is not an integer; {@code WRONGTYPE} when the key holds a non-hash
     */
    public CompletableFuture<List<Long>> htransfer(K key, K from, K to, long amount) {
        return send(args().w(Cmd.HTRANSFER).k(key).k(from).k(to).l(amount), Reply.LONG_PAIR);
    }

    /**
     * Sends {@code HTRANSFER key from to amount MAX max}: {@link #htransfer(Object, Object, Object, long)} that
     * also refuses when {@code to} would end above {@code max}.
     *
     * @param key the hash key
     * @param from the field to subtract from
     * @param to the field to add to
     * @param amount the amount to move; must be greater than zero
     * @param max the highest value {@code to} may reach
     * @return {@code [fromAfter, toAfter]}, or {@code null} when nothing was moved
     * @throws RuvioServerException {@code ERR} when {@code amount} is not positive, the fields are the same or a
     * field is not an integer; {@code WRONGTYPE} when the key holds a non-hash
     */
    public CompletableFuture<List<Long>> htransfer(K key, K from, K to, long amount, long max) {
        return send(args().w(Cmd.HTRANSFER).k(key).k(from).k(to).l(amount).w(Cmd.MAX).l(max), Reply.LONG_PAIR);
    }

    /**
     * Sends {@code HINCRBYFLOAT key field delta}: adds a floating-point number to a field. A missing key or field counts as 0.
     *
     * @param key the hash key
     * @param field the field to change
     * @param delta the amount to add (may be negative)
     * @return the field value after the addition
     * @throws RuvioServerException {@code ERR} when the field is not a number; {@code WRONGTYPE} when the key holds a non-hash
     */
    public CompletableFuture<Double> hincrbyfloat(K key, K field, double delta) {
        return send(args().w(Cmd.HINCRBYFLOAT).k(key).k(field).d(delta), Reply.DOUBLE);
    }

    /**
     * Sends {@code HSETNX key field value}: sets a field only when it does not exist yet.
     *
     * @param key the hash key
     * @param field the field to set
     * @param value the value to store
     * @return {@code true} when the field was written, {@code false} when it already existed (nothing changes)
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-hash
     */
    public CompletableFuture<Boolean> hsetnx(K key, K field, V value) {
        return kfv(Cmd.HSETNX, key, field, value, Reply.BOOL);
    }

    /**
     * Sends {@code HSTRLEN key field}.
     *
     * @param key the hash key
     * @param field the field to measure
     * @return the value length in bytes, {@code 0} when the key or field does not exist
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-hash
     */
    public CompletableFuture<Long> hstrlen(K key, K field) {
        return kf(Cmd.HSTRLEN, key, field, Reply.LONG);
    }

    /**
     * Sends {@code HSCAN key cursor}: walks a hash a page at a time. Start with {@code "0"} and continue with
     * {@link ScanResult#cursor()} until {@link ScanResult#isFinished()}. A field can appear twice if the hash
     * is rebuilt between pages, but none is skipped. See {@link io.github.salihcantekin.ruvio.RuvioClient#hscanIterable}.
     *
     * @param key the hash key
     * @param cursor {@code "0"} to start, otherwise the previous page's cursor
     * @return the next cursor and the field/value pairs of this page (empty page for a missing key)
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-hash
     */
    public CompletableFuture<ScanResult<KeyValue<K, V>>> hscan(K key, String cursor) {
        return send(args().w(Cmd.HSCAN).k(key).s(cursor), r(SCAN_FIELDS));
    }

    /**
     * Sends {@code HSCAN key cursor [MATCH pattern] [COUNT n]}: like {@link #hscan(Object, String)} with a field-name filter and a page size hint. The {@code TYPE} option of {@code ScanArgs} does not apply here.
     *
     * @param key the hash key
     * @param cursor {@code "0"} to start, otherwise the previous page's cursor
     * @param options {@code MATCH} glob on field names and {@code COUNT} hint
     * @return the next cursor and the matching field/value pairs of this page
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-hash
     */
    public CompletableFuture<ScanResult<KeyValue<K, V>>> hscan(K key, String cursor, ScanArgs options) {
        Args a = args().w(Cmd.HSCAN).k(key).s(cursor);
        options.write(a, false);
        return send(a, r(SCAN_FIELDS));
    }

    /**
     * Sends {@code HRANDFIELD key}: picks one random field.
     *
     * @param key the hash key
     * @return a field name, or {@code null} when the key does not exist
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-hash
     */
    public CompletableFuture<K> hrandfield(K key) {
        return k(Cmd.HRANDFIELD, key, r(Reply.KEY));
    }

    /**
     * Sends {@code HRANDFIELD key count}: picks random fields. Does not remove anything.
     *
     * @param key the hash key
     * @param count a positive count returns up to that many distinct fields; a negative count returns {@code -count} fields that may repeat (limited to 1,048,576)
     * @return the chosen field names, empty when the key does not exist
     * @throws RuvioServerException {@code ERR count is out of range} for a negative count beyond the limit; {@code WRONGTYPE} when the key holds a non-hash
     */
    public CompletableFuture<List<K>> hrandfield(K key, long count) {
        return kl(Cmd.HRANDFIELD, key, count, r(Reply.KEY_LIST));
    }

    /**
     * Sends {@code HRANDFIELD key count WITHVALUES}: like {@link #hrandfield(Object, long)}, with each field's value.
     *
     * @param key the hash key
     * @param count positive for distinct fields, negative for repeats allowed
     * @return the chosen field/value pairs, empty when the key does not exist
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-hash
     */
    public CompletableFuture<List<KeyValue<K, V>>> hrandfieldWithValues(K key, long count) {
        return send(args().w(Cmd.HRANDFIELD).k(key).l(count).s("WITHVALUES"), r(FIELD_PAIRS));
    }

    // ---- field TTLs ------------------------------------------------------------------------

    private CompletableFuture<List<Long>> fieldTtl(byte[] cmd, K key, long time, ExpireCondition condition, K[] fields) {
        Args a = args().w(cmd).k(key).l(time);
        if (condition != null && condition != ExpireCondition.ALWAYS) {
            a.w(condition.fragment());
        }
        a.w(Cmd.FIELDS).l(fields.length).keys(fields);
        return send(a, Reply.LONG_LIST);
    }

    private CompletableFuture<List<Long>> fieldQuery(byte[] cmd, K key, K[] fields) {
        return send(args().w(cmd).k(key).w(Cmd.FIELDS).l(fields.length).keys(fields), Reply.LONG_LIST);
    }

    /**
     * Sends {@code HEXPIRE key seconds FIELDS n field [field ...]}: gives individual hash fields a time to live. When a field's TTL passes, the field is removed; the hash disappears with its last field.
     *
     * <pre>{@code
     * List<Long> r = client.hexpire("session:1", 60, "token", "nonce"); // [1, 1]
     * }</pre>
     *
     * @param key the hash key
     * @param seconds seconds from now
     * @param fields one or more fields
     * @return one status per field, in order: {@code 1} TTL set, {@code 2} the field was deleted (TTL already due),
     * {@code 0} not applied, {@code -2} the field (or the key) does not exist
     * @throws IllegalArgumentException when no field is given
     * @throws RuvioServerException {@code ERR} for an out-of-range time; {@code WRONGTYPE} when the key holds a non-hash
     */
    @SafeVarargs
    public final CompletableFuture<List<Long>> hexpire(K key, long seconds, K... fields) {
        return fieldTtl(Cmd.HEXPIRE, key, seconds, null, fields);
    }

    /**
     * Sends {@code HEXPIRE key seconds [NX|XX|GT|LT] FIELDS n field [field ...]}: like
     * {@link #hexpire(Object, long, Object[])} but only for fields whose current TTL satisfies the condition.
     *
     * @param key the hash key
     * @param seconds seconds from now
     * @param condition {@code NX} only fields without a TTL, {@code XX} only fields with one, {@code GT}/{@code LT} only when the new deadline is later/earlier; {@code ALWAYS} (or null) sends no flag
     * @param fields one or more fields
     * @return one status per field: {@code 1} set, {@code 2} deleted, {@code 0} the condition was not met, {@code -2} no such field
     * @throws RuvioServerException {@code ERR} for an out-of-range time; {@code WRONGTYPE} when the key holds a non-hash
     */
    @SafeVarargs
    public final CompletableFuture<List<Long>> hexpire(K key, long seconds, ExpireCondition condition, K... fields) {
        return fieldTtl(Cmd.HEXPIRE, key, seconds, condition, fields);
    }

    /**
     * Sends {@code HPEXPIRE key milliseconds FIELDS n field [field ...]}: like {@link #hexpire(Object, long, Object[])} in milliseconds.
     *
     * @param key the hash key
     * @param millis milliseconds from now
     * @param fields one or more fields
     * @return one status per field: {@code 1} set, {@code 2} deleted, {@code 0} not applied, {@code -2} no such field
     * @throws RuvioServerException {@code ERR} for an out-of-range time; {@code WRONGTYPE} when the key holds a non-hash
     */
    @SafeVarargs
    public final CompletableFuture<List<Long>> hpexpire(K key, long millis, K... fields) {
        return fieldTtl(Cmd.HPEXPIRE, key, millis, null, fields);
    }

    /**
     * Sends {@code HPEXPIRE key milliseconds [NX|XX|GT|LT] FIELDS n field [field ...]}: milliseconds variant with a condition.
     *
     * @param key the hash key
     * @param millis milliseconds from now
     * @param condition which fields to change, see {@link ExpireCondition}
     * @param fields one or more fields
     * @return one status per field: {@code 1} set, {@code 2} deleted, {@code 0} condition not met, {@code -2} no such field
     * @throws RuvioServerException {@code ERR} for an out-of-range time; {@code WRONGTYPE} when the key holds a non-hash
     */
    @SafeVarargs
    public final CompletableFuture<List<Long>> hpexpire(K key, long millis, ExpireCondition condition, K... fields) {
        return fieldTtl(Cmd.HPEXPIRE, key, millis, condition, fields);
    }

    /**
     * Sends {@code HEXPIREAT key unix-seconds FIELDS n field [field ...]}: expires fields at an absolute Unix time in seconds.
     *
     * @param key the hash key
     * @param unixSeconds absolute deadline in seconds since 1970-01-01 UTC; a time in the past deletes the fields
     * @param fields one or more fields
     * @return one status per field: {@code 1} set, {@code 2} deleted, {@code 0} not applied, {@code -2} no such field
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-hash
     */
    @SafeVarargs
    public final CompletableFuture<List<Long>> hexpireat(K key, long unixSeconds, K... fields) {
        return fieldTtl(Cmd.HEXPIREAT, key, unixSeconds, null, fields);
    }

    /**
     * Sends {@code HEXPIREAT key unix-seconds [NX|XX|GT|LT] FIELDS n field [field ...]}.
     *
     * @param key the hash key
     * @param unixSeconds absolute deadline in seconds since 1970-01-01 UTC
     * @param condition which fields to change, see {@link ExpireCondition}
     * @param fields one or more fields
     * @return one status per field: {@code 1} set, {@code 2} deleted, {@code 0} condition not met, {@code -2} no such field
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-hash
     */
    @SafeVarargs
    public final CompletableFuture<List<Long>> hexpireat(K key, long unixSeconds, ExpireCondition condition, K... fields) {
        return fieldTtl(Cmd.HEXPIREAT, key, unixSeconds, condition, fields);
    }

    /**
     * Sends {@code HPEXPIREAT key unix-ms FIELDS n field [field ...]}: expires fields at an absolute Unix time in milliseconds.
     *
     * @param key the hash key
     * @param unixMillis absolute deadline in milliseconds since 1970-01-01 UTC; a time in the past deletes the fields
     * @param fields one or more fields
     * @return one status per field: {@code 1} set, {@code 2} deleted, {@code 0} not applied, {@code -2} no such field
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-hash
     */
    @SafeVarargs
    public final CompletableFuture<List<Long>> hpexpireat(K key, long unixMillis, K... fields) {
        return fieldTtl(Cmd.HPEXPIREAT, key, unixMillis, null, fields);
    }

    /**
     * Sends {@code HPEXPIREAT key unix-ms [NX|XX|GT|LT] FIELDS n field [field ...]}.
     *
     * @param key the hash key
     * @param unixMillis absolute deadline in milliseconds since 1970-01-01 UTC
     * @param condition which fields to change, see {@link ExpireCondition}
     * @param fields one or more fields
     * @return one status per field: {@code 1} set, {@code 2} deleted, {@code 0} condition not met, {@code -2} no such field
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-hash
     */
    @SafeVarargs
    public final CompletableFuture<List<Long>> hpexpireat(K key, long unixMillis, ExpireCondition condition, K... fields) {
        return fieldTtl(Cmd.HPEXPIREAT, key, unixMillis, condition, fields);
    }

    /**
     * Sends {@code HTTL key FIELDS n field [field ...]}: remaining time to live of fields, in seconds (rounded up).
     *
     * @param key the hash key
     * @param fields one or more fields
     * @return one value per field: the seconds left, {@code -1} when the field has no TTL, {@code -2} when the field or key does not exist
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-hash
     */
    @SafeVarargs
    public final CompletableFuture<List<Long>> httl(K key, K... fields) {
        return fieldQuery(Cmd.HTTL, key, fields);
    }

    /**
     * Sends {@code HPTTL key FIELDS n field [field ...]}: remaining time to live of fields, in milliseconds.
     *
     * @param key the hash key
     * @param fields one or more fields
     * @return one value per field: the milliseconds left, {@code -1} no TTL, {@code -2} no such field or key
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-hash
     */
    @SafeVarargs
    public final CompletableFuture<List<Long>> hpttl(K key, K... fields) {
        return fieldQuery(Cmd.HPTTL, key, fields);
    }

    /**
     * Sends {@code HEXPIRETIME key FIELDS n field [field ...]}: the absolute expiry of fields as Unix seconds.
     *
     * @param key the hash key
     * @param fields one or more fields
     * @return one value per field: the Unix time in seconds, {@code -1} no TTL, {@code -2} no such field or key
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-hash
     */
    @SafeVarargs
    public final CompletableFuture<List<Long>> hexpiretime(K key, K... fields) {
        return fieldQuery(Cmd.HEXPIRETIME, key, fields);
    }

    /**
     * Sends {@code HPEXPIRETIME key FIELDS n field [field ...]}: the absolute expiry of fields as Unix milliseconds.
     *
     * @param key the hash key
     * @param fields one or more fields
     * @return one value per field: the Unix time in milliseconds, {@code -1} no TTL, {@code -2} no such field or key
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-hash
     */
    @SafeVarargs
    public final CompletableFuture<List<Long>> hpexpiretime(K key, K... fields) {
        return fieldQuery(Cmd.HPEXPIRETIME, key, fields);
    }

    /**
     * Sends {@code HPERSIST key FIELDS n field [field ...]}: removes the TTL of fields so they live on.
     *
     * @param key the hash key
     * @param fields one or more fields
     * @return one value per field: {@code 1} a TTL was removed, {@code -1} the field had no TTL, {@code -2} no such field or key
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-hash
     */
    @SafeVarargs
    public final CompletableFuture<List<Long>> hpersist(K key, K... fields) {
        return fieldQuery(Cmd.HPERSIST, key, fields);
    }
}
