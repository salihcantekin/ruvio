// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.commands;

import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.RuvioServerException;
import io.github.salihcantekin.ruvio.args.StreamTrim;
import io.github.salihcantekin.ruvio.args.XAddArgs;
import io.github.salihcantekin.ruvio.args.XClaimArgs;
import io.github.salihcantekin.ruvio.args.XReadArgs;
import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Cmd;
import io.github.salihcantekin.ruvio.internal.Reply;
import io.github.salihcantekin.ruvio.internal.Request;
import io.github.salihcantekin.ruvio.model.AutoClaimResult;
import io.github.salihcantekin.ruvio.model.KeyValue;
import io.github.salihcantekin.ruvio.model.PendingEntry;
import io.github.salihcantekin.ruvio.model.PendingSummary;
import io.github.salihcantekin.ruvio.model.StreamEntry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Stream commands. Field names use the key codec and field values the value codec. BLOCK reads
 * hold the connection while they wait: run them on {@code client.dedicated()}.
 *
 * @param <K> key type (also hash fields and stream field names)
 * @param <V> value type (also members, list elements and stream field values)
 */
public abstract class StreamCommands<K, V> extends GeoCommands<K, V> {
    /**
     * Creates the command family with the given codecs.
     *
     * @param keys the key codec
     * @param values the value codec
     */
    protected StreamCommands(RuvioCodec<K> keys, RuvioCodec<V> values) {
        super(keys, values);
    }

    static StreamEntry<Object, Object> entry(Object tree, Request<?> q) {
        List<Object> parts = Reply.list(tree);
        String id = Reply.str(parts.get(0));
        Object rawFields = parts.size() > 1 ? parts.get(1) : null;
        List<KeyValue<Object, Object>> fields = Collections.emptyList();
        if (rawFields != null) {
            List<Object> f = Reply.list(rawFields);
            fields = new ArrayList<>(f.size() / 2);
            for (int i = 0; i + 1 < f.size(); i += 2) {
                fields.add(new KeyValue<>(Reply.decodeWith(q.keys, f.get(i)), Reply.decodeWith(q.values, f.get(i + 1))));
            }
        }
        return new StreamEntry<>(id, fields);
    }

    static List<StreamEntry<Object, Object>> entries(Object tree, Request<?> q) {
        if (tree == null) {
            return Collections.emptyList();
        }
        List<Object> items = Reply.list(tree);
        if (!items.isEmpty() && !(items.get(0) instanceof List) && items.get(0) != null) {
            // A single entry not wrapped in a list (observed on Ruvio's XREAD BLOCK wake-up reply).
            List<StreamEntry<Object, Object>> one = new ArrayList<>(1);
            one.add(entry(items, q));
            return one;
        }
        List<StreamEntry<Object, Object>> out = new ArrayList<>(items.size());
        for (Object item : items) {
            if (item != null) {
                out.add(entry(item, q));
            }
        }
        return out;
    }

    static final Reply<List<StreamEntry<Object, Object>>> ENTRIES = Reply.of(StreamCommands::entries);

    /** XREAD / XREADGROUP: key -> entries, in reply order. Empty on timeout. */
    @SuppressWarnings("unchecked")
    static final Reply<Map<Object, List<StreamEntry<Object, Object>>>> READ = Reply.of((tree, q) -> {
        Map<Object, List<StreamEntry<Object, Object>>> out = new LinkedHashMap<>();
        if (tree == null) {
            return out;
        }
        if (tree instanceof Map) {
            for (Map.Entry<Object, Object> e : ((Map<Object, Object>) tree).entrySet()) {
                out.put(Reply.decodeWith(q.keys, e.getKey()), entries(e.getValue(), q));
            }
            return out;
        }
        for (Object stream : Reply.list(tree)) {
            List<Object> pair = Reply.list(stream);
            out.put(Reply.decodeWith(q.keys, pair.get(0)), entries(pair.get(1), q));
        }
        return out;
    });

    static final Reply<PendingSummary> PENDING_SUMMARY = Reply.of((tree, q) -> {
        List<Object> items = Reply.list(tree);
        Map<String, Long> consumers = new LinkedHashMap<>();
        if (items.size() > 3 && items.get(3) != null) {
            for (Object row : Reply.list(items.get(3))) {
                List<Object> p = Reply.list(row);
                consumers.put(Reply.str(p.get(0)), Reply.lng(p.get(1)));
            }
        }
        return new PendingSummary(Reply.lng(items.get(0)), Reply.str(items.get(1)), Reply.str(items.get(2)), consumers);
    });

    static final Reply<List<PendingEntry>> PENDING_ROWS = Reply.of((tree, q) -> {
        List<Object> items = Reply.list(tree);
        List<PendingEntry> out = new ArrayList<>(items.size());
        for (Object row : items) {
            List<Object> p = Reply.list(row);
            out.add(new PendingEntry(Reply.str(p.get(0)), Reply.str(p.get(1)), Reply.lng(p.get(2)), Reply.lng(p.get(3))));
        }
        return out;
    });

    static final Reply<AutoClaimResult<Object, Object>> AUTOCLAIM = Reply.of((tree, q) -> {
        List<Object> items = Reply.list(tree);
        List<StreamEntry<Object, Object>> entries = new ArrayList<>();
        List<String> ids = new ArrayList<>();
        for (Object item : Reply.list(items.get(1))) {
            if (item instanceof List) {
                StreamEntry<Object, Object> e = entry(item, q);
                entries.add(e);
                ids.add(e.id());
            } else if (item != null) {
                ids.add(Reply.str(item));
            }
        }
        List<String> deleted = new ArrayList<>();
        if (items.size() > 2) {
            for (Object d : Reply.list(items.get(2))) {
                deleted.add(Reply.str(d));
            }
        }
        return new AutoClaimResult<>(Reply.str(items.get(0)), entries, ids, deleted);
    });

    @SuppressWarnings("unchecked")
    static final Reply<Map<String, Object>> INFO_MAP = Reply.of((tree, q) -> (Map<String, Object>) (Map<?, ?>) Reply.fieldMap(Reply.text(tree)));

    static final Reply<List<Map<String, Object>>> INFO_LIST = Reply.of((tree, q) -> {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Object row : Reply.list(tree)) {
            out.add(Reply.fieldMap(Reply.text(row)));
        }
        return out;
    });

    private void fields(Args a, Map<K, V> fields) {
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("a stream entry needs at least one field");
        }
        for (Map.Entry<K, V> e : fields.entrySet()) {
            a.k(e.getKey()).v(e.getValue());
        }
    }

    /**
     * Sends {@code XADD key * field value [field value ...]}: appends an entry to a stream with a server-generated id, creating the stream when missing.
     *
     * <pre>{@code
     * String id = client.xadd("events", Map.of("type", "signup", "user", "42"));
     * }</pre>
     *
     * @param key the stream key
     * @param fields the entry's field names (key codec) and values (value codec); must not be empty
     * @return the new entry id, like {@code 1718000000000-0}
     * @throws IllegalArgumentException when {@code fields} is empty
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-stream
     */
    public CompletableFuture<String> xadd(K key, Map<K, V> fields) {
        Args a = args().w(Cmd.XADD).k(key).s("*");
        fields(a, fields);
        return send(a, Reply.STRING);
    }

    /**
     * Sends {@code XADD key id field value ...} with an explicit id.
     *
     * @param key the stream key
     * @param id {@code *} for a generated id, {@code ms-*} to fix only the millisecond part, or {@code ms-seq}; it must be greater than the last id
     * @param fields the entry's fields; must not be empty
     * @return the id the entry got
     * @throws IllegalArgumentException when {@code fields} is empty
     * @throws RuvioServerException {@code ERR} when the id is invalid or not greater than the stream's last id; {@code WRONGTYPE} for a non-stream
     */
    public CompletableFuture<String> xadd(K key, String id, Map<K, V> fields) {
        Args a = args().w(Cmd.XADD).k(key).s(id);
        fields(a, fields);
        return send(a, Reply.STRING);
    }

    /**
     * Sends {@code XADD key [NOMKSTREAM] [DELAY ms] [MAXLEN|MINID [~] threshold] id field value ...}.
     *
     * <pre>{@code
     * client.xadd("jobs", XAddArgs.builder().maxLen(10_000).delay(5_000), Map.of("job", "email"));
     * }</pre>
     *
     * @param key the stream key
     * @param options id, {@code noMkStream()} (do not create the stream), {@code maxLen}/{@code minId} trimming (exact even with {@code ~}), and the Ruvio {@code delay(ms)} that hides the entry from {@code XREADGROUP} for that long
     * @param fields the entry's fields; must not be empty
     * @return the id the entry got, or {@code null} when {@code noMkStream()} was set and the stream does not exist
     * @throws IllegalArgumentException when {@code fields} is empty
     * @throws RuvioServerException {@code ERR} for an invalid id; {@code WRONGTYPE} for a non-stream
     */
    public CompletableFuture<String> xadd(K key, XAddArgs options, Map<K, V> fields) {
        Args a = args().w(Cmd.XADD).k(key);
        options.write(a);
        fields(a, fields);
        return send(a, Reply.STRING);
    }

    /**
     * Sends {@code XLEN key}.
     *
     * @param key the stream key
     * @return the number of entries, {@code 0} for a missing key
     * @throws RuvioServerException {@code WRONGTYPE} for a non-stream
     */
    public CompletableFuture<Long> xlen(K key) {
        return k(Cmd.XLEN, key, Reply.LONG);
    }

    /**
     * Sends {@code XRANGE key start end}: entries between two ids, oldest first.
     *
     * <pre>{@code
     * List<StreamEntry<String, String>> all = client.xrange("events", "-", "+");
     * }</pre>
     *
     * @param key the stream key
     * @param start the first id, inclusive; {@code -} for the beginning
     * @param end the last id, inclusive; {@code +} for the end
     * @return the entries, empty for a missing key
     * @throws RuvioServerException {@code ERR} for an invalid id; {@code WRONGTYPE} for a non-stream
     */
    public CompletableFuture<List<StreamEntry<K, V>>> xrange(K key, String start, String end) {
        return send(args().w(Cmd.XRANGE).k(key).s(start).s(end), r(ENTRIES));
    }

    /**
     * Sends {@code XRANGE key start end COUNT n}.
     *
     * @param key the stream key
     * @param start the first id, inclusive ({@code -} for the beginning)
     * @param end the last id, inclusive ({@code +} for the end)
     * @param count the most entries to return
     * @return the entries, oldest first
     * @throws RuvioServerException {@code ERR} for an invalid id; {@code WRONGTYPE} for a non-stream
     */
    public CompletableFuture<List<StreamEntry<K, V>>> xrange(K key, String start, String end, long count) {
        return send(args().w(Cmd.XRANGE).k(key).s(start).s(end).w(Cmd.COUNT).l(count), r(ENTRIES));
    }

    /**
     * Sends {@code XREVRANGE key end start}: like {@link #xrange(Object, String, String)} newest first. The high id comes first.
     *
     * @param key the stream key
     * @param end the highest id, inclusive ({@code +} for the end)
     * @param start the lowest id, inclusive ({@code -} for the beginning)
     * @return the entries, newest first
     * @throws RuvioServerException {@code ERR} for an invalid id; {@code WRONGTYPE} for a non-stream
     */
    public CompletableFuture<List<StreamEntry<K, V>>> xrevrange(K key, String end, String start) {
        return send(args().w(Cmd.XREVRANGE).k(key).s(end).s(start), r(ENTRIES));
    }

    /**
     * Sends {@code XREVRANGE key end start COUNT n}.
     *
     * @param key the stream key
     * @param end the highest id, inclusive
     * @param start the lowest id, inclusive
     * @param count the most entries to return
     * @return the entries, newest first
     * @throws RuvioServerException {@code ERR} for an invalid id; {@code WRONGTYPE} for a non-stream
     */
    public CompletableFuture<List<StreamEntry<K, V>>> xrevrange(K key, String end, String start, long count) {
        return send(args().w(Cmd.XREVRANGE).k(key).s(end).s(start).w(Cmd.COUNT).l(count), r(ENTRIES));
    }

    /**
     * Sends {@code XREAD STREAMS key id}: entries added after {@code id}, without waiting.
     *
     * @param key the stream key
     * @param id read entries with a greater id: {@code 0} for everything, {@code $} for "nothing yet" (use it only with blocking reads), or a stored id
     * @return a map from the stream key to its new entries; empty when there is nothing new
     * @throws RuvioServerException {@code ERR} for an invalid id; {@code WRONGTYPE} for a non-stream
     */
    public CompletableFuture<Map<K, List<StreamEntry<K, V>>>> xread(K key, String id) {
        return send(args().w(Cmd.XREAD).w(Cmd.STREAMS).k(key).s(id), r(READ));
    }

    /**
     * Sends {@code XREAD [COUNT n] [BLOCK ms] STREAMS key [key ...] id [id ...]}. {@code BLOCK} waits on the server and holds the shared connection (use {@link io.github.salihcantekin.ruvio.RuvioClient#dedicated()}); a blocked read does not remove entries. All keys must hash to one slot in a cluster.
     *
     * <pre>{@code
     * try (RuvioClient<String, String> reader = client.dedicated()) {
     *     var got = reader.xread(XReadArgs.builder().block(5_000).count(10), Map.of("events", "$"));
     * }
     * }</pre>
     *
     * @param options {@code count(n)} and {@code block(ms)} ({@code 0} waits until data arrives or the server stops)
     * @param streams stream key to last-seen id, in insertion order (use a {@code LinkedHashMap}); {@code $} means the current end
     * @return a map from stream key to the new entries; empty when the block time passed with nothing new
     * @throws RuvioServerException {@code ERR} for an invalid id; {@code WRONGTYPE}; {@code CROSSSLOT} in a cluster
     */
    public CompletableFuture<Map<K, List<StreamEntry<K, V>>>> xread(XReadArgs options, Map<K, String> streams) {
        Args a = args().w(Cmd.XREAD);
        options.write(a, false);
        a.w(Cmd.STREAMS);
        for (K key : streams.keySet()) {
            a.k(key);
        }
        for (String id : streams.values()) {
            a.s(id);
        }
        return send(a, r(READ));
    }

    /**
     * Sends {@code XDEL key id [id ...]}: deletes entries by id.
     *
     * @param key the stream key
     * @param ids one or more entry ids
     * @return how many entries existed and were deleted
     * @throws RuvioServerException {@code ERR} for an invalid id; {@code WRONGTYPE} for a non-stream
     */
    public CompletableFuture<Long> xdel(K key, String... ids) {
        return send(args().w(Cmd.XDEL).k(key).strings(ids), Reply.LONG);
    }

    /**
     * Sends {@code XSETID key last-id}: moves the stream's last generated id forward without adding an entry.
     *
     * @param key the stream key
     * @param lastId the new last id; it cannot be lower than the current last id or the highest entry
     * @throws RuvioServerException {@code ERR} when the id is invalid or would move backward; {@code WRONGTYPE} for a non-stream
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> xsetid(K key, String lastId) {
        return ks(Cmd.XSETID, key, lastId, Reply.OK);
    }

    /**
     * Sends {@code XTRIM key MAXLEN [~] n}: keeps at most {@code maxLen} newest entries. Ruvio trims exactly even with {@code ~}; the {@code LIMIT} option is not supported.
     *
     * @param key the stream key
     * @param maxLen the most entries to keep; zero or greater
     * @param approximate {@code true} sends the {@code ~} flag (accepted for compatibility; the trim is still exact)
     * @return how many entries were removed
     * @throws RuvioServerException {@code WRONGTYPE} for a non-stream
     */
    public CompletableFuture<Long> xtrimMaxlen(K key, long maxLen, boolean approximate) {
        Args a = args().w(Cmd.XTRIM).k(key).s("MAXLEN");
        if (approximate) a.s("~");
        return send(a.l(maxLen), Reply.LONG);
    }

    /**
     * Sends {@code XTRIM key MINID [~] id}: removes entries with an id lower than {@code minId}.
     *
     * @param key the stream key
     * @param minId the lowest id to keep
     * @param approximate {@code true} sends the {@code ~} flag (the trim is still exact)
     * @return how many entries were removed
     * @throws RuvioServerException {@code ERR} for an invalid id; {@code WRONGTYPE} for a non-stream
     */
    public CompletableFuture<Long> xtrimMinid(K key, String minId, boolean approximate) {
        Args a = args().w(Cmd.XTRIM).k(key).s("MINID");
        if (approximate) a.s("~");
        return send(a.s(minId), Reply.LONG);
    }

    /**
     * Sends {@code XTRIM key MAXLEN|MINID =|~ threshold} from a {@link StreamTrim}; the keywords are sent from pre-encoded bytes. The {@code LIMIT} option is not supported.
     *
     * <pre>{@code
     * long removed = client.xtrim("events", StreamTrim.maxLen(1000));
     * }</pre>
     *
     * @param key the stream key
     * @param trim the trim rule
     * @return how many entries were removed
     * @throws RuvioServerException {@code ERR} for an invalid id; {@code WRONGTYPE} for a non-stream
     */
    public CompletableFuture<Long> xtrim(K key, StreamTrim trim) {
        Args a = args().w(Cmd.XTRIM).k(key);
        trim.write(a);
        return send(a, Reply.LONG);
    }

    /**
     * Sends {@code XGROUP CREATE key group id}: creates a consumer group. The stream must already exist; see the overload with {@code mkStream}.
     *
     * @param key the stream key
     * @param group the group name
     * @param id where the group starts reading: {@code $} for only new entries, {@code 0} for the whole stream, or a stored id
     * @throws RuvioServerException {@code BUSYGROUP} when the group exists; {@code ERR} when the stream does not exist; {@code WRONGTYPE} for a non-stream
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> xgroupCreate(K key, String group, String id) {
        return send(args().w(Cmd.XGROUP).s("CREATE").k(key).s(group).s(id), Reply.OK);
    }

    /**
     * Sends {@code XGROUP CREATE key group id [MKSTREAM]}.
     *
     * <pre>{@code
     * client.xgroupCreate("jobs", "workers", "0", true);
     * }</pre>
     *
     * @param key the stream key
     * @param group the group name
     * @param id {@code $}, {@code 0} or a stored id
     * @param mkStream {@code true} to create an empty stream when the key does not exist
     * @throws RuvioServerException {@code BUSYGROUP} when the group exists; {@code ERR} when the stream is missing and {@code mkStream} is false; {@code WRONGTYPE} for a non-stream
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> xgroupCreate(K key, String group, String id, boolean mkStream) {
        Args a = args().w(Cmd.XGROUP).s("CREATE").k(key).s(group).s(id);
        if (mkStream) a.s("MKSTREAM");
        return send(a, Reply.OK);
    }

    /**
     * Sends {@code XGROUP CREATECONSUMER key group consumer}: registers a consumer with no pending entries. Consumers are also created by their first read.
     *
     * @param key the stream key
     * @param group the group name
     * @param consumer the consumer name
     * @return {@code true} when created, {@code false} when it already existed
     * @throws RuvioServerException {@code NOGROUP} when the stream or group does not exist; {@code WRONGTYPE}
     */
    public CompletableFuture<Boolean> xgroupCreateconsumer(K key, String group, String consumer) {
        return send(args().w(Cmd.XGROUP).s("CREATECONSUMER").k(key).s(group).s(consumer), Reply.BOOL);
    }

    /**
     * Sends {@code XGROUP DESTROY key group}: deletes a group and its pending entries.
     *
     * @param key the stream key
     * @param group the group name
     * @return {@code true} when the group existed and was removed
     * @throws RuvioServerException {@code WRONGTYPE} for a non-stream
     */
    public CompletableFuture<Boolean> xgroupDestroy(K key, String group) {
        return send(args().w(Cmd.XGROUP).s("DESTROY").k(key).s(group), Reply.BOOL);
    }

    /**
     * Sends {@code XGROUP SETID key group id}: moves the group's read position.
     *
     * @param key the stream key
     * @param group the group name
     * @param id {@code $} for the end, {@code 0} for the start, or a stored id
     * @throws RuvioServerException {@code NOGROUP} when the stream or group does not exist; {@code ERR} for an invalid id
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> xgroupSetid(K key, String group, String id) {
        return send(args().w(Cmd.XGROUP).s("SETID").k(key).s(group).s(id), Reply.OK);
    }

    /**
     * Sends {@code XGROUP DELCONSUMER key group consumer}: removes a consumer and drops its pending entries.
     *
     * @param key the stream key
     * @param group the group name
     * @param consumer the consumer name
     * @return how many pending entries the consumer held
     * @throws RuvioServerException {@code NOGROUP} when the stream or group does not exist
     */
    public CompletableFuture<Long> xgroupDelconsumer(K key, String group, String consumer) {
        return send(args().w(Cmd.XGROUP).s("DELCONSUMER").k(key).s(group).s(consumer), Reply.LONG);
    }

    /**
     * Sends {@code XREADGROUP GROUP group consumer STREAMS key >}: delivers entries no consumer of the group has received yet, and marks them pending until {@link #xack}. Ruvio reads exactly one stream per call. Entries added with a {@code DELAY} stay hidden until it passes.
     *
     * <pre>{@code
     * var batch = client.xreadgroup("workers", "w1", "jobs");
     * }</pre>
     *
     * @param group the group name
     * @param consumer the consumer name (created on first use)
     * @param key the stream key
     * @return a map from the stream key to the delivered entries; empty when none are available
     * @throws RuvioServerException {@code NOGROUP} when the stream or group does not exist; {@code WRONGTYPE}
     */
    public CompletableFuture<Map<K, List<StreamEntry<K, V>>>> xreadgroup(String group, String consumer, K key) {
        return send(args().w(Cmd.XREADGROUP).s("GROUP").s(group).s(consumer).w(Cmd.STREAMS).k(key).s(">"), r(READ));
    }

    /**
     * Sends {@code XREADGROUP GROUP group consumer [COUNT n] [BLOCK ms] [NOACK] STREAMS key id}. {@code BLOCK} holds the shared connection (use {@link io.github.salihcantekin.ruvio.RuvioClient#dedicated()}) and hands the next entry to the longest-waiting consumer.
     *
     * @param group the group name
     * @param consumer the consumer name
     * @param options {@code count(n)}, {@code block(ms)} ({@code 0} waits without limit) and {@code noAck()} (do not add to the pending list)
     * @param key the stream key
     * @param id {@code >} for new entries, or a stored id to re-read this consumer's own pending entries after that id
     * @return a map from the stream key to the entries; empty when none are available before the block time
     * @throws RuvioServerException {@code NOGROUP} when the stream or group does not exist; {@code ERR} for an invalid id
     */
    public CompletableFuture<Map<K, List<StreamEntry<K, V>>>> xreadgroup(String group, String consumer, XReadArgs options, K key, String id) {
        Args a = args().w(Cmd.XREADGROUP).s("GROUP").s(group).s(consumer);
        options.write(a, true);
        return send(a.w(Cmd.STREAMS).k(key).s(id), r(READ));
    }

    /**
     * Sends {@code XACK key group id [id ...]}: marks entries as processed and removes them from the pending list.
     *
     * @param key the stream key
     * @param group the group name
     * @param ids one or more entry ids
     * @return how many ids were pending and are now acknowledged
     * @throws RuvioServerException {@code ERR} for an invalid id; {@code WRONGTYPE}
     */
    public CompletableFuture<Long> xack(K key, String group, String... ids) {
        return send(args().w(Cmd.XACK).k(key).s(group).strings(ids), Reply.LONG);
    }

    /**
     * Sends {@code XPENDING key group}: summary of unacknowledged entries.
     *
     * @param key the stream key
     * @param group the group name
     * @return the count, the smallest and greatest pending id (null when none) and the per-consumer counts
     * @throws RuvioServerException {@code NOGROUP} when the stream or group does not exist
     */
    public CompletableFuture<PendingSummary> xpending(K key, String group) {
        return send(args().w(Cmd.XPENDING).k(key).s(group), PENDING_SUMMARY);
    }

    /**
     * Sends {@code XPENDING key group start end count}: lists pending entries.
     *
     * @param key the stream key
     * @param group the group name
     * @param start the first id, inclusive ({@code -} for the lowest)
     * @param end the last id, inclusive ({@code +} for the highest)
     * @param count the most rows to return
     * @return one row per pending entry: id, consumer, idle milliseconds and delivery count
     * @throws RuvioServerException {@code NOGROUP} when the stream or group does not exist
     */
    public CompletableFuture<List<PendingEntry>> xpending(K key, String group, String start, String end, long count) {
        return send(args().w(Cmd.XPENDING).k(key).s(group).s(start).s(end).l(count), PENDING_ROWS);
    }

    /**
     * Sends {@code XPENDING key group [IDLE min-idle] start end count [consumer]}.
     *
     * @param key the stream key
     * @param group the group name
     * @param minIdleMillis only entries idle at least this long, in milliseconds; zero or less omits the filter
     * @param start the first id, inclusive ({@code -} for the lowest)
     * @param end the last id, inclusive ({@code +} for the highest)
     * @param count the most rows to return
     * @param consumer only entries of this consumer; {@code null} for all
     * @return one row per matching pending entry
     * @throws RuvioServerException {@code NOGROUP} when the stream or group does not exist
     */
    public CompletableFuture<List<PendingEntry>> xpending(K key, String group, long minIdleMillis, String start, String end, long count, String consumer) {
        Args a = args().w(Cmd.XPENDING).k(key).s(group);
        if (minIdleMillis > 0) a.s("IDLE").l(minIdleMillis);
        a.s(start).s(end).l(count);
        if (consumer != null) a.s(consumer);
        return send(a, PENDING_ROWS);
    }

    /**
     * Sends {@code XAUTOCLAIM key group consumer min-idle start}: takes over pending entries that have been idle too long, for example from a crashed consumer. Claims up to 100 entries; call again with {@link AutoClaimResult#nextId()} until it is {@code 0-0}.
     *
     * @param key the stream key
     * @param group the group name
     * @param consumer the consumer that receives the entries
     * @param minIdleMillis only entries idle at least this many milliseconds
     * @param start the id to start scanning from; {@code 0-0} at first
     * @return the next start id, the claimed entries, and ids that no longer exist in the stream
     * @throws RuvioServerException {@code NOGROUP} when the stream or group does not exist
     */
    public CompletableFuture<AutoClaimResult<K, V>> xautoclaim(K key, String group, String consumer, long minIdleMillis, String start) {
        return send(args().w(Cmd.XAUTOCLAIM).k(key).s(group).s(consumer).l(minIdleMillis).s(start), r(AUTOCLAIM));
    }

    /**
     * Sends {@code XAUTOCLAIM key group consumer min-idle start [COUNT n] [JUSTID]}.
     *
     * @param key the stream key
     * @param group the group name
     * @param consumer the consumer that receives the entries
     * @param minIdleMillis only entries idle at least this many milliseconds
     * @param start the id to start scanning from; {@code 0-0} at first
     * @param count the most entries to claim; zero or less uses the server default (100)
     * @param justId {@code true} to return only ids ({@link AutoClaimResult#entries()} is then empty; use {@link AutoClaimResult#claimedIds()})
     * @return the next start id, the claimed entries or ids, and deleted ids
     * @throws RuvioServerException {@code NOGROUP} when the stream or group does not exist
     */
    public CompletableFuture<AutoClaimResult<K, V>> xautoclaim(K key, String group, String consumer, long minIdleMillis, String start, long count, boolean justId) {
        Args a = args().w(Cmd.XAUTOCLAIM).k(key).s(group).s(consumer).l(minIdleMillis).s(start);
        if (count > 0) a.w(Cmd.COUNT).l(count);
        if (justId) a.s("JUSTID");
        return send(a, r(AUTOCLAIM));
    }

    /**
     * Sends {@code XCLAIM key group consumer min-idle id [id ...]}: takes over specific pending entries that have been idle long enough.
     *
     * @param key the stream key
     * @param group the group name
     * @param consumer the consumer that receives the entries
     * @param minIdleMillis only claim entries idle at least this many milliseconds
     * @param ids the entry ids to claim
     * @return the claimed entries
     * @throws RuvioServerException {@code NOGROUP} when the stream or group does not exist; {@code ERR} for an invalid id
     */
    public CompletableFuture<List<StreamEntry<K, V>>> xclaim(K key, String group, String consumer, long minIdleMillis, String... ids) {
        return send(args().w(Cmd.XCLAIM).k(key).s(group).s(consumer).l(minIdleMillis).strings(ids), r(ENTRIES));
    }

    /**
     * Sends {@code XCLAIM key group consumer min-idle id ... [IDLE ms] [TIME ms] [RETRYCOUNT n] [FORCE] [LASTID id]}.
     *
     * @param key the stream key
     * @param group the group name
     * @param consumer the consumer that receives the entries
     * @param minIdleMillis only claim entries idle at least this many milliseconds
     * @param options {@code idle}, {@code time} (Unix ms), {@code retryCount}, {@code force} (claim even entries not in the pending list) and {@code lastId}
     * @param ids the entry ids to claim
     * @return the claimed entries
     * @throws RuvioServerException {@code NOGROUP} when the stream or group does not exist; {@code ERR} for an invalid id
     */
    public CompletableFuture<List<StreamEntry<K, V>>> xclaim(K key, String group, String consumer, long minIdleMillis, XClaimArgs options, String... ids) {
        Args a = args().w(Cmd.XCLAIM).k(key).s(group).s(consumer).l(minIdleMillis).strings(ids);
        options.write(a);
        return send(a, r(ENTRIES));
    }

    /**
     * Sends {@code XCLAIM ... JUSTID}: like {@link #xclaim(Object, String, String, long, XClaimArgs, String[])} but returns only ids and does not bump the delivery count.
     *
     * @param key the stream key
     * @param group the group name
     * @param consumer the consumer that receives the entries
     * @param minIdleMillis only claim entries idle at least this many milliseconds
     * @param options extra claim options
     * @param ids the entry ids to claim
     * @return the claimed ids
     * @throws RuvioServerException {@code NOGROUP} when the stream or group does not exist; {@code ERR} for an invalid id
     */
    public CompletableFuture<List<String>> xclaimJustid(K key, String group, String consumer, long minIdleMillis, XClaimArgs options, String... ids) {
        Args a = args().w(Cmd.XCLAIM).k(key).s(group).s(consumer).l(minIdleMillis).strings(ids);
        options.write(a);
        return send(a.s("JUSTID"), Reply.STRING_LIST);
    }

    /**
     * Sends {@code XINFO STREAM key}: general facts about a stream.
     *
     * @param key the stream key
     * @return a map of field name (such as {@code length}, {@code last-generated-id}, {@code groups}) to value; entries are nested lists of strings
     * @throws RuvioServerException {@code ERR no such key} when the stream does not exist; {@code WRONGTYPE}
     */
    public CompletableFuture<Map<String, Object>> xinfoStream(K key) {
        return send(args().w(Cmd.XINFO).s("STREAM").k(key), INFO_MAP);
    }

    /**
     * Sends {@code XINFO GROUPS key}.
     *
     * @param key the stream key
     * @return one map per group (name, consumers, pending, last-delivered-id)
     * @throws RuvioServerException {@code ERR no such key} when the stream does not exist; {@code WRONGTYPE}
     */
    public CompletableFuture<List<Map<String, Object>>> xinfoGroups(K key) {
        return send(args().w(Cmd.XINFO).s("GROUPS").k(key), INFO_LIST);
    }

    /**
     * Sends {@code XINFO CONSUMERS key group}.
     *
     * @param key the stream key
     * @param group the group name
     * @return one map per consumer (name, pending, idle)
     * @throws RuvioServerException {@code NOGROUP} when the stream or group does not exist
     */
    public CompletableFuture<List<Map<String, Object>>> xinfoConsumers(K key, String group) {
        return send(args().w(Cmd.XINFO).s("CONSUMERS").k(key).s(group), INFO_LIST);
    }
}
