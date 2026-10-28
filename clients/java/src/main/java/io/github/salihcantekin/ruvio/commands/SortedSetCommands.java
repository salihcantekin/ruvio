// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.commands;

import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.RuvioServerException;
import io.github.salihcantekin.ruvio.args.ScanArgs;
import io.github.salihcantekin.ruvio.args.ZAddArgs;
import io.github.salihcantekin.ruvio.args.ZRangeArgs;
import io.github.salihcantekin.ruvio.args.ZStoreArgs;
import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Cmd;
import io.github.salihcantekin.ruvio.internal.Reply;
import io.github.salihcantekin.ruvio.model.KeyValue;
import io.github.salihcantekin.ruvio.model.ScanResult;
import io.github.salihcantekin.ruvio.model.ScoredValue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Sorted-set commands. Score bounds are strings so that {@code -inf}, {@code +inf} and exclusive
 * {@code (1.5} bounds can be expressed; lex bounds use {@code -}, {@code +}, {@code [a}, {@code (a}.
 *
 * @param <K> key type (also hash fields and stream field names)
 * @param <V> value type (also members, list elements and stream field values)
 */
public abstract class SortedSetCommands<K, V> extends SetCommands<K, V> {
    /**
     * Creates the command family with the given codecs.
     *
     * @param keys the key codec
     * @param values the value codec
     */
    protected SortedSetCommands(RuvioCodec<K> keys, RuvioCodec<V> values) {
        super(keys, values);
    }

    static final Reply<ScanResult<ScoredValue<Object>>> SCAN_SCORED = Reply.of((tree, q) -> {
        List<Object> items = Reply.list(tree);
        List<Object> raw = Reply.list(items.get(1));
        List<ScoredValue<Object>> out = new ArrayList<>(raw.size() / 2);
        for (int i = 0; i + 1 < raw.size(); i += 2) {
            out.add(new ScoredValue<>(Reply.decodeWith(q.values, raw.get(i)), Reply.dbl(raw.get(i + 1))));
        }
        return new ScanResult<>(Reply.str(items.get(0)), out);
    });

    static final Reply<KeyValue<Object, ScoredValue<Object>>> KEY_SCORED = Reply.of((tree, q) -> {
        if (tree == null) {
            return null;
        }
        List<Object> items = Reply.list(tree);
        return new KeyValue<>(Reply.decodeWith(q.keys, items.get(0)),
                new ScoredValue<>(Reply.decodeWith(q.values, items.get(1)), Reply.dbl(items.get(2))));
    });

    /**
     * Sends {@code ZADD key score member}: adds a member with a score, or changes the score of an existing member. Members are ordered by score, then by bytes.
     *
     * <pre>{@code
     * client.zadd("leaders", 120.5, "ada");
     * }</pre>
     *
     * @param key the sorted set key
     * @param score the score (a double; {@code NaN} is refused)
     * @param member the member
     * @return {@code 1} when the member is new, {@code 0} when only its score changed or nothing changed
     * @throws RuvioServerException {@code ERR} for an invalid score; {@code WRONGTYPE} when the key holds a non-sorted-set
     */
    public CompletableFuture<Long> zadd(K key, double score, V member) {
        return send(args().w(Cmd.ZADD).k(key).d(score).v(member), Reply.LONG);
    }

    /**
     * Sends {@code ZADD key score member [score member ...]}: adds or updates several members in one command.
     *
     * @param key the sorted set key
     * @param members the members and their scores; must not be empty
     * @return how many members were new
     * @throws RuvioServerException {@code ERR} for an invalid score; {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<Long> zadd(K key, Map<V, Double> members) {
        Args a = args().w(Cmd.ZADD).k(key);
        for (Map.Entry<V, Double> e : members.entrySet()) {
            a.d(e.getValue()).v(e.getKey());
        }
        return send(a, Reply.LONG);
    }

    /**
     * Sends {@code ZADD key [NX|XX] [GT|LT] [CH] score member ...}.
     *
     * <pre>{@code
     * long newOnly = client.zadd("leaders", ZAddArgs.builder().nx(), Map.of("ada", 10.0));
     * }</pre>
     *
     * @param key the sorted set key
     * @param options {@code nx()} only add new members; {@code xx()} only update existing members; {@code gt()}/{@code lt()} only update when the new score is greater/lower (new members are still added); {@code ch()} count changed members too
     * @param members the members and their scores
     * @return without {@code ch()}: how many members were new; with {@code ch()}: how many were new or changed score
     * @throws RuvioServerException {@code ERR} for incompatible flags (for example {@code NX} with {@code XX}); {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<Long> zadd(K key, ZAddArgs options, Map<V, Double> members) {
        Args a = args().w(Cmd.ZADD).k(key);
        options.write(a);
        for (Map.Entry<V, Double> e : members.entrySet()) {
            a.d(e.getValue()).v(e.getKey());
        }
        return send(a, Reply.LONG);
    }

    /**
     * Sends {@code ZADD key INCR increment member}: adds {@code increment} to a member's score, creating it with that score when missing (same as {@link #zincrby}).
     *
     * @param key the sorted set key
     * @param increment the amount to add
     * @param member the member
     * @return the new score
     * @throws RuvioServerException {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<Double> zaddIncr(K key, double increment, V member) {
        return send(args().w(Cmd.ZADD).k(key).s("INCR").d(increment).v(member), Reply.DOUBLE);
    }

    /**
     * Sends {@code ZADD key [NX|XX] [GT|LT] INCR increment member}.
     *
     * @param key the sorted set key
     * @param options the conditions to apply
     * @param increment the amount to add
     * @param member the member
     * @return the new score, or {@code null} when a condition rejected the update
     * @throws RuvioServerException {@code ERR} for incompatible flags; {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<Double> zaddIncr(K key, ZAddArgs options, double increment, V member) {
        Args a = args().w(Cmd.ZADD).k(key);
        options.write(a);
        return send(a.s("INCR").d(increment).v(member), Reply.DOUBLE);
    }

    /**
     * Sends {@code ZRANGE key start stop}: members by rank, lowest score first.
     *
     * <pre>{@code
     * List<String> top3Lowest = client.zrange("leaders", 0, 2);
     * }</pre>
     *
     * @param key the sorted set key
     * @param start first rank, inclusive, zero based; negative counts from the highest rank
     * @param stop last rank, inclusive; negative counts from the highest rank ({@code -1} is the last)
     * @return the members in order; empty for a missing key or empty range
     * @throws RuvioServerException {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<List<V>> zrange(K key, long start, long stop) {
        return kll(Cmd.ZRANGE, key, start, stop, r(Reply.VALUE_LIST));
    }

    /**
     * Sends {@code ZRANGE key start stop WITHSCORES}: like {@link #zrange(Object, long, long)} with each score.
     *
     * @param key the sorted set key
     * @param start first rank, inclusive; negative counts from the end
     * @param stop last rank, inclusive; negative counts from the end
     * @return members with scores, lowest first
     * @throws RuvioServerException {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<List<ScoredValue<V>>> zrangeWithScores(K key, long start, long stop) {
        return send(args().w(Cmd.ZRANGE).k(key).l(start).l(stop).w(Cmd.WITHSCORES), r(Reply.SCORED));
    }

    /**
     * Sends the unified {@code ZRANGE key start stop [BYSCORE|BYLEX] [REV] [LIMIT offset count]}.
     *
     * <pre>{@code
     * List<String> mid = client.zrange("leaders", "10", "(50", ZRangeArgs.builder().byScore().limit(0, 20));
     * }</pre>
     *
     * @param key the sorted set key
     * @param start the lower bound as text: a rank, a score bound ({@code 1.5}, {@code (1.5} exclusive, {@code -inf}, {@code +inf}) with {@code byScore()}, or a lex bound ({@code -}, {@code +}, {@code [a}, {@code (a}) with {@code byLex()}
     * @param stop the upper bound in the same form
     * @param options {@code byScore()}, {@code byLex()}, {@code rev()} and {@code limit(offset, count)}; with {@code rev()} give the higher bound first
     * @return the members in range order
     * @throws RuvioServerException {@code ERR} for a bad bound or a {@code LIMIT} without {@code BYSCORE}/{@code BYLEX}; {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<List<V>> zrange(K key, String start, String stop, ZRangeArgs options) {
        Args a = args().w(Cmd.ZRANGE).k(key).s(start).s(stop);
        options.write(a);
        return send(a, r(Reply.VALUE_LIST));
    }

    /**
     * Like {@link #zrange(Object, String, String, ZRangeArgs)} with {@code WITHSCORES}.
     *
     * @param key the sorted set key
     * @param start the lower bound as text (rank, score or lex bound, depending on {@code options})
     * @param stop the upper bound as text
     * @param options range mode, order and limit
     * @return members with scores in range order
     * @throws RuvioServerException {@code ERR} for a bad bound; {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<List<ScoredValue<V>>> zrangeWithScores(K key, String start, String stop, ZRangeArgs options) {
        Args a = args().w(Cmd.ZRANGE).k(key).s(start).s(stop);
        options.write(a);
        return send(a.w(Cmd.WITHSCORES), r(Reply.SCORED));
    }

    /**
     * Sends {@code ZREVRANGE key start stop}: members by rank, highest score first.
     *
     * @param key the sorted set key
     * @param start first rank counted from the highest, inclusive; negative counts from the lowest
     * @param stop last rank, inclusive
     * @return the members, highest score first
     * @throws RuvioServerException {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<List<V>> zrevrange(K key, long start, long stop) {
        return kll(Cmd.ZREVRANGE, key, start, stop, r(Reply.VALUE_LIST));
    }

    /**
     * Sends {@code ZREVRANGE key start stop WITHSCORES}.
     *
     * @param key the sorted set key
     * @param start first rank counted from the highest, inclusive
     * @param stop last rank, inclusive
     * @return members with scores, highest first
     * @throws RuvioServerException {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<List<ScoredValue<V>>> zrevrangeWithScores(K key, long start, long stop) {
        return send(args().w(Cmd.ZREVRANGE).k(key).l(start).l(stop).w(Cmd.WITHSCORES), r(Reply.SCORED));
    }

    /**
     * Sends {@code ZRANGEBYSCORE key min max}: members whose score is in a range, lowest first.
     *
     * @param key the sorted set key
     * @param min the lower score bound: a number, {@code (5} for exclusive, or {@code -inf}
     * @param max the upper score bound: a number, {@code (5} for exclusive, or {@code +inf}
     * @return the members in score order
     * @throws RuvioServerException {@code ERR min or max is not a float}; {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<List<V>> zrangebyscore(K key, String min, String max) {
        return send(args().w(Cmd.ZRANGEBYSCORE).k(key).s(min).s(max), r(Reply.VALUE_LIST));
    }

    /**
     * Sends {@code ZRANGEBYSCORE key min max LIMIT offset count}.
     *
     * @param key the sorted set key
     * @param min the lower score bound ({@code -inf}, {@code (5}, {@code 5})
     * @param max the upper score bound ({@code +inf}, {@code (5}, {@code 5})
     * @param offset how many matches to skip; zero or greater
     * @param count the most matches to return; negative returns the rest
     * @return the members in score order
     * @throws RuvioServerException {@code ERR} for a bad bound or negative offset; {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<List<V>> zrangebyscore(K key, String min, String max, long offset, long count) {
        return send(args().w(Cmd.ZRANGEBYSCORE).k(key).s(min).s(max).w(Cmd.LIMIT).l(offset).l(count), r(Reply.VALUE_LIST));
    }

    /**
     * Sends {@code ZRANGEBYSCORE key min max WITHSCORES}.
     *
     * @param key the sorted set key
     * @param min the lower score bound ({@code -inf}, {@code (5}, {@code 5})
     * @param max the upper score bound
     * @return members with scores, lowest first
     * @throws RuvioServerException {@code ERR} for a bad bound; {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<List<ScoredValue<V>>> zrangebyscoreWithScores(K key, String min, String max) {
        return send(args().w(Cmd.ZRANGEBYSCORE).k(key).s(min).s(max).w(Cmd.WITHSCORES), r(Reply.SCORED));
    }

    /**
     * Sends {@code ZRANGEBYSCORE key min max WITHSCORES LIMIT offset count}.
     *
     * @param key the sorted set key
     * @param min the lower score bound
     * @param max the upper score bound
     * @param offset how many matches to skip
     * @param count the most matches to return; negative returns the rest
     * @return members with scores, lowest first
     * @throws RuvioServerException {@code ERR} for a bad bound or negative offset; {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<List<ScoredValue<V>>> zrangebyscoreWithScores(K key, String min, String max, long offset, long count) {
        return send(args().w(Cmd.ZRANGEBYSCORE).k(key).s(min).s(max).w(Cmd.WITHSCORES).w(Cmd.LIMIT).l(offset).l(count), r(Reply.SCORED));
    }

    /**
     * Sends {@code ZRANGEBYLEX key min max}: members in a lexicographic range. Meant for sets whose members all have the same score; the score is ignored.
     *
     * @param key the sorted set key
     * @param min the lower bound: {@code -} (lowest), {@code [a} inclusive or {@code (a} exclusive
     * @param max the upper bound: {@code +} (highest), {@code [z} inclusive or {@code (z} exclusive
     * @return the members in byte order
     * @throws RuvioServerException {@code ERR} for a bound without {@code [}, {@code (}, {@code -} or {@code +}; {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<List<V>> zrangebylex(K key, String min, String max) {
        return send(args().w(Cmd.ZRANGEBYLEX).k(key).s(min).s(max), r(Reply.VALUE_LIST));
    }

    /**
     * Sends {@code ZRANGEBYLEX key min max LIMIT offset count}.
     *
     * @param key the sorted set key
     * @param min the lower lex bound
     * @param max the upper lex bound
     * @param offset how many matches to skip
     * @param count the most matches to return; negative returns the rest
     * @return the members in byte order
     * @throws RuvioServerException {@code ERR} for a bad bound or negative offset; {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<List<V>> zrangebylex(K key, String min, String max, long offset, long count) {
        return send(args().w(Cmd.ZRANGEBYLEX).k(key).s(min).s(max).w(Cmd.LIMIT).l(offset).l(count), r(Reply.VALUE_LIST));
    }

    /**
     * Sends {@code ZREVRANGEBYLEX key max min}: like {@link #zrangebylex(Object, String, String)} but in descending order. The high bound comes first.
     *
     * @param key the sorted set key
     * @param max the upper lex bound ({@code +}, {@code [z}, {@code (z})
     * @param min the lower lex bound ({@code -}, {@code [a}, {@code (a})
     * @return the members in reverse byte order
     * @throws RuvioServerException {@code ERR} for a bad bound; {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<List<V>> zrevrangebylex(K key, String max, String min) {
        return send(args().w(Cmd.ZREVRANGEBYLEX).k(key).s(max).s(min), r(Reply.VALUE_LIST));
    }

    /**
     * Sends {@code ZREVRANGEBYLEX key max min LIMIT offset count}.
     *
     * @param key the sorted set key
     * @param max the upper lex bound
     * @param min the lower lex bound
     * @param offset how many matches to skip
     * @param count the most matches to return; negative returns the rest
     * @return the members in reverse byte order
     * @throws RuvioServerException {@code ERR} for a bad bound or negative offset; {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<List<V>> zrevrangebylex(K key, String max, String min, long offset, long count) {
        return send(args().w(Cmd.ZREVRANGEBYLEX).k(key).s(max).s(min).w(Cmd.LIMIT).l(offset).l(count), r(Reply.VALUE_LIST));
    }

    /**
     * Sends {@code ZREM key member [member ...]}: removes members. A set left empty disappears.
     *
     * @param key the sorted set key
     * @param members one or more members
     * @return how many members existed and were removed
     * @throws RuvioServerException {@code WRONGTYPE} for a non-sorted-set
     */
    @SafeVarargs
    public final CompletableFuture<Long> zrem(K key, V... members) {
        return kvs(Cmd.ZREM, key, Reply.LONG, members);
    }

    /**
     * Sends {@code ZINCRBY key increment member}: adds to a member's score, creating the member (and set) when missing.
     *
     * @param key the sorted set key
     * @param increment the amount to add (may be negative)
     * @param member the member
     * @return the new score
     * @throws RuvioServerException {@code ERR} when the result is NaN; {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<Double> zincrby(K key, double increment, V member) {
        return send(args().w(Cmd.ZINCRBY).k(key).d(increment).v(member), Reply.DOUBLE);
    }

    /**
     * Sends {@code ZINCRBY key increment member EX|PX ttl}: like {@link #zincrby(Object, double, Object)}, and a sorted set this
     * call creates gets a time to live. An existing key keeps its deadline.
     *
     * @param key the sorted set key
     * @param increment the amount to add
     * @param member the member
     * @param ttl time to live of a newly created key: whole seconds are sent as {@code EX}, anything finer as {@code PX}
     * @return the new score
     * @throws IllegalArgumentException when {@code ttl} is null, zero, negative or too large
     * @throws RuvioServerException {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<Double> zincrby(K key, double increment, V member, Duration ttl) {
        return send(args().w(Cmd.ZINCRBY).k(key).d(increment).v(member).ttl(ttl), Reply.DOUBLE);
    }

    /**
     * Sends {@code ZCARD key}.
     *
     * @param key the sorted set key
     * @return the number of members, {@code 0} for a missing key
     * @throws RuvioServerException {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<Long> zcard(K key) {
        return k(Cmd.ZCARD, key, Reply.LONG);
    }

    /**
     * Sends {@code ZSCORE key member}.
     *
     * @param key the sorted set key
     * @param member the member
     * @return the score, or {@code null} when the member or key does not exist
     * @throws RuvioServerException {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<Double> zscore(K key, V member) {
        return kv(Cmd.ZSCORE, key, member, Reply.DOUBLE);
    }

    /**
     * Sends {@code ZMSCORE key member [member ...]}.
     *
     * @param key the sorted set key
     * @param members one or more members
     * @return one score per member in order; {@code null} for a member (or key) that does not exist
     * @throws RuvioServerException {@code WRONGTYPE} for a non-sorted-set
     */
    @SafeVarargs
    public final CompletableFuture<List<Double>> zmscore(K key, V... members) {
        return kvs(Cmd.ZMSCORE, key, Reply.DOUBLE_LIST, members);
    }

    /**
     * Sends {@code ZRANK key member}: position counted from the lowest score.
     *
     * @param key the sorted set key
     * @param member the member
     * @return the zero-based rank, or {@code null} when the member or key does not exist
     * @throws RuvioServerException {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<Long> zrank(K key, V member) {
        return kv(Cmd.ZRANK, key, member, Reply.LONG);
    }

    /**
     * Sends {@code ZREVRANK key member}: position counted from the highest score.
     *
     * @param key the sorted set key
     * @param member the member
     * @return the zero-based rank from the top, or {@code null} when the member or key does not exist
     * @throws RuvioServerException {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<Long> zrevrank(K key, V member) {
        return kv(Cmd.ZREVRANK, key, member, Reply.LONG);
    }

    /**
     * Sends {@code ZPOPMIN key}: removes and returns the member with the lowest score.
     *
     * @param key the sorted set key
     * @return a list with that one member and its score; empty when the set is empty or missing
     * @throws RuvioServerException {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<List<ScoredValue<V>>> zpopmin(K key) {
        return k(Cmd.ZPOPMIN, key, r(Reply.SCORED));
    }

    /**
     * Sends {@code ZPOPMIN key count}: removes and returns up to {@code count} lowest members.
     *
     * @param key the sorted set key
     * @param count how many to remove; zero or greater
     * @return members with scores, lowest first; empty when the set is empty or missing
     * @throws RuvioServerException {@code ERR} for a negative count; {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<List<ScoredValue<V>>> zpopmin(K key, long count) {
        return kl(Cmd.ZPOPMIN, key, count, r(Reply.SCORED));
    }

    /**
     * Sends {@code ZPOPBYSCORE key max MIN min COUNT count WITHSCORES}: removes and returns the members that are due, up to {@code count} of them with a score between {@code min} and {@code max}, lowest score first. With scores set to due times this takes the jobs that are ready.
     *
     * @param key the sorted set key
     * @param min the lower score bound ({@code -inf}, {@code (5}, {@code 5})
     * @param max the upper score bound ({@code +inf}, {@code (5}, {@code 5})
     * @param count how many to remove, 1 to 10000
     * @return members with scores, lowest first; empty when nothing is due
     * @throws RuvioServerException {@code ERR} for a bad bound or a count outside 1 to 10000; {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<List<ScoredValue<V>>> zpopbyscore(K key, String min, String max, long count) {
        return send(args().w(Cmd.ZPOPBYSCORE).k(key).s(max).s("MIN").s(min).w(Cmd.COUNT).l(count).w(Cmd.WITHSCORES), r(Reply.SCORED));
    }

    /**
     * Sends {@code ZMOVEDUE source destination max COUNT count}: moves the members that are due from a sorted set to the tail of a list, up to {@code count} members with a score of at most {@code max}, lowest score first. Clients blocked on {@code destination} are served first.
     *
     * @param source the sorted set to take members from
     * @param destination the list that receives them
     * @param max the upper score bound ({@code +inf}, {@code (5}, {@code 5})
     * @param count how many to move, 1 to 10000
     * @return how many members moved
     * @throws RuvioServerException {@code ERR} for a bad bound or a count outside 1 to 10000; {@code WRONGTYPE} when {@code source} is not a sorted set or {@code destination} is not a list; {@code CROSSSLOT} in a cluster
     */
    public CompletableFuture<Long> zmovedue(K source, K destination, String max, long count) {
        return send(args().w(Cmd.ZMOVEDUE).k(source).k(destination).s(max).w(Cmd.COUNT).l(count), Reply.LONG);
    }

    /**
     * Sends {@code ZPOPMAX key}: removes and returns the member with the highest score.
     *
     * @param key the sorted set key
     * @return a list with that one member and its score; empty when the set is empty or missing
     * @throws RuvioServerException {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<List<ScoredValue<V>>> zpopmax(K key) {
        return k(Cmd.ZPOPMAX, key, r(Reply.SCORED));
    }

    /**
     * Sends {@code ZPOPMAX key count}: removes and returns up to {@code count} highest members.
     *
     * @param key the sorted set key
     * @param count how many to remove; zero or greater
     * @return members with scores, highest first; empty when the set is empty or missing
     * @throws RuvioServerException {@code ERR} for a negative count; {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<List<ScoredValue<V>>> zpopmax(K key, long count) {
        return kl(Cmd.ZPOPMAX, key, count, r(Reply.SCORED));
    }

    /**
     * Sends {@code BZPOPMIN key [key ...] timeout}: pops the lowest member of the first non-empty set, waiting for one. Holds the shared connection while it waits; use {@link io.github.salihcantekin.ruvio.RuvioClient#dedicated()}. At most 1,024 clients can wait on one shard.
     *
     * @param timeoutSeconds seconds to wait; {@code 0} waits until a member arrives
     * @param keys one or more sorted set keys (same slot in a cluster)
     * @return the key, member and score, or {@code null} on timeout
     * @throws RuvioServerException {@code ERR max number of blocking clients reached}; {@code WRONGTYPE} for a non-sorted-set
     */
    @SafeVarargs
    public final CompletableFuture<KeyValue<K, ScoredValue<V>>> bzpopmin(double timeoutSeconds, K... keys) {
        return send(args().w(Cmd.BZPOPMIN).keys(keys).d(timeoutSeconds), r(KEY_SCORED));
    }

    /**
     * Sends {@code BZPOPMAX key [key ...] timeout}: like {@link #bzpopmin(double, Object[])} for the highest member.
     *
     * @param timeoutSeconds seconds to wait; {@code 0} waits until a member arrives
     * @param keys one or more sorted set keys (same slot in a cluster)
     * @return the key, member and score, or {@code null} on timeout
     * @throws RuvioServerException {@code ERR max number of blocking clients reached}; {@code WRONGTYPE} for a non-sorted-set
     */
    @SafeVarargs
    public final CompletableFuture<KeyValue<K, ScoredValue<V>>> bzpopmax(double timeoutSeconds, K... keys) {
        return send(args().w(Cmd.BZPOPMAX).keys(keys).d(timeoutSeconds), r(KEY_SCORED));
    }

    /**
     * Sends {@code ZREMRANGEBYRANK key start stop}: removes members by rank.
     *
     * @param key the sorted set key
     * @param start first rank, inclusive; negative counts from the end
     * @param stop last rank, inclusive
     * @return how many members were removed
     * @throws RuvioServerException {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<Long> zremrangebyrank(K key, long start, long stop) {
        return kll(Cmd.ZREMRANGEBYRANK, key, start, stop, Reply.LONG);
    }

    /**
     * Sends {@code ZREMRANGEBYSCORE key min max}: removes members in a score range.
     *
     * @param key the sorted set key
     * @param min the lower score bound ({@code -inf}, {@code (5}, {@code 5})
     * @param max the upper score bound ({@code +inf}, {@code (5}, {@code 5})
     * @return how many members were removed
     * @throws RuvioServerException {@code ERR} for a bad bound; {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<Long> zremrangebyscore(K key, String min, String max) {
        return send(args().w(Cmd.ZREMRANGEBYSCORE).k(key).s(min).s(max), Reply.LONG);
    }

    /**
     * Sends {@code ZREMRANGEBYLEX key min max}: removes members in a lexicographic range.
     *
     * @param key the sorted set key
     * @param min the lower lex bound ({@code -}, {@code [a}, {@code (a})
     * @param max the upper lex bound ({@code +}, {@code [z}, {@code (z})
     * @return how many members were removed
     * @throws RuvioServerException {@code ERR} for a bad bound; {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<Long> zremrangebylex(K key, String min, String max) {
        return send(args().w(Cmd.ZREMRANGEBYLEX).k(key).s(min).s(max), Reply.LONG);
    }

    /**
     * Sends {@code ZCOUNT key min max}: counts members in a score range.
     *
     * @param key the sorted set key
     * @param min the lower score bound ({@code -inf}, {@code (5}, {@code 5})
     * @param max the upper score bound ({@code +inf}, {@code (5}, {@code 5})
     * @return the number of members in range, {@code 0} for a missing key
     * @throws RuvioServerException {@code ERR} for a bad bound; {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<Long> zcount(K key, String min, String max) {
        return send(args().w(Cmd.ZCOUNT).k(key).s(min).s(max), Reply.LONG);
    }

    /**
     * Sends {@code ZLEXCOUNT key min max}: counts members in a lexicographic range.
     *
     * @param key the sorted set key
     * @param min the lower lex bound ({@code -}, {@code [a}, {@code (a})
     * @param max the upper lex bound ({@code +}, {@code [z}, {@code (z})
     * @return the number of members in range, {@code 0} for a missing key
     * @throws RuvioServerException {@code ERR} for a bad bound; {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<Long> zlexcount(K key, String min, String max) {
        return send(args().w(Cmd.ZLEXCOUNT).k(key).s(min).s(max), Reply.LONG);
    }

    /**
     * Sends {@code ZSCAN key cursor}: walks a sorted set a page at a time, in score order. Start with {@code "0"} and continue with {@link ScanResult#cursor()} until {@link ScanResult#isFinished()}. A write between pages can shift members between pages. See {@link io.github.salihcantekin.ruvio.RuvioClient#zscanIterable}.
     *
     * @param key the sorted set key
     * @param cursor {@code "0"} to start, otherwise the previous page's cursor
     * @return the next cursor and the members with scores of this page
     * @throws RuvioServerException {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<ScanResult<ScoredValue<V>>> zscan(K key, String cursor) {
        return send(args().w(Cmd.ZSCAN).k(key).s(cursor), r(SCAN_SCORED));
    }

    /**
     * Sends {@code ZSCAN key cursor [MATCH pattern] [COUNT n]}.
     *
     * @param key the sorted set key
     * @param cursor {@code "0"} to start, otherwise the previous page's cursor
     * @param options {@code MATCH} glob on members and {@code COUNT} hint
     * @return the next cursor and the matching members of this page
     * @throws RuvioServerException {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<ScanResult<ScoredValue<V>>> zscan(K key, String cursor, ScanArgs options) {
        Args a = args().w(Cmd.ZSCAN).k(key).s(cursor);
        options.write(a, false);
        return send(a, r(SCAN_SCORED));
    }

    // ---- set algebra -----------------------------------------------------------------------

    private Args algebra(byte[] cmd, K destination, ZStoreArgs options, K[] keys) {
        Args a = args().w(cmd);
        if (destination != null) {
            a.k(destination);
        }
        a.l(keys.length).keys(keys);
        if (options != null) {
            options.write(a);
        }
        return a;
    }

    /**
     * Sends {@code ZUNIONSTORE destination numkeys key [key ...]}: stores the union of sorted sets, adding the scores of members present in several. All keys must hash to one slot in a cluster.
     *
     * @param destination the key that receives the result (overwritten)
     * @param keys one or more source sorted sets
     * @return the number of members in the stored result
     * @throws RuvioServerException {@code WRONGTYPE} for a non-sorted-set source; {@code CROSSSLOT} in a cluster
     */
    @SafeVarargs
    public final CompletableFuture<Long> zunionstore(K destination, K... keys) {
        return send(algebra(Cmd.ZUNIONSTORE, destination, null, keys), Reply.LONG);
    }

    /**
     * Sends {@code ZUNIONSTORE destination numkeys key ... [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX]}.
     *
     * @param destination the key that receives the result (overwritten)
     * @param options per-source weights (one per key, multiplied into the scores) and how to combine duplicates
     * @param keys one or more source sorted sets
     * @return the number of members in the stored result
     * @throws RuvioServerException {@code ERR} when the weight count differs from the key count; {@code WRONGTYPE}; {@code CROSSSLOT} in a cluster
     */
    @SafeVarargs
    public final CompletableFuture<Long> zunionstore(K destination, ZStoreArgs options, K... keys) {
        return send(algebra(Cmd.ZUNIONSTORE, destination, options, keys), Reply.LONG);
    }

    /**
     * Sends {@code ZINTERSTORE destination numkeys key [key ...]}: stores members present in every source, adding their scores. All keys must hash to one slot in a cluster.
     *
     * @param destination the key that receives the result (overwritten)
     * @param keys one or more source sorted sets
     * @return the number of members in the stored result
     * @throws RuvioServerException {@code WRONGTYPE} for a non-sorted-set source; {@code CROSSSLOT} in a cluster
     */
    @SafeVarargs
    public final CompletableFuture<Long> zinterstore(K destination, K... keys) {
        return send(algebra(Cmd.ZINTERSTORE, destination, null, keys), Reply.LONG);
    }

    /**
     * Sends {@code ZINTERSTORE destination numkeys key ... [WEIGHTS ...] [AGGREGATE ...]}.
     *
     * @param destination the key that receives the result (overwritten)
     * @param options per-source weights and the aggregate function
     * @param keys one or more source sorted sets
     * @return the number of members in the stored result
     * @throws RuvioServerException {@code ERR} when the weight count differs from the key count; {@code WRONGTYPE}; {@code CROSSSLOT} in a cluster
     */
    @SafeVarargs
    public final CompletableFuture<Long> zinterstore(K destination, ZStoreArgs options, K... keys) {
        return send(algebra(Cmd.ZINTERSTORE, destination, options, keys), Reply.LONG);
    }

    /**
     * Sends {@code ZDIFFSTORE destination numkeys key [key ...]}: stores the members of the first set that are in none of the others.
     *
     * @param destination the key that receives the result (overwritten)
     * @param keys the first sorted set followed by the sets to subtract
     * @return the number of members in the stored result
     * @throws RuvioServerException {@code WRONGTYPE}; {@code CROSSSLOT} in a cluster
     */
    @SafeVarargs
    public final CompletableFuture<Long> zdiffstore(K destination, K... keys) {
        return send(algebra(Cmd.ZDIFFSTORE, destination, null, keys), Reply.LONG);
    }

    /**
     * Sends {@code ZUNION numkeys key [key ...]}: the union of sorted sets, without storing it.
     *
     * @param keys one or more sorted set keys (same slot in a cluster)
     * @return the members, ordered by combined score
     * @throws RuvioServerException {@code WRONGTYPE}; {@code CROSSSLOT} in a cluster
     */
    @SafeVarargs
    public final CompletableFuture<List<V>> zunion(K... keys) {
        return send(algebra(Cmd.ZUNION, null, null, keys), r(Reply.VALUE_LIST));
    }

    /**
     * Sends {@code ZUNION numkeys key ... [WEIGHTS ...] [AGGREGATE ...]}.
     *
     * @param options per-source weights and the aggregate function
     * @param keys one or more sorted set keys (same slot in a cluster)
     * @return the members, ordered by combined score
     * @throws RuvioServerException {@code ERR} when the weight count differs from the key count; {@code WRONGTYPE}; {@code CROSSSLOT} in a cluster
     */
    @SafeVarargs
    public final CompletableFuture<List<V>> zunion(ZStoreArgs options, K... keys) {
        return send(algebra(Cmd.ZUNION, null, options, keys), r(Reply.VALUE_LIST));
    }

    /**
     * Sends {@code ZUNION numkeys key ... [WEIGHTS ...] [AGGREGATE ...] WITHSCORES}.
     *
     * @param options per-source weights and the aggregate function
     * @param keys one or more sorted set keys (same slot in a cluster)
     * @return the members with their combined scores
     * @throws RuvioServerException {@code ERR} when the weight count differs from the key count; {@code WRONGTYPE}; {@code CROSSSLOT} in a cluster
     */
    @SafeVarargs
    public final CompletableFuture<List<ScoredValue<V>>> zunionWithScores(ZStoreArgs options, K... keys) {
        return send(algebra(Cmd.ZUNION, null, options, keys).w(Cmd.WITHSCORES), r(Reply.SCORED));
    }

    /**
     * Sends {@code ZINTER numkeys key [key ...]}: members present in every sorted set, without storing.
     *
     * @param keys one or more sorted set keys (same slot in a cluster)
     * @return the common members, ordered by combined score
     * @throws RuvioServerException {@code WRONGTYPE}; {@code CROSSSLOT} in a cluster
     */
    @SafeVarargs
    public final CompletableFuture<List<V>> zinter(K... keys) {
        return send(algebra(Cmd.ZINTER, null, null, keys), r(Reply.VALUE_LIST));
    }

    /**
     * Sends {@code ZINTER numkeys key ... [WEIGHTS ...] [AGGREGATE ...]}.
     *
     * @param options per-source weights and the aggregate function
     * @param keys one or more sorted set keys (same slot in a cluster)
     * @return the common members, ordered by combined score
     * @throws RuvioServerException {@code ERR} when the weight count differs from the key count; {@code WRONGTYPE}; {@code CROSSSLOT} in a cluster
     */
    @SafeVarargs
    public final CompletableFuture<List<V>> zinter(ZStoreArgs options, K... keys) {
        return send(algebra(Cmd.ZINTER, null, options, keys), r(Reply.VALUE_LIST));
    }

    /**
     * Sends {@code ZINTER numkeys key ... [WEIGHTS ...] [AGGREGATE ...] WITHSCORES}.
     *
     * @param options per-source weights and the aggregate function
     * @param keys one or more sorted set keys (same slot in a cluster)
     * @return the common members with their combined scores
     * @throws RuvioServerException {@code ERR} when the weight count differs from the key count; {@code WRONGTYPE}; {@code CROSSSLOT} in a cluster
     */
    @SafeVarargs
    public final CompletableFuture<List<ScoredValue<V>>> zinterWithScores(ZStoreArgs options, K... keys) {
        return send(algebra(Cmd.ZINTER, null, options, keys).w(Cmd.WITHSCORES), r(Reply.SCORED));
    }

    /**
     * Sends {@code ZDIFF numkeys key [key ...]}: members of the first sorted set that are in none of the others.
     *
     * @param keys the first sorted set followed by the sets to subtract (same slot in a cluster)
     * @return the remaining members
     * @throws RuvioServerException {@code WRONGTYPE}; {@code CROSSSLOT} in a cluster
     */
    @SafeVarargs
    public final CompletableFuture<List<V>> zdiff(K... keys) {
        return send(algebra(Cmd.ZDIFF, null, null, keys), r(Reply.VALUE_LIST));
    }

    /**
     * Sends {@code ZDIFF numkeys key [key ...] WITHSCORES}.
     *
     * @param keys the first sorted set followed by the sets to subtract (same slot in a cluster)
     * @return the remaining members with the scores they have in the first set
     * @throws RuvioServerException {@code WRONGTYPE}; {@code CROSSSLOT} in a cluster
     */
    @SafeVarargs
    public final CompletableFuture<List<ScoredValue<V>>> zdiffWithScores(K... keys) {
        return send(algebra(Cmd.ZDIFF, null, null, keys).w(Cmd.WITHSCORES), r(Reply.SCORED));
    }

    /**
     * Sends {@code ZRANDMEMBER key}: returns a random member without removing it.
     *
     * @param key the sorted set key
     * @return a member, or {@code null} when the set is empty or missing
     * @throws RuvioServerException {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<V> zrandmember(K key) {
        return k(Cmd.ZRANDMEMBER, key, r(Reply.VALUE));
    }

    /**
     * Sends {@code ZRANDMEMBER key count}.
     *
     * @param key the sorted set key
     * @param count positive: up to that many distinct members; negative: {@code -count} members that may repeat (at most 1,048,576)
     * @return the members, empty when the set is empty or missing
     * @throws RuvioServerException {@code ERR count is out of range}; {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<List<V>> zrandmember(K key, long count) {
        return kl(Cmd.ZRANDMEMBER, key, count, r(Reply.VALUE_LIST));
    }

    /**
     * Sends {@code ZRANDMEMBER key count WITHSCORES}.
     *
     * @param key the sorted set key
     * @param count positive: up to that many distinct members; negative: members may repeat
     * @return the members with scores, empty when the set is empty or missing
     * @throws RuvioServerException {@code ERR count is out of range}; {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<List<ScoredValue<V>>> zrandmemberWithScores(K key, long count) {
        return send(args().w(Cmd.ZRANDMEMBER).k(key).l(count).w(Cmd.WITHSCORES), r(Reply.SCORED));
    }
}
