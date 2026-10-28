// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.commands;

import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.RuvioException;
import io.github.salihcantekin.ruvio.RuvioServerException;
import io.github.salihcantekin.ruvio.args.ExpireCondition;
import io.github.salihcantekin.ruvio.args.KeyType;
import io.github.salihcantekin.ruvio.args.RestoreArgs;
import io.github.salihcantekin.ruvio.args.ScanArgs;
import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Cmd;
import io.github.salihcantekin.ruvio.internal.Reply;
import io.github.salihcantekin.ruvio.model.ChangeNote;
import io.github.salihcantekin.ruvio.model.IdemResult;
import io.github.salihcantekin.ruvio.model.LimitOutcome;
import io.github.salihcantekin.ruvio.model.ScanResult;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Generic key commands plus the Ruvio coordination primitives (LIMIT, IDEM, ONCE, LEASE, SEMAPHORE, CHANGES).
 *
 * @param <K> key type (also hash fields and stream field names)
 * @param <V> value type (also members, list elements and stream field values)
 */
public abstract class KeyCommands<K, V> extends StringCommands<K, V> {
    /**
     * Creates the command family with the given codecs.
     *
     * @param keys the key codec
     * @param values the value codec
     */
    protected KeyCommands(RuvioCodec<K> keys, RuvioCodec<V> values) {
        super(keys, values);
    }

    static final Reply<IdemResult> IDEM = Reply.of((tree, q) -> {
        List<Object> items = Reply.list(tree);
        String state = Reply.str(items.get(0));
        byte[] result = items.size() > 1 && items.get(1) instanceof byte[] ? (byte[]) items.get(1) : null;
        return new IdemResult(IdemResult.State.valueOf(state.toUpperCase(java.util.Locale.ROOT)), result);
    });

    static final Reply<List<ChangeNote>> CHANGES = Reply.of((tree, q) -> {
        List<Object> rows = Reply.list(tree);
        List<ChangeNote> out = new ArrayList<>(rows.size());
        for (Object row : rows) {
            List<Object> parts = Reply.list(row);
            out.add(new ChangeNote(Reply.lng(parts.get(0)), parts.size() > 1 ? Reply.str(parts.get(1)) : ""));
        }
        return out;
    });

    static final Reply<ScanResult<Object>> SCAN_KEYS = Reply.of((tree, q) -> {
        List<Object> items = Reply.list(tree);
        List<Object> raw = Reply.list(items.get(1));
        List<Object> out = new ArrayList<>(raw.size());
        for (Object o : raw) {
            out.add(Reply.decodeWith(q.keys, o));
        }
        return new ScanResult<>(Reply.str(items.get(0)), out);
    });

    // ---- Ruvio coordination ----------------------------------------------------------------

    /**
     * Sends the Ruvio command {@code LIMIT key max window_ms}: a fixed-window rate limiter kept on the server.
     * The first call starts a window of {@code windowMillis}; each call inside the window uses one of
     * {@code max} permits. When the permits are gone, calls are rejected until the window ends. Rejected
     * calls are not counted. For a result object see {@link io.github.salihcantekin.ruvio.RuvioClient#rateLimit}.
     *
     * <pre>{@code
     * Long left = client.limit("rl:user:1", 100, 60_000); // null when 100 calls were already made this minute
     * }</pre>
     *
     * @param key the limiter key (one key per subject, for example per user or IP)
     * @param max permits per window; must be greater than zero
     * @param windowMillis window length in milliseconds; must be greater than zero
     * @return the permits left after this call ({@code max - 1} on the first), or {@code null} when the window is full
     * @throws RuvioServerException {@code ERR} when {@code max} or {@code windowMillis} is not positive; {@code WRONGTYPE} when the key holds another type
     */
    public CompletableFuture<Long> limit(K key, long max, long windowMillis) {
        return kll(Cmd.LIMIT, key, max, windowMillis, Reply.LONG);
    }

    /**
     * Sends the Ruvio command {@code LIMIT key max window_ms SLIDING}: a sliding-window rate limiter.
     * The previous window counts for the part of it that still overlaps the last {@code windowMillis}, so a burst
     * at a window edge is not doubled. The key keeps two counters, not one entry per call. A key keeps the mode it
     * was created with.
     *
     * <pre>{@code
     * Long left = client.limitSliding("rl:user:1", 100, 60_000); // null when the call does not fit
     * }</pre>
     *
     * @param key the limiter key
     * @param max calls per window; from 1 to 2147483647
     * @param windowMillis window length in milliseconds; must be greater than zero
     * @return the calls left after this one, or {@code null} when it does not fit (nothing is counted)
     * @throws RuvioServerException {@code ERR} when an argument is out of range or the key was created by another {@code LIMIT} mode; {@code WRONGTYPE} when the key holds another type
     */
    public CompletableFuture<Long> limitSliding(K key, long max, long windowMillis) {
        return send(args().w(Cmd.LIMIT).k(key).l(max).l(windowMillis).w(Cmd.SLIDING), Reply.LONG);
    }

    /**
     * Sends the Ruvio command {@code LIMIT key capacity refill_ms TOKENS COST cost}: a token bucket.
     * The bucket holds {@code capacity} tokens and refills completely over {@code refillMillis}; a call takes
     * {@code cost} tokens. The key keeps one timestamp and disappears when the bucket is full again.
     *
     * <pre>{@code
     * Long left = client.limitTokens("rl:api:7", 10, 1_000, 2); // 10 tokens per second, this call takes 2
     * }</pre>
     *
     * @param key the limiter key
     * @param capacity bucket size; from 1 to 1000000
     * @param refillMillis time to refill an empty bucket in milliseconds; up to 4294967295
     * @param cost tokens this call takes; from 1 to {@code capacity}
     * @return the tokens left after this call, or {@code null} when the bucket holds too few (nothing is taken)
     * @throws RuvioServerException {@code ERR} when an argument is out of range or the key was created by another {@code LIMIT} mode; {@code WRONGTYPE} when the key holds another type
     */
    public CompletableFuture<Long> limitTokens(K key, long capacity, long refillMillis, long cost) {
        return send(args().w(Cmd.LIMIT).k(key).l(capacity).l(refillMillis).w(Cmd.TOKENS).w(Cmd.COST).l(cost), Reply.LONG);
    }

    /**
     * Sends {@code LIMIT key max window_ms SLIDING WITHRETRY}: {@link #limitSliding} that also says how long to wait.
     *
     * @param key the limiter key
     * @param max calls per window; from 1 to 2147483647
     * @param windowMillis window length in milliseconds; must be greater than zero
     * @return the outcome; {@code retryAfterMillis} is the wait until the same call would pass
     * @throws RuvioServerException as {@link #limitSliding}
     */
    public CompletableFuture<LimitOutcome> limitSlidingWithRetry(K key, long max, long windowMillis) {
        return send(args().w(Cmd.LIMIT).k(key).l(max).l(windowMillis).w(Cmd.SLIDING).w(Cmd.WITHRETRY), Reply.LONG_LIST)
                .thenApply(KeyCommands::limitOutcome);
    }

    /**
     * Sends {@code LIMIT key capacity refill_ms TOKENS COST cost WITHRETRY}: {@link #limitTokens} that also says how long to wait.
     *
     * @param key the limiter key
     * @param capacity bucket size; from 1 to 1000000
     * @param refillMillis time to refill an empty bucket in milliseconds; up to 4294967295
     * @param cost tokens this call takes; from 1 to {@code capacity}
     * @return the outcome; {@code retryAfterMillis} is the wait until the same call would pass
     * @throws RuvioServerException as {@link #limitTokens}
     */
    public CompletableFuture<LimitOutcome> limitTokensWithRetry(K key, long capacity, long refillMillis, long cost) {
        return send(args().w(Cmd.LIMIT).k(key).l(capacity).l(refillMillis).w(Cmd.TOKENS).w(Cmd.COST).l(cost).w(Cmd.WITHRETRY),
                Reply.LONG_LIST).thenApply(KeyCommands::limitOutcome);
    }

    private static LimitOutcome limitOutcome(List<Long> reply) {
        if (reply == null || reply.size() != 2) {
            throw new RuvioException("LIMIT WITHRETRY expects two integers, got " + reply);
        }
        long remaining = reply.get(0);
        return new LimitOutcome(remaining >= 0, Math.max(remaining, 0), Math.max(reply.get(1), 0));
    }

    /**
     * Sends the Ruvio command {@code IDEM key BEGIN fingerprint owner ttl_ms}: reserves a request so it runs once.
     * The server only compares the tokens; it does not hash the request or check who the owner is. Use a digest of
     * every input that defines the request as the fingerprint, and an unguessable per-attempt value as the owner.
     * Reserve the key exclusively for this command: ordinary writes such as {@code SET} or {@code DEL} break its guarantees.
     *
     * <p>Only the {@code ACQUIRED} case writes. Other cases leave the record and its deadline unchanged, so a
     * longer TTL on a retry does not extend it. This is not exactly-once for external side effects: a caller can
     * crash after acting and before {@link #idemComplete(Object, String, String, byte[])}.
     *
     * <pre>{@code
     * IdemResult r = client.idemBegin("order:42", digest, attemptToken, 60_000);
     * switch (r.state()) {
     *     case ACQUIRED -> { run(); client.idemComplete("order:42", digest, attemptToken, "saved"); }
     *     case COMPLETED -> reuse(r.resultString());
     *     case PENDING -> retryLater();
     *     case CONFLICT -> reject(); // same key, different fingerprint
     * }
     * }</pre>
     *
     * @param key the idempotency key; scope it to the tenant and operation
     * @param fingerprint opaque request digest, 1 to 128 bytes
     * @param owner opaque per-attempt token, 1 to 128 bytes
     * @param ttlMillis how long the record is kept, from 1 to 604800000 (7 days)
     * @return {@code ACQUIRED} (you own a new pending record), {@code PENDING} (same fingerprint, not completed yet),
     * {@code CONFLICT} (live record has another fingerprint) or {@code COMPLETED} (same fingerprint; the saved result is in the reply)
     * @throws RuvioServerException {@code ERR} for tokens outside 1-128 bytes, a TTL out of range, or a key that does not hold an IDEM record; {@code WRONGTYPE} for another type
     * @see io.github.salihcantekin.ruvio.RuvioClient#idempotent
     */
    public CompletableFuture<IdemResult> idemBegin(K key, String fingerprint, String owner, long ttlMillis) {
        return send(args().w(Cmd.IDEM).k(key).w(Cmd.BEGIN).s(fingerprint).s(owner).l(ttlMillis), IDEM);
    }

    /**
     * Sends {@code IDEM key COMPLETE fingerprint owner result}: publishes the outcome of an attempt that got
     * {@code ACQUIRED} from {@link #idemBegin}. Retention is not extended. The result is binary safe and at
     * most 4 MiB (also limited by the server's value size limit).
     *
     * <pre>{@code
     * long ok = client.idemComplete("order:42", digest, attemptToken, resultBytes); // 1
     * }</pre>
     *
     * @param key the idempotency key
     * @param fingerprint the same fingerprint used in {@code idemBegin}
     * @param owner the same owner token used in {@code idemBegin}
     * @param result the bytes later callers will replay
     * @return {@code 1} when the result was stored (also when the identical result was already stored), {@code 0} when the
     * record is missing or expired, the owner or fingerprint differ, or a different result is already stored
     * @throws RuvioServerException {@code ERR} for invalid tokens or a result over the limit
     */
    public CompletableFuture<Long> idemComplete(K key, String fingerprint, String owner, byte[] result) {
        return send(args().w(Cmd.IDEM).k(key).w(Cmd.COMPLETE).s(fingerprint).s(owner).b(result), Reply.LONG);
    }

    /**
     * Sends {@code IDEM key ABORT fingerprint owner}: gives up the pending record that this attempt got from
     * {@link #idemBegin}, so the next {@code BEGIN} acquires the key at once. A completed record is never
     * removed. Abort only when the work did not take effect or is safe to run again; a retry after an abort
     * runs it again.
     *
     * <pre>{@code
     * boolean freed = client.idemAbort("order:42", digest, attemptToken); // true
     * }</pre>
     *
     * @param key the idempotency key
     * @param fingerprint the same fingerprint used in {@code idemBegin}
     * @param owner the same owner token used in {@code idemBegin}
     * @return {@code true} when the pending record of this owner and fingerprint was removed; {@code false} when
     * nothing changed (the key is missing or expired, the record is completed, or another owner or fingerprint holds it)
     * @throws RuvioServerException {@code ERR} for invalid tokens; {@code INVALID} or {@code WRONGTYPE} for a key
     * that holds no IDEM record
     */
    public CompletableFuture<Boolean> idemAbort(K key, String fingerprint, String owner) {
        return send(args().w(Cmd.IDEM).k(key).w(Cmd.ABORT).s(fingerprint).s(owner), Reply.BOOL);
    }

    /**
     * Like {@link #idemComplete(Object, String, String, byte[])} with the result given as text (sent as UTF-8).
     *
     * @param key the idempotency key
     * @param fingerprint the same fingerprint used in {@code idemBegin}
     * @param owner the same owner token used in {@code idemBegin}
     * @param result the text later callers will replay
     * @return {@code 1} when stored, {@code 0} when the record is missing, expired, owned by another attempt, or already completed with a different result
     */
    public CompletableFuture<Long> idemComplete(K key, String fingerprint, String owner, String result) {
        return send(args().w(Cmd.IDEM).k(key).w(Cmd.COMPLETE).s(fingerprint).s(owner).s(result), Reply.LONG);
    }

    /**
     * Sends the Ruvio command {@code ONCE key ttl_ms}: a "first one wins" marker. Use it to run a job or send a notice once per period.
     *
     * <pre>{@code
     * if (client.once("welcome-mail:user1", 86_400_000)) {
     *     sendWelcomeMail();
     * }
     * }</pre>
     *
     * @param key the marker key
     * @param ttlMillis how long the marker lives, in milliseconds; must be greater than zero
     * @return {@code true} for the first call (the marker is created), {@code false} until the marker expires
     * @throws RuvioServerException {@code ERR} when {@code ttlMillis} is not positive; {@code WRONGTYPE} when the key holds another type
     */
    public CompletableFuture<Boolean> once(K key, long ttlMillis) {
        return kl(Cmd.ONCE, key, ttlMillis, Reply.BOOL);
    }

    /**
     * Sends the Ruvio command {@code LEASE key ttl_ms}: takes an exclusive, time-limited hold on a key. Pass the
     * token to {@link #release(Object, long)} to give it up early. See also
     * {@link io.github.salihcantekin.ruvio.RuvioClient#tryLease}, which wraps the token in an object.
     *
     * <pre>{@code
     * Long token = client.lease("job:nightly", 30_000);
     * if (token != null) {
     *     try { runJob(); } finally { client.release("job:nightly", token); }
     * }
     * }</pre>
     *
     * @param key the lease key
     * @param ttlMillis how long the lease is held, in milliseconds; must be greater than zero
     * @return the holder token, or {@code null} while another holder's lease is still live
     * @throws RuvioServerException {@code ERR} when {@code ttlMillis} is not positive; {@code WRONGTYPE} when the key holds another type
     */
    public CompletableFuture<Long> lease(K key, long ttlMillis) {
        return kl(Cmd.LEASE, key, ttlMillis, Reply.LONG);
    }

    /**
     * Sends the Ruvio command {@code SEMAPHORE key max ttl_ms}: takes one of at most {@code max} slots. Each
     * holder has its own TTL; when the last holder expires or is released, the key goes away. Give the slot back with
     * {@link #release(Object, long)}. See also {@link io.github.salihcantekin.ruvio.RuvioClient#tryAcquirePermit}.
     *
     * <pre>{@code
     * Long permit = client.semaphore("pool:render", 4, 60_000);
     * if (permit != null) {
     *     try { render(); } finally { client.release("pool:render", permit); }
     * }
     * }</pre>
     *
     * @param key the semaphore key
     * @param max the number of slots; must be greater than zero
     * @param ttlMillis how long this holder's slot lasts, in milliseconds; must be greater than zero
     * @return the holder token, or {@code null} when all {@code max} slots are held
     * @throws RuvioServerException {@code ERR} when {@code max} or {@code ttlMillis} is not positive; {@code WRONGTYPE} when the key holds another type
     */
    public CompletableFuture<Long> semaphore(K key, long max, long ttlMillis) {
        return kll(Cmd.SEMAPHORE, key, max, ttlMillis, Reply.LONG);
    }

    /**
     * Sends the Ruvio command {@code RENEW key token ttl}: moves the deadline of a lease or semaphore slot you hold to
     * now + {@code ttlMillis}. The fencing token does not change and no holder is created.
     *
     * @param key the lease or semaphore key
     * @param token the token returned by {@link #lease(Object, long)} or {@link #semaphore(Object, long, long)}
     * @param ttlMillis the new lifetime in milliseconds (positive); it replaces the remaining time
     * @return {@code true} when the token was live and its deadline moved, {@code false} when the key is missing or the
     * token is stale or does not match
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds something other than a lease or semaphore
     */
    public CompletableFuture<Boolean> renew(K key, long token, long ttlMillis) {
        return kll(Cmd.RENEW, key, token, ttlMillis, Reply.BOOL);
    }

    /**
     * Sends the Ruvio command {@code RELEASE key token}: gives up a lease or one semaphore slot.
     *
     * @param key the lease or semaphore key
     * @param token the token returned by {@link #lease(Object, long)} or {@link #semaphore(Object, long, long)}
     * @return {@code true} when the token was live and the hold was released, {@code false} when the key is missing or the
     * token is stale or does not match
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds something other than a lease or semaphore
     */
    public CompletableFuture<Boolean> release(K key, long token) {
        return kl(Cmd.RELEASE, key, token, Reply.BOOL);
    }

    /**
     * Sends the Ruvio command {@code CHANGES START}: turns on the small in-memory change feed of the shard the
     * command reaches, and returns the cursor to pass to {@link #changes(long)}. Nothing is recorded before this
     * call, the feed is empty after a restart, and only the last 1024 notes are kept. For a durable, resumable stream
     * use the server's {@code CHANGES SUBSCRIBE}.
     *
     * @return the current cursor; notes created after it have a higher sequence
     */
    public CompletableFuture<Long> changesStart() {
        return c00(Cmd.CHANGES, Cmd.START, Reply.LONG);
    }

    /**
     * Sends {@code CHANGES cursor}: reads notes of the in-memory feed that have a sequence above {@code cursor}.
     * Each note is text like {@code SET key}, {@code LIMIT key} or {@code RELEASE key}. Call {@link #changesStart()} first.
     *
     * <pre>{@code
     * long cursor = client.changesStart();
     * // ... writes happen ...
     * for (ChangeNote n : client.changes(cursor)) {
     *     cursor = n.sequence();
     *     System.out.println(n.text());
     * }
     * }</pre>
     *
     * @param cursor the last sequence already seen (from {@code changesStart} or {@link ChangeNote#sequence()})
     * @return the newer notes, oldest first; empty when none (or when the feed was never started)
     */
    public CompletableFuture<List<ChangeNote>> changes(long cursor) {
        return send(args().w(Cmd.CHANGES).l(cursor), CHANGES);
    }

    // ---- keys ------------------------------------------------------------------------------

    /**
     * Sends {@code DEL key [key ...]}: removes keys of any type.
     * In a cluster, keys in different slots are sent per slot and the counts added up; that is not atomic.
     *
     * <pre>{@code
     * long removed = client.del("a", "b");
     * }</pre>
     *
     * @param keys one or more keys
     * @return how many keys existed and were removed
     * @throws IllegalArgumentException when no key is given
     */
    @SafeVarargs
    public final CompletableFuture<Long> del(K... keys) {
        return keys(Cmd.DEL, Reply.LONG, keys);
    }

    /**
     * Sends {@code UNLINK key [key ...]}: same as {@link #del(Object[])}; Ruvio removes the keys synchronously.
     *
     * @param keys one or more keys
     * @return how many keys existed and were removed
     * @throws IllegalArgumentException when no key is given
     */
    @SafeVarargs
    public final CompletableFuture<Long> unlink(K... keys) {
        return keys(Cmd.UNLINK, Reply.LONG, keys);
    }

    /**
     * Sends {@code EXISTS key [key ...]}: counts keys that exist. A key given twice is counted twice.
     *
     * @param keys one or more keys
     * @return how many of the given keys exist
     * @throws IllegalArgumentException when no key is given
     */
    @SafeVarargs
    public final CompletableFuture<Long> exists(K... keys) {
        return keys(Cmd.EXISTS, Reply.LONG, keys);
    }

    /**
     * Sends {@code TYPE key}.
     *
     * @param key the key to inspect
     * @return {@code string} (also for {@code SETV} and IDEM values), {@code list}, {@code set}, {@code zset}
     * (also geo), {@code hash}, {@code stream}, {@code json}, {@code bloomfilter}, {@code lease}, {@code limit},
     * {@code semaphore}, {@code once}, or {@code none} when the key does not exist
     */
    public CompletableFuture<String> type(K key) {
        return k(Cmd.TYPE, key, Reply.STRING);
    }

    /**
     * Sends {@code RENAME key newkey}: moves a value to another key name, replacing the target. In a cluster both
     * keys must hash to the same slot (use a {@code {tag}}), otherwise the client throws {@code CROSSSLOT} before sending.
     *
     * @param key the existing key
     * @param newKey the new name
     * @throws RuvioServerException {@code ERR no such key} when {@code key} does not exist; {@code CROSSSLOT} in a cluster
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> rename(K key, K newKey) {
        return kk(Cmd.RENAME, key, newKey, Reply.OK);
    }

    /**
     * Sends {@code RENAMENX key newkey}: renames only when the new name is unused.
     *
     * @param key the existing key
     * @param newKey the new name
     * @return {@code true} when renamed, {@code false} when {@code newKey} already exists
     * @throws RuvioServerException {@code ERR no such key} when {@code key} does not exist; {@code CROSSSLOT} in a cluster
     */
    public CompletableFuture<Boolean> renamenx(K key, K newKey) {
        return kk(Cmd.RENAMENX, key, newKey, Reply.BOOL);
    }

    /**
     * Sends {@code SCAN cursor}: walks the keys of one shard a page at a time without blocking the server.
     * Start with {@code "0"} and pass back {@link ScanResult#cursor()} until {@link ScanResult#isFinished()}.
     * A key that exists for the whole walk is returned at least once; a key can repeat if the table was rebuilt
     * between calls. A page can be empty or short while the cursor is not yet {@code "0"}. In a cluster this covers only the
     * node the call lands on; {@link io.github.salihcantekin.ruvio.RuvioClient#scanIterable} walks every node.
     *
     * <pre>{@code
     * String cursor = "0";
     * do {
     *     ScanResult<String> page = client.scan(cursor);
     *     page.items().forEach(System.out::println);
     *     cursor = page.cursor();
     * } while (!"0".equals(cursor));
     * }</pre>
     *
     * @param cursor {@code "0"} to start, otherwise the cursor of the previous page
     * @return the next cursor and the keys of this page
     * @see io.github.salihcantekin.ruvio.RuvioClient#scanIterable
     */
    public CompletableFuture<ScanResult<K>> scan(String cursor) {
        return send(args().w(Cmd.SCAN).s(cursor), r(SCAN_KEYS));
    }

    /**
     * Sends {@code SCAN cursor [MATCH pattern] [COUNT n] [TYPE type]}: like {@link #scan(String)} with filters.
     * {@code COUNT} bounds the table slots examined per call (about 10 per requested key), so a page can hold fewer keys than asked.
     *
     * <pre>{@code
     * ScanResult<String> page = client.scan("0", ScanArgs.match("user:*").limit(500).type(KeyType.HASH));
     * }</pre>
     *
     * @param cursor {@code "0"} to start, otherwise the cursor of the previous page
     * @param options pattern, count hint and type filter (use {@code ScanArgs.type(KeyType)} with a {@link KeyType} such as {@code KeyType.HASH})
     * @return the next cursor and the matching keys of this page
     */
    public CompletableFuture<ScanResult<K>> scan(String cursor, ScanArgs options) {
        Args a = args().w(Cmd.SCAN).s(cursor);
        options.write(a, true);
        return send(a, r(SCAN_KEYS));
    }

    /**
     * Sends {@code DBSIZE}: the number of keys in the selected database of the shard that answers. Keys past
     * their TTL count until the server removes them. With several shards and {@code port_mode = "per-shard"} each
     * shard counts its own keys; sum over {@link io.github.salihcantekin.ruvio.RuvioClient#onNode} views for a total.
     *
     * @return the key count
     */
    public CompletableFuture<Long> dbsize() {
        return c0(Cmd.DBSIZE, Reply.LONG);
    }

    /**
     * Sends {@code FLUSHDB}: deletes every key of the selected database. Dangerous: it cannot be undone.
     * In a multi-shard server with the default {@code port_mode = "per-shard"}, and in a cluster client (which
     * sends keyless commands to its seed node), it clears only the shard that receives the command. To empty
     * everything, call it on each node with {@link io.github.salihcantekin.ruvio.RuvioClient#onNode}.
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> flushdb() {
        return c0(Cmd.FLUSHDB, Reply.OK);
    }

    /**
     * Sends {@code FLUSHALL}: deletes every key of every database. Dangerous: it cannot be undone.
     * Like {@link #flushdb()}, with {@code port_mode = "per-shard"} it clears only the shard that receives it;
     * repeat it on each node ({@link io.github.salihcantekin.ruvio.RuvioClient#onNode}) to empty a whole cluster.
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> flushall() {
        return c0(Cmd.FLUSHALL, Reply.OK);
    }

    /**
     * Sends {@code SWAPDB first second}: exchanges two logical databases in O(1). Connections on one see the other's keys.
     * Not available in cluster mode.
     *
     * @param first a database index, from 0 to {@code databases - 1} (16 databases by default)
     * @param second another database index in the same range
     * @throws RuvioServerException {@code ERR} when an index is out of range or cluster mode is on
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> swapdb(int first, int second) {
        return send(args().w(Cmd.SWAPDB).l(first).l(second), Reply.OK);
    }

    /**
     * Sends {@code MOVE key db}: moves a key to another logical database. Not available in cluster mode.
     *
     * @param key the key to move
     * @param database the target database index, from 0 to {@code databases - 1}
     * @return {@code true} when moved, {@code false} when the key is missing or already exists in the target
     * @throws RuvioServerException {@code ERR} when {@code database} is out of range or equals the current one, or cluster mode is on
     */
    public CompletableFuture<Boolean> move(K key, int database) {
        return kl(Cmd.MOVE, key, database, Reply.BOOL);
    }

    /**
     * Sends {@code EXPIRE key seconds}: gives a key a time to live in seconds. Replaces an earlier TTL.
     *
     * @param key the key
     * @param seconds seconds from now; zero or negative deletes the key
     * @return {@code true} when the TTL was set (or the key deleted), {@code false} when the key does not exist
     */
    public CompletableFuture<Boolean> expire(K key, long seconds) {
        return kl(Cmd.EXPIRE, key, seconds, Reply.BOOL);
    }

    /**
     * Sends {@code EXPIRE key seconds NX|XX|GT|LT}: like {@link #expire(Object, long)}, applied only when the condition holds. A key
     * without a deadline counts as infinite, so {@code GT} never applies to it and {@code LT} always does.
     *
     * @param key the key
     * @param seconds seconds from now
     * @param condition when to apply it: {@code NX} only without a deadline, {@code XX} only with one, {@code GT} only when later, {@code LT} only when earlier; {@link ExpireCondition#ALWAYS} sends no flag
     * @return {@code true} when the deadline was set, {@code false} when the key does not exist or the condition failed
     */
    public CompletableFuture<Boolean> expire(K key, long seconds, ExpireCondition condition) {
        Args a = args().w(Cmd.EXPIRE).k(key).l(seconds);
        if (condition != null && condition != ExpireCondition.ALWAYS) {
            a.w(condition.fragment());
        }
        return send(a, Reply.BOOL);
    }

    /**
     * Sends {@code PEXPIRE key milliseconds}: like {@link #expire(Object, long)} in milliseconds.
     *
     * @param key the key
     * @param millis milliseconds from now; zero or negative deletes the key
     * @return {@code true} when the TTL was set (or the key deleted), {@code false} when the key does not exist
     */
    public CompletableFuture<Boolean> pexpire(K key, long millis) {
        return kl(Cmd.PEXPIRE, key, millis, Reply.BOOL);
    }

    /**
     * Sends {@code PEXPIRE key millis NX|XX|GT|LT}: like {@link #pexpire(Object, long)}, applied only when the condition holds. A key
     * without a deadline counts as infinite, so {@code GT} never applies to it and {@code LT} always does.
     *
     * @param key the key
     * @param millis milliseconds from now
     * @param condition when to apply it: {@code NX} only without a deadline, {@code XX} only with one, {@code GT} only when later, {@code LT} only when earlier; {@link ExpireCondition#ALWAYS} sends no flag
     * @return {@code true} when the deadline was set, {@code false} when the key does not exist or the condition failed
     */
    public CompletableFuture<Boolean> pexpire(K key, long millis, ExpireCondition condition) {
        Args a = args().w(Cmd.PEXPIRE).k(key).l(millis);
        if (condition != null && condition != ExpireCondition.ALWAYS) {
            a.w(condition.fragment());
        }
        return send(a, Reply.BOOL);
    }

    /**
     * Sends {@code EXPIREAT key timestamp}: expires a key at an absolute Unix time in seconds.
     *
     * @param key the key
     * @param unixSeconds seconds since 1970-01-01 UTC; a time in the past deletes the key
     * @return {@code true} when the deadline was set (or the key deleted), {@code false} when the key does not exist
     */
    public CompletableFuture<Boolean> expireat(K key, long unixSeconds) {
        return kl(Cmd.EXPIREAT, key, unixSeconds, Reply.BOOL);
    }

    /**
     * Sends {@code EXPIREAT key unixSeconds NX|XX|GT|LT}: like {@link #expireat(Object, long)}, applied only when the condition holds. A key
     * without a deadline counts as infinite, so {@code GT} never applies to it and {@code LT} always does.
     *
     * @param key the key
     * @param unixSeconds seconds since 1970-01-01 UTC
     * @param condition when to apply it: {@code NX} only without a deadline, {@code XX} only with one, {@code GT} only when later, {@code LT} only when earlier; {@link ExpireCondition#ALWAYS} sends no flag
     * @return {@code true} when the deadline was set, {@code false} when the key does not exist or the condition failed
     */
    public CompletableFuture<Boolean> expireat(K key, long unixSeconds, ExpireCondition condition) {
        Args a = args().w(Cmd.EXPIREAT).k(key).l(unixSeconds);
        if (condition != null && condition != ExpireCondition.ALWAYS) {
            a.w(condition.fragment());
        }
        return send(a, Reply.BOOL);
    }

    /**
     * Sends {@code PEXPIREAT key timestamp}: expires a key at an absolute Unix time in milliseconds.
     *
     * @param key the key
     * @param unixMillis milliseconds since 1970-01-01 UTC; a time in the past deletes the key
     * @return {@code true} when the deadline was set (or the key deleted), {@code false} when the key does not exist
     */
    public CompletableFuture<Boolean> pexpireat(K key, long unixMillis) {
        return kl(Cmd.PEXPIREAT, key, unixMillis, Reply.BOOL);
    }

    /**
     * Sends {@code PEXPIREAT key unixMillis NX|XX|GT|LT}: like {@link #pexpireat(Object, long)}, applied only when the condition holds. A key
     * without a deadline counts as infinite, so {@code GT} never applies to it and {@code LT} always does.
     *
     * @param key the key
     * @param unixMillis milliseconds since 1970-01-01 UTC
     * @param condition when to apply it: {@code NX} only without a deadline, {@code XX} only with one, {@code GT} only when later, {@code LT} only when earlier; {@link ExpireCondition#ALWAYS} sends no flag
     * @return {@code true} when the deadline was set, {@code false} when the key does not exist or the condition failed
     */
    public CompletableFuture<Boolean> pexpireat(K key, long unixMillis, ExpireCondition condition) {
        Args a = args().w(Cmd.PEXPIREAT).k(key).l(unixMillis);
        if (condition != null && condition != ExpireCondition.ALWAYS) {
            a.w(condition.fragment());
        }
        return send(a, Reply.BOOL);
    }

    /**
     * Sends {@code TTL key}.
     *
     * @param key the key
     * @return the remaining time to live in seconds, {@code -1} when the key has no expiry, {@code -2} when it does not exist
     */
    public CompletableFuture<Long> ttl(K key) {
        return k(Cmd.TTL, key, Reply.LONG);
    }

    /**
     * Sends {@code PTTL key}.
     *
     * @param key the key
     * @return the remaining time to live in milliseconds, {@code -1} when the key has no expiry, {@code -2} when it does not exist
     */
    public CompletableFuture<Long> pttl(K key) {
        return k(Cmd.PTTL, key, Reply.LONG);
    }

    /**
     * Sends {@code PERSIST key}: removes a key's expiry so it lives on.
     *
     * @param key the key
     * @return {@code true} when an expiry was removed, {@code false} when the key is missing or had none
     */
    public CompletableFuture<Boolean> persist(K key) {
        return k(Cmd.PERSIST, key, Reply.BOOL);
    }

    /**
     * Sends {@code EXPIRETIME key}.
     *
     * @param key the key
     * @return the absolute expiry as Unix seconds, {@code -1} when the key has no expiry, {@code -2} when it does not exist
     */
    public CompletableFuture<Long> expiretime(K key) {
        return k(Cmd.EXPIRETIME, key, Reply.LONG);
    }

    /**
     * Sends {@code PEXPIRETIME key}.
     *
     * @param key the key
     * @return the absolute expiry as Unix milliseconds, {@code -1} when the key has no expiry, {@code -2} when it does not exist
     */
    public CompletableFuture<Long> pexpiretime(K key) {
        return k(Cmd.PEXPIRETIME, key, Reply.LONG);
    }

    // ---- keyspace --------------------------------------------------------------------------

    /**
     * Sends {@code KEYS pattern}: lists the keys of the selected database that match a glob ({@code *}, {@code ?}).
     * It walks the whole database, so the server refuses it when the database has more than 100,000 keys; use
     * {@link #scan(String, ScanArgs)} instead. It covers one shard only (see {@link #dbsize()}).
     *
     * @param pattern a glob such as {@code user:*}
     * @return the matching keys, in no particular order; empty when none
     * @throws RuvioServerException {@code ERR KEYS is limited to 100000 keys per database, use SCAN}
     */
    public CompletableFuture<List<K>> keys(String pattern) {
        return send(args().w(Cmd.KEYS).s(pattern), r(Reply.KEY_LIST));
    }

    /**
     * Sends {@code RANDOMKEY}.
     *
     * @return a random key of the selected database, or {@code null} when it is empty
     */
    public CompletableFuture<K> randomkey() {
        return c0(Cmd.RANDOMKEY, r(Reply.KEY));
    }

    /**
     * Sends {@code TOUCH key [key ...]}: marks keys as accessed (for LRU/LFU eviction) without reading them.
     * In a cluster, keys in different slots are sent per slot and the counts added up.
     *
     * @param keys one or more keys
     * @return how many of the keys exist
     * @throws IllegalArgumentException when no key is given
     */
    @SafeVarargs
    public final CompletableFuture<Long> touch(K... keys) {
        return keys(Cmd.TOUCH, Reply.LONG, keys);
    }

    /**
     * Sends {@code COPY source destination}: duplicates a key (value and TTL, also JSON documents) into a new name in the selected database.
     *
     * @param source the key to copy
     * @param destination the new key; the copy fails if it already exists
     * @return {@code true} when copied, {@code false} when {@code source} is missing or {@code destination} exists
     * @throws RuvioServerException {@code CROSSSLOT} in a cluster when the keys hash to different slots
     */
    public CompletableFuture<Boolean> copy(K source, K destination) {
        return kk(Cmd.COPY, source, destination, Reply.BOOL);
    }

    /**
     * Sends {@code COPY source destination [REPLACE]}.
     *
     * @param source the key to copy
     * @param destination the target key
     * @param replace {@code true} to overwrite an existing {@code destination}
     * @return {@code true} when copied, {@code false} when {@code source} is missing or {@code destination} exists and {@code replace} is false
     * @throws RuvioServerException {@code CROSSSLOT} in a cluster when the keys hash to different slots
     */
    public CompletableFuture<Boolean> copy(K source, K destination, boolean replace) {
        Args a = args().w(Cmd.COPY).k(source).k(destination);
        if (replace) {
            a.s("REPLACE");
        }
        return send(a, Reply.BOOL);
    }

    /**
     * Sends {@code OBJECT ENCODING key}.
     *
     * @param key the key to inspect
     * @return the internal encoding name, or {@code null} when the key does not exist
     */
    public CompletableFuture<String> objectEncoding(K key) {
        return send(args().w(Cmd.OBJECT).s("ENCODING").k(key), Reply.STRING);
    }

    /**
     * Sends {@code OBJECT IDLETIME key}: seconds since the key was last accessed. Needs an LRU {@code maxmemory-policy}.
     *
     * @param key the key to inspect
     * @return the idle seconds, or {@code null} when the key does not exist
     * @throws RuvioServerException {@code ERR} when an LFU policy is selected
     */
    public CompletableFuture<Long> objectIdletime(K key) {
        return send(args().w(Cmd.OBJECT).s("IDLETIME").k(key), Reply.LONG);
    }

    /**
     * Sends {@code OBJECT FREQ key}: the access-frequency counter. Needs the {@code allkeys-lfu} {@code maxmemory-policy}.
     *
     * @param key the key to inspect
     * @return the frequency counter, or {@code null} when the key does not exist
     * @throws RuvioServerException {@code ERR} when an LFU policy is not selected
     */
    public CompletableFuture<Long> objectFreq(K key) {
        return send(args().w(Cmd.OBJECT).s("FREQ").k(key), Reply.LONG);
    }

    /**
     * Sends {@code MEMORY USAGE key}.
     *
     * @param key the key to inspect
     * @return the approximate bytes the key uses, or {@code null} when it does not exist
     */
    public CompletableFuture<Long> memoryUsage(K key) {
        return send(args().w(Cmd.MEMORY).s("USAGE").k(key), Reply.LONG);
    }

    /**
     * Sends {@code MEMORY USAGE key SAMPLES n}: like {@link #memoryUsage(Object)}; the sample count is accepted for Redis compatibility.
     *
     * @param key the key to inspect
     * @param samples number of nested elements to sample
     * @return the approximate bytes the key uses, or {@code null} when it does not exist
     */
    public CompletableFuture<Long> memoryUsage(K key, long samples) {
        return send(args().w(Cmd.MEMORY).s("USAGE").k(key).s("SAMPLES").l(samples), Reply.LONG);
    }

    /**
     * Sends {@code DUMP key}: serializes a key (any type, JSON included) into Ruvio's own payload. Only Ruvio's
     * {@link #restore(Object, long, byte[])} can read it; Redis payloads are not compatible.
     *
     * @param key the key to serialize
     * @return the payload, or {@code null} when the key does not exist
     */
    public CompletableFuture<byte[]> dump(K key) {
        return k(Cmd.DUMP, key, Reply.BYTES);
    }

    /**
     * Sends {@code RESTORE key ttl payload}: creates a key from a {@link #dump(Object)} payload.
     *
     * @param key the key to create
     * @param ttlMillis time to live in milliseconds, or {@code 0} for no expiry
     * @param payload the bytes returned by {@code dump}
     * @throws RuvioServerException {@code BUSYKEY} when the key already exists; {@code ERR} for a negative TTL or a damaged payload
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> restore(K key, long ttlMillis, byte[] payload) {
        return send(args().w(Cmd.RESTORE).k(key).l(ttlMillis).b(payload), Reply.OK);
    }

    /**
     * Sends {@code RESTORE key ttl payload [REPLACE] [ABSTTL] [IDLETIME s] [FREQ n]}.
     *
     * @param key the key to create
     * @param ttlMillis time to live in milliseconds ({@code 0} for no expiry), or an absolute Unix time in milliseconds with {@code absTtl()}
     * @param payload the bytes returned by {@code dump}
     * @param options {@code replace()} overwrites an existing key; {@code absTtl()} reads the TTL as an absolute time; idle time and frequency are accepted and ignored
     * @throws RuvioServerException {@code BUSYKEY} when the key exists and {@code replace} was not set; {@code ERR} for a negative TTL or a damaged payload
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> restore(K key, long ttlMillis, byte[] payload, RestoreArgs options) {
        Args a = args().w(Cmd.RESTORE).k(key).l(ttlMillis).b(payload);
        options.write(a);
        return send(a, Reply.OK);
    }
}
