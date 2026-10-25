// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.commands;

import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.RuvioServerException;
import io.github.salihcantekin.ruvio.args.BitFieldArgs;
import io.github.salihcantekin.ruvio.args.BitOp;
import io.github.salihcantekin.ruvio.args.BitUnit;
import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Cmd;
import io.github.salihcantekin.ruvio.internal.Reply;
import io.github.salihcantekin.ruvio.internal.Request;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Lua scripts and functions, bitmaps and HyperLogLog.
 *
 * @param <K> key type (also hash fields and stream field names)
 * @param <V> value type (also members, list elements and stream field values)
 */
public abstract class ScriptingCommands<K, V> extends JsonCommands<K, V> {
    /**
     * Creates the command family with the given codecs.
     *
     * @param keys the key codec
     * @param values the value codec
     */
    protected ScriptingCommands(RuvioCodec<K> keys, RuvioCodec<V> values) {
        super(keys, values);
    }

    /** Script results: bulk strings decoded with the value codec, integers Long, arrays List. */
    static Object scriptValue(Object tree, Request<?> q) {
        if (tree instanceof byte[]) {
            return Reply.decodeWith(q.values, tree);
        }
        if (tree instanceof List) {
            List<Object> in = Reply.list(tree);
            List<Object> out = new ArrayList<>(in.size());
            for (Object o : in) {
                out.add(scriptValue(o, q));
            }
            return out;
        }
        return tree;
    }

    static final Reply<Object> SCRIPT = Reply.of(ScriptingCommands::scriptValue);

    static final Reply<List<String>> FUNCTION_NAMES = Reply.of((tree, q) -> {
        List<String> out = new ArrayList<>();
        collectNames(tree, out);
        return out;
    });

    private static void collectNames(Object tree, List<String> out) {
        if (tree instanceof List) {
            for (Object o : Reply.list(tree)) {
                collectNames(o, out);
            }
        } else if (tree != null) {
            out.add(Reply.str(tree));
        }
    }

    private Args script(byte[] cmd, String body, K[] keys, V[] arguments) {
        Args a = args().w(cmd).s(body).l(keys == null ? 0 : keys.length);
        if (keys != null) a.keys(keys);
        if (arguments != null) a.values(arguments);
        return a;
    }

    /**
     * Sends {@code EVAL script numkeys key [key ...] arg [arg ...]}: runs a Lua 5.1 script on the server, atomically. Inside the script, {@code KEYS} and {@code ARGV} hold the arguments and {@code ruvio.call(...)} / {@code ruvio.pcall(...)} run commands, but only on the declared keys. Every declared key must hash to one slot (use a {@code {tag}}); a script with no keys runs on the shard the connection reaches. Writes are applied as one batch; a Lua error or a refused batch restores the declared keys. Scripts cannot run inside a transaction. Runtime limits (instructions, time, memory) are set in the server configuration.
     *
     * <pre>{@code
     * Object n = client.eval("return ruvio.call('INCR', KEYS[1])", new String[] {"hits"});
     * }</pre>
     *
     * @param script the Lua source
     * @param keys the keys the script may touch (pass an empty array for none)
     * @param arguments extra values available as {@code ARGV}
     * @return the script result converted to Java: a bulk string is decoded with the value codec, an integer is a {@code Long}, an array is a {@code List}, {@code nil} is {@code null}
     * @throws RuvioServerException {@code ERR} for a Lua error or a limit that was hit; {@code CROSSSLOT} when the keys span slots; {@code NOPERM} when the user may not run a command the script calls
     */
    @SafeVarargs
    public final CompletableFuture<Object> eval(String script, K[] keys, V... arguments) {
        return send(script(Cmd.EVAL, script, keys, arguments), SCRIPT);
    }

    /**
     * Sends {@code EVALSHA sha1 numkeys key ... arg ...}: runs a script already stored by {@link #scriptLoad(String)} or {@code eval}, without sending its source again. The cache is memory only and per shard, so it is empty after a restart.
     *
     * @param sha1 the 40-character hex digest of the script
     * @param keys the keys the script may touch (empty array for none)
     * @param arguments extra values available as {@code ARGV}
     * @return the script result, converted as for {@link #eval}
     * @throws RuvioServerException {@code NOSCRIPT} when the digest is not cached on that shard; {@code ERR} for a Lua error; {@code CROSSSLOT}
     */
    @SafeVarargs
    public final CompletableFuture<Object> evalsha(String sha1, K[] keys, V... arguments) {
        return send(script(Cmd.EVALSHA, sha1, keys, arguments), SCRIPT);
    }

    /**
     * Runs a script by its SHA1 and sends the whole script only when the shard does not have it: {@code EVALSHA}, then {@code EVAL} once on {@code NOSCRIPT}. The digest is computed here, and {@code EVAL} also caches the script on that shard. The retry is safe because {@code NOSCRIPT} is answered before any command of the script ran, so a script never runs twice.
     *
     * <pre>{@code
     * Object v = client.evalCached("return ARGV[1]", new String[0], "x");
     * }</pre>
     *
     * @param script the Lua source
     * @param keys the keys the script may touch (pass an empty array for none)
     * @param arguments extra values available as {@code ARGV}
     * @return the script result, converted as for {@link #eval}
     * @throws RuvioServerException as for {@link #eval}
     */
    @SafeVarargs
    public final CompletableFuture<Object> evalCached(String script, K[] keys, V... arguments) {
        return evalsha(sha1Hex(script), keys, arguments).handle((value, error) -> {
            if (error == null) {
                return CompletableFuture.completedFuture(value);
            }
            Throwable cause = error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;

            if (cause instanceof RuvioServerException && "NOSCRIPT".equals(((RuvioServerException) cause).code())) {
                return eval(script, keys, arguments);
            }
            return CompletableFuture.<Object>failedFuture(cause);
        }).thenCompose(next -> next);
    }

    private static String sha1Hex(String script) {
        try {
            return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-1").digest(script.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Sends {@code SCRIPT LOAD script}: compiles a script into the cache of the shard the command reaches, without running it. In a cluster, load it on every node you call {@code evalsha} on (see {@link io.github.salihcantekin.ruvio.RuvioClient#onNode}).
     *
     * @param script the Lua source
     * @return the SHA1 digest to use with {@link #evalsha}
     * @throws RuvioServerException {@code ERR} when the script does not compile
     */
    public CompletableFuture<String> scriptLoad(String script) {
        return send(args().w(Cmd.SCRIPT).s("LOAD").s(script), Reply.STRING);
    }

    /**
     * Sends {@code SCRIPT EXISTS sha1 [sha1 ...]}.
     *
     * @param sha1s one or more digests
     * @return one flag per digest: {@code true} when that script is cached on the shard
     */
    public CompletableFuture<List<Boolean>> scriptExists(String... sha1s) {
        return send(args().w(Cmd.SCRIPT).s("EXISTS").strings(sha1s), Reply.BOOL_LIST);
    }

    /**
     * Sends {@code SCRIPT FLUSH}: empties the script cache of the shard the command reaches.
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> scriptFlush() {
        return send(args().w(Cmd.SCRIPT).s("FLUSH"), Reply.OK);
    }

    /**
     * Sends {@code SCRIPT KILL}. Ruvio cannot interrupt a script this way; it stops runaway scripts with its instruction and time limits instead, so this always fails.
     *
     * @throws RuvioServerException {@code NOTBUSY} always
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> scriptKill() {
        return send(args().w(Cmd.SCRIPT).s("KILL"), Reply.OK);
    }

    /**
     * Sends {@code FCALL function numkeys key ... arg ...}: runs a function stored with {@link #functionLoad}. The body runs like an {@link #eval} script, with {@code KEYS} and {@code ARGV}.
     *
     * @param function the function name
     * @param keys the keys the function may touch (empty array for none)
     * @param arguments extra values available as {@code ARGV}
     * @return the result, converted as for {@link #eval}
     * @throws RuvioServerException {@code ERR function not found}; {@code ERR} for a Lua error; {@code CROSSSLOT}
     */
    @SafeVarargs
    public final CompletableFuture<Object> fcall(String function, K[] keys, V... arguments) {
        return send(script(Cmd.FCALL, function, keys, arguments), SCRIPT);
    }

    /**
     * Sends the Ruvio form of {@code FUNCTION LOAD name body}: stores a named Lua body in the memory of the shard the command reaches. It is not replayed after a restart, and it is refused while replication is on. Loading a name again replaces it.
     *
     * @param name the function name
     * @param body the Lua source, written like an {@link #eval} script
     * @throws RuvioServerException {@code ERR} when the body does not compile or the server forbids it
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> functionLoad(String name, String body) {
        return send(args().w(Cmd.FUNCTION).s("LOAD").s(name).s(body), Reply.OK);
    }

    /**
     * Sends {@code FUNCTION LIST}.
     *
     * @return the names of the functions loaded on that shard, in byte order
     */
    public CompletableFuture<List<String>> functionList() {
        return send(args().w(Cmd.FUNCTION).s("LIST"), FUNCTION_NAMES);
    }

    /**
     * Sends {@code FUNCTION DELETE name}.
     *
     * @param name the function name
     * @return {@code true} when the function existed and was removed
     */
    public CompletableFuture<Boolean> functionDelete(String name) {
        return send(args().w(Cmd.FUNCTION).s("DELETE").s(name), Reply.BOOL);
    }

    // ---- bitmaps ---------------------------------------------------------------------------

    /**
     * Sends {@code SETBIT key offset value}: sets one bit of a string, growing it with zero bytes as needed. Rewrites the whole value (cost grows with its size); an existing TTL is kept. Bitmaps are plain strings of at most 16 MiB.
     *
     * <pre>{@code
     * long before = client.setbit("online", 1042, true);
     * }</pre>
     *
     * @param key the bitmap key
     * @param offset the bit position, from 0 to 134,217,727; bit 0 is the most significant bit of the first byte
     * @param value the new bit
     * @return the previous bit, {@code 0} or {@code 1}
     * @throws RuvioServerException {@code ERR bit offset is not an integer or out of range}; {@code WRONGTYPE} for a non-string
     */
    public CompletableFuture<Long> setbit(K key, long offset, boolean value) {
        return kll(Cmd.SETBIT, key, offset, value ? 1 : 0, Reply.LONG);
    }

    /**
     * Sends {@code SETBIT key offset value EX|PX ttl}: like {@link #setbit(Object, long, boolean)}, and a bitmap this call creates
     * gets a time to live. An existing key keeps its deadline.
     *
     * @param key the bitmap key
     * @param offset the bit position, from 0 to 134,217,727
     * @param value the new bit
     * @param ttl time to live of a newly created key: whole seconds are sent as {@code EX}, anything finer as {@code PX}
     * @return the previous bit, {@code 0} or {@code 1}
     * @throws IllegalArgumentException when {@code ttl} is null, zero, negative or too large
     * @throws RuvioServerException {@code ERR bit offset is not an integer or out of range}; {@code WRONGTYPE} for a non-string
     */
    public CompletableFuture<Long> setbit(K key, long offset, boolean value, Duration ttl) {
        return send(args().w(Cmd.SETBIT).k(key).l(offset).l(value ? 1 : 0).ttl(ttl), Reply.LONG);
    }

    /**
     * Sends {@code GETBIT key offset}.
     *
     * @param key the bitmap key
     * @param offset the bit position, zero or greater
     * @return {@code 0} or {@code 1}; {@code 0} when the key is missing or the offset is past the end
     * @throws RuvioServerException {@code ERR bit offset is not an integer or out of range}; {@code WRONGTYPE}
     */
    public CompletableFuture<Long> getbit(K key, long offset) {
        return kl(Cmd.GETBIT, key, offset, Reply.LONG);
    }

    /**
     * Sends {@code BITCOUNT key}: counts the bits set to 1 in the whole string.
     *
     * @param key the bitmap key
     * @return the number of set bits, {@code 0} when the key is missing
     * @throws RuvioServerException {@code WRONGTYPE}
     */
    public CompletableFuture<Long> bitcount(K key) {
        return k(Cmd.BITCOUNT, key, Reply.LONG);
    }

    /**
     * Sends {@code BITCOUNT key start end}: counts set bits in a byte range.
     *
     * @param key the bitmap key
     * @param start the first byte, inclusive; negative counts from the end
     * @param end the last byte, inclusive; negative counts from the end
     * @return the number of set bits in the range
     * @throws RuvioServerException {@code WRONGTYPE}
     */
    public CompletableFuture<Long> bitcount(K key, long start, long end) {
        return kll(Cmd.BITCOUNT, key, start, end, Reply.LONG);
    }

    /**
     * Sends {@code BITCOUNT key start end BIT|BYTE}. Prefer the {@link BitUnit} overload.
     *
     * @param key the bitmap key
     * @param start the first position, inclusive; negative counts from the end
     * @param end the last position, inclusive; negative counts from the end
     * @param bitUnits {@code true} reads {@code start}/{@code end} as bit positions ({@code BIT}); {@code false} as byte positions ({@code BYTE})
     * @return the number of set bits in the range
     * @throws RuvioServerException {@code WRONGTYPE}
     */
    public CompletableFuture<Long> bitcount(K key, long start, long end, boolean bitUnits) {
        return bitcount(key, start, end, bitUnits ? BitUnit.BIT : BitUnit.BYTE);
    }

    /**
     * Sends {@code BITCOUNT key start end BYTE|BIT} with an enum unit instead of a boolean.
     *
     * <pre>{@code
     * long n = client.bitcount("online", 0, 7, BitUnit.BIT);
     * }</pre>
     *
     * @param key the bitmap key
     * @param start the first position, inclusive; negative counts from the end
     * @param end the last position, inclusive; negative counts from the end
     * @param unit {@link BitUnit#BYTE} reads {@code start}/{@code end} as byte positions, {@link BitUnit#BIT} as bit positions
     * @return the number of set bits in the range
     * @throws RuvioServerException {@code WRONGTYPE}
     */
    public CompletableFuture<Long> bitcount(K key, long start, long end, BitUnit unit) {
        return send(args().w(Cmd.BITCOUNT).k(key).l(start).l(end).w(unit.fragment()), Reply.LONG);
    }

    /**
     * Sends {@code BITPOS key bit}: the position of the first bit with the given value.
     *
     * @param key the bitmap key
     * @param bit the bit value to look for
     * @return the bit position, or {@code -1} when a {@code 1} is not found; for a missing key, {@code 0} when looking for {@code 0}
     * @throws RuvioServerException {@code WRONGTYPE}
     */
    public CompletableFuture<Long> bitpos(K key, boolean bit) {
        return kl(Cmd.BITPOS, key, bit ? 1 : 0, Reply.LONG);
    }

    /**
     * Sends {@code BITPOS key bit start}: like {@link #bitpos(Object, boolean)} starting at a byte.
     *
     * @param key the bitmap key
     * @param bit the bit value to look for
     * @param start the first byte to look at; negative counts from the end
     * @return the bit position (counted from the start of the string), or {@code -1} when not found
     * @throws RuvioServerException {@code WRONGTYPE}
     */
    public CompletableFuture<Long> bitpos(K key, boolean bit, long start) {
        return kll(Cmd.BITPOS, key, bit ? 1 : 0, start, Reply.LONG);
    }

    /**
     * Sends {@code BITPOS key bit start end}: looks only in a byte range.
     *
     * @param key the bitmap key
     * @param bit the bit value to look for
     * @param start the first byte, inclusive; negative counts from the end
     * @param end the last byte, inclusive; negative counts from the end
     * @return the bit position (counted from the start of the string), or {@code -1} when not found in the range
     * @throws RuvioServerException {@code WRONGTYPE}
     */
    public CompletableFuture<Long> bitpos(K key, boolean bit, long start, long end) {
        return send(args().w(Cmd.BITPOS).k(key).l(bit ? 1 : 0).l(start).l(end), Reply.LONG);
    }

    /**
     * Sends {@code BITOP op destination key [key ...]}: combines strings bit by bit (shorter ones are padded with zeros) and stores the result. An empty result deletes {@code destination}. In a cluster all keys must hash to one slot.
     *
     * @param op a {@link BitOp}: {@code AND}, {@code OR}, {@code XOR}, or {@code NOT} (which takes exactly one source key)
     * @param destination the key that receives the result
     * @param keys one or more source keys
     * @return the length of the result in bytes
     * @throws RuvioServerException {@code ERR} for {@code NOT} with several keys; {@code WRONGTYPE}; {@code CROSSSLOT} in a cluster
     */
    @SafeVarargs
    public final CompletableFuture<Long> bitop(BitOp op, K destination, K... keys) {
        return send(args().w(Cmd.BITOP).w(op.fragment()).k(destination).keys(keys), Reply.LONG);
    }

    /**
     * Sends {@code BITFIELD key GET|SET|INCRBY ... [OVERFLOW WRAP|SAT|FAIL]}: reads and writes integer fields packed inside a string. Types are {@code i1} to {@code i64} (signed) and {@code u1} to {@code u63} (unsigned), given as text or as {@link BitFieldArgs.Type}; {@code OVERFLOW} takes a {@link BitFieldArgs.Overflow} mode; an offset is a bit number or {@code #n} (n times the type width). A write grows the string and rewrites the value.
     *
     * <pre>{@code
     * List<Long> r = client.bitfield("stats", BitFieldArgs.builder().set(BitFieldArgs.Type.unsigned(8), 0, 200).incrBy(BitFieldArgs.Type.unsigned(8), 0, 100));
     * }</pre>
     *
     * @param key the key of the string
     * @param operations the operations to run in order, built with {@link BitFieldArgs}
     * @return one value per {@code GET}/{@code SET}/{@code INCRBY}: the field value, the old value for {@code SET}, the new value for {@code INCRBY}; {@code null} for an {@code INCRBY} stopped by {@code OVERFLOW FAIL}
     * @throws RuvioServerException {@code ERR} for a bad type or offset; {@code WRONGTYPE}
     */
    public CompletableFuture<List<Long>> bitfield(K key, BitFieldArgs operations) {
        Args a = args().w(Cmd.BITFIELD).k(key);
        operations.write(a);
        return send(a, Reply.LONG_LIST);
    }

    /**
     * Sends {@code BITFIELD_RO key GET type offset ...}: read-only fields; any write operation is refused.
     *
     * @param key the key of the string
     * @param getOperations only {@code get} operations
     * @return one value per {@code GET}
     * @throws RuvioServerException {@code ERR} when the operations include a write; {@code WRONGTYPE}
     */
    public CompletableFuture<List<Long>> bitfieldRo(K key, BitFieldArgs getOperations) {
        Args a = args().w(Cmd.BITFIELD_RO).k(key);
        getOperations.write(a);
        return send(a, Reply.LONG_LIST);
    }

    // ---- HyperLogLog -----------------------------------------------------------------------

    /**
     * Sends {@code PFADD key [element ...]}: adds elements to a HyperLogLog, which counts distinct items in about 12 KB however many there are. A HyperLogLog is a string with a {@code HYLL} header; Ruvio always writes the dense form and rewrites the whole value on each change. With no elements it only creates an empty HyperLogLog.
     *
     * <pre>{@code
     * client.pfadd("visitors:2026-10-17", "u1", "u2");
     * long n = client.pfcount("visitors:2026-10-17");
     * }</pre>
     *
     * @param key the HyperLogLog key
     * @param elements zero or more elements
     * @return {@code true} when the estimate changed (or the key was created), {@code false} otherwise
     * @throws RuvioServerException {@code WRONGTYPE} when the key is not a valid HyperLogLog
     */
    @SafeVarargs
    public final CompletableFuture<Boolean> pfadd(K key, V... elements) {
        if (elements.length == 0) {
            return k(Cmd.PFADD, key, Reply.BOOL);
        }
        return kvs(Cmd.PFADD, key, Reply.BOOL, elements);
    }

    /**
     * Sends {@code PFCOUNT key [key ...]}: estimated number of distinct elements; with several keys, of their union (same slot in a cluster). The error is about 0.8%.
     *
     * @param keys one or more HyperLogLog keys
     * @return the estimate, {@code 0} for missing keys
     * @throws RuvioServerException {@code WRONGTYPE} when a key is not a valid HyperLogLog; {@code CROSSSLOT} in a cluster
     */
    @SafeVarargs
    public final CompletableFuture<Long> pfcount(K... keys) {
        return keys(Cmd.PFCOUNT, Reply.LONG, keys);
    }

    /**
     * Sends {@code PFMERGE destination sourcekey [sourcekey ...]}: stores the union of HyperLogLogs in {@code destination} (which is also part of the union). All keys must hash to one slot in a cluster.
     *
     * @param destination the key that receives the union
     * @param sources one or more HyperLogLog keys
     * @throws RuvioServerException {@code WRONGTYPE} when a key is not a valid HyperLogLog; {@code CROSSSLOT} in a cluster
     * @return a future that completes with no value once the server has acknowledged the command
     */
    @SafeVarargs
    public final CompletableFuture<Void> pfmerge(K destination, K... sources) {
        return send(args().w(Cmd.PFMERGE).k(destination).keys(sources), Reply.OK);
    }
}
