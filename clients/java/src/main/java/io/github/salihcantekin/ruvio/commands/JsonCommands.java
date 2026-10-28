// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.commands;

import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.RuvioServerException;
import io.github.salihcantekin.ruvio.args.BfInsertArgs;
import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Cmd;
import io.github.salihcantekin.ruvio.internal.Reply;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * JSON.* (simple {@code .field} and {@code [N]} paths, Ruvio JSON.INDEX / JSON.FIND) and BF.*
 * (RedisBloom wire shape). JSON documents and paths are UTF-8 text.
 *
 * @param <K> key type (also hash fields and stream field names)
 * @param <V> value type (also members, list elements and stream field values)
 */
public abstract class JsonCommands<K, V> extends StreamCommands<K, V> {
    /**
     * Creates the command family with the given codecs.
     *
     * @param keys the key codec
     * @param values the value codec
     */
    protected JsonCommands(RuvioCodec<K> keys, RuvioCodec<V> values) {
        super(keys, values);
    }

    static final Reply<Map<String, Object>> BF_INFO = Reply.of((tree, q) -> Reply.fieldMap(Reply.text(tree)));

    static final Reply<Long> FIRST_LONG = Reply.of((tree, q) -> {
        Object v = tree instanceof List ? Reply.list(tree).get(0) : tree;
        return v == null ? null : Reply.lng(v);
    });

    // ---- JSON ------------------------------------------------------------------------------

    /**
     * Sends the Ruvio command {@code JSON.GET key}: reads a whole JSON document as compact JSON text. JSON is a separate key type ({@code TYPE} reports {@code json}); string commands refuse it with {@code WRONGTYPE}. Paths are limited to {@code $}, {@code .field} and {@code [N]}: no wildcards or filters.
     *
     * <pre>{@code
     * client.jsonSet("user:1", "$", "{\"name\":\"Ada\",\"tags\":[\"a\"]}");
     * String doc = client.jsonGet("user:1"); // {"name":"Ada","tags":["a"]}
     * }</pre>
     *
     * @param key the JSON key
     * @return the document text, or {@code null} when the key does not exist
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-JSON value
     */
    public CompletableFuture<String> jsonGet(K key) {
        return k(Cmd.JSON_GET, key, Reply.STRING);
    }

    /**
     * Sends {@code JSON.GET key path}: reads the value at a path.
     *
     * @param key the JSON key
     * @param path {@code $} (root), {@code .name}, {@code $.users[0].name}; a path may also start with {@code .} or {@code [}
     * @return the value as JSON text (strings keep their quotes), or {@code null} when the key or path does not exist
     * @throws RuvioServerException {@code ERR invalid JSON path} for unsupported syntax; {@code WRONGTYPE}
     */
    public CompletableFuture<String> jsonGet(K key, String path) {
        return ks(Cmd.JSON_GET, key, path, Reply.STRING);
    }

    /**
     * Sends {@code JSON.MGET key [key ...] path}: reads one path from several documents. In a cluster all keys must hash to one slot.
     *
     * @param path the path to read from each document
     * @param keys one or more JSON keys
     * @return one JSON text per key in order; {@code null} for a key that is missing, is not JSON, or lacks the path
     * @throws RuvioServerException {@code ERR invalid JSON path}; {@code CROSSSLOT} in a cluster
     */
    @SafeVarargs
    public final CompletableFuture<List<String>> jsonMget(String path, K... keys) {
        return send(args().w(Cmd.JSON_MGET).keys(keys).s(path), Reply.STRING_LIST);
    }

    /**
     * Sends {@code JSON.SET key path json}: writes a JSON value. A missing key can only be created at {@code $}. A final object field may be created; missing intermediate objects are not. Setting {@code [N]} where {@code N} equals the array length appends. Existing TTLs are kept. The document is limited to 16 MiB.
     *
     * <pre>{@code
     * client.jsonSet("user:1", "$.name", "\"Grace\"");
     * }</pre>
     *
     * @param key the JSON key
     * @param path where to write: {@code $} for the whole document, or {@code .field}/{@code [N]} paths
     * @param json the new value as JSON text; it is validated
     * @return {@code true} when written, {@code false} when the target cannot be reached (missing key at a non-root path, or absent parent)
     * @throws RuvioServerException {@code ERR invalid JSON value} or {@code ERR invalid JSON path}; {@code WRONGTYPE} when the key holds a non-JSON value
     */
    public CompletableFuture<Boolean> jsonSet(K key, String path, String json) {
        return send(args().w(Cmd.JSON_SET).k(key).s(path).s(json), Reply.OK_BOOL);
    }

    /**
     * Sends {@code JSON.SET key path json EX|PX ttl}: like {@link #jsonSet(Object, String, String)}, and a document this call
     * creates gets a time to live. An existing key keeps its deadline.
     *
     * @param key the JSON key
     * @param path where to write
     * @param json the new value as JSON text
     * @param ttl time to live of a newly created key: whole seconds are sent as {@code EX}, anything finer as {@code PX}
     * @return {@code true} when written, {@code false} when the target cannot be reached
     * @throws IllegalArgumentException when {@code ttl} is null, zero, negative or too large
     * @throws RuvioServerException {@code ERR invalid JSON value} or {@code ERR invalid JSON path}; {@code WRONGTYPE} when the key holds a non-JSON value
     */
    public CompletableFuture<Boolean> jsonSet(K key, String path, String json, Duration ttl) {
        return send(args().w(Cmd.JSON_SET).k(key).s(path).s(json).ttl(ttl), Reply.OK_BOOL);
    }

    /**
     * Sends {@code JSON.SET key path json NX}: writes only when the path does not exist yet.
     *
     * @param key the JSON key
     * @param path where to write
     * @param json the new value as JSON text
     * @return {@code true} when written, {@code false} when the path already exists (or cannot be reached)
     * @throws RuvioServerException {@code ERR invalid JSON value} or {@code ERR invalid JSON path}; {@code WRONGTYPE}
     */
    public CompletableFuture<Boolean> jsonSetNx(K key, String path, String json) {
        return send(args().w(Cmd.JSON_SET).k(key).s(path).s(json).w(Cmd.NX), Reply.OK_BOOL);
    }

    /**
     * Sends {@code JSON.SET key path json NX EX|PX ttl}: like {@link #jsonSetNx(Object, String, String)}, and a document this call
     * creates gets a time to live. An existing key keeps its deadline.
     *
     * @param key the JSON key
     * @param path where to write
     * @param json the new value as JSON text
     * @param ttl time to live of a newly created key: whole seconds are sent as {@code EX}, anything finer as {@code PX}
     * @return {@code true} when written, {@code false} when the path already exists (or cannot be reached)
     * @throws IllegalArgumentException when {@code ttl} is null, zero, negative or too large
     * @throws RuvioServerException {@code ERR invalid JSON value} or {@code ERR invalid JSON path}; {@code WRONGTYPE} when the key holds a non-JSON value
     */
    public CompletableFuture<Boolean> jsonSetNx(K key, String path, String json, Duration ttl) {
        return send(args().w(Cmd.JSON_SET).k(key).s(path).s(json).w(Cmd.NX).ttl(ttl), Reply.OK_BOOL);
    }

    /**
     * Sends {@code JSON.SET key path json XX}: writes only when the path already exists.
     *
     * @param key the JSON key
     * @param path where to write
     * @param json the new value as JSON text
     * @return {@code true} when written, {@code false} when the path does not exist
     * @throws RuvioServerException {@code ERR invalid JSON value} or {@code ERR invalid JSON path}; {@code WRONGTYPE}
     */
    public CompletableFuture<Boolean> jsonSetXx(K key, String path, String json) {
        return send(args().w(Cmd.JSON_SET).k(key).s(path).s(json).w(Cmd.XX), Reply.OK_BOOL);
    }

    /**
     * Sends {@code JSON.SET key path json XX EX|PX ttl}: like {@link #jsonSetXx(Object, String, String)}, and a document this call
     * creates gets a time to live. An existing key keeps its deadline.
     *
     * @param key the JSON key
     * @param path where to write
     * @param json the new value as JSON text
     * @param ttl time to live of a newly created key: whole seconds are sent as {@code EX}, anything finer as {@code PX}
     * @return {@code true} when written, {@code false} when the path does not exist
     * @throws IllegalArgumentException when {@code ttl} is null, zero, negative or too large
     * @throws RuvioServerException {@code ERR invalid JSON value} or {@code ERR invalid JSON path}; {@code WRONGTYPE} when the key holds a non-JSON value
     */
    public CompletableFuture<Boolean> jsonSetXx(K key, String path, String json, Duration ttl) {
        return send(args().w(Cmd.JSON_SET).k(key).s(path).s(json).w(Cmd.XX).ttl(ttl), Reply.OK_BOOL);
    }

    /**
     * Sends {@code JSON.MERGE key path patch}: applies an RFC 7396 merge patch. Object members merge recursively, {@code null} members delete fields, and arrays and other values replace the target. This is not RFC 6902 JSON Patch. A missing key can only be created at {@code $}; when the target cannot be reached nothing changes.
     *
     * <pre>{@code
     * client.jsonMerge("user:1", "$", "{\"city\":\"London\",\"tags\":null}");
     * }</pre>
     *
     * @param key the JSON key
     * @param path the target path, usually {@code $}
     * @param patch the patch as JSON text
     * @throws RuvioServerException {@code ERR invalid JSON value} or {@code ERR invalid JSON path}; {@code WRONGTYPE}
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> jsonMerge(K key, String path, String patch) {
        return send(args().w(Cmd.JSON_MERGE).k(key).s(path).s(patch), Reply.OK);
    }

    /**
     * Sends {@code JMERGE key path patch}: the short alias of {@link #jsonMerge(Object, String, String)}.
     *
     * @param key the JSON key
     * @param path the target path, usually {@code $}
     * @param patch the patch as JSON text
     * @throws RuvioServerException {@code ERR invalid JSON value} or {@code ERR invalid JSON path}; {@code WRONGTYPE}
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> jmerge(K key, String path, String patch) {
        return send(args().w(Cmd.JMERGE).k(key).s(path).s(patch), Reply.OK);
    }

    /**
     * Sends {@code JSON.DEL key}: deletes the whole JSON key.
     *
     * @param key the JSON key
     * @return {@code 1} when the key was removed, {@code 0} when it did not exist
     * @throws RuvioServerException {@code WRONGTYPE} when the key holds a non-JSON value
     */
    public CompletableFuture<Long> jsonDel(K key) {
        return k(Cmd.JSON_DEL, key, Reply.LONG);
    }

    /**
     * Sends {@code JSON.DEL key path}: deletes an object field or array element, or the whole key for {@code $}.
     *
     * @param key the JSON key
     * @param path the path to remove
     * @return {@code 1} when something was removed, {@code 0} when the key or path does not exist
     * @throws RuvioServerException {@code ERR invalid JSON path}; {@code WRONGTYPE}
     */
    public CompletableFuture<Long> jsonDel(K key, String path) {
        return ks(Cmd.JSON_DEL, key, path, Reply.LONG);
    }

    /**
     * Sends {@code JSON.FORGET key path}: alias of {@link #jsonDel(Object, String)}.
     *
     * @param key the JSON key
     * @param path the path to remove
     * @return {@code 1} when something was removed, {@code 0} otherwise
     * @throws RuvioServerException {@code ERR invalid JSON path}; {@code WRONGTYPE}
     */
    public CompletableFuture<Long> jsonForget(K key, String path) {
        return ks(Cmd.JSON_FORGET, key, path, Reply.LONG);
    }

    /**
     * Sends {@code JSON.TYPE key path}.
     *
     * @param key the JSON key
     * @param path the path to inspect
     * @return {@code object}, {@code array}, {@code string}, {@code integer}, {@code number}, {@code boolean} or {@code null}; Java {@code null} when the key or path does not exist
     * @throws RuvioServerException {@code ERR invalid JSON path}; {@code WRONGTYPE}
     */
    public CompletableFuture<String> jsonType(K key, String path) {
        return ks(Cmd.JSON_TYPE, key, path, Reply.STRING);
    }

    /**
     * Sends {@code JSON.STRLEN key path}.
     *
     * @param key the JSON key
     * @param path the path of a JSON string
     * @return the string length in UTF-8 bytes, or {@code null} when the key or path does not exist
     * @throws RuvioServerException {@code WRONGTYPE JSON value is not a string}
     */
    public CompletableFuture<Long> jsonStrlen(K key, String path) {
        return ks(Cmd.JSON_STRLEN, key, path, Reply.LONG);
    }

    /**
     * Sends {@code JSON.ARRLEN key path}.
     *
     * @param key the JSON key
     * @param path the path of a JSON array
     * @return the number of elements, or {@code null} when the key or path does not exist
     * @throws RuvioServerException {@code WRONGTYPE JSON value is not an array}
     */
    public CompletableFuture<Long> jsonArrlen(K key, String path) {
        return ks(Cmd.JSON_ARRLEN, key, path, Reply.LONG);
    }

    /**
     * Sends {@code JSON.OBJKEYS key path}.
     *
     * @param key the JSON key
     * @param path the path of a JSON object
     * @return the member names in document order; empty when the key or path does not exist
     * @throws RuvioServerException {@code WRONGTYPE JSON value is not an object}
     */
    public CompletableFuture<List<String>> jsonObjkeys(K key, String path) {
        return ks(Cmd.JSON_OBJKEYS, key, path, Reply.STRING_LIST);
    }

    /**
     * Sends {@code JSON.OBJLEN key path}.
     *
     * @param key the JSON key
     * @param path the path of a JSON object
     * @return the number of members, or {@code null} when the key or path does not exist
     * @throws RuvioServerException {@code WRONGTYPE JSON value is not an object}
     */
    public CompletableFuture<Long> jsonObjlen(K key, String path) {
        return ks(Cmd.JSON_OBJLEN, key, path, Reply.LONG);
    }

    /**
     * Sends {@code JSON.NUMINCRBY key path delta}: adds to a number. Whole numbers stay exact integers when both sides are integers.
     *
     * @param key the JSON key
     * @param path the path of a JSON number
     * @param delta the amount to add (may be negative)
     * @return the new number as JSON text, or {@code null} when the key or path does not exist
     * @throws RuvioServerException {@code WRONGTYPE JSON value is not a number}; {@code ERR result is not a finite number}
     */
    public CompletableFuture<String> jsonNumincrby(K key, String path, double delta) {
        return send(args().w(Cmd.JSON_NUMINCRBY).k(key).s(path).d(delta), Reply.STRING);
    }

    /**
     * Sends {@code JSON.NUMMULTBY key path factor}: multiplies a number.
     *
     * @param key the JSON key
     * @param path the path of a JSON number
     * @param factor the multiplier
     * @return the new number as JSON text, or {@code null} when the key or path does not exist
     * @throws RuvioServerException {@code WRONGTYPE JSON value is not a number}; {@code ERR result is not a finite number}
     */
    public CompletableFuture<String> jsonNummultby(K key, String path, double factor) {
        return send(args().w(Cmd.JSON_NUMMULTBY).k(key).s(path).d(factor), Reply.STRING);
    }

    /**
     * Sends {@code JSON.TOGGLE key path}: flips a boolean.
     *
     * @param key the JSON key
     * @param path the path of a JSON boolean
     * @return the new value; {@code false} also when the key or path does not exist
     * @throws RuvioServerException {@code WRONGTYPE JSON value is not a boolean}
     */
    public CompletableFuture<Boolean> jsonToggle(K key, String path) {
        return ks(Cmd.JSON_TOGGLE, key, path, Reply.BOOL);
    }

    /**
     * Sends {@code JSON.ARRAPPEND key path json [json ...]}: appends values to an array. The cost grows with the array length; for append-heavy data use a list.
     *
     * @param key the JSON key
     * @param path the path of a JSON array
     * @param jsonValues one or more values as JSON text
     * @return the array length after the append, or {@code null} when the key or path does not exist
     * @throws RuvioServerException {@code WRONGTYPE JSON value is not an array}; {@code ERR invalid JSON value}
     */
    public CompletableFuture<Long> jsonArrappend(K key, String path, String... jsonValues) {
        return send(args().w(Cmd.JSON_ARRAPPEND).k(key).s(path).strings(jsonValues), Reply.LONG);
    }

    /**
     * Sends {@code JSON.ARRINSERT key path index json [json ...]}: inserts values before an index.
     *
     * @param key the JSON key
     * @param path the path of a JSON array
     * @param index the position to insert at; negative counts from the end; must not exceed the length
     * @param jsonValues one or more values as JSON text
     * @return the array length after the insert, or {@code null} when the key or path does not exist
     * @throws RuvioServerException {@code ERR index out of range}; {@code WRONGTYPE JSON value is not an array}; {@code ERR invalid JSON value}
     */
    public CompletableFuture<Long> jsonArrinsert(K key, String path, long index, String... jsonValues) {
        return send(args().w(Cmd.JSON_ARRINSERT).k(key).s(path).l(index).strings(jsonValues), Reply.LONG);
    }

    /**
     * Sends {@code JSON.ARRPOP key}: removes and returns the last element of the root array.
     *
     * @param key the JSON key
     * @return the removed element as JSON text, or {@code null} when the array is empty or the key does not exist
     * @throws RuvioServerException {@code WRONGTYPE JSON value is not an array}
     */
    public CompletableFuture<String> jsonArrpop(K key) {
        return k(Cmd.JSON_ARRPOP, key, Reply.STRING);
    }

    /**
     * Sends {@code JSON.ARRPOP key path}: removes and returns the last element of an array.
     *
     * @param key the JSON key
     * @param path the path of a JSON array
     * @return the removed element as JSON text, or {@code null} when the array is empty or the key or path does not exist
     * @throws RuvioServerException {@code WRONGTYPE JSON value is not an array}
     */
    public CompletableFuture<String> jsonArrpop(K key, String path) {
        return ks(Cmd.JSON_ARRPOP, key, path, Reply.STRING);
    }

    /**
     * Sends {@code JSON.ARRPOP key path index}: removes and returns the element at an index.
     *
     * @param key the JSON key
     * @param path the path of a JSON array
     * @param index the position; negative counts from the end; out-of-range values are clamped to the array
     * @return the removed element as JSON text, or {@code null} when the array is empty or the key or path does not exist
     * @throws RuvioServerException {@code WRONGTYPE JSON value is not an array}
     */
    public CompletableFuture<String> jsonArrpop(K key, String path, long index) {
        return send(args().w(Cmd.JSON_ARRPOP).k(key).s(path).l(index), Reply.STRING);
    }

    /**
     * Sends {@code JSON.ARRTRIM key path start stop}: keeps only the elements in a range.
     *
     * @param key the JSON key
     * @param path the path of a JSON array
     * @param start the first element to keep; negative counts from the end
     * @param stop the last element to keep, inclusive; negative counts from the end
     * @return the array length after trimming, or {@code null} when the key or path does not exist
     * @throws RuvioServerException {@code WRONGTYPE JSON value is not an array}
     */
    public CompletableFuture<Long> jsonArrtrim(K key, String path, long start, long stop) {
        return send(args().w(Cmd.JSON_ARRTRIM).k(key).s(path).l(start).l(stop), Reply.LONG);
    }

    /**
     * Sends {@code JSON.ARRINDEX key path scalar}: finds a scalar in an array.
     *
     * @param key the JSON key
     * @param path the path of a JSON array
     * @param scalarJson the value to find as JSON text, for example {@code "\"a\""} or {@code 3}; objects and arrays are refused
     * @return the index of the first match, {@code -1} when not found, or {@code null} when the key or path does not exist
     * @throws RuvioServerException {@code ERR JSON value is not a scalar}; {@code WRONGTYPE JSON value is not an array}
     */
    public CompletableFuture<Long> jsonArrindex(K key, String path, String scalarJson) {
        return send(args().w(Cmd.JSON_ARRINDEX).k(key).s(path).s(scalarJson), Reply.LONG);
    }

    /**
     * Sends {@code JSON.ARRINDEX key path scalar start stop}: like {@link #jsonArrindex(Object, String, String)} within a range.
     *
     * @param key the JSON key
     * @param path the path of a JSON array
     * @param scalarJson the value to find as JSON text
     * @param start the first index to look at; negative counts from the end
     * @param stop the index to stop before; {@code 0} means the end of the array; negative counts from the end
     * @return the index of the first match, {@code -1} when not found, or {@code null} when the key or path does not exist
     * @throws RuvioServerException {@code ERR JSON value is not a scalar}; {@code WRONGTYPE JSON value is not an array}
     */
    public CompletableFuture<Long> jsonArrindex(K key, String path, String scalarJson, long start, long stop) {
        return send(args().w(Cmd.JSON_ARRINDEX).k(key).s(path).s(scalarJson).l(start).l(stop), Reply.LONG);
    }

    /**
     * Sends {@code JSON.CLEAR key path}: empties an array or object, or sets a number to 0.
     *
     * @param key the JSON key
     * @param path the path to clear
     * @return {@code 1} when something was cleared, {@code 0} when the value was already empty, is another type (string, boolean, null) or the path does not exist
     * @throws RuvioServerException {@code ERR invalid JSON path}; {@code WRONGTYPE}
     */
    public CompletableFuture<Long> jsonClear(K key, String path) {
        return ks(Cmd.JSON_CLEAR, key, path, Reply.LONG);
    }

    /**
     * Sends {@code JSON.STRAPPEND key path json-string}: appends to a JSON string.
     *
     * @param key the JSON key
     * @param path the path of a JSON string
     * @param jsonString the text to append, as a JSON string literal including the quotes, for example {@code "\"abc\""}
     * @return the string length in bytes after the append, or {@code null} when the key or path does not exist
     * @throws RuvioServerException {@code WRONGTYPE JSON value is not a string}
     */
    public CompletableFuture<Long> jsonStrappend(K key, String path, String jsonString) {
        return send(args().w(Cmd.JSON_STRAPPEND).k(key).s(path).s(jsonString), Reply.LONG);
    }

    /**
     * Sends the Ruvio command {@code JSON.INDEX CREATE name PREFIX prefix PATH path [TYPE type]}: builds an exact-match index on one scalar field of every JSON key that starts with {@code prefix}. It is built in the background, then follows every write, delete and expiry; postings live in memory and are rebuilt at startup. Database 0 only. With several shards, each shard indexes its own keys ({@code JSON.INDEX LIST} prints one line per shard).
     *
     * <pre>{@code
     * client.jsonIndexCreate("by_status", "order:", "$.status", "string");
     * }</pre>
     *
     * @param name the index name
     * @param prefix only keys starting with this text are indexed
     * @param path the field to index, for example {@code $.status}; the root {@code $} is refused
     * @param type {@code string}, {@code number} or {@code bool}; {@code null} for the default
     * @throws RuvioServerException {@code ERR index already exists}; {@code ERR invalid JSON path}; {@code ERR TYPE must be string, number or bool}; {@code ERR JSON.INDEX CREATE must run in database 0}
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> jsonIndexCreate(String name, String prefix, String path, String type) {
        Args a = args().w(Cmd.JSON_INDEX).s("CREATE").s(name).s("PREFIX").s(prefix).s("PATH").s(path);
        if (type != null) a.w(Cmd.TYPE).s(type);
        return send(a, Reply.OK);
    }

    /**
     * Sends {@code JSON.INDEX DROP name}.
     *
     * @param name the index name
     * @return {@code 1} when dropped, {@code 0} when no such index existed
     * @throws RuvioServerException {@code ERR JSON.INDEX DROP must run in database 0}
     */
    public CompletableFuture<Long> jsonIndexDrop(String name) {
        return send(args().w(Cmd.JSON_INDEX).s("DROP").s(name), Reply.LONG);
    }

    /**
     * Sends {@code JSON.INDEX LIST}.
     *
     * @return one description line per index (per shard), empty when there are none
     */
    public CompletableFuture<List<String>> jsonIndexList() {
        return send(args().w(Cmd.JSON_INDEX).s("LIST"), Reply.STRING_LIST);
    }

    /**
     * Sends the Ruvio command {@code JSON.FIND name VALUE v}: finds JSON keys whose indexed field equals a value. Returns at most 100 keys; use the overload with {@code count} and {@code after} to page.
     *
     * <pre>{@code
     * List<String> keys = client.jsonFind("by_status", "shipped");
     * }</pre>
     *
     * @param index the index name
     * @param value the value to match, as plain text ({@code true}/{@code false} for bool indexes, a number for number indexes)
     * @return matching keys in ascending order, empty when none
     * @throws RuvioServerException {@code ERR no such index}; {@code ERR index is building} until the first build finishes; {@code ERR VALUE does not match the index type}
     */
    public CompletableFuture<List<K>> jsonFind(String index, String value) {
        return send(args().w(Cmd.JSON_FIND).s(index).s("VALUE").s(value), r(Reply.KEY_LIST));
    }

    /**
     * Sends {@code JSON.FIND name VALUE v COUNT n [AFTER key]}: paged lookup.
     *
     * @param index the index name
     * @param value the value to match
     * @param count the page size, from 1 to 1000
     * @param after the last key of the previous page, or {@code null} for the first page
     * @return up to {@code count} matching keys in ascending order, starting after {@code after}
     * @throws RuvioServerException {@code ERR COUNT must be between 1 and 1000}; {@code ERR no such index}; {@code ERR index is building}; {@code ERR VALUE does not match the index type}
     */
    public CompletableFuture<List<K>> jsonFind(String index, String value, long count, K after) {
        Args a = args().w(Cmd.JSON_FIND).s(index).s("VALUE").s(value).w(Cmd.COUNT).l(count);
        if (after != null) a.s("AFTER").k(after);
        return send(a, r(Reply.KEY_LIST));
    }

    // ---- Bloom -----------------------------------------------------------------------------

    /**
     * Sends {@code BF.RESERVE key error capacity}: creates an empty scalable Bloom filter. When it fills up, a larger layer with a lower false-positive rate is added (default growth factor 2).
     *
     * <pre>{@code
     * client.bfReserve("seen:urls", 0.01, 100_000);
     * }</pre>
     *
     * @param key the filter key
     * @param errorRate target false-positive rate between 0 and 1 exclusive, for example {@code 0.01}
     * @param capacity expected number of items; greater than zero
     * @throws RuvioServerException {@code ERR item exists} when the key already exists; {@code ERR invalid Bloom configuration} for a bad rate or capacity (a layer above 64 MiB is refused)
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> bfReserve(K key, double errorRate, long capacity) {
        return send(args().w(Cmd.BF_RESERVE).k(key).d(errorRate).l(capacity), Reply.OK);
    }

    /**
     * Sends {@code BF.RESERVE key error capacity [EXPANSION n | NONSCALING]}.
     *
     * @param key the filter key
     * @param errorRate target false-positive rate between 0 and 1 exclusive
     * @param capacity expected number of items; greater than zero
     * @param expansion growth factor of each new layer; zero or less omits it (default 2)
     * @param nonScaling {@code true} to keep one layer: adds then fail with {@code ERR non scaling filter is full} once it is full
     * @throws RuvioServerException {@code ERR item exists}; {@code ERR invalid Bloom configuration}
     * @return a future that completes with no value once the server has acknowledged the command
     */
    public CompletableFuture<Void> bfReserve(K key, double errorRate, long capacity, long expansion, boolean nonScaling) {
        Args a = args().w(Cmd.BF_RESERVE).k(key).d(errorRate).l(capacity);
        if (nonScaling) {
            a.s("NONSCALING");
        } else if (expansion > 0) {
            a.s("EXPANSION").l(expansion);
        }
        return send(a, Reply.OK);
    }

    /**
     * Sends {@code BF.ADD key item}: adds an item; a missing key is created with error rate 0.01 and capacity 100.
     *
     * @param key the filter key
     * @param item the item
     * @return {@code true} when the item was new, {@code false} when it was probably already present
     * @throws RuvioServerException {@code ERR non scaling filter is full}; {@code WRONGTYPE} when the key holds another type
     */
    public CompletableFuture<Boolean> bfAdd(K key, V item) {
        return kv(Cmd.BF_ADD, key, item, Reply.BOOL);
    }

    /**
     * Sends {@code BF.MADD key item [item ...]}: adds several items (one log record).
     *
     * @param key the filter key
     * @param items one or more items
     * @return one flag per item: {@code true} when new, {@code false} when probably already present
     * @throws RuvioServerException {@code ERR non scaling filter is full}; {@code WRONGTYPE}
     */
    @SafeVarargs
    public final CompletableFuture<List<Boolean>> bfMadd(K key, V... items) {
        return kvs(Cmd.BF_MADD, key, Reply.BOOL_LIST, items);
    }

    /**
     * Sends {@code BF.INSERT key [CAPACITY c] [ERROR e] [EXPANSION n] [NOCREATE] [NONSCALING] ITEMS item ...}: adds items, creating the filter with the given settings when missing.
     *
     * @param key the filter key
     * @param options creation settings; {@code null} for defaults ({@code noCreate()} fails instead of creating)
     * @param items one or more items
     * @return one flag per item: {@code true} when new, {@code false} when probably already present
     * @throws RuvioServerException {@code ERR not found} with {@code noCreate()} on a missing key; {@code ERR non scaling filter is full}; {@code ERR invalid Bloom configuration}
     */
    @SafeVarargs
    public final CompletableFuture<List<Boolean>> bfInsert(K key, BfInsertArgs options, V... items) {
        Args a = args().w(Cmd.BF_INSERT).k(key);
        if (options != null) {
            options.write(a);
        }
        return send(a.s("ITEMS").values(items), Reply.BOOL_LIST);
    }

    /**
     * Sends {@code BF.EXISTS key item}.
     *
     * @param key the filter key
     * @param item the item
     * @return {@code true} when the item is probably present (false positives are possible), {@code false} when it is certainly absent or the key does not exist
     * @throws RuvioServerException {@code WRONGTYPE}
     */
    public CompletableFuture<Boolean> bfExists(K key, V item) {
        return kv(Cmd.BF_EXISTS, key, item, Reply.BOOL);
    }

    /**
     * Sends {@code BF.MEXISTS key item [item ...]}.
     *
     * @param key the filter key
     * @param items one or more items
     * @return one flag per item: {@code true} when probably present, {@code false} when certainly absent
     * @throws RuvioServerException {@code WRONGTYPE}
     */
    @SafeVarargs
    public final CompletableFuture<List<Boolean>> bfMexists(K key, V... items) {
        return kvs(Cmd.BF_MEXISTS, key, Reply.BOOL_LIST, items);
    }

    /**
     * Sends {@code BF.CARD key}.
     *
     * @param key the filter key
     * @return the number of items added, {@code 0} when the key does not exist
     * @throws RuvioServerException {@code WRONGTYPE}
     */
    public CompletableFuture<Long> bfCard(K key) {
        return k(Cmd.BF_CARD, key, Reply.LONG);
    }

    /**
     * Sends {@code BF.INFO key}.
     *
     * @param key the filter key
     * @return a map with {@code Capacity}, {@code Size}, {@code Number of filters}, {@code Number of items inserted} and {@code Expansion rate}
     * @throws RuvioServerException {@code ERR not found} when the key does not exist; {@code WRONGTYPE}
     */
    public CompletableFuture<Map<String, Object>> bfInfo(K key) {
        return k(Cmd.BF_INFO, key, BF_INFO);
    }

    /**
     * Sends {@code BF.INFO key field}: one value of the info.
     *
     * @param key the filter key
     * @param field {@code CAPACITY}, {@code SIZE}, {@code FILTERS}, {@code ITEMS} or {@code EXPANSION}
     * @return the value, or {@code null} for {@code EXPANSION} on a non-scaling filter
     * @throws RuvioServerException {@code ERR not found} when the key does not exist; {@code ERR} for an unknown field
     */
    public CompletableFuture<Long> bfInfo(K key, String field) {
        return ks(Cmd.BF_INFO, key, field, FIRST_LONG);
    }
}
