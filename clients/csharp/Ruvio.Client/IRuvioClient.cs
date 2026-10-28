using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Ruvio.Client;

/// <summary>
/// Commands on one logical database of a connected <see cref="IRuvioConnection"/>.
/// The connection owns the sockets and disposes every client it creates.
/// </summary>
/// <remarks>
/// <para>
/// Every method sends one command (or, for a few, several that the client splits by shard) and returns a
/// <see cref="ValueTask{TResult}"/>. Calls on one connection are multiplexed over a single socket, so many
/// tasks can run at once without waiting for each other (one socket per shard). Only blocking commands (<c>BLPOP</c>, <c>BLMOVE</c>,
/// <c>XREAD BLOCK</c> and similar) hold the socket until they finish.
/// </para>
/// <para>
/// A server error reply becomes a <see cref="RuvioException"/> with <see cref="RuvioException.IsServerReply"/>
/// set; the connection stays usable. Cancelling a <see cref="CancellationToken"/> only abandons the wait: a command
/// that was already sent still runs on the server.
/// </para>
/// <para>
/// With several shards the client routes each key by hash slot, follows <c>MOVED</c> redirects up to 3 times, and
/// splits <c>MGET</c>, <c>MSET</c>, <c>DEL</c>, <c>EXISTS</c>, <c>TOUCH</c> and <c>UNLINK</c> by shard (the parts are not
/// atomic together). Other multi-key commands whose keys fall in different slots throw before anything is sent
/// (<c>CROSSSLOT</c>); use hash tags such as <c>{user:1}:cart</c> to keep related keys in one slot.
/// <c>DBSIZE</c>, <c>FLUSHDB</c> and <c>FLUSHALL</c> run on every shard. Commands without a key go to the seed shard.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// await using var connection = await RuvioConnection.ConnectAsync("localhost", 6379);
/// IRuvioClient db = await connection.GetDefaultDatabaseAsync();
/// await db.SetAsync("greeting", "hello");
/// string? text = await db.GetStringAsync("greeting");
/// </code>
/// </example>
public partial interface IRuvioClient
{
    /// <summary>The logical database this client sends its commands to (the number given to <c>SELECT</c>).</summary>
    int Database { get; }

    /// <summary>The host name or address of the server this client was configured with.</summary>
    string Host { get; }

    /// <summary>The RESP port of the server this client was configured with.</summary>
    int Port { get; }

    /// <summary>
    /// Raised for every pub/sub message that arrives on the subscriber socket, for both channel and pattern
    /// subscriptions. Handlers run while the subscriber socket is being read, so keep them short and do not block in them.
    /// </summary>
    event EventHandler<PubSubMessage>? Message;

    /// <summary>
    /// Sends <c>PING</c> and returns the server's reply. Use it as a liveness check or to measure round-trip time.
    /// </summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><c>"PONG"</c>.</returns>
    /// <example>
    /// <code>
    /// string reply = await db.PingAsync();   // "PONG"
    /// </code>
    /// </example>
    ValueTask<string> PingAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>PING message</c>; the server echoes <paramref name="message"/> back.</summary>
    /// <param name="message">Text to echo.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The same text that was sent.</returns>
    /// <example>
    /// <code>
    /// string echoed = await db.PingAsync("hello");   // "hello"
    /// </code>
    /// </example>
    ValueTask<string> PingAsync(string message, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>GET</c> and returns the value as raw bytes, without any text decoding.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The stored bytes, or <see langword="null"/> when the key does not exist (or has expired). An empty string comes back as an empty array.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Use this overload for binary payloads. For text use <see cref="GetStringAsync"/>; for numbers use <see cref="GetInt64Async"/>.</remarks>
    /// <example>
    /// <code>
    /// byte[]? bytes = await db.GetAsync("blob:1");
    /// </code>
    /// </example>
    ValueTask<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>GET</c> and decodes the value as UTF-8 text.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The string, or <see langword="null"/> when the key does not exist or has expired.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>); or the stored bytes are not valid UTF-8.</exception>
    /// <example>
    /// <code>
    /// await db.SetAsync("greeting", "hello");
    /// string? text = await db.GetStringAsync("greeting");   // "hello"
    /// string? none = await db.GetStringAsync("missing");    // null
    /// </code>
    /// </example>
    ValueTask<string?> GetStringAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>GET</c> and converts the value to <typeparamref name="T"/>.</summary>
    /// <param name="key">The key to read.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The converted value. A missing key gives <c>default(T)</c>: <see langword="null"/> for <see cref="string"/>, <c>byte[]</c> and the nullable forms.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>); the value cannot be converted (not an integer, out of range for <see cref="int"/>, not UTF-8), <typeparamref name="T"/> is not a supported type, or <typeparamref name="T"/> is <c>int</c> or <c>long</c> and the key is missing.</exception>
    /// <remarks>Supported types are <see cref="string"/>, <c>byte[]</c>, <see cref="int"/>, <see cref="long"/> and <c>int?</c> / <c>long?</c>. Integers are read from their decimal text, which is what <c>INCR</c> and <c>SET</c> store. Use <c>GetAsync&lt;long?&gt;</c> or <see cref="GetInt64Async"/> when the key may be absent.</remarks>
    /// <example>
    /// <code>
    /// long? hits = await db.GetAsync&lt;long?&gt;("counter");
    /// string? name = await db.GetAsync&lt;string&gt;("user:1:name");
    /// </code>
    /// </example>
    ValueTask<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>GET</c> and parses the value as a 32-bit decimal integer.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The number, or <see langword="null"/> when the key is missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>); the value is not an integer or does not fit in <see cref="int"/>.</exception>
    /// <example>
    /// <code>
    /// int? stock = await db.GetInt32Async("stock:sku-42");
    /// </code>
    /// </example>
    ValueTask<int?> GetInt32Async(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>GET</c> and parses the value as a 64-bit decimal integer, such as a counter written by <see cref="IncrementAsync(string, CancellationToken)"/>.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The number, or <see langword="null"/> when the key is missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>); the value is not an integer.</exception>
    /// <example>
    /// <code>
    /// long? views = await db.GetInt64Async("page:views");
    /// </code>
    /// </example>
    ValueTask<long?> GetInt64Async(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>SET key value</c>. Creates the key or replaces whatever it held (any type), and clears an existing TTL.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The text to store (UTF-8).</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> once the value is written.</returns>
    /// <remarks>Use the overloads with an expiry or a <see cref="SetMode"/> for a TTL or conditional writes. To keep the current TTL use <see cref="SetKeepingTimeToLiveAsync"/>.</remarks>
    /// <example>
    /// <code>
    /// await db.SetAsync("greeting", "hello");
    /// </code>
    /// </example>
    ValueTask<bool> SetAsync(string key, string value, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>SET key value [PX ms] [NX|XX]</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The text to store (UTF-8).</param>
    /// <param name="expiry">Time to live from now, or <see langword="null"/> for no expiry. Rounded up to whole milliseconds and sent as <c>PX</c>. Must be positive: a negative value throws <see cref="ArgumentOutOfRangeException"/> and zero is refused by the server.</param>
    /// <param name="mode"><see cref="SetMode.Always"/> writes unconditionally (default), <see cref="SetMode.IfNotExists"/> sends <c>NX</c>, <see cref="SetMode.IfExists"/> sends <c>XX</c>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when the value was written; <see langword="false"/> when <c>NX</c> or <c>XX</c> skipped the write.</returns>
    /// <remarks><see cref="SetMode.IfNotExists"/> with an expiry is the basis of a simple lock; <see cref="RuvioClientHelpers.TryAcquireLockAsync"/> adds a safe release.</remarks>
    /// <example>
    /// <code>
    /// bool created = await db.SetAsync("session:ada", "token", TimeSpan.FromMinutes(30), SetMode.IfNotExists);
    /// </code>
    /// </example>
    ValueTask<bool> SetAsync(string key, string value, TimeSpan? expiry, SetMode mode = SetMode.Always, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>SET key value PXAT unixMs [NX|XX]</c>: the key expires at an absolute point in time.</summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The text to store (UTF-8).</param>
    /// <param name="expiresAt">When the key expires. Sent as Unix milliseconds. A time in the past stores a key that is already expired.</param>
    /// <param name="mode"><see cref="SetMode.Always"/>, <see cref="SetMode.IfNotExists"/> (<c>NX</c>) or <see cref="SetMode.IfExists"/> (<c>XX</c>).</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when written; <see langword="false"/> when <c>NX</c> or <c>XX</c> skipped the write.</returns>
    /// <example>
    /// <code>
    /// await db.SetAsync("promo:summer", "on", DateTimeOffset.UtcNow.AddDays(7));
    /// </code>
    /// </example>
    ValueTask<bool> SetAsync(string key, string value, DateTimeOffset expiresAt, SetMode mode = SetMode.Always, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>SET key value KEEPTTL</c>: replaces the value but leaves the key's current expiry untouched.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The text to store (UTF-8).</param>
    /// <param name="mode"><see cref="SetMode.Always"/>, <see cref="SetMode.IfNotExists"/> (<c>NX</c>) or <see cref="SetMode.IfExists"/> (<c>XX</c>).</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when written; <see langword="false"/> when <c>NX</c> or <c>XX</c> skipped the write.</returns>
    /// <example>
    /// <code>
    /// await db.SetKeepingTimeToLiveAsync("session:ada", "refreshed", SetMode.IfExists);
    /// </code>
    /// </example>
    ValueTask<bool> SetKeepingTimeToLiveAsync(string key, string value, SetMode mode = SetMode.Always, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>SET key value [PX ms] [NX|XX] GET</c>: writes the new value and returns the one it replaced, in one step.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The text to store (UTF-8).</param>
    /// <param name="expiry">Optional time to live, rounded up to milliseconds (<c>PX</c>). Negative throws <see cref="ArgumentOutOfRangeException"/>; zero is refused by the server.</param>
    /// <param name="mode"><see cref="SetMode.Always"/> (default), <see cref="SetMode.IfNotExists"/> (<c>NX</c>) or <see cref="SetMode.IfExists"/> (<c>XX</c>).</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The previous string, or <see langword="null"/> when the key did not exist. With <c>NX</c> or <c>XX</c> the previous value is returned even when the write itself was skipped.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>); <c>GET</c> only works on strings.</exception>
    /// <example>
    /// <code>
    /// string? old = await db.SetAndGetAsync("config:mode", "maintenance");
    /// </code>
    /// </example>
    ValueTask<string?> SetAndGetAsync(string key, string value, TimeSpan? expiry = null, SetMode mode = SetMode.Always, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>INCR</c>: adds 1 to the 64-bit integer stored at the key. A missing key starts at 0, so the first call returns 1.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The value after the increment.</returns>
    /// <exception cref="RuvioException">The value is not an integer or the result overflows 64 bits; <c>WRONGTYPE</c> for another type.</exception>
    /// <remarks>An existing TTL is kept. The operation is atomic on the server, so concurrent callers never lose an increment.</remarks>
    /// <example>
    /// <code>
    /// long visits = await db.IncrementAsync("page:views");
    /// </code>
    /// </example>
    ValueTask<long> IncrementAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets a time to live on an existing key: <c>EXPIRE</c> when <paramref name="ttl"/> is a whole number of seconds, otherwise <c>PEXPIRE</c>.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="ttl">Time to live from now, rounded up to milliseconds. Negative throws <see cref="ArgumentOutOfRangeException"/>. Zero deletes the key immediately, as in Redis.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when the key existed and the TTL was set; <see langword="false"/> when the key does not exist.</returns>
    /// <example>
    /// <code>
    /// bool ok = await db.ExpireAsync("session:ada", TimeSpan.FromMinutes(30));
    /// </code>
    /// </example>
    ValueTask<bool> ExpireAsync(string key, TimeSpan ttl, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>DEL</c> for one key.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>1 when the key existed and was removed, 0 when it did not exist.</returns>
    /// <example>
    /// <code>
    /// long removed = await db.DeleteAsync("session:ada");
    /// </code>
    /// </example>
    ValueTask<long> DeleteAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>DEL</c> for several keys and returns how many existed.</summary>
    /// <param name="keys">The keys to delete. Must contain at least one key, otherwise <see cref="ArgumentException"/> is thrown.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The number of keys that existed and were removed (a missing key counts 0). Keys of any type are removed.</returns>
    /// <remarks>Against a multi-shard server the keys may hash to different slots. The client then sends one <c>DEL</c> per slot and adds up the counts. That is not atomic: if one part fails, the others have already run.</remarks>
    /// <example>
    /// <code>
    /// long removed = await db.DeleteAsync(new[] { "a", "b", "c" });
    /// </code>
    /// </example>
    ValueTask<long> DeleteAsync(IReadOnlyList<string> keys, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends any command as words and returns the raw reply. Use it for commands that have no typed method.
    /// </summary>
    /// <param name="arguments">The command name followed by its arguments, for example <c>"HGETALL", "user:1"</c>. At least one word is required.</param>
    /// <returns>The reply as a <see cref="RespValue"/> (<see cref="RespKind"/> tells which shape).</returns>
    /// <exception cref="RuvioException">The server answers with an error (for example an unknown command or wrong arity), or <c>SELECT</c> is sent: a client is bound to one database, so ask the connection for the database instead.</exception>
    /// <remarks>An error reply is thrown as <see cref="RuvioException"/> and the connection stays usable. Against a multi-shard server the key is taken from the command table and the command is routed to the owning shard; a command whose keys span slots fails with <c>CROSSSLOT</c> before anything is sent.</remarks>
    /// <example>
    /// <code>
    /// RespValue reply = await db.ExecuteAsync("OBJECT", "ENCODING", "user:1");
    /// string? encoding = reply.AsString();
    /// </code>
    /// </example>
    ValueTask<RespValue> ExecuteAsync(params string[] arguments);

    /// <summary>
    /// Sends any command as a list of words and returns the raw reply. Same as the <c>params</c> overload, with a cancellation token.
    /// </summary>
    /// <param name="arguments">The command name followed by its arguments. Do not change the list until the call completes.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The reply as a <see cref="RespValue"/>.</returns>
    /// <exception cref="RuvioException">The server answers with an error, or <c>SELECT</c> is sent (see the other overload).</exception>
    /// <example>
    /// <code>
    /// RespValue reply = await db.ExecuteAsync(new[] { "LLEN", "queue" }, token);
    /// long length = reply.Integer;
    /// </code>
    /// </example>
    ValueTask<RespValue> ExecuteAsync(IReadOnlyList<string> arguments, CancellationToken cancellationToken = default);

    /// <summary>Sends a command whose words are raw bytes, so binary-safe values never go through text conversion.</summary>
    /// <param name="arguments">The command name and arguments as byte buffers. Do not modify the memory until the call completes.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The reply as a <see cref="RespValue"/>; read binary bulks from <see cref="RespValue.Bytes"/>.</returns>
    /// <exception cref="RuvioException">The server answers with an error; <see cref="NotSupportedException"/> when the client implementation has no binary support, or (cluster) the command is not one of the routable single-key commands.</exception>
    /// <remarks>In a multi-shard deployment only a fixed set of commands can be routed: single-key string and hash commands, <c>EVAL</c> / <c>EVALSHA</c> with keys in one slot, and <c>DEL</c> / <c>UNLINK</c> / <c>EXISTS</c> (split per slot). Others fail explicitly. Failures are never replayed.</remarks>
    /// <example>
    /// <code>
    /// var args = new ReadOnlyMemory&lt;byte&gt;[] { "SET"u8.ToArray(), "k"u8.ToArray(), new byte[] { 0, 1, 2, 255 } };
    /// await db.ExecuteBinaryAsync(args);
    /// </code>
    /// </example>
    ValueTask<RespValue> ExecuteBinaryAsync(IReadOnlyList<ReadOnlyMemory<byte>> arguments, CancellationToken cancellationToken = default)
        => throw new NotSupportedException("This client does not implement binary commands.");

    /// <summary>
    /// Sends several commands as one pipeline: all are written together and the replies are read back in order, saving round trips.
    /// </summary>
    /// <param name="commands">Each item is one command as a list of words. Must not be empty. Do not change the lists until the call completes.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>One <see cref="RespValue"/> per command, in the same order.</returns>
    /// <exception cref="RuvioException"><see cref="ArgumentException"/> for an empty pipeline; a <c>SELECT</c> command is refused.</exception>
    /// <remarks>Error replies do not throw here: a failed command appears as a <see cref="RespValue"/> of kind <see cref="RespKind.Error"/> at its position. A pipeline is not a transaction (other callers' commands may run between its commands) unless it contains <c>MULTI</c> ... <c>EXEC</c>, which then must stay in one slot. Against a multi-shard server the commands are grouped per shard, sent concurrently, and the replies put back in the original order; <c>MGET</c>, <c>MSET</c>, <c>DEL</c>, <c>EXISTS</c>, <c>UNLINK</c> and <c>TOUCH</c> over several slots are split and merged at their position.</remarks>
    /// <example>
    /// <code>
    /// var replies = await db.ExecuteManyAsync(new[]
    /// {
    ///     new[] { "SET", "a", "1" },
    ///     new[] { "INCR", "a" },
    ///     new[] { "GET", "a" },
    /// });
    /// string? a = replies[2].AsString();   // "2"
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<RespValue>> ExecuteManyAsync(IReadOnlyList<IReadOnlyList<string>> commands, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>LIMIT key max windowMs</c>, Ruvio's fixed-window rate limiter on a single key.</summary>
    /// <param name="key">The key.</param>
    /// <param name="max">Calls allowed per window. Must be positive.</param>
    /// <param name="windowMs">Window length in milliseconds. Must be positive. The window starts with the first accepted call and is the key's lifetime.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The allowance left after this call (<c>max - 1</c> on the first call of a window, then counting down to 0), or <see langword="null"/> when the window is already full. A refused call is not counted.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>); the key holds another type, or <paramref name="max"/> / <paramref name="windowMs"/> is not positive.</exception>
    /// <remarks>Native Ruvio command, not in Redis. For a typed result use <see cref="RuvioClientHelpers.RateLimitAsync"/>.</remarks>
    /// <example>
    /// <code>
    /// long? left = await db.LimitAsync("rate:user:42", 100, 60_000);
    /// if (left is null) { /* over the limit, try again after the window */ }
    /// </code>
    /// </example>
    ValueTask<long?> LimitAsync(string key, long max, long windowMs, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>LIMIT key max windowMs SLIDING</c>, a sliding-window rate limiter on a single key.</summary>
    /// <param name="key">The key.</param>
    /// <param name="max">Calls allowed per window, from 1 to 2147483647.</param>
    /// <param name="windowMs">Window length in milliseconds. Must be positive.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The allowance left after this call, or <see langword="null"/> when it does not fit. A refused call is not counted.</returns>
    /// <exception cref="RuvioException">The key holds another type (<c>WRONGTYPE</c>), the key was created by another <c>LIMIT</c> mode, or an argument is out of range.</exception>
    /// <remarks>The previous window counts for the part of it that still overlaps the last <paramref name="windowMs"/>, so a burst at a window edge is not doubled. The key keeps two counters, not one entry per call.</remarks>
    /// <example>
    /// <code>
    /// long? left = await db.LimitSlidingAsync("rate:user:42", 100, 60_000);
    /// </code>
    /// </example>
    ValueTask<long?> LimitSlidingAsync(string key, long max, long windowMs, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>LIMIT key capacity refillMs TOKENS COST cost</c>, a token bucket on a single key.</summary>
    /// <param name="key">The key.</param>
    /// <param name="capacity">Bucket size, from 1 to 1000000.</param>
    /// <param name="refillMs">Time to refill an empty bucket in milliseconds, up to 4294967295.</param>
    /// <param name="cost">Tokens this call takes, from 1 to <paramref name="capacity"/>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The tokens left after this call, or <see langword="null"/> when the bucket holds too few. Nothing is taken then.</returns>
    /// <exception cref="RuvioException">The key holds another type (<c>WRONGTYPE</c>), the key was created by another <c>LIMIT</c> mode, or an argument is out of range.</exception>
    /// <remarks>The key keeps one timestamp and disappears when the bucket is full again.</remarks>
    /// <example>
    /// <code>
    /// long? left = await db.LimitTokensAsync("rate:api:7", 10, 1_000, 2); // 10 per second, this call takes 2
    /// </code>
    /// </example>
    ValueTask<long?> LimitTokensAsync(string key, long capacity, long refillMs, long cost = 1, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>LIMIT key max windowMs SLIDING WITHRETRY</c>: <see cref="LimitSlidingAsync"/> that also says how long to wait.</summary>
    /// <param name="key">The key.</param>
    /// <param name="max">Calls allowed per window, from 1 to 2147483647.</param>
    /// <param name="windowMs">Window length in milliseconds. Must be positive.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>A <see cref="LimitOutcome"/> with the wait until the same call would pass.</returns>
    /// <exception cref="RuvioException">As <see cref="LimitSlidingAsync"/>.</exception>
    ValueTask<LimitOutcome> LimitSlidingWithRetryAsync(string key, long max, long windowMs, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>LIMIT key capacity refillMs TOKENS COST cost WITHRETRY</c>: <see cref="LimitTokensAsync"/> that also says how long to wait.</summary>
    /// <param name="key">The key.</param>
    /// <param name="capacity">Bucket size, from 1 to 1000000.</param>
    /// <param name="refillMs">Time to refill an empty bucket in milliseconds, up to 4294967295.</param>
    /// <param name="cost">Tokens this call takes, from 1 to <paramref name="capacity"/>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>A <see cref="LimitOutcome"/> with the wait until the same call would pass.</returns>
    /// <exception cref="RuvioException">As <see cref="LimitTokensAsync"/>.</exception>
    ValueTask<LimitOutcome> LimitTokensWithRetryAsync(string key, long capacity, long refillMs, long cost = 1, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>ONCE key ttlMs</c>, Ruvio's "first caller wins" marker with an expiry. Use it to run something at most once per period.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="ttlMs">How long the marker lasts, in milliseconds. Must be positive.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>1 for the first call (the marker is created); 0 for later calls until the marker expires. A later call does not refresh the TTL.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>); the key holds a different kind of value, or <paramref name="ttlMs"/> is not positive.</exception>
    /// <remarks>Native Ruvio command. For idempotent request handling that also stores a result, see <see cref="BeginIdempotencyAsync"/>.</remarks>
    /// <example>
    /// <code>
    /// if (await db.OnceAsync("welcome-mail:42", 86_400_000) == 1) { /* send the mail */ }
    /// </code>
    /// </example>
    ValueTask<long> OnceAsync(string key, long ttlMs, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>IDEM key BEGIN fingerprint owner ttlMs</c>: reserves an idempotency key for one request, or reports what happened to an earlier attempt. Low-level form; <see cref="TryBeginIdempotencyAsync"/> returns a typed result.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="fingerprint">Digest of the request that identifies it (1 to 128 bytes). A retry of the same request must send the same fingerprint.</param>
    /// <param name="owner">Token that identifies this attempt (1 to 128 bytes); only the same owner may complete the record.</param>
    /// <param name="ttlMs">How long the record is kept, 1 to 604800000 (7 days). Later calls do not extend it.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>A <see cref="RespValue"/> array: <c>["acquired"]</c> (this caller owns the work), <c>["pending"]</c> (another attempt is running), <c>["conflict"]</c> (same key, different fingerprint) or <c>["completed", result]</c> with the stored result.</returns>
    /// <exception cref="RuvioException">The arguments are out of range (empty or over 128 bytes, TTL outside 1 to 7 days).</exception>
    /// <remarks>Reserve the key space for this command: ordinary <c>SET</c>/<c>DEL</c>/expiry commands on an IDEM key break its guarantees. It is not exactly-once for side effects outside Ruvio.</remarks>
    /// <example>
    /// <code>
    /// RespValue reply = await db.BeginIdempotencyAsync("order:42", requestDigest, attemptId, 60_000);
    /// </code>
    /// </example>
    ValueTask<RespValue> BeginIdempotencyAsync(string key, string fingerprint, string owner, long ttlMs, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>IDEM key COMPLETE fingerprint owner result</c>: stores the outcome of the request that <see cref="BeginIdempotencyAsync"/> reserved.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="fingerprint">The same fingerprint used in the begin call.</param>
    /// <param name="owner">The same owner token used in the begin call.</param>
    /// <param name="result">The result to store for replays, up to 4 MiB.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when the result was stored, or an identical completion was repeated; <see langword="false"/> when the record is missing or expired, the owner or fingerprint differs, or a different result is already stored.</returns>
    /// <exception cref="RuvioException">The reply is not the expected 0 or 1, or an argument is out of range.</exception>
    /// <remarks>Completing does not extend the record's lifetime.</remarks>
    /// <example>
    /// <code>
    /// bool saved = await db.CompleteIdempotencyAsync("order:42", requestDigest, attemptId, "receipt-9001");
    /// </code>
    /// </example>
    ValueTask<bool> CompleteIdempotencyAsync(string key, string fingerprint, string owner, string result, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>TAKE key count</c>: subtracts from an integer only when the key holds at least that much. Use it for stock, credits or tokens that must never go negative.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="count">Amount to take. Must be positive.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The amount left after the subtraction, or <see langword="null"/> when the key is missing or holds less than <paramref name="count"/> (nothing is changed or created).</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>); the value is not an integer, or <paramref name="count"/> is not positive.</exception>
    /// <remarks>Native Ruvio command. The subtraction is atomic, so concurrent callers cannot oversell.</remarks>
    /// <example>
    /// <code>
    /// long? left = await db.TakeAsync("stock:sku-42", 1);
    /// if (left is null) { /* sold out */ }
    /// </code>
    /// </example>
    ValueTask<long?> TakeAsync(string key, long count, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>TRANSFER source destination amount [MAX max]</c>: moves an amount between two integer keys in one step, only when <paramref name="source"/> holds at least that much. A missing <paramref name="destination"/> starts at 0. Both writes are one WAL record.
    /// </summary>
    /// <param name="source">The key to subtract from.</param>
    /// <param name="destination">The key to add to. On a sharded server it must share a slot with <paramref name="source"/>; a hash tag such as <c>{user:42}</c> puts them together.</param>
    /// <param name="amount">Amount to move. Must be positive.</param>
    /// <param name="max">Optional ceiling for <paramref name="destination"/> after the move.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The balances after the move, or <see langword="null"/> when <paramref name="source"/> is missing or short, or <paramref name="destination"/> would overflow or pass <paramref name="max"/> (nothing is written).</returns>
    /// <exception cref="RuvioException">A key holds a value of another type (<c>WRONGTYPE</c>); a value is not an integer; <paramref name="amount"/> is not positive; the two keys are the same; the keys are in different slots (<c>CROSSSLOT</c>).</exception>
    /// <remarks>Native Ruvio command. The check and both writes are atomic, so concurrent callers cannot overdraw.</remarks>
    /// <example>
    /// <code>
    /// var moved = await db.TransferAsync("{user:42}:wallet", "{user:42}:escrow", 30);
    /// if (moved is null) { /* not enough balance */ }
    /// </code>
    /// </example>
    ValueTask<(long Source, long Destination)?> TransferAsync(string source, string destination, long amount, long? max = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>HTRANSFER key from to amount [MAX max]</c>: <see cref="TransferAsync"/> between two integer fields of one hash. A missing <paramref name="to"/> field starts at 0 and field TTLs stay.
    /// </summary>
    /// <param name="key">The hash key.</param>
    /// <param name="from">The field to subtract from.</param>
    /// <param name="to">The field to add to.</param>
    /// <param name="amount">Amount to move. Must be positive.</param>
    /// <param name="max">Optional ceiling for the <paramref name="to"/> field after the move.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The balances after the move, or <see langword="null"/> when the key or <paramref name="from"/> is missing or short, or <paramref name="to"/> would overflow or pass <paramref name="max"/> (nothing is written).</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>); a field is not an integer; <paramref name="amount"/> is not positive; the two fields are the same.</exception>
    /// <example>
    /// <code>
    /// var moved = await db.TransferHashFieldAsync("account:7", "available", "held", 30);
    /// </code>
    /// </example>
    ValueTask<(long From, long To)?> TransferHashFieldAsync(string key, string from, string to, long amount, long? max = null, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>GETEX key ttlMs</c>: reads a string and sets its expiry in one step ("sliding" expiry).</summary>
    /// <param name="key">The key.</param>
    /// <param name="ttlMs">New time to live from now, in milliseconds. Must be positive. (Ruvio takes milliseconds here, not Redis's <c>EX</c>/<c>PX</c> options.)</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The string, or <see langword="null"/> when the key is missing (nothing is stored).</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>); the key holds another type, or <paramref name="ttlMs"/> is not positive.</exception>
    /// <example>
    /// <code>
    /// string? session = await db.GetExpireAsync("session:ada", 1_800_000);
    /// </code>
    /// </example>
    ValueTask<string?> GetExpireAsync(string key, long ttlMs, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>LEASE key ttlMs</c>: acquires an exclusive lease with a fencing token.</summary>
    /// <param name="key">The key.</param>
    /// <param name="ttlMs">How long the lease is held, in milliseconds. Must be positive.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>A fencing token (it only grows for one key; expiry and release keep the sequence), or <see langword="null"/> while another holder's lease is still live.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>); the key holds another kind of value, or <paramref name="ttlMs"/> is not positive.</exception>
    /// <remarks>Pass the token to the resource you guard and reject smaller ones. Free the lease with <see cref="ReleaseAsync"/>, or use <see cref="RuvioClientHelpers.TryAcquireLeaseAsync"/> for an <c>await using</c> wrapper.</remarks>
    /// <example>
    /// <code>
    /// long? token = await db.LeaseAsync("lease:report-job", 30_000);
    /// </code>
    /// </example>
    ValueTask<long?> LeaseAsync(string key, long ttlMs, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>SEMAPHORE key max ttlMs</c>: takes one of <paramref name="max"/> slots, each with its own expiry.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="max">Number of holders allowed at once. Must be positive.</param>
    /// <param name="ttlMs">How long this holder keeps the slot, in milliseconds. Must be positive.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>A token for this slot, or <see langword="null"/> when <paramref name="max"/> holders are live.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>); the key holds another kind of value, or an argument is not positive.</exception>
    /// <remarks>Release the slot with <see cref="ReleaseAsync"/> using the same token, or use <see cref="RuvioClientHelpers.TryAcquireSemaphoreAsync"/>.</remarks>
    /// <example>
    /// <code>
    /// long? slot = await db.AcquireSemaphoreAsync("sem:uploads", 4, 60_000);
    /// </code>
    /// </example>
    ValueTask<long?> AcquireSemaphoreAsync(string key, long max, long ttlMs, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>INCRBY key delta MAX limit</c>: adds to a counter only while the result stays at or below a ceiling.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="delta">Amount to add.</param>
    /// <param name="limit">Highest value the counter may reach.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The new value, or <see langword="null"/> when the result would exceed <paramref name="limit"/> (the key is left unchanged).</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>); the value is not an integer.</exception>
    /// <remarks>Ruvio extension of <c>INCRBY</c>. Use it for quotas and capacity counters.</remarks>
    /// <example>
    /// <code>
    /// long? seats = await db.IncrementWithLimitAsync("event:7:booked", 1, 100);
    /// </code>
    /// </example>
    ValueTask<long?> IncrementWithLimitAsync(string key, long delta, long limit, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>RENEW key token ttl</c>: moves the deadline of a lease from <see cref="LeaseAsync"/> or a semaphore slot from <see cref="AcquireSemaphoreAsync"/> to now + <paramref name="ttlMilliseconds"/>. The token does not change and no holder is created.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="token">The token that acquire returned.</param>
    /// <param name="ttlMilliseconds">The new lifetime in milliseconds (positive). It replaces the remaining time.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>1 when the token held the lease or slot and its deadline moved; 0 when it no longer held it (expired, released or taken by someone else), in which case nothing changes.</returns>
    /// <exception cref="RuvioException">The server refuses the command, for example the key holds another type.</exception>
    ValueTask<long> RenewAsync(string key, long token, long ttlMilliseconds, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>RELEASE key token</c>: gives back a lease from <see cref="LeaseAsync"/> or a semaphore slot from <see cref="AcquireSemaphoreAsync"/>.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="token">The token that acquire returned.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>1 when the token held the lease or slot and it was released; 0 when the token no longer held it (expired or taken by someone else), in which case nothing changes.</returns>
    /// <example>
    /// <code>
    /// long freed = await db.ReleaseAsync("lease:report-job", token);
    /// </code>
    /// </example>
    ValueTask<long> ReleaseAsync(string key, long token, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>SETV key value</c>: writes a versioned string and starts it at version 1.</summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The text to store.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The new version number (1 for a fresh key).</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>); the key holds another type.</exception>
    /// <remarks>Native Ruvio command for optimistic concurrency. A plain <c>SET</c> replaces a versioned value and drops its version.</remarks>
    /// <example>
    /// <code>
    /// long version = await db.SetVersionAsync("doc:1", "draft");   // 1
    /// </code>
    /// </example>
    ValueTask<long> SetVersionAsync(string key, string value, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>SETV key value version</c>: writes only when <paramref name="version"/> is still the current version (compare-and-set).
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The text to store.</param>
    /// <param name="version">The version you last read (the number returned by the previous <c>SetVersionAsync</c>).</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The next version number.</returns>
    /// <exception cref="RuvioException">The key's current version differs from <paramref name="version"/> (the value is left unchanged), or the key holds another type.</exception>
    /// <example>
    /// <code>
    /// long v1 = await db.SetVersionAsync("doc:1", "draft");
    /// long v2 = await db.SetVersionAsync("doc:1", "final", v1);   // 2
    /// </code>
    /// </example>
    ValueTask<long> SetVersionAsync(string key, string value, long version, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>CHANGES START</c>: switches on this shard's in-memory change feed and returns the starting cursor.
    /// </summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The cursor to pass to <see cref="ReadChangesAsync"/>.</returns>
    /// <remarks>Nothing is recorded before this call. The feed is kept in memory per shard (the newest 1024 notes) and is empty after a restart; it is not the durable change stream.</remarks>
    /// <example>
    /// <code>
    /// long cursor = await db.StartChangesAsync();
    /// </code>
    /// </example>
    ValueTask<long> StartChangesAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>CHANGES cursor</c>: reads the notes recorded after <paramref name="cursor"/>.</summary>
    /// <param name="cursor">A cursor from <see cref="StartChangesAsync"/> or the <see cref="ChangeNote.Sequence"/> of the last note you processed.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The notes after the cursor in order, each with its sequence number and text such as <c>SET key</c>. Empty when there is nothing new, or the feed was never started.</returns>
    /// <exception cref="RuvioException">The reply is not an array of notes.</exception>
    /// <example>
    /// <code>
    /// foreach (ChangeNote note in await db.ReadChangesAsync(cursor))
    /// {
    ///     cursor = note.Sequence;
    ///     Console.WriteLine(note.Text);
    /// }
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<ChangeNote>> ReadChangesAsync(long cursor, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>MGET</c>: reads several strings in one round trip.</summary>
    /// <param name="keys">The keys to read.</param>
    /// <returns>One entry per key, in key order; <see langword="null"/> for a key that is missing or does not hold a string (no <c>WRONGTYPE</c> error).</returns>
    /// <remarks>Against a multi-shard server keys in different slots are fetched with one <c>MGET</c> per slot and merged back in key order (not atomic). Use a <c>{tag}</c> to keep keys on one shard.</remarks>
    /// <example>
    /// <code>
    /// IReadOnlyList&lt;string?&gt; values = await db.GetManyAsync("a", "b", "c");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string?>> GetManyAsync(params string[] keys);

    /// <summary>Sends <c>MSET</c>: writes several strings in one round trip.</summary>
    /// <param name="pairs">The key/value pairs to write.</param>
    /// <remarks>Replaces existing values and clears their TTLs. Against a multi-shard server pairs in different slots are sent as one <c>MSET</c> per slot, so the whole call is not atomic. Use <see cref="SetManyIfNotExistsAsync"/> for all-or-nothing creation on one slot.</remarks>
    /// <example>
    /// <code>
    /// await db.SetManyAsync(("a", "1"), ("b", "2"));
    /// </code>
    /// </example>
    ValueTask SetManyAsync(params (string Key, string Value)[] pairs);

    /// <summary>
    /// Sends <c>GETSET</c>: stores a new value and returns the old one. Prefer <see cref="SetAndGetAsync"/>, which has expiry and condition options.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The new text.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The previous string, or <see langword="null"/> when the key did not exist.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Clears any TTL on the key.</remarks>
    /// <example>
    /// <code>
    /// string? previous = await db.GetSetAsync("token", "new");
    /// </code>
    /// </example>
    ValueTask<string?> GetSetAsync(string key, string value, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>JSON.MGET</c>: reads the same path from several JSON documents.</summary>
    /// <param name="path">The JSON path to read from every key.</param>
    /// <param name="keys">The JSON keys. They must all hash to the same slot (use a <c>{tag}</c>).</param>
    /// <returns>One entry per key, in order: the JSON text at the path, or <see langword="null"/> for a missing key or a key that is not JSON.</returns>
    /// <exception cref="RuvioException">The keys span several slots on a multi-shard server (<c>CROSSSLOT</c>, raised before anything is sent).</exception>
    /// <example>
    /// <code>
    /// var names = await db.JsonGetManyAsync("$.name", "{u}:1", "{u}:2");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string?>> JsonGetManyAsync(string path, params string[] keys);

    /// <summary>Sends <c>JSON.GET</c>: reads a JSON document or a part of it.</summary>
    /// <param name="key">The key.</param>
    /// <param name="path">A JSON path such as <c>$</c> (the whole document), <c>$.name</c> or <c>$.items[0]</c>. Only simple <c>.field</c> and <c>[N]</c> steps are supported.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>Compact JSON text, or <see langword="null"/> when the key or path does not exist.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>); the key does not hold JSON.</exception>
    /// <remarks>Only simple <c>.field</c> and <c>[N]</c> paths are supported, not the full RedisJSON path language.</remarks>
    /// <example>
    /// <code>
    /// string? json = await db.JsonGetAsync("user:1");              // {"name":"Ada","age":36}
    /// string? name = await db.JsonGetAsync("user:1", "$.name");     // "Ada"
    /// </code>
    /// </example>
    ValueTask<string?> JsonGetAsync(string key, string path = "$", CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>JSON.SET</c>: writes a JSON value at a path. Use path <c>$</c> to create or replace the whole document.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="path">A JSON path such as <c>$</c> (the whole document), <c>$.name</c> or <c>$.items[0]</c>. Only simple <c>.field</c> and <c>[N]</c> steps are supported.</param>
    /// <param name="json">The value as JSON text, for example <c>{"a":1}</c> or <c>"text"</c> (strings need their quotes).</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when written; <see langword="false"/> when the path cannot be reached (nothing was written).</returns>
    /// <exception cref="RuvioException"><paramref name="json"/> is not valid JSON, the document would exceed 16 MiB, or the key holds a non-JSON value.</exception>
    /// <remarks>Only existing intermediate nodes are created through; an array path with index equal to the length appends. For typed values use <see cref="JsonSetValueAsync{T}(string, string, T, System.Text.Json.JsonSerializerOptions?, CancellationToken)"/>.</remarks>
    /// <example>
    /// <code>
    /// await db.JsonSetAsync("user:1", "$", """{"name":"Ada","age":36}""");
    /// await db.JsonSetAsync("user:1", "$.age", "37");
    /// </code>
    /// </example>
    ValueTask<bool> JsonSetAsync(string key, string path, string json, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>JSON.MERGE</c>: applies an RFC 7396 merge patch at a path. Fields in the patch are added or replaced, <c>null</c> removes a field, and other fields stay.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="path">A JSON path such as <c>$</c> (the whole document), <c>$.name</c> or <c>$.items[0]</c>. Only simple <c>.field</c> and <c>[N]</c> steps are supported.</param>
    /// <param name="patch">The merge patch as JSON text.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when applied; <see langword="false"/> when the path cannot be reached.</returns>
    /// <exception cref="RuvioException"><paramref name="patch"/> is not valid JSON, or the key holds a non-JSON value.</exception>
    /// <example>
    /// <code>
    /// await db.JsonMergeAsync("user:1", "$", """{"age":37,"nickname":null}""");
    /// </code>
    /// </example>
    ValueTask<bool> JsonMergeAsync(string key, string path, string patch, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>JSON.DEL</c>: removes a field, array element or the whole document.</summary>
    /// <param name="key">The key.</param>
    /// <param name="path">A JSON path such as <c>$</c> (the whole document), <c>$.name</c> or <c>$.items[0]</c>. Only simple <c>.field</c> and <c>[N]</c> steps are supported.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>1 when something was removed, otherwise 0. Deleting path <c>$</c> removes the key.</returns>
    /// <example>
    /// <code>
    /// long removed = await db.JsonDeleteAsync("user:1", "$.nickname");
    /// </code>
    /// </example>
    ValueTask<long> JsonDeleteAsync(string key, string path = "$", CancellationToken cancellationToken = default);

    /// <summary>Sends <c>JSON.TYPE</c>: names the JSON type at a path.</summary>
    /// <param name="key">The key.</param>
    /// <param name="path">A JSON path such as <c>$</c> (the whole document), <c>$.name</c> or <c>$.items[0]</c>. Only simple <c>.field</c> and <c>[N]</c> steps are supported.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><c>object</c>, <c>array</c>, <c>string</c>, <c>integer</c>, <c>number</c>, <c>boolean</c> or <c>null</c>; <see langword="null"/> when the key or path does not exist.</returns>
    /// <example>
    /// <code>
    /// string? kind = await db.JsonTypeAsync("user:1", "$.age");   // "integer"
    /// </code>
    /// </example>
    ValueTask<string?> JsonTypeAsync(string key, string path = "$", CancellationToken cancellationToken = default);

    /// <summary>Sends <c>JSON.STRLEN</c>: the length of a JSON string value.</summary>
    /// <param name="key">The key.</param>
    /// <param name="path">A JSON path such as <c>$</c> (the whole document), <c>$.name</c> or <c>$.items[0]</c>. Only simple <c>.field</c> and <c>[N]</c> steps are supported.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The byte length of the string, or <see langword="null"/> when the key or path does not exist.</returns>
    /// <exception cref="RuvioException">The value at the path is not a string.</exception>
    /// <example>
    /// <code>
    /// long? length = await db.JsonStringLengthAsync("user:1", "$.name");
    /// </code>
    /// </example>
    ValueTask<long?> JsonStringLengthAsync(string key, string path = "$", CancellationToken cancellationToken = default);

    /// <summary>Sends <c>JSON.ARRLEN</c>: the number of elements of a JSON array.</summary>
    /// <param name="key">The key.</param>
    /// <param name="path">A JSON path such as <c>$</c> (the whole document), <c>$.name</c> or <c>$.items[0]</c>. Only simple <c>.field</c> and <c>[N]</c> steps are supported.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The element count, or <see langword="null"/> when the key or path does not exist.</returns>
    /// <exception cref="RuvioException">The value at the path is not an array.</exception>
    /// <example>
    /// <code>
    /// long? count = await db.JsonArrayLengthAsync("cart:7", "$.items");
    /// </code>
    /// </example>
    ValueTask<long?> JsonArrayLengthAsync(string key, string path = "$", CancellationToken cancellationToken = default);

    /// <summary>Sends <c>JSON.OBJKEYS</c>: the member names of a JSON object.</summary>
    /// <param name="key">The key.</param>
    /// <param name="path">A JSON path such as <c>$</c> (the whole document), <c>$.name</c> or <c>$.items[0]</c>. Only simple <c>.field</c> and <c>[N]</c> steps are supported.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The names in document order. Empty when the key or path does not exist.</returns>
    /// <exception cref="RuvioException">The value at the path is not an object.</exception>
    /// <example>
    /// <code>
    /// IReadOnlyList&lt;string&gt; names = await db.JsonObjectKeysAsync("user:1");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> JsonObjectKeysAsync(string key, string path = "$", CancellationToken cancellationToken = default);

    /// <summary>Sends <c>JSON.NUMINCRBY</c>: adds a number to a numeric value inside the document.</summary>
    /// <param name="key">The key.</param>
    /// <param name="path">The path of the number.</param>
    /// <param name="by">Amount to add. Negative values subtract.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The new number as JSON text (<c>"38"</c>, <c>"2.5"</c>); <see langword="null"/> when the key or path does not exist. Integer arithmetic is used while both operands are integers and the result fits.</returns>
    /// <exception cref="RuvioException">The value at the path is not a number.</exception>
    /// <example>
    /// <code>
    /// string? age = await db.JsonIncrementAsync("user:1", "$.age", 1);
    /// </code>
    /// </example>
    ValueTask<string?> JsonIncrementAsync(string key, string path, double by, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>JSON.TOGGLE</c>: flips a boolean value.</summary>
    /// <param name="key">The key.</param>
    /// <param name="path">A JSON path such as <c>$</c> (the whole document), <c>$.name</c> or <c>$.items[0]</c>. Only simple <c>.field</c> and <c>[N]</c> steps are supported.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The new value, or <see langword="null"/> when the key or path does not exist.</returns>
    /// <exception cref="RuvioException">The value at the path is not a boolean.</exception>
    /// <example>
    /// <code>
    /// bool? active = await db.JsonToggleAsync("user:1", "$.active");
    /// </code>
    /// </example>
    ValueTask<bool?> JsonToggleAsync(string key, string path = "$", CancellationToken cancellationToken = default);

    /// <summary>Sends <c>JSON.ARRAPPEND</c> with one value: adds it to the end of an array.</summary>
    /// <param name="key">The key.</param>
    /// <param name="path">The path of the array.</param>
    /// <param name="json">The element as JSON text (strings need quotes).</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The new array length, or <see langword="null"/> when the key or path does not exist.</returns>
    /// <exception cref="RuvioException">The value at the path is not an array.</exception>
    /// <remarks>Cost grows with the array length; for append-heavy data use a list (<see cref="PushListRightAsync"/>).</remarks>
    /// <example>
    /// <code>
    /// long? length = await db.JsonArrayAppendAsync("cart:7", "$.items", """{"sku":"a1","qty":2}""");
    /// </code>
    /// </example>
    ValueTask<long?> JsonArrayAppendAsync(string key, string path, string json, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>JSON.CLEAR</c>: empties an array or object, or sets a number to 0.</summary>
    /// <param name="key">The key.</param>
    /// <param name="path">A JSON path such as <c>$</c> (the whole document), <c>$.name</c> or <c>$.items[0]</c>. Only simple <c>.field</c> and <c>[N]</c> steps are supported.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>How many values were changed; 0 when the value was already empty or cannot be cleared.</returns>
    /// <example>
    /// <code>
    /// long cleared = await db.JsonClearAsync("cart:7", "$.items");
    /// </code>
    /// </example>
    ValueTask<long> JsonClearAsync(string key, string path = "$", CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads a JSON value and deserializes it to <typeparamref name="T"/> using reflection-based <c>System.Text.Json</c>.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="path">The JSON path; <c>$</c> for the whole document.</param>
    /// <param name="options">Serializer options, or <see langword="null"/> to use <see cref="RuvioClientOptions.JsonSerializerOptions"/> of the connection.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The value, or <c>default</c> when the key or path does not exist.</returns>
    /// <exception cref="RuvioException">The text cannot be deserialized to <typeparamref name="T"/>; wrongtype.</exception>
    /// <remarks>Not trim or AOT safe. Use the <see cref="System.Text.Json.Serialization.Metadata.JsonTypeInfo{T}"/> overload in trimmed apps.</remarks>
    /// <example>
    /// <code>
    /// User? user = await db.JsonGetValueAsync&lt;User&gt;("user:1");
    /// </code>
    /// </example>
    [RequiresUnreferencedCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    [RequiresDynamicCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    ValueTask<T?> JsonGetValueAsync<T>(string key, string path = "$", JsonSerializerOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>Reads a JSON value and deserializes it with source-generated metadata (trimming and AOT safe).</summary>
    /// <param name="key">The key.</param>
    /// <param name="path">The JSON path; <c>$</c> for the whole document.</param>
    /// <param name="typeInfo">Metadata from your <c>JsonSerializerContext</c>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The value, or <c>default</c> when the key or path does not exist.</returns>
    /// <exception cref="RuvioException">The text cannot be deserialized; wrongtype.</exception>
    /// <example>
    /// <code>
    /// User? user = await db.JsonGetValueAsync("user:1", "$", AppJson.Default.User);
    /// </code>
    /// </example>
    ValueTask<T?> JsonGetValueAsync<T>(string key, string path, JsonTypeInfo<T> typeInfo, CancellationToken cancellationToken = default);

    /// <summary>
    /// Serializes <paramref name="value"/> to JSON and writes it at <paramref name="path"/> (<c>JSON.SET</c>). A plain string <c>"izmir"</c> is stored as <c>"izmir"</c>, with quotes.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="path">The JSON path; <c>$</c> to replace the whole document.</param>
    /// <param name="value">A string, number, bool, array or object.</param>
    /// <param name="options">Serializer options, or <see langword="null"/> for the connection's <see cref="RuvioClientOptions.JsonSerializerOptions"/>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when written; <see langword="false"/> when the path cannot be reached.</returns>
    /// <exception cref="RuvioException">The value cannot be serialized; the key holds a non-JSON value.</exception>
    /// <remarks>Not trim or AOT safe; use the <see cref="System.Text.Json.Serialization.Metadata.JsonTypeInfo{T}"/> overload there.</remarks>
    /// <example>
    /// <code>
    /// await db.JsonSetValueAsync("user:1", "$", new User("Ada", 36));
    /// </code>
    /// </example>
    [RequiresUnreferencedCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    [RequiresDynamicCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    ValueTask<bool> JsonSetValueAsync<T>(string key, string path, T value, JsonSerializerOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Serializes <paramref name="value"/> with source-generated metadata and writes it at <paramref name="path"/> (<c>JSON.SET</c>). Trimming and AOT safe.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="path">The JSON path; <c>$</c> to replace the whole document.</param>
    /// <param name="value">The value to store.</param>
    /// <param name="typeInfo">Metadata from your <c>JsonSerializerContext</c>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when written; <see langword="false"/> when the path cannot be reached.</returns>
    /// <example>
    /// <code>
    /// await db.JsonSetValueAsync("user:1", "$", user, AppJson.Default.User);
    /// </code>
    /// </example>
    ValueTask<bool> JsonSetValueAsync<T>(string key, string path, T value, JsonTypeInfo<T> typeInfo, CancellationToken cancellationToken = default);

    /// <summary>Serializes <paramref name="patch"/> and sends it as a <c>JSON.MERGE</c> (RFC 7396) patch.</summary>
    /// <param name="key">The key.</param>
    /// <param name="path">The path to patch; <c>$</c> for the whole document.</param>
    /// <param name="patch">The patch object. Properties that serialize to <c>null</c> remove the field.</param>
    /// <param name="options">Serializer options, or <see langword="null"/> for the connection's <see cref="RuvioClientOptions.JsonSerializerOptions"/>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when applied; <see langword="false"/> when the path cannot be reached.</returns>
    /// <remarks>Not trim or AOT safe; use the type-info overload there.</remarks>
    /// <example>
    /// <code>
    /// await db.JsonMergeValueAsync("user:1", "$", new { age = 37 });
    /// </code>
    /// </example>
    [RequiresUnreferencedCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    [RequiresDynamicCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    ValueTask<bool> JsonMergeValueAsync<T>(string key, string path, T patch, JsonSerializerOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Serializes <paramref name="patch"/> with source-generated metadata and sends it as a <c>JSON.MERGE</c> patch. Trimming and AOT safe.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="path">The path to patch; <c>$</c> for the whole document.</param>
    /// <param name="patch">The patch value.</param>
    /// <param name="typeInfo">Metadata from your <c>JsonSerializerContext</c>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when applied; <see langword="false"/> when the path cannot be reached.</returns>
    /// <example>
    /// <code>
    /// await db.JsonMergeValueAsync("user:1", "$", patch, AppJson.Default.UserPatch);
    /// </code>
    /// </example>
    ValueTask<bool> JsonMergeValueAsync<T>(string key, string path, T patch, JsonTypeInfo<T> typeInfo, CancellationToken cancellationToken = default);

    /// <summary>
    /// Serializes <paramref name="value"/> and appends it to the array at <paramref name="path"/> (<c>JSON.ARRAPPEND</c>).
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="path">The path of the array.</param>
    /// <param name="value">The element to add.</param>
    /// <param name="options">Serializer options, or <see langword="null"/> for the connection's <see cref="RuvioClientOptions.JsonSerializerOptions"/>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The new array length; <see langword="null"/> when the key or path does not exist.</returns>
    /// <remarks>Not trim or AOT safe; use the type-info overload there.</remarks>
    /// <example>
    /// <code>
    /// long? length = await db.JsonArrayAppendValueAsync("cart:7", "$.items", new CartItem("a1", 2));
    /// </code>
    /// </example>
    [RequiresUnreferencedCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    [RequiresDynamicCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    ValueTask<long?> JsonArrayAppendValueAsync<T>(string key, string path, T value, JsonSerializerOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Serializes <paramref name="value"/> with source-generated metadata and appends it to the array at <paramref name="path"/>. Trimming and AOT safe.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="path">The path of the array.</param>
    /// <param name="value">The element to add.</param>
    /// <param name="typeInfo">Metadata from your <c>JsonSerializerContext</c>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The new array length; <see langword="null"/> when the key or path does not exist.</returns>
    /// <example>
    /// <code>
    /// long? length = await db.JsonArrayAppendValueAsync("cart:7", "$.items", item, AppJson.Default.CartItem);
    /// </code>
    /// </example>
    ValueTask<long?> JsonArrayAppendValueAsync<T>(string key, string path, T value, JsonTypeInfo<T> typeInfo, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>APPEND</c>: adds text to the end of a string, creating the key if it is missing.</summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The text to append.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The length of the string after the append, in bytes.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>An existing TTL is kept.</remarks>
    /// <example>
    /// <code>
    /// long length = await db.AppendAsync("log:today", "line\n");
    /// </code>
    /// </example>
    ValueTask<long> AppendAsync(string key, string value, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>STRLEN</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The length of the string in bytes; 0 when the key does not exist.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long bytes = await db.GetStringLengthAsync("greeting");
    /// </code>
    /// </example>
    ValueTask<long> GetStringLengthAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>DECR</c>: subtracts 1 from the 64-bit integer at the key. A missing key starts at 0.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The value after the decrement.</returns>
    /// <exception cref="RuvioException">The value is not an integer or would overflow; wrongtype.</exception>
    /// <example>
    /// <code>
    /// long left = await db.DecrementAsync("stock:sku-42");
    /// </code>
    /// </example>
    ValueTask<long> DecrementAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>INCRBY</c>: adds an integer to the 64-bit counter at the key. A missing key starts at 0.</summary>
    /// <param name="key">The key.</param>
    /// <param name="delta">Amount to add; negative values subtract.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The value after the change.</returns>
    /// <exception cref="RuvioException">The value is not an integer or the result overflows; wrongtype.</exception>
    /// <example>
    /// <code>
    /// long total = await db.IncrementByAsync("bytes:sent", 1024);
    /// </code>
    /// </example>
    ValueTask<long> IncrementByAsync(string key, long delta, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>DECRBY</c>: subtracts an integer from the 64-bit counter at the key. A missing key starts at 0.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="delta">Amount to subtract.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The value after the change.</returns>
    /// <exception cref="RuvioException">The value is not an integer or the result overflows; wrongtype.</exception>
    /// <example>
    /// <code>
    /// long left = await db.DecrementByAsync("credits:42", 5);
    /// </code>
    /// </example>
    ValueTask<long> DecrementByAsync(string key, long delta, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>UNLINK</c>. Ruvio removes the keys synchronously, so it behaves exactly like <c>DEL</c>.</summary>
    /// <param name="keys">The keys to remove.</param>
    /// <returns>How many existing keys were removed.</returns>
    /// <remarks>On a multi-shard server keys in different slots are split per slot and the counts added (not atomic).</remarks>
    /// <example>
    /// <code>
    /// long removed = await db.UnlinkAsync("a", "b");
    /// </code>
    /// </example>
    ValueTask<long> UnlinkAsync(params string[] keys);

    /// <summary>Sends <c>EXISTS</c>: counts how many of the keys exist. A key listed twice counts twice.</summary>
    /// <param name="keys">The keys to check.</param>
    /// <returns>The number of keys that exist, of any type.</returns>
    /// <remarks>On a multi-shard server keys in different slots are split per slot and the counts added.</remarks>
    /// <example>
    /// <code>
    /// bool there = await db.ExistsAsync("session:ada") == 1;
    /// </code>
    /// </example>
    ValueTask<long> ExistsAsync(params string[] keys);

    /// <summary>Sends <c>TYPE</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><c>string</c>, <c>list</c>, <c>set</c>, <c>hash</c>, <c>zset</c>, <c>stream</c>, <c>json</c>, <c>bloomfilter</c>, <c>lease</c>, <c>limit</c>, <c>semaphore</c>, <c>once</c>, or <c>none</c> when the key does not exist. Geo keys report <c>zset</c>.</returns>
    /// <example>
    /// <code>
    /// string kind = await db.GetKeyTypeAsync("user:1");
    /// </code>
    /// </example>
    ValueTask<string> GetKeyTypeAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>RENAME</c>: moves a key (value and TTL) to a new name, replacing any key already there.</summary>
    /// <param name="key">The key.</param>
    /// <param name="newKey">The new name.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <exception cref="RuvioException">The key does not exist (<c>no such key</c>); on a multi-shard server both names must hash to the same slot, otherwise <c>CROSSSLOT</c> is raised before anything is sent (use a <c>{tag}</c>).</exception>
    /// <example>
    /// <code>
    /// await db.RenameAsync("job:{7}:pending", "job:{7}:done");
    /// </code>
    /// </example>
    ValueTask RenameAsync(string key, string newKey, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>SCAN</c>: reads one page of key names. Call repeatedly until the returned cursor is 0, or use <see cref="RuvioClientHelpers.ScanKeysAsync"/> to iterate lazily.
    /// </summary>
    /// <param name="cursor">Start with 0, then pass the cursor from the previous page.</param>
    /// <param name="pattern">Optional <c>MATCH</c> glob (<c>*</c>, <c>?</c>, <c>[abc]</c>, <c>[^a-z]</c>, <c>\</c> escape).</param>
    /// <param name="count">Optional <c>COUNT</c> hint for the page size. A page may hold more or fewer keys.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>A <see cref="ScanPage"/> with the next cursor (0 when the walk is finished) and the keys of this page.</returns>
    /// <remarks>Unlike <c>KEYS</c> this never blocks the server for long. Keys added or removed during the walk may or may not appear, and a key can be returned more than once. Only the selected database is scanned. Cursors follow sorted live keys, not Redis's dictionary order. On a multi-shard server the client walks the shards one after another and encodes the shard in the cursor, so keep passing the returned cursor unchanged.</remarks>
    /// <example>
    /// <code>
    /// long cursor = 0;
    /// do
    /// {
    ///     ScanPage page = await db.ScanAsync(cursor, "user:*", 100);
    ///     foreach (string key in page.Keys) Console.WriteLine(key);
    ///     cursor = page.Cursor;
    /// }
    /// while (cursor != 0);
    /// </code>
    /// </example>
    ValueTask<ScanPage> ScanAsync(long cursor, string? pattern = null, int? count = null, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>DBSIZE</c>: the number of keys in the selected database.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The key count. Keys that are past their TTL but not yet reclaimed are still counted.</returns>
    /// <remarks>The server counts one shard. On a multi-shard server the client sends the command to every shard and adds the answers.</remarks>
    /// <example>
    /// <code>
    /// long keys = await db.GetDatabaseSizeAsync();
    /// </code>
    /// </example>
    ValueTask<long> GetDatabaseSizeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets an absolute expiry time on a key: <c>EXPIREAT</c> when <paramref name="when"/> is a whole second, otherwise <c>PEXPIREAT</c>.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="when">The point in time at which the key expires. A time in the past removes the key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when the key existed and the deadline was applied; <see langword="false"/> when the key does not exist.</returns>
    /// <example>
    /// <code>
    /// await db.ExpireAtAsync("promo:summer", new DateTimeOffset(2027, 9, 1, 0, 0, 0, TimeSpan.Zero));
    /// </code>
    /// </example>
    ValueTask<bool> ExpireAtAsync(string key, DateTimeOffset when, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>TTL</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>Seconds left; -1 when the key has no expiry; -2 when the key does not exist.</returns>
    /// <example>
    /// <code>
    /// long seconds = await db.GetTimeToLiveAsync("session:ada");
    /// </code>
    /// </example>
    ValueTask<long> GetTimeToLiveAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>PTTL</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>Milliseconds left; -1 when the key has no expiry; -2 when the key does not exist.</returns>
    /// <example>
    /// <code>
    /// long ms = await db.GetTimeToLiveMillisecondsAsync("session:ada");
    /// </code>
    /// </example>
    ValueTask<long> GetTimeToLiveMillisecondsAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>PERSIST</c>: removes the expiry so the key lives forever.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when an expiry was removed; <see langword="false"/> when the key has none or does not exist.</returns>
    /// <example>
    /// <code>
    /// await db.PersistAsync("session:ada");
    /// </code>
    /// </example>
    ValueTask<bool> PersistAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>LPUSH</c>: adds values to the head of a list, creating it if needed. With several values each is pushed in turn, so the last value ends up first.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="values">The values to push; at least one.</param>
    /// <returns>The list length after the push.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long length = await db.PushListLeftAsync("queue", "a", "b");   // list is now b, a
    /// </code>
    /// </example>
    ValueTask<long> PushListLeftAsync(string key, params string[] values);

    /// <summary>Sends <c>RPUSH</c>: adds values to the tail of a list, creating it if needed, in the order given.</summary>
    /// <param name="key">The key.</param>
    /// <param name="values">The values to push; at least one.</param>
    /// <returns>The list length after the push.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// await db.PushListRightAsync("queue", "job-1", "job-2");
    /// </code>
    /// </example>
    ValueTask<long> PushListRightAsync(string key, params string[] values);

    /// <summary>Sends <c>LPOP</c>: removes and returns the first element.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The element, or <see langword="null"/> when the list is empty or missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// string? job = await db.PopListLeftAsync("queue");
    /// </code>
    /// </example>
    ValueTask<string?> PopListLeftAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>LPOP key count</c>: removes and returns up to <paramref name="count"/> elements from the head.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="count">How many to remove. Must be positive.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The removed elements, head first. Empty when the list is empty or missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>); <paramref name="count"/> is not positive.</exception>
    /// <example>
    /// <code>
    /// IReadOnlyList&lt;string&gt; batch = await db.PopListLeftAsync("queue", 10);
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> PopListLeftAsync(string key, long count, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>RPOP</c>: removes and returns the last element.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The element, or <see langword="null"/> when the list is empty or missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// string? last = await db.PopListRightAsync("queue");
    /// </code>
    /// </example>
    ValueTask<string?> PopListRightAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>RPOP key count</c>: removes and returns up to <paramref name="count"/> elements from the tail.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="count">How many to remove. Must be positive.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The removed elements, tail first. Empty when the list is empty or missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>); <paramref name="count"/> is not positive.</exception>
    /// <example>
    /// <code>
    /// IReadOnlyList&lt;string&gt; batch = await db.PopListRightAsync("queue", 10);
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> PopListRightAsync(string key, long count, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>LLEN</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The number of elements; 0 for a missing key.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long length = await db.GetListLengthAsync("queue");
    /// </code>
    /// </example>
    ValueTask<long> GetListLengthAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>LINDEX</c>: reads one element by position without removing it.</summary>
    /// <param name="key">The key.</param>
    /// <param name="index">Zero-based position; negative values count from the end (-1 is the last).</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The element, or <see langword="null"/> when the index is out of range or the key is missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// string? first = await db.GetListItemAsync("queue", 0);
    /// </code>
    /// </example>
    ValueTask<string?> GetListItemAsync(string key, long index, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>LRANGE</c>: reads a slice of a list.</summary>
    /// <param name="key">The key.</param>
    /// <param name="start">First position, zero-based; negative counts from the end.</param>
    /// <param name="stop">Last position, <b>inclusive</b>; negative counts from the end (-1 is the last element).</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The elements in list order; empty for a missing key or an empty range.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// IReadOnlyList&lt;string&gt; all = await db.GetListRangeAsync("queue", 0, -1);
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> GetListRangeAsync(string key, long start, long stop, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>LTRIM</c>: keeps only the elements in the range and removes the rest.</summary>
    /// <param name="key">The key.</param>
    /// <param name="start">First position to keep, zero-based; negative counts from the end.</param>
    /// <param name="stop">Last position to keep, inclusive; negative counts from the end.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// await db.TrimListAsync("recent", 0, 99);   // keep the first 100
    /// </code>
    /// </example>
    ValueTask TrimListAsync(string key, long start, long stop, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>BLPOP</c> for one key: removes the first element, waiting for one to arrive when the list is empty.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="timeout">How long to wait. Rounded up to whole seconds; <see cref="TimeSpan.Zero"/> waits until an element arrives.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The key and element popped, or <see langword="null"/> when the timeout passed.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>While it waits, the shared command connection sends nothing else: other calls on the same client queue behind it. Use a connection of its own for blocking pops. A shard allows at most 1,024 blocked clients, and blocking is shard-local.</remarks>
    /// <example>
    /// <code>
    /// var item = await worker.PopListLeftBlockingAsync("queue", TimeSpan.FromSeconds(5));
    /// if (item is { } popped) Console.WriteLine(popped.Value);
    /// </code>
    /// </example>
    ValueTask<KeyValuePair<string, string>?> PopListLeftBlockingAsync(string key, TimeSpan timeout, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>BLPOP</c> over several lists: pops from the first non-empty one in the order given, waiting when all are empty.
    /// </summary>
    /// <param name="keys">The lists to watch, in priority order. On a multi-shard server they must share a slot.</param>
    /// <param name="timeout">How long to wait, rounded up to whole seconds; zero waits until an element arrives.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The key that was popped and the element, or <see langword="null"/> on timeout.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Holds the shared connection while waiting; see the single-key overload.</remarks>
    /// <example>
    /// <code>
    /// var item = await worker.PopListLeftBlockingAsync(new[] { "urgent", "normal" }, TimeSpan.FromSeconds(5));
    /// </code>
    /// </example>
    ValueTask<KeyValuePair<string, string>?> PopListLeftBlockingAsync(IReadOnlyList<string> keys, TimeSpan timeout, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>BRPOP</c> for one key: removes the last element, waiting for one to arrive when the list is empty.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="timeout">How long to wait. Rounded up to whole seconds; <see cref="TimeSpan.Zero"/> waits until an element arrives.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The key and element popped, or <see langword="null"/> when the timeout passed.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Holds the shared command connection while waiting; use a connection of its own. At most 1,024 blocked clients per shard.</remarks>
    /// <example>
    /// <code>
    /// var item = await worker.PopListRightBlockingAsync("stack", TimeSpan.FromSeconds(5));
    /// </code>
    /// </example>
    ValueTask<KeyValuePair<string, string>?> PopListRightBlockingAsync(string key, TimeSpan timeout, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>BRPOP</c> over several lists: pops from the first non-empty one in the order given, waiting when all are empty.
    /// </summary>
    /// <param name="keys">The lists to watch, in priority order. On a multi-shard server they must share a slot.</param>
    /// <param name="timeout">How long to wait, rounded up to whole seconds; zero waits until an element arrives.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The key that was popped and the element, or <see langword="null"/> on timeout.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Holds the shared connection while waiting; see the single-key overload.</remarks>
    /// <example>
    /// <code>
    /// var item = await worker.PopListRightBlockingAsync(new[] { "a", "b" }, TimeSpan.FromSeconds(5));
    /// </code>
    /// </example>
    ValueTask<KeyValuePair<string, string>?> PopListRightBlockingAsync(IReadOnlyList<string> keys, TimeSpan timeout, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>SADD</c>: adds members to a set, creating it if needed. Members already present are ignored.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="members">The members to add; at least one.</param>
    /// <returns>How many members were new.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long added = await db.AddSetMembersAsync("tags:post:1", "csharp", "redis");
    /// </code>
    /// </example>
    ValueTask<long> AddSetMembersAsync(string key, params string[] members);

    /// <summary>Sends <c>SREM</c>: removes members from a set.</summary>
    /// <param name="key">The key.</param>
    /// <param name="members">The members to remove.</param>
    /// <returns>How many were actually in the set and removed. An emptied set disappears.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long removed = await db.RemoveSetMembersAsync("tags:post:1", "redis");
    /// </code>
    /// </example>
    ValueTask<long> RemoveSetMembersAsync(string key, params string[] members);

    /// <summary>Sends <c>SISMEMBER</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="member">The member.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when the member is in the set; <see langword="false"/> when it is not or the key is missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// bool tagged = await db.SetContainsAsync("tags:post:1", "csharp");
    /// </code>
    /// </example>
    ValueTask<bool> SetContainsAsync(string key, string member, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>SCARD</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The number of members; 0 for a missing key.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long count = await db.GetSetCountAsync("tags:post:1");
    /// </code>
    /// </example>
    ValueTask<long> GetSetCountAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>SMEMBERS</c>: every member of the set.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The members, sorted by the server. Empty for a missing key. For big sets prefer the lazy <see cref="RuvioClientHelpers.ScanSetMembersAsync"/>.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// IReadOnlyList&lt;string&gt; tags = await db.GetSetMembersAsync("tags:post:1");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> GetSetMembersAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>SINTER</c>: members present in every set.</summary>
    /// <param name="keys">The sets. On a multi-shard server they must hash to one slot (use a <c>{tag}</c>), otherwise <c>CROSSSLOT</c> is raised before anything is sent.</param>
    /// <returns>The common members, sorted. A missing key counts as an empty set.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// var both = await db.GetSetIntersectionAsync("{t}:a", "{t}:b");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> GetSetIntersectionAsync(params string[] keys);

    /// <summary>Sends <c>SUNION</c>: members present in at least one set.</summary>
    /// <param name="keys">The sets. On a multi-shard server they must share a slot.</param>
    /// <returns>The combined members, sorted.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// var all = await db.GetSetUnionAsync("{t}:a", "{t}:b");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> GetSetUnionAsync(params string[] keys);

    /// <summary>Sends <c>SDIFF</c>: members of the first set that are in none of the others.</summary>
    /// <param name="keys">The first key is the set to subtract from, the rest are subtracted. On a multi-shard server they must share a slot.</param>
    /// <returns>The remaining members, sorted.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// var onlyA = await db.GetSetDifferenceAsync("{t}:a", "{t}:b");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> GetSetDifferenceAsync(params string[] keys);

    /// <summary>Sends <c>SINTERSTORE</c>: stores the intersection in <paramref name="destination"/>, replacing it.</summary>
    /// <param name="destination">The key that receives the result.</param>
    /// <param name="keys">The source sets; all keys including the destination must share a slot on a multi-shard server.</param>
    /// <returns>The size of the stored set. An empty result removes the destination.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long size = await db.StoreSetIntersectionAsync("{t}:both", "{t}:a", "{t}:b");
    /// </code>
    /// </example>
    ValueTask<long> StoreSetIntersectionAsync(string destination, params string[] keys);

    /// <summary>Sends <c>SUNIONSTORE</c>: stores the union in <paramref name="destination"/>, replacing it.</summary>
    /// <param name="destination">The key that receives the result.</param>
    /// <param name="keys">The source sets; all keys including the destination must share a slot on a multi-shard server.</param>
    /// <returns>The size of the stored set.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long size = await db.StoreSetUnionAsync("{t}:all", "{t}:a", "{t}:b");
    /// </code>
    /// </example>
    ValueTask<long> StoreSetUnionAsync(string destination, params string[] keys);

    /// <summary>Sends <c>SDIFFSTORE</c>: stores the difference in <paramref name="destination"/>, replacing it.</summary>
    /// <param name="destination">The key that receives the result.</param>
    /// <param name="keys">The first key is the set to subtract from, the rest are subtracted; all keys including the destination must share a slot on a multi-shard server.</param>
    /// <returns>The size of the stored set.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long size = await db.StoreSetDifferenceAsync("{t}:onlyA", "{t}:a", "{t}:b");
    /// </code>
    /// </example>
    ValueTask<long> StoreSetDifferenceAsync(string destination, params string[] keys);

    /// <summary>Sends <c>SMOVE</c>: moves a member from one set to another.</summary>
    /// <param name="source">The set to take the member from.</param>
    /// <param name="destination">The set to add the member to.</param>
    /// <param name="member">The member.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when the member was moved; <see langword="false"/> when it was not in the source set.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>); on a multi-shard server both keys must share a slot (<c>CROSSSLOT</c> otherwise).</exception>
    /// <example>
    /// <code>
    /// bool moved = await db.MoveSetMemberAsync("{t}:todo", "{t}:done", "task-1");
    /// </code>
    /// </example>
    ValueTask<bool> MoveSetMemberAsync(string source, string destination, string member, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>SPOP</c>: removes and returns one random member.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The member, or <see langword="null"/> when the set is empty or missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// string? winner = await db.PopSetMemberAsync("raffle");
    /// </code>
    /// </example>
    ValueTask<string?> PopSetMemberAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>SPOP key count</c>: removes and returns up to <paramref name="count"/> random members.</summary>
    /// <param name="key">The key.</param>
    /// <param name="count">How many to remove.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The removed members; fewer if the set is smaller. Empty for a missing key.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// var winners = await db.PopSetMembersAsync("raffle", 3);
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> PopSetMembersAsync(string key, long count, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>SRANDMEMBER</c>: returns one random member without removing it.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The member, or <see langword="null"/> when the set is empty or missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// string? sample = await db.GetSetRandomMemberAsync("tags:post:1");
    /// </code>
    /// </example>
    ValueTask<string?> GetSetRandomMemberAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>SRANDMEMBER key count</c>: returns random members without removing them.</summary>
    /// <param name="key">The key.</param>
    /// <param name="count">A positive number returns up to that many distinct members; a negative number returns exactly <c>|count|</c> members and may repeat; 0 returns an empty list.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The sampled members.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// var three = await db.GetSetRandomMembersAsync("tags:post:1", 3);
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> GetSetRandomMembersAsync(string key, long count, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>HSET</c>: sets one or more fields of a hash, creating the hash if needed.</summary>
    /// <param name="key">The key.</param>
    /// <param name="fields">The field/value pairs to write.</param>
    /// <returns>How many fields were new (fields that already existed are overwritten but not counted).</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Writing a field drops that field's TTL.</remarks>
    /// <example>
    /// <code>
    /// await db.SetHashFieldsAsync("user:1", ("name", "Ada"), ("city", "London"));
    /// </code>
    /// </example>
    ValueTask<long> SetHashFieldsAsync(string key, params (string Field, string Value)[] fields);

    /// <summary>Sends <c>HMSET</c>: writes fields like <see cref="SetHashFieldsAsync"/> but the server replies <c>OK</c> instead of the count of new fields.</summary>
    /// <param name="key">The key.</param>
    /// <param name="fields">The field/value pairs to write.</param>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Redis marks <c>HMSET</c> deprecated; prefer <see cref="SetHashFieldsAsync"/> in new code.</remarks>
    /// <example>
    /// <code>
    /// await db.SetHashManyAsync("user:1", ("name", "Ada"), ("city", "London"));
    /// </code>
    /// </example>
    ValueTask SetHashManyAsync(string key, params (string Field, string Value)[] fields);

    /// <summary>Sends <c>HGET</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="field">The hash field name.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The field's value, or <see langword="null"/> when the field or the key does not exist.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// string? city = await db.GetHashFieldAsync("user:1", "city");
    /// </code>
    /// </example>
    ValueTask<string?> GetHashFieldAsync(string key, string field, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>HDEL</c>: removes fields from a hash.</summary>
    /// <param name="key">The key.</param>
    /// <param name="fields">The fields to remove.</param>
    /// <returns>How many existed and were removed. An emptied hash disappears.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long removed = await db.DeleteHashFieldsAsync("user:1", "city");
    /// </code>
    /// </example>
    ValueTask<long> DeleteHashFieldsAsync(string key, params string[] fields);

    /// <summary>Sends <c>HLEN</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The number of fields; 0 for a missing key.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long fields = await db.GetHashLengthAsync("user:1");
    /// </code>
    /// </example>
    ValueTask<long> GetHashLengthAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>HGETALL</c>: every field and value.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>A dictionary of field to value (empty for a missing key). Enumeration order is unspecified. For big hashes use <see cref="RuvioClientHelpers.ScanHashEntriesAsync"/>.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// var user = await db.GetHashAllAsync("user:1");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyDictionary<string, string>> GetHashAllAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>HMGET</c>: reads several fields.</summary>
    /// <param name="key">The key.</param>
    /// <param name="fields">The fields to read.</param>
    /// <returns>One value per field in order; <see langword="null"/> for a field that does not exist.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// var values = await db.GetHashFieldsAsync("user:1", "name", "city");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string?>> GetHashFieldsAsync(string key, params string[] fields);

    /// <summary>Sends <c>HEXISTS</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="field">The hash field name.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when the field exists.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// bool has = await db.HashFieldExistsAsync("user:1", "city");
    /// </code>
    /// </example>
    ValueTask<bool> HashFieldExistsAsync(string key, string field, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>HKEYS</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The field names, in unspecified order. Empty for a missing key.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// var names = await db.GetHashFieldNamesAsync("user:1");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> GetHashFieldNamesAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>HVALS</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The values, in unspecified order. Empty for a missing key.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// var values = await db.GetHashFieldValuesAsync("user:1");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> GetHashFieldValuesAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>HINCRBY</c>: adds an integer to a hash field. A missing field starts at 0.</summary>
    /// <param name="key">The key.</param>
    /// <param name="field">The hash field name.</param>
    /// <param name="delta">Amount to add; negative subtracts.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The field's value after the change.</returns>
    /// <exception cref="RuvioException">The field is not an integer or the result overflows; wrongtype.</exception>
    /// <remarks>A field TTL is kept.</remarks>
    /// <example>
    /// <code>
    /// long hits = await db.IncrementHashFieldAsync("stats", "hits", 1);
    /// </code>
    /// </example>
    ValueTask<long> IncrementHashFieldAsync(string key, string field, long delta, CancellationToken cancellationToken = default);

    /// <summary>Sets a time to live on hash fields: <c>HEXPIRE</c> for whole seconds, otherwise <c>HPEXPIRE</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="ttl">Time to live from now, rounded up to milliseconds. Zero deletes the fields at once.</param>
    /// <param name="fields">The fields to expire.</param>
    /// <returns>One code per field, in order: 1 TTL set, 0 condition not met, 2 field deleted (zero TTL), -2 no such field or key.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// IReadOnlyList&lt;long&gt; codes = await db.ExpireHashFieldsAsync("session:1", TimeSpan.FromMinutes(5), "token");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<long>> ExpireHashFieldsAsync(string key, TimeSpan ttl, params string[] fields);

    /// <summary>
    /// Like <see cref="ExpireHashFieldsAsync(string, TimeSpan, string[])"/>, but only when the field's current TTL meets <paramref name="condition"/>.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="ttl">Time to live from now, rounded up to milliseconds.</param>
    /// <param name="condition"><see cref="FieldExpireCondition.IfNoTtl"/> (<c>NX</c>), <see cref="FieldExpireCondition.IfTtl"/> (<c>XX</c>), <see cref="FieldExpireCondition.IfGreater"/> (<c>GT</c>) or <see cref="FieldExpireCondition.IfLess"/> (<c>LT</c>). A field without a TTL counts as infinite for <c>GT</c> and <c>LT</c>.</param>
    /// <param name="fields">The fields to expire.</param>
    /// <returns>One code per field: 1 TTL set, 0 condition not met, 2 field deleted, -2 no such field or key.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// await db.ExpireHashFieldsAsync("session:1", TimeSpan.FromMinutes(5), FieldExpireCondition.IfNoTtl, "token");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<long>> ExpireHashFieldsAsync(string key, TimeSpan ttl, FieldExpireCondition condition, params string[] fields);

    /// <summary>
    /// Sets an absolute deadline on hash fields: <c>HEXPIREAT</c> for a whole second, otherwise <c>HPEXPIREAT</c>.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="when">The deadline. A time in the past deletes the fields.</param>
    /// <param name="fields">The fields to expire.</param>
    /// <returns>One code per field: 1 TTL set, 2 field deleted, -2 no such field or key.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// await db.ExpireHashFieldsAtAsync("session:1", DateTimeOffset.UtcNow.AddHours(1), "token");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<long>> ExpireHashFieldsAtAsync(string key, DateTimeOffset when, params string[] fields);

    /// <summary>Sends <c>HTTL</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="fields">The fields to inspect.</param>
    /// <returns>Per field, seconds left (rounded up); -1 when the field has no TTL; -2 when the field or key is missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// var ttl = await db.GetHashFieldTimeToLiveAsync("session:1", "token");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<long>> GetHashFieldTimeToLiveAsync(string key, params string[] fields);

    /// <summary>Sends <c>HPTTL</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="fields">The fields to inspect.</param>
    /// <returns>Per field, milliseconds left; -1 when the field has no TTL; -2 when the field or key is missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// var ttl = await db.GetHashFieldTimeToLiveMillisecondsAsync("session:1", "token");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<long>> GetHashFieldTimeToLiveMillisecondsAsync(string key, params string[] fields);

    /// <summary>Sends <c>HEXPIRETIME</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="fields">The fields to inspect.</param>
    /// <returns>Per field, the Unix time in seconds at which it expires; -1 when it has no TTL; -2 when the field or key is missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// var at = await db.GetHashFieldExpireTimeAsync("session:1", "token");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<long>> GetHashFieldExpireTimeAsync(string key, params string[] fields);

    /// <summary>Sends <c>HPEXPIRETIME</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="fields">The fields to inspect.</param>
    /// <returns>Per field, the Unix time in milliseconds at which it expires; -1 when it has no TTL; -2 when the field or key is missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// var at = await db.GetHashFieldExpireTimeMillisecondsAsync("session:1", "token");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<long>> GetHashFieldExpireTimeMillisecondsAsync(string key, params string[] fields);

    /// <summary>Sends <c>HPERSIST</c>: removes the TTL from hash fields.</summary>
    /// <param name="key">The key.</param>
    /// <param name="fields">The fields to make permanent.</param>
    /// <returns>Per field: 1 the TTL was removed, -1 the field had no TTL, -2 the field or key is missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// await db.PersistHashFieldsAsync("session:1", "token");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<long>> PersistHashFieldsAsync(string key, params string[] fields);

    /// <summary>Sends <c>HSETNX</c>: sets a field only when it does not exist yet.</summary>
    /// <param name="key">The key.</param>
    /// <param name="field">The hash field name.</param>
    /// <param name="value">The value to store.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when the field was created; <see langword="false"/> when it already existed (nothing written).</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// bool first = await db.SetHashFieldIfAbsentAsync("user:1", "created", "2026-10-17");
    /// </code>
    /// </example>
    ValueTask<bool> SetHashFieldIfAbsentAsync(string key, string field, string value, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>HSTRLEN</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="field">The hash field name.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The length of the field's value in bytes; 0 when the field or key is missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long bytes = await db.GetHashFieldLengthAsync("user:1", "name");
    /// </code>
    /// </example>
    ValueTask<long> GetHashFieldLengthAsync(string key, string field, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>HSCAN</c>: reads one page of fields and values.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cursor">Start with 0, then pass the cursor of the previous page.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>A <see cref="HashScanPage"/>: the next cursor (0 when done) and this page's fields. A missing key gives cursor 0 and no fields.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>For a lazy walk use <see cref="RuvioClientHelpers.ScanHashEntriesAsync"/>.</remarks>
    /// <example>
    /// <code>
    /// HashScanPage page = await db.ScanHashAsync("user:1", 0);
    /// </code>
    /// </example>
    ValueTask<HashScanPage> ScanHashAsync(string key, long cursor, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>HSCAN</c> with optional <c>MATCH</c> and <c>COUNT</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cursor">Start with 0, then pass the cursor of the previous page.</param>
    /// <param name="pattern">Optional glob for field names; null matches all.</param>
    /// <param name="count">Optional page-size hint.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>A <see cref="HashScanPage"/>: the next cursor (0 when done) and this page's fields.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// HashScanPage page = await db.ScanHashAsync("user:1", 0, "addr*", 50);
    /// </code>
    /// </example>
    ValueTask<HashScanPage> ScanHashAsync(string key, long cursor, string? pattern, int? count = null, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>ZADD</c>: adds members with scores, or updates the score of members that exist.</summary>
    /// <param name="key">The key.</param>
    /// <param name="entries">The members and scores.</param>
    /// <returns>How many members were new (score updates are not counted).</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// await db.AddSortedSetAsync("board", new SortedSetEntry("ada", 100), new SortedSetEntry("bob", 50));
    /// </code>
    /// </example>
    ValueTask<long> AddSortedSetAsync(string key, params SortedSetEntry[] entries);

    /// <summary>Sends <c>ZADD</c> for one member.</summary>
    /// <param name="key">The key.</param>
    /// <param name="member">The member.</param>
    /// <param name="score">The score; members are ordered by it (ties by member bytes).</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when the member was new; <see langword="false"/> when it already existed and only its score was set.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// bool isNew = await db.AddSortedSetAsync("board", "ada", 100);
    /// </code>
    /// </example>
    ValueTask<bool> AddSortedSetAsync(string key, string member, double score, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>ZADD</c> with <c>(member, score)</c> tuples.</summary>
    /// <param name="key">The key.</param>
    /// <param name="entries">The members and scores.</param>
    /// <returns>How many members were new.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// await db.AddSortedSetAsync("board", ("ada", 100), ("bob", 50));
    /// </code>
    /// </example>
    ValueTask<long> AddSortedSetAsync(string key, params (string Member, double Score)[] entries);

    /// <summary>Sends <c>ZADD</c> with options and <c>(member, score)</c> tuples.</summary>
    /// <param name="key">The key.</param>
    /// <param name="flags">Combination of <see cref="SortedSetAddFlags"/>: <c>NX</c> (only add), <c>XX</c> (only update), <c>GT</c> / <c>LT</c> (update only if the score rises / falls; a missing member is still added), <c>CH</c> (count changed members too). The server refuses <c>NX</c> with <c>XX</c>, <c>GT</c> or <c>LT</c>, and <c>GT</c> with <c>LT</c>.</param>
    /// <param name="entries">The members and scores.</param>
    /// <returns>The number of new members; with <see cref="SortedSetAddFlags.Changed"/> new plus members whose score changed.</returns>
    /// <exception cref="RuvioException">Incompatible flags; wrongtype.</exception>
    /// <example>
    /// <code>
    /// await db.AddSortedSetAsync("board", SortedSetAddFlags.GreaterThan | SortedSetAddFlags.Changed, ("ada", 120));
    /// </code>
    /// </example>
    ValueTask<long> AddSortedSetAsync(string key, SortedSetAddFlags flags, params (string Member, double Score)[] entries);

    /// <summary>Sends <c>ZADD</c> with options and <see cref="SortedSetEntry"/> values.</summary>
    /// <param name="key">The key.</param>
    /// <param name="flags">Combination of <see cref="SortedSetAddFlags"/> (see the tuple overload for the rules).</param>
    /// <param name="entries">The members and scores.</param>
    /// <returns>The number of new members; with <see cref="SortedSetAddFlags.Changed"/> new plus changed members.</returns>
    /// <exception cref="RuvioException">Incompatible flags; wrongtype.</exception>
    /// <example>
    /// <code>
    /// await db.AddSortedSetAsync("board", SortedSetAddFlags.IfNotExists, new SortedSetEntry("cy", 10));
    /// </code>
    /// </example>
    ValueTask<long> AddSortedSetAsync(string key, SortedSetAddFlags flags, params SortedSetEntry[] entries);

    /// <summary>Sends <c>ZADD ... INCR</c>: adds to one member's score, honoring the flags.</summary>
    /// <param name="key">The key.</param>
    /// <param name="member">The member (created with the score <paramref name="delta"/> if missing and the flags allow).</param>
    /// <param name="delta">Amount to add.</param>
    /// <param name="flags"><see cref="SortedSetAddFlags"/> such as <c>NX</c>, <c>XX</c>, <c>GT</c>, <c>LT</c>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The new score, or <see langword="null"/> when a flag prevented the update.</returns>
    /// <exception cref="RuvioException">Incompatible flags; wrongtype.</exception>
    /// <example>
    /// <code>
    /// double? score = await db.IncrementSortedSetAsync("board", "ada", 5, SortedSetAddFlags.IfExists);
    /// </code>
    /// </example>
    ValueTask<double?> IncrementSortedSetAsync(string key, string member, double delta, SortedSetAddFlags flags, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>ZRANGE</c> (or <c>ZREVRANGE</c> when descending): members by position in score order.</summary>
    /// <param name="key">The key.</param>
    /// <param name="start">First rank, zero-based; negative counts from the end.</param>
    /// <param name="stop">Last rank, inclusive; -1 is the last member.</param>
    /// <param name="descending">When true, rank 0 is the highest score.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The members in order; empty for a missing key.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// var top3 = await db.GetSortedSetRangeByRankAsync("board", 0, 2, descending: true);
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> GetSortedSetRangeByRankAsync(string key, long start, long stop, bool descending = false, CancellationToken cancellationToken = default);

    /// <summary>Like <see cref="GetSortedSetRangeByRankAsync"/> but also returns each score (<c>WITHSCORES</c>).</summary>
    /// <param name="key">The key.</param>
    /// <param name="start">First rank, zero-based; negative counts from the end.</param>
    /// <param name="stop">Last rank, inclusive; -1 is the last member.</param>
    /// <param name="descending">When true, rank 0 is the highest score.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>Members with scores in order.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// foreach (SortedSetEntry e in await db.GetSortedSetRangeByRankWithScoresAsync("board", 0, -1, true))
    ///     Console.WriteLine($"{e.Member}: {e.Score}");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<SortedSetEntry>> GetSortedSetRangeByRankWithScoresAsync(string key, long start, long stop, bool descending = false, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>ZRANGE ... BYLEX</c>: members between two names, assuming all scores are equal.</summary>
    /// <param name="key">The key.</param>
    /// <param name="min">Lower bound: <c>-</c> (start), <c>[member</c> (inclusive) or <c>(member</c> (exclusive).</param>
    /// <param name="max">Upper bound: <c>+</c> (end), <c>[member</c> or <c>(member</c>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The members in lexical order.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>); a bound has an invalid form.</exception>
    /// <example>
    /// <code>
    /// var names = await db.GetSortedSetRangeByLexAsync("names", "[a", "(b");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> GetSortedSetRangeByLexAsync(string key, string min, string max, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>ZRANGEBYSCORE ... WITHSCORES</c>: members whose score is in a range.</summary>
    /// <param name="key">The key.</param>
    /// <param name="min">Lowest score as text: a number, <c>-inf</c>, or <c>(5</c> for exclusive.</param>
    /// <param name="max">Highest score as text: a number, <c>+inf</c>, or <c>(5</c> for exclusive.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>Members with scores, ascending.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>); a bound is not a number.</exception>
    /// <example>
    /// <code>
    /// var mid = await db.GetSortedSetRangeByScoreWithScoresAsync("board", "50", "(100");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<SortedSetEntry>> GetSortedSetRangeByScoreWithScoresAsync(string key, string min, string max, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>ZREM</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="members">The members to remove.</param>
    /// <returns>How many existed and were removed.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long removed = await db.RemoveSortedSetMembersAsync("board", "bob");
    /// </code>
    /// </example>
    ValueTask<long> RemoveSortedSetMembersAsync(string key, params string[] members);

    /// <summary>Sends <c>ZPOPMIN</c>: removes and returns the lowest-scored members.</summary>
    /// <param name="key">The key.</param>
    /// <param name="count">How many to pop; default 1.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The removed members with scores, lowest first. Empty for a missing key.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// var next = await db.PopSortedSetMinAsync("timers");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<SortedSetEntry>> PopSortedSetMinAsync(string key, long count = 1, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>ZPOPBYSCORE</c>: removes and returns the members that are due, lowest score first. With scores set to due times this takes the jobs that are ready.</summary>
    /// <param name="key">The key.</param>
    /// <param name="min">Lowest score: a number, <c>-inf</c>, or <c>(5</c> for exclusive.</param>
    /// <param name="max">Highest score: a number, <c>+inf</c>, or <c>(5</c> for exclusive.</param>
    /// <param name="count">How many to pop, 1 to 10000.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The removed members with scores, lowest first. Empty when nothing is due.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// var jobs = await db.PopSortedSetByScoreAsync("scheduled", "-inf", nowUnixSeconds.ToString(), 100);
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<SortedSetEntry>> PopSortedSetByScoreAsync(string key, string min, string max, long count = 1, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>ZMOVEDUE</c>: moves the members that are due from a sorted set to the tail of a list, lowest score first. Clients blocked on the list are served first. Both keys must hash to the same slot on a sharded server.</summary>
    /// <param name="source">The sorted set to take members from.</param>
    /// <param name="destination">The list that receives them.</param>
    /// <param name="max">Highest score: a number, <c>+inf</c>, or <c>(5</c> for exclusive.</param>
    /// <param name="count">How many to move, 1 to 10000.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>How many members were moved.</returns>
    /// <exception cref="RuvioException">A key holds a value of another type (<c>WRONGTYPE</c>), the keys are in different slots (<c>CROSSSLOT</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long moved = await db.MoveDueSortedSetAsync("{q}scheduled", "{q}ready", nowUnixSeconds.ToString(), 100);
    /// </code>
    /// </example>
    ValueTask<long> MoveDueSortedSetAsync(string source, string destination, string max, long count = 1, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>ZPOPMAX</c>: removes and returns the highest-scored members.</summary>
    /// <param name="key">The key.</param>
    /// <param name="count">How many to pop; default 1.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The removed members with scores, highest first. Empty for a missing key.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// var best = await db.PopSortedSetMaxAsync("board", 3);
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<SortedSetEntry>> PopSortedSetMaxAsync(string key, long count = 1, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>BZPOPMIN</c> for one key: pops the lowest member, waiting if the set is empty.</summary>
    /// <param name="key">The key.</param>
    /// <param name="timeout">How long to wait. Rounded up to whole seconds; zero waits forever.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The key and the popped entry, or <see langword="null"/> when the timeout passed.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Holds the shared command connection while waiting; use a connection of its own. Blocking is shard-local, at most 1,024 blocked clients per shard.</remarks>
    /// <example>
    /// <code>
    /// var popped = await worker.PopSortedSetMinBlockingAsync("timers", TimeSpan.FromSeconds(5));
    /// </code>
    /// </example>
    ValueTask<(string Key, SortedSetEntry Entry)?> PopSortedSetMinBlockingAsync(string key, TimeSpan timeout, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>BZPOPMIN</c> over several keys: pops from the first non-empty set in the order given.</summary>
    /// <param name="keys">The sets to watch; on a multi-shard server they must share a slot.</param>
    /// <param name="timeout">How long to wait, rounded up to whole seconds; zero waits forever.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The key and entry popped, or <see langword="null"/> on timeout.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Holds the shared connection while waiting.</remarks>
    /// <example>
    /// <code>
    /// var popped = await worker.PopSortedSetMinBlockingAsync(new[] { "t1", "t2" }, TimeSpan.FromSeconds(5));
    /// </code>
    /// </example>
    ValueTask<(string Key, SortedSetEntry Entry)?> PopSortedSetMinBlockingAsync(IReadOnlyList<string> keys, TimeSpan timeout, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>BZPOPMAX</c> for one key: pops the highest member, waiting if the set is empty.</summary>
    /// <param name="key">The key.</param>
    /// <param name="timeout">How long to wait. Rounded up to whole seconds; zero waits forever.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The key and the popped entry, or <see langword="null"/> when the timeout passed.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Holds the shared command connection while waiting; use a connection of its own.</remarks>
    /// <example>
    /// <code>
    /// var popped = await worker.PopSortedSetMaxBlockingAsync("board", TimeSpan.FromSeconds(5));
    /// </code>
    /// </example>
    ValueTask<(string Key, SortedSetEntry Entry)?> PopSortedSetMaxBlockingAsync(string key, TimeSpan timeout, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>BZPOPMAX</c> over several keys: pops from the first non-empty set in the order given.</summary>
    /// <param name="keys">The sets to watch; on a multi-shard server they must share a slot.</param>
    /// <param name="timeout">How long to wait, rounded up to whole seconds; zero waits forever.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The key and entry popped, or <see langword="null"/> on timeout.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Holds the shared connection while waiting.</remarks>
    /// <example>
    /// <code>
    /// var popped = await worker.PopSortedSetMaxBlockingAsync(new[] { "b1", "b2" }, TimeSpan.FromSeconds(5));
    /// </code>
    /// </example>
    ValueTask<(string Key, SortedSetEntry Entry)?> PopSortedSetMaxBlockingAsync(IReadOnlyList<string> keys, TimeSpan timeout, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>ZREMRANGEBYRANK</c>: removes members by position.</summary>
    /// <param name="key">The key.</param>
    /// <param name="start">First rank, zero-based; negative counts from the end.</param>
    /// <param name="stop">Last rank, inclusive.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>How many members were removed.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// await db.RemoveSortedSetRangeByRankAsync("board", 0, -101);   // keep only the top 100
    /// </code>
    /// </example>
    ValueTask<long> RemoveSortedSetRangeByRankAsync(string key, long start, long stop, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>ZREMRANGEBYSCORE</c>: removes members whose score is in a range.</summary>
    /// <param name="key">The key.</param>
    /// <param name="min">Lowest score: a number, <c>-inf</c>, or <c>(5</c> for exclusive.</param>
    /// <param name="max">Highest score: a number, <c>+inf</c>, or <c>(5</c> for exclusive.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>How many members were removed.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// await db.RemoveSortedSetRangeByScoreAsync("expiring", "-inf", nowUnixSeconds.ToString());
    /// </code>
    /// </example>
    ValueTask<long> RemoveSortedSetRangeByScoreAsync(string key, string min, string max, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>ZREMRANGEBYLEX</c>: removes members in a lexical range (all scores should be equal).</summary>
    /// <param name="key">The key.</param>
    /// <param name="min">Lower bound: <c>-</c>, <c>[member</c> or <c>(member</c>.</param>
    /// <param name="max">Upper bound: <c>+</c>, <c>[member</c> or <c>(member</c>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>How many members were removed.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// await db.RemoveSortedSetRangeByLexAsync("names", "[a", "(b");
    /// </code>
    /// </example>
    ValueTask<long> RemoveSortedSetRangeByLexAsync(string key, string min, string max, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>ZINCRBY</c>: adds to a member's score, creating the member with that score if missing.</summary>
    /// <param name="key">The key.</param>
    /// <param name="member">The member.</param>
    /// <param name="delta">Amount to add; negative subtracts.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The new score.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// double score = await db.IncrementSortedSetAsync("board", "ada", 10);
    /// </code>
    /// </example>
    ValueTask<double> IncrementSortedSetAsync(string key, string member, double delta, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>ZCARD</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The number of members; 0 for a missing key.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long size = await db.GetSortedSetCountAsync("board");
    /// </code>
    /// </example>
    ValueTask<long> GetSortedSetCountAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>ZSCORE</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="member">The member.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The member's score, or <see langword="null"/> when the member or key does not exist.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// double? score = await db.GetSortedSetScoreAsync("board", "ada");
    /// </code>
    /// </example>
    ValueTask<double?> GetSortedSetScoreAsync(string key, string member, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>GEOADD</c>: stores named places in a geospatial index. A geo index is an ordinary sorted set whose score is a 52-bit geohash, so <c>TYPE</c> reports <c>zset</c> and the sorted-set methods work on it.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="places">One or more places. <c>Longitude</c> is in degrees from -180 to 180 (east positive); <c>Latitude</c> is in degrees from -85.05112878 to 85.05112878 (north positive); <c>Member</c> is the name. Note the order: longitude first.</param>
    /// <returns>How many places were new. Re-adding an existing member moves it and is not counted.</returns>
    /// <exception cref="RuvioException">A coordinate is outside the allowed range (<c>invalid longitude,latitude pair</c>); wrongtype.</exception>
    /// <remarks>Positions are stored with a precision well under a metre, so <see cref="GeoPositionAsync"/> returns slightly rounded coordinates. Queries use a great-circle distance on a sphere of radius 6372797.56 m, like Redis.</remarks>
    /// <example>
    /// <code>
    /// await db.AddGeoAsync("shops",
    ///     (28.9784, 41.0082, "istanbul"),
    ///     (32.8597, 39.9334, "ankara"));
    /// </code>
    /// </example>
    ValueTask<long> AddGeoAsync(string key, params (double Longitude, double Latitude, string Member)[] places);

    /// <summary>Sends <c>GEODIST</c>: the distance between two members.</summary>
    /// <param name="key">The key.</param>
    /// <param name="from">The first member.</param>
    /// <param name="to">The second member.</param>
    /// <param name="unit">Result unit: <c>m</c> (default), <c>km</c>, <c>ft</c> or <c>mi</c>. Case-insensitive; other values are refused by the server.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The distance in the unit asked for, or <see langword="null"/> when either member (or the key) does not exist.</returns>
    /// <exception cref="RuvioException">An unsupported unit; wrongtype.</exception>
    /// <example>
    /// <code>
    /// double? km = await db.GeoDistanceAsync("shops", "istanbul", "ankara", "km");   // about 350
    /// </code>
    /// </example>
    ValueTask<double?> GeoDistanceAsync(string key, string from, string to, string unit = "m", CancellationToken cancellationToken = default);

    /// <summary>Sends <c>GEODIST</c> with a typed unit: the distance between two members.</summary>
    /// <param name="key">The key.</param>
    /// <param name="from">The first member.</param>
    /// <param name="to">The second member.</param>
    /// <param name="unit">The unit of the result, a <see cref="GeoUnit"/>: meters, kilometers, feet or miles.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The distance in <paramref name="unit"/>, or <see langword="null"/> when either member (or the key) does not exist.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// double? km = await db.GeoDistanceAsync("shops", "istanbul", "ankara", GeoUnit.Kilometers);   // about 350
    /// </code>
    /// </example>
    ValueTask<double?> GeoDistanceAsync(string key, string from, string to, GeoUnit unit, CancellationToken cancellationToken = default)
        => RespRead.OptionalDouble(ExecuteAsync(new string[] { "GEODIST", key, from, to, Keywords.Word(unit) }, cancellationToken), "GEODIST");

    /// <summary>Sends <c>GEOHASH</c>: the standard geohash string of each member.</summary>
    /// <param name="key">The key.</param>
    /// <param name="members">The members to look up.</param>
    /// <returns>One 11-character geohash per member, in order, or <see langword="null"/> for a member that does not exist.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// var hashes = await db.GeoHashAsync("shops", "istanbul");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string?>> GeoHashAsync(string key, params string[] members);

    /// <summary>Sends <c>GEOPOS</c>: the coordinates of each member.</summary>
    /// <param name="key">The key.</param>
    /// <param name="members">The members to look up.</param>
    /// <returns>One <c>(Longitude, Latitude)</c> pair per member in degrees, or <see langword="null"/> for a member that does not exist.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// var pos = await db.GeoPositionAsync("shops", "istanbul");
    /// if (pos[0] is { } p) Console.WriteLine($"{p.Longitude},{p.Latitude}");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<(double Longitude, double Latitude)?>> GeoPositionAsync(string key, params string[] members);

    /// <summary>Sends <c>GEOSEARCH FROMLONLAT ... BYRADIUS ... ASC</c>: members within a circle around a point.</summary>
    /// <param name="key">The key.</param>
    /// <param name="longitude">Centre longitude in degrees (-180 to 180).</param>
    /// <param name="latitude">Centre latitude in degrees (-85.05112878 to 85.05112878).</param>
    /// <param name="radius">Radius in <paramref name="unit"/>. Must not be negative.</param>
    /// <param name="unit"><c>m</c>, <c>km</c>, <c>ft</c> or <c>mi</c> (case-insensitive).</param>
    /// <returns>Member names inside the circle, nearest first. Empty when none match or the key is missing.</returns>
    /// <exception cref="RuvioException">A coordinate out of range, a negative radius or an unsupported unit; wrongtype.</exception>
    /// <example>
    /// <code>
    /// var near = await db.GeoSearchAsync("shops", 29.0, 41.0, 25, "km");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> GeoSearchAsync(string key, double longitude, double latitude, double radius, string unit);

    /// <summary>
    /// Sends <c>GEOSEARCHSTORE ... FROMMEMBER ... BYRADIUS</c>: stores the members within a circle around another member into a new geo set.
    /// </summary>
    /// <param name="destination">The key that receives the result. It is replaced; an empty result removes it.</param>
    /// <param name="source">The geo set to search.</param>
    /// <param name="member">The member whose position is the centre.</param>
    /// <param name="radius">Radius in <paramref name="unit"/>. Must not be negative.</param>
    /// <param name="unit"><c>m</c>, <c>km</c>, <c>ft</c> or <c>mi</c>.</param>
    /// <returns>How many members were stored.</returns>
    /// <exception cref="RuvioException">The centre member does not exist (<c>could not decode requested zset member</c>); a negative radius or unsupported unit; wrongtype.</exception>
    /// <remarks>On a multi-shard server <paramref name="destination"/> and <paramref name="source"/> must share a slot.</remarks>
    /// <example>
    /// <code>
    /// long stored = await db.GeoSearchStoreAsync("{s}:near", "{s}:shops", "istanbul", 50, "km");
    /// </code>
    /// </example>
    ValueTask<long> GeoSearchStoreAsync(string destination, string source, string member, double radius, string unit);

    /// <summary>Sends <c>GEORADIUSBYMEMBER ... ASC</c>: members within a radius of an existing member.</summary>
    /// <param name="key">The key.</param>
    /// <param name="member">The member whose position is the centre.</param>
    /// <param name="radius">Radius in <paramref name="unit"/>. Must not be negative.</param>
    /// <param name="unit"><c>m</c>, <c>km</c>, <c>ft</c> or <c>mi</c>.</param>
    /// <returns>Member names within the radius (including the centre member), nearest first.</returns>
    /// <exception cref="RuvioException">The centre member does not exist; a negative radius or unsupported unit; wrongtype.</exception>
    /// <remarks><c>GEOSEARCH</c> supersedes this command in Redis; it is kept for compatibility.</remarks>
    /// <example>
    /// <code>
    /// var near = await db.GeoRadiusByMemberAsync("shops", "istanbul", 100, "km");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> GeoRadiusByMemberAsync(string key, string member, double radius, string unit);

    /// <summary>Sends <c>GEORADIUS ... ASC</c>: members within a radius of a point.</summary>
    /// <param name="key">The key.</param>
    /// <param name="longitude">Centre longitude in degrees (-180 to 180).</param>
    /// <param name="latitude">Centre latitude in degrees (-85.05112878 to 85.05112878).</param>
    /// <param name="radius">Radius in <paramref name="unit"/>. Must not be negative.</param>
    /// <param name="unit"><c>m</c>, <c>km</c>, <c>ft</c> or <c>mi</c>.</param>
    /// <returns>Member names within the radius, nearest first.</returns>
    /// <exception cref="RuvioException">A coordinate out of range, a negative radius or unsupported unit; wrongtype.</exception>
    /// <remarks>Kept for compatibility; <see cref="GeoSearchAsync(string, double, double, double, GeoUnit, GeoOrder)"/> is the current form.</remarks>
    /// <example>
    /// <code>
    /// var near = await db.GeoRadiusAsync("shops", 29.0, 41.0, 25, "km");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> GeoRadiusAsync(string key, double longitude, double latitude, double radius, string unit);

    /// <summary>
    /// Sends <c>GEOSEARCH FROMLONLAT ... BYRADIUS ... ASC|DESC</c> with a typed unit and order: members within a circle around a point.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="longitude">Centre longitude in degrees (-180 to 180).</param>
    /// <param name="latitude">Centre latitude in degrees (-85.05112878 to 85.05112878).</param>
    /// <param name="radius">Radius in <paramref name="unit"/>. Must not be negative.</param>
    /// <param name="unit">The unit of <paramref name="radius"/>, a <see cref="GeoUnit"/>.</param>
    /// <param name="order"><see cref="GeoOrder.Ascending"/> (default) lists the nearest member first; <see cref="GeoOrder.Descending"/> the farthest first.</param>
    /// <returns>Member names inside the circle in the requested order. Empty when none match or the key is missing.</returns>
    /// <exception cref="RuvioException">A coordinate out of range or a negative radius; wrongtype.</exception>
    /// <example>
    /// <code>
    /// var near = await db.GeoSearchAsync("shops", 29.0, 41.0, 25, GeoUnit.Kilometers);
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> GeoSearchAsync(string key, double longitude, double latitude, double radius, GeoUnit unit, GeoOrder order = GeoOrder.Ascending)
        => RespRead.Strings(ExecuteAsync(new string[] { "GEOSEARCH", key, "FROMLONLAT", RespRead.Number(longitude), RespRead.Number(latitude), "BYRADIUS", RespRead.Number(radius), Keywords.Word(unit), Keywords.Word(order) }, CancellationToken.None), "GEOSEARCH");

    /// <summary>
    /// Sends <c>GEOSEARCHSTORE ... FROMMEMBER ... BYRADIUS</c> with a typed unit: stores the members within a circle around another member into a new geo set.
    /// </summary>
    /// <param name="destination">The key that receives the result. It is replaced; an empty result removes it.</param>
    /// <param name="source">The geo set to search.</param>
    /// <param name="member">The member whose position is the centre.</param>
    /// <param name="radius">Radius in <paramref name="unit"/>. Must not be negative.</param>
    /// <param name="unit">The unit of <paramref name="radius"/>, a <see cref="GeoUnit"/>.</param>
    /// <returns>How many members were stored.</returns>
    /// <exception cref="RuvioException">The centre member does not exist (<c>could not decode requested zset member</c>); a negative radius; wrongtype.</exception>
    /// <remarks>On a multi-shard server <paramref name="destination"/> and <paramref name="source"/> must share a slot.</remarks>
    /// <example>
    /// <code>
    /// long stored = await db.GeoSearchStoreAsync("{s}:near", "{s}:shops", "istanbul", 50, GeoUnit.Kilometers);
    /// </code>
    /// </example>
    ValueTask<long> GeoSearchStoreAsync(string destination, string source, string member, double radius, GeoUnit unit)
        => RespRead.Integer(ExecuteAsync(new string[] { "GEOSEARCHSTORE", destination, source, "FROMMEMBER", member, "BYRADIUS", RespRead.Number(radius), Keywords.Word(unit) }, CancellationToken.None), "GEOSEARCHSTORE");

    /// <summary>
    /// Sends <c>GEORADIUSBYMEMBER</c> with a typed unit and order: members within a radius of an existing member.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="member">The member whose position is the centre.</param>
    /// <param name="radius">Radius in <paramref name="unit"/>. Must not be negative.</param>
    /// <param name="unit">The unit of <paramref name="radius"/>, a <see cref="GeoUnit"/>.</param>
    /// <param name="order"><see cref="GeoOrder.Ascending"/> (default) lists the nearest member first; <see cref="GeoOrder.Descending"/> the farthest first.</param>
    /// <returns>Member names within the radius (including the centre member) in the requested order.</returns>
    /// <exception cref="RuvioException">The centre member does not exist; a negative radius; wrongtype.</exception>
    /// <remarks><c>GEOSEARCH</c> supersedes this command in Redis; it is kept for compatibility.</remarks>
    /// <example>
    /// <code>
    /// var near = await db.GeoRadiusByMemberAsync("shops", "istanbul", 100, GeoUnit.Kilometers);
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> GeoRadiusByMemberAsync(string key, string member, double radius, GeoUnit unit, GeoOrder order = GeoOrder.Ascending)
        => RespRead.Strings(ExecuteAsync(new string[] { "GEORADIUSBYMEMBER", key, member, RespRead.Number(radius), Keywords.Word(unit), Keywords.Word(order) }, CancellationToken.None), "GEORADIUSBYMEMBER");

    /// <summary>Sends <c>GEORADIUS</c> with a typed unit and order: members within a radius of a point.</summary>
    /// <param name="key">The key.</param>
    /// <param name="longitude">Centre longitude in degrees (-180 to 180).</param>
    /// <param name="latitude">Centre latitude in degrees (-85.05112878 to 85.05112878).</param>
    /// <param name="radius">Radius in <paramref name="unit"/>. Must not be negative.</param>
    /// <param name="unit">The unit of <paramref name="radius"/>, a <see cref="GeoUnit"/>.</param>
    /// <param name="order"><see cref="GeoOrder.Ascending"/> (default) lists the nearest member first; <see cref="GeoOrder.Descending"/> the farthest first.</param>
    /// <returns>Member names within the radius in the requested order.</returns>
    /// <exception cref="RuvioException">A coordinate out of range or a negative radius; wrongtype.</exception>
    /// <remarks>Kept for compatibility; <see cref="GeoSearchAsync(string, double, double, double, GeoUnit, GeoOrder)"/> is the current form.</remarks>
    /// <example>
    /// <code>
    /// var near = await db.GeoRadiusAsync("shops", 29.0, 41.0, 25, GeoUnit.Kilometers);
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> GeoRadiusAsync(string key, double longitude, double latitude, double radius, GeoUnit unit, GeoOrder order = GeoOrder.Ascending)
        => RespRead.Strings(ExecuteAsync(new string[] { "GEORADIUS", key, RespRead.Number(longitude), RespRead.Number(latitude), RespRead.Number(radius), Keywords.Word(unit), Keywords.Word(order) }, CancellationToken.None), "GEORADIUS");

    /// <summary>Sends <c>ZRANK</c> (or <c>ZREVRANK</c> when descending).</summary>
    /// <param name="key">The key.</param>
    /// <param name="member">The member.</param>
    /// <param name="descending">When true, rank 0 is the member with the highest score.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The zero-based rank, or <see langword="null"/> when the member or key does not exist.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long? place = await db.GetSortedSetRankAsync("board", "ada", descending: true);
    /// </code>
    /// </example>
    ValueTask<long?> GetSortedSetRankAsync(string key, string member, bool descending = false, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>BF.RESERVE</c>: creates an empty scalable Bloom filter.</summary>
    /// <param name="key">The key.</param>
    /// <param name="errorRate">Target false-positive rate between 0 and 1, for example 0.01.</param>
    /// <param name="capacity">Number of items the first layer is sized for. When it fills, a larger, tighter layer is added.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <exception cref="RuvioException">The key already exists (of any type), or the error rate or capacity is invalid.</exception>
    /// <remarks>RedisBloom command shape; module compatibility is not certified. <see cref="AddBloomFilterAsync"/> on a missing key creates a filter with error 0.01 and capacity 100.</remarks>
    /// <example>
    /// <code>
    /// await db.ReserveBloomFilterAsync("seen:urls", 0.001, 1_000_000);
    /// </code>
    /// </example>
    ValueTask ReserveBloomFilterAsync(string key, double errorRate, long capacity, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>BF.ADD</c>: adds an item.</summary>
    /// <param name="key">The key.</param>
    /// <param name="item">The item to add.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when the item was new; <see langword="false"/> when it was (probably) already present.</returns>
    /// <exception cref="RuvioException">A non-scaling filter is full; wrongtype.</exception>
    /// <example>
    /// <code>
    /// bool isNew = await db.AddBloomFilterAsync("seen:urls", "https://example.com");
    /// </code>
    /// </example>
    ValueTask<bool> AddBloomFilterAsync(string key, string item, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>BF.EXISTS</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="item">The item to test.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="false"/> means certainly not added; <see langword="true"/> means probably added (false positives happen at the filter's error rate). A missing key gives false.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// bool seen = await db.BloomFilterContainsAsync("seen:urls", "https://example.com");
    /// </code>
    /// </example>
    ValueTask<bool> BloomFilterContainsAsync(string key, string item, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>XADD key NOMKSTREAM id field value ...</c>: appends an entry only when the stream already exists.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="id">Entry id: <c>*</c> (server picks), <c>ms-*</c> (fixed millisecond, server picks the sequence) or <c>ms-seq</c> (explicit; must be greater than the stream's last id).</param>
    /// <param name="fields">The field/value pairs of the entry; at least one.</param>
    /// <returns>The id of the new entry, or <see langword="null"/> when the stream does not exist (nothing is created).</returns>
    /// <exception cref="RuvioException">An explicit id is not greater than the last id; wrongtype.</exception>
    /// <example>
    /// <code>
    /// string? id = await db.AddStreamEntryIfExistsAsync("events", "*", ("type", "login"));
    /// </code>
    /// </example>
    ValueTask<string?> AddStreamEntryIfExistsAsync(string key, string id, params (string Field, string Value)[] fields);

    /// <summary>Sends <c>XADD</c>: appends an entry to a stream, creating the stream if needed.</summary>
    /// <param name="key">The key.</param>
    /// <param name="id">Entry id: <c>*</c> (server picks), <c>ms-*</c> or an explicit <c>ms-seq</c>, which must be greater than the stream's last id.</param>
    /// <param name="fields">The field/value pairs of the entry; at least one.</param>
    /// <returns>The id of the new entry, for example <c>1760000000000-0</c>.</returns>
    /// <exception cref="RuvioException">An explicit id is not greater than the last id (a millisecond behind the top id is rejected); wrongtype.</exception>
    /// <example>
    /// <code>
    /// string id = await db.AddStreamEntryAsync("events", "*", ("type", "login"), ("user", "ada"));
    /// </code>
    /// </example>
    ValueTask<string> AddStreamEntryAsync(string key, string id, params (string Field, string Value)[] fields);

    /// <summary>
    /// Sends <c>XADD key DELAY ms ...</c> (Ruvio extension): appends an entry that consumer groups cannot read until the delay has passed. Use it for scheduled or retried work.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="delayMs">How long <c>XREADGROUP</c> hides the entry, in milliseconds. <c>XRANGE</c> and <c>XREAD</c> still show it.</param>
    /// <param name="id">Entry id: <c>*</c>, <c>ms-*</c> or explicit <c>ms-seq</c>.</param>
    /// <param name="fields">The field/value pairs of the entry; at least one.</param>
    /// <returns>The id of the new entry.</returns>
    /// <exception cref="RuvioException">An explicit id is not greater than the last id; wrongtype.</exception>
    /// <example>
    /// <code>
    /// string id = await db.AddStreamEntryDelayedAsync("jobs", 60_000, "*", ("task", "send-reminder"));
    /// </code>
    /// </example>
    ValueTask<string> AddStreamEntryDelayedAsync(string key, long delayMs, string id, params (string Field, string Value)[] fields);

    /// <summary>
    /// Sends <c>XADD key MAXLEN n ...</c>: appends an entry and trims the stream so it keeps at most <paramref name="maxLength"/> of the newest entries.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="id">Entry id: <c>*</c>, <c>ms-*</c> or explicit <c>ms-seq</c>.</param>
    /// <param name="maxLength">The most entries to keep. Trimming is exact (Ruvio treats <c>~</c> as <c>=</c>).</param>
    /// <param name="fields">The field/value pairs of the entry; at least one.</param>
    /// <returns>The id of the new entry.</returns>
    /// <exception cref="RuvioException">An explicit id is not greater than the last id; wrongtype.</exception>
    /// <example>
    /// <code>
    /// await db.AddStreamEntryAsync("events", "*", 10_000, ("type", "login"));
    /// </code>
    /// </example>
    ValueTask<string> AddStreamEntryAsync(string key, string id, long maxLength, params (string Field, string Value)[] fields);

    /// <summary>
    /// Sends <c>XADD key MAXLEN|MINID [=|~] threshold id ...</c> with a typed <see cref="StreamTrim"/>: appends an entry and trims the stream in the same command.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="id">Entry id: <c>*</c> (see <see cref="StreamIds.New"/>), <c>ms-*</c> or explicit <c>ms-seq</c>.</param>
    /// <param name="trim">The trim rule: <see cref="StreamTrim.MaxLen"/> or <see cref="StreamTrim.MinId"/>, exact or approximate.</param>
    /// <param name="fields">The field/value pairs of the entry; at least one.</param>
    /// <returns>The id of the new entry.</returns>
    /// <exception cref="RuvioException">An explicit id is not greater than the last id; wrongtype.</exception>
    /// <example>
    /// <code>
    /// await db.AddStreamEntryAsync("events", StreamIds.New, StreamTrim.MaxLen(10_000, StreamTrimMode.Approximate), ("type", "login"));
    /// </code>
    /// </example>
    ValueTask<string> AddStreamEntryAsync(string key, string id, StreamTrim trim, params (string Field, string Value)[] fields)
    {
        var arguments = new List<string>(fields.Length * 2 + 6) { "XADD", key, Keywords.Word(trim.Strategy), Keywords.Word(trim.Mode) };

        arguments.Add(trim.Strategy == StreamTrimStrategy.MinId ? trim.Id ?? throw new ArgumentException("A MINID trim needs an id.", nameof(trim)) : RespRead.Number(trim.Length));
        arguments.Add(id);

        foreach ((string field, string value) in fields)
        {
            arguments.Add(field);
            arguments.Add(value);
        }

        return RespRead.RequiredText(ExecuteAsync(arguments, CancellationToken.None), "XADD");
    }

    /// <summary>Sends <c>XLEN</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The number of entries; 0 for a missing key.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long n = await db.GetStreamLengthAsync("events");
    /// </code>
    /// </example>
    ValueTask<long> GetStreamLengthAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>XINFO STREAM</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The raw reply: length, last generated id, group count, first and last entry. It has no radix-tree or entries-added fields.</returns>
    /// <exception cref="RuvioException">The key does not exist; wrongtype.</exception>
    /// <example>
    /// <code>
    /// RespValue info = await db.GetStreamInfoAsync("events");
    /// </code>
    /// </example>
    ValueTask<RespValue> GetStreamInfoAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>XINFO GROUPS</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The raw reply: one record per group with its name, consumers, pending count, last delivered id and lag.</returns>
    /// <exception cref="RuvioException">The key does not exist; wrongtype.</exception>
    /// <example>
    /// <code>
    /// RespValue groups = await db.GetStreamGroupsAsync("events");
    /// </code>
    /// </example>
    ValueTask<RespValue> GetStreamGroupsAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>XSETID</c>: moves the stream's last generated id forward without adding an entry.</summary>
    /// <param name="key">The key.</param>
    /// <param name="id">The new last id, <c>ms-seq</c>. It must not be below the stream's current top id.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <exception cref="RuvioException">The stream does not exist, or the id is below the top id.</exception>
    /// <example>
    /// <code>
    /// await db.SetStreamIdAsync("events", "1760000000000-0");
    /// </code>
    /// </example>
    ValueTask SetStreamIdAsync(string key, string id, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>XAUTOCLAIM</c>: takes over pending entries that another consumer has left idle too long.</summary>
    /// <param name="key">The key.</param>
    /// <param name="group">The consumer group name.</param>
    /// <param name="consumer">The consumer that receives the entries.</param>
    /// <param name="minIdleMs">Only entries idle for at least this many milliseconds are claimed.</param>
    /// <param name="start">Id to start scanning after (exclusive); <c>0-0</c> starts at the beginning.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The raw reply: the next start id (<c>0-0</c> when the scan is complete), the claimed entries, and ids dropped because their entry no longer exists.</returns>
    /// <exception cref="RuvioException"><c>NOGROUP</c> when the group or stream does not exist.</exception>
    /// <example>
    /// <code>
    /// RespValue claimed = await db.AutoClaimStreamAsync("jobs", "workers", "w2", 30_000, "0-0");
    /// </code>
    /// </example>
    ValueTask<RespValue> AutoClaimStreamAsync(string key, string group, string consumer, long minIdleMs, string start, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>XINFO CONSUMERS</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="group">The consumer group name.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The raw reply: consumers that hold pending entries, with pending count and idle time. A consumer with nothing pending is not listed.</returns>
    /// <exception cref="RuvioException">The group does not exist.</exception>
    /// <example>
    /// <code>
    /// RespValue consumers = await db.GetStreamConsumersAsync("jobs", "workers");
    /// </code>
    /// </example>
    ValueTask<RespValue> GetStreamConsumersAsync(string key, string group, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>XRANGE</c> (or <c>XREVRANGE</c> when reversed): reads entries between two ids.</summary>
    /// <param name="key">The key.</param>
    /// <param name="start">Lowest id, inclusive: <c>-</c> (the beginning), <c>ms</c> or <c>ms-seq</c>.</param>
    /// <param name="end">Highest id, inclusive: <c>+</c> (the end), <c>ms</c> or <c>ms-seq</c>. Exclusive bounds are not supported.</param>
    /// <param name="reverse">When true, entries come newest first. The arguments stay (<paramref name="start"/> low, <paramref name="end"/> high); the client swaps them for <c>XREVRANGE</c>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The entries in order; empty when none match or the key is missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// IReadOnlyList&lt;StreamEntry&gt; all = await db.GetStreamRangeAsync("events", "-", "+");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<StreamEntry>> GetStreamRangeAsync(string key, string start, string end, bool reverse = false, CancellationToken cancellationToken = default);

    /// <summary>Like the other range overload, with <c>COUNT</c> limiting how many entries come back.</summary>
    /// <param name="key">The key.</param>
    /// <param name="start">Lowest id, inclusive: <c>-</c>, <c>ms</c> or <c>ms-seq</c>.</param>
    /// <param name="end">Highest id, inclusive: <c>+</c>, <c>ms</c> or <c>ms-seq</c>.</param>
    /// <param name="count">The most entries to return. When reversed the newest matches are kept.</param>
    /// <param name="reverse">When true, entries come newest first.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>Up to <paramref name="count"/> entries.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// var last10 = await db.GetStreamRangeAsync("events", "-", "+", 10, reverse: true);
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<StreamEntry>> GetStreamRangeAsync(string key, string start, string end, int count, bool reverse = false, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>XDEL</c>: removes entries by id.</summary>
    /// <param name="key">The key.</param>
    /// <param name="ids">The ids to remove; a repeated id counts once.</param>
    /// <returns>How many entries were removed. The stream and its last id remain even when it becomes empty.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long n = await db.DeleteStreamEntriesAsync("events", "1760000000000-0");
    /// </code>
    /// </example>
    ValueTask<long> DeleteStreamEntriesAsync(string key, params string[] ids);

    /// <summary>Sends <c>XTRIM key MINID id</c>: removes entries with an id below <paramref name="id"/>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="id">The lowest id to keep (kept itself).</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>How many entries were removed.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long n = await db.TrimStreamMinIdAsync("events", "1760000000000-0");
    /// </code>
    /// </example>
    ValueTask<long> TrimStreamMinIdAsync(string key, string id, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>XTRIM key MAXLEN</c>: keeps only the newest entries.</summary>
    /// <param name="key">The key.</param>
    /// <param name="maxLength">The most entries to keep. Trimming is exact.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>How many entries were removed.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long n = await db.TrimStreamAsync("events", 10_000);
    /// </code>
    /// </example>
    ValueTask<long> TrimStreamAsync(string key, long maxLength, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>XTRIM key MAXLEN|MINID [=|~] threshold</c> with a typed <see cref="StreamTrim"/>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="trim">The trim rule: <see cref="StreamTrim.MaxLen"/> or <see cref="StreamTrim.MinId"/>, exact or approximate.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>How many entries were removed.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long n = await db.TrimStreamAsync("events", StreamTrim.MaxLen(10_000, StreamTrimMode.Approximate));
    /// </code>
    /// </example>
    ValueTask<long> TrimStreamAsync(string key, StreamTrim trim, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(new string[] { "XTRIM", key, Keywords.Word(trim.Strategy), Keywords.Word(trim.Mode), trim.Strategy == StreamTrimStrategy.MinId ? trim.Id ?? throw new ArgumentException("A MINID trim needs an id.", nameof(trim)) : RespRead.Number(trim.Length) }, cancellationToken), "XTRIM");

    /// <summary>
    /// Sends <c>XREAD STREAMS</c>: reads entries newer than a given id from one or more streams, without waiting.
    /// </summary>
    /// <param name="streams">Each stream key with the last id you have seen (exclusive). <c>0</c> reads from the beginning; <c>$</c> means "the current end", which returns nothing without blocking.</param>
    /// <returns>One <see cref="StreamReadResult"/> per stream that has new entries; empty when there is nothing new.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>On a multi-shard server the streams must share a slot. To wait for new entries use the overload with <see cref="StreamReadOptions"/>.</remarks>
    /// <example>
    /// <code>
    /// var results = await db.GetStreamUpdatesAsync(("events", "0"));
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<StreamReadResult>> GetStreamUpdatesAsync(params (string Key, string Id)[] streams);

    /// <summary>Sends <c>XREAD</c> with <c>COUNT</c> and <c>BLOCK</c>.</summary>
    /// <param name="options"><see cref="StreamReadOptions.Count"/> limits entries per stream; <see cref="StreamReadOptions.Block"/> waits for new entries (<see cref="TimeSpan.Zero"/> waits until one arrives). <see cref="StreamReadOptions.NoAcknowledge"/> is for group reads and is rejected here with <see cref="ArgumentException"/>.</param>
    /// <param name="streams">Each stream key with the last id you have seen (exclusive); <c>$</c> means only entries added after the call.</param>
    /// <returns>One result per stream with new entries. A block that times out returns an empty list.</returns>
    /// <exception cref="RuvioException"><paramref name="options"/> has <c>NoAcknowledge</c> or a negative <c>Block</c>; wrongtype.</exception>
    /// <remarks>A blocking read holds the shared command connection until it returns; use a connection of its own. Blocking is shard-local.</remarks>
    /// <example>
    /// <code>
    /// var results = await worker.GetStreamUpdatesAsync(
    ///     new StreamReadOptions { Count = 100, Block = TimeSpan.FromSeconds(5) },
    ///     ("events", "$"));
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<StreamReadResult>> GetStreamUpdatesAsync(StreamReadOptions options, params (string Key, string Id)[] streams);

    /// <summary>Sends <c>XGROUP CREATECONSUMER</c>: registers a consumer in a group before it has read anything.</summary>
    /// <param name="key">The key.</param>
    /// <param name="group">The consumer group name.</param>
    /// <param name="consumer">The consumer name.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>1 when the consumer name is new; 0 when the group already had it.</returns>
    /// <exception cref="RuvioException"><c>NOGROUP</c> when the group does not exist.</exception>
    /// <example>
    /// <code>
    /// long created = await db.CreateStreamConsumerAsync("jobs", "workers", "w1");
    /// </code>
    /// </example>
    ValueTask<long> CreateStreamConsumerAsync(string key, string group, string consumer, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>XGROUP CREATE</c>: creates a consumer group on an existing stream.</summary>
    /// <param name="key">The key.</param>
    /// <param name="group">The consumer group name.</param>
    /// <param name="id">Where the group starts reading: <c>$</c> (only new entries), <c>0</c> (the whole stream) or an explicit id.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <exception cref="RuvioException">The stream does not exist, or the group already exists (<c>BUSYGROUP</c>).</exception>
    /// <example>
    /// <code>
    /// await db.CreateStreamGroupAsync("jobs", "workers", "0");
    /// </code>
    /// </example>
    ValueTask CreateStreamGroupAsync(string key, string group, string id, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>XGROUP CREATE</c>, optionally creating the stream (<c>MKSTREAM</c>).</summary>
    /// <param name="key">The key.</param>
    /// <param name="group">The consumer group name.</param>
    /// <param name="id">Where the group starts reading: <c>$</c>, <c>0</c> or an explicit id.</param>
    /// <param name="createStream">When true, an empty stream is created if the key is missing.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <exception cref="RuvioException">The group already exists (<c>BUSYGROUP</c>); with <paramref name="createStream"/> false, the stream does not exist.</exception>
    /// <example>
    /// <code>
    /// await db.CreateStreamGroupAsync("jobs", "workers", "$", createStream: true);
    /// </code>
    /// </example>
    ValueTask CreateStreamGroupAsync(string key, string group, string id, bool createStream, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>XREADGROUP GROUP group consumer STREAMS key id</c>: reads entries for one consumer of a group, without waiting.
    /// </summary>
    /// <param name="group">The consumer group name.</param>
    /// <param name="consumer">The consumer name.</param>
    /// <param name="key">The stream.</param>
    /// <param name="id">Use <c>&gt;</c> to receive entries never delivered to the group. Ruvio supports only <c>&gt;</c> here, and one stream per call.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>A result for the stream holding the new entries, which are added to the consumer's pending list until acknowledged. Empty when nothing is available.</returns>
    /// <exception cref="RuvioException"><c>NOGROUP</c> when the stream or group does not exist.</exception>
    /// <remarks>Entries added with a delay are skipped until the delay has passed. Acknowledge with <see cref="AcknowledgeStreamAsync"/>.</remarks>
    /// <example>
    /// <code>
    /// var batch = await db.GetStreamGroupUpdatesAsync("workers", "w1", "jobs", "&gt;");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<StreamReadResult>> GetStreamGroupUpdatesAsync(string group, string consumer, string key, string id, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>XREADGROUP</c> with <c>COUNT</c>, <c>BLOCK</c> and <c>NOACK</c>.</summary>
    /// <param name="group">The consumer group name.</param>
    /// <param name="consumer">The consumer name.</param>
    /// <param name="key">The stream.</param>
    /// <param name="id">Use <c>&gt;</c> (only new entries are supported).</param>
    /// <param name="options"><see cref="StreamReadOptions.Count"/> limits the batch; <see cref="StreamReadOptions.Block"/> waits for a new entry; <see cref="StreamReadOptions.NoAcknowledge"/> advances the group without adding pending entries.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The new entries; empty when nothing is available or a block times out.</returns>
    /// <exception cref="RuvioException"><c>NOGROUP</c>; a negative <c>Block</c>.</exception>
    /// <remarks>A blocking read holds the shared command connection until it returns; use a connection of its own.</remarks>
    /// <example>
    /// <code>
    /// var batch = await worker.GetStreamGroupUpdatesAsync("workers", "w1", "jobs", "&gt;",
    ///     new StreamReadOptions { Count = 10, Block = TimeSpan.FromSeconds(5) });
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<StreamReadResult>> GetStreamGroupUpdatesAsync(string group, string consumer, string key, string id, StreamReadOptions options, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>XGROUP DESTROY</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="group">The consumer group name.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>1 when the group was removed; 0 when it did not exist.</returns>
    /// <example>
    /// <code>
    /// long n = await db.DestroyStreamGroupAsync("jobs", "workers");
    /// </code>
    /// </example>
    ValueTask<long> DestroyStreamGroupAsync(string key, string group, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>XGROUP SETID</c>: moves the group's last-delivered id. Pending entries stay as they are.</summary>
    /// <param name="key">The key.</param>
    /// <param name="group">The consumer group name.</param>
    /// <param name="id"><c>$</c>, <c>0</c> or an explicit id.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <exception cref="RuvioException"><c>NOGROUP</c> when the group does not exist.</exception>
    /// <example>
    /// <code>
    /// await db.SetStreamGroupIdAsync("jobs", "workers", "0");
    /// </code>
    /// </example>
    ValueTask SetStreamGroupIdAsync(string key, string group, string id, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>XGROUP DELCONSUMER</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="group">The consumer group name.</param>
    /// <param name="consumer">The consumer name.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>How many pending entries the consumer held (they are dropped); 0 for an unknown consumer.</returns>
    /// <exception cref="RuvioException"><c>NOGROUP</c> when the group does not exist.</exception>
    /// <example>
    /// <code>
    /// long dropped = await db.DeleteStreamGroupConsumerAsync("jobs", "workers", "w1");
    /// </code>
    /// </example>
    ValueTask<long> DeleteStreamGroupConsumerAsync(string key, string group, string consumer, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>XACK</c>: marks entries as processed by removing them from the group's pending list. The stream entries stay stored.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="group">The consumer group name.</param>
    /// <param name="ids">The ids to acknowledge; a repeated id counts once.</param>
    /// <returns>How many ids were pending and are now acknowledged.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long n = await db.AcknowledgeStreamAsync("jobs", "workers", id);
    /// </code>
    /// </example>
    ValueTask<long> AcknowledgeStreamAsync(string key, string group, params string[] ids);

    /// <summary>Sends <c>XPENDING key group</c>: the summary of a group's pending entries.</summary>
    /// <param name="key">The key.</param>
    /// <param name="group">The consumer group name.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The raw reply: the pending count, smallest id, greatest id and a per-consumer count. An empty group gives 0, two nulls and an empty list.</returns>
    /// <exception cref="RuvioException"><c>NOGROUP</c>.</exception>
    /// <example>
    /// <code>
    /// RespValue summary = await db.GetStreamPendingAsync("jobs", "workers");
    /// </code>
    /// </example>
    ValueTask<RespValue> GetStreamPendingAsync(string key, string group, CancellationToken cancellationToken = default);

    /// <summary>Sends the extended <c>XPENDING</c>: lists pending entries one by one.</summary>
    /// <param name="key">The key.</param>
    /// <param name="group">The consumer group name.</param>
    /// <param name="start">Lowest id, inclusive (<c>-</c> for the beginning).</param>
    /// <param name="end">Highest id, inclusive (<c>+</c> for the end).</param>
    /// <param name="count">The most rows to return.</param>
    /// <param name="consumer">Only this consumer's entries; null for all.</param>
    /// <param name="minIdle">Only entries idle for at least this long; null for all.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>One row per pending entry: id, consumer, idle milliseconds and delivery count.</returns>
    /// <exception cref="RuvioException"><c>NOGROUP</c>.</exception>
    /// <example>
    /// <code>
    /// var stuck = await db.GetStreamPendingAsync("jobs", "workers", "-", "+", 50, minIdle: TimeSpan.FromMinutes(1));
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<StreamPendingEntry>> GetStreamPendingAsync(string key, string group, string start, string end, long count, string? consumer = null, TimeSpan? minIdle = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>XCLAIM</c>: moves pending entries to <paramref name="consumer"/> when they have been idle long enough.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="group">The consumer group name.</param>
    /// <param name="consumer">The consumer name.</param>
    /// <param name="minIdleMilliseconds">Only entries idle for at least this many milliseconds are claimed.</param>
    /// <param name="ids">The entry ids to claim; a repeated id is claimed once.</param>
    /// <returns>The claimed entries. An id deleted from the stream is dropped from the pending list and not returned.</returns>
    /// <exception cref="RuvioException"><c>NOGROUP</c>.</exception>
    /// <example>
    /// <code>
    /// var mine = await db.ClaimStreamEntriesAsync("jobs", "workers", "w2", 30_000, id);
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<StreamEntry>> ClaimStreamEntriesAsync(string key, string group, string consumer, long minIdleMilliseconds, params string[] ids);

    /// <summary>
    /// Sends <c>XCLAIM</c> with <c>IDLE</c>, <c>TIME</c>, <c>RETRYCOUNT</c>, <c>FORCE</c> and <c>LASTID</c> taken from <paramref name="options"/>.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="group">The consumer group name.</param>
    /// <param name="consumer">The consumer name.</param>
    /// <param name="minIdleMilliseconds">Only entries idle for at least this many milliseconds are claimed.</param>
    /// <param name="options">The extra claim settings (see <see cref="StreamClaimOptions"/>). <c>Idle</c> and <c>Time</c> cannot be combined.</param>
    /// <param name="ids">The entry ids to claim.</param>
    /// <returns>The claimed entries.</returns>
    /// <exception cref="RuvioException"><c>Idle</c> and <c>Time</c> are both set (<see cref="ArgumentException"/>); <c>NOGROUP</c>.</exception>
    /// <example>
    /// <code>
    /// var mine = await db.ClaimStreamEntriesAsync("jobs", "workers", "w2", 30_000, new StreamClaimOptions { RetryCount = 3 }, id);
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<StreamEntry>> ClaimStreamEntriesAsync(string key, string group, string consumer, long minIdleMilliseconds, StreamClaimOptions options, params string[] ids);

    /// <summary>
    /// Sends <c>XCLAIM ... JUSTID</c>: claims entries but returns only their ids, without reading the entries.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="group">The consumer group name.</param>
    /// <param name="consumer">The consumer name.</param>
    /// <param name="minIdleMilliseconds">Only entries idle for at least this many milliseconds are claimed.</param>
    /// <param name="options">Extra claim settings, or null for none.</param>
    /// <param name="ids">The entry ids to claim.</param>
    /// <returns>The ids that were claimed.</returns>
    /// <exception cref="RuvioException"><c>Idle</c> and <c>Time</c> are both set (<see cref="ArgumentException"/>); <c>NOGROUP</c>.</exception>
    /// <example>
    /// <code>
    /// var ids = await db.ClaimStreamEntryIdsAsync("jobs", "workers", "w2", 30_000, null, id);
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> ClaimStreamEntryIdsAsync(string key, string group, string consumer, long minIdleMilliseconds, StreamClaimOptions? options, params string[] ids);

    /// <summary>Sends <c>INFO</c>: server statistics as text.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The <c>INFO</c> text: <c>name:value</c> lines grouped under <c># Section</c> headings.</returns>
    /// <example>
    /// <code>
    /// string info = await db.GetInfoAsync();
    /// </code>
    /// </example>
    ValueTask<string> GetInfoAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>INFO section</c>.</summary>
    /// <param name="section">The section to return, for example <c>keyspace</c>; <c>prometheus</c> returns the metrics in Prometheus text format.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The text of that section.</returns>
    /// <remarks>On a multi-shard server the answer comes from the one shard the client sent it to.</remarks>
    /// <example>
    /// <code>
    /// string metrics = await db.GetInfoAsync("prometheus");
    /// </code>
    /// </example>
    ValueTask<string> GetInfoAsync(string section, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>INFO section</c> with a typed <see cref="InfoSection"/>.</summary>
    /// <param name="section">The section to return; <see cref="InfoSection.Prometheus"/> returns the metrics in Prometheus text format.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The text of that section.</returns>
    /// <remarks>On a multi-shard server the answer comes from the one shard the client sent it to.</remarks>
    /// <example>
    /// <code>
    /// string keyspace = await db.GetInfoAsync(InfoSection.Keyspace);
    /// </code>
    /// </example>
    ValueTask<string> GetInfoAsync(InfoSection section, CancellationToken cancellationToken = default)
        => RespRead.RequiredText(ExecuteAsync(new string[] { "INFO", Keywords.Word(section) }, cancellationToken), "INFO");

    /// <summary>Sends <c>SAVE</c>: writes a checkpoint in the server process and returns when it is done.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <exception cref="RuvioException">Persistence is not configured or the checkpoint fails.</exception>
    /// <example>
    /// <code>
    /// await db.SaveAsync();
    /// </code>
    /// </example>
    ValueTask SaveAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>BGSAVE</c>: writes a checkpoint. Ruvio runs it in-process, not in a forked child as Redis does.
    /// </summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <exception cref="RuvioException">Persistence is not configured or the checkpoint fails.</exception>
    /// <example>
    /// <code>
    /// await db.SaveInBackgroundAsync();
    /// </code>
    /// </example>
    ValueTask SaveInBackgroundAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>PUBLISH</c>: delivers a message to every client subscribed to the channel or to a matching pattern.
    /// </summary>
    /// <param name="channel">The channel name.</param>
    /// <param name="message">The message text.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The number of clients that received it.</returns>
    /// <remarks>Messages are not stored: a subscriber that is not connected misses them. A subscriber whose queue is full is disconnected.</remarks>
    /// <example>
    /// <code>
    /// long receivers = await db.PublishAsync("news", "hello");
    /// </code>
    /// </example>
    ValueTask<long> PublishAsync(string channel, string message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>SPUBLISH</c>: delivers a message to subscribers of a sharded channel on the shard that owns the channel's hash slot.
    /// </summary>
    /// <param name="channel">The channel name. It is hashed like a key, so <c>{tag}</c> works.</param>
    /// <param name="message">The message text.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The number of subscribers that received it.</returns>
    /// <remarks>Pair it with <see cref="SubscribeShardedAsync(string, Action{PubSubMessage}, CancellationToken)"/>. Not stored and not durable.</remarks>
    /// <example>
    /// <code>
    /// long receivers = await db.PublishShardedAsync("room:{42}", "hi");
    /// </code>
    /// </example>
    ValueTask<long> PublishShardedAsync(string channel, string message, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>EVAL</c>: runs a Lua 5.1 script on the server.</summary>
    /// <param name="script">The Lua source. It reads <c>KEYS</c> and <c>ARGV</c> and calls commands with <c>ruvio.call</c>.</param>
    /// <param name="keyCount">How many of <paramref name="arguments"/> are key names; the rest go to <c>ARGV</c>.</param>
    /// <param name="arguments">The key names first, then the other arguments.</param>
    /// <returns>The script's result as a <see cref="RespValue"/>.</returns>
    /// <exception cref="RuvioException">A Lua error or a script limit (instruction quota, time limit); the keys span slots on a multi-shard server (<c>CROSSSLOT</c>). A failed script's writes are rolled back.</exception>
    /// <remarks>Declared keys must share a slot, and the script may only touch them. A script with no keys runs on the connection's shard.</remarks>
    /// <example>
    /// <code>
    /// RespValue sum = await db.EvaluateAsync("return tonumber(ARGV[1]) + tonumber(ARGV[2])", 0, "2", "3");
    /// long result = sum.Integer;   // 5
    /// </code>
    /// </example>
    ValueTask<RespValue> EvaluateAsync(string script, long keyCount, params string[] arguments);

    /// <summary>Sends <c>MULTI</c>: starts queuing commands.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <remarks>All callers share one connection, so commands other tasks send before <see cref="ExecuteTransactionAsync"/> are queued into the same transaction. Prefer <see cref="ExecuteManyAsync"/> with the whole <c>MULTI</c> ... <c>EXEC</c> sequence, which is written as one unit. A transaction is shard-local and same-slot, holds at most 1,024 commands, and cannot be nested.</remarks>
    /// <example>
    /// <code>
    /// await db.BeginTransactionAsync();
    /// </code>
    /// </example>
    ValueTask BeginTransactionAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>EXEC</c>: runs the queued commands as one unit.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>One reply per queued command, in order; an empty array when nothing was queued. If a watched key changed, the transaction is aborted and the result is empty.</returns>
    /// <exception cref="RuvioException"><c>EXECABORT</c> when a command was refused while queuing (wrong arity, unknown command, cross-slot); <c>EXEC without MULTI</c>.</exception>
    /// <example>
    /// <code>
    /// IReadOnlyList&lt;RespValue&gt; replies = await db.ExecuteTransactionAsync();
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<RespValue>> ExecuteTransactionAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>DISCARD</c>: drops the queued commands. Watches stay in place.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <exception cref="RuvioException"><c>DISCARD without MULTI</c>.</exception>
    /// <example>
    /// <code>
    /// await db.DiscardTransactionAsync();
    /// </code>
    /// </example>
    ValueTask DiscardTransactionAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>WATCH</c>: makes the next <c>EXEC</c> abort if any of these keys is changed first (optimistic locking).
    /// </summary>
    /// <param name="keys">The keys to watch (at most 128). On a multi-shard server they must share a slot.</param>
    /// <remarks>A change by any connection, including this one, counts.</remarks>
    /// <example>
    /// <code>
    /// await db.WatchAsync("balance:ada");
    /// </code>
    /// </example>
    ValueTask WatchAsync(params string[] keys);

    /// <summary>Sends <c>UNWATCH</c>: forgets all watched keys.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <example>
    /// <code>
    /// await db.UnwatchAsync();
    /// </code>
    /// </example>
    ValueTask UnwatchAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>ACL WHOAMI</c>.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The user name from the last <c>AUTH</c> (<c>default</c> when none was used).</returns>
    /// <example>
    /// <code>
    /// string me = await db.GetAclUserNameAsync();
    /// </code>
    /// </example>
    ValueTask<string> GetAclUserNameAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>CLUSTER SLOTS</c>: which shard owns which hash slots.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The raw reply: slot ranges with the host and port of each owner. The topology is static; host and port come from the server's advertised address settings.</returns>
    /// <remarks>The client reads this itself on connect and routes commands by it; you rarely need to call it.</remarks>
    /// <example>
    /// <code>
    /// RespValue slots = await db.GetClusterSlotsAsync();
    /// </code>
    /// </example>
    ValueTask<RespValue> GetClusterSlotsAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>CLUSTER NODES</c>.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The node table as text, one line per shard. Every shard is a primary.</returns>
    /// <example>
    /// <code>
    /// string nodes = await db.GetClusterNodesAsync();
    /// </code>
    /// </example>
    ValueTask<string> GetClusterNodesAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>CLUSTER SHARDS</c>.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The raw reply describing each shard. Every shard is a primary.</returns>
    /// <example>
    /// <code>
    /// RespValue shards = await db.GetClusterShardsAsync();
    /// </code>
    /// </example>
    ValueTask<RespValue> GetClusterShardsAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>CLUSTER INFO</c>.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The state text; <c>cluster_state</c> is always <c>ok</c> and every slot is assigned.</returns>
    /// <example>
    /// <code>
    /// string info = await db.GetClusterInfoAsync();
    /// </code>
    /// </example>
    ValueTask<string> GetClusterInfoAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>CLUSTER MYID</c>.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The id of the shard that answered.</returns>
    /// <example>
    /// <code>
    /// string id = await db.GetClusterMyIdAsync();
    /// </code>
    /// </example>
    ValueTask<string> GetClusterMyIdAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>CLUSTER KEYSLOT</c>: asks the server which hash slot a key maps to. It also works on a single-shard server.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>A slot number from 0 to 16383. Only the text inside the first <c>{tag}</c> is hashed when the key has one.</returns>
    /// <remarks><see cref="RuvioClient.GetHashSlot"/> computes the same number locally without a round trip.</remarks>
    /// <example>
    /// <code>
    /// long slot = await db.GetClusterKeySlotAsync("user:{42}:profile");
    /// </code>
    /// </example>
    ValueTask<long> GetClusterKeySlotAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>READONLY</c>. Ruvio has no replicas to read from, so this changes nothing; the reply is <c>OK</c> in cluster mode.
    /// </summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <example>
    /// <code>
    /// await db.ReadOnlyAsync();
    /// </code>
    /// </example>
    ValueTask ReadOnlyAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>READWRITE</c>. Ruvio has no replicas, so this changes nothing; the reply is <c>OK</c> in cluster mode.
    /// </summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <example>
    /// <code>
    /// await db.ReadWriteAsync();
    /// </code>
    /// </example>
    ValueTask ReadWriteAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>SCRIPT LOAD</c>: compiles and caches a Lua script.</summary>
    /// <param name="script">The Lua source.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The script's SHA1 digest, for <see cref="EvaluateShaAsync"/>.</returns>
    /// <remarks>The cache is per shard and memory-only: after a server restart <c>EVALSHA</c> answers <c>NOSCRIPT</c>.</remarks>
    /// <example>
    /// <code>
    /// string sha = await db.LoadScriptAsync("return redis.call('GET', KEYS[1])");
    /// </code>
    /// </example>
    ValueTask<string> LoadScriptAsync(string script, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>EVALSHA</c>: runs a cached script by digest.</summary>
    /// <param name="sha">The digest returned by <see cref="LoadScriptAsync"/>.</param>
    /// <param name="keyCount">How many of <paramref name="arguments"/> are key names.</param>
    /// <param name="arguments">The key names first, then the other arguments.</param>
    /// <returns>The script's result as a <see cref="RespValue"/>.</returns>
    /// <exception cref="RuvioException"><c>NOSCRIPT</c> when the digest is not cached on that shard; script errors; <c>CROSSSLOT</c>.</exception>
    /// <example>
    /// <code>
    /// RespValue value = await db.EvaluateShaAsync(sha, 1, "greeting");
    /// </code>
    /// </example>
    ValueTask<RespValue> EvaluateShaAsync(string sha, long keyCount, params string[] arguments);

    /// <summary>Sends <c>SCRIPT EXISTS</c>.</summary>
    /// <param name="hashes">The digests to check.</param>
    /// <returns>One flag per digest: true when that script is cached.</returns>
    /// <example>
    /// <code>
    /// var present = await db.ScriptExistsAsync(sha);
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<bool>> ScriptExistsAsync(params string[] hashes);

    /// <summary>
    /// Sends <c>SCRIPT FLUSH</c>: empties the script cache. On a multi-shard server the client sends it to every shard.
    /// </summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <example>
    /// <code>
    /// await db.FlushScriptsAsync();
    /// </code>
    /// </example>
    ValueTask FlushScriptsAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>SCRIPT KILL</c>.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <exception cref="RuvioException">Always <c>NOTBUSY</c> in practice: Ruvio stops runaway scripts with an instruction quota and a time limit instead of letting you kill them.</exception>
    /// <example>
    /// <code>
    /// try { await db.KillScriptAsync(); } catch (RuvioException) { /* NOTBUSY */ }
    /// </code>
    /// </example>
    ValueTask KillScriptAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>FCALL</c>: runs a function stored with <see cref="LoadFunctionAsync"/>.</summary>
    /// <param name="name">The function name.</param>
    /// <param name="keyCount">How many of <paramref name="arguments"/> are key names.</param>
    /// <param name="arguments">The key names first, then the other arguments.</param>
    /// <returns>The function's result as a <see cref="RespValue"/>.</returns>
    /// <exception cref="RuvioException">The function does not exist; Lua errors; <c>CROSSSLOT</c>.</exception>
    /// <example>
    /// <code>
    /// RespValue result = await db.CallFunctionAsync("double", 0, "21");
    /// </code>
    /// </example>
    ValueTask<RespValue> CallFunctionAsync(string name, long keyCount, params string[] arguments);

    /// <summary>Sends <c>FUNCTION LOAD name body</c>: stores a named Lua function on the shard.</summary>
    /// <param name="name">The name to call it by.</param>
    /// <param name="body">Plain Lua, not a Redis library with a shebang line.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <exception cref="RuvioException">The body does not compile.</exception>
    /// <remarks>The function is kept in memory only and is lost when the server restarts.</remarks>
    /// <example>
    /// <code>
    /// await db.LoadFunctionAsync("double", "return tonumber(ARGV[1]) * 2");
    /// </code>
    /// </example>
    ValueTask LoadFunctionAsync(string name, string body, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>FUNCTION LIST</c>.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The raw reply: the loaded function names in byte order.</returns>
    /// <example>
    /// <code>
    /// RespValue functions = await db.GetFunctionListAsync();
    /// </code>
    /// </example>
    ValueTask<RespValue> GetFunctionListAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>FUNCTION DELETE</c>.</summary>
    /// <param name="name">The function to remove.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when removed; <see langword="false"/> when no function had that name.</returns>
    /// <example>
    /// <code>
    /// bool removed = await db.DeleteFunctionAsync("double");
    /// </code>
    /// </example>
    ValueTask<bool> DeleteFunctionAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>ACL USERS</c>.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The names of all users, including ones added with <see cref="SetAclUserAsync"/> since the last restart.</returns>
    /// <example>
    /// <code>
    /// var users = await db.GetAclUsersAsync();
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> GetAclUsersAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>CLIENT GETNAME</c>.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The name set with <see cref="SetConnectionNameAsync"/>, or <see langword="null"/>/empty when none.</returns>
    /// <example>
    /// <code>
    /// string? name = await db.GetConnectionNameAsync();
    /// </code>
    /// </example>
    ValueTask<string?> GetConnectionNameAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>CLIENT SETNAME</c>: labels this connection (visible in <c>CLIENT LIST</c>).</summary>
    /// <param name="name">The label; an empty string clears it. The name is not persistent.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <example>
    /// <code>
    /// await db.SetConnectionNameAsync("billing-worker");
    /// </code>
    /// </example>
    ValueTask SetConnectionNameAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>CLIENT TRACKING ON|OFF</c>.</summary>
    /// <param name="enabled">True to turn server-assisted invalidation on, false to turn it off.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <exception cref="RuvioException">Turning it on requires RESP3, which this client does not speak, so the server refuses; <c>OFF</c> is accepted.</exception>
    /// <remarks>Ruvio supports tracking with RESP3 connections only (no REDIRECT, BCAST, PREFIX, OPTIN or OPTOUT).</remarks>
    /// <example>
    /// <code>
    /// await db.SetTrackingAsync(false);
    /// </code>
    /// </example>
    ValueTask SetTrackingAsync(bool enabled, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>CLIENT DURABILITY REPLICAS n [TIMEOUT ms]</c>: holds this connection's replies until <paramref name="replicas"/> replicas have the writes on disk.</summary>
    /// <param name="replicas">How many replicas must have the writes; zero turns the wait off.</param>
    /// <param name="timeout">How long to wait before the replies become <c>NOREPLICAS</c> errors (the writes stay applied); null keeps the server's default of 5 seconds.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <exception cref="RuvioException">The server has no replication, or the connection is inside <c>MULTI</c>.</exception>
    /// <remarks>The setting belongs to the connection. <see cref="RuvioClientOptions.DurabilityReplicas"/> sets it on every connection the client opens.</remarks>
    /// <example>
    /// <code>
    /// await db.SetDurabilityAsync(1, TimeSpan.FromSeconds(5));
    /// </code>
    /// </example>
    ValueTask SetDurabilityAsync(int replicas, TimeSpan? timeout = null, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>CONFIG GET</c>.</summary>
    /// <param name="parameter">A setting name or glob, for example <c>databases</c> or <c>*</c>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The raw reply: a flat list of name, value pairs. <c>requirepass</c> is redacted.</returns>
    /// <example>
    /// <code>
    /// RespValue databases = await db.GetConfigAsync("databases");
    /// </code>
    /// </example>
    ValueTask<RespValue> GetConfigAsync(string parameter, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>CONFIG SET</c>: changes live settings on every shard of the server before it answers.</summary>
    /// <param name="settings">Setting names, as <c>CONFIG GET</c> shows them, and their new values.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <remarks>All pairs apply or none: an unknown or restart-only name, or an invalid value, throws and changes nothing. The change is not written to the configuration file (see <see cref="RewriteConfigAsync"/>) and is not replicated.</remarks>
    /// <example>
    /// <code>
    /// await db.SetConfigAsync([("maxmemory", "1073741824"), ("maxmemory-policy", "allkeys-lru")]);
    /// </code>
    /// </example>
    ValueTask SetConfigAsync(IReadOnlyList<(string Name, string Value)> settings, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>CONFIG REWRITE</c>: writes the live settings into the configuration file the server started from.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <remarks>Fails when the server runs without a configuration file.</remarks>
    /// <example>
    /// <code>
    /// await db.RewriteConfigAsync();
    /// </code>
    /// </example>
    ValueTask RewriteConfigAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>REPLICAOF host port</c>: every shard of the server follows the primary at <paramref name="host"/>:<paramref name="port"/> (shard <c>i</c> dials <c>port + i</c>).</summary>
    /// <param name="host">The primary's host.</param>
    /// <param name="port">The primary's base replication port.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <remarks>Writes are refused at once. A shard continues from its own log when the new primary's history allows it, otherwise it takes a full copy. Lasts until the server restarts.</remarks>
    /// <example>
    /// <code>
    /// await db.ReplicaOfAsync("10.0.0.5", 26379);
    /// </code>
    /// </example>
    ValueTask ReplicaOfAsync(string host, int port, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>REPLICAOF NO ONE</c>: promotes a replica in place, one epoch above its old primary.</summary>
    /// <param name="force">Adds <c>FORCE</c>: promote even when a shard has not finished copying its primary, which may lose writes.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <example>
    /// <code>
    /// await db.ReplicaOfNoOneAsync();
    /// </code>
    /// </example>
    ValueTask ReplicaOfNoOneAsync(bool force = false, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>ROLE</c> for the shard this connection is on.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The raw reply: <c>["master", sequence, []]</c> on a primary, <c>["slave", host, port, "connect"|"connected", sequence]</c> on a replica.</returns>
    /// <example>
    /// <code>
    /// RespValue role = await db.GetRoleAsync();
    /// </code>
    /// </example>
    ValueTask<RespValue> GetRoleAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>FLUSHDB</c>: deletes every key of the selected database.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <remarks>Only this client's database is emptied. In a multi-shard deployment (<c>port_mode = "per-shard"</c>) each shard holds its own keys: a discovering client sends the command to every shard, but a client or connection that talks to a single shard port clears only that shard. <c>ASYNC</c> runs synchronously on Ruvio. This cannot be undone.</remarks>
    /// <example>
    /// <code>
    /// await db.FlushDatabaseAsync();
    /// </code>
    /// </example>
    ValueTask FlushDatabaseAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>FLUSHALL</c>: deletes every key of every database.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <remarks>In a multi-shard deployment a discovering client sends it to every shard; a connection to a single shard port clears only that shard. This cannot be undone.</remarks>
    /// <example>
    /// <code>
    /// await db.FlushAllDatabasesAsync();
    /// </code>
    /// </example>
    ValueTask FlushAllDatabasesAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>SWAPDB</c>: exchanges the contents of two logical databases in constant time.</summary>
    /// <param name="first">Index of the first database (0 to <c>databases - 1</c>).</param>
    /// <param name="second">Index of the second database.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <exception cref="RuvioException">An index is out of range; refused on a multi-shard server (only database 0 exists).</exception>
    /// <remarks>Connections already blocked on a key do not wake because of the swap.</remarks>
    /// <example>
    /// <code>
    /// await db.SwapDatabasesAsync(1, 2);
    /// </code>
    /// </example>
    ValueTask SwapDatabasesAsync(int first, int second, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>MOVE</c>: moves a key to another logical database.</summary>
    /// <param name="key">The key.</param>
    /// <param name="database">The target database index.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when moved; <see langword="false"/> when the key is missing or already exists in the target.</returns>
    /// <exception cref="RuvioException">The target equals the current database or is out of range; refused on a multi-shard server.</exception>
    /// <example>
    /// <code>
    /// bool moved = await db.MoveKeyAsync("user:1", 4);
    /// </code>
    /// </example>
    ValueTask<bool> MoveKeyAsync(string key, int database, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>HELLO</c>: asks for server details and optionally a protocol version.</summary>
    /// <param name="protocol">Protocol version, 2 (default). This client reads only RESP2; do not request 3, because it could not read the replies.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The raw reply: server name, version, protocol and related fields. Connecting does not send this command.</returns>
    /// <exception cref="RuvioException">An unknown version (<c>NOPROTO</c>).</exception>
    /// <example>
    /// <code>
    /// RespValue hello = await db.HelloAsync();
    /// </code>
    /// </example>
    ValueTask<RespValue> HelloAsync(int protocol = 2, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>ACL LIST</c>.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>One rule line per user. Passwords are redacted.</returns>
    /// <example>
    /// <code>
    /// var rules = await db.GetAclListAsync();
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> GetAclListAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>ACL GETUSER</c>.</summary>
    /// <param name="user">The user name.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The raw reply describing the user's flags, command and key rules. Passwords are counted, not shown.</returns>
    /// <example>
    /// <code>
    /// RespValue info = await db.GetAclUserAsync("app");
    /// </code>
    /// </example>
    ValueTask<RespValue> GetAclUserAsync(string user, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>ACL CAT</c>.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The names of Ruvio's command categories.</returns>
    /// <example>
    /// <code>
    /// var categories = await db.GetAclCategoriesAsync();
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> GetAclCategoriesAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>ACL SETUSER</c>: creates a user or changes its rules.</summary>
    /// <param name="user">The user name.</param>
    /// <param name="rules">Rule words, applied in order: <c>on</c> / <c>off</c>, <c>&gt;password</c>, <c>nopass</c>, <c>resetpass</c>, <c>~pattern</c> or <c>allkeys</c>, <c>+command</c> / <c>-command</c>. A new user starts with no password, no commands and no keys.</param>
    /// <exception cref="RuvioException">A rule is invalid; the user is left unchanged.</exception>
    /// <remarks>Open connections keep the rules from their last <c>AUTH</c>. The change lasts until restart unless saved with <see cref="SaveAclAsync"/> (needs <c>security.acl_file</c>).</remarks>
    /// <example>
    /// <code>
    /// await db.SetAclUserAsync("app", "on", "&gt;secret", "~app:*", "+get", "+set");
    /// </code>
    /// </example>
    ValueTask SetAclUserAsync(string user, params string[] rules);

    /// <summary>Sends <c>ACL LOAD</c>: replaces the user directory with the contents of the ACL file.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <exception cref="RuvioException">No ACL file is configured, or a line is invalid (nothing changes).</exception>
    /// <example>
    /// <code>
    /// await db.LoadAclAsync();
    /// </code>
    /// </example>
    ValueTask LoadAclAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>ACL SAVE</c>: writes the current user directory, including passwords, to the ACL file.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <exception cref="RuvioException">No ACL file (<c>security.acl_file</c>) is configured.</exception>
    /// <example>
    /// <code>
    /// await db.SaveAclAsync();
    /// </code>
    /// </example>
    ValueTask SaveAclAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>SUBSCRIBE</c> on a dedicated subscriber socket (not the command socket).</summary>
    /// <param name="channels">The channel names; at least one.</param>
    /// <returns>One confirmation per channel. Messages arrive later through <see cref="Message"/>, <see cref="ListenAsync"/> or <see cref="ReadPubSubMessageAsync"/>.</returns>
    /// <exception cref="RuvioException">No channel given (<see cref="ArgumentException"/>).</exception>
    /// <remarks>A subscription lives on the shard the client connected to; a dropped subscriber socket is reopened and the channels are subscribed again when <see cref="RuvioClientOptions.Reconnect"/> is on. Messages sent while disconnected are lost.</remarks>
    /// <example>
    /// <code>
    /// await db.SubscribeAsync("news");
    /// db.Message += (_, m) =&gt; Console.WriteLine($"{m.Channel}: {m.Payload}");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<RespValue>> SubscribeAsync(params string[] channels);

    /// <summary>Subscribes to one channel and calls <paramref name="handler"/> for every later message on it.</summary>
    /// <param name="channel">The channel name.</param>
    /// <param name="handler">Called on the subscriber's reader thread; keep it short and do not block.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <example>
    /// <code>
    /// await db.SubscribeAsync("news", message =&gt; Console.WriteLine(message.Payload));
    /// </code>
    /// </example>
    ValueTask SubscribeAsync(string channel, Action<PubSubMessage> handler, CancellationToken cancellationToken = default);

    /// <summary>Subscribes to one channel and calls an asynchronous handler for every later message.</summary>
    /// <param name="channel">The channel name.</param>
    /// <param name="handler">Started for each message and not awaited, so a slow handler does not hold up delivery. An exception in it is not observed by the client: catch inside the handler.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <example>
    /// <code>
    /// await db.SubscribeAsync("news", async message =&gt; await HandleAsync(message));
    /// </code>
    /// </example>
    ValueTask SubscribeAsync(string channel, Func<PubSubMessage, Task> handler, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>PSUBSCRIBE</c> and calls <paramref name="handler"/> for every message whose channel matches the pattern (delivered as <c>pmessage</c>).
    /// </summary>
    /// <param name="pattern">A glob using <c>*</c> and <c>?</c>. Character classes are not supported.</param>
    /// <param name="handler">Called on the subscriber's reader thread; keep it short.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <remarks>Keyspace notices are not delivered to patterns; subscribe to the exact channel.</remarks>
    /// <example>
    /// <code>
    /// await db.SubscribePatternsAsync("orders:*", m =&gt; Console.WriteLine($"{m.Channel}: {m.Payload}"));
    /// </code>
    /// </example>
    ValueTask SubscribePatternsAsync(string pattern, Action<PubSubMessage> handler, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>PSUBSCRIBE</c> and calls an asynchronous handler for every matching message.</summary>
    /// <param name="pattern">A glob using <c>*</c> and <c>?</c>.</param>
    /// <param name="handler">Started for each message and not awaited; catch exceptions inside it.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <example>
    /// <code>
    /// await db.SubscribePatternsAsync("orders:*", async m =&gt; await HandleAsync(m));
    /// </code>
    /// </example>
    ValueTask SubscribePatternsAsync(string pattern, Func<PubSubMessage, Task> handler, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>SSUBSCRIBE</c> and calls <paramref name="handler"/> for every message published with <see cref="PublishShardedAsync"/> (delivered as <c>smessage</c>).
    /// </summary>
    /// <param name="channel">The sharded channel; it is hashed like a key, so <c>{tag}</c> works.</param>
    /// <param name="handler">Called on the subscriber's reader thread; keep it short.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <remarks>Sharded subscriptions go to the shard that owns the channel's slot. Pattern subscriptions are not available for sharded channels.</remarks>
    /// <example>
    /// <code>
    /// await db.SubscribeShardedAsync("room:{42}", m =&gt; Console.WriteLine(m.Payload));
    /// </code>
    /// </example>
    ValueTask SubscribeShardedAsync(string channel, Action<PubSubMessage> handler, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>SSUBSCRIBE</c> and calls an asynchronous handler for every sharded message.</summary>
    /// <param name="channel">The sharded channel.</param>
    /// <param name="handler">Started for each message and not awaited; catch exceptions inside it.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <example>
    /// <code>
    /// await db.SubscribeShardedAsync("room:{42}", async m =&gt; await HandleAsync(m));
    /// </code>
    /// </example>
    ValueTask SubscribeShardedAsync(string channel, Func<PubSubMessage, Task> handler, CancellationToken cancellationToken = default);

    /// <summary>
    /// Calls <paramref name="handler"/> when a key of the selected database expires. The expired key's name is in <see cref="PubSubMessage.Payload"/>.
    /// </summary>
    /// <param name="handler">Called on the subscriber's reader thread; keep it short.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <remarks>Subscribes with <c>SSUBSCRIBE __keyevent@&lt;db&gt;__:expired</c>. The server must be started with keyspace events enabled (<c>notify_keyspace_events = "Ex"</c> or similar, read at startup). Notices are live and best-effort, and on a multi-shard server each shard publishes only its own keys.</remarks>
    /// <example>
    /// <code>
    /// await db.SubscribeExpiredAsync(m =&gt; Console.WriteLine($"expired: {m.Payload}"));
    /// </code>
    /// </example>
    ValueTask SubscribeExpiredAsync(
        Action<PubSubMessage> handler,
        CancellationToken cancellationToken = default) =>
        SubscribeShardedAsync($"__keyevent@{Database}__:expired", handler, cancellationToken);

    /// <summary>
    /// Asynchronous-handler form of <see cref="SubscribeExpiredAsync(Action{PubSubMessage}, CancellationToken)"/>.
    /// </summary>
    /// <param name="handler">Started for each notice and not awaited; catch exceptions inside it.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <example>
    /// <code>
    /// await db.SubscribeExpiredAsync(async m =&gt; await OnExpiredAsync(m.Payload));
    /// </code>
    /// </example>
    ValueTask SubscribeExpiredAsync(
        Func<PubSubMessage, Task> handler,
        CancellationToken cancellationToken = default) =>
        SubscribeShardedAsync($"__keyevent@{Database}__:expired", handler, cancellationToken);

    /// <summary>
    /// Calls <paramref name="handler"/> when a key of the selected database is evicted to free memory. The key's name is in <see cref="PubSubMessage.Payload"/>.
    /// </summary>
    /// <param name="handler">Called on the subscriber's reader thread; keep it short.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <remarks>Needs keyspace events enabled with the <c>e</c> class (<c>notify_keyspace_events = "Ee"</c>) and an eviction policy with a memory budget; the default <c>noeviction</c> never evicts. Best-effort, shard-local.</remarks>
    /// <example>
    /// <code>
    /// await db.SubscribeEvictedAsync(m =&gt; Console.WriteLine($"evicted: {m.Payload}"));
    /// </code>
    /// </example>
    ValueTask SubscribeEvictedAsync(
        Action<PubSubMessage> handler,
        CancellationToken cancellationToken = default) =>
        SubscribeShardedAsync($"__keyevent@{Database}__:evicted", handler, cancellationToken);

    /// <summary>
    /// Asynchronous-handler form of <see cref="SubscribeEvictedAsync(Action{PubSubMessage}, CancellationToken)"/>.
    /// </summary>
    /// <param name="handler">Started for each notice and not awaited; catch exceptions inside it.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <example>
    /// <code>
    /// await db.SubscribeEvictedAsync(async m =&gt; await OnEvictedAsync(m.Payload));
    /// </code>
    /// </example>
    ValueTask SubscribeEvictedAsync(
        Func<PubSubMessage, Task> handler,
        CancellationToken cancellationToken = default) =>
        SubscribeShardedAsync($"__keyevent@{Database}__:evicted", handler, cancellationToken);

    /// <summary>
    /// Streams Pub/Sub messages as an <see cref="IAsyncEnumerable{T}"/>. Opens the subscriber socket if needed.
    /// </summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>Every <c>message</c>, <c>pmessage</c> and <c>smessage</c> received on this client's subscriptions, until cancelled. Subscribe first.</returns>
    /// <example>
    /// <code>
    /// await db.SubscribeAsync("news");
    /// await foreach (PubSubMessage m in db.ListenAsync(token))
    ///     Console.WriteLine(m.Payload);
    /// </code>
    /// </example>
    IAsyncEnumerable<PubSubMessage> ListenAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>UNSUBSCRIBE</c> and drops the handlers registered for those channels.</summary>
    /// <param name="channels">The channels to leave. With none, every exact (non-pattern) subscription on this client is dropped.</param>
    /// <returns>One confirmation per channel.</returns>
    /// <example>
    /// <code>
    /// await db.UnsubscribeAsync("news");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<RespValue>> UnsubscribeAsync(params string[] channels);

    /// <summary>Sends <c>PSUBSCRIBE</c> on the dedicated subscriber socket.</summary>
    /// <param name="patterns">The glob patterns (<c>*</c> and <c>?</c>); at least one.</param>
    /// <returns>One confirmation per pattern. Matches arrive as <c>pmessage</c> through <see cref="Message"/>, <see cref="ListenAsync"/> or <see cref="ReadPubSubMessageAsync"/>.</returns>
    /// <exception cref="RuvioException">No pattern given (<see cref="ArgumentException"/>).</exception>
    /// <example>
    /// <code>
    /// await db.SubscribePatternsAsync("orders:*");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<RespValue>> SubscribePatternsAsync(params string[] patterns);

    /// <summary>Sends <c>PUNSUBSCRIBE</c> and drops the handlers registered for those patterns.</summary>
    /// <param name="patterns">The patterns to leave. With none, every pattern subscription on this client is dropped.</param>
    /// <returns>One confirmation per pattern.</returns>
    /// <example>
    /// <code>
    /// await db.UnsubscribePatternsAsync("orders:*");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<RespValue>> UnsubscribePatternsAsync(params string[] patterns);

    /// <summary>Sends <c>SSUBSCRIBE</c> on the dedicated subscriber socket.</summary>
    /// <param name="channels">The sharded channels; at least one. Each goes to the shard that owns its slot.</param>
    /// <returns>One confirmation per channel. Messages arrive as <c>smessage</c>.</returns>
    /// <exception cref="RuvioException">No channel given (<see cref="ArgumentException"/>); on a wrong shard the server redirects (the client follows the routing table).</exception>
    /// <example>
    /// <code>
    /// await db.SubscribeShardedAsync("room:{42}");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<RespValue>> SubscribeShardedAsync(params string[] channels);

    /// <summary>Sends <c>SUNSUBSCRIBE</c> and drops the handlers registered for those channels.</summary>
    /// <param name="channels">The channels to leave. With none, every sharded subscription is dropped.</param>
    /// <returns>One confirmation per channel.</returns>
    /// <example>
    /// <code>
    /// await db.UnsubscribeShardedAsync("room:{42}");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<RespValue>> UnsubscribeShardedAsync(params string[] channels);

    /// <summary>Reads the next raw RESP value from the subscriber socket without sending a command.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>A <see cref="RespValue"/> array shaped like <c>["message", channel, payload]</c> (or <c>pmessage</c> / <c>smessage</c>). Note <see cref="RespValue.AsString"/> on that array is null.</returns>
    /// <remarks>Prefer <see cref="ReadPubSubMessageAsync"/> or <see cref="ListenAsync"/>.</remarks>
    /// <example>
    /// <code>
    /// RespValue push = await db.ReadMessageAsync(token);
    /// </code>
    /// </example>
    ValueTask<RespValue> ReadMessageAsync(CancellationToken cancellationToken = default);

    /// <summary>Waits for the next Pub/Sub delivery, skipping subscribe confirmations.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The message: its kind, pattern (for <c>pmessage</c>), channel and payload.</returns>
    /// <example>
    /// <code>
    /// await db.SubscribeAsync("news");
    /// PubSubMessage m = await db.ReadPubSubMessageAsync(token);
    /// </code>
    /// </example>
    ValueTask<PubSubMessage> ReadPubSubMessageAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>KEYS pattern</c>: lists the keys that match a glob pattern.</summary>
    /// <param name="pattern">A glob: <c>*</c>, <c>?</c>, <c>[abc]</c>, <c>[^a-z]</c> and <c>\</c> to escape.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>Matching keys in no defined order. Empty when none match.</returns>
    /// <exception cref="RuvioException">A shard holds more than 100,000 keys; use <see cref="ScanAsync(long, string, int?, CancellationToken)"/>.</exception>
    /// <remarks>Walks the whole database in one call. In a cluster the call goes to every shard and the lists are joined.</remarks>
    /// <example>
    /// <code>
    /// IReadOnlyList&lt;string&gt; keys = await db.GetKeysAsync("session:*");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> GetKeysAsync(string pattern, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>RANDOMKEY</c>.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>A pseudo-random key, or <see langword="null"/> when the database is empty.</returns>
    /// <remarks>Not uniform. With several shards the first non-empty answer is used.</remarks>
    /// <example>
    /// <code>
    /// string? any = await db.GetRandomKeyAsync();
    /// </code>
    /// </example>
    ValueTask<string?> GetRandomKeyAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>TOUCH</c>: counts how many of the keys exist.</summary>
    /// <param name="keys">The keys to check.</param>
    /// <returns>How many exist.</returns>
    /// <remarks>Does not update the access time used by eviction. In a cluster the keys are split by shard and the counts added; the split is not atomic.</remarks>
    /// <example>
    /// <code>
    /// long present = await db.TouchAsync("a", "b", "c");
    /// </code>
    /// </example>
    ValueTask<long> TouchAsync(params string[] keys);

    /// <summary>Sends <c>RENAMENX key newKey</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="newKey">The new name.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when renamed; <see langword="false"/> when <paramref name="newKey"/> already exists.</returns>
    /// <exception cref="RuvioException"><paramref name="key"/> does not exist.</exception>
    /// <remarks>Both keys must share a hash slot in a cluster; otherwise the client throws before sending (<c>CROSSSLOT</c>).</remarks>
    /// <example>
    /// <code>
    /// bool moved = await db.RenameIfNotExistsAsync("tmp:1", "final:1");
    /// </code>
    /// </example>
    ValueTask<bool> RenameIfNotExistsAsync(string key, string newKey, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>EXPIRETIME</c>: the absolute expiry as Unix time in seconds.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>Unix seconds; <c>-1</c> when the key has no expiry; <c>-2</c> when it does not exist.</returns>
    /// <example>
    /// <code>
    /// long at = await db.GetExpireTimeAsync("session:1");
    /// </code>
    /// </example>
    ValueTask<long> GetExpireTimeAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>PEXPIRETIME</c>: the absolute expiry as Unix time in milliseconds.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>Unix milliseconds; <c>-1</c> when the key has no expiry; <c>-2</c> when it does not exist.</returns>
    /// <example>
    /// <code>
    /// long at = await db.GetExpireTimeMillisecondsAsync("session:1");
    /// </code>
    /// </example>
    ValueTask<long> GetExpireTimeMillisecondsAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>COPY source destination [REPLACE]</c>.</summary>
    /// <param name="source">The source key.</param>
    /// <param name="destination">The key that receives the result.</param>
    /// <param name="replace">When true, an existing destination is overwritten. When false the copy is refused if it exists.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when copied; <see langword="false"/> when the source is missing or the destination exists without <paramref name="replace"/>.</returns>
    /// <exception cref="RuvioException">The type is not copyable: streams, JSON, Bloom filters and hashes with field TTLs are refused.</exception>
    /// <remarks>Copies strings, lists, sets, hashes and sorted sets with their TTL as one atomic write, costing O(N). Both keys must share a hash slot in a cluster.</remarks>
    /// <example>
    /// <code>
    /// bool ok = await db.CopyAsync("cart:1", "cart:1:backup", replace: true);
    /// </code>
    /// </example>
    ValueTask<bool> CopyAsync(string source, string destination, bool replace = false, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>ECHO message</c>.</summary>
    /// <param name="message">The text to echo.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The same text.</returns>
    /// <example>
    /// <code>
    /// string text = await db.EchoAsync("hi");
    /// </code>
    /// </example>
    ValueTask<string> EchoAsync(string message, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>OBJECT ENCODING</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The encoding name Redis 7.0 would report (<c>int</c>, <c>embstr</c>, <c>raw</c>, <c>quicklist</c>, <c>intset</c>, <c>hashtable</c>, <c>listpack</c>, <c>skiplist</c>, <c>stream</c>), or <see langword="null"/> when the key does not exist.</returns>
    /// <remarks>This is a compatibility name, not the physical layout.</remarks>
    /// <example>
    /// <code>
    /// string? enc = await db.GetObjectEncodingAsync("user:1");
    /// </code>
    /// </example>
    ValueTask<string?> GetObjectEncodingAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>OBJECT IDLETIME</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>Seconds since the key was last accessed, or <see langword="null"/> when it does not exist.</returns>
    /// <exception cref="RuvioException">The server's <c>maxmemory-policy</c> is not an LRU policy.</exception>
    /// <example>
    /// <code>
    /// long? idle = await db.GetObjectIdleSecondsAsync("user:1");
    /// </code>
    /// </example>
    ValueTask<long?> GetObjectIdleSecondsAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>OBJECT FREQ</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The access frequency counter, or <see langword="null"/> when the key does not exist.</returns>
    /// <exception cref="RuvioException">The server's <c>maxmemory-policy</c> is not <c>allkeys-lfu</c>.</exception>
    /// <example>
    /// <code>
    /// long? freq = await db.GetObjectFrequencyAsync("user:1");
    /// </code>
    /// </example>
    ValueTask<long?> GetObjectFrequencyAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>MEMORY USAGE</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The bytes the key uses: the key, value and collection storage the memory budget charges, plus fixed entry overhead. <see langword="null"/> when the key does not exist.</returns>
    /// <remarks>The size is exact for Ruvio but not comparable byte for byte with Redis.</remarks>
    /// <example>
    /// <code>
    /// long? bytes = await db.GetMemoryUsageAsync("big:list");
    /// </code>
    /// </example>
    ValueTask<long?> GetMemoryUsageAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>SLOWLOG LEN</c>.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The number of entries in the slow log of the shard that answered.</returns>
    /// <remarks>Per shard. Only commands that take the generic dispatcher are timed (not GET, SET, INCR, PING on the fast path); blocking commands are skipped. The threshold and length are set in the config file or environment, not with <c>CONFIG SET</c>.</remarks>
    /// <example>
    /// <code>
    /// long n = await db.GetSlowLogLengthAsync();
    /// </code>
    /// </example>
    ValueTask<long> GetSlowLogLengthAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>SLOWLOG RESET</c>: empties the slow log of the shard that answered.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <example>
    /// <code>
    /// await db.ResetSlowLogAsync();
    /// </code>
    /// </example>
    ValueTask ResetSlowLogAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>SLOWLOG GET count</c>.</summary>
    /// <param name="count">How many of the newest entries to return. Default 10.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The entries as raw reply items. Each holds the command name and its first key (cut to 128 bytes), not every argument.</returns>
    /// <remarks>Per shard.</remarks>
    /// <example>
    /// <code>
    /// var entries = await db.GetSlowLogAsync(5);
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<RespValue>> GetSlowLogAsync(long count = 10, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>CLIENT ID</c>.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The id of this client's connection.</returns>
    /// <remarks>Ids are per shard: with several shards two connections can share an id.</remarks>
    /// <example>
    /// <code>
    /// long id = await db.GetClientIdAsync();
    /// </code>
    /// </example>
    ValueTask<long> GetClientIdAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>CLIENT INFO</c>.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>This connection's <c>CLIENT LIST</c> line: <c>id</c>, <c>addr</c>, <c>laddr</c>, <c>name</c>, <c>age</c>, <c>db</c>, <c>multi</c>, <c>resp</c>, <c>user</c>.</returns>
    /// <remarks>Reports the shard that answered.</remarks>
    /// <example>
    /// <code>
    /// string line = await db.GetClientInfoAsync();
    /// </code>
    /// </example>
    ValueTask<string> GetClientInfoAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>CLIENT LIST</c>.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>One line per connection with <c>id</c>, <c>addr</c>, <c>laddr</c>, <c>name</c>, <c>age</c>, <c>db</c>, <c>multi</c>, <c>resp</c> and <c>user</c>.</returns>
    /// <remarks>Each shard knows only its own connections; with several shards the reply covers the shard that got the command, and ids repeat across shards. <c>idle</c> is always 0.</remarks>
    /// <example>
    /// <code>
    /// string clients = await db.GetClientListAsync();
    /// </code>
    /// </example>
    ValueTask<string> GetClientListAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends <c>CLIENT LIST TYPE type</c>: the connections of one kind.</summary>
    /// <param name="type">The kind of connection: normal, master, replica or pubsub.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>One line per matching connection, in the format of <see cref="GetClientListAsync(CancellationToken)"/>.</returns>
    /// <example>
    /// <code>
    /// string normal = await db.GetClientListAsync(ClientType.Normal);
    /// </code>
    /// </example>
    ValueTask<string> GetClientListAsync(ClientType type, CancellationToken cancellationToken = default)
        => RespRead.RequiredText(ExecuteAsync(new string[] { "CLIENT", "LIST", "TYPE", Keywords.Word(type) }, cancellationToken), "CLIENT LIST");

    /// <summary>Sends <c>CLIENT KILL ID id</c>.</summary>
    /// <param name="id">The connection id from <see cref="GetClientIdAsync"/> or <see cref="GetClientListAsync(CancellationToken)"/>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The number of connections closed (0 or 1).</returns>
    /// <remarks>Reaches only the shard that got the command.</remarks>
    /// <example>
    /// <code>
    /// long closed = await db.KillClientAsync(42);
    /// </code>
    /// </example>
    ValueTask<long> KillClientAsync(long id, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>LINSERT key BEFORE|AFTER pivot value</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="before">True inserts before the pivot; false inserts after it.</param>
    /// <param name="pivot">An existing element to find; the first match is used.</param>
    /// <param name="value">The element to insert.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The new list length, <c>-1</c> when the pivot is not found, or 0 when the key does not exist.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Rewrites the list (O(N)) as one atomic write.</remarks>
    /// <example>
    /// <code>
    /// long len = await db.InsertListAsync("queue", before: true, "b", "a");
    /// </code>
    /// </example>
    ValueTask<long> InsertListAsync(string key, bool before, string pivot, string value, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>LINSERT key BEFORE|AFTER pivot value</c> with a typed position.</summary>
    /// <param name="key">The key.</param>
    /// <param name="position"><see cref="InsertPosition.Before"/> or <see cref="InsertPosition.After"/> the pivot.</param>
    /// <param name="pivot">An existing element to find; the first match is used.</param>
    /// <param name="value">The element to insert.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The new list length, <c>-1</c> when the pivot is not found, or 0 when the key does not exist.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Rewrites the list (O(N)) as one atomic write.</remarks>
    /// <example>
    /// <code>
    /// long len = await db.InsertListAsync("queue", InsertPosition.After, "b", "c");
    /// </code>
    /// </example>
    ValueTask<long> InsertListAsync(string key, InsertPosition position, string pivot, string value, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(new string[] { "LINSERT", key, Keywords.Word(position), pivot, value }, cancellationToken), "LINSERT");

    /// <summary>Sends <c>LSET key index value</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="index">Position to replace. Negative counts from the end.</param>
    /// <param name="value">The value to store.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <exception cref="RuvioException">The key does not exist, or the index is outside the list.</exception>
    /// <remarks>Rewrites the list (O(N)) as one atomic write.</remarks>
    /// <example>
    /// <code>
    /// await db.SetListItemAsync("queue", 0, "new head");
    /// </code>
    /// </example>
    ValueTask SetListItemAsync(string key, long index, string value, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>LREM key count value</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="count">How many matches to remove. Positive scans from the head, negative from the tail, 0 removes all.</param>
    /// <param name="value">The element to remove.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The number removed (0 when the key is missing).</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Rewrites the list (O(N)) as one atomic write.</remarks>
    /// <example>
    /// <code>
    /// long removed = await db.RemoveListItemsAsync("queue", 0, "dup");
    /// </code>
    /// </example>
    ValueTask<long> RemoveListItemsAsync(string key, long count, string value, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>LPOS key value</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The element to find.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The zero-based index of the first match, or <see langword="null"/> when absent or the key is missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Read-only, O(N).</remarks>
    /// <example>
    /// <code>
    /// long? at = await db.GetListPositionAsync("queue", "job-7");
    /// </code>
    /// </example>
    ValueTask<long?> GetListPositionAsync(string key, string value, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>LPUSHX</c>: pushes to the head only when the list already exists.</summary>
    /// <param name="key">The key.</param>
    /// <param name="values">The elements, pushed one after another.</param>
    /// <returns>The new length, or 0 when the key does not exist (nothing is created).</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long len = await db.PushListLeftIfExistsAsync("queue", "a");
    /// </code>
    /// </example>
    ValueTask<long> PushListLeftIfExistsAsync(string key, params string[] values);

    /// <summary>Sends <c>RPUSHX</c>: pushes to the tail only when the list already exists.</summary>
    /// <param name="key">The key.</param>
    /// <param name="values">The elements, pushed in order.</param>
    /// <returns>The new length, or 0 when the key does not exist (nothing is created).</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long len = await db.PushListRightIfExistsAsync("queue", "a");
    /// </code>
    /// </example>
    ValueTask<long> PushListRightIfExistsAsync(string key, params string[] values);

    /// <summary>
    /// Sends <c>LMOVE source destination LEFT|RIGHT LEFT|RIGHT</c>: atomically pops one element and pushes it to another list.
    /// </summary>
    /// <param name="source">The source key.</param>
    /// <param name="destination">The key that receives the result.</param>
    /// <param name="fromLeft">True pops from the head of the source; false from the tail.</param>
    /// <param name="toLeft">True pushes to the head of the destination; false to the tail.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The moved element, or <see langword="null"/> when the source is empty or missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Both keys must share a hash slot in a cluster.</remarks>
    /// <example>
    /// <code>
    /// string? job = await db.MoveListItemAsync("todo", "doing", fromLeft: true, toLeft: false);
    /// </code>
    /// </example>
    ValueTask<string?> MoveListItemAsync(string source, string destination, bool fromLeft, bool toLeft, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>LMOVE source destination LEFT|RIGHT LEFT|RIGHT</c> with typed sides: atomically pops one element and pushes it to another list.
    /// </summary>
    /// <param name="source">The source key.</param>
    /// <param name="destination">The key that receives the result.</param>
    /// <param name="from">The end of the source to pop from, a <see cref="ListSide"/>.</param>
    /// <param name="to">The end of the destination to push to, a <see cref="ListSide"/>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The moved element, or <see langword="null"/> when the source is empty or missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Both keys must share a hash slot in a cluster.</remarks>
    /// <example>
    /// <code>
    /// string? job = await db.MoveListItemAsync("todo", "doing", ListSide.Left, ListSide.Right);
    /// </code>
    /// </example>
    ValueTask<string?> MoveListItemAsync(string source, string destination, ListSide from, ListSide to, CancellationToken cancellationToken = default)
        => RespRead.Text(ExecuteAsync(new string[] { "LMOVE", source, destination, Keywords.Word(from), Keywords.Word(to) }, cancellationToken));

    /// <summary>
    /// Sends <c>RPOPLPUSH source destination</c>: pops the tail of one list and pushes it to the head of another.
    /// </summary>
    /// <param name="source">The source key.</param>
    /// <param name="destination">The key that receives the result.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The moved element, or <see langword="null"/> when the source is empty or missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Both keys must share a hash slot in a cluster. Prefer <see cref="MoveListItemAsync(string, string, bool, bool, CancellationToken)"/>.</remarks>
    /// <example>
    /// <code>
    /// string? job = await db.PopListRightPushLeftAsync("todo", "doing");
    /// </code>
    /// </example>
    ValueTask<string?> PopListRightPushLeftAsync(string source, string destination, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>BLMOVE</c>: like <see cref="MoveListItemAsync(string, string, bool, bool, CancellationToken)"/> but waits for an element when the source is empty.
    /// </summary>
    /// <param name="source">The source key.</param>
    /// <param name="destination">The key that receives the result.</param>
    /// <param name="fromLeft">True pops from the head of the source; false from the tail.</param>
    /// <param name="toLeft">True pushes to the head of the destination; false to the tail.</param>
    /// <param name="timeout">How long to wait, rounded up to whole seconds. <see cref="TimeSpan.Zero"/> waits forever.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The moved element, or <see langword="null"/> when the timeout passes.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Holds the shared connection for the whole wait, so other calls on this client queue behind it. The server retries every 5 ms and allows at most 1,024 blocked clients per shard. Not allowed inside <c>MULTI</c> or scripts. Both keys must share a hash slot in a cluster.</remarks>
    /// <example>
    /// <code>
    /// string? job = await db.MoveListItemBlockingAsync("todo", "doing", true, false, TimeSpan.FromSeconds(5));
    /// </code>
    /// </example>
    ValueTask<string?> MoveListItemBlockingAsync(string source, string destination, bool fromLeft, bool toLeft, TimeSpan timeout, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>BLMOVE</c> with typed sides: like the non-blocking move but waits for an element when the source is empty.
    /// </summary>
    /// <param name="source">The source key.</param>
    /// <param name="destination">The key that receives the result.</param>
    /// <param name="from">The end of the source to pop from, a <see cref="ListSide"/>.</param>
    /// <param name="to">The end of the destination to push to, a <see cref="ListSide"/>.</param>
    /// <param name="timeout">How long to wait, rounded up to whole seconds. <see cref="TimeSpan.Zero"/> waits forever.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The moved element, or <see langword="null"/> when the timeout passes.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Holds the shared connection for the whole wait, so other calls on this client queue behind it. The server retries every 5 ms and allows at most 1,024 blocked clients per shard. Not allowed inside <c>MULTI</c> or scripts. Both keys must share a hash slot in a cluster.</remarks>
    /// <example>
    /// <code>
    /// string? job = await db.MoveListItemBlockingAsync("todo", "doing", ListSide.Left, ListSide.Right, TimeSpan.FromSeconds(5));
    /// </code>
    /// </example>
    ValueTask<string?> MoveListItemBlockingAsync(string source, string destination, ListSide from, ListSide to, TimeSpan timeout, CancellationToken cancellationToken = default)
        => RespRead.Text(ExecuteAsync(new string[] { "BLMOVE", source, destination, Keywords.Word(from), Keywords.Word(to), RespRead.Number((long)Math.Ceiling(timeout.TotalSeconds)) }, cancellationToken));

    /// <summary>Sends <c>LMPOP numkeys key... LEFT|RIGHT COUNT count</c>: pops from the first non-empty list.</summary>
    /// <param name="keys">The lists to try, in order.</param>
    /// <param name="fromLeft">True pops from the head; false from the tail.</param>
    /// <param name="count">The most elements to pop. Default 1.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The key that was popped and its elements, or <see langword="null"/> when every list is empty.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>In a cluster all keys must share a hash slot.</remarks>
    /// <example>
    /// <code>
    /// var popped = await db.PopListManyAsync(new[] { "q:high", "q:low" }, fromLeft: true, count: 10);
    /// </code>
    /// </example>
    ValueTask<KeyValuePair<string, IReadOnlyList<string>>?> PopListManyAsync(IReadOnlyList<string> keys, bool fromLeft, long count = 1, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>LMPOP numkeys key... LEFT|RIGHT COUNT count</c> with a typed side: pops from the first non-empty list.
    /// </summary>
    /// <param name="keys">The lists to try, in order.</param>
    /// <param name="side">The end to pop from, a <see cref="ListSide"/>.</param>
    /// <param name="count">The most elements to pop. Default 1.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The key that was popped and its elements, or <see langword="null"/> when every list is empty.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>In a cluster all keys must share a hash slot.</remarks>
    /// <example>
    /// <code>
    /// var popped = await db.PopListManyAsync(new[] { "q:high", "q:low" }, ListSide.Left, 10);
    /// </code>
    /// </example>
    ValueTask<KeyValuePair<string, IReadOnlyList<string>>?> PopListManyAsync(IReadOnlyList<string> keys, ListSide side, long count = 1, CancellationToken cancellationToken = default)
        => RespRead.ListPop(ExecuteAsync(PopListManyCommand(keys, side, count), cancellationToken), "LMPOP");

    private static string[] PopListManyCommand(IReadOnlyList<string> keys, ListSide side, long count)
    {
        ArgumentNullException.ThrowIfNull(keys);

        var parts = new string[keys.Count + 5];

        parts[0] = "LMPOP";
        parts[1] = RespRead.Number(keys.Count);

        for (int index = 0; index < keys.Count; index++)
        {
            parts[index + 2] = keys[index];
        }

        parts[^3] = Keywords.Word(side);
        parts[^2] = "COUNT";
        parts[^1] = RespRead.Number(count);

        return parts;
    }

    /// <summary>Sends <c>BITFIELD key operation...</c>: reads and writes integer fields packed inside a string.</summary>
    /// <param name="key">The key.</param>
    /// <param name="operations">The sub-commands as words, in order: <c>GET type offset</c>, <c>SET type offset value</c>, <c>INCRBY type offset delta</c> and <c>OVERFLOW WRAP|SAT|FAIL</c>. Pass each word as its own string, for example <c>"SET", "u8", "0", "255"</c>.</param>
    /// <returns>One entry per GET, SET and INCRBY: the number read, the old value (SET) or the new value (INCRBY). An entry is <see langword="null"/> when OVERFLOW FAIL refused it.</returns>
    /// <exception cref="RuvioException">A type or offset is invalid, or a field would end beyond bit 134,217,728 (a 16 MiB bitmap).</exception>
    /// <remarks>Types are <c>i1</c> to <c>i64</c> and <c>u1</c> to <c>u63</c>; an offset is a bit number or <c>#n</c> for field number n. A write rewrites the whole value (O(size)) and keeps the TTL.</remarks>
    /// <example>
    /// <code>
    /// var r = await db.BitFieldAsync("counters", "INCRBY", "u8", "0", "1", "GET", "u8", "0");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<long?>> BitFieldAsync(string key, params string[] operations);

    /// <summary>
    /// Sends <c>BITFIELD_RO key operation...</c>: the read-only form of <see cref="BitFieldAsync(string, string[])"/>.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="operations">Only <c>GET type offset</c> triples, each word as its own string.</param>
    /// <returns>One number per GET.</returns>
    /// <exception cref="RuvioException">A write sub-command is given, or a type or offset is invalid.</exception>
    /// <remarks>Same types and limits as <see cref="BitFieldAsync(string, string[])"/>.</remarks>
    /// <example>
    /// <code>
    /// var r = await db.BitFieldReadOnlyAsync("counters", "GET", "u8", "0");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<long?>> BitFieldReadOnlyAsync(string key, params string[] operations);

    /// <summary>
    /// Sends <c>BITFIELD key step...</c> from typed steps: reads and writes integer fields packed inside a string.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="first">The first step. At least one step is required.</param>
    /// <param name="rest">More steps, run in order. Build steps with <see cref="BitFieldStep"/> (<c>Get</c>, <c>Set</c>, <c>IncrBy</c>, <c>Overflow</c>) and field types with <see cref="BitFieldType"/>.</param>
    /// <returns>One entry per <c>Get</c>, <c>Set</c> and <c>IncrBy</c> step: the number read, the old value (<c>Set</c>) or the new value (<c>IncrBy</c>). An entry is <see langword="null"/> when <see cref="BitFieldOverflow.Fail"/> refused it. <c>Overflow</c> steps give no entry.</returns>
    /// <exception cref="RuvioException">A field would end beyond bit 134,217,728 (a 16 MiB bitmap); <c>WRONGTYPE</c> for another type.</exception>
    /// <remarks>Types are <c>i1</c> to <c>i64</c> and <c>u1</c> to <c>u63</c>. A write rewrites the whole value (O(size)) and keeps the TTL.</remarks>
    /// <example>
    /// <code>
    /// var r = await db.BitFieldAsync("counters", BitFieldStep.IncrBy(BitFieldType.Unsigned(8), 0, 1), BitFieldStep.Get(BitFieldType.Unsigned(8), 0));
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<long?>> BitFieldAsync(string key, BitFieldStep first, params BitFieldStep[] rest)
        => RespRead.BitFieldResults(ExecuteAsync(BitFieldStep.Command("BITFIELD", key, first, rest), CancellationToken.None), "BITFIELD");

    /// <summary>Sends <c>BITFIELD_RO key step...</c> from typed steps: the read-only form of <c>BITFIELD</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="first">The first step. It must be a <see cref="BitFieldStep.Get"/> step.</param>
    /// <param name="rest">More <c>Get</c> steps, run in order.</param>
    /// <returns>One number per <c>Get</c> step.</returns>
    /// <exception cref="RuvioException">A step other than <c>Get</c> is given.</exception>
    /// <remarks>Same types and limits as <c>BitFieldAsync</c>.</remarks>
    /// <example>
    /// <code>
    /// var r = await db.BitFieldReadOnlyAsync("counters", BitFieldStep.Get(BitFieldType.Unsigned(8), 0));
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<long?>> BitFieldReadOnlyAsync(string key, BitFieldStep first, params BitFieldStep[] rest)
        => RespRead.BitFieldResults(ExecuteAsync(BitFieldStep.Command("BITFIELD_RO", key, first, rest), CancellationToken.None), "BITFIELD_RO");

    /// <summary>Sends <c>DUMP</c>: serializes the value of a key.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The payload, or <see langword="null"/> when the key does not exist.</returns>
    /// <exception cref="RuvioException">The type is not supported: streams, JSON, Bloom filters and hashes with field TTLs are refused.</exception>
    /// <remarks>The payload is Ruvio's own format (<c>RVD1</c> plus a checksummed record), not Redis RDB; only Ruvio's <see cref="RestoreAsync"/> reads it. The TTL is not part of it.</remarks>
    /// <example>
    /// <code>
    /// byte[]? blob = await db.DumpAsync("user:1");
    /// </code>
    /// </example>
    ValueTask<byte[]?> DumpAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Sends <c>RESTORE key ttl payload [REPLACE] [ABSTTL]</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="payload">A payload from <see cref="DumpAsync"/>.</param>
    /// <param name="ttlMilliseconds">Expiry in milliseconds; 0 means none. With <paramref name="absoluteTtl"/> it is a Unix timestamp in milliseconds.</param>
    /// <param name="replace">When true, an existing key is overwritten; otherwise the call fails if the key exists.</param>
    /// <param name="absoluteTtl">Treat the TTL as an absolute Unix time. A time already past creates nothing.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <exception cref="RuvioException">The key exists and <paramref name="replace"/> is false, or the payload is not valid Ruvio dump data.</exception>
    /// <remarks>One atomic write.</remarks>
    /// <example>
    /// <code>
    /// await db.RestoreAsync("user:1:copy", blob!, replace: true);
    /// </code>
    /// </example>
    ValueTask RestoreAsync(string key, byte[] payload, long ttlMilliseconds = 0, bool replace = false, bool absoluteTtl = false, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>FILLGET key fill_ms [WAIT wait_ms]</c>: a cache read that lets one caller rebuild a missing value while the others wait. A Ruvio extension.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="fill">How long the fill marker lives for the caller that is told to fill. The marker lapses after this time.</param>
    /// <param name="wait">When set, a caller that would get <see cref="FillState.Busy"/> is held up to this long until the filler stores a value, the marker lapses (the waiter becomes the filler) or the wait ends with <c>Busy</c>. Null does not wait.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>A <see cref="FillResult"/>: <c>Hit</c> with the value; <c>Fill</c> with a token, so this caller must build the value and call <see cref="FillSetAsync"/>; <c>Busy</c> while another caller fills; or <c>Stale</c> with the old value inside a stale-while-revalidate window.</returns>
    /// <exception cref="RuvioException"><paramref name="wait"/> is used inside a transaction or script.</exception>
    /// <remarks>Markers live in memory only. Most callers should use <c>GetOrCreateAsync</c>, which wraps this protocol.</remarks>
    /// <example>
    /// <code>
    /// var r = await db.FillGetAsync("page:home", TimeSpan.FromSeconds(10));
    /// if (r.State == FillState.Fill) await db.FillSetAsync("page:home", Render(), TimeSpan.FromMinutes(1), r.Token!.Value);
    /// </code>
    /// </example>
    ValueTask<FillResult> FillGetAsync(string key, TimeSpan fill, TimeSpan? wait = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <c>FILLSET key value ttl_ms token [SWR stale_ms]</c>: stores the value built after <see cref="FillGetAsync"/>.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value to cache.</param>
    /// <param name="ttl">How long the value is fresh.</param>
    /// <param name="token">The token from <see cref="FillResult.Token"/>.</param>
    /// <param name="stale">Stale-while-revalidate window: how long the value may still be served as <c>Stale</c> after <paramref name="ttl"/>. Null stores none. The soft expiry is not persisted.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when the token was still live and the value was stored; <see langword="false"/> when the marker had lapsed.</returns>
    /// <example>
    /// <code>
    /// bool stored = await db.FillSetAsync("page:home", html, TimeSpan.FromMinutes(1), token);
    /// </code>
    /// </example>
    ValueTask<bool> FillSetAsync(string key, string value, TimeSpan ttl, long token, TimeSpan? stale = null, CancellationToken cancellationToken = default);
}
