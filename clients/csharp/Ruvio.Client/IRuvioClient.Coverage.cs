// Default members for the coverage pass. An IRuvioClient implementation that only implements
// ExecuteAsync(IReadOnlyList<string>, ...) still gets every command below; RuvioClient overrides each one
// with the inline (allocation-free argument) path.

namespace Ruvio.Client;

public partial interface IRuvioClient
{
    /// <summary>Sends <c>SET key value IFEQ expected [PX ms]</c>: compare-and-set on a string. A Ruvio extension.</summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The new value.</param>
    /// <param name="expected">The value the key must currently hold.</param>
    /// <param name="expiry">New time to live, rounded to milliseconds. Null removes any TTL; use <see cref="SetIfEqualKeepingTimeToLiveAsync"/> to keep it.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when the key matched and was written; <see langword="false"/> when it was missing or held another value.</returns>
    /// <exception cref="RuvioException">The key holds a value that is not a string (<c>WRONGTYPE</c>).</exception>
    /// <example>
    /// <code>
    /// bool swapped = await db.SetIfEqualAsync("lock:1", "new", expected: "old");
    /// </code>
    /// </example>
    ValueTask<bool> SetIfEqualAsync(string key, string value, string expected, TimeSpan? expiry = null, CancellationToken cancellationToken = default)
        => RespRead.NotNull(ExecuteAsync(CoverageCommands.SetIfEqual(key, value, expected, expiry).AsList(), cancellationToken));

    /// <summary>Sends <c>SET key value IFGT [PX ms]</c>: writes when the key is missing or the value is greater than the stored integer (watermark, version, highest bid). A Ruvio extension.</summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The new number.</param>
    /// <param name="expiry">New time to live, rounded to milliseconds. Null removes any TTL.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when written; <see langword="false"/> when the stored number is greater or equal.</returns>
    /// <exception cref="RuvioException">The key holds a value that is not a string (<c>WRONGTYPE</c>), or the stored string is not an integer.</exception>
    /// <example>
    /// <code>
    /// bool advanced = await db.SetIfGreaterAsync("watermark", 1_700_000_000L);
    /// </code>
    /// </example>
    ValueTask<bool> SetIfGreaterAsync(string key, long value, TimeSpan? expiry = null, CancellationToken cancellationToken = default)
        => RespRead.NotNull(ExecuteAsync(CoverageCommands.SetIfGreater(key, value, expiry).AsList(), cancellationToken));

    /// <summary>Sends <c>SET key value IFLT [PX ms]</c>: writes when the key is missing or the value is smaller than the stored integer (lowest price, earliest deadline). A Ruvio extension.</summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The new number.</param>
    /// <param name="expiry">New time to live, rounded to milliseconds. Null removes any TTL.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when written; <see langword="false"/> when the stored number is smaller or equal.</returns>
    /// <exception cref="RuvioException">The key holds a value that is not a string (<c>WRONGTYPE</c>), or the stored string is not an integer.</exception>
    /// <example>
    /// <code>
    /// bool lowered = await db.SetIfLessAsync("best:price", 499L);
    /// </code>
    /// </example>
    ValueTask<bool> SetIfLessAsync(string key, long value, TimeSpan? expiry = null, CancellationToken cancellationToken = default)
        => RespRead.NotNull(ExecuteAsync(CoverageCommands.SetIfLess(key, value, expiry).AsList(), cancellationToken));

    /// <summary>Sends <c>SET key value IFEQ expected KEEPTTL</c>: compare-and-set that keeps the current expiry.</summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The new value.</param>
    /// <param name="expected">The value the key must currently hold.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when written; <see langword="false"/> when the key was missing or held another value.</returns>
    /// <exception cref="RuvioException">The key holds a value that is not a string (<c>WRONGTYPE</c>).</exception>
    /// <example>
    /// <code>
    /// bool swapped = await db.SetIfEqualKeepingTimeToLiveAsync("lock:1", "new", "old");
    /// </code>
    /// </example>
    ValueTask<bool> SetIfEqualKeepingTimeToLiveAsync(string key, string value, string expected, CancellationToken cancellationToken = default)
        => RespRead.NotNull(ExecuteAsync(CoverageCommands.SetIfEqualKeepingTimeToLive(key, value, expected).AsList(), cancellationToken));

    /// <summary>
    /// Sends <c>DELEX key IFEQ expected</c>: deletes a string only when it still holds the expected value. A Ruvio extension.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="expected">The value the key must hold.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when deleted; <see langword="false"/> when the key was missing or held another value.</returns>
    /// <exception cref="RuvioException">The key holds a value that is not a string (<c>WRONGTYPE</c>).</exception>
    /// <remarks>The safe way to release a lock you took with a token.</remarks>
    /// <example>
    /// <code>
    /// bool released = await db.DeleteIfEqualAsync("lock:1", myToken);
    /// </code>
    /// </example>
    ValueTask<bool> DeleteIfEqualAsync(string key, string expected, CancellationToken cancellationToken = default)
        => RespRead.Flag(ExecuteAsync(CoverageCommands.DeleteIfEqual(key, expected).AsList(), cancellationToken), "DELEX");

    /// <summary>Sends <c>SETEX key seconds value</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="seconds">Time to live in seconds. Must be positive.</param>
    /// <param name="value">The value to store.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <exception cref="RuvioException"><paramref name="seconds"/> is zero or negative.</exception>
    /// <example>
    /// <code>
    /// await db.SetExpireAsync("otp:1", 60, "123456");
    /// </code>
    /// </example>
    ValueTask SetExpireAsync(string key, long seconds, string value, CancellationToken cancellationToken = default)
        => RespRead.Ok(ExecuteAsync(CoverageCommands.SetExpire(key, seconds, value).AsList(), cancellationToken));

    /// <summary>Sends <c>PSETEX key milliseconds value</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="milliseconds">Time to live in milliseconds. Must be positive.</param>
    /// <param name="value">The value to store.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <exception cref="RuvioException"><paramref name="milliseconds"/> is zero or negative.</exception>
    /// <example>
    /// <code>
    /// await db.SetExpireMillisecondsAsync("otp:1", 1500, "123456");
    /// </code>
    /// </example>
    ValueTask SetExpireMillisecondsAsync(string key, long milliseconds, string value, CancellationToken cancellationToken = default)
        => RespRead.Ok(ExecuteAsync(CoverageCommands.SetExpireMilliseconds(key, milliseconds, value).AsList(), cancellationToken));

    /// <summary>Sends <c>SETNX key value</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value to store.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when the key was created; <see langword="false"/> when it already existed.</returns>
    /// <remarks><see cref="SetAsync(string, string, TimeSpan?, SetMode, CancellationToken)"/> with <see cref="SetMode.IfNotExists"/> does the same and can add a TTL in one call.</remarks>
    /// <example>
    /// <code>
    /// bool created = await db.SetIfNotExistsAsync("flag", "1");
    /// </code>
    /// </example>
    ValueTask<bool> SetIfNotExistsAsync(string key, string value, CancellationToken cancellationToken = default)
        => RespRead.Flag(ExecuteAsync(CoverageCommands.SetIfNotExists(key, value).AsList(), cancellationToken), "SETNX");

    /// <summary>Sends <c>GETDEL</c>: reads a string and deletes it in one step.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The old value, or <see langword="null"/> when the key did not exist.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// string? token = await db.GetDeleteAsync("magic-link:1");
    /// </code>
    /// </example>
    ValueTask<string?> GetDeleteAsync(string key, CancellationToken cancellationToken = default)
        => RespRead.Text(ExecuteAsync(CoverageCommands.GetDelete(key).AsList(), cancellationToken));

    /// <summary>Sends <c>INCRBYFLOAT</c>: adds a floating-point number to a string.</summary>
    /// <param name="key">The key.</param>
    /// <param name="delta">The amount to add. Negative subtracts.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The new value. A missing key starts at 0.</returns>
    /// <exception cref="RuvioException">The value is not a number, or the result is not finite; <c>WRONGTYPE</c> for another type.</exception>
    /// <remarks>Computed in double precision and printed with 16 significant digits (Redis uses long double). A TTL is kept.</remarks>
    /// <example>
    /// <code>
    /// double total = await db.IncrementByFloatAsync("balance", 2.5);
    /// </code>
    /// </example>
    ValueTask<double> IncrementByFloatAsync(string key, double delta, CancellationToken cancellationToken = default)
        => RespRead.Double(ExecuteAsync(CoverageCommands.IncrementByFloat(key, delta).AsList(), cancellationToken), "INCRBYFLOAT");

    /// <summary>Sends <c>GETRANGE</c>: returns part of a string.</summary>
    /// <param name="key">The key.</param>
    /// <param name="start">First byte, zero-based. Negative counts from the end.</param>
    /// <param name="end">Last byte, inclusive. Negative counts from the end.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The bytes in the range decoded as text; empty when the key is missing or the range lies outside the string.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>A range wholly before the start of the string returns an empty string (Redis 7.0 returns the first byte).</remarks>
    /// <example>
    /// <code>
    /// string head = await db.GetRangeAsync("greeting", 0, 4);
    /// </code>
    /// </example>
    ValueTask<string> GetRangeAsync(string key, long start, long end, CancellationToken cancellationToken = default)
        => RespRead.RequiredText(ExecuteAsync(CoverageCommands.GetRange("GETRANGE", key, start, end).AsList(), cancellationToken), "GETRANGE");

    /// <summary>Sends <c>SUBSTR</c>, the older name of <c>GETRANGE</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="start">First byte, zero-based. Negative counts from the end.</param>
    /// <param name="end">Last byte, inclusive. Negative counts from the end.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The bytes in the range decoded as text; empty when the key is missing or the range is outside.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Prefer <see cref="GetRangeAsync"/>.</remarks>
    /// <example>
    /// <code>
    /// string part = await db.GetSubstringAsync("greeting", 0, 4);
    /// </code>
    /// </example>
    ValueTask<string> GetSubstringAsync(string key, long start, long end, CancellationToken cancellationToken = default)
        => RespRead.RequiredText(ExecuteAsync(CoverageCommands.GetRange("SUBSTR", key, start, end).AsList(), cancellationToken), "SUBSTR");

    /// <summary>Sends <c>SETRANGE</c>: overwrites part of a string from <paramref name="offset"/>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="offset">Byte position to start at. A gap past the end is padded with zero bytes.</param>
    /// <param name="value">The bytes to write.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The new string length in bytes.</returns>
    /// <exception cref="RuvioException">The result would pass 16 MiB; <c>WRONGTYPE</c> for another type.</exception>
    /// <remarks>Rewrites the value (O(size)) and keeps the TTL.</remarks>
    /// <example>
    /// <code>
    /// long length = await db.SetRangeAsync("greeting", 6, "World");
    /// </code>
    /// </example>
    ValueTask<long> SetRangeAsync(string key, long offset, string value, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(CoverageCommands.SetRange(key, offset, value).AsList(), cancellationToken), "SETRANGE");

    /// <summary>Sends <c>MSETNX</c>: sets several keys only if none of them exists.</summary>
    /// <param name="pairs">The key and value pairs.</param>
    /// <returns><see langword="true"/> when all were set; <see langword="false"/> when at least one existed and nothing was written.</returns>
    /// <remarks>All-or-nothing, so in a cluster every key must share a hash slot (use hash tags such as <c>{user:1}:a</c>); otherwise the client throws before sending (<c>CROSSSLOT</c>).</remarks>
    /// <example>
    /// <code>
    /// bool ok = await db.SetManyIfNotExistsAsync(("{u:1}:a", "1"), ("{u:1}:b", "2"));
    /// </code>
    /// </example>
    ValueTask<bool> SetManyIfNotExistsAsync(params (string Key, string Value)[] pairs)
        => RespRead.Flag(ExecuteAsync(CoverageCommands.SetManyIfNotExists(pairs).AsList(), CancellationToken.None), "MSETNX");

    /// <summary>Sends <c>SETBIT</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="offset">Bit position. Bit 0 is the most significant bit of the first byte.</param>
    /// <param name="value">The new bit.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The bit that was there before.</returns>
    /// <exception cref="RuvioException">The offset is negative or beyond the 16 MiB limit; <c>WRONGTYPE</c> for another type.</exception>
    /// <remarks>Grows the string with zero bytes as needed.</remarks>
    /// <example>
    /// <code>
    /// bool was = await db.SetBitAsync("online", 1234, true);
    /// </code>
    /// </example>
    ValueTask<bool> SetBitAsync(string key, long offset, bool value, CancellationToken cancellationToken = default)
        => RespRead.Flag(ExecuteAsync(CoverageCommands.SetBit(key, offset, value).AsList(), cancellationToken), "SETBIT");

    /// <summary>Sends <c>GETBIT</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="offset">Bit position. Bit 0 is the most significant bit of the first byte.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The bit. A missing key or an offset past the end gives <see langword="false"/>.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// bool on = await db.GetBitAsync("online", 1234);
    /// </code>
    /// </example>
    ValueTask<bool> GetBitAsync(string key, long offset, CancellationToken cancellationToken = default)
        => RespRead.Flag(ExecuteAsync(CoverageCommands.GetBit(key, offset).AsList(), cancellationToken), "GETBIT");

    /// <summary>Sends <c>BITCOUNT</c>: counts bits set to 1.</summary>
    /// <param name="key">The key.</param>
    /// <param name="start">First position of the range. Null counts the whole string.</param>
    /// <param name="end">Last position, inclusive. Negative counts from the end.</param>
    /// <param name="unit">Whether the range is in bytes or bits.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The number of set bits; 0 for a missing key.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long active = await db.BitCountAsync("online");
    /// </code>
    /// </example>
    ValueTask<long> BitCountAsync(string key, long? start = null, long? end = null, BitUnit unit = BitUnit.Byte, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(CoverageCommands.BitCount(key, start, end, unit).AsList(), cancellationToken), "BITCOUNT");

    /// <summary>Sends <c>BITPOS</c>: finds the first bit equal to a value.</summary>
    /// <param name="key">The key.</param>
    /// <param name="bit">The bit to look for.</param>
    /// <param name="start">First position of the range. Null searches from the start.</param>
    /// <param name="end">Last position, inclusive. Negative counts from the end.</param>
    /// <param name="unit">Whether the range is in bytes or bits.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The bit position, or <c>-1</c> when it is not found.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long firstFree = await db.BitPositionAsync("online", false);
    /// </code>
    /// </example>
    ValueTask<long> BitPositionAsync(string key, bool bit, long? start = null, long? end = null, BitUnit unit = BitUnit.Byte, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(CoverageCommands.BitPosition(key, bit, start, end, unit).AsList(), cancellationToken), "BITPOS");

    /// <summary>Sends <c>BITOP</c>: combines strings bit by bit and stores the result.</summary>
    /// <param name="operation"><c>AND</c>, <c>OR</c>, <c>XOR</c> or <c>NOT</c>.</param>
    /// <param name="destination">The key that receives the result.</param>
    /// <param name="keys">The source keys. <see cref="BitOperation.Not"/> takes exactly one.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The length in bytes of the stored result.</returns>
    /// <exception cref="RuvioException">The result would pass 16 MiB; <c>WRONGTYPE</c> for another type.</exception>
    /// <remarks>In a cluster all keys must share a hash slot.</remarks>
    /// <example>
    /// <code>
    /// long bytes = await db.BitOperationAsync(BitOperation.And, "both", new[] { "a", "b" });
    /// </code>
    /// </example>
    ValueTask<long> BitOperationAsync(BitOperation operation, string destination, IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(CoverageCommands.BitOperation(operation, destination, keys).AsList(), cancellationToken), "BITOP");

    /// <summary>
    /// Sends <c>PFADD</c>: adds elements to a HyperLogLog, a small structure that estimates distinct counts.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="elements">The elements to add.</param>
    /// <returns><see langword="true"/> when the estimate may have changed.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Uses the dense encoding only (a 12,304-byte string); every call rewrites the value.</remarks>
    /// <example>
    /// <code>
    /// await db.HyperLogLogAddAsync("visitors", "u1", "u2");
    /// </code>
    /// </example>
    ValueTask<bool> HyperLogLogAddAsync(string key, params string[] elements)
        => RespRead.Flag(ExecuteAsync(CoverageCommands.Members("PFADD", key, elements).AsList(), CancellationToken.None), "PFADD");

    /// <summary>Sends <c>PFCOUNT</c>: estimates the number of distinct elements.</summary>
    /// <param name="keys">One or more HyperLogLog keys. With several, the estimate is for their union (nothing is stored).</param>
    /// <returns>The estimated count; 0 for a missing key.</returns>
    /// <exception cref="RuvioException">A key is not a HyperLogLog.</exception>
    /// <remarks>In a cluster all keys must share a hash slot.</remarks>
    /// <example>
    /// <code>
    /// long n = await db.HyperLogLogCountAsync("visitors");
    /// </code>
    /// </example>
    ValueTask<long> HyperLogLogCountAsync(params string[] keys)
        => RespRead.Integer(ExecuteAsync(CoverageCommands.Keys("PFCOUNT", keys).AsList(), CancellationToken.None), "PFCOUNT");

    /// <summary>Sends <c>PFMERGE</c>: stores the union of HyperLogLogs.</summary>
    /// <param name="destination">The key that receives the result.</param>
    /// <param name="sources">The HyperLogLog keys to merge.</param>
    /// <exception cref="RuvioException">A key is not a HyperLogLog.</exception>
    /// <remarks>The destination takes part in the union, as in Redis. In a cluster all keys must share a hash slot.</remarks>
    /// <example>
    /// <code>
    /// await db.HyperLogLogMergeAsync("visitors:all", "visitors:mon", "visitors:tue");
    /// </code>
    /// </example>
    ValueTask HyperLogLogMergeAsync(string destination, params string[] sources)
        => RespRead.Ok(ExecuteAsync(CoverageCommands.Members("PFMERGE", destination, sources).AsList(), CancellationToken.None));

    /// <summary>
    /// Sends <c>LPUSHCAP</c>: pushes to the head and then trims the list to at most <paramref name="cap"/> elements. A Ruvio extension.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="cap">The maximum list length after the push. The oldest elements (from the tail) are dropped.</param>
    /// <param name="values">The elements to push.</param>
    /// <returns>The list length after the push and trim.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long len = await db.PushListLeftCappedAsync("recent", 100, "event");
    /// </code>
    /// </example>
    ValueTask<long> PushListLeftCappedAsync(string key, long cap, params string[] values)
        => RespRead.Integer(ExecuteAsync(CoverageCommands.Capped("LPUSHCAP", key, cap, values).AsList(), CancellationToken.None), "LPUSHCAP");

    /// <summary>
    /// Sends <c>RPUSHCAP</c>: pushes to the tail and then trims the list to at most <paramref name="cap"/> elements. A Ruvio extension.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="cap">The maximum list length after the push. The oldest elements (from the head) are dropped.</param>
    /// <param name="values">The elements to push.</param>
    /// <returns>The list length after the push and trim.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long len = await db.PushListRightCappedAsync("log", 1000, "line");
    /// </code>
    /// </example>
    ValueTask<long> PushListRightCappedAsync(string key, long cap, params string[] values)
        => RespRead.Integer(ExecuteAsync(CoverageCommands.Capped("RPUSHCAP", key, cap, values).AsList(), CancellationToken.None), "RPUSHCAP");

    /// <summary>
    /// Sends <c>BLMPOP</c>: like <see cref="PopListManyAsync(IReadOnlyList{string}, bool, long, CancellationToken)"/> but waits when every list is empty.
    /// </summary>
    /// <param name="keys">The lists to try, in order.</param>
    /// <param name="fromLeft">True pops from the head; false from the tail.</param>
    /// <param name="timeout">How long to wait, rounded up to whole seconds. <see cref="TimeSpan.Zero"/> waits forever.</param>
    /// <param name="count">The most elements to pop. Default 1.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The key that was popped and its elements, or <see langword="null"/> when the timeout passes.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Holds the shared connection for the whole wait. The server retries every 5 ms, allows 1,024 blocked clients per shard, and refuses the command in <c>MULTI</c> and scripts.</remarks>
    /// <example>
    /// <code>
    /// var popped = await db.PopListManyBlockingAsync(new[] { "q" }, true, TimeSpan.FromSeconds(5));
    /// </code>
    /// </example>
    ValueTask<KeyValuePair<string, IReadOnlyList<string>>?> PopListManyBlockingAsync(IReadOnlyList<string> keys, bool fromLeft, TimeSpan timeout, long count = 1, CancellationToken cancellationToken = default)
        => RespRead.ListPop(ExecuteAsync(CoverageCommands.PopListManyBlocking(keys, fromLeft, timeout, count).AsList(), cancellationToken), "BLMPOP");

    /// <summary>
    /// Sends <c>BLMPOP</c> with a typed side: like the non-blocking pop but waits when every list is empty.
    /// </summary>
    /// <param name="keys">The lists to try, in order.</param>
    /// <param name="side">The end to pop from, a <see cref="ListSide"/>.</param>
    /// <param name="timeout">How long to wait, rounded up to whole seconds. <see cref="TimeSpan.Zero"/> waits forever.</param>
    /// <param name="count">The most elements to pop. Default 1.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The key that was popped and its elements, or <see langword="null"/> when the timeout passes.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Holds the shared connection for the whole wait. The server retries every 5 ms, allows 1,024 blocked clients per shard, and refuses the command in <c>MULTI</c> and scripts.</remarks>
    /// <example>
    /// <code>
    /// var popped = await db.PopListManyBlockingAsync(new[] { "q" }, ListSide.Left, TimeSpan.FromSeconds(5));
    /// </code>
    /// </example>
    ValueTask<KeyValuePair<string, IReadOnlyList<string>>?> PopListManyBlockingAsync(IReadOnlyList<string> keys, ListSide side, TimeSpan timeout, long count = 1, CancellationToken cancellationToken = default)
        => PopListManyBlockingAsync(keys, side == ListSide.Left, timeout, count, cancellationToken);

    /// <summary>Sends <c>SMISMEMBER</c>: checks several members at once.</summary>
    /// <param name="key">The key.</param>
    /// <param name="members">The members to test.</param>
    /// <returns>One flag per member, in order. All false when the key is missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// IReadOnlyList&lt;bool&gt; flags = await db.SetContainsManyAsync("tags", "a", "b");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<bool>> SetContainsManyAsync(string key, params string[] members)
        => RespRead.Flags(ExecuteAsync(CoverageCommands.Members("SMISMEMBER", key, members).AsList(), CancellationToken.None), "SMISMEMBER");

    /// <summary>Sends <c>SINTERCARD</c>: counts the members common to all sets without returning them.</summary>
    /// <param name="keys">The sets.</param>
    /// <param name="limit">Stop counting at this number. 0 means no limit.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The size of the intersection (capped at <paramref name="limit"/> when set).</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>In a cluster all keys must share a hash slot.</remarks>
    /// <example>
    /// <code>
    /// long common = await db.GetSetIntersectionCountAsync(new[] { "a", "b" });
    /// </code>
    /// </example>
    ValueTask<long> GetSetIntersectionCountAsync(IReadOnlyList<string> keys, long limit = 0, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(CoverageCommands.SetIntersectionCount(keys, limit).AsList(), cancellationToken), "SINTERCARD");

    /// <summary>Sends <c>SSCAN</c>: walks a set one page at a time.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cursor">0 to start; then the cursor of the previous page. The scan is done when the returned cursor is 0.</param>
    /// <param name="pattern">Glob filter on members. Null returns all.</param>
    /// <param name="count">A hint for the page size. Null uses the server default.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>A <see cref="SetScanPage"/> with the next cursor and the members.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>A missing key gives cursor 0 and no members. The cursor is a position in member order.</remarks>
    /// <example>
    /// <code>
    /// var page = await db.ScanSetAsync("tags", 0, pattern: "a*");
    /// </code>
    /// </example>
    ValueTask<SetScanPage> ScanSetAsync(string key, long cursor, string? pattern = null, int? count = null, CancellationToken cancellationToken = default)
        => RespRead.SetScan(ExecuteAsync(CoverageCommands.KeyScan("SSCAN", key, cursor, pattern, count).AsList(), cancellationToken));

    /// <summary>Sends <c>SCAN cursor [MATCH pattern] [COUNT n] [TYPE type]</c>.</summary>
    /// <param name="cursor">0 to start; then the cursor of the previous page. The scan is done when the returned cursor is 0.</param>
    /// <param name="pattern">Glob filter on key names. Null returns all.</param>
    /// <param name="count">A hint for the page size. Null uses the server default.</param>
    /// <param name="type">Only keys of this type, for example <c>string</c>, <c>list</c>, <c>set</c>, <c>zset</c>, <c>hash</c> or <c>stream</c>. Null returns all.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>A <see cref="ScanPage"/> with the next cursor and the keys.</returns>
    /// <remarks>Cursors walk sorted live keys and are not Redis dict cursors. In a cluster the cursor covers the shards in turn.</remarks>
    /// <example>
    /// <code>
    /// var page = await db.ScanAsync(0, "user:*", 100, "string");
    /// </code>
    /// </example>
    ValueTask<ScanPage> ScanAsync(long cursor, string? pattern, int? count, string? type, CancellationToken cancellationToken = default)
        => RuvioClient.ScanPageOf(ExecuteAsync(CoverageCommands.Scan(cursor, pattern, count, type).AsList(), cancellationToken));

    /// <summary>Sends <c>SCAN cursor [MATCH pattern] [COUNT n] TYPE type</c> with a typed value type.</summary>
    /// <param name="cursor">0 to start; then the cursor of the previous page. The scan is done when the returned cursor is 0.</param>
    /// <param name="pattern">Glob filter on key names. Null returns all.</param>
    /// <param name="count">A hint for the page size. Null uses the server default.</param>
    /// <param name="type">Only keys of this <see cref="KeyType"/>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>A <see cref="ScanPage"/> with the next cursor and the keys.</returns>
    /// <remarks>Cursors walk sorted live keys and are not Redis dict cursors. In a cluster the cursor covers the shards in turn.</remarks>
    /// <example>
    /// <code>
    /// var page = await db.ScanAsync(0, "user:*", 100, KeyType.Hash);
    /// </code>
    /// </example>
    ValueTask<ScanPage> ScanAsync(long cursor, string? pattern, int? count, KeyType type, CancellationToken cancellationToken = default)
        => ScanAsync(cursor, pattern, count, Keywords.Word(type), cancellationToken);

    /// <summary>
    /// Sends <c>ZRANGEBYLEX</c> (or <c>ZREVRANGEBYLEX</c>): members in a lexicographic range, with an optional page.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="min">Lower bound: <c>-</c> for the start, or <c>[member</c> (inclusive) or <c>(member</c> (exclusive).</param>
    /// <param name="max">Upper bound: <c>+</c> for the end, or <c>[member</c> or <c>(member</c>.</param>
    /// <param name="descending">When true, returns the range from high to low.</param>
    /// <param name="offset">How many matches to skip. Default 0.</param>
    /// <param name="count">The most members to return. A negative value returns the rest. Default -1.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The members in the range; empty when the key is missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Meant for sets whose members all have the same score; the order ignores scores.</remarks>
    /// <example>
    /// <code>
    /// var names = await db.GetSortedSetRangeByLexAsync("names", "[a", "(b", false);
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> GetSortedSetRangeByLexAsync(string key, string min, string max, bool descending, long offset = 0, long count = -1, CancellationToken cancellationToken = default)
        => RespRead.Strings(ExecuteAsync(CoverageCommands.RangeByLex(key, min, max, descending, offset, count).AsList(), cancellationToken), "ZRANGEBYLEX");

    /// <summary>Sends <c>ZRANGE key min max BYSCORE [REV] [LIMIT offset count]</c>: members in a score range.</summary>
    /// <param name="key">The key.</param>
    /// <param name="min">Lower bound as text: a number, <c>(5</c> for exclusive, <c>-inf</c> or <c>+inf</c>.</param>
    /// <param name="max">Upper bound as text, in the same forms.</param>
    /// <param name="descending">When true, returns the range from high to low.</param>
    /// <param name="offset">How many matches to skip. Default 0.</param>
    /// <param name="count">The most members to return. A negative value returns the rest. Default -1.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The members in the range, lowest score first (highest first when descending); empty when the key is missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// var top = await db.GetSortedSetRangeByScoreAsync("scores", "100", "+inf", descending: false, count: 10);
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> GetSortedSetRangeByScoreAsync(string key, string min, string max, bool descending = false, long offset = 0, long count = -1, CancellationToken cancellationToken = default)
        => RespRead.Strings(ExecuteAsync(CoverageCommands.RangeByScore(key, min, max, descending, offset, count).AsList(), cancellationToken), "ZRANGE");

    /// <summary>Sends <c>ZCOUNT</c>: counts the members with a score in a range.</summary>
    /// <param name="key">The key.</param>
    /// <param name="min">Lower bound: a number, <c>(5</c> for exclusive, or <c>-inf</c>.</param>
    /// <param name="max">Upper bound, in the same forms (<c>+inf</c> for no limit).</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The count; 0 for a missing key.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long n = await db.CountSortedSetByScoreAsync("scores", "50", "+inf");
    /// </code>
    /// </example>
    ValueTask<long> CountSortedSetByScoreAsync(string key, string min, string max, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(new RespCommand("ZCOUNT", key, min, max).AsList(), cancellationToken), "ZCOUNT");

    /// <summary>Sends <c>ZLEXCOUNT</c>: counts the members in a lexicographic range.</summary>
    /// <param name="key">The key.</param>
    /// <param name="min">Lower bound: <c>-</c>, <c>[member</c> or <c>(member</c>.</param>
    /// <param name="max">Upper bound: <c>+</c>, <c>[member</c> or <c>(member</c>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The count; 0 for a missing key.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long n = await db.CountSortedSetByLexAsync("names", "[a", "(b");
    /// </code>
    /// </example>
    ValueTask<long> CountSortedSetByLexAsync(string key, string min, string max, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(new RespCommand("ZLEXCOUNT", key, min, max).AsList(), cancellationToken), "ZLEXCOUNT");

    /// <summary>Sends <c>ZMSCORE</c>: reads the scores of several members at once.</summary>
    /// <param name="key">The key.</param>
    /// <param name="members">The members to look up.</param>
    /// <returns>One entry per member in order: the score, or <see langword="null"/> when the member (or key) does not exist.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// var scores = await db.GetSortedSetScoresAsync("lb", "ann", "bob");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<double?>> GetSortedSetScoresAsync(string key, params string[] members)
        => RespRead.Scores(ExecuteAsync(CoverageCommands.Members("ZMSCORE", key, members).AsList(), CancellationToken.None), "ZMSCORE");

    /// <summary>Sends <c>ZRANDMEMBER</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>A pseudo-random member, or <see langword="null"/> when the key is missing or empty.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Reads the whole set (O(N)).</remarks>
    /// <example>
    /// <code>
    /// string? m = await db.GetSortedSetRandomMemberAsync("lb");
    /// </code>
    /// </example>
    ValueTask<string?> GetSortedSetRandomMemberAsync(string key, CancellationToken cancellationToken = default)
        => RespRead.Text(ExecuteAsync(new RespCommand("ZRANDMEMBER", key).AsList(), cancellationToken));

    /// <summary>Sends <c>ZRANDMEMBER key count</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="count">How many members. A positive count returns distinct members (at most the set size); a negative count may repeat members and returns exactly <c>|count|</c>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The members; empty when the key is missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Reads the whole set (O(N)).</remarks>
    /// <example>
    /// <code>
    /// var sample = await db.GetSortedSetRandomMembersAsync("lb", 3);
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> GetSortedSetRandomMembersAsync(string key, long count, CancellationToken cancellationToken = default)
        => RespRead.Strings(ExecuteAsync(CoverageCommands.RandomMembers("ZRANDMEMBER", key, count, null).AsList(), cancellationToken), "ZRANDMEMBER");

    /// <summary>Sends <c>ZRANDMEMBER key count WITHSCORES</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="count">How many members. A positive count returns distinct members; a negative count may repeat members and returns exactly <c>|count|</c>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The members with their scores; empty when the key is missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Reads the whole set (O(N)).</remarks>
    /// <example>
    /// <code>
    /// var sample = await db.GetSortedSetRandomMembersWithScoresAsync("lb", 3);
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<SortedSetEntry>> GetSortedSetRandomMembersWithScoresAsync(string key, long count, CancellationToken cancellationToken = default)
        => RespRead.Entries(ExecuteAsync(CoverageCommands.RandomMembers("ZRANDMEMBER", key, count, "WITHSCORES").AsList(), cancellationToken), "ZRANDMEMBER");

    /// <summary>Sends <c>ZSCAN</c>: walks a sorted set one page at a time.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cursor">0 to start; then the cursor of the previous page. The scan is done when the returned cursor is 0.</param>
    /// <param name="pattern">Glob filter on members. Null returns all.</param>
    /// <param name="count">A hint for the page size. Null uses the server default.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>A <see cref="SortedSetScanPage"/> with the next cursor and the entries.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>A missing key gives cursor 0 and no entries. The cursor is a position in score order.</remarks>
    /// <example>
    /// <code>
    /// var page = await db.ScanSortedSetAsync("lb", 0);
    /// </code>
    /// </example>
    ValueTask<SortedSetScanPage> ScanSortedSetAsync(string key, long cursor, string? pattern = null, int? count = null, CancellationToken cancellationToken = default)
        => RespRead.SortedSetScan(ExecuteAsync(CoverageCommands.KeyScan("ZSCAN", key, cursor, pattern, count).AsList(), cancellationToken));

    /// <summary>
    /// Sends <c>ZUNION numkeys key... [WEIGHTS ...] [AGGREGATE ...] WITHSCORES</c>: the union of sorted sets, returned.
    /// </summary>
    /// <param name="keys">The sorted sets.</param>
    /// <param name="weights">One multiplier per key applied to its scores. Null means 1 for all.</param>
    /// <param name="aggregate">How scores of one member combine: sum, min or max. Null uses sum.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The union members with scores, lowest score first.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>In a cluster all keys must share a hash slot.</remarks>
    /// <example>
    /// <code>
    /// var all = await db.GetSortedSetUnionAsync(new[] { "a", "b" });
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<SortedSetEntry>> GetSortedSetUnionAsync(IReadOnlyList<string> keys, IReadOnlyList<double>? weights = null, SortedSetAggregate? aggregate = null, CancellationToken cancellationToken = default)
        => RespRead.Entries(ExecuteAsync(CoverageCommands.Combine("ZUNION", null, keys, weights, aggregate, true).AsList(), cancellationToken), "ZUNION");

    /// <summary>
    /// Sends <c>ZINTER numkeys key... [WEIGHTS ...] [AGGREGATE ...] WITHSCORES</c>: members present in every sorted set, returned.
    /// </summary>
    /// <param name="keys">The sorted sets.</param>
    /// <param name="weights">One multiplier per key applied to its scores. Null means 1 for all.</param>
    /// <param name="aggregate">How scores of one member combine: sum, min or max. Null uses sum.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The common members with scores, lowest score first.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>In a cluster all keys must share a hash slot.</remarks>
    /// <example>
    /// <code>
    /// var common = await db.GetSortedSetIntersectionAsync(new[] { "a", "b" });
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<SortedSetEntry>> GetSortedSetIntersectionAsync(IReadOnlyList<string> keys, IReadOnlyList<double>? weights = null, SortedSetAggregate? aggregate = null, CancellationToken cancellationToken = default)
        => RespRead.Entries(ExecuteAsync(CoverageCommands.Combine("ZINTER", null, keys, weights, aggregate, true).AsList(), cancellationToken), "ZINTER");

    /// <summary>
    /// Sends <c>ZDIFF numkeys key... WITHSCORES</c>: members of the first sorted set that are in none of the others.
    /// </summary>
    /// <param name="keys">The first key is the base; the rest are subtracted.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The remaining members with their scores.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>In a cluster all keys must share a hash slot.</remarks>
    /// <example>
    /// <code>
    /// var only = await db.GetSortedSetDifferenceAsync(new[] { "a", "b" });
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<SortedSetEntry>> GetSortedSetDifferenceAsync(IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
        => RespRead.Entries(ExecuteAsync(CoverageCommands.Combine("ZDIFF", null, keys, null, null, true).AsList(), cancellationToken), "ZDIFF");

    /// <summary>Sends <c>ZUNIONSTORE</c>: stores the union of sorted sets.</summary>
    /// <param name="destination">The key that receives the result.</param>
    /// <param name="keys">The sorted sets.</param>
    /// <param name="weights">One multiplier per key applied to its scores. Null means 1 for all.</param>
    /// <param name="aggregate">How scores of one member combine: sum, min or max. Null uses sum.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The number of members in the stored result.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Replaces an existing destination. In a cluster all keys must share a hash slot.</remarks>
    /// <example>
    /// <code>
    /// long n = await db.StoreSortedSetUnionAsync("all", new[] { "a", "b" });
    /// </code>
    /// </example>
    ValueTask<long> StoreSortedSetUnionAsync(string destination, IReadOnlyList<string> keys, IReadOnlyList<double>? weights = null, SortedSetAggregate? aggregate = null, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(CoverageCommands.Combine("ZUNIONSTORE", destination, keys, weights, aggregate, false).AsList(), cancellationToken), "ZUNIONSTORE");

    /// <summary>Sends <c>ZINTERSTORE</c>: stores the members present in every sorted set.</summary>
    /// <param name="destination">The key that receives the result.</param>
    /// <param name="keys">The sorted sets.</param>
    /// <param name="weights">One multiplier per key applied to its scores. Null means 1 for all.</param>
    /// <param name="aggregate">How scores of one member combine: sum, min or max. Null uses sum.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The number of members in the stored result.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Replaces an existing destination. In a cluster all keys must share a hash slot.</remarks>
    /// <example>
    /// <code>
    /// long n = await db.StoreSortedSetIntersectionAsync("both", new[] { "a", "b" });
    /// </code>
    /// </example>
    ValueTask<long> StoreSortedSetIntersectionAsync(string destination, IReadOnlyList<string> keys, IReadOnlyList<double>? weights = null, SortedSetAggregate? aggregate = null, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(CoverageCommands.Combine("ZINTERSTORE", destination, keys, weights, aggregate, false).AsList(), cancellationToken), "ZINTERSTORE");

    /// <summary>Sends <c>ZDIFFSTORE</c>: stores the members of the first sorted set that are in none of the others.</summary>
    /// <param name="destination">The key that receives the result.</param>
    /// <param name="keys">The first key is the base; the rest are subtracted.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The number of members in the stored result.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Replaces an existing destination. In a cluster all keys must share a hash slot.</remarks>
    /// <example>
    /// <code>
    /// long n = await db.StoreSortedSetDifferenceAsync("only-a", new[] { "a", "b" });
    /// </code>
    /// </example>
    ValueTask<long> StoreSortedSetDifferenceAsync(string destination, IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(CoverageCommands.Combine("ZDIFFSTORE", destination, keys, null, null, false).AsList(), cancellationToken), "ZDIFFSTORE");

    /// <summary>Sends <c>HRANDFIELD</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>A pseudo-random field name, or <see langword="null"/> when the key is missing or empty.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Reads the whole hash (O(N)).</remarks>
    /// <example>
    /// <code>
    /// string? f = await db.GetHashRandomFieldAsync("user:1");
    /// </code>
    /// </example>
    ValueTask<string?> GetHashRandomFieldAsync(string key, CancellationToken cancellationToken = default)
        => RespRead.Text(ExecuteAsync(new RespCommand("HRANDFIELD", key).AsList(), cancellationToken));

    /// <summary>Sends <c>HRANDFIELD key count</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="count">How many fields. A positive count returns distinct fields (at most the hash size); a negative count may repeat fields and returns exactly <c>|count|</c>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The field names; empty when the key is missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Reads the whole hash (O(N)).</remarks>
    /// <example>
    /// <code>
    /// var fields = await db.GetHashRandomFieldsAsync("user:1", 2);
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> GetHashRandomFieldsAsync(string key, long count, CancellationToken cancellationToken = default)
        => RespRead.Strings(ExecuteAsync(CoverageCommands.RandomMembers("HRANDFIELD", key, count, null).AsList(), cancellationToken), "HRANDFIELD");

    /// <summary>Sends <c>HRANDFIELD key count WITHVALUES</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="count">How many fields. A positive count returns distinct fields; a negative count may repeat fields and returns exactly <c>|count|</c>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>Field and value pairs; empty when the key is missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <remarks>Reads the whole hash (O(N)).</remarks>
    /// <example>
    /// <code>
    /// var pairs = await db.GetHashRandomFieldsWithValuesAsync("user:1", 2);
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<KeyValuePair<string, string>>> GetHashRandomFieldsWithValuesAsync(string key, long count, CancellationToken cancellationToken = default)
        => RespRead.Pairs(ExecuteAsync(CoverageCommands.RandomMembers("HRANDFIELD", key, count, "WITHVALUES").AsList(), cancellationToken), "HRANDFIELD");

    /// <summary>Sends <c>HINCRBYFLOAT</c>: adds a floating-point number to a hash field.</summary>
    /// <param name="key">The key.</param>
    /// <param name="field">The hash field name.</param>
    /// <param name="delta">The amount to add. Negative subtracts.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The new value. A missing key or field starts at 0.</returns>
    /// <exception cref="RuvioException">The field is not a number, or the result is not finite; <c>WRONGTYPE</c> for another type.</exception>
    /// <example>
    /// <code>
    /// double v = await db.IncrementHashFieldByFloatAsync("stats", "avg", 0.5);
    /// </code>
    /// </example>
    ValueTask<double> IncrementHashFieldByFloatAsync(string key, string field, double delta, CancellationToken cancellationToken = default)
        => RespRead.Double(ExecuteAsync(CoverageCommands.IncrementHashFieldByFloat(key, field, delta).AsList(), cancellationToken), "HINCRBYFLOAT");

    /// <summary>
    /// Sends <c>BF.RESERVE key error capacity [EXPANSION n | NONSCALING]</c>: creates a Bloom filter with chosen growth.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="errorRate">The false-positive rate to aim for, between 0 and 1 (for example 0.01).</param>
    /// <param name="capacity">How many items the first layer is sized for.</param>
    /// <param name="expansion">For a scaling filter, how many times larger each new layer is. Null uses the server default (2). Not combined with <paramref name="nonScaling"/>.</param>
    /// <param name="nonScaling">When true the filter never grows; adds fail once it is full.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <exception cref="RuvioException">The key already exists, or an argument is out of range. A layer above 64 MiB is refused.</exception>
    /// <remarks>A scaling filter adds a layer when full, each with half the error rate of the one before.</remarks>
    /// <example>
    /// <code>
    /// await db.ReserveBloomFilterAsync("seen", 0.001, 1_000_000, expansion: null, nonScaling: true);
    /// </code>
    /// </example>
    ValueTask ReserveBloomFilterAsync(string key, double errorRate, long capacity, long? expansion, bool nonScaling = false, CancellationToken cancellationToken = default)
        => RespRead.Ok(ExecuteAsync(CoverageCommands.ReserveBloom(key, errorRate, capacity, expansion, nonScaling).AsList(), cancellationToken));

    /// <summary>Sends <c>BF.MADD</c>: adds several items in one call.</summary>
    /// <param name="key">The key.</param>
    /// <param name="items">The items to add.</param>
    /// <returns>One flag per item: true when added, false when it was probably already present.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>); or a non-scaling filter is full.</exception>
    /// <remarks>A missing key becomes a scalable filter (error 0.01, capacity 100, expansion 2). The whole batch is one write.</remarks>
    /// <example>
    /// <code>
    /// IReadOnlyList&lt;bool&gt; added = await db.AddBloomFilterManyAsync("seen", "a", "b");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<bool>> AddBloomFilterManyAsync(string key, params string[] items)
        => RespRead.Flags(ExecuteAsync(CoverageCommands.Members("BF.MADD", key, items).AsList(), CancellationToken.None), "BF.MADD");

    /// <summary>Sends <c>BF.MEXISTS</c>: checks several items at once.</summary>
    /// <param name="key">The key.</param>
    /// <param name="items">The items to test.</param>
    /// <returns>One flag per item: false means definitely absent, true means probably present. All false when the key is missing.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// IReadOnlyList&lt;bool&gt; hits = await db.BloomFilterContainsManyAsync("seen", "a", "b");
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<bool>> BloomFilterContainsManyAsync(string key, params string[] items)
        => RespRead.Flags(ExecuteAsync(CoverageCommands.Members("BF.MEXISTS", key, items).AsList(), CancellationToken.None), "BF.MEXISTS");

    /// <summary>Sends <c>BF.INSERT</c>: adds items, creating the filter with the given settings when it is new.</summary>
    /// <param name="key">The key.</param>
    /// <param name="items">The items to add.</param>
    /// <param name="options">Creation settings, used only when the key is new. Null uses the defaults.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>One flag per item: true when added, false when probably already present.</returns>
    /// <exception cref="RuvioException"><see cref="BloomInsertOptions.NoCreate"/> is set and the key is missing (<c>not found</c>); a non-scaling filter is full; <c>WRONGTYPE</c> for another type.</exception>
    /// <example>
    /// <code>
    /// await db.InsertBloomFilterAsync("seen", new[] { "a" }, new BloomInsertOptions { Capacity = 10_000, ErrorRate = 0.01 });
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<bool>> InsertBloomFilterAsync(string key, IReadOnlyList<string> items, BloomInsertOptions? options = null, CancellationToken cancellationToken = default)
        => RespRead.Flags(ExecuteAsync(CoverageCommands.InsertBloom(key, items, options).AsList(), cancellationToken), "BF.INSERT");

    /// <summary>Sends <c>BF.INFO</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>A <see cref="BloomFilterInfo"/>: capacity, size in bytes (bit arrays plus header), number of layers, items inserted and the expansion rate (null when non-scaling).</returns>
    /// <exception cref="RuvioException">The key does not exist or is not a Bloom filter.</exception>
    /// <example>
    /// <code>
    /// var info = await db.GetBloomFilterInfoAsync("seen");
    /// </code>
    /// </example>
    ValueTask<BloomFilterInfo> GetBloomFilterInfoAsync(string key, CancellationToken cancellationToken = default)
        => RespRead.BloomInfo(ExecuteAsync(new RespCommand("BF.INFO", key).AsList(), cancellationToken));

    /// <summary>Sends <c>BF.CARD</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The number of items added so far (counted when an item sets a new bit); 0 for a missing key.</returns>
    /// <exception cref="RuvioException">The key holds a value of another type (<c>WRONGTYPE</c>), or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// long n = await db.GetBloomFilterCardinalityAsync("seen");
    /// </code>
    /// </example>
    ValueTask<long> GetBloomFilterCardinalityAsync(string key, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(new RespCommand("BF.CARD", key).AsList(), cancellationToken), "BF.CARD");

    /// <summary>Sends <c>JSON.SET key path json [NX|XX]</c> with a write condition.</summary>
    /// <param name="key">The key.</param>
    /// <param name="path">A JSON path such as <c>$</c> (the whole document), <c>$.name</c> or <c>$.items[0]</c>. Only simple <c>.field</c> and <c>[N]</c> steps are supported.</param>
    /// <param name="json">The new value as JSON text, for example <c>"\"Ada\""</c> or <c>"{\"a\":1}"</c>.</param>
    /// <param name="mode"><see cref="SetMode.IfNotExists"/> writes only when the path is missing (<c>NX</c>); <see cref="SetMode.IfExists"/> writes only when it exists (<c>XX</c>); <see cref="SetMode.Always"/> adds no condition.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when the value was written; <see langword="false"/> when the condition stopped it.</returns>
    /// <exception cref="RuvioException">The key holds a value that is not JSON (<c>WRONGTYPE</c>), <paramref name="json"/> is not valid JSON, the path is not a supported simple path, or the document would pass 16 MiB.</exception>
    /// <remarks>Only existing intermediate nodes are created through; missing parents are an error.</remarks>
    /// <example>
    /// <code>
    /// bool created = await db.JsonSetAsync("user:1", "$.nick", "\"ada\"", SetMode.IfNotExists);
    /// </code>
    /// </example>
    ValueTask<bool> JsonSetAsync(string key, string path, string json, SetMode mode, CancellationToken cancellationToken = default)
        => RespRead.NotNull(ExecuteAsync(CoverageCommands.JsonSet(key, path, json, mode).AsList(), cancellationToken));

    /// <summary>Sends <c>JSON.OBJLEN</c>: the number of members of the object at <paramref name="path"/>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="path">A JSON path such as <c>$</c> (the whole document), <c>$.name</c> or <c>$.items[0]</c>. Only simple <c>.field</c> and <c>[N]</c> steps are supported.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The member count, or <see langword="null"/> when the key or path does not exist.</returns>
    /// <exception cref="RuvioException">The value at the path is not an object.</exception>
    /// <example>
    /// <code>
    /// long? members = await db.JsonObjectLengthAsync("user:1");
    /// </code>
    /// </example>
    ValueTask<long?> JsonObjectLengthAsync(string key, string path = "$", CancellationToken cancellationToken = default)
        => RespRead.OptionalInteger(ExecuteAsync(new RespCommand("JSON.OBJLEN", key, path).AsList(), cancellationToken), "JSON.OBJLEN");

    /// <summary>Sends <c>JSON.STRAPPEND</c>: appends to the JSON string at <paramref name="path"/>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="path">A path to a string value. Required here; there is no default.</param>
    /// <param name="json">A JSON string literal including its quotes, for example <c>"\" world\""</c>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The new string length in bytes, or <see langword="null"/> when the key or path does not exist.</returns>
    /// <exception cref="RuvioException">The value at the path is not a string, or <paramref name="json"/> is not a JSON string.</exception>
    /// <example>
    /// <code>
    /// long? length = await db.JsonStringAppendAsync("doc", "$.title", "\"!\"");
    /// </code>
    /// </example>
    ValueTask<long?> JsonStringAppendAsync(string key, string path, string json, CancellationToken cancellationToken = default)
        => RespRead.OptionalInteger(ExecuteAsync(new RespCommand("JSON.STRAPPEND", key, path, json).AsList(), cancellationToken), "JSON.STRAPPEND");

    /// <summary>Sends <c>JSON.NUMMULTBY</c>: multiplies the number at <paramref name="path"/> by a factor.</summary>
    /// <param name="key">The key.</param>
    /// <param name="path">A path to a number. Required here.</param>
    /// <param name="by">The factor.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The new number as JSON text, or <see langword="null"/> when the key or path does not exist.</returns>
    /// <exception cref="RuvioException">The value at the path is not a number, or the product is not finite.</exception>
    /// <remarks>Integers stay integers unless the product overflows.</remarks>
    /// <example>
    /// <code>
    /// string? price = await db.JsonMultiplyAsync("item:1", "$.price", 1.2);
    /// </code>
    /// </example>
    ValueTask<string?> JsonMultiplyAsync(string key, string path, double by, CancellationToken cancellationToken = default)
        => RespRead.Text(ExecuteAsync(new RespCommand("JSON.NUMMULTBY", key, path, RespRead.Number(by)).AsList(), cancellationToken));

    /// <summary>
    /// Sends <c>JSON.ARRINSERT</c>: inserts values into the array at <paramref name="path"/> before <paramref name="index"/>.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="path">A path to an array. Required here.</param>
    /// <param name="index">Position to insert at. A negative index counts from the end. It must lie within 0 to the array length.</param>
    /// <param name="json">One or more JSON values (each already JSON text).</param>
    /// <returns>The new array length, or <see langword="null"/> when the key or path does not exist.</returns>
    /// <exception cref="RuvioException">The value is not an array, or the index is outside the array.</exception>
    /// <example>
    /// <code>
    /// long? length = await db.JsonArrayInsertAsync("doc", "$.tags", 0, "\"first\"");
    /// </code>
    /// </example>
    ValueTask<long?> JsonArrayInsertAsync(string key, string path, long index, params string[] json)
        => RespRead.OptionalInteger(ExecuteAsync(CoverageCommands.JsonArrayInsert(key, path, index, json).AsList(), CancellationToken.None), "JSON.ARRINSERT");

    /// <summary>Sends <c>JSON.ARRPOP</c>: removes and returns one element of the array at <paramref name="path"/>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="path">A JSON path such as <c>$</c> (the whole document), <c>$.name</c> or <c>$.items[0]</c>. Only simple <c>.field</c> and <c>[N]</c> steps are supported.</param>
    /// <param name="index">Position to remove. Negative counts from the end; the default <c>-1</c> is the last element. An index outside the array is clamped.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The removed element as JSON text, or <see langword="null"/> when the array is empty or the key or path does not exist.</returns>
    /// <exception cref="RuvioException">The value at the path is not an array.</exception>
    /// <example>
    /// <code>
    /// string? last = await db.JsonArrayPopAsync("doc", "$.tags");
    /// </code>
    /// </example>
    ValueTask<string?> JsonArrayPopAsync(string key, string path = "$", long index = -1, CancellationToken cancellationToken = default)
        => RespRead.Text(ExecuteAsync(new RespCommand("JSON.ARRPOP", key, path, index).AsList(), cancellationToken));

    /// <summary>
    /// Sends <c>JSON.ARRTRIM</c>: keeps only the elements from <paramref name="start"/> to <paramref name="stop"/> of the array.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="path">A path to an array. Required here.</param>
    /// <param name="start">First index to keep (inclusive). Negative counts from the end.</param>
    /// <param name="stop">Last index to keep (inclusive). Negative counts from the end.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The new array length, or <see langword="null"/> when the key or path does not exist.</returns>
    /// <exception cref="RuvioException">The value at the path is not an array.</exception>
    /// <remarks>A reversed range or one past the end empties the array.</remarks>
    /// <example>
    /// <code>
    /// long? kept = await db.JsonArrayTrimAsync("doc", "$.log", -100, -1);   // last 100 entries
    /// </code>
    /// </example>
    ValueTask<long?> JsonArrayTrimAsync(string key, string path, long start, long stop, CancellationToken cancellationToken = default)
        => RespRead.OptionalInteger(ExecuteAsync(new RespCommand("JSON.ARRTRIM", key, path, start, stop).AsList(), cancellationToken), "JSON.ARRTRIM");

    /// <summary>Sends <c>JSON.ARRINDEX</c>: finds the first array element equal to a scalar.</summary>
    /// <param name="key">The key.</param>
    /// <param name="path">A path to an array. Required here.</param>
    /// <param name="scalar">The value to find, as JSON text. Only scalars (string, number, boolean, null) are supported. <c>1</c> and <c>1.0</c> are different values.</param>
    /// <param name="start">First index to search. Default 0.</param>
    /// <param name="stop">Last index to search; 0 means to the end of the array.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The index of the first match, <c>-1</c> when absent, or <see langword="null"/> when the key or path does not exist.</returns>
    /// <exception cref="RuvioException">The value at the path is not an array.</exception>
    /// <example>
    /// <code>
    /// long? at = await db.JsonArrayIndexAsync("doc", "$.tags", "\"new\"");
    /// </code>
    /// </example>
    ValueTask<long?> JsonArrayIndexAsync(string key, string path, string scalar, long start = 0, long stop = 0, CancellationToken cancellationToken = default)
        => RespRead.OptionalInteger(ExecuteAsync(CoverageCommands.JsonArrayIndex(key, path, scalar, start, stop).AsList(), cancellationToken), "JSON.ARRINDEX");

    /// <summary>
    /// Sends <c>JSON.FORGET</c>, an alias of <c>JSON.DEL</c>: removes the value at <paramref name="path"/>.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="path">The path to remove. The default <c>$</c> removes the whole key.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>1 when something was removed, 0 when the key or path did not exist.</returns>
    /// <exception cref="RuvioException">The key holds a value that is not JSON.</exception>
    /// <remarks>Same behaviour as <see cref="JsonDeleteAsync"/>.</remarks>
    /// <example>
    /// <code>
    /// long removed = await db.JsonForgetAsync("doc", "$.draft");
    /// </code>
    /// </example>
    ValueTask<long> JsonForgetAsync(string key, string path = "$", CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(new RespCommand("JSON.FORGET", key, path).AsList(), cancellationToken), "JSON.FORGET");

    /// <summary>
    /// Sends <c>JMERGE</c>, an alias of <c>JSON.MERGE</c>: applies an RFC 7396 merge patch at <paramref name="path"/>.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="path">A JSON path such as <c>$</c> (the whole document), <c>$.name</c> or <c>$.items[0]</c>. Only simple <c>.field</c> and <c>[N]</c> steps are supported.</param>
    /// <param name="patch">The merge patch as JSON text. A <c>null</c> member deletes that member.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when the patch was applied.</returns>
    /// <exception cref="RuvioException">The key holds a value that is not JSON, or the patch or path is invalid.</exception>
    /// <remarks>Same behaviour as <see cref="JsonMergeAsync"/>. This is a merge patch, not RFC 6902 JSON Patch.</remarks>
    /// <example>
    /// <code>
    /// await db.JMergeAsync("user:1", "$", "{\"nick\":null,\"age\":31}");
    /// </code>
    /// </example>
    ValueTask<bool> JMergeAsync(string key, string path, string patch, CancellationToken cancellationToken = default)
        => RespRead.NotNull(ExecuteAsync(new RespCommand("JMERGE", key, path, patch).AsList(), cancellationToken));

    /// <summary>
    /// Sends <c>JSON.INDEX CREATE name PREFIX prefix PATH path TYPE type</c>: defines an exact-match index over one scalar field of the JSON keys that start with a prefix. A Ruvio extension.
    /// </summary>
    /// <param name="name">The index name.</param>
    /// <param name="prefix">Only keys starting with this text are indexed.</param>
    /// <param name="path">The field to index, such as <c>$.email</c>. One scalar field per index.</param>
    /// <param name="type">The kind of value held at the field. Default <see cref="JsonIndexType.String"/>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <exception cref="RuvioException">The index already exists, or the arguments are invalid.</exception>
    /// <remarks>The index is built in the background; <see cref="JsonFindAsync"/> fails with <c>index is building</c> until the first build ends. It then follows every write, delete, expiry, rename and eviction. Indexes work in database 0 only, keep postings in memory, and are rebuilt at startup. The definition is stored as a reserved key that <c>KEYS</c> and <c>DBSIZE</c> can see; <c>FLUSHDB</c> and <c>FLUSHALL</c> on database 0 drop it.</remarks>
    /// <example>
    /// <code>
    /// await db.JsonCreateIndexAsync("by_email", "user:", "$.email");
    /// </code>
    /// </example>
    ValueTask JsonCreateIndexAsync(string name, string prefix, string path, JsonIndexType type = JsonIndexType.String, CancellationToken cancellationToken = default)
        => RespRead.Ok(ExecuteAsync(CoverageCommands.JsonCreateIndex(name, prefix, path, type).AsList(), cancellationToken));

    /// <summary>Sends <c>JSON.INDEX DROP name</c>.</summary>
    /// <param name="name">The index name.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when the index existed and was dropped; <see langword="false"/> otherwise.</returns>
    /// <example>
    /// <code>
    /// bool dropped = await db.JsonDropIndexAsync("by_email");
    /// </code>
    /// </example>
    ValueTask<bool> JsonDropIndexAsync(string name, CancellationToken cancellationToken = default)
        => RespRead.Flag(ExecuteAsync(new RespCommand("JSON.INDEX", "DROP", name).AsList(), cancellationToken), "JSON.INDEX");

    /// <summary>Sends <c>JSON.INDEX LIST</c>.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>One text line per index and shard, as the server prints them.</returns>
    /// <example>
    /// <code>
    /// foreach (string line in await db.JsonListIndexesAsync()) Console.WriteLine(line);
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> JsonListIndexesAsync(CancellationToken cancellationToken = default)
        => RespRead.Strings(ExecuteAsync(new RespCommand("JSON.INDEX", "LIST").AsList(), cancellationToken), "JSON.INDEX");

    /// <summary>
    /// Sends <c>JSON.FIND index VALUE value [COUNT n] [AFTER key]</c>: lists the keys whose indexed field equals a value. A Ruvio extension.
    /// </summary>
    /// <param name="index">The index name from <see cref="JsonCreateIndexAsync"/>.</param>
    /// <param name="value">The value to match, as text. Matching is exact equality.</param>
    /// <param name="count">Maximum keys to return, 1 to 1000. Null uses the server default.</param>
    /// <param name="after">Paging: return only keys after this one in ascending order. Pass the last key of the previous page.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The matching keys in ascending order. Empty when nothing matches.</returns>
    /// <exception cref="RuvioException">The index does not exist, or it is still building (<c>index is building</c>).</exception>
    /// <remarks>Runs on every shard and merges the results.</remarks>
    /// <example>
    /// <code>
    /// IReadOnlyList&lt;string&gt; keys = await db.JsonFindAsync("by_email", "ada@example.com", count: 10);
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<string>> JsonFindAsync(string index, string value, int? count = null, string? after = null, CancellationToken cancellationToken = default)
        => RespRead.Strings(ExecuteAsync(CoverageCommands.JsonFind(index, value, count, after).AsList(), cancellationToken), "JSON.FIND");

    /// <summary>
    /// Sends <c>RUVIO</c>, a Ruvio-only command with no arguments that answers <c>PONG</c>. Use it to tell a Ruvio server from plain Redis.
    /// </summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><c>"PONG"</c>.</returns>
    /// <exception cref="RuvioException">Any other server answers with an unknown-command error.</exception>
    /// <example>
    /// <code>
    /// string reply = await db.RuvioAsync();
    /// </code>
    /// </example>
    ValueTask<string> RuvioAsync(CancellationToken cancellationToken = default)
        => RespRead.RequiredText(ExecuteAsync(new RespCommand("RUVIO").AsList(), cancellationToken), "RUVIO");

    /// <summary>Sends <c>RUVIO OWNER</c>: asks which shard rule the server uses to own keys.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The server's description of the shard rule, as text.</returns>
    /// <example>
    /// <code>
    /// string owner = await db.GetRuvioOwnerAsync();
    /// </code>
    /// </example>
    ValueTask<string> GetRuvioOwnerAsync(CancellationToken cancellationToken = default)
        => RespRead.RequiredText(ExecuteAsync(new RespCommand("RUVIO", "OWNER").AsList(), cancellationToken), "RUVIO");

    /// <summary>Sends <c>RUVIO MOVESLOTS first-last shard</c>: moves the hash slots <paramref name="first"/> to <paramref name="last"/> to <paramref name="shard"/> (counted from 0) of the same server, which keeps serving them meanwhile. Completes once the move has started; <see cref="GetMoveSlotsStatusAsync"/> follows it. One move runs at a time, and every slot of the range must be on one shard.</summary>
    /// <param name="first">First hash slot of the range (0-16383).</param>
    /// <param name="last">Last hash slot of the range, inclusive.</param>
    /// <param name="shard">Shard of the same server to move the range to.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <exception cref="RuvioException">The range is already on that shard, spans shards, another move runs, the server has one shard, or replication is on.</exception>
    /// <example>
    /// <code>
    /// await db.MoveSlotsAsync(0, 1000, 2);
    /// </code>
    /// </example>
    ValueTask MoveSlotsAsync(int first, int last, int shard, CancellationToken cancellationToken = default)
        => RespRead.Ok(ExecuteAsync(new RespCommand("RUVIO", "MOVESLOTS", RespRead.Number(first) + "-" + RespRead.Number(last), RespRead.Number(shard)).AsList(), cancellationToken));

    /// <summary>Sends <c>RUVIO MOVESLOTS STATUS</c>: the running or last slot move. <c>state</c> is <c>none</c>, <c>copying</c>, <c>cleaning</c>, <c>cancelling</c>, <c>done</c> or <c>cancelled</c>; the other names are <c>first</c>, <c>last</c>, <c>from</c>, <c>to</c>, <c>keys</c>, <c>rounds</c>, <c>elapsed_ms</c>, <c>freeze_ms</c> and <c>error</c>.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>Name to value, as text.</returns>
    /// <example>
    /// <code>
    /// var status = await db.GetMoveSlotsStatusAsync();
    /// </code>
    /// </example>
    ValueTask<IReadOnlyDictionary<string, string>> GetMoveSlotsStatusAsync(CancellationToken cancellationToken = default)
        => RespRead.Map(ExecuteAsync(new RespCommand("RUVIO", "MOVESLOTS", "STATUS").AsList(), cancellationToken), "RUVIO");

    /// <summary>Sends <c>COMMAND</c>: lists the command table.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The raw reply, one entry per command (name, arity, flags and positions of the first and last key).</returns>
    /// <remarks>Entries use the Redis 7 shape with empty tips, key specs and subcommands. Key positions of Ruvio-native commands are informational.</remarks>
    /// <example>
    /// <code>
    /// RespValue table = await db.GetCommandsAsync();
    /// </code>
    /// </example>
    ValueTask<RespValue> GetCommandsAsync(CancellationToken cancellationToken = default)
        => ExecuteAsync(new RespCommand("COMMAND").AsList(), cancellationToken);

    /// <summary>Sends <c>COMMAND COUNT</c>.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The number of commands the server knows.</returns>
    /// <example>
    /// <code>
    /// long n = await db.GetCommandCountAsync();
    /// </code>
    /// </example>
    ValueTask<long> GetCommandCountAsync(CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(new RespCommand("COMMAND", "COUNT").AsList(), cancellationToken), "COMMAND");

    /// <summary>Sends <c>COMMAND INFO name...</c>.</summary>
    /// <param name="names">Command names, for example <c>"get"</c>. With none, the server lists all commands.</param>
    /// <returns>The raw reply: one entry per name, null for an unknown command.</returns>
    /// <remarks>Entries use the Redis 7 shape with empty tips, key specs and subcommands.</remarks>
    /// <example>
    /// <code>
    /// RespValue info = await db.GetCommandInfoAsync("get", "set");
    /// </code>
    /// </example>
    ValueTask<RespValue> GetCommandInfoAsync(params string[] names)
        => ExecuteAsync(CoverageCommands.Words("COMMAND", "INFO", names).AsList(), CancellationToken.None);

    /// <summary>Sends <c>COMMAND DOCS name...</c>.</summary>
    /// <param name="names">Command names.</param>
    /// <returns>The raw reply. Ruvio returns empty documentation maps.</returns>
    /// <example>
    /// <code>
    /// RespValue docs = await db.GetCommandDocsAsync("get");
    /// </code>
    /// </example>
    ValueTask<RespValue> GetCommandDocsAsync(params string[] names)
        => ExecuteAsync(CoverageCommands.Words("COMMAND", "DOCS", names).AsList(), CancellationToken.None);

    /// <summary>Sends <c>LATENCY LATEST</c>.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The latency events, as raw reply items. Ruvio records no latency-spike events, so the list is always empty. Use <c>INFO latencystats</c> instead.</returns>
    /// <example>
    /// <code>
    /// var events = await db.GetLatencyLatestAsync();   // always empty
    /// </code>
    /// </example>
    ValueTask<IReadOnlyList<RespValue>> GetLatencyLatestAsync(CancellationToken cancellationToken = default)
        => RuvioClient.ItemsOf(ExecuteAsync(new RespCommand("LATENCY", "LATEST").AsList(), cancellationToken));

    /// <summary>
    /// Sends <c>WAIT replicas timeout</c>: blocks until replicas have synced this shard's last write to disk, or the timeout passes.
    /// </summary>
    /// <param name="replicas">How many replicas must have synced.</param>
    /// <param name="timeout">How long to wait, in whole milliseconds. <see cref="TimeSpan.Zero"/> waits until shutdown.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>How many replicas reached the write. A server with replication off, or a replica, answers 0.</returns>
    /// <remarks>Blocks only the calling client. A missing replica does not drop the write. Not allowed inside a transaction or script.</remarks>
    /// <example>
    /// <code>
    /// long synced = await db.WaitForReplicasAsync(1, TimeSpan.FromSeconds(1));
    /// </code>
    /// </example>
    ValueTask<long> WaitForReplicasAsync(long replicas, TimeSpan timeout, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(CoverageCommands.Wait(replicas, timeout).AsList(), cancellationToken), "WAIT");

    /// <summary>Sends <c>AUTH password</c> for the default user.</summary>
    /// <param name="password">The password.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <exception cref="RuvioException">The password is wrong, or the server needs no password.</exception>
    /// <remarks>Authentication belongs to the connection, which the whole client shares. Prefer setting the credentials in <see cref="RuvioClientOptions"/> so reconnects authenticate again.</remarks>
    /// <example>
    /// <code>
    /// await db.AuthenticateAsync("secret");
    /// </code>
    /// </example>
    ValueTask AuthenticateAsync(string password, CancellationToken cancellationToken = default)
        => RespRead.Ok(ExecuteAsync(new RespCommand("AUTH", password).AsList(), cancellationToken));

    /// <summary>Sends <c>AUTH username password</c> for an ACL user.</summary>
    /// <param name="username">The ACL user name.</param>
    /// <param name="password">The user's password.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <exception cref="RuvioException">The credentials are wrong.</exception>
    /// <remarks>Authentication belongs to the connection, which the whole client shares. Prefer setting the credentials in <see cref="RuvioClientOptions"/> so reconnects authenticate again.</remarks>
    /// <example>
    /// <code>
    /// await db.AuthenticateAsync("app", "secret");
    /// </code>
    /// </example>
    ValueTask AuthenticateAsync(string username, string password, CancellationToken cancellationToken = default)
        => RespRead.Ok(ExecuteAsync(new RespCommand("AUTH", username, password).AsList(), cancellationToken));

    /// <summary>Sends <c>CLIENT KILL ADDR address</c>: closes the client connections with that remote address.</summary>
    /// <param name="address">The client address as <c>ip:port</c>, as shown by <see cref="GetClientListAsync(CancellationToken)"/>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The number of connections closed.</returns>
    /// <remarks>Each shard knows only its own connections. With several shards the command reaches the shard that got it.</remarks>
    /// <example>
    /// <code>
    /// long closed = await db.KillClientAsync("10.0.0.5:51234");
    /// </code>
    /// </example>
    ValueTask<long> KillClientAsync(string address, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(new RespCommand("CLIENT", "KILL", "ADDR", address).AsList(), cancellationToken), "CLIENT");

    /// <summary>
    /// Same as <see cref="BeginIdempotencyAsync"/> but takes the retention as a <see cref="TimeSpan"/>. Sends <c>IDEM key BEGIN fingerprint owner ttl_ms</c>.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="fingerprint">A token (1 to 128 bytes) that identifies the request content. A retry with another fingerprint is a conflict.</param>
    /// <param name="owner">A token (1 to 128 bytes) for the caller that does the work.</param>
    /// <param name="ttl">How long the record lives, from 1 ms to 7 days. It is not renewed by retries or completion.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>An <see cref="IdempotencyBegin"/>: <c>Acquired</c> to run the work (then call <see cref="CompleteIdempotencyAsync"/>), <c>Pending</c> while another owner runs it, <c>Conflict</c> for a different fingerprint, or <c>Completed</c> with the stored result.</returns>
    /// <exception cref="RuvioException">The token or retention is out of range, or the key holds a non-IDEM value.</exception>
    /// <remarks>Not exactly-once for external effects. Pair it with <see cref="CompleteIdempotencyAsync"/>.</remarks>
    /// <example>
    /// <code>
    /// var begin = await db.TryBeginIdempotencyAsync("pay:42", "sha256:ab", "worker-1", TimeSpan.FromMinutes(10));
    /// </code>
    /// </example>
    ValueTask<IdempotencyBegin> TryBeginIdempotencyAsync(string key, string fingerprint, string owner, TimeSpan ttl, CancellationToken cancellationToken = default)
        => RespRead.Idempotency(ExecuteAsync(new RespCommand("IDEM", key, "BEGIN", fingerprint, owner, RespRead.Milliseconds(ttl)).AsList(), cancellationToken));
}
