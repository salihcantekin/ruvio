// Default members for the deadline that is set only when a command creates its key (EX / PX), and for the
// EXPIRE family with a condition. An IRuvioClient implementation that only implements
// ExecuteAsync(IReadOnlyList<string>, ...) still gets every member below; RuvioClient overrides each one
// with the inline (allocation-free argument) path.

namespace Ruvio.Client;

public partial interface IRuvioClient
{
    /// <summary>
    /// Sends <c>EXPIRE key seconds condition</c> (or <c>PEXPIRE</c> for a sub-second part): sets a time to live on an existing key, only when the key's current deadline meets <paramref name="condition"/>.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="ttl">Time to live from now, rounded up to milliseconds. Negative throws <see cref="ArgumentOutOfRangeException"/>.</param>
    /// <param name="condition"><see cref="ExpireCondition.Nx"/> (no deadline yet), <see cref="ExpireCondition.Xx"/> (a deadline exists), <see cref="ExpireCondition.Gt"/> (new deadline later) or <see cref="ExpireCondition.Lt"/> (new deadline earlier). A key without a deadline counts as infinite for <c>GT</c> and <c>LT</c>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when the TTL was set; <see langword="false"/> when the key does not exist or the condition failed.</returns>
    /// <example>
    /// <code>
    /// bool extended = await db.ExpireAsync("session:ada", TimeSpan.FromMinutes(30), ExpireCondition.Gt);
    /// </code>
    /// </example>
    ValueTask<bool> ExpireAsync(string key, TimeSpan ttl, ExpireCondition condition, CancellationToken cancellationToken = default)
        => RespRead.Flag(ExecuteAsync(TtlCommands.Expire(key, ttl, condition).AsList(), cancellationToken), "EXPIRE");

    /// <summary>
    /// Sends <c>EXPIREAT key unixSeconds condition</c> (or <c>PEXPIREAT</c> for a sub-second part): sets an absolute deadline, only when the key's current deadline meets <paramref name="condition"/>.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="when">The point in time the key expires.</param>
    /// <param name="condition"><see cref="ExpireCondition.Nx"/>, <see cref="ExpireCondition.Xx"/>, <see cref="ExpireCondition.Gt"/> or <see cref="ExpireCondition.Lt"/>. A key without a deadline counts as infinite for <c>GT</c> and <c>LT</c>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when the deadline was set; <see langword="false"/> when the key does not exist or the condition failed.</returns>
    /// <example>
    /// <code>
    /// bool ok = await db.ExpireAtAsync("promo:summer", new DateTimeOffset(2027, 9, 1, 0, 0, 0, TimeSpan.Zero), ExpireCondition.Nx);
    /// </code>
    /// </example>
    ValueTask<bool> ExpireAtAsync(string key, DateTimeOffset when, ExpireCondition condition, CancellationToken cancellationToken = default)
        => RespRead.Flag(ExecuteAsync(TtlCommands.ExpireAt(key, when, condition).AsList(), cancellationToken), "EXPIREAT");

    /// <summary>Sends <c>INCR key EX|PX ttl</c>: like <see cref="IncrementAsync(string, CancellationToken)"/>, and when the call creates the key it also sets a time to live.</summary>
    /// <param name="key">The key.</param>
    /// <param name="ttl">Time to live for a key this call creates. Whole seconds are sent as <c>EX</c>, otherwise <c>PX</c> in milliseconds (rounded up). Must be positive. An existing key keeps its own deadline.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The value after the increment.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="ttl"/> is zero or negative.</exception>
    /// <exception cref="RuvioException">The value is not an integer or the result overflows 64 bits; <c>WRONGTYPE</c> for another type.</exception>
    /// <example>
    /// <code>
    /// long hits = await db.IncrementAsync("rate:ada", TimeSpan.FromSeconds(60));
    /// </code>
    /// </example>
    ValueTask<long> IncrementAsync(string key, TimeSpan ttl, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(TtlCommands.Increment(key, ttl).AsList(), cancellationToken), "INCR");

    /// <summary>Sends <c>DECR key EX|PX ttl</c>: like <see cref="DecrementAsync(string, CancellationToken)"/>, and when the call creates the key it also sets a time to live.</summary>
    /// <param name="key">The key.</param>
    /// <param name="ttl">Time to live for a key this call creates (<c>EX</c> for whole seconds, otherwise <c>PX</c>). Must be positive. An existing key keeps its own deadline.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The value after the decrement.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="ttl"/> is zero or negative.</exception>
    /// <exception cref="RuvioException">The value is not an integer or the result overflows 64 bits; <c>WRONGTYPE</c> for another type.</exception>
    /// <example>
    /// <code>
    /// long left = await db.DecrementAsync("stock:sku-42", TimeSpan.FromHours(1));
    /// </code>
    /// </example>
    ValueTask<long> DecrementAsync(string key, TimeSpan ttl, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(TtlCommands.Decrement(key, ttl).AsList(), cancellationToken), "DECR");

    /// <summary>Sends <c>INCRBY key delta EX|PX ttl</c>: like <see cref="IncrementByAsync(string, long, CancellationToken)"/>, and when the call creates the key it also sets a time to live.</summary>
    /// <param name="key">The key.</param>
    /// <param name="delta">The amount to add. Negative subtracts.</param>
    /// <param name="ttl">Time to live for a key this call creates (<c>EX</c> for whole seconds, otherwise <c>PX</c>). Must be positive. An existing key keeps its own deadline.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The value after the increment.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="ttl"/> is zero or negative.</exception>
    /// <exception cref="RuvioException">The value is not an integer or the result overflows 64 bits; <c>WRONGTYPE</c> for another type.</exception>
    /// <example>
    /// <code>
    /// long total = await db.IncrementByAsync("bytes:ada", 1024, TimeSpan.FromMinutes(5));
    /// </code>
    /// </example>
    ValueTask<long> IncrementByAsync(string key, long delta, TimeSpan ttl, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(TtlCommands.IncrementBy(key, delta, ttl).AsList(), cancellationToken), "INCRBY");

    /// <summary>Sends <c>DECRBY key delta EX|PX ttl</c>: like <see cref="DecrementByAsync(string, long, CancellationToken)"/>, and when the call creates the key it also sets a time to live.</summary>
    /// <param name="key">The key.</param>
    /// <param name="delta">The amount to subtract.</param>
    /// <param name="ttl">Time to live for a key this call creates (<c>EX</c> for whole seconds, otherwise <c>PX</c>). Must be positive. An existing key keeps its own deadline.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The value after the decrement.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="ttl"/> is zero or negative.</exception>
    /// <exception cref="RuvioException">The value is not an integer or the result overflows 64 bits; <c>WRONGTYPE</c> for another type.</exception>
    /// <example>
    /// <code>
    /// long left = await db.DecrementByAsync("credits:42", 5, TimeSpan.FromDays(1));
    /// </code>
    /// </example>
    ValueTask<long> DecrementByAsync(string key, long delta, TimeSpan ttl, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(TtlCommands.DecrementBy(key, delta, ttl).AsList(), cancellationToken), "DECRBY");

    /// <summary>Sends <c>INCRBY key delta EX|PX ttl MAX limit</c>: like <see cref="IncrementWithLimitAsync(string, long, long, CancellationToken)"/>, and when the call creates the key it also sets a time to live.</summary>
    /// <param name="key">The key.</param>
    /// <param name="delta">The amount to add.</param>
    /// <param name="limit">The highest value the counter may reach.</param>
    /// <param name="ttl">Time to live for a key this call creates (<c>EX</c> for whole seconds, otherwise <c>PX</c>). Must be positive. An existing key keeps its own deadline.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The new value, or <see langword="null"/> when the increment would pass <paramref name="limit"/>.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="ttl"/> is zero or negative.</exception>
    /// <exception cref="RuvioException">The value is not an integer; <c>WRONGTYPE</c> for another type.</exception>
    /// <example>
    /// <code>
    /// long? seats = await db.IncrementWithLimitAsync("event:7:booked", 1, 100, TimeSpan.FromDays(7));
    /// </code>
    /// </example>
    ValueTask<long?> IncrementWithLimitAsync(string key, long delta, long limit, TimeSpan ttl, CancellationToken cancellationToken = default)
        => RespRead.OptionalInteger(ExecuteAsync(TtlCommands.IncrementWithLimit(key, delta, limit, ttl).AsList(), cancellationToken), "INCRBY");

    /// <summary>Sends <c>INCRBYFLOAT key delta EX|PX ttl</c>: like <see cref="IncrementByFloatAsync(string, double, CancellationToken)"/>, and when the call creates the key it also sets a time to live.</summary>
    /// <param name="key">The key.</param>
    /// <param name="delta">The amount to add. Negative subtracts.</param>
    /// <param name="ttl">Time to live for a key this call creates (<c>EX</c> for whole seconds, otherwise <c>PX</c>). Must be positive. An existing key keeps its own deadline.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The new value.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="ttl"/> is zero or negative.</exception>
    /// <exception cref="RuvioException">The value is not a number, or the result is not finite; <c>WRONGTYPE</c> for another type.</exception>
    /// <example>
    /// <code>
    /// double total = await db.IncrementByFloatAsync("balance", 2.5, TimeSpan.FromHours(1));
    /// </code>
    /// </example>
    ValueTask<double> IncrementByFloatAsync(string key, double delta, TimeSpan ttl, CancellationToken cancellationToken = default)
        => RespRead.Double(ExecuteAsync(TtlCommands.IncrementByFloat(key, delta, ttl).AsList(), cancellationToken), "INCRBYFLOAT");

    /// <summary>Sends <c>HINCRBY key field delta EX|PX ttl</c>: like <see cref="IncrementHashFieldAsync(string, string, long, CancellationToken)"/>, and when the call creates the hash it also sets a time to live on the key.</summary>
    /// <param name="key">The key.</param>
    /// <param name="field">The field.</param>
    /// <param name="delta">The amount to add. Negative subtracts.</param>
    /// <param name="ttl">Time to live for a key this call creates (<c>EX</c> for whole seconds, otherwise <c>PX</c>). Must be positive. An existing key keeps its own deadline.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The field value after the increment.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="ttl"/> is zero or negative.</exception>
    /// <exception cref="RuvioException">The field is not an integer or the result overflows 64 bits; <c>WRONGTYPE</c> for another type.</exception>
    /// <example>
    /// <code>
    /// long hits = await db.IncrementHashFieldAsync("stats", "hits", 1, TimeSpan.FromMinutes(10));
    /// </code>
    /// </example>
    ValueTask<long> IncrementHashFieldAsync(string key, string field, long delta, TimeSpan ttl, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteAsync(TtlCommands.IncrementHashField(key, field, delta, ttl).AsList(), cancellationToken), "HINCRBY");

    /// <summary>Sends <c>ZINCRBY key delta member EX|PX ttl</c>: like <see cref="IncrementSortedSetAsync(string, string, double, CancellationToken)"/>, and when the call creates the sorted set it also sets a time to live.</summary>
    /// <param name="key">The key.</param>
    /// <param name="member">The member.</param>
    /// <param name="delta">The amount to add to the score. Negative subtracts.</param>
    /// <param name="ttl">Time to live for a key this call creates (<c>EX</c> for whole seconds, otherwise <c>PX</c>). Must be positive. An existing key keeps its own deadline.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The new score.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="ttl"/> is zero or negative.</exception>
    /// <exception cref="RuvioException"><c>WRONGTYPE</c> for another type, or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// double score = await db.IncrementSortedSetAsync("board:daily", "ada", 10, TimeSpan.FromDays(1));
    /// </code>
    /// </example>
    ValueTask<double> IncrementSortedSetAsync(string key, string member, double delta, TimeSpan ttl, CancellationToken cancellationToken = default)
        => RespRead.Double(ExecuteAsync(TtlCommands.IncrementSortedSet(key, member, delta, ttl).AsList(), cancellationToken), "ZINCRBY");

    /// <summary>Sends <c>SETBIT key offset bit EX|PX ttl</c>: like <see cref="SetBitAsync(string, long, bool, CancellationToken)"/>, and when the call creates the key it also sets a time to live.</summary>
    /// <param name="key">The key.</param>
    /// <param name="offset">Bit position. Bit 0 is the most significant bit of the first byte.</param>
    /// <param name="value">The new bit.</param>
    /// <param name="ttl">Time to live for a key this call creates (<c>EX</c> for whole seconds, otherwise <c>PX</c>). Must be positive. An existing key keeps its own deadline.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns>The bit that was there before.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="ttl"/> is zero or negative.</exception>
    /// <exception cref="RuvioException">The offset is negative or beyond the 16 MiB limit; <c>WRONGTYPE</c> for another type.</exception>
    /// <example>
    /// <code>
    /// bool was = await db.SetBitAsync("online:today", 1234, true, TimeSpan.FromDays(1));
    /// </code>
    /// </example>
    ValueTask<bool> SetBitAsync(string key, long offset, bool value, TimeSpan ttl, CancellationToken cancellationToken = default)
        => RespRead.Flag(ExecuteAsync(TtlCommands.SetBit(key, offset, value, ttl).AsList(), cancellationToken), "SETBIT");

    /// <summary>Sends <c>ZADD key EX|PX ttl [NX|XX] [GT|LT] [CH] score member ...</c>: like <see cref="AddSortedSetAsync(string, SortedSetAddFlags, SortedSetEntry[])"/>, and when the call creates the sorted set it also sets a time to live.</summary>
    /// <param name="key">The key.</param>
    /// <param name="ttl">Time to live for a key this call creates (<c>EX</c> for whole seconds, otherwise <c>PX</c>). Must be positive. An existing key keeps its own deadline.</param>
    /// <param name="flags">Combination of <see cref="SortedSetAddFlags"/>; <see cref="SortedSetAddFlags.None"/> adds or updates every member.</param>
    /// <param name="entries">The members and scores.</param>
    /// <returns>The number of new members; with <see cref="SortedSetAddFlags.Changed"/> new plus changed members.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="ttl"/> is zero or negative.</exception>
    /// <exception cref="RuvioException">The server refuses the flag combination; <c>WRONGTYPE</c> for another type.</exception>
    /// <example>
    /// <code>
    /// await db.AddSortedSetAsync("board:daily", TimeSpan.FromDays(1), SortedSetAddFlags.None, new SortedSetEntry("ada", 120));
    /// </code>
    /// </example>
    ValueTask<long> AddSortedSetAsync(string key, TimeSpan ttl, SortedSetAddFlags flags, params SortedSetEntry[] entries)
        => RespRead.Integer(ExecuteAsync(TtlCommands.SortedSetAdd(key, ttl, flags, entries).AsList(), CancellationToken.None), "ZADD");

    /// <summary>Tuple form of <see cref="AddSortedSetAsync(string, TimeSpan, SortedSetAddFlags, SortedSetEntry[])"/>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="ttl">Time to live for a key this call creates (<c>EX</c> for whole seconds, otherwise <c>PX</c>). Must be positive. An existing key keeps its own deadline.</param>
    /// <param name="flags">Combination of <see cref="SortedSetAddFlags"/>.</param>
    /// <param name="entries">The members and scores.</param>
    /// <returns>The number of new members; with <see cref="SortedSetAddFlags.Changed"/> new plus changed members.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="ttl"/> is zero or negative.</exception>
    /// <exception cref="RuvioException">The server refuses the flag combination; <c>WRONGTYPE</c> for another type.</exception>
    /// <example>
    /// <code>
    /// await db.AddSortedSetAsync("board:daily", TimeSpan.FromDays(1), SortedSetAddFlags.None, ("ada", 120));
    /// </code>
    /// </example>
    ValueTask<long> AddSortedSetAsync(string key, TimeSpan ttl, SortedSetAddFlags flags, params (string Member, double Score)[] entries)
        => AddSortedSetAsync(key, ttl, flags, Array.ConvertAll(entries, entry => new SortedSetEntry(entry.Member, entry.Score)));

    /// <summary>Sends <c>GEOADD key EX|PX ttl longitude latitude member ...</c>: like <see cref="AddGeoAsync(string, ValueTuple{double, double, string}[])"/>, and when the call creates the key it also sets a time to live.</summary>
    /// <param name="key">The key.</param>
    /// <param name="ttl">Time to live for a key this call creates (<c>EX</c> for whole seconds, otherwise <c>PX</c>). Must be positive. An existing key keeps its own deadline.</param>
    /// <param name="places">The places: longitude, latitude and member name.</param>
    /// <returns>The number of new members.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="ttl"/> is zero or negative.</exception>
    /// <exception cref="RuvioException">A coordinate is out of range; <c>WRONGTYPE</c> for another type.</exception>
    /// <example>
    /// <code>
    /// await db.AddGeoAsync("couriers", TimeSpan.FromMinutes(15), (32.85, 39.92, "ada"));
    /// </code>
    /// </example>
    ValueTask<long> AddGeoAsync(string key, TimeSpan ttl, params (double Longitude, double Latitude, string Member)[] places)
        => RespRead.Integer(ExecuteAsync(TtlCommands.GeoAdd(key, ttl, places).AsList(), CancellationToken.None), "GEOADD");

    /// <summary>Sends <c>XADD key EX|PX ttl id field value ...</c>: like <see cref="AddStreamEntryAsync(string, string, ValueTuple{string, string}[])"/>, and when the call creates the stream it also sets a time to live.</summary>
    /// <param name="key">The key.</param>
    /// <param name="ttl">Time to live for a stream this call creates (<c>EX</c> for whole seconds, otherwise <c>PX</c>). Must be positive. An existing stream keeps its own deadline.</param>
    /// <param name="id">Entry id: <c>*</c>, <c>ms-*</c> or explicit <c>ms-seq</c>.</param>
    /// <param name="fields">The field/value pairs of the entry; at least one.</param>
    /// <returns>The id of the new entry.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="ttl"/> is zero or negative.</exception>
    /// <exception cref="RuvioException">An explicit id is not greater than the last id; wrongtype.</exception>
    /// <example>
    /// <code>
    /// string id = await db.AddStreamEntryAsync("events:today", TimeSpan.FromDays(1), "*", ("type", "login"));
    /// </code>
    /// </example>
    ValueTask<string> AddStreamEntryAsync(string key, TimeSpan ttl, string id, params (string Field, string Value)[] fields)
        => RespRead.RequiredText(ExecuteAsync(TtlCommands.StreamAdd(key, ttl, id, fields).AsList(), CancellationToken.None), "XADD");

    /// <summary>Sends <c>JSON.SET key path value [NX|XX] EX|PX ttl</c>: like <see cref="JsonSetAsync(string, string, string, SetMode, CancellationToken)"/>, and when the call creates the key it also sets a time to live.</summary>
    /// <param name="key">The key.</param>
    /// <param name="path">The path; <c>$</c> for the whole document.</param>
    /// <param name="json">The JSON text to store.</param>
    /// <param name="mode"><see cref="SetMode.Always"/>, <see cref="SetMode.IfNotExists"/> (<c>NX</c>) or <see cref="SetMode.IfExists"/> (<c>XX</c>).</param>
    /// <param name="ttl">Time to live for a key this call creates (<c>EX</c> for whole seconds, otherwise <c>PX</c>). Must be positive. An existing key keeps its own deadline.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply with an <see cref="OperationCanceledException"/>. A command that was already sent still runs on the server.</param>
    /// <returns><see langword="true"/> when written; <see langword="false"/> when <c>NX</c> or <c>XX</c> skipped the write.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="ttl"/> is zero or negative.</exception>
    /// <exception cref="RuvioException">The JSON is invalid, or the key holds a value of another type (<c>WRONGTYPE</c>).</exception>
    /// <example>
    /// <code>
    /// await db.JsonSetAsync("session:ada", "$", """{"user":"ada"}""", SetMode.IfNotExists, TimeSpan.FromMinutes(30));
    /// </code>
    /// </example>
    ValueTask<bool> JsonSetAsync(string key, string path, string json, SetMode mode, TimeSpan ttl, CancellationToken cancellationToken = default)
        => RespRead.NotNull(ExecuteAsync(TtlCommands.JsonSet(key, path, json, mode, ttl).AsList(), cancellationToken));
}
