// client_coverage: APPEND DEL EVAL EXEC EXPIRE GET HGET HGETALL HSET INCR
// LPUSH LRANGE MGET MSET MULTI PING SADD SET SMEMBERS STRLEN TTL ZADD ZRANGE

using System.Globalization;
using System.Runtime.CompilerServices;

namespace Ruvio.Client;

public sealed partial class RuvioClient
{
    /// <inheritdoc />
    public ValueTask<RespValue> BeginIdempotencyAsync(string key, string fingerprint, string owner, long ttlMs,
        CancellationToken cancellationToken = default)
        => ExecuteCommandAsync(new RespCommand("IDEM", key, "BEGIN", fingerprint, owner, ttlMs), cancellationToken);

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<bool> CompleteIdempotencyAsync(string key, string fingerprint, string owner, string result,
        CancellationToken cancellationToken = default)
    {
        var reply = await ExecuteCommandAsync(new RespCommand("IDEM", key, "COMPLETE", fingerprint, owner, result), cancellationToken)
            .ConfigureAwait(false);

        if (reply.Kind != RespKind.Integer || reply.Integer is < 0 or > 1)
            throw new RuvioException("Invalid idempotency completion response.");

        return reply.Integer == 1;
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<long?> LimitAsync(
        string key,
        long max,
        long windowMs,
        CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(
            new RespCommand("LIMIT", key, max, windowMs),
            cancellationToken);

        return reply.Kind == RespKind.Null ? null : reply.Integer;
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<long?> LimitSlidingAsync(
        string key,
        long max,
        long windowMs,
        CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(
            new RespCommand("LIMIT", key, max, windowMs, "SLIDING"),
            cancellationToken);

        return reply.Kind == RespKind.Null ? null : reply.Integer;
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<long?> LimitTokensAsync(
        string key,
        long capacity,
        long refillMs,
        long cost = 1,
        CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(
            new RespCommand("LIMIT", key, capacity, refillMs, "TOKENS", "COST").With(cost),
            cancellationToken);

        return reply.Kind == RespKind.Null ? null : reply.Integer;
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<LimitOutcome> LimitSlidingWithRetryAsync(
        string key,
        long max,
        long windowMs,
        CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(
            new RespCommand("LIMIT", key, max, windowMs, "SLIDING", "WITHRETRY"),
            cancellationToken);

        return ReadLimitOutcome(reply);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<LimitOutcome> LimitTokensWithRetryAsync(
        string key,
        long capacity,
        long refillMs,
        long cost = 1,
        CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(
            new RespCommand("LIMIT", key, capacity, refillMs, "TOKENS", "COST").With(cost).With("WITHRETRY"),
            cancellationToken);

        return ReadLimitOutcome(reply);
    }

    private static LimitOutcome ReadLimitOutcome(RespValue reply)
    {
        if (reply.Items is not { Count: 2 } items
            || items[0].Kind != RespKind.Integer
            || items[1].Kind != RespKind.Integer)
        {
            throw new RuvioException("Invalid LIMIT WITHRETRY response.");
        }

        long remaining = items[0].Integer;

        return new LimitOutcome(remaining >= 0, Math.Max(remaining, 0), Math.Max(items[1].Integer, 0));
    }

    /// <inheritdoc />
    public ValueTask<long> OnceAsync(string key, long ttlMs, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("ONCE", key, ttlMs), cancellationToken);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<long?> TakeAsync(string key, long count, CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("TAKE", key, count), cancellationToken);

        return reply.Kind == RespKind.Null ? null : reply.Integer;
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<(long Source, long Destination)?> TransferAsync(string source, string destination, long amount, long? max = null, CancellationToken cancellationToken = default)
    {
        var command = new RespCommand("TRANSFER", source, destination, amount);

        if (max is { } cap)
        {
            command = command.With("MAX").With(cap);
        }

        return BalancePair("TRANSFER", await ExecuteCommandAsync(command, cancellationToken));
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<(long From, long To)?> TransferHashFieldAsync(string key, string from, string to, long amount, long? max = null, CancellationToken cancellationToken = default)
    {
        var command = new RespCommand("HTRANSFER", key, from, to, amount);

        if (max is { } cap)
        {
            command = command.With("MAX").With(cap);
        }

        return BalancePair("HTRANSFER", await ExecuteCommandAsync(command, cancellationToken));
    }

    /// <inheritdoc />
    public ValueTask<string?> GetExpireAsync(string key, long ttlMs, CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("GETEX", key, ttlMs), cancellationToken);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<long?> LeaseAsync(string key, long ttlMs, CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("LEASE", key, ttlMs), cancellationToken);

        return reply.Kind == RespKind.Null ? null : reply.Integer;
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<long?> AcquireSemaphoreAsync(string key, long max, long ttlMs, CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("SEMAPHORE", key, max, ttlMs), cancellationToken);

        return reply.Kind == RespKind.Null ? null : reply.Integer;
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<long?> IncrementWithLimitAsync(string key, long delta, long limit, CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("INCRBY", key, delta, "MAX", limit), cancellationToken);

        return reply.Kind == RespKind.Null ? null : reply.Integer;
    }

    /// <inheritdoc />
    public ValueTask<long> RenewAsync(string key, long token, long ttlMilliseconds, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("RENEW", key, token, ttlMilliseconds), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> ReleaseAsync(string key, long token, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("RELEASE", key, token), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> SetVersionAsync(string key, string value, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("SETV", key, value), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> SetVersionAsync(string key, string value, long version, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("SETV", key, value, version), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> StartChangesAsync(CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("CHANGES", "START"), cancellationToken);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<ChangeNote>> ReadChangesAsync(
        long cursor,
        CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("CHANGES", cursor), cancellationToken);

        if (reply.Items is null)
        {
            throw new RuvioException("CHANGES did not return an array");
        }

        var notes = new List<ChangeNote>(reply.Items.Count);

        foreach (RespValue row in reply.Items)
        {
            if (row.Items is not { Count: >= 2 } parts)
            {
                throw new RuvioException("CHANGES note is not a sequence and a text");
            }

            notes.Add(new ChangeNote(parts[0].Integer, parts[1].AsString() ?? ""));
        }

        return notes;
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<string?>> GetManyAsync(
        params string[] keys)
    {
        return await ArrayOfNullableBulks(Join("MGET", keys));
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask SetManyAsync(params (string Key, string Value)[] pairs)
    {
        await Ok(FlattenPairs("MSET", pairs));
    }

    /// <inheritdoc />
    public ValueTask<string?> GetSetAsync(string key, string value, CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("GETSET", key, value), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<string?> JsonGetAsync(string key, string path = "$", CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("JSON.GET", key, path), cancellationToken);
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<string?>> JsonGetManyAsync(string path, params string[] keys)
    {
        string[] arguments = new string[keys.Length + 1];

        keys.CopyTo(arguments, 0);
        arguments[keys.Length] = path;

        return await ArrayOfNullableBulks(Join("JSON.MGET", arguments));
    }

    /// <inheritdoc />
    public async ValueTask<bool> JsonSetAsync(string key, string path, string json, CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("JSON.SET", key, path, json), cancellationToken);

        return reply.Kind is not RespKind.Null;
    }

    /// <inheritdoc />
    public async ValueTask<bool> JsonMergeAsync(string key, string path, string patch, CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("JSON.MERGE", key, path, patch), cancellationToken);

        return reply.Kind is not RespKind.Null;
    }

    /// <inheritdoc />
    public ValueTask<string?> JsonTypeAsync(string key, string path = "$", CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("JSON.TYPE", key, path), cancellationToken);
    }

    /// <inheritdoc />
    public async ValueTask<long?> JsonStringLengthAsync(string key, string path = "$", CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("JSON.STRLEN", key, path), cancellationToken);

        return reply.Kind == RespKind.Null ? null : reply.Integer;
    }

    /// <inheritdoc />
    public async ValueTask<long?> JsonArrayLengthAsync(string key, string path = "$", CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("JSON.ARRLEN", key, path), cancellationToken);

        return reply.Kind == RespKind.Null ? null : reply.Integer;
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<string>> JsonObjectKeysAsync(string key, string path = "$", CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("JSON.OBJKEYS", key, path), cancellationToken);

        if (reply.Items is null)
        {
            return [];
        }

        return reply.Items.Select(item => item.AsString() ?? string.Empty).ToArray();
    }

    /// <inheritdoc />
    public ValueTask<string?> JsonIncrementAsync(string key, string path, double by, CancellationToken cancellationToken = default)
    {
        return BulkOrNull(
            new RespCommand("JSON.NUMINCRBY", key, path, by.ToString("R", CultureInfo.InvariantCulture)),
            cancellationToken);
    }

    /// <inheritdoc />
    public async ValueTask<bool?> JsonToggleAsync(string key, string path = "$", CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("JSON.TOGGLE", key, path), cancellationToken);

        return reply.Kind == RespKind.Null ? null : reply.Integer != 0;
    }

    /// <inheritdoc />
    public async ValueTask<long?> JsonArrayAppendAsync(string key, string path, string json, CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("JSON.ARRAPPEND", key, path, json), cancellationToken);

        return reply.Kind == RespKind.Null ? null : reply.Integer;
    }

    /// <inheritdoc />
    public async ValueTask<long> JsonClearAsync(string key, string path = "$", CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("JSON.CLEAR", key, path), cancellationToken);

        return reply.Kind == RespKind.Null ? 0 : reply.Integer;
    }

    /// <inheritdoc />
    public ValueTask<long> JsonDeleteAsync(string key, string path = "$", CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("JSON.DEL", key, path), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> AppendAsync(string key, string value, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("APPEND", key, value), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> GetStringLengthAsync(string key, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("STRLEN", key), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> DecrementAsync(string key, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("DECR", key), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> IncrementByAsync(string key, long delta, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("INCRBY", key, delta), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> DecrementByAsync(string key, long delta, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("DECRBY", key, delta), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> UnlinkAsync(params string[] keys)
    {
        return Integer(Join("UNLINK", keys));
    }

    /// <inheritdoc />
    public ValueTask<long> ExistsAsync(params string[] keys)
    {
        return Integer(Join("EXISTS", keys));
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string> GetKeyTypeAsync(string key, CancellationToken cancellationToken = default)
    {
        return (await ExecuteCommandAsync(new RespCommand("TYPE", key), cancellationToken)).AsString()
            ?? throw new RuvioException("TYPE returned a null reply");
    }

    /// <inheritdoc />
    public ValueTask RenameAsync(string key, string newKey, CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("RENAME", key, newKey), cancellationToken);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<ScanPage> ScanAsync(
        long cursor,
        string? pattern = null,
        int? count = null,
        CancellationToken cancellationToken = default)
    {
        var arguments = new List<string> { "SCAN", Number(cursor) };

        if (pattern is not null)
        {
            arguments.Add("MATCH");
            arguments.Add(pattern);
        }

        if (count is not null)
        {
            arguments.Add("COUNT");
            arguments.Add(Number(count.Value));
        }

        return ParseScan(await ExecuteAsync(arguments, cancellationToken));
    }

    /// <inheritdoc />
    public ValueTask<long> GetDatabaseSizeAsync(CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("DBSIZE"), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<bool> ExpireAtAsync(string key, DateTimeOffset when, CancellationToken cancellationToken = default)
    {
        long milliseconds = when.ToUnixTimeMilliseconds();

        return milliseconds % 1000 == 0
            ? Flag(new RespCommand("EXPIREAT", key, milliseconds / 1000), cancellationToken)
            : Flag(new RespCommand("PEXPIREAT", key, milliseconds), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> GetTimeToLiveAsync(string key, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("TTL", key), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> GetTimeToLiveMillisecondsAsync(string key, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("PTTL", key), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<bool> PersistAsync(string key, CancellationToken cancellationToken = default)
    {
        return Flag(new RespCommand("PERSIST", key), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> PushListLeftAsync(string key, params string[] values)
    {
        return Integer(RespCommand.Keyed("LPUSH", key, values));
    }

    /// <inheritdoc />
    public ValueTask<long> PushListRightAsync(string key, params string[] values)
    {
        return Integer(RespCommand.Keyed("RPUSH", key, values));
    }

    /// <inheritdoc />
    public ValueTask<string?> PopListLeftAsync(string key, CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("LPOP", key), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> PopListLeftAsync(string key, long count, CancellationToken cancellationToken = default)
    {
        return ArrayOfBulks(new RespCommand("LPOP", key, count), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<string?> PopListRightAsync(string key, CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("RPOP", key), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> PopListRightAsync(string key, long count, CancellationToken cancellationToken = default)
    {
        return ArrayOfBulks(new RespCommand("RPOP", key, count), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> GetListLengthAsync(string key, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("LLEN", key), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<string?> GetListItemAsync(string key, long index, CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("LINDEX", key, index), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> GetListRangeAsync(string key, long start, long stop, CancellationToken cancellationToken = default)
    {
        return ArrayOfBulks(new RespCommand("LRANGE", key, start, stop), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask TrimListAsync(string key, long start, long stop, CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("LTRIM", key, start, stop), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<KeyValuePair<string, string>?> PopListLeftBlockingAsync(
        string key,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        return PopListLeftBlockingAsync([key], timeout, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<KeyValuePair<string, string>?> PopListLeftBlockingAsync(
        IReadOnlyList<string> keys,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        return BlockPop("BLPOP", keys, timeout, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<KeyValuePair<string, string>?> PopListRightBlockingAsync(
        string key,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        return PopListRightBlockingAsync([key], timeout, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<KeyValuePair<string, string>?> PopListRightBlockingAsync(
        IReadOnlyList<string> keys,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        return BlockPop("BRPOP", keys, timeout, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> AddSetMembersAsync(string key, params string[] members)
    {
        return Integer(RespCommand.Keyed("SADD", key, members));
    }

    /// <inheritdoc />
    public ValueTask<long> RemoveSetMembersAsync(string key, params string[] members)
    {
        return Integer(RespCommand.Keyed("SREM", key, members));
    }

    /// <inheritdoc />
    public ValueTask<bool> SetContainsAsync(string key, string member, CancellationToken cancellationToken = default)
    {
        return Flag(new RespCommand("SISMEMBER", key, member), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> GetSetCountAsync(string key, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("SCARD", key), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> GetSetMembersAsync(string key, CancellationToken cancellationToken = default)
    {
        return ArrayOfBulks(new RespCommand("SMEMBERS", key), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> GetSetIntersectionAsync(params string[] keys)
    {
        return ArrayOfBulks(Join("SINTER", keys));
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> GetSetUnionAsync(params string[] keys)
    {
        return ArrayOfBulks(Join("SUNION", keys));
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> GetSetDifferenceAsync(params string[] keys)
    {
        return ArrayOfBulks(Join("SDIFF", keys));
    }

    /// <inheritdoc />
    public ValueTask<long> StoreSetIntersectionAsync(string destination, params string[] keys)
    {
        return Integer(Join("SINTERSTORE", destination, keys));
    }

    /// <inheritdoc />
    public ValueTask<long> StoreSetUnionAsync(string destination, params string[] keys)
    {
        return Integer(Join("SUNIONSTORE", destination, keys));
    }

    /// <inheritdoc />
    public ValueTask<long> StoreSetDifferenceAsync(string destination, params string[] keys)
    {
        return Integer(Join("SDIFFSTORE", destination, keys));
    }

    /// <inheritdoc />
    public ValueTask<bool> MoveSetMemberAsync(string source, string destination, string member, CancellationToken cancellationToken = default)
    {
        return Flag(new RespCommand("SMOVE", source, destination, member), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<string?> PopSetMemberAsync(string key, CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("SPOP", key), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> PopSetMembersAsync(string key, long count, CancellationToken cancellationToken = default)
    {
        return ArrayOfBulks(new RespCommand("SPOP", key, count), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<string?> GetSetRandomMemberAsync(string key, CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("SRANDMEMBER", key), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> GetSetRandomMembersAsync(string key, long count, CancellationToken cancellationToken = default)
    {
        return ArrayOfBulks(new RespCommand("SRANDMEMBER", key, count), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> SetHashFieldsAsync(string key, params (string Field, string Value)[] fields)
    {
        return Integer(RespCommand.KeyedPairs("HSET", key, fields));
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask SetHashManyAsync(string key, params (string Field, string Value)[] fields)
    {
        await Ok(RespCommand.KeyedPairs("HMSET", key, fields));
    }

    /// <inheritdoc />
    public ValueTask<string?> GetHashFieldAsync(string key, string field, CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("HGET", key, field), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> DeleteHashFieldsAsync(string key, params string[] fields)
    {
        return Integer(RespCommand.Keyed("HDEL", key, fields));
    }

    /// <inheritdoc />
    public ValueTask<long> GetHashLengthAsync(string key, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("HLEN", key), cancellationToken);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyDictionary<string, string>> GetHashAllAsync(string key, CancellationToken cancellationToken = default)
    {
        return Pairs((await ExecuteCommandAsync(new RespCommand("HGETALL", key), cancellationToken)).Items);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string?>> GetHashFieldsAsync(string key, params string[] fields)
    {
        return ArrayOfNullableBulks(RespCommand.Keyed("HMGET", key, fields));
    }

    /// <inheritdoc />
    public ValueTask<bool> HashFieldExistsAsync(string key, string field, CancellationToken cancellationToken = default)
    {
        return Flag(new RespCommand("HEXISTS", key, field), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> GetHashFieldNamesAsync(string key, CancellationToken cancellationToken = default)
    {
        return ArrayOfBulks(new RespCommand("HKEYS", key), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> GetHashFieldValuesAsync(string key, CancellationToken cancellationToken = default)
    {
        return ArrayOfBulks(new RespCommand("HVALS", key), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> IncrementHashFieldAsync(string key, string field, long delta, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("HINCRBY", key, field, delta), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<long>> ExpireHashFieldsAsync(string key, TimeSpan ttl, params string[] fields)
    {
        return ExpireHashFieldsAsync(key, ttl, FieldExpireCondition.Always, fields);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<long>> ExpireHashFieldsAsync(
        string key,
        TimeSpan ttl,
        FieldExpireCondition condition,
        params string[] fields)
    {
        long milliseconds = (long)Math.Ceiling(ttl.TotalMilliseconds);
        string[] head = milliseconds % 1000 == 0
            ? ["HEXPIRE", key, Number(milliseconds / 1000)]
            : ["HPEXPIRE", key, Number(milliseconds)];

        return IntegerArray(FieldBlock(head, condition, fields));
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<long>> ExpireHashFieldsAtAsync(string key, DateTimeOffset when, params string[] fields)
    {
        long milliseconds = when.ToUnixTimeMilliseconds();
        string[] head = milliseconds % 1000 == 0
            ? ["HEXPIREAT", key, Number(milliseconds / 1000)]
            : ["HPEXPIREAT", key, Number(milliseconds)];

        return IntegerArray(FieldBlock(head, FieldExpireCondition.Always, fields));
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<long>> GetHashFieldTimeToLiveAsync(string key, params string[] fields)
    {
        return IntegerArray(FieldBlock(["HTTL", key], FieldExpireCondition.Always, fields));
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<long>> GetHashFieldTimeToLiveMillisecondsAsync(string key, params string[] fields)
    {
        return IntegerArray(FieldBlock(["HPTTL", key], FieldExpireCondition.Always, fields));
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<long>> GetHashFieldExpireTimeAsync(string key, params string[] fields)
    {
        return IntegerArray(FieldBlock(["HEXPIRETIME", key], FieldExpireCondition.Always, fields));
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<long>> GetHashFieldExpireTimeMillisecondsAsync(string key, params string[] fields)
    {
        return IntegerArray(FieldBlock(["HPEXPIRETIME", key], FieldExpireCondition.Always, fields));
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<long>> PersistHashFieldsAsync(string key, params string[] fields)
    {
        return IntegerArray(FieldBlock(["HPERSIST", key], FieldExpireCondition.Always, fields));
    }

    /// <inheritdoc />
    public ValueTask<bool> SetHashFieldIfAbsentAsync(string key, string field, string value, CancellationToken cancellationToken = default)
    {
        return Flag(new RespCommand("HSETNX", key, field, value), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> GetHashFieldLengthAsync(string key, string field, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("HSTRLEN", key, field), cancellationToken);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<HashScanPage> ScanHashAsync(string key, long cursor, CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("HSCAN", key, cursor), cancellationToken);
        ScanPage page = ParseScan(reply);

        return new HashScanPage(page.Cursor, PairsFromFlat(page.Keys));
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<HashScanPage> ScanHashAsync(
        string key,
        long cursor,
        string? pattern,
        int? count = null,
        CancellationToken cancellationToken = default)
    {
        var arguments = new List<string> { "HSCAN", key, Number(cursor) };

        if (pattern is not null)
        {
            arguments.Add("MATCH");
            arguments.Add(pattern);
        }

        if (count is not null)
        {
            arguments.Add("COUNT");
            arguments.Add(Number(count.Value));
        }

        ScanPage page = ParseScan(await ExecuteAsync(arguments, cancellationToken));

        return new HashScanPage(page.Cursor, PairsFromFlat(page.Keys));
    }

    /// <inheritdoc />
    public ValueTask<long> AddSortedSetAsync(string key, params SortedSetEntry[] entries)
    {
        var arguments = new List<string> { "ZADD", key };

        foreach (SortedSetEntry entry in entries)
        {
            arguments.Add(Number(entry.Score));
            arguments.Add(entry.Member);
        }

        return Integer(arguments);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<bool> AddSortedSetAsync(
        string key,
        string member,
        double score,
        CancellationToken cancellationToken = default)
    {
        return await Integer(new RespCommand("ZADD", key, Number(score), member), cancellationToken) > 0;
    }

    /// <inheritdoc />
    public ValueTask<long> AddSortedSetAsync(string key, params (string Member, double Score)[] entries)
    {
        return AddSortedSetAsync(key, SortedSetAddFlags.None, entries);
    }

    /// <inheritdoc />
    public ValueTask<long> AddSortedSetAsync(string key, SortedSetAddFlags flags, params (string Member, double Score)[] entries)
    {
        return AddSortedSetAsync(
            key,
            flags,
            Array.ConvertAll(entries, entry => new SortedSetEntry(entry.Member, entry.Score)));
    }

    /// <inheritdoc />
    public ValueTask<long> AddSortedSetAsync(string key, SortedSetAddFlags flags, params SortedSetEntry[] entries)
    {
        List<string> arguments = SortedSetAddArguments(key, flags);

        foreach (SortedSetEntry entry in entries)
        {
            arguments.Add(Number(entry.Score));
            arguments.Add(entry.Member);
        }

        return Integer(arguments);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<double?> IncrementSortedSetAsync(
        string key,
        string member,
        double delta,
        SortedSetAddFlags flags,
        CancellationToken cancellationToken = default)
    {
        List<string> arguments = SortedSetAddArguments(key, flags);

        arguments.Add("INCR");
        arguments.Add(Number(delta));
        arguments.Add(member);

        string? text = await BulkOrNull(arguments, cancellationToken);

        return text is null ? null : ParseDouble(text);
    }

    private static List<string> SortedSetAddArguments(string key, SortedSetAddFlags flags)
    {
        var arguments = new List<string> { "ZADD", key };

        if (flags.HasFlag(SortedSetAddFlags.IfNotExists))
        {
            arguments.Add("NX");
        }

        if (flags.HasFlag(SortedSetAddFlags.IfExists))
        {
            arguments.Add("XX");
        }

        if (flags.HasFlag(SortedSetAddFlags.GreaterThan))
        {
            arguments.Add("GT");
        }

        if (flags.HasFlag(SortedSetAddFlags.LessThan))
        {
            arguments.Add("LT");
        }

        if (flags.HasFlag(SortedSetAddFlags.Changed))
        {
            arguments.Add("CH");
        }

        return arguments;
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> GetSortedSetRangeByRankAsync(
        string key,
        long start,
        long stop,
        bool descending = false,
        CancellationToken cancellationToken = default)
    {
        string command = descending ? "ZREVRANGE" : "ZRANGE";

        return ArrayOfBulks(new RespCommand(command, key, start, stop), cancellationToken);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<SortedSetEntry>> GetSortedSetRangeByRankWithScoresAsync(
        string key,
        long start,
        long stop,
        bool descending = false,
        CancellationToken cancellationToken = default)
    {
        string command = descending ? "ZREVRANGE" : "ZRANGE";
        RespValue reply = await ExecuteCommandAsync(new RespCommand(command, key, start, stop, "WITHSCORES"), cancellationToken);

        return ParseSortedSet(reply);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> GetSortedSetRangeByLexAsync(
        string key,
        string min,
        string max,
        CancellationToken cancellationToken = default)
    {
        return ArrayOfBulks(new RespCommand("ZRANGE", key, min, max, "BYLEX"), cancellationToken);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<SortedSetEntry>> GetSortedSetRangeByScoreWithScoresAsync(
        string key,
        string min,
        string max,
        CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("ZRANGEBYSCORE", key, min, max, "WITHSCORES"), cancellationToken);

        return ParseSortedSet(reply);
    }

    /// <inheritdoc />
    public ValueTask<long> RemoveSortedSetMembersAsync(string key, params string[] members)
    {
        return Integer(RespCommand.Keyed("ZREM", key, members));
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<SortedSetEntry>> PopSortedSetMinAsync(string key, long count = 1, CancellationToken cancellationToken = default)
    {
        return ParseSortedSet(await ExecuteCommandAsync(new RespCommand("ZPOPMIN", key, count), cancellationToken));
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<SortedSetEntry>> PopSortedSetByScoreAsync(string key, string min, string max, long count = 1, CancellationToken cancellationToken = default)
    {
        return ParseSortedSet(await ExecuteCommandAsync(new RespCommand("ZPOPBYSCORE", key, max, "MIN", min, "COUNT").With(count).With("WITHSCORES"), cancellationToken));
    }

    /// <inheritdoc />
    public ValueTask<long> MoveDueSortedSetAsync(string source, string destination, string max, long count = 1, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("ZMOVEDUE", source, destination, max, "COUNT", count), cancellationToken);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<SortedSetEntry>> PopSortedSetMaxAsync(string key, long count = 1, CancellationToken cancellationToken = default)
    {
        return ParseSortedSet(await ExecuteCommandAsync(new RespCommand("ZPOPMAX", key, count), cancellationToken));
    }

    /// <inheritdoc />
    public ValueTask<(string Key, SortedSetEntry Entry)?> PopSortedSetMinBlockingAsync(
        string key,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        return PopSortedSetMinBlockingAsync([key], timeout, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<(string Key, SortedSetEntry Entry)?> PopSortedSetMinBlockingAsync(
        IReadOnlyList<string> keys,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        return BlockPopSorted("BZPOPMIN", keys, timeout, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<(string Key, SortedSetEntry Entry)?> PopSortedSetMaxBlockingAsync(
        string key,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        return PopSortedSetMaxBlockingAsync([key], timeout, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<(string Key, SortedSetEntry Entry)?> PopSortedSetMaxBlockingAsync(
        IReadOnlyList<string> keys,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        return BlockPopSorted("BZPOPMAX", keys, timeout, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> RemoveSortedSetRangeByRankAsync(string key, long start, long stop, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("ZREMRANGEBYRANK", key, start, stop), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> RemoveSortedSetRangeByScoreAsync(string key, string min, string max, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("ZREMRANGEBYSCORE", key, min, max), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> RemoveSortedSetRangeByLexAsync(string key, string min, string max, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("ZREMRANGEBYLEX", key, min, max), cancellationToken);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<double> IncrementSortedSetAsync(string key, string member, double delta, CancellationToken cancellationToken = default)
    {
        string? text = await BulkOrNull(new RespCommand("ZINCRBY", key, Number(delta), member), cancellationToken);

        return ParseDouble(text ?? throw new RuvioException("ZINCRBY returned a null reply"));
    }

    /// <inheritdoc />
    public ValueTask<long> GetSortedSetCountAsync(string key, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("ZCARD", key), cancellationToken);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<double?> GetSortedSetScoreAsync(string key, string member, CancellationToken cancellationToken = default)
    {
        string? text = await BulkOrNull(new RespCommand("ZSCORE", key, member), cancellationToken);

        return text is null ? null : ParseDouble(text);
    }

    /// <inheritdoc />
    public ValueTask<long> AddGeoAsync(
        string key,
        params (double Longitude, double Latitude, string Member)[] places)
    {
        var arguments = new string[2 + places.Length * 3];

        arguments[0] = "GEOADD";
        arguments[1] = key;
        int cursor = 2;

        foreach ((double longitude, double latitude, string member) in places)
        {
            arguments[cursor++] = Number(longitude);
            arguments[cursor++] = Number(latitude);
            arguments[cursor++] = member;
        }

        return Integer(arguments, CancellationToken.None);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<double?> GeoDistanceAsync(
        string key,
        string from,
        string to,
        string unit = "m",
        CancellationToken cancellationToken = default)
    {
        string? text = await BulkOrNull(new RespCommand("GEODIST", key, from, to, unit), cancellationToken);

        return text is null ? null : ParseDouble(text);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<double?> GeoDistanceAsync(
        string key,
        string from,
        string to,
        GeoUnit unit,
        CancellationToken cancellationToken = default)
    {
        string? text = await BulkOrNull(new RespCommand("GEODIST", key, from, to, Keywords.Arg(unit)), cancellationToken);

        return text is null ? null : ParseDouble(text);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string?>> GeoHashAsync(
        string key,
        params string[] members)
    {
        var arguments = new List<string> { "GEOHASH", key };

        arguments.AddRange(members);

        return ArrayOfNullableBulks(arguments, CancellationToken.None);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<(double Longitude, double Latitude)?>> GeoPositionAsync(
        string key,
        params string[] members)
    {
        var arguments = new List<string> { "GEOPOS", key };

        arguments.AddRange(members);
        RespValue reply = await ExecuteAsync(arguments, CancellationToken.None);

        if (reply.Kind is not RespKind.Array || reply.Items is null)
        {
            throw new RuvioException($"GEOPOS returned {reply.Kind}");
        }

        var points = new List<(double Longitude, double Latitude)?>(reply.Items.Count);

        foreach (RespValue item in reply.Items)
        {
            if (item.Kind is RespKind.Null)
            {
                points.Add(null);
                continue;
            }

            if (item.Kind is not RespKind.Array || item.Items is null || item.Items.Count != 2)
            {
                throw new RuvioException($"GEOPOS returned {item.Kind}");
            }

            points.Add((
                ParseDouble(item.Items[0].AsString() ?? throw new RuvioException("GEOPOS returned a null coordinate")),
                ParseDouble(item.Items[1].AsString() ?? throw new RuvioException("GEOPOS returned a null coordinate"))));
        }

        return points;
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> GeoSearchAsync(
        string key,
        double longitude,
        double latitude,
        double radius,
        string unit)
    {
        return ArrayOfBulks(
            new string[] { "GEOSEARCH", key, "FROMLONLAT", Number(longitude), Number(latitude), "BYRADIUS", Number(radius), unit, "ASC" },
            CancellationToken.None);
    }

    /// <inheritdoc />
    public ValueTask<long> GeoSearchStoreAsync(
        string destination,
        string source,
        string member,
        double radius,
        string unit)
    {
        return Integer(
            new string[] { "GEOSEARCHSTORE", destination, source, "FROMMEMBER", member, "BYRADIUS", Number(radius), unit },
            CancellationToken.None);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> GeoRadiusByMemberAsync(
        string key,
        string member,
        double radius,
        string unit)
    {
        return ArrayOfBulks(
            new RespCommand("GEORADIUSBYMEMBER", key, member, Number(radius), unit, "ASC"),
            CancellationToken.None);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> GeoRadiusAsync(
        string key,
        double longitude,
        double latitude,
        double radius,
        string unit)
    {
        return ArrayOfBulks(
            new string[] { "GEORADIUS", key, Number(longitude), Number(latitude), Number(radius), unit, "ASC" },
            CancellationToken.None);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> GeoSearchAsync(
        string key,
        double longitude,
        double latitude,
        double radius,
        GeoUnit unit,
        GeoOrder order = GeoOrder.Ascending)
    {
        return ArrayOfBulks(
            new string[] { "GEOSEARCH", key, "FROMLONLAT", Number(longitude), Number(latitude), "BYRADIUS", Number(radius), Keywords.Word(unit), Keywords.Word(order) },
            CancellationToken.None);
    }

    /// <inheritdoc />
    public ValueTask<long> GeoSearchStoreAsync(
        string destination,
        string source,
        string member,
        double radius,
        GeoUnit unit)
    {
        return Integer(
            new string[] { "GEOSEARCHSTORE", destination, source, "FROMMEMBER", member, "BYRADIUS", Number(radius), Keywords.Word(unit) },
            CancellationToken.None);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> GeoRadiusByMemberAsync(
        string key,
        string member,
        double radius,
        GeoUnit unit,
        GeoOrder order = GeoOrder.Ascending)
    {
        return ArrayOfBulks(
            new RespCommand("GEORADIUSBYMEMBER", key, member, Number(radius), Keywords.Arg(unit), Keywords.Arg(order)),
            CancellationToken.None);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> GeoRadiusAsync(
        string key,
        double longitude,
        double latitude,
        double radius,
        GeoUnit unit,
        GeoOrder order = GeoOrder.Ascending)
    {
        return ArrayOfBulks(
            new string[] { "GEORADIUS", key, Number(longitude), Number(latitude), Number(radius), Keywords.Word(unit), Keywords.Word(order) },
            CancellationToken.None);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<long?> GetSortedSetRankAsync(
        string key,
        string member,
        bool descending = false,
        CancellationToken cancellationToken = default)
    {
        string command = descending ? "ZREVRANK" : "ZRANK";
        RespValue reply = await ExecuteCommandAsync(new RespCommand(command, key, member), cancellationToken);

        if (reply.Kind is RespKind.Null)
        {
            return null;
        }

        if (reply.Kind is not RespKind.Integer)
        {
            throw new RuvioException($"{command} returned {reply.Kind}");
        }

        return reply.Integer;
    }

    /// <inheritdoc />
    public ValueTask ReserveBloomFilterAsync(string key, double errorRate, long capacity, CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("BF.RESERVE", key, Number(errorRate), capacity), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<bool> AddBloomFilterAsync(string key, string item, CancellationToken cancellationToken = default)
    {
        return Flag(new RespCommand("BF.ADD", key, item), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<bool> BloomFilterContainsAsync(string key, string item, CancellationToken cancellationToken = default)
    {
        return Flag(new RespCommand("BF.EXISTS", key, item), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<string?> AddStreamEntryIfExistsAsync(
        string key,
        string id,
        params (string Field, string Value)[] fields)
    {
        var arguments = new List<string> { "XADD", key, "NOMKSTREAM" };

        arguments.AddRange(FlattenPairs(id, fields));

        return BulkOrNull(arguments);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string> AddStreamEntryAsync(string key, string id, params (string Field, string Value)[] fields)
    {
        string? result = await BulkOrNull(FlattenPairs("XADD", key, id, fields));

        return result ?? throw new RuvioException("XADD returned a null reply");
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string> AddStreamEntryDelayedAsync(
        string key,
        long delayMs,
        string id,
        params (string Field, string Value)[] fields)
    {
        var arguments = new List<string> { "XADD", key, "DELAY", Number(delayMs) };

        arguments.AddRange(FlattenPairs(id, fields));
        string? result = await BulkOrNull(arguments);

        return result ?? throw new RuvioException("XADD returned a null reply");
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string> AddStreamEntryAsync(
        string key,
        string id,
        long maxLength,
        params (string Field, string Value)[] fields)
    {
        var arguments = new List<string> { "XADD", key, "MAXLEN", Number(maxLength) };

        arguments.AddRange(FlattenPairs(id, fields));
        string? result = await BulkOrNull(arguments);

        return result ?? throw new RuvioException("XADD returned a null reply");
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string> AddStreamEntryAsync(
        string key,
        string id,
        StreamTrim trim,
        params (string Field, string Value)[] fields)
    {
        var arguments = new List<string> { "XADD", key, Keywords.Word(trim.Strategy), Keywords.Word(trim.Mode) };

        arguments.Add(trim.Strategy == StreamTrimStrategy.MinId ? trim.Id ?? throw new ArgumentException("A MINID trim needs an id.", nameof(trim)) : Number(trim.Length));
        arguments.AddRange(FlattenPairs(id, fields));
        string? result = await BulkOrNull(arguments);

        return result ?? throw new RuvioException("XADD returned a null reply");
    }

    /// <inheritdoc />
    public ValueTask<long> GetStreamLengthAsync(string key, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("XLEN", key), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<RespValue> GetStreamInfoAsync(string key, CancellationToken cancellationToken = default)
    {
        return ExecuteCommandAsync(new RespCommand("XINFO", "STREAM", key), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<RespValue> GetStreamGroupsAsync(string key, CancellationToken cancellationToken = default)
    {
        return ExecuteCommandAsync(new RespCommand("XINFO", "GROUPS", key), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask SetStreamIdAsync(string key, string id, CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("XSETID", key, id), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<RespValue> AutoClaimStreamAsync(
        string key,
        string group,
        string consumer,
        long minIdleMs,
        string start,
        CancellationToken cancellationToken = default)
    {
        return ExecuteCommandAsync(
            new RespCommand("XAUTOCLAIM", key, group, consumer, minIdleMs, start),
            cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<RespValue> GetStreamConsumersAsync(
        string key,
        string group,
        CancellationToken cancellationToken = default)
    {
        return ExecuteCommandAsync(new RespCommand("XINFO", "CONSUMERS", key, group), cancellationToken);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<StreamEntry>> GetStreamRangeAsync(
        string key,
        string start,
        string end,
        bool reverse = false,
        CancellationToken cancellationToken = default)
    {
        string command = reverse ? "XREVRANGE" : "XRANGE";

        return ParseStreamEntries(await ExecuteCommandAsync(new RespCommand(command, key, start, end), cancellationToken));
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<StreamEntry>> GetStreamRangeAsync(
        string key,
        string start,
        string end,
        int count,
        bool reverse = false,
        CancellationToken cancellationToken = default)
    {
        string command = reverse ? "XREVRANGE" : "XRANGE";
        RespValue reply = await ExecuteCommandAsync(new RespCommand(command, key, start, end, "COUNT", count), cancellationToken);

        return ParseStreamEntries(reply);
    }

    /// <inheritdoc />
    public ValueTask<long> DeleteStreamEntriesAsync(string key, params string[] ids)
    {
        return Integer(Join("XDEL", key, ids));
    }

    /// <inheritdoc />
    public ValueTask<long> TrimStreamMinIdAsync(string key, string id, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("XTRIM", key, "MINID", id), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> TrimStreamAsync(string key, long maxLength, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("XTRIM", key, "MAXLEN", maxLength), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> TrimStreamAsync(string key, StreamTrim trim, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("XTRIM", key, Keywords.Arg(trim.Strategy), Keywords.Arg(trim.Mode), Keywords.Threshold(trim)), cancellationToken);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<StreamReadResult>> GetStreamUpdatesAsync(
        params (string Key, string Id)[] streams)
    {
        var arguments = new List<string> { "XREAD", "STREAMS" };

        AddStreams(arguments, streams);

        return ParseStreamRead(await ExecuteAsync(arguments));
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<StreamReadResult>> GetStreamUpdatesAsync(
        StreamReadOptions options,
        params (string Key, string Id)[] streams)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (options.NoAcknowledge)
        {
            throw new ArgumentException("NOACK applies only to XREADGROUP.", nameof(options));
        }

        var arguments = new List<string> { "XREAD" };

        AddStreamReadOptions(arguments, options);

        arguments.Add("STREAMS");
        AddStreams(arguments, streams);

        return ParseStreamRead(await ExecuteAsync(arguments));
    }

    /// <inheritdoc />
    public ValueTask<long> CreateStreamConsumerAsync(
        string key,
        string group,
        string consumer,
        CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("XGROUP", "CREATECONSUMER", key, group, consumer), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask CreateStreamGroupAsync(string key, string group, string id, CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("XGROUP", "CREATE", key, group, id), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask CreateStreamGroupAsync(
        string key,
        string group,
        string id,
        bool createStream,
        CancellationToken cancellationToken = default)
    {
        IReadOnlyList<string> arguments = createStream
            ? ["XGROUP", "CREATE", key, group, id, "MKSTREAM"]
            : ["XGROUP", "CREATE", key, group, id];

        return Ok(new RespCommand(arguments), cancellationToken);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<StreamReadResult>> GetStreamGroupUpdatesAsync(
        string group,
        string consumer,
        string key,
        string id,
        CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteAsync(
            ["XREADGROUP", "GROUP", group, consumer, "STREAMS", key, id],
            cancellationToken);

        return ParseStreamRead(reply);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<StreamReadResult>> GetStreamGroupUpdatesAsync(
        string group,
        string consumer,
        string key,
        string id,
        StreamReadOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);

        var arguments = new List<string> { "XREADGROUP", "GROUP", group, consumer };

        AddStreamReadOptions(arguments, options);

        if (options.NoAcknowledge)
        {
            arguments.Add("NOACK");
        }

        arguments.AddRange(["STREAMS", key, id]);

        return ParseStreamRead(await ExecuteAsync(arguments, cancellationToken));
    }

    private static void AddStreamReadOptions(List<string> arguments, StreamReadOptions options)
    {
        if (options.Count is { } count)
        {
            arguments.Add("COUNT");
            arguments.Add(Number(count));
        }

        if (options.Block is { } block)
        {
            if (block < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(options), "Block cannot be negative.");
            }

            arguments.Add("BLOCK");
            arguments.Add(Number((long)Math.Ceiling(block.TotalMilliseconds)));
        }
    }

    /// <inheritdoc />
    public ValueTask<long> DestroyStreamGroupAsync(string key, string group, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("XGROUP", "DESTROY", key, group), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask SetStreamGroupIdAsync(string key, string group, string id, CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("XGROUP", "SETID", key, group, id), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> DeleteStreamGroupConsumerAsync(string key, string group, string consumer, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("XGROUP", "DELCONSUMER", key, group, consumer), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> AcknowledgeStreamAsync(string key, string group, params string[] ids)
    {
        return Integer(Join("XACK", key, group, ids));
    }

    /// <inheritdoc />
    public ValueTask<RespValue> GetStreamPendingAsync(string key, string group, CancellationToken cancellationToken = default)
    {
        return ExecuteCommandAsync(new RespCommand("XPENDING", key, group), cancellationToken);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<StreamPendingEntry>> GetStreamPendingAsync(
        string key,
        string group,
        string start,
        string end,
        long count,
        string? consumer = null,
        TimeSpan? minIdle = null,
        CancellationToken cancellationToken = default)
    {
        var arguments = new List<string> { "XPENDING", key, group };

        if (minIdle is { } idle)
        {
            arguments.Add("IDLE");
            arguments.Add(Number((long)Math.Ceiling(idle.TotalMilliseconds)));
        }

        arguments.AddRange([start, end, Number(count)]);

        if (consumer is not null)
        {
            arguments.Add(consumer);
        }

        RespValue reply = await ExecuteAsync(arguments, cancellationToken);
        var rows = new List<StreamPendingEntry>();

        foreach (RespValue row in reply.Items ?? [])
        {
            if (row.Items is not { Count: 4 } fields)
            {
                throw new RuvioException("XPENDING returned an unexpected row");
            }

            rows.Add(new StreamPendingEntry(
                fields[0].AsString() ?? "",
                fields[1].AsString() ?? "",
                fields[2].Integer,
                fields[3].Integer));
        }

        return rows;
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<StreamEntry>> ClaimStreamEntriesAsync(
        string key,
        string group,
        string consumer,
        long minIdleMilliseconds,
        params string[] ids)
    {
        var arguments = new List<string> { "XCLAIM", key, group, consumer, Number(minIdleMilliseconds) };

        arguments.AddRange(ids);

        return ParseStreamEntries(await ExecuteAsync(arguments));
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<StreamEntry>> ClaimStreamEntriesAsync(
        string key,
        string group,
        string consumer,
        long minIdleMilliseconds,
        StreamClaimOptions options,
        params string[] ids)
    {
        List<string> arguments = ClaimArguments(key, group, consumer, minIdleMilliseconds, options, ids);

        return ParseStreamEntries(await ExecuteAsync(arguments));
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> ClaimStreamEntryIdsAsync(
        string key,
        string group,
        string consumer,
        long minIdleMilliseconds,
        StreamClaimOptions? options,
        params string[] ids)
    {
        List<string> arguments = ClaimArguments(key, group, consumer, minIdleMilliseconds, options, ids);

        arguments.Add("JUSTID");

        return ArrayOfBulks(arguments);
    }

    private static List<string> ClaimArguments(
        string key,
        string group,
        string consumer,
        long minIdleMilliseconds,
        StreamClaimOptions? options,
        string[] ids)
    {
        var arguments = new List<string> { "XCLAIM", key, group, consumer, Number(minIdleMilliseconds) };

        arguments.AddRange(ids);

        if (options is null)
        {
            return arguments;
        }

        if (options.Idle is not null && options.Time is not null)
        {
            throw new ArgumentException("IDLE and TIME cannot be combined.", nameof(options));
        }

        if (options.Idle is { } idle)
        {
            arguments.Add("IDLE");
            arguments.Add(Number((long)Math.Ceiling(idle.TotalMilliseconds)));
        }

        if (options.Time is { } time)
        {
            arguments.Add("TIME");
            arguments.Add(Number(time.ToUnixTimeMilliseconds()));
        }

        if (options.RetryCount is { } retryCount)
        {
            arguments.Add("RETRYCOUNT");
            arguments.Add(Number(retryCount));
        }

        if (options.Force)
        {
            arguments.Add("FORCE");
        }

        if (options.LastId is not null)
        {
            arguments.Add("LASTID");
            arguments.Add(options.LastId);
        }

        return arguments;
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string> GetInfoAsync(CancellationToken cancellationToken = default)
    {
        return await BulkOrNull(new RespCommand("INFO"), cancellationToken)
            ?? throw new RuvioException("INFO returned a null reply");
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string> GetInfoAsync(string section, CancellationToken cancellationToken = default)
    {
        return await BulkOrNull(new RespCommand("INFO", section), cancellationToken)
            ?? throw new RuvioException("INFO returned a null reply");
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string> GetInfoAsync(InfoSection section, CancellationToken cancellationToken = default)
    {
        return await BulkOrNull(new RespCommand("INFO", Keywords.Arg(section)), cancellationToken)
            ?? throw new RuvioException("INFO returned a null reply");
    }

    /// <inheritdoc />
    public ValueTask SaveAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("SAVE"), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask SaveInBackgroundAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("BGSAVE"), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> PublishAsync(string channel, string message, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("PUBLISH", channel, message), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> PublishShardedAsync(string channel, string message, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("SPUBLISH", channel, message), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<RespValue> EvaluateAsync(string script, long keyCount, params string[] arguments)
    {
        return ExecuteAsync(Join("EVAL", script, Number(keyCount), arguments));
    }

    /// <inheritdoc />
    public ValueTask BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("MULTI"), cancellationToken);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<RespValue>> ExecuteTransactionAsync(CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("EXEC"), cancellationToken);

        return reply.Items ?? [];
    }

    /// <inheritdoc />
    public ValueTask DiscardTransactionAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("DISCARD"), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask WatchAsync(params string[] keys)
    {
        return Ok(Join("WATCH", keys));
    }

    /// <inheritdoc />
    public ValueTask UnwatchAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("UNWATCH"), cancellationToken);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string> GetAclUserNameAsync(CancellationToken cancellationToken = default)
    {
        return await BulkOrNull(new RespCommand("ACL", "WHOAMI"), cancellationToken)
            ?? throw new RuvioException("ACL WHOAMI returned a null reply");
    }

    /// <inheritdoc />
    public ValueTask<RespValue> GetClusterSlotsAsync(CancellationToken cancellationToken = default)
    {
        return ExecuteCommandAsync(new RespCommand("CLUSTER", "SLOTS"), cancellationToken);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string> GetClusterNodesAsync(CancellationToken cancellationToken = default)
    {
        return await BulkOrNull(new RespCommand("CLUSTER", "NODES"), cancellationToken)
            ?? throw new RuvioException("CLUSTER NODES returned a null reply");
    }

    /// <inheritdoc />
    public ValueTask<RespValue> GetClusterShardsAsync(CancellationToken cancellationToken = default)
    {
        return ExecuteCommandAsync(new RespCommand("CLUSTER", "SHARDS"), cancellationToken);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string> GetClusterInfoAsync(CancellationToken cancellationToken = default)
    {
        return await BulkOrNull(new RespCommand("CLUSTER", "INFO"), cancellationToken)
            ?? throw new RuvioException("CLUSTER INFO returned a null reply");
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string> GetClusterMyIdAsync(CancellationToken cancellationToken = default)
    {
        return await BulkOrNull(new RespCommand("CLUSTER", "MYID"), cancellationToken)
            ?? throw new RuvioException("CLUSTER MYID returned a null reply");
    }

    /// <inheritdoc />
    public ValueTask<long> GetClusterKeySlotAsync(string key, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("CLUSTER", "KEYSLOT", key), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask ReadOnlyAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("READONLY"), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask ReadWriteAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("READWRITE"), cancellationToken);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string> LoadScriptAsync(string script, CancellationToken cancellationToken = default)
    {
        return await BulkOrNull(new RespCommand("SCRIPT", "LOAD", script), cancellationToken)
            ?? throw new RuvioException("SCRIPT LOAD returned a null reply");
    }

    /// <inheritdoc />
    public ValueTask<RespValue> EvaluateShaAsync(string sha, long keyCount, params string[] arguments)
    {
        return ExecuteAsync(Join("EVALSHA", sha, Number(keyCount), arguments));
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<IReadOnlyList<bool>> ScriptExistsAsync(params string[] hashes)
    {
        RespValue reply = await ExecuteAsync(Join("SCRIPT", "EXISTS", hashes));

        IReadOnlyList<RespValue> items = reply.Items ?? [];
        var exists = new bool[items.Count];

        for (int index = 0; index < exists.Length; index++)
        {
            exists[index] = items[index].Integer > 0;
        }

        return exists;
    }

    /// <inheritdoc />
    public ValueTask FlushScriptsAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("SCRIPT", "FLUSH"), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask KillScriptAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("SCRIPT", "KILL"), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<RespValue> CallFunctionAsync(string name, long keyCount, params string[] arguments)
    {
        return ExecuteAsync(Join("FCALL", name, Number(keyCount), arguments));
    }

    /// <inheritdoc />
    public ValueTask LoadFunctionAsync(string name, string body, CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("FUNCTION", "LOAD", name, body), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<RespValue> GetFunctionListAsync(CancellationToken cancellationToken = default)
    {
        return ExecuteCommandAsync(new RespCommand("FUNCTION", "LIST"), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<bool> DeleteFunctionAsync(string name, CancellationToken cancellationToken = default)
    {
        return Flag(new RespCommand("FUNCTION", "DELETE", name), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> GetAclUsersAsync(CancellationToken cancellationToken = default)
    {
        return ArrayOfBulks(new RespCommand("ACL", "USERS"), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<string?> GetConnectionNameAsync(CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("CLIENT", "GETNAME"), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask SetConnectionNameAsync(string name, CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("CLIENT", "SETNAME", name), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask SetTrackingAsync(bool enabled, CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("CLIENT", "TRACKING", enabled ? "ON" : "OFF"), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask SetDurabilityAsync(int replicas, TimeSpan? timeout = null, CancellationToken cancellationToken = default)
    {
        string count = replicas.ToString(CultureInfo.InvariantCulture);

        return timeout is { } limit
            ? Ok(new RespCommand("CLIENT", "DURABILITY", "REPLICAS", count, "TIMEOUT", ((long)limit.TotalMilliseconds).ToString(CultureInfo.InvariantCulture)), cancellationToken)
            : Ok(new RespCommand("CLIENT", "DURABILITY", "REPLICAS", count), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<RespValue> GetConfigAsync(string parameter, CancellationToken cancellationToken = default)
    {
        return ExecuteCommandAsync(new RespCommand("CONFIG", "GET", parameter), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask SetConfigAsync(IReadOnlyList<(string Name, string Value)> settings, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(settings);

        var arguments = new string[2 + settings.Count * 2];

        arguments[0] = "CONFIG";
        arguments[1] = "SET";
        int index = 2;

        foreach ((string name, string value) in settings)
        {
            arguments[index++] = name;
            arguments[index++] = value;
        }

        return Ok(arguments, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask RewriteConfigAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("CONFIG", "REWRITE"), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask ReplicaOfAsync(string host, int port, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(host);

        return Ok(new RespCommand("REPLICAOF", host, port.ToString(System.Globalization.CultureInfo.InvariantCulture)), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask ReplicaOfNoOneAsync(bool force = false, CancellationToken cancellationToken = default)
    {
        return Ok(force ? new RespCommand("REPLICAOF", "NO", "ONE", "FORCE") : new RespCommand("REPLICAOF", "NO", "ONE"), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<RespValue> GetRoleAsync(CancellationToken cancellationToken = default)
        => ExecuteCommandAsync(new RespCommand("ROLE"), cancellationToken);

    /// <inheritdoc />
    public ValueTask FlushDatabaseAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("FLUSHDB"), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask FlushAllDatabasesAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("FLUSHALL"), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask SwapDatabasesAsync(int first, int second, CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("SWAPDB", first, second), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<bool> MoveKeyAsync(string key, int database, CancellationToken cancellationToken = default)
    {
        return Flag(new RespCommand("MOVE", key, database), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<RespValue> HelloAsync(int protocol = 2, CancellationToken cancellationToken = default)
    {
        return ExecuteCommandAsync(new RespCommand("HELLO", protocol), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> GetAclListAsync(CancellationToken cancellationToken = default)
    {
        return ArrayOfBulks(new RespCommand("ACL", "LIST"), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<RespValue> GetAclUserAsync(string user, CancellationToken cancellationToken = default)
    {
        return ExecuteCommandAsync(new RespCommand("ACL", "GETUSER", user), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> GetAclCategoriesAsync(CancellationToken cancellationToken = default)
    {
        return ArrayOfBulks(new RespCommand("ACL", "CAT"), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask SetAclUserAsync(string user, params string[] rules)
    {
        return Ok(Join("ACL", "SETUSER", user, rules));
    }

    /// <inheritdoc />
    public ValueTask LoadAclAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("ACL", "LOAD"), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask SaveAclAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("ACL", "SAVE"), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<RespValue>> SubscribeAsync(params string[] channels)
    {
        return ExecuteManyReplies(Join("SUBSCRIBE", channels), channels.Length);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask SubscribeAsync(
        string channel,
        Action<PubSubMessage> handler,
        CancellationToken cancellationToken = default)
    {
        RegisterHandler(channel, handler);

        await ExecuteManyReplies(Join("SUBSCRIBE", channel), 1, cancellationToken);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask SubscribeAsync(
        string channel,
        Func<PubSubMessage, Task> handler,
        CancellationToken cancellationToken = default)
    {
        RegisterHandler(channel, handler);

        await ExecuteManyReplies(Join("SUBSCRIBE", channel), 1, cancellationToken);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask SubscribePatternsAsync(
        string pattern,
        Action<PubSubMessage> handler,
        CancellationToken cancellationToken = default)
    {
        RegisterHandler(pattern, handler);

        await ExecuteManyReplies(Join("PSUBSCRIBE", pattern), 1, cancellationToken);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask SubscribePatternsAsync(
        string pattern,
        Func<PubSubMessage, Task> handler,
        CancellationToken cancellationToken = default)
    {
        RegisterHandler(pattern, handler);

        await ExecuteManyReplies(Join("PSUBSCRIBE", pattern), 1, cancellationToken);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask SubscribeShardedAsync(
        string channel,
        Action<PubSubMessage> handler,
        CancellationToken cancellationToken = default)
    {
        RegisterHandler(channel, handler);

        await ExecuteManyReplies(Join("SSUBSCRIBE", channel), 1, cancellationToken);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask SubscribeShardedAsync(
        string channel,
        Func<PubSubMessage, Task> handler,
        CancellationToken cancellationToken = default)
    {
        RegisterHandler(channel, handler);

        await ExecuteManyReplies(Join("SSUBSCRIBE", channel), 1, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask SubscribeExpiredAsync(
        Action<PubSubMessage> handler,
        CancellationToken cancellationToken = default)
    {
        return SubscribeKeyEventAsync("expired", handler, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask SubscribeExpiredAsync(
        Func<PubSubMessage, Task> handler,
        CancellationToken cancellationToken = default)
    {
        return SubscribeKeyEventAsync("expired", handler, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask SubscribeEvictedAsync(
        Action<PubSubMessage> handler,
        CancellationToken cancellationToken = default)
    {
        return SubscribeKeyEventAsync("evicted", handler, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask SubscribeEvictedAsync(
        Func<PubSubMessage, Task> handler,
        CancellationToken cancellationToken = default)
    {
        return SubscribeKeyEventAsync("evicted", handler, cancellationToken);
    }

    private ValueTask SubscribeKeyEventAsync(
        string eventName,
        Action<PubSubMessage> handler,
        CancellationToken cancellationToken)
    {
        return SubscribeShardedAsync($"__keyevent@{_database}__:{eventName}", handler, cancellationToken);
    }

    private ValueTask SubscribeKeyEventAsync(
        string eventName,
        Func<PubSubMessage, Task> handler,
        CancellationToken cancellationToken)
    {
        return SubscribeShardedAsync($"__keyevent@{_database}__:{eventName}", handler, cancellationToken);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<PubSubMessage> ListenAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            yield return await ReadPubSubDeliveryAsync(cancellationToken);
        }
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<RespValue>> UnsubscribeAsync(params string[] channels)
    {
        RemoveHandlers(channels);

        return channels.Length == 0
            ? ExecuteManyReplies(["UNSUBSCRIBE"], 1)
            : ExecuteManyReplies(Join("UNSUBSCRIBE", channels), channels.Length);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<RespValue>> SubscribePatternsAsync(params string[] patterns)
    {
        return ExecuteManyReplies(Join("PSUBSCRIBE", patterns), patterns.Length);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<RespValue>> UnsubscribePatternsAsync(params string[] patterns)
    {
        RemoveHandlers(patterns);

        return patterns.Length == 0
            ? ExecuteManyReplies(["PUNSUBSCRIBE"], 1)
            : ExecuteManyReplies(Join("PUNSUBSCRIBE", patterns), patterns.Length);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<RespValue>> SubscribeShardedAsync(params string[] channels)
    {
        return ExecuteManyReplies(Join("SSUBSCRIBE", channels), channels.Length);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<RespValue>> UnsubscribeShardedAsync(params string[] channels)
    {
        RemoveHandlers(channels);

        return channels.Length == 0
            ? ExecuteManyReplies(["SUNSUBSCRIBE"], 1)
            : ExecuteManyReplies(Join("SUNSUBSCRIBE", channels), channels.Length);
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<RespValue> ReadMessageAsync(CancellationToken cancellationToken = default)
    {
        RespValue reply = await ReadFromSubscriptionAsync(cancellationToken);

        return RuvioSocket.ThrowOnError(reply);
    }

    /// <inheritdoc />
    public ValueTask<PubSubMessage> ReadPubSubMessageAsync(CancellationToken cancellationToken = default)
    {
        return new ValueTask<PubSubMessage>(ReadPubSubDeliveryAsync(cancellationToken));
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<IReadOnlyList<RespValue>> ExecuteManyReplies(
        IReadOnlyList<string> arguments,
        int replies,
        CancellationToken cancellationToken = default)
    {
        if (replies < 1)
        {
            throw new ArgumentException("A subscribe command needs at least one channel or pattern.");
        }

        IReadOnlyList<RespValue> values = await ExecuteOnConnectionAsync(arguments, replies, cancellationToken);

        TrackSubscription(arguments);

        foreach (RespValue reply in values)
        {
            if (reply.Kind is RespKind.Error)
            {
                throw new RuvioException(reply.Text ?? "ERR", isServerReply: true);
            }
        }

        return values;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<long> Integer(RespCommand arguments, CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(arguments, cancellationToken);

        if (reply.Kind is not RespKind.Integer)
        {
            throw new RuvioException($"{arguments.Name} returned {reply.Kind}");
        }

        return reply.Integer;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<bool> Flag(RespCommand arguments, CancellationToken cancellationToken = default)
    {
        return await Integer(arguments, cancellationToken) > 0;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask Ok(RespCommand arguments, CancellationToken cancellationToken = default)
    {
        await ExecuteCommandAsync(arguments, cancellationToken);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<string?> BulkOrNull(RespCommand arguments, CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(arguments, cancellationToken);

        if (reply.Kind is RespKind.Null)
        {
            return null;
        }

        return reply.AsString();
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<IReadOnlyList<string>> ArrayOfBulks(RespCommand arguments, CancellationToken cancellationToken = default)
    {
        string?[] values = Bulks(await ExecuteCommandAsync(arguments, cancellationToken), arguments.Name);

        foreach (string? value in values)
        {
            if (value is null)
            {
                throw new RuvioException($"{arguments.Name} returned a null bulk");
            }
        }

        return values!;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<IReadOnlyList<string?>> ArrayOfNullableBulks(RespCommand arguments, CancellationToken cancellationToken = default)
    {
        return Bulks(await ExecuteCommandAsync(arguments, cancellationToken), arguments.Name);
    }

    private static string?[] Bulks(RespValue reply, string? name)
    {
        if (reply.Kind is RespKind.Null)
        {
            return [];
        }

        if (reply.Kind is not RespKind.Array || reply.Items is not { } items)
        {
            throw new RuvioException($"{name} returned {reply.Kind}");
        }

        var values = new string?[items.Count];

        for (int index = 0; index < values.Length; index++)
        {
            RespValue item = items[index];
            values[index] = item.Kind is RespKind.Null ? null : item.AsString();
        }

        return values;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<KeyValuePair<string, string>?> BlockPop(
        string command,
        IReadOnlyList<string> keys,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var arguments = new List<string> { command };

        arguments.AddRange(keys);
        arguments.Add(Number((long)Math.Ceiling(timeout.TotalSeconds)));
        RespValue reply = await ExecuteAsync(arguments, cancellationToken);

        if (reply.Kind is RespKind.Null)
        {
            return null;
        }

        if (reply.Items is not { Count: 2 })
        {
            throw new RuvioException($"{command} returned {reply.Kind}");
        }

        return new KeyValuePair<string, string>(
            reply.Items[0].AsString() ?? "",
            reply.Items[1].AsString() ?? "");
    }

    private static ScanPage ParseScan(RespValue reply)
    {
        if (reply.Items is not { Count: 2 })
        {
            throw new RuvioException("SCAN returned an unexpected shape");
        }

        long cursor = long.Parse(reply.Items[0].AsString() ?? "0", CultureInfo.InvariantCulture);
        IReadOnlyList<RespValue> items = reply.Items[1].Items ?? [];
        var keys = new string[items.Count];

        for (int index = 0; index < keys.Length; index++)
        {
            keys[index] = items[index].AsString() ?? "";
        }

        return new ScanPage(cursor, keys);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<(string Key, SortedSetEntry Entry)?> BlockPopSorted(
        string command,
        IReadOnlyList<string> keys,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var arguments = new List<string> { command };

        arguments.AddRange(keys);
        arguments.Add(Number((long)Math.Ceiling(timeout.TotalSeconds)));
        RespValue reply = await ExecuteAsync(arguments, cancellationToken);

        if (reply.Kind is RespKind.Null)
        {
            return null;
        }

        if (reply.Items is not { Count: 3 } items)
        {
            throw new RuvioException($"{command} returned {reply.Kind}");
        }

        return (
            items[0].AsString() ?? "",
            new SortedSetEntry(items[1].AsString() ?? "", ParseDouble(items[2].AsString() ?? "0")));
    }

    /// <summary>The two balances of a <c>TRANSFER</c> or <c>HTRANSFER</c> reply; a null reply means nothing moved.</summary>
    private static (long, long)? BalancePair(string name, RespValue reply)
    {
        if (reply.Kind == RespKind.Null)
        {
            return null;
        }

        if (reply.Items is not { Count: 2 } items || items[0].Kind is not RespKind.Integer || items[1].Kind is not RespKind.Integer)
        {
            throw new RuvioException($"{name} returned {reply.Kind}");
        }

        return (items[0].Integer, items[1].Integer);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<IReadOnlyList<long>> IntegerArray(RespCommand arguments)
    {
        RespValue reply = await ExecuteCommandAsync(arguments, CancellationToken.None);

        if (reply.Items is not { } items)
        {
            throw new RuvioException($"{arguments.Name} returned {reply.Kind}");
        }

        var values = new long[items.Count];

        for (int index = 0; index < values.Length; index++)
        {
            if (items[index].Kind is not RespKind.Integer)
            {
                throw new RuvioException($"{arguments.Name} returned {reply.Kind}");
            }

            values[index] = items[index].Integer;
        }

        return values;
    }

    /// <summary>Appends every stream key, then every id, as XREAD expects.</summary>
    private static void AddStreams(List<string> arguments, (string Key, string Id)[] streams)
    {
        arguments.EnsureCapacity(arguments.Count + (streams.Length * 2));

        foreach ((string key, string _) in streams)
        {
            arguments.Add(key);
        }

        foreach ((string _, string id) in streams)
        {
            arguments.Add(id);
        }
    }

    /// <summary>Appends an optional <c>NX</c>/<c>XX</c>/<c>GT</c>/<c>LT</c>, then <c>FIELDS numfields field ...</c>.</summary>
    private static string[] FieldBlock(string[] head, FieldExpireCondition condition, string[] fields)
    {
        string? flag = condition switch
        {
            FieldExpireCondition.IfNoTtl => "NX",
            FieldExpireCondition.IfTtl => "XX",
            FieldExpireCondition.IfGreater => "GT",
            FieldExpireCondition.IfLess => "LT",
            _ => null,
        };

        var arguments = new string[head.Length + (flag is null ? 2 : 3) + fields.Length];
        int cursor = head.Length;

        head.CopyTo(arguments, 0);

        if (flag is not null)
        {
            arguments[cursor++] = flag;
        }

        arguments[cursor++] = "FIELDS";
        arguments[cursor++] = Number(fields.Length);
        fields.CopyTo(arguments, cursor);

        return arguments;
    }

    private static IReadOnlyList<SortedSetEntry> ParseSortedSet(RespValue reply)
    {
        IReadOnlyList<RespValue> items = reply.Items ?? [];

        if (items.Count % 2 != 0)
        {
            throw new RuvioException("sorted set reply is not member/score pairs");
        }

        var entries = new List<SortedSetEntry>(items.Count / 2);

        for (int index = 0; index < items.Count; index += 2)
        {
            entries.Add(new SortedSetEntry(
                items[index].AsString() ?? "",
                ParseDouble(items[index + 1].AsString() ?? "0")));
        }

        return entries;
    }

    private static IReadOnlyList<StreamEntry> ParseStreamEntries(RespValue reply)
    {
        if (reply.Kind is RespKind.Null || reply.Items is null)
        {
            return [];
        }

        var entries = new List<StreamEntry>();

        foreach (RespValue item in reply.Items)
        {
            if (item.Items is not { Count: 2 })
            {
                continue;
            }

            string id = item.Items[0].AsString() ?? "";
            IReadOnlyList<KeyValuePair<string, string>> fields = RespRead.Pairs(item.Items[1], "stream entry");

            entries.Add(new StreamEntry(id, fields));
        }

        return entries;
    }

    private static IReadOnlyList<StreamReadResult> ParseStreamRead(RespValue reply)
    {
        if (reply.Kind is RespKind.Null || reply.Items is null)
        {
            return [];
        }

        var results = new List<StreamReadResult>();

        foreach (RespValue item in reply.Items)
        {
            if (item.Items is not { Count: 2 })
            {
                continue;
            }

            string key = item.Items[0].AsString() ?? "";

            results.Add(new StreamReadResult(key, ParseStreamEntries(item.Items[1])));
        }

        return results;
    }

    private static IReadOnlyDictionary<string, string> Pairs(IReadOnlyList<RespValue>? items)
    {
        items ??= [];
        var map = new Dictionary<string, string>();

        for (int index = 0; index + 1 < items.Count; index += 2)
        {
            map[items[index].AsString() ?? ""] = items[index + 1].AsString() ?? "";
        }

        return map;
    }

    private static IReadOnlyDictionary<string, string> PairsFromFlat(IReadOnlyList<string> items)
    {
        var map = new Dictionary<string, string>();

        for (int index = 0; index + 1 < items.Count; index += 2)
        {
            map[items[index]] = items[index + 1];
        }

        return map;
    }

    private static string Number(long value) => value.ToString(CultureInfo.InvariantCulture);

    private static string Number(double value) => value.ToString("G", CultureInfo.InvariantCulture);

    private static double ParseDouble(string text) =>
        double.Parse(text, CultureInfo.InvariantCulture);

    private static string[] Join(string command, params string[] arguments)
    {
        var values = new string[arguments.Length + 1];

        values[0] = command;
        arguments.CopyTo(values, 1);

        return values;
    }

    private static string[] Join(string command, string key, params string[] arguments)
    {
        var values = new string[arguments.Length + 2];

        values[0] = command;
        values[1] = key;
        arguments.CopyTo(values, 2);

        return values;
    }

    private static string[] Join(string command, string first, string second, params string[] arguments)
    {
        var values = new string[arguments.Length + 3];

        values[0] = command;
        values[1] = first;
        values[2] = second;
        arguments.CopyTo(values, 3);

        return values;
    }

    private static string[] FlattenPairs(string command, params (string Left, string Right)[] pairs)
    {
        var values = new string[1 + pairs.Length * 2];

        values[0] = command;
        int index = 1;

        foreach ((string left, string right) in pairs)
        {
            values[index++] = left;
            values[index++] = right;
        }

        return values;
    }

    private static string[] FlattenPairs(string command, string key, params (string Left, string Right)[] pairs)
    {
        var values = new string[2 + pairs.Length * 2];

        values[0] = command;
        values[1] = key;
        int index = 2;

        foreach ((string left, string right) in pairs)
        {
            values[index++] = left;
            values[index++] = right;
        }

        return values;
    }

    private static string[] FlattenPairs(string command, string key, string id, params (string Left, string Right)[] pairs)
    {
        var values = new string[3 + pairs.Length * 2];

        values[0] = command;
        values[1] = key;
        values[2] = id;
        int index = 3;

        foreach ((string left, string right) in pairs)
        {
            values[index++] = left;
            values[index++] = right;
        }

        return values;
    }
}
