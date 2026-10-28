namespace Ruvio.Client;

/// <summary>
/// Builds the frames for the commands added in the coverage pass. <see cref="RuvioClient"/> sends them on the
/// inline <see cref="RespCommand"/> path; the default <see cref="IRuvioClient"/> members send <c>AsList()</c>
/// through <see cref="IRuvioClient.ExecuteAsync(IReadOnlyList{string}, CancellationToken)"/>.
/// Up to five arguments stay inline (no argument array, integers are written without a string).
/// </summary>
internal static class CoverageCommands
{
    internal static RespCommand SetIfEqual(string key, string value, string expected, TimeSpan? expiry) =>
        expiry is { } ttl
            ? new RespCommand(["SET", key, value, "IFEQ", expected, "PX", RespRead.Number(PositiveMilliseconds(ttl, nameof(expiry)))])
            : new RespCommand("SET", key, value, "IFEQ", expected);

    internal static RespCommand SetIfGreater(string key, long value, TimeSpan? expiry) =>
        expiry is { } ttl
            ? new RespCommand(["SET", key, RespRead.Number(value), "IFGT", "PX", RespRead.Number(PositiveMilliseconds(ttl, nameof(expiry)))])
            : new RespCommand("SET", key, value, "IFGT");

    internal static RespCommand SetIfLess(string key, long value, TimeSpan? expiry) =>
        expiry is { } ttl
            ? new RespCommand(["SET", key, RespRead.Number(value), "IFLT", "PX", RespRead.Number(PositiveMilliseconds(ttl, nameof(expiry)))])
            : new RespCommand("SET", key, value, "IFLT");

    internal static RespCommand SetIfEqualKeepingTimeToLive(string key, string value, string expected) =>
        new("SET", key, value, "IFEQ", expected, "KEEPTTL");

    internal static RespCommand DeleteIfEqual(string key, string expected) => new("DELEX", key, "IFEQ", expected);

    internal static RespCommand SetExpire(string key, long seconds, string value) => new("SETEX", key, seconds, value);

    internal static RespCommand SetExpireMilliseconds(string key, long milliseconds, string value) =>
        new("PSETEX", key, milliseconds, value);

    internal static RespCommand SetIfNotExists(string key, string value) => new("SETNX", key, value);

    internal static RespCommand GetDelete(string key) => new("GETDEL", key);

    internal static RespCommand IncrementByFloat(string key, double delta) => new("INCRBYFLOAT", key, RespRead.Number(delta));

    internal static RespCommand GetRange(string name, string key, long start, long end) => new(name, key, start, end);

    internal static RespCommand SetRange(string key, long offset, string value) => new("SETRANGE", key, offset, value);

    internal static RespCommand SetManyIfNotExists((string Key, string Value)[] pairs)
    {
        ArgumentNullException.ThrowIfNull(pairs);
        var parts = new string[1 + (pairs.Length * 2)];
        parts[0] = "MSETNX";
        int index = 1;

        foreach ((string key, string value) in pairs)
        {
            parts[index++] = key;
            parts[index++] = value;
        }

        return new RespCommand(parts);
    }

    internal static RespCommand SetBit(string key, long offset, bool value) => new("SETBIT", key, offset, value ? 1L : 0L);

    internal static RespCommand GetBit(string key, long offset) => new("GETBIT", key, offset);

    internal static RespCommand BitCount(string key, long? start, long? end, BitUnit unit)
    {
        if (start is null && end is null)
        {
            return new RespCommand("BITCOUNT", key);
        }

        if (start is not { } from || end is not { } to)
        {
            throw new ArgumentException("BITCOUNT needs both start and end, or neither.");
        }

        return new RespCommand("BITCOUNT", key, from, to, RespRead.Word(unit));
    }

    internal static RespCommand BitPosition(string key, bool bit, long? start, long? end, BitUnit unit)
    {
        var command = new RespCommand("BITPOS", key, bit ? 1L : 0L);

        if (start is not { } from)
        {
            return end is null ? command : throw new ArgumentException("BITPOS end needs a start.");
        }

        command = command.With(from);

        return end is { } to ? command.With(to).With(RespRead.Word(unit)) : command;
    }

    internal static RespCommand BitOperation(BitOperation operation, string destination, IReadOnlyList<string> keys)
    {
        ArgumentNullException.ThrowIfNull(keys);
        var parts = new string[3 + keys.Count];
        parts[0] = "BITOP";
        parts[1] = RespRead.Word(operation);
        parts[2] = destination;

        for (int index = 0; index < keys.Count; index++)
        {
            parts[3 + index] = keys[index];
        }

        return new RespCommand(parts);
    }

    internal static RespCommand Keys(string name, string[] keys)
    {
        ArgumentNullException.ThrowIfNull(keys);
        var parts = new string[keys.Length + 1];
        parts[0] = name;
        keys.CopyTo(parts, 1);

        return new RespCommand(parts);
    }

    internal static RespCommand Capped(string name, string key, long cap, string[] values)
    {
        ArgumentNullException.ThrowIfNull(values);

        if (values.Length <= 3)
        {
            RespCommand command = new(name, key, cap);

            foreach (string value in values)
            {
                command = command.With(value);
            }

            return command;
        }

        var parts = new string[values.Length + 3];
        parts[0] = name;
        parts[1] = key;
        parts[2] = RespRead.Number(cap);
        values.CopyTo(parts, 3);

        return new RespCommand(parts);
    }

    internal static RespCommand PopListManyBlocking(IReadOnlyList<string> keys, bool fromLeft, TimeSpan timeout, long count)
    {
        ArgumentNullException.ThrowIfNull(keys);
        var parts = new string[keys.Count + 6];
        parts[0] = "BLMPOP";
        parts[1] = RespRead.Number(RespRead.Seconds(timeout));
        parts[2] = RespRead.Number(keys.Count);

        for (int index = 0; index < keys.Count; index++)
        {
            parts[3 + index] = keys[index];
        }

        parts[^3] = fromLeft ? "LEFT" : "RIGHT";
        parts[^2] = "COUNT";
        parts[^1] = RespRead.Number(count);

        return new RespCommand(parts);
    }

    internal static RespCommand SetIntersectionCount(IReadOnlyList<string> keys, long limit)
    {
        ArgumentNullException.ThrowIfNull(keys);
        var parts = new string[keys.Count + (limit > 0 ? 4 : 2)];
        parts[0] = "SINTERCARD";
        parts[1] = RespRead.Number(keys.Count);

        for (int index = 0; index < keys.Count; index++)
        {
            parts[2 + index] = keys[index];
        }

        if (limit > 0)
        {
            parts[^2] = "LIMIT";
            parts[^1] = RespRead.Number(limit);
        }

        return new RespCommand(parts);
    }

    /// <summary><c>SSCAN</c>, <c>ZSCAN</c>, <c>HSCAN</c>: key, cursor, optional <c>MATCH</c> and <c>COUNT</c>.</summary>
    internal static RespCommand KeyScan(string name, string key, long cursor, string? pattern, int? count) =>
        (pattern, count) switch
        {
            (null, null) => new RespCommand(name, key, cursor),
            (null, { } size) => new RespCommand(name, key, cursor, "COUNT", size),
            ({ } match, null) => new RespCommand(name, key, cursor, "MATCH", match),
            ({ } match, { } size) => new RespCommand([name, key, RespRead.Number(cursor), "MATCH", match, "COUNT", RespRead.Number(size)]),
        };

    internal static RespCommand Scan(long cursor, string? pattern, int? count, string? type)
    {
        var parts = new List<string>(8) { "SCAN", RespRead.Number(cursor) };

        if (pattern is not null)
        {
            parts.Add("MATCH");
            parts.Add(pattern);
        }

        if (count is { } size)
        {
            parts.Add("COUNT");
            parts.Add(RespRead.Number(size));
        }

        if (type is not null)
        {
            parts.Add("TYPE");
            parts.Add(type);
        }

        return new RespCommand(parts);
    }

    internal static RespCommand RangeByLex(string key, string min, string max, bool descending, long offset, long count)
    {
        string name = descending ? "ZREVRANGEBYLEX" : "ZRANGEBYLEX";
        (string first, string second) = descending ? (max, min) : (min, max);

        return offset != 0 || count >= 0
            ? new RespCommand([name, key, first, second, "LIMIT", RespRead.Number(offset), RespRead.Number(count)])
            : new RespCommand(name, key, first, second);
    }

    /// <summary>With <c>REV</c> the high bound goes first, as in Redis.</summary>
    internal static RespCommand RangeByScore(string key, string min, string max, bool descending, long offset, long count)
    {
        bool limit = offset != 0 || count >= 0;

        return (descending, limit) switch
        {
            (false, false) => new RespCommand("ZRANGE", key, min, max, "BYSCORE"),
            (true, false) => new RespCommand("ZRANGE", key, max, min, "BYSCORE", "REV"),
            (false, true) => new RespCommand(["ZRANGE", key, min, max, "BYSCORE", "LIMIT", RespRead.Number(offset), RespRead.Number(count)]),
            (true, true) => new RespCommand(["ZRANGE", key, max, min, "BYSCORE", "REV", "LIMIT", RespRead.Number(offset), RespRead.Number(count)]),
        };
    }

    internal static RespCommand Members(string name, string key, string[] members)
    {
        ArgumentNullException.ThrowIfNull(members);

        return RespCommand.Keyed(name, key, members);
    }

    internal static RespCommand RandomMembers(string name, string key, long count, string? with) =>
        with is null ? new RespCommand(name, key, count) : new RespCommand(name, key, count, with);

    /// <summary><c>ZUNION</c>/<c>ZINTER</c>/<c>ZDIFF</c> (destination null) or their <c>STORE</c> forms.</summary>
    internal static RespCommand Combine(
        string name,
        string? destination,
        IReadOnlyList<string> keys,
        IReadOnlyList<double>? weights,
        SortedSetAggregate? aggregate,
        bool withScores)
    {
        ArgumentNullException.ThrowIfNull(keys);

        if (weights is not null && weights.Count != keys.Count)
        {
            throw new ArgumentException("Give one weight per key.", nameof(weights));
        }

        var parts = new List<string>(4 + keys.Count + (weights?.Count + 1 ?? 0) + 3) { name };

        if (destination is not null)
        {
            parts.Add(destination);
        }

        parts.Add(RespRead.Number(keys.Count));
        parts.AddRange(keys);

        if (weights is not null)
        {
            parts.Add("WEIGHTS");

            foreach (double weight in weights)
            {
                parts.Add(RespRead.Number(weight));
            }
        }

        if (aggregate is { } mode)
        {
            parts.Add("AGGREGATE");
            parts.Add(RespRead.Word(mode));
        }

        if (withScores)
        {
            parts.Add("WITHSCORES");
        }

        return new RespCommand(parts);
    }

    internal static RespCommand IncrementHashFieldByFloat(string key, string field, double delta) =>
        new("HINCRBYFLOAT", key, field, RespRead.Number(delta));

    internal static RespCommand ReserveBloom(string key, double errorRate, long capacity, long? expansion, bool nonScaling)
    {
        var command = new RespCommand("BF.RESERVE", key, RespRead.Number(errorRate), capacity);

        if (expansion is { } growth)
        {
            command = command.With("EXPANSION").With(growth);
        }

        return nonScaling ? command.With("NONSCALING") : command;
    }

    internal static RespCommand InsertBloom(string key, IReadOnlyList<string> items, BloomInsertOptions? options)
    {
        ArgumentNullException.ThrowIfNull(items);
        var parts = new List<string>(14 + items.Count) { "BF.INSERT", key };

        if (options is not null)
        {
            if (options.Ttl is { } lifetime)
            {
                (string word, long value) = RespRead.CreateTtl(lifetime, nameof(options.Ttl));

                parts.Add(word);
                parts.Add(RespRead.Number(value));
            }

            if (options.Capacity is { } capacity)
            {
                parts.Add("CAPACITY");
                parts.Add(RespRead.Number(capacity));
            }

            if (options.ErrorRate is { } error)
            {
                parts.Add("ERROR");
                parts.Add(RespRead.Number(error));
            }

            if (options.Expansion is { } expansion)
            {
                parts.Add("EXPANSION");
                parts.Add(RespRead.Number(expansion));
            }

            if (options.NoCreate)
            {
                parts.Add("NOCREATE");
            }

            if (options.NonScaling)
            {
                parts.Add("NONSCALING");
            }
        }

        parts.Add("ITEMS");
        parts.AddRange(items);

        return new RespCommand(parts);
    }

    internal static RespCommand JsonSet(string key, string path, string json, SetMode mode) => mode switch
    {
        SetMode.IfNotExists => new RespCommand("JSON.SET", key, path, json, "NX"),
        SetMode.IfExists => new RespCommand("JSON.SET", key, path, json, "XX"),
        _ => new RespCommand("JSON.SET", key, path, json),
    };

    internal static RespCommand JsonArrayInsert(string key, string path, long index, string[] values)
    {
        ArgumentNullException.ThrowIfNull(values);

        if (values.Length <= 2)
        {
            RespCommand command = new("JSON.ARRINSERT", key, path, index);

            foreach (string value in values)
            {
                command = command.With(value);
            }

            return command;
        }

        var parts = new string[values.Length + 4];
        parts[0] = "JSON.ARRINSERT";
        parts[1] = key;
        parts[2] = path;
        parts[3] = RespRead.Number(index);
        values.CopyTo(parts, 4);

        return new RespCommand(parts);
    }

    internal static RespCommand JsonArrayIndex(string key, string path, string scalar, long start, long stop) =>
        start == 0 && stop == 0
            ? new RespCommand("JSON.ARRINDEX", key, path, scalar)
            : new RespCommand("JSON.ARRINDEX", key, path, scalar, start, stop);

    internal static RespCommand JsonCreateIndex(string name, string prefix, string path, JsonIndexType type) =>
        new(["JSON.INDEX", "CREATE", name, "PREFIX", prefix, "PATH", path, "TYPE", RespRead.Word(type)]);

    internal static RespCommand JsonFind(string index, string value, int? count, string? after) => (count, after) switch
    {
        (null, null) => new RespCommand("JSON.FIND", index, "VALUE", value),
        ({ } size, null) => new RespCommand("JSON.FIND", index, "VALUE", value, "COUNT", size),
        (null, { } last) => new RespCommand("JSON.FIND", index, "VALUE", value, "AFTER", last),
        ({ } size, { } last) => new RespCommand(["JSON.FIND", index, "VALUE", value, "COUNT", RespRead.Number(size), "AFTER", last]),
    };

    internal static RespCommand Words(string name, string subcommand, string[] names)
    {
        ArgumentNullException.ThrowIfNull(names);
        var parts = new string[names.Length + 2];
        parts[0] = name;
        parts[1] = subcommand;
        names.CopyTo(parts, 2);

        return new RespCommand(parts);
    }

    internal static RespCommand Wait(long replicas, TimeSpan timeout) =>
        new("WAIT", replicas, timeout <= TimeSpan.Zero ? 0L : RespRead.Milliseconds(timeout));

    private static long PositiveMilliseconds(TimeSpan ttl, string name) =>
        ttl > TimeSpan.Zero ? RespRead.Milliseconds(ttl) : throw new ArgumentOutOfRangeException(name, "Expiry must be positive.");
}
