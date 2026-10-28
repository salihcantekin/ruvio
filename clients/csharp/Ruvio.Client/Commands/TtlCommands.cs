namespace Ruvio.Client;

/// <summary>
/// Builds the frames for the deadline set only when a command creates its key (<c>EX</c> / <c>PX</c>) and for the
/// <c>EXPIRE</c> family with a condition. <see cref="RuvioClient"/> sends them on the inline <see cref="RespCommand"/> path;
/// the default <see cref="IRuvioClient"/> members send <c>AsList()</c>.
/// </summary>
internal static class TtlCommands
{
    internal static RespCommand WithCreateTtl(RespCommand command, TimeSpan ttl, string name)
    {
        (string word, long value) = RespRead.CreateTtl(ttl, name);

        return command.With(word).With(value);
    }

    internal static RespCommand Increment(string key, TimeSpan ttl) =>
        WithCreateTtl(new RespCommand("INCR", key), ttl, nameof(ttl));

    internal static RespCommand Decrement(string key, TimeSpan ttl) =>
        WithCreateTtl(new RespCommand("DECR", key), ttl, nameof(ttl));

    internal static RespCommand IncrementBy(string key, long delta, TimeSpan ttl) =>
        WithCreateTtl(new RespCommand("INCRBY", key, delta), ttl, nameof(ttl));

    internal static RespCommand DecrementBy(string key, long delta, TimeSpan ttl) =>
        WithCreateTtl(new RespCommand("DECRBY", key, delta), ttl, nameof(ttl));

    internal static RespCommand IncrementWithLimit(string key, long delta, long limit, TimeSpan ttl) =>
        WithCreateTtl(new RespCommand("INCRBY", key, delta), ttl, nameof(ttl)).With("MAX").With(limit);

    internal static RespCommand IncrementByFloat(string key, double delta, TimeSpan ttl) =>
        WithCreateTtl(new RespCommand("INCRBYFLOAT", key, RespRead.Number(delta)), ttl, nameof(ttl));

    internal static RespCommand IncrementHashField(string key, string field, long delta, TimeSpan ttl) =>
        WithCreateTtl(new RespCommand("HINCRBY", key, field, delta), ttl, nameof(ttl));

    internal static RespCommand IncrementSortedSet(string key, string member, double delta, TimeSpan ttl) =>
        WithCreateTtl(new RespCommand("ZINCRBY", key, RespRead.Number(delta), member), ttl, nameof(ttl));

    internal static RespCommand SetBit(string key, long offset, bool value, TimeSpan ttl) =>
        WithCreateTtl(new RespCommand("SETBIT", key, offset, value ? 1L : 0L), ttl, nameof(ttl));

    internal static RespCommand SortedSetAdd(string key, TimeSpan ttl, SortedSetAddFlags flags, SortedSetEntry[] entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        (string word, long value) = RespRead.CreateTtl(ttl, nameof(ttl));
        var parts = new List<string>(9 + (entries.Length * 2)) { "ZADD", key, word, RespRead.Number(value) };

        if (flags.HasFlag(SortedSetAddFlags.IfNotExists))
        {
            parts.Add("NX");
        }

        if (flags.HasFlag(SortedSetAddFlags.IfExists))
        {
            parts.Add("XX");
        }

        if (flags.HasFlag(SortedSetAddFlags.GreaterThan))
        {
            parts.Add("GT");
        }

        if (flags.HasFlag(SortedSetAddFlags.LessThan))
        {
            parts.Add("LT");
        }

        if (flags.HasFlag(SortedSetAddFlags.Changed))
        {
            parts.Add("CH");
        }

        foreach (SortedSetEntry entry in entries)
        {
            parts.Add(RespRead.Number(entry.Score));
            parts.Add(entry.Member);
        }

        return new RespCommand(parts);
    }

    internal static RespCommand GeoAdd(string key, TimeSpan ttl, (double Longitude, double Latitude, string Member)[] places)
    {
        ArgumentNullException.ThrowIfNull(places);
        (string word, long value) = RespRead.CreateTtl(ttl, nameof(ttl));
        var parts = new List<string>(4 + (places.Length * 3)) { "GEOADD", key, word, RespRead.Number(value) };

        foreach ((double longitude, double latitude, string member) in places)
        {
            parts.Add(RespRead.Number(longitude));
            parts.Add(RespRead.Number(latitude));
            parts.Add(member);
        }

        return new RespCommand(parts);
    }

    internal static RespCommand StreamAdd(string key, TimeSpan ttl, string id, (string Field, string Value)[] fields)
    {
        ArgumentNullException.ThrowIfNull(fields);
        (string word, long value) = RespRead.CreateTtl(ttl, nameof(ttl));
        var parts = new List<string>(5 + (fields.Length * 2)) { "XADD", key, word, RespRead.Number(value), id };

        foreach ((string field, string fieldValue) in fields)
        {
            parts.Add(field);
            parts.Add(fieldValue);
        }

        return new RespCommand(parts);
    }

    internal static RespCommand JsonSet(string key, string path, string json, SetMode mode, TimeSpan ttl)
    {
        var command = new RespCommand("JSON.SET", key, path, json);

        if (mode == SetMode.IfNotExists)
        {
            command = command.With("NX");
        }
        else if (mode == SetMode.IfExists)
        {
            command = command.With("XX");
        }

        return WithCreateTtl(command, ttl, nameof(ttl));
    }

    internal static RespCommand Expire(string key, TimeSpan ttl, ExpireCondition condition)
    {
        if (ttl < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(ttl), "Expiry cannot be negative.");
        }

        long milliseconds = (long)Math.Ceiling(ttl.TotalMilliseconds);
        RespCommand command = milliseconds % 1000 == 0
            ? new RespCommand("EXPIRE", key, milliseconds / 1000)
            : new RespCommand("PEXPIRE", key, milliseconds);

        return command.With(Keywords.Arg(condition));
    }

    internal static RespCommand ExpireAt(string key, DateTimeOffset when, ExpireCondition condition)
    {
        long milliseconds = when.ToUnixTimeMilliseconds();
        RespCommand command = milliseconds % 1000 == 0
            ? new RespCommand("EXPIREAT", key, milliseconds / 1000)
            : new RespCommand("PEXPIREAT", key, milliseconds);

        return command.With(Keywords.Arg(condition));
    }
}
