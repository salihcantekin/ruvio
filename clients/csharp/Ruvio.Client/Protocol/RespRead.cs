using System.Globalization;
using System.Runtime.CompilerServices;

namespace Ruvio.Client;

/// <summary>
/// Reply shapes shared by <see cref="RuvioClient"/> and the default <see cref="IRuvioClient"/> members.
/// Each awaiting helper uses a pooled state machine, so a typed call adds no allocation beyond its result.
/// </summary>
internal static class RespRead
{
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal static async ValueTask<long> Integer(ValueTask<RespValue> pending, string name) =>
        Integer(await pending.ConfigureAwait(false), name);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal static async ValueTask<bool> Flag(ValueTask<RespValue> pending, string name) =>
        Integer(await pending.ConfigureAwait(false), name) > 0;

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal static async ValueTask<bool> NotNull(ValueTask<RespValue> pending) =>
        (await pending.ConfigureAwait(false)).Kind is not RespKind.Null;

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal static async ValueTask Ok(ValueTask<RespValue> pending) => _ = await pending.ConfigureAwait(false);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal static async ValueTask<long?> OptionalInteger(ValueTask<RespValue> pending, string name)
    {
        RespValue reply = await pending.ConfigureAwait(false);

        return reply.Kind switch
        {
            RespKind.Null => null,
            RespKind.Integer => reply.Integer,
            _ => throw new RuvioException($"{name} returned {reply.Kind}"),
        };
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal static async ValueTask<string?> Text(ValueTask<RespValue> pending)
    {
        RespValue reply = await pending.ConfigureAwait(false);

        return reply.Kind is RespKind.Null ? null : reply.AsString();
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal static async ValueTask<string> RequiredText(ValueTask<RespValue> pending, string name) =>
        (await pending.ConfigureAwait(false)).AsString() ?? throw new RuvioException($"{name} returned a null reply");

    internal static async ValueTask<double?> OptionalDouble(ValueTask<RespValue> pending, string name)
    {
        RespValue reply = await pending.ConfigureAwait(false);

        return reply.Kind is RespKind.Null
            ? null
            : ParseDouble(reply.AsString() ?? throw new RuvioException($"{name} returned a null reply"));
    }

    internal static async ValueTask<IReadOnlyList<long?>> BitFieldResults(ValueTask<RespValue> pending, string name)
    {
        RespValue reply = await pending.ConfigureAwait(false);

        if (reply.Items is not { } items)
        {
            throw new RuvioException($"{name} returned {reply.Kind}");
        }

        var results = new long?[items.Count];

        for (int i = 0; i < results.Length; i++)
        {
            results[i] = items[i].Kind switch
            {
                RespKind.Null => null,
                RespKind.Integer => items[i].Integer,
                _ => throw new RuvioException($"{name} returned {items[i].Kind} in an array"),
            };
        }

        return results;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal static async ValueTask<double> Double(ValueTask<RespValue> pending, string name) =>
        ParseDouble((await pending.ConfigureAwait(false)).AsString() ?? throw new RuvioException($"{name} returned a null reply"));

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal static async ValueTask<IReadOnlyList<string>> Strings(ValueTask<RespValue> pending, string name) =>
        Strings(await pending.ConfigureAwait(false), name);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal static async ValueTask<IReadOnlyList<bool>> Flags(ValueTask<RespValue> pending, string name) =>
        Flags(await pending.ConfigureAwait(false), name);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal static async ValueTask<IReadOnlyList<double?>> Scores(ValueTask<RespValue> pending, string name) =>
        Scores(await pending.ConfigureAwait(false), name);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal static async ValueTask<IReadOnlyList<SortedSetEntry>> Entries(ValueTask<RespValue> pending, string name) =>
        Entries(await pending.ConfigureAwait(false), name);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal static async ValueTask<IReadOnlyDictionary<string, string>> Map(ValueTask<RespValue> pending, string name)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (var (field, value) in Pairs(await pending.ConfigureAwait(false), name))
        {
            map[field] = value;
        }

        return map;
    }

    internal static async ValueTask<IReadOnlyList<KeyValuePair<string, string>>> Pairs(ValueTask<RespValue> pending, string name) =>
        Pairs(await pending.ConfigureAwait(false), name);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal static async ValueTask<SetScanPage> SetScan(ValueTask<RespValue> pending)
    {
        (long cursor, IReadOnlyList<RespValue> items) = Page(await pending.ConfigureAwait(false), "SSCAN");
        var members = new string[items.Count];

        for (int index = 0; index < members.Length; index++)
        {
            members[index] = items[index].AsString() ?? "";
        }

        return new SetScanPage(cursor, members);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal static async ValueTask<SortedSetScanPage> SortedSetScan(ValueTask<RespValue> pending)
    {
        (long cursor, IReadOnlyList<RespValue> items) = Page(await pending.ConfigureAwait(false), "ZSCAN");

        return new SortedSetScanPage(cursor, EntriesOf(items, "ZSCAN"));
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal static async ValueTask<BloomFilterInfo> BloomInfo(ValueTask<RespValue> pending)
    {
        RespValue reply = await pending.ConfigureAwait(false);

        if (reply.Items is not { } items || items.Count % 2 != 0)
        {
            throw new RuvioException($"BF.INFO returned {reply.Kind}");
        }

        long capacity = 0, size = 0, filters = 0, inserted = 0;
        long? expansion = null;

        for (int index = 0; index < items.Count; index += 2)
        {
            RespValue value = items[index + 1];

            switch (items[index].AsString())
            {
                case "Capacity": capacity = value.Integer; break;
                case "Size": size = value.Integer; break;
                case "Number of filters": filters = value.Integer; break;
                case "Number of items inserted": inserted = value.Integer; break;
                case "Expansion rate": expansion = value.Kind is RespKind.Null ? null : value.Integer; break;
            }
        }

        return new BloomFilterInfo(capacity, size, filters, inserted, expansion);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal static async ValueTask<KeyValuePair<string, IReadOnlyList<string>>?> ListPop(ValueTask<RespValue> pending, string name)
    {
        RespValue reply = await pending.ConfigureAwait(false);

        if (reply.Kind is RespKind.Null)
        {
            return null;
        }

        if (reply.Items is not { Count: 2 } items || items[1].Items is null)
        {
            throw new RuvioException($"{name} returned {reply.Kind}");
        }

        return new KeyValuePair<string, IReadOnlyList<string>>(items[0].AsString() ?? "", Strings(items[1], name));
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal static async ValueTask<IdempotencyBegin> Idempotency(ValueTask<RespValue> pending)
    {
        RespValue reply = await pending.ConfigureAwait(false);

        if (reply.Items is not { Count: >= 1 } items)
        {
            throw new RuvioException($"IDEM returned {reply.Kind}");
        }

        return items[0].AsString() switch
        {
            "acquired" => new IdempotencyBegin(IdempotencyState.Acquired, null),
            "pending" => new IdempotencyBegin(IdempotencyState.Pending, null),
            "conflict" => new IdempotencyBegin(IdempotencyState.Conflict, null),
            "completed" when items.Count >= 2 => new IdempotencyBegin(IdempotencyState.Completed, items[1].Bytes ?? []),
            _ => throw new RuvioException("IDEM returned an unexpected state"),
        };
    }

    internal static long Integer(RespValue reply, string name) =>
        reply.Kind is RespKind.Integer ? reply.Integer : throw new RuvioException($"{name} returned {reply.Kind}");

    internal static IReadOnlyList<string> Strings(RespValue reply, string name)
    {
        if (reply.Kind is RespKind.Null)
        {
            return [];
        }

        IReadOnlyList<RespValue> items = reply.Items ?? throw new RuvioException($"{name} returned {reply.Kind}");
        var values = new string[items.Count];

        for (int index = 0; index < values.Length; index++)
        {
            values[index] = items[index].AsString() ?? "";
        }

        return values;
    }

    /// <summary>An array of 0/1 integers. An error element (for example a full non-scaling filter) throws.</summary>
    internal static IReadOnlyList<bool> Flags(RespValue reply, string name)
    {
        IReadOnlyList<RespValue> items = reply.Items ?? throw new RuvioException($"{name} returned {reply.Kind}");
        var values = new bool[items.Count];

        for (int index = 0; index < values.Length; index++)
        {
            RespValue item = items[index];

            if (item.Kind is RespKind.Error)
            {
                throw new RuvioException(item.Text ?? "ERR", isServerReply: true);
            }

            values[index] = Integer(item, name) > 0;
        }

        return values;
    }

    internal static IReadOnlyList<double?> Scores(RespValue reply, string name)
    {
        IReadOnlyList<RespValue> items = reply.Items ?? throw new RuvioException($"{name} returned {reply.Kind}");
        var values = new double?[items.Count];

        for (int index = 0; index < values.Length; index++)
        {
            values[index] = items[index].Kind is RespKind.Null ? null : ParseDouble(items[index].AsString() ?? "0");
        }

        return values;
    }

    internal static IReadOnlyList<SortedSetEntry> Entries(RespValue reply, string name) =>
        (reply.Kind is RespKind.Null) ? Array.Empty<SortedSetEntry>() : EntriesOf(reply.Items ?? throw new RuvioException($"{name} returned {reply.Kind}"), name);

    internal static IReadOnlyList<KeyValuePair<string, string>> Pairs(RespValue reply, string name)
    {
        if (reply.Kind is RespKind.Null)
        {
            return [];
        }

        IReadOnlyList<RespValue> items = reply.Items ?? throw new RuvioException($"{name} returned {reply.Kind}");

        if (items.Count % 2 != 0)
        {
            throw new RuvioException($"{name} reply is not pairs");
        }

        var pairs = new KeyValuePair<string, string>[items.Count / 2];

        for (int index = 0; index < pairs.Length; index++)
        {
            pairs[index] = new(items[index * 2].AsString() ?? "", items[(index * 2) + 1].AsString() ?? "");
        }

        return pairs;
    }

    private static SortedSetEntry[] EntriesOf(IReadOnlyList<RespValue> items, string name)
    {
        if (items.Count % 2 != 0)
        {
            throw new RuvioException($"{name} reply is not member/score pairs");
        }

        var entries = new SortedSetEntry[items.Count / 2];

        for (int index = 0; index < entries.Length; index++)
        {
            entries[index] = new SortedSetEntry(
                items[index * 2].AsString() ?? "",
                ParseDouble(items[(index * 2) + 1].AsString() ?? "0"));
        }

        return entries;
    }

    private static (long Cursor, IReadOnlyList<RespValue> Items) Page(RespValue reply, string name)
    {
        if (reply.Items is not { Count: 2 } parts)
        {
            throw new RuvioException($"{name} returned an unexpected shape");
        }

        long cursor = long.Parse(parts[0].AsString() ?? "0", NumberStyles.Integer, CultureInfo.InvariantCulture);

        return (cursor, parts[1].Items ?? []);
    }

    internal static double ParseDouble(string text) => text switch
    {
        "inf" or "+inf" => double.PositiveInfinity,
        "-inf" => double.NegativeInfinity,
        _ => double.Parse(text, NumberStyles.Float, CultureInfo.InvariantCulture),
    };

    internal static string Number(double value) => value switch
    {
        double.PositiveInfinity => "+inf",
        double.NegativeInfinity => "-inf",
        _ => value.ToString("R", CultureInfo.InvariantCulture),
    };

    internal static string Number(long value) => value.ToString(CultureInfo.InvariantCulture);

    internal static long Milliseconds(TimeSpan span) => Math.Max(1, (long)Math.Ceiling(span.TotalMilliseconds));

    internal static long Seconds(TimeSpan span) => (long)Math.Ceiling(span.TotalSeconds);

    /// <summary>
    /// The <c>EX</c> or <c>PX</c> word and value for a deadline that is set only when a command creates the key:
    /// <c>EX</c> for a whole number of seconds, otherwise <c>PX</c> with the time rounded up to milliseconds.
    /// </summary>
    internal static (string Word, long Value) CreateTtl(TimeSpan ttl, string name)
    {
        if (ttl <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(name, "Expiry must be positive.");
        }

        long milliseconds = Milliseconds(ttl);

        return milliseconds % 1000 == 0 ? ("EX", milliseconds / 1000) : ("PX", milliseconds);
    }

    internal static string Word(BitOperation operation) => operation switch
    {
        BitOperation.And => "AND",
        BitOperation.Or => "OR",
        BitOperation.Xor => "XOR",
        BitOperation.Not => "NOT",
        _ => throw new ArgumentOutOfRangeException(nameof(operation)),
    };

    internal static string Word(BitUnit unit) => unit == BitUnit.Bit ? "BIT" : "BYTE";

    internal static string Word(SortedSetAggregate aggregate) => aggregate switch
    {
        SortedSetAggregate.Sum => "SUM",
        SortedSetAggregate.Min => "MIN",
        SortedSetAggregate.Max => "MAX",
        _ => throw new ArgumentOutOfRangeException(nameof(aggregate)),
    };

    internal static string Word(JsonIndexType type) => type switch
    {
        JsonIndexType.String => "string",
        JsonIndexType.Number => "number",
        JsonIndexType.Bool => "bool",
        _ => throw new ArgumentOutOfRangeException(nameof(type)),
    };
}
