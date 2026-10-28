using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;

namespace Ruvio.Client;

public sealed partial class RuvioClient
{
    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> GetKeysAsync(string pattern, CancellationToken cancellationToken = default)
    {
        return ArrayOfBulks(new RespCommand("KEYS", pattern), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<string?> GetRandomKeyAsync(CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("RANDOMKEY"), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> TouchAsync(params string[] keys)
    {
        return Integer(Join("TOUCH", keys));
    }

    /// <inheritdoc />
    public ValueTask<bool> RenameIfNotExistsAsync(string key, string newKey, CancellationToken cancellationToken = default)
    {
        return Flag(new RespCommand("RENAMENX", key, newKey), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> GetExpireTimeAsync(string key, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("EXPIRETIME", key), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> GetExpireTimeMillisecondsAsync(string key, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("PEXPIRETIME", key), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<bool> CopyAsync(string source, string destination, bool replace = false, CancellationToken cancellationToken = default)
    {
        return replace
            ? Flag(new RespCommand("COPY", source, destination, "REPLACE"), cancellationToken)
            : Flag(new RespCommand("COPY", source, destination), cancellationToken);
    }

    /// <inheritdoc />
    public async ValueTask<string> EchoAsync(string message, CancellationToken cancellationToken = default)
    {
        return (await ExecuteCommandAsync(new RespCommand("ECHO", message), cancellationToken).ConfigureAwait(false)).AsString()
            ?? throw RuvioException.Client("ECHO returned a null reply");
    }

    /// <inheritdoc />
    public ValueTask<string?> GetObjectEncodingAsync(string key, CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("OBJECT", "ENCODING", key), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long?> GetObjectIdleSecondsAsync(string key, CancellationToken cancellationToken = default)
    {
        return OptionalInteger(new RespCommand("OBJECT", "IDLETIME", key), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long?> GetObjectFrequencyAsync(string key, CancellationToken cancellationToken = default)
    {
        return OptionalInteger(new RespCommand("OBJECT", "FREQ", key), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long?> GetMemoryUsageAsync(string key, CancellationToken cancellationToken = default)
    {
        return OptionalInteger(new RespCommand("MEMORY", "USAGE", key), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> GetSlowLogLengthAsync(CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("SLOWLOG", "LEN"), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask ResetSlowLogAsync(CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("SLOWLOG", "RESET"), cancellationToken);
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<RespValue>> GetSlowLogAsync(long count = 10, CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteCommandAsync(new RespCommand("SLOWLOG", "GET", count), cancellationToken).ConfigureAwait(false);

        return reply.Items ?? throw RuvioException.Client($"SLOWLOG returned {reply.Kind}");
    }

    /// <inheritdoc />
    public ValueTask<long> GetClientIdAsync(CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("CLIENT", "ID"), cancellationToken);
    }

    /// <inheritdoc />
    public async ValueTask<string> GetClientInfoAsync(CancellationToken cancellationToken = default)
    {
        return (await ExecuteCommandAsync(new RespCommand("CLIENT", "INFO"), cancellationToken).ConfigureAwait(false)).AsString()
            ?? throw RuvioException.Client("CLIENT INFO returned a null reply");
    }

    /// <inheritdoc />
    public async ValueTask<string> GetClientListAsync(CancellationToken cancellationToken = default)
    {
        return (await ExecuteCommandAsync(new RespCommand("CLIENT", "LIST"), cancellationToken).ConfigureAwait(false)).AsString()
            ?? throw RuvioException.Client("CLIENT LIST returned a null reply");
    }

    /// <inheritdoc />
    public async ValueTask<string> GetClientListAsync(ClientType type, CancellationToken cancellationToken = default)
    {
        return (await ExecuteCommandAsync(new RespCommand("CLIENT", "LIST", "TYPE", Keywords.Arg(type)), cancellationToken).ConfigureAwait(false)).AsString()
            ?? throw RuvioException.Client("CLIENT LIST returned a null reply");
    }

    /// <inheritdoc />
    public ValueTask<long> KillClientAsync(long id, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("CLIENT", "KILL", "ID", id), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> InsertListAsync(string key, bool before, string pivot, string value, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("LINSERT", key, before ? "BEFORE" : "AFTER", pivot, value), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> InsertListAsync(string key, InsertPosition position, string pivot, string value, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("LINSERT", key, Keywords.Arg(position), pivot, value), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask SetListItemAsync(string key, long index, string value, CancellationToken cancellationToken = default)
    {
        return Ok(new RespCommand("LSET", key, index, value), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> RemoveListItemsAsync(string key, long count, string value, CancellationToken cancellationToken = default)
    {
        return Integer(new RespCommand("LREM", key, count, value), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long?> GetListPositionAsync(string key, string value, CancellationToken cancellationToken = default)
    {
        return OptionalInteger(new RespCommand("LPOS", key, value), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long> PushListLeftIfExistsAsync(string key, params string[] values)
    {
        return Integer(RespCommand.Keyed("LPUSHX", key, values));
    }

    /// <inheritdoc />
    public ValueTask<long> PushListRightIfExistsAsync(string key, params string[] values)
    {
        return Integer(RespCommand.Keyed("RPUSHX", key, values));
    }

    /// <inheritdoc />
    public ValueTask<string?> MoveListItemAsync(
        string source,
        string destination,
        bool fromLeft,
        bool toLeft,
        CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("LMOVE", source, destination, Side(fromLeft), Side(toLeft)), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<string?> MoveListItemAsync(
        string source,
        string destination,
        ListSide from,
        ListSide to,
        CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("LMOVE", source, destination, Keywords.Arg(from), Keywords.Arg(to)), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<string?> PopListRightPushLeftAsync(string source, string destination, CancellationToken cancellationToken = default)
    {
        return BulkOrNull(new RespCommand("RPOPLPUSH", source, destination), cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<string?> MoveListItemBlockingAsync(
        string source,
        string destination,
        bool fromLeft,
        bool toLeft,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        return BulkOrNull(
            new RespCommand("BLMOVE", source, destination, Side(fromLeft), Side(toLeft), Number((long)Math.Ceiling(timeout.TotalSeconds))),
            cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<string?> MoveListItemBlockingAsync(
        string source,
        string destination,
        ListSide from,
        ListSide to,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        return BulkOrNull(
            new RespCommand("BLMOVE", source, destination, Keywords.Arg(from), Keywords.Arg(to), Number((long)Math.Ceiling(timeout.TotalSeconds))),
            cancellationToken);
    }

    /// <inheritdoc />
    public async ValueTask<KeyValuePair<string, IReadOnlyList<string>>?> PopListManyAsync(
        IReadOnlyList<string> keys,
        bool fromLeft,
        long count = 1,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keys);

        var arguments = new List<string> { "LMPOP", Number(keys.Count) };

        arguments.AddRange(keys);
        arguments.Add(Side(fromLeft));
        arguments.Add("COUNT");
        arguments.Add(Number(count));
        RespValue reply = await ExecuteAsync(arguments, cancellationToken).ConfigureAwait(false);

        if (reply.Kind is RespKind.Null)
        {
            return null;
        }

        if (reply.Items is not { Count: 2 } items || items[1].Items is not { } elements)
        {
            throw RuvioException.Client($"LMPOP returned {reply.Kind}");
        }

        var values = new List<string>(elements.Count);

        foreach (RespValue element in elements)
        {
            values.Add(element.AsString() ?? "");
        }

        return new KeyValuePair<string, IReadOnlyList<string>>(items[0].AsString() ?? "", values);
    }

    /// <inheritdoc />
    public ValueTask<KeyValuePair<string, IReadOnlyList<string>>?> PopListManyAsync(
        IReadOnlyList<string> keys,
        ListSide side,
        long count = 1,
        CancellationToken cancellationToken = default)
    {
        return PopListManyAsync(keys, side == ListSide.Left, count, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<long?>> BitFieldAsync(string key, BitFieldStep first, params BitFieldStep[] rest)
    {
        return BitField("BITFIELD", key, BitFieldStep.Words(first, rest));
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<long?>> BitFieldReadOnlyAsync(string key, BitFieldStep first, params BitFieldStep[] rest)
    {
        return BitField("BITFIELD_RO", key, BitFieldStep.Words(first, rest));
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<long?>> BitFieldAsync(string key, params string[] operations)
    {
        return BitField("BITFIELD", key, operations);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<long?>> BitFieldReadOnlyAsync(string key, params string[] operations)
    {
        return BitField("BITFIELD_RO", key, operations);
    }

    private async ValueTask<IReadOnlyList<long?>> BitField(string name, string key, string[] operations)
    {
        ArgumentNullException.ThrowIfNull(operations);

        var arguments = new List<string> { name, key };

        arguments.AddRange(operations);
        RespValue reply = await ExecuteAsync(arguments, CancellationToken.None).ConfigureAwait(false);

        if (reply.Items is not { } items)
        {
            throw RuvioException.Client($"{name} returned {reply.Kind}");
        }

        var results = new long?[items.Count];

        for (int i = 0; i < results.Length; i++)
        {
            results[i] = items[i].Kind switch
            {
                RespKind.Null => null,
                RespKind.Integer => items[i].Integer,
                _ => throw RuvioException.Client($"{name} returned {items[i].Kind} in an array"),
            };
        }

        return results;
    }

    /// <inheritdoc />
    public async ValueTask<byte[]?> DumpAsync(string key, CancellationToken cancellationToken = default)
    {
        RespValue reply = await ExecuteBinaryAsync([Bytes("DUMP"), Bytes(key)], cancellationToken).ConfigureAwait(false);

        return reply.Kind switch
        {
            RespKind.Null => null,
            RespKind.Bulk => reply.Bytes ?? [],
            _ => throw RuvioException.Client($"DUMP returned {reply.Kind}"),
        };
    }

    /// <inheritdoc />
    public async ValueTask RestoreAsync(
        string key,
        byte[] payload,
        long ttlMilliseconds = 0,
        bool replace = false,
        bool absoluteTtl = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(payload);

        var arguments = new List<ReadOnlyMemory<byte>> { Bytes("RESTORE"), Bytes(key), Bytes(Number(ttlMilliseconds)), payload };

        if (replace)
        {
            arguments.Add(Bytes("REPLACE"));
        }

        if (absoluteTtl)
        {
            arguments.Add(Bytes("ABSTTL"));
        }

        _ = await ExecuteBinaryAsync(arguments, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<FillResult> FillGetAsync(
        string key,
        TimeSpan fill,
        TimeSpan? wait = null,
        CancellationToken cancellationToken = default)
    {
        RespCommand command = wait is { } limit
            ? new RespCommand("FILLGET", key, Milliseconds(fill), "WAIT", Milliseconds(limit))
            : new RespCommand("FILLGET", key, Milliseconds(fill));

        return FillResult.FromReply(await ExecuteCommandAsync(command, cancellationToken).ConfigureAwait(false));
    }

    /// <inheritdoc />
    public ValueTask<bool> FillSetAsync(
        string key,
        string value,
        TimeSpan ttl,
        long token,
        TimeSpan? stale = null,
        CancellationToken cancellationToken = default)
    {
        return stale is { } window
            ? Flag(new RespCommand(["FILLSET", key, value, Number(Milliseconds(ttl)), Number(token), "SWR", Number(Milliseconds(window))]), cancellationToken)
            : Flag(new RespCommand("FILLSET", key, value, Milliseconds(ttl), token), cancellationToken);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<long?> OptionalInteger(RespCommand arguments, CancellationToken cancellationToken)
    {
        RespValue reply = await ExecuteCommandAsync(arguments, cancellationToken).ConfigureAwait(false);

        return reply.Kind switch
        {
            RespKind.Null => null,
            RespKind.Integer => reply.Integer,
            _ => throw RuvioException.Client($"{arguments.Name} returned {reply.Kind}"),
        };
    }

    private static string Side(bool left) => left ? "LEFT" : "RIGHT";

    private static long Milliseconds(TimeSpan span) => Math.Max(1, (long)Math.Ceiling(span.TotalMilliseconds));

    private static ReadOnlyMemory<byte> Bytes(string text) => Encoding.UTF8.GetBytes(text);
}
