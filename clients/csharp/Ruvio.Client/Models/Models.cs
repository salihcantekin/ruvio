namespace Ruvio.Client;

/// <summary>A sorted-set member and its score.</summary>
public readonly record struct SortedSetEntry(string Member, double Score);

/// <summary>When <c>HEXPIRE</c> and its siblings may change a field TTL. A field without a TTL counts as an infinite TTL for <see cref="IfGreater"/> and <see cref="IfLess"/>.</summary>
public enum FieldExpireCondition
{
    /// <summary>Always set the TTL.</summary>
    Always,

    /// <summary>Only when the field has no TTL (<c>NX</c>).</summary>
    IfNoTtl,

    /// <summary>Only when the field already has a TTL (<c>XX</c>).</summary>
    IfTtl,

    /// <summary>Only when the new deadline is later (<c>GT</c>).</summary>
    IfGreater,

    /// <summary>Only when the new deadline is earlier (<c>LT</c>).</summary>
    IfLess,
}

/// <summary><c>ZADD</c> options. The server refuses <c>NX</c> with <c>XX</c>, <c>GT</c>, or <c>LT</c>, and <c>GT</c> with <c>LT</c>.</summary>
[Flags]
public enum SortedSetAddFlags
{
    /// <summary>Add or update every member.</summary>
    None = 0,

    /// <summary>Only add new members (<c>NX</c>).</summary>
    IfNotExists = 1,

    /// <summary>Only update existing members (<c>XX</c>).</summary>
    IfExists = 2,

    /// <summary>Only update when the new score is greater (<c>GT</c>). A missing member is still added.</summary>
    GreaterThan = 4,

    /// <summary>Only update when the new score is lower (<c>LT</c>). A missing member is still added.</summary>
    LessThan = 8,

    /// <summary>Count changed members as well as new ones (<c>CH</c>).</summary>
    Changed = 16,
}

/// <summary><c>XREAD</c> and <c>XREADGROUP</c> options.</summary>
public sealed class StreamReadOptions
{
    /// <summary>Most entries to return per stream (<c>COUNT</c>).</summary>
    public int? Count { get; init; }

    /// <summary>
    /// How long to wait for a new entry (<c>BLOCK</c>). <see cref="TimeSpan.Zero"/> waits until an entry
    /// arrives. The connection sends nothing else while it waits.
    /// </summary>
    public TimeSpan? Block { get; init; }

    /// <summary><c>XREADGROUP</c> only: advance the group without adding pending entries (<c>NOACK</c>).</summary>
    public bool NoAcknowledge { get; init; }
}

/// <summary><c>XCLAIM</c> options.</summary>
public sealed class StreamClaimOptions
{
    /// <summary>Set the claimed entries' idle time (<c>IDLE</c>). Cannot be combined with <see cref="Time"/>.</summary>
    public TimeSpan? Idle { get; init; }

    /// <summary>Set the claimed entries' last delivery time (<c>TIME</c>).</summary>
    public DateTimeOffset? Time { get; init; }

    /// <summary>Set the delivery count (<c>RETRYCOUNT</c>).</summary>
    public long? RetryCount { get; init; }

    /// <summary>Claim ids that are in the stream but not pending (<c>FORCE</c>).</summary>
    public bool Force { get; init; }

    /// <summary>Move the group's last delivered id (<c>LASTID</c>).</summary>
    public string? LastId { get; init; }
}

/// <summary>One row of the extended <c>XPENDING</c> form.</summary>
public readonly record struct StreamPendingEntry(string Id, string Consumer, long IdleMilliseconds, long DeliveryCount);

/// <summary>One <c>SCAN</c> page.</summary>
public readonly record struct ScanPage(long Cursor, IReadOnlyList<string> Keys);

/// <summary>One <c>HSCAN</c> page.</summary>
public readonly record struct HashScanPage(long Cursor, IReadOnlyDictionary<string, string> Fields);

/// <summary>One stream entry. Fields retain their order and may contain repeated names.</summary>
public readonly record struct StreamEntry(string Id, IReadOnlyList<KeyValuePair<string, string>> Fields);

/// <summary>Entries read from one stream key.</summary>
public readonly record struct StreamReadResult(string Key, IReadOnlyList<StreamEntry> Entries);

/// <summary>A Pub/Sub delivery: <c>message</c>, <c>pmessage</c>, or <c>smessage</c>.</summary>
public readonly record struct PubSubMessage(string Kind, string? Pattern, string Channel, string Payload)
{
    /// <summary>
    /// Reads a <c>message</c>, <c>pmessage</c>, or <c>smessage</c> array.
    /// Subscribe confirmations and other arrays return false.
    /// </summary>
    public static bool TryParse(RespValue value, out PubSubMessage message)
    {
        message = default;

        if (value.Items is not { } items || items.Count < 3)
        {
            return false;
        }

        string kind = items[0].AsString() ?? "";

        if (kind is "message" or "smessage" && items.Count >= 3)
        {
            message = new PubSubMessage(kind, null, items[1].AsString() ?? "", items[2].AsString() ?? "");

            return true;
        }

        if (kind is "pmessage" && items.Count >= 4)
        {
            message = new PubSubMessage(
                kind,
                items[1].AsString(),
                items[2].AsString() ?? "",
                items[3].AsString() ?? "");

            return true;
        }

        return false;
    }
}

/// <summary>One note from <c>CHANGES</c>: a sequence and the text, such as <c>SET key</c>.</summary>
public readonly record struct ChangeNote(long Sequence, string Text);

/// <summary>The state a <c>FILLGET</c> reply reports.</summary>
public enum FillState
{
    /// <summary>The value is stored and fresh.</summary>
    Hit,

    /// <summary>Nothing is stored and this caller must build the value, then call <c>FILLSET</c> with the token.</summary>
    Fill,

    /// <summary>Nothing is stored and another caller is building it.</summary>
    Busy,

    /// <summary>The value is past its soft TTL. <see cref="FillResult.Token"/> is set when this caller must refresh it.</summary>
    Stale,
}

/// <summary>The answer to <c>FILLGET</c>.</summary>
/// <param name="State">What the server decided for this caller.</param>
/// <param name="Value">The stored bytes for <see cref="FillState.Hit"/> and <see cref="FillState.Stale"/>; otherwise null.</param>
/// <param name="Token">The fill token for <see cref="FillState.Fill"/>, or for <see cref="FillState.Stale"/> when this caller refreshes.</param>
public readonly record struct FillResult(FillState State, byte[]? Value, long? Token)
{
    /// <summary>The stored value decoded as UTF-8; null when there is no value.</summary>
    public string? Text => Value is null ? null : System.Text.Encoding.UTF8.GetString(Value);

    /// <summary>Reads a <c>FILLGET</c> reply. Throws <see cref="RuvioException"/> for any other shape.</summary>
    public static FillResult FromReply(RespValue reply)
    {
        ArgumentNullException.ThrowIfNull(reply);

        if (reply.Items is not { Count: >= 2 } items || items[0].AsString() is not { } state)
        {
            throw new RuvioException("FILLGET returned an unexpected reply");
        }

        long? ParseToken(RespValue item) => item.Kind == RespKind.Null
            ? null
            : long.Parse(item.AsString() ?? "", System.Globalization.CultureInfo.InvariantCulture);

        return (state, items.Count) switch
        {
            ("HIT", 2) => new FillResult(FillState.Hit, items[1].Bytes ?? [], null),
            ("FILL", 2) => new FillResult(FillState.Fill, null, ParseToken(items[1])),
            ("BUSY", 2) => new FillResult(FillState.Busy, null, null),
            ("STALE", 3) => new FillResult(FillState.Stale, items[1].Bytes ?? [], ParseToken(items[2])),
            _ => throw new RuvioException("FILLGET returned an unexpected reply"),
        };
    }
}

/// <summary>The answer to <c>FILLGET</c> with the stored value decoded from JSON.</summary>
/// <param name="State">What the server decided for this caller.</param>
/// <param name="Value">The decoded value for <see cref="FillState.Hit"/> and <see cref="FillState.Stale"/>; otherwise default.</param>
/// <param name="Token">The fill token for <see cref="FillState.Fill"/>, or for <see cref="FillState.Stale"/> when this caller refreshes.</param>
public readonly record struct FillResult<T>(FillState State, T? Value, long? Token);
