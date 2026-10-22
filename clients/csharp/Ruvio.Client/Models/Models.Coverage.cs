namespace Ruvio.Client;

/// <summary>The <c>BITOP</c> operation.</summary>
public enum BitOperation
{
    /// <summary><c>AND</c>.</summary>
    And,

    /// <summary><c>OR</c>.</summary>
    Or,

    /// <summary><c>XOR</c>.</summary>
    Xor,

    /// <summary><c>NOT</c>: takes exactly one source key.</summary>
    Not,
}

/// <summary>Whether a <c>BITCOUNT</c> or <c>BITPOS</c> range counts bytes or bits.</summary>
public enum BitUnit
{
    /// <summary>The range is in bytes (<c>BYTE</c>, the default).</summary>
    Byte,

    /// <summary>The range is in bits (<c>BIT</c>).</summary>
    Bit,
}

/// <summary>How <c>ZUNION</c> and <c>ZINTER</c> combine the scores of one member.</summary>
public enum SortedSetAggregate
{
    /// <summary>Add the scores (<c>AGGREGATE SUM</c>, the default).</summary>
    Sum,

    /// <summary>Keep the lowest score (<c>AGGREGATE MIN</c>).</summary>
    Min,

    /// <summary>Keep the highest score (<c>AGGREGATE MAX</c>).</summary>
    Max,
}

/// <summary>The value type a <c>JSON.INDEX</c> indexes.</summary>
public enum JsonIndexType
{
    /// <summary>Strings (<c>TYPE string</c>, the default).</summary>
    String,

    /// <summary>Numbers (<c>TYPE number</c>).</summary>
    Number,

    /// <summary>Booleans (<c>TYPE bool</c>).</summary>
    Bool,
}

/// <summary>One <c>SSCAN</c> page.</summary>
public readonly record struct SetScanPage(long Cursor, IReadOnlyList<string> Members);

/// <summary>One <c>ZSCAN</c> page.</summary>
public readonly record struct SortedSetScanPage(long Cursor, IReadOnlyList<SortedSetEntry> Entries);

/// <summary>The <c>BF.INFO</c> reply.</summary>
/// <param name="Capacity">Items the first layer was sized for.</param>
/// <param name="Size">Bytes of the bit arrays plus the filter header.</param>
/// <param name="Filters">Number of layers.</param>
/// <param name="Items">Items inserted so far.</param>
/// <param name="ExpansionRate">Growth factor of each new layer; null for a non-scaling filter.</param>
public readonly record struct BloomFilterInfo(long Capacity, long Size, long Filters, long Items, long? ExpansionRate);

/// <summary><c>BF.INSERT</c> options. The creation options apply only when the key is new.</summary>
public sealed class BloomInsertOptions
{
    /// <summary>
    /// <c>EX</c> or <c>PX</c>: a time to live for a new filter, set only when the call creates the key. A filter that already exists keeps its deadline.
    /// Whole seconds are sent as <c>EX</c>, otherwise <c>PX</c> in milliseconds. Must be positive.
    /// </summary>
    public TimeSpan? Ttl { get; init; }

    /// <summary><c>CAPACITY</c> of a new filter.</summary>
    public long? Capacity { get; init; }

    /// <summary><c>ERROR</c> rate of a new filter.</summary>
    public double? ErrorRate { get; init; }

    /// <summary><c>EXPANSION</c> of a new filter.</summary>
    public long? Expansion { get; init; }

    /// <summary><c>NOCREATE</c>: a missing key is an error instead of a new filter.</summary>
    public bool NoCreate { get; init; }

    /// <summary><c>NONSCALING</c>: a new filter never adds a layer; a full one answers an error.</summary>
    public bool NonScaling { get; init; }
}

/// <summary>The answer of <c>LIMIT</c> as a rate-limit decision.</summary>
/// <param name="Allowed">True when this call fit in the window.</param>
/// <param name="Remaining">The allowance left after this call; 0 when it was refused.</param>
public readonly record struct RateLimitResult(bool Allowed, long Remaining);

/// <summary>The answer of the <c>WITHRETRY</c> forms of <c>LIMIT ... SLIDING</c> and <c>LIMIT ... TOKENS</c>.</summary>
/// <param name="Allowed">True when this call was let through.</param>
/// <param name="Remaining">The allowance left after this call; 0 when it was refused.</param>
/// <param name="RetryAfterMs">Milliseconds until the same call would pass; 0 when it was let through. Fit for a <c>Retry-After</c> header.</param>
public readonly record struct LimitOutcome(bool Allowed, long Remaining, long RetryAfterMs);

/// <summary>What <c>IDEM key BEGIN</c> decided.</summary>
public enum IdempotencyState
{
    /// <summary>This caller owns the operation and must call <c>CompleteIdempotencyAsync</c>.</summary>
    Acquired,

    /// <summary>Another owner is still running it.</summary>
    Pending,

    /// <summary>The key is held with a different fingerprint.</summary>
    Conflict,

    /// <summary>The operation already finished; <see cref="IdempotencyBegin.Result"/> holds its stored result.</summary>
    Completed,
}

/// <summary>The answer of <c>IDEM key BEGIN</c>.</summary>
/// <param name="State">What the server decided.</param>
/// <param name="Result">The stored result bytes for <see cref="IdempotencyState.Completed"/>; otherwise null.</param>
public readonly record struct IdempotencyBegin(IdempotencyState State, byte[]? Result)
{
    /// <summary>The stored result decoded as UTF-8, for a result that was written as text; null otherwise.</summary>
    public string? ResultText => Result is null ? null : System.Text.Encoding.UTF8.GetString(Result);
}

/// <summary>Options for the <c>GetOrCreateAsync</c> cache-aside helper.</summary>
public sealed class GetOrCreateOptions
{
    /// <summary>Shared default options.</summary>
    public static GetOrCreateOptions Default { get; } = new();

    /// <summary>How long the fill marker lives while one caller builds the value (<c>FILLGET</c> fill time). Default 10 seconds.</summary>
    public TimeSpan FillTimeout { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>How long other callers poll for the builder before building the value themselves. Default 5 seconds.</summary>
    public TimeSpan WaitTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>Stale-while-revalidate window (<c>FILLSET ... SWR</c>). Null stores no stale window.</summary>
    public TimeSpan? StaleWindow { get; init; }
}
