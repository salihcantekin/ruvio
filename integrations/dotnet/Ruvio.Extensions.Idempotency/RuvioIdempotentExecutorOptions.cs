namespace Ruvio.Extensions.Idempotency;

/// <summary>Settings for a <see cref="RuvioIdempotentExecutor"/>. Read once, when it is constructed.</summary>
public sealed class RuvioIdempotentExecutorOptions
{
    /// <summary>The longest retention <c>IDEM</c> accepts: 7 days.</summary>
    public static readonly TimeSpan MaximumRetention = TimeSpan.FromDays(7);

    /// <summary>Prefix for the record keys. Reserve it for this executor: ordinary writes to these keys break the guarantees.</summary>
    public string KeyPrefix { get; set; } = "idem:";

    /// <summary>
    /// How long a record lives when a call passes no retention: how long a repeat is recognised, and also how long a
    /// call that failed blocks the key (see the README). Whole milliseconds from 1 ms to 7 days; 1 day by default.
    /// </summary>
    public TimeSpan DefaultRetention { get; set; } = TimeSpan.FromDays(1);

    /// <summary>
    /// When the work throws (or is cancelled), give up the pending record with <c>IDEM ... ABORT</c> so the next call
    /// with the same key runs at once instead of getting <see cref="IdempotentOutcome.Pending"/> until the retention
    /// ends. The exception still reaches the caller. Off by default: turn it on only for work that throws before it
    /// takes effect, or that is safe to repeat, because the next call runs it again. Needs a server with
    /// <c>IDEM ... ABORT</c>; a failed abort leaves the record pending.
    /// </summary>
    public bool AbortOnException { get; set; }

    /// <summary>
    /// The key of a connection registered with <c>AddKeyedRuvioClient</c>, used by <c>AddRuvioIdempotentExecutor</c>.
    /// Null (the default) uses the unkeyed <c>IRuvioClient</c>.
    /// </summary>
    public string? ClientKey { get; set; }

    internal void Validate()
    {
        if (string.IsNullOrEmpty(KeyPrefix))
        {
            throw new ArgumentException("KeyPrefix must not be empty.", nameof(KeyPrefix));
        }

        RuvioIdempotentExecutor.RetentionMilliseconds(DefaultRetention, nameof(DefaultRetention));
    }
}
