namespace Ruvio.AspNetCore.RateLimiting;

/// <summary>How a policy counts requests.</summary>
public enum RuvioRateLimitAlgorithm
{
    /// <summary>A counter that resets after each window (<c>LIMIT</c>). The default.</summary>
    FixedWindow = 0,

    /// <summary>Weighs the previous window by how much of it still overlaps, so a burst at a window edge is not doubled (<c>LIMIT ... SLIDING</c>).</summary>
    SlidingWindow = 1,

    /// <summary>A bucket of <c>Permits</c> tokens that refills over the window (<c>LIMIT ... TOKENS</c>).</summary>
    TokenBucket = 2,
}

/// <summary>A shared budget. Endpoints with the same policy share it. Fixed window unless <see cref="Algorithm"/> says otherwise.</summary>
[AttributeUsage(AttributeTargets.Method | AttributeTargets.Class)]
public sealed class RuvioRateLimitAttribute : Attribute
{
    /// <summary>
    /// Refers to a policy defined with <see cref="RuvioRateLimitingOptions.AddPolicy(string, long, TimeSpan, RuvioRateLimitAlgorithm)"/>; its permits, window and algorithm
    /// come from there. An unknown name stops host start (and fails a request on an endpoint added after start)
    /// with <see cref="InvalidOperationException"/>.
    /// </summary>
    /// <param name="policy">The name of a registered policy.</param>
    public RuvioRateLimitAttribute(string policy)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(policy);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(policy.Length, 128, nameof(policy));

        Policy = policy;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="RuvioRateLimitAttribute"/> class with the specified policy, permits, and window duration.
    /// </summary>
    /// <param name="policy">The rate-limiting policy name.</param>
    /// <param name="permits">The number of permits allowed within the window.</param>
    /// <param name="windowSeconds">The duration of the fixed window in seconds.</param>
    public RuvioRateLimitAttribute(string policy, long permits, int windowSeconds)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(policy);

        if (policy.Length > 128)
            throw new ArgumentOutOfRangeException(nameof(policy));

        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(permits);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(windowSeconds);

        Policy = policy;
        Permits = permits;
        WindowSeconds = windowSeconds;
    }

    /// <summary>
    /// Gets the rate-limiting policy name.
    /// </summary>
    public string Policy { get; }

    /// <summary>
    /// Gets the number of permits allowed within the window; 0 when only a policy name was given.
    /// </summary>
    public long Permits { get; }

    /// <summary>
    /// Gets the duration of the fixed window in seconds.
    /// </summary>
    public int WindowSeconds { get; }

    /// <summary>True when only the policy name was given, so the settings come from the registered policy.</summary>
    internal bool IsNamed => Permits == 0;

    /// <summary>
    /// Gets or sets how requests are counted. For <see cref="RuvioRateLimitAlgorithm.TokenBucket"/>, <see cref="Permits"/>
    /// is the bucket size and <see cref="WindowSeconds"/> the time to refill an empty bucket. A policy keeps the algorithm
    /// it was first used with; changing it needs a new policy name.
    /// </summary>
    public RuvioRateLimitAlgorithm Algorithm { get; init; }

    private RuvioLimit[]? inline;

    // The single limit of an inline attribute, built on first use and reused afterwards.
    internal RuvioLimit[] InlineLimits
        => inline ??= [new RuvioLimit(Policy, Permits, WindowSeconds, Algorithm, null, false)];
}

/// <summary>
/// Exempts an endpoint or controller from rate limiting, including <see cref="RuvioRateLimitingOptions.DefaultPolicy"/>.
/// If it sits next to a <see cref="RuvioRateLimitAttribute"/>, the one closer to the endpoint wins.
/// </summary>
[AttributeUsage(AttributeTargets.Method | AttributeTargets.Class)]
public sealed class DisableRuvioRateLimitAttribute : Attribute;
