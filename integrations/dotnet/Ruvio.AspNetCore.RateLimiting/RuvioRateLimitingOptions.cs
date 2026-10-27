using Microsoft.AspNetCore.Http;
using Ruvio.Client;

namespace Ruvio.AspNetCore.RateLimiting;

/// <summary>Settings for Ruvio-backed rate limiting.</summary>
public sealed class RuvioRateLimitingOptions
{
    /// <summary>Application-specific namespace. Distinct deployments should use distinct prefixes.</summary>
    public string KeyPrefix { get; set; } = "ruvio:rate:v1:";

    /// <summary>
    /// Required trusted tenant/user identifier, or an explicit constant for a global budget.
    /// Runs after authentication/authorization. Do not trust arbitrary forwarded headers.
    /// </summary>
    public Func<HttpContext, string>? PartitionKey { get; set; }

    /// <summary>
    /// What a blank partition (null, empty or whitespace) does. The default, <see cref="RuvioEmptyPartitionMode.Throw"/>,
    /// treats it as a configuration error; <see cref="RuvioEmptyPartitionMode.SharedBucket"/> puts every such request
    /// into one shared budget per policy, so a missing key never means unlimited.
    /// </summary>
    public RuvioEmptyPartitionMode EmptyPartition { get; set; }

    /// <summary>
    /// The name of a policy added with <see cref="AddPolicy(string, long, TimeSpan, RuvioRateLimitAlgorithm)"/> that
    /// applies to every routed endpoint without rate-limit metadata. Opt an endpoint or group out with
    /// <see cref="RuvioRateLimitEndpointExtensions.DisableRuvioRateLimit{TBuilder}"/> or <see cref="DisableRuvioRateLimitAttribute"/>.
    /// Null (the default) limits only the endpoints that ask for it.
    /// </summary>
    public string? DefaultPolicy { get; set; }

    /// <summary>
    /// Runs after a request is rejected, once the 429 status and <c>Retry-After</c> header are set; it may change the
    /// status or write a body. Not called for outage answers.
    /// </summary>
    public Func<HttpContext, RuvioRateLimitRejection, ValueTask>? OnRejected { get; set; }

    /// <summary>
    /// What happens when Ruvio cannot be reached. The default, <see cref="RuvioOutageMode.Throw"/>, lets the storage
    /// exception reach the application; <see cref="RuvioOutageMode.FailClosed"/> answers 503 and
    /// <see cref="RuvioOutageMode.FailOpen"/> lets the request through unlimited. Add a circuit breaker with
    /// <see cref="RuvioOutageOptions.CircuitBreaker"/> to stop waiting on a Ruvio that is down.
    /// </summary>
    public RuvioOutageOptions Outage { get; } = new();

    private readonly Dictionary<string, RuvioLimit[]> policies = new(StringComparer.Ordinal);

    /// <summary>
    /// Defines a named policy that endpoints refer to by name only: <c>[RuvioRateLimit("api")]</c> or
    /// <c>.WithRuvioRateLimit("api")</c>. The window is rounded up to whole seconds. Endpoints that spell out their own
    /// permits and window keep working and do not need a registration.
    /// </summary>
    /// <param name="name">The policy name, 1-128 characters and unique among the named policies.</param>
    /// <param name="permits">The number of requests allowed per window, or the bucket size for a token bucket.</param>
    /// <param name="window">The window length, or the time to refill an empty token bucket.</param>
    /// <param name="algorithm">How requests are counted. A policy keeps the algorithm it was first used with.</param>
    /// <returns>The same options, so calls can be chained.</returns>
    public RuvioRateLimitingOptions AddPolicy(
        string name, long permits, TimeSpan window,
        RuvioRateLimitAlgorithm algorithm = RuvioRateLimitAlgorithm.FixedWindow)
        => AddPolicy(name, builder => builder.Limit(permits, window, null, algorithm));

    /// <summary>
    /// Like <see cref="AddPolicy(string, long, TimeSpan, RuvioRateLimitAlgorithm)"/>, with a partition key of its own
    /// that replaces <see cref="PartitionKey"/> for this policy, for example the client IP on a login endpoint.
    /// </summary>
    /// <param name="name">The policy name, 1-128 characters and unique among the named policies.</param>
    /// <param name="permits">The number of requests allowed per window, or the bucket size for a token bucket.</param>
    /// <param name="window">The window length, or the time to refill an empty token bucket.</param>
    /// <param name="partitionKey">Picks the budget a request counts against. Same rules as <see cref="PartitionKey"/>.</param>
    /// <param name="algorithm">How requests are counted.</param>
    /// <returns>The same options, so calls can be chained.</returns>
    public RuvioRateLimitingOptions AddPolicy(
        string name, long permits, TimeSpan window, Func<HttpContext, string> partitionKey,
        RuvioRateLimitAlgorithm algorithm = RuvioRateLimitAlgorithm.FixedWindow)
    {
        ArgumentNullException.ThrowIfNull(partitionKey);

        return AddPolicy(name, builder => builder.Limit(permits, window, partitionKey, algorithm));
    }

    /// <summary>
    /// Defines a named policy made of several limits. A request must pass every one; they run in the order added, one
    /// storage command each. They are not one atomic unit: a request rejected by a later limit has already used a permit
    /// of the earlier ones, so list the most restrictive limit first.
    /// </summary>
    /// <param name="name">The policy name, 1-128 characters and unique among the named policies.</param>
    /// <param name="configure">Adds the limits with <see cref="RuvioPolicyBuilder.Limit"/> and <see cref="RuvioPolicyBuilder.Concurrency"/>.</param>
    /// <returns>The same options, so calls can be chained.</returns>
    public RuvioRateLimitingOptions AddPolicy(string name, Action<RuvioPolicyBuilder> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(name.Length, 128, nameof(name));
        ArgumentNullException.ThrowIfNull(configure);

        if (name.Contains('\0'))
            throw new ArgumentException("A policy name must not contain a NUL character.", nameof(name));

        var builder = new RuvioPolicyBuilder(name);

        configure(builder);

        var limits = builder.Build();

        if (!policies.TryAdd(name, limits))
            throw new ArgumentException($"A rate-limit policy named '{name}' is already defined.", nameof(name));

        return this;
    }

    internal bool TryGetPolicy(string name, out RuvioLimit[] limits) => policies.TryGetValue(name, out limits!);

    // Every limit has its own key, or the shared PartitionKey exists.
    internal bool PartitionsResolve()
        => PartitionKey is not null || (policies.Count > 0 && policies.Values.All(p => p.All(l => l.Partition is not null)));
}

/// <summary>What a blank partition value does.</summary>
public enum RuvioEmptyPartitionMode
{
    /// <summary>The request fails with <see cref="InvalidOperationException"/>. The default.</summary>
    Throw = 0,

    /// <summary>All requests with a blank partition share one budget per policy.</summary>
    SharedBucket = 1,
}

/// <summary>Why a request was rejected.</summary>
public enum RuvioRejectionReason
{
    /// <summary>The permits of a window or token bucket are used up.</summary>
    RateLimit = 0,

    /// <summary>The most concurrent requests are already running.</summary>
    Concurrency = 1,
}

/// <summary>Describes a rejected request to <see cref="RuvioRateLimitingOptions.OnRejected"/>.</summary>
/// <param name="Policy">The policy name.</param>
/// <param name="Reason">Which kind of limit rejected the request.</param>
/// <param name="LimitIndex">The position of the rejecting limit in the policy, starting at 0.</param>
/// <param name="RetryAfter">The delay already written to <c>Retry-After</c>.</param>
public readonly record struct RuvioRateLimitRejection(
    string Policy, RuvioRejectionReason Reason, int LimitIndex, TimeSpan RetryAfter);

/// <summary>Collects the limits of one named policy.</summary>
public sealed class RuvioPolicyBuilder
{
    private readonly string name;
    private readonly List<RuvioLimit> limits = [];

    internal RuvioPolicyBuilder(string name) => this.name = name;

    /// <summary>Adds a rate limit: <paramref name="permits"/> requests per <paramref name="window"/> per partition.</summary>
    /// <param name="permits">The requests allowed per window, or the bucket size for a token bucket.</param>
    /// <param name="window">The window length, rounded up to whole seconds.</param>
    /// <param name="partitionKey">Replaces <see cref="RuvioRateLimitingOptions.PartitionKey"/> for this limit; null uses it.</param>
    /// <param name="algorithm">How requests are counted.</param>
    /// <returns>The same builder.</returns>
    public RuvioPolicyBuilder Limit(
        long permits, TimeSpan window, Func<HttpContext, string>? partitionKey = null,
        RuvioRateLimitAlgorithm algorithm = RuvioRateLimitAlgorithm.FixedWindow)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(permits);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(window, TimeSpan.Zero);

        if (!Enum.IsDefined(algorithm))
            throw new ArgumentOutOfRangeException(nameof(algorithm));

        int seconds = checked((int)Math.Ceiling(window.TotalSeconds));

        limits.Add(new RuvioLimit(KeyName(false), permits, seconds, algorithm, partitionKey, false));

        return this;
    }

    /// <summary>
    /// Adds a concurrency limit: at most <paramref name="maxConcurrent"/> requests run at once per partition. A slot is
    /// taken before the endpoint runs and released after it; it also expires after <paramref name="maxRequestDuration"/>,
    /// so a crashed instance cannot hold it forever (a request that runs longer can be over-admitted).
    /// </summary>
    /// <param name="maxConcurrent">The most requests in flight.</param>
    /// <param name="maxRequestDuration">The slot expiry; 30 seconds when null.</param>
    /// <param name="partitionKey">Replaces <see cref="RuvioRateLimitingOptions.PartitionKey"/> for this limit; null uses it.</param>
    /// <returns>The same builder.</returns>
    public RuvioPolicyBuilder Concurrency(
        long maxConcurrent, TimeSpan? maxRequestDuration = null, Func<HttpContext, string>? partitionKey = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxConcurrent);

        var ttl = maxRequestDuration ?? TimeSpan.FromSeconds(30);

        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(ttl, TimeSpan.Zero, nameof(maxRequestDuration));

        int seconds = checked((int)Math.Ceiling(ttl.TotalSeconds));

        limits.Add(new RuvioLimit(KeyName(true), maxConcurrent, seconds, RuvioRateLimitAlgorithm.FixedWindow, partitionKey, true));

        return this;
    }

    // The first rate limit keeps the plain name, so a one-limit policy shares its bucket with an inline attribute of that name.
    private string KeyName(bool concurrency)
        => limits.Count == 0 && !concurrency ? name : name + "\0" + (concurrency ? "c" : "r") + limits.Count;

    internal RuvioLimit[] Build()
    {
        if (limits.Count == 0)
            throw new InvalidOperationException($"The policy '{name}' needs at least one limit.");

        if (limits.Count(l => l.IsConcurrency) > 1)
            throw new InvalidOperationException($"The policy '{name}' can have only one concurrency limit.");

        return [.. limits];
    }
}

/// <summary>One limit of a policy. For a concurrency limit, Permits is the maximum and WindowSeconds the slot expiry.</summary>
internal sealed record RuvioLimit(
    string KeyName, long Permits, int WindowSeconds, RuvioRateLimitAlgorithm Algorithm,
    Func<HttpContext, string>? Partition, bool IsConcurrency);
