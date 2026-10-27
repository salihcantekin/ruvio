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
    /// What happens when Ruvio cannot be reached. The default, <see cref="RuvioOutageMode.Throw"/>, lets the storage
    /// exception reach the application; <see cref="RuvioOutageMode.FailClosed"/> answers 503 and
    /// <see cref="RuvioOutageMode.FailOpen"/> lets the request through unlimited. Add a circuit breaker with
    /// <see cref="RuvioOutageOptions.CircuitBreaker"/> to stop waiting on a Ruvio that is down.
    /// </summary>
    public RuvioOutageOptions Outage { get; } = new();

    private readonly Dictionary<string, NamedPolicy> policies = new(StringComparer.Ordinal);

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
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(name.Length, 128, nameof(name));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(permits);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(window, TimeSpan.Zero);

        if (!Enum.IsDefined(algorithm))
            throw new ArgumentOutOfRangeException(nameof(algorithm));

        int seconds = checked((int)Math.Ceiling(window.TotalSeconds));

        if (!policies.TryAdd(name, new NamedPolicy(permits, seconds, algorithm)))
            throw new ArgumentException($"A rate-limit policy named '{name}' is already defined.", nameof(name));

        return this;
    }

    internal bool TryGetPolicy(string name, out NamedPolicy policy) => policies.TryGetValue(name, out policy);

    internal readonly record struct NamedPolicy(long Permits, int WindowSeconds, RuvioRateLimitAlgorithm Algorithm);
}
