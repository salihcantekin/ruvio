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
}
