namespace Ruvio.Client;

/// <summary>
/// Thrown instead of calling Ruvio while a <see cref="RuvioCircuitBreaker"/> is open.
/// </summary>
/// <param name="retryAfter">How long until the circuit lets a trial call through.</param>
public sealed class RuvioCircuitOpenException(TimeSpan retryAfter)
    : Exception("The Ruvio circuit breaker is open; Ruvio was not called.")
{
    /// <summary>How long until the circuit lets a trial call through.</summary>
    public TimeSpan RetryAfter { get; } = retryAfter;
}
