namespace Ruvio.Client;

/// <summary>What an integration does when Ruvio cannot be reached.</summary>
public enum RuvioOutageMode
{
    /// <summary>
    /// The storage exception reaches the caller (an HTTP request ends in whatever the application's exception
    /// handling makes of it). With a circuit breaker, a request refused by an open circuit throws
    /// <see cref="RuvioCircuitOpenException"/>.
    /// </summary>
    Throw = 0,

    /// <summary>
    /// The HTTP middleware answers 503 Service Unavailable (with <c>Retry-After</c> while a circuit is open) and the
    /// request does not run. Caches, which have no response to send, behave as <see cref="Throw"/>.
    /// </summary>
    FailClosed = 1,

    /// <summary>
    /// The feature steps aside and the work goes on without it: a request runs without its idempotency, rate-limit
    /// or lock check, a cache read is a miss, and a cache write or removal is dropped. The application accepts the
    /// risk that comes with running unchecked while Ruvio is away.
    /// </summary>
    FailOpen = 2,
}
