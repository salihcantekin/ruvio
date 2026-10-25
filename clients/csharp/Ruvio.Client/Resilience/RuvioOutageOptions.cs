using System.Net.Sockets;

namespace Ruvio.Client;

/// <summary>
/// What an integration does when Ruvio cannot be reached, and whether it stops calling Ruvio for a while
/// after repeated failures. Both are off in the sense that nothing changes until they are set: the mode defaults to
/// the integration's own behavior and there is no circuit breaker.
/// </summary>
/// <remarks>
/// Only outage errors count: an unreachable server, a dropped connection, a timeout, an open circuit and the replies
/// <c>LOADING</c>, <c>MASTERDOWN</c> and <c>CLUSTERDOWN</c>. A server error reply such as <c>WRONGTYPE</c> is a
/// bug in the caller and always reaches it, in every mode.
/// </remarks>
public sealed class RuvioOutageOptions
{
    private readonly object _gate = new();
    private RuvioCircuitBreakerOptions? _circuit;
    private RuvioCircuitBreaker? _breaker;

    /// <summary>Creates options with <paramref name="mode"/> as the starting mode.</summary>
    public RuvioOutageOptions(RuvioOutageMode mode = RuvioOutageMode.Throw) => Mode = mode;

    /// <summary>What happens when Ruvio cannot be reached. See <see cref="RuvioOutageMode"/>.</summary>
    public RuvioOutageMode Mode { get; set; }

    /// <summary>
    /// Stops calling Ruvio after repeated outage failures. Null (the default) means every call tries Ruvio.
    /// Read when the first call is made; changing it afterwards has no effect.
    /// </summary>
    public RuvioCircuitBreakerOptions? CircuitBreaker
    {
        get => _circuit;
        set
        {
            lock (_gate)
            {
                _circuit = value;
                _breaker = null;
            }
        }
    }

    /// <summary>The breaker, or null when none is configured. Created on first use.</summary>
    public RuvioCircuitBreaker? Breaker
    {
        get
        {
            if (_circuit is null)
                return null;

            lock (_gate)
                return _breaker ??= _circuit is null ? null : new RuvioCircuitBreaker(_circuit);
        }
    }

    /// <summary>Checks the settings; throws when they are out of range.</summary>
    public void Validate()
    {
        if (!Enum.IsDefined(Mode))
            throw new ArgumentOutOfRangeException(nameof(Mode));

        _circuit?.Validate();
    }

    /// <summary>True when <see cref="Validate"/> would not throw; for options validation callbacks.</summary>
    public bool IsValid()
    {
        try
        {
            Validate();

            return true;
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    /// <summary>
    /// Asks to call Ruvio. False when an open circuit refuses the call, which then counts as an outage
    /// (<see cref="RuvioCircuitOpenException"/> says how long to wait). Costs one field read when no breaker is configured.
    /// </summary>
    public bool TryEnter() => _circuit is null || Breaker!.TryAcquire();

    /// <summary>The wait to put in a 503 <c>Retry-After</c> header: the open circuit's remaining break, otherwise zero.</summary>
    public TimeSpan RetryAfter => _circuit is null ? TimeSpan.Zero : Breaker!.RetryAfter;

    /// <summary>The call that <see cref="TryEnter"/> let through got its answer.</summary>
    public void Succeeded()
    {
        if (_circuit is not null)
            Breaker!.RecordSuccess();
    }

    /// <summary>The call that <see cref="TryEnter"/> let through ended without telling anything about Ruvio.</summary>
    public void Abandoned()
    {
        if (_circuit is not null)
            Breaker!.RecordAbandoned();
    }

    /// <summary>
    /// The call that <see cref="TryEnter"/> let through threw <paramref name="error"/>. Feeds the breaker and tells whether
    /// the error is an outage; meant for an exception filter, <c>catch (Exception e) when (outage.Failed(e, token))</c>.
    /// </summary>
    public bool Failed(Exception error, CancellationToken cancellationToken)
    {
        bool outage = IsOutage(error, cancellationToken);

        if (_circuit is not null)
        {
            if (outage)
                Breaker!.RecordFailure();
            else if (error is RuvioException { IsServerReply: true })
                Breaker!.RecordSuccess();
            else
                Breaker!.RecordAbandoned();
        }

        return outage;
    }

    /// <summary>
    /// Whether <paramref name="error"/> means Ruvio could not be reached or is not serving yet. A cancelled
    /// <paramref name="cancellationToken"/> is the caller's own doing and never an outage.
    /// </summary>
    public static bool IsOutage(Exception error, CancellationToken cancellationToken) => error switch
    {
        SocketException or IOException or TimeoutException or RuvioCircuitOpenException => true,
        RuvioException { IsServerReply: false } => true,
        RuvioException { IsServerReply: true } reply => reply.Message.StartsWith("LOADING", StringComparison.Ordinal)
            || reply.Message.StartsWith("MASTERDOWN", StringComparison.Ordinal)
            || reply.Message.StartsWith("CLUSTERDOWN", StringComparison.Ordinal),
        OperationCanceledException => !cancellationToken.IsCancellationRequested,
        _ => false,
    };
}
