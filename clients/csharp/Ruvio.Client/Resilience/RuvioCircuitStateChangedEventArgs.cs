namespace Ruvio.Client;

/// <summary>A change of a <see cref="RuvioCircuitBreaker"/>'s state, raised by <see cref="RuvioCircuitBreaker.StateChanged"/>.</summary>
/// <param name="previous">The state before the change.</param>
/// <param name="current">The state after the change.</param>
/// <param name="retryAfter">How long an opened circuit stays open; zero for the other states.</param>
public sealed class RuvioCircuitStateChangedEventArgs(RuvioCircuitState previous, RuvioCircuitState current, TimeSpan retryAfter)
    : EventArgs
{
    /// <summary>The state before the change.</summary>
    public RuvioCircuitState Previous { get; } = previous;

    /// <summary>The state after the change.</summary>
    public RuvioCircuitState Current { get; } = current;

    /// <summary>How long an opened circuit stays open before a trial call; zero unless <see cref="Current"/> is open.</summary>
    public TimeSpan RetryAfter { get; } = retryAfter;
}
