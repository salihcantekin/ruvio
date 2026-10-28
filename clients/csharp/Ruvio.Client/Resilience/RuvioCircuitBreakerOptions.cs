namespace Ruvio.Client;

/// <summary>Settings of a <see cref="RuvioCircuitBreaker"/>.</summary>
public sealed class RuvioCircuitBreakerOptions
{
    /// <summary>How many outage failures in a row open the circuit. Defaults to 5.</summary>
    public int FailureThreshold { get; set; } = 5;

    /// <summary>
    /// How long the circuit stays open and Ruvio is not called. After that, one trial call decides: if it
    /// works the circuit closes, otherwise it opens for another <see cref="BreakDuration"/>. Defaults to 30 seconds.
    /// </summary>
    public TimeSpan BreakDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>The clock behind <see cref="BreakDuration"/>. The system clock by default; tests can pass a fake one.</summary>
    public TimeProvider TimeProvider { get; set; } = TimeProvider.System;

    internal void Validate()
    {
        ArgumentNullException.ThrowIfNull(TimeProvider);

        if (FailureThreshold < 1)
            throw new ArgumentOutOfRangeException(nameof(FailureThreshold), "FailureThreshold must be at least 1.");

        if (BreakDuration < TimeSpan.FromMilliseconds(1))
            throw new ArgumentOutOfRangeException(nameof(BreakDuration), "BreakDuration must be at least one millisecond.");
    }
}
