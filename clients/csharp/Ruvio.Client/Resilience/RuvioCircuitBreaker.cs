using System.Diagnostics.Metrics;

namespace Ruvio.Client;

/// <summary>Where a <see cref="RuvioCircuitBreaker"/> stands.</summary>
public enum RuvioCircuitState
{
    /// <summary>Calls go to Ruvio.</summary>
    Closed = 0,

    /// <summary>Ruvio is not called.</summary>
    Open = 1,

    /// <summary>One trial call is out; the others are refused until it answers.</summary>
    HalfOpen = 2,
}

/// <summary>
/// Stops calling Ruvio after repeated outage failures, so a request does not wait out a connect or command
/// timeout while Ruvio is down. While the circuit is closed the cost is one volatile read per call.
/// </summary>
public sealed class RuvioCircuitBreaker
{
    private static readonly Meter Meter = new("Ruvio.Client");
    private static readonly Counter<long> Transitions = Meter.CreateCounter<long>("ruvio_client_circuit_transitions");
    private readonly object _gate = new();
    private readonly int _threshold;
    private readonly TimeSpan _duration;
    private readonly TimeProvider _time;
    private volatile int _state;
    private int _failures;
    private long _openedAt;
    private bool _probing;

    /// <summary>Creates a closed circuit.</summary>
    public RuvioCircuitBreaker(RuvioCircuitBreakerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        _threshold = options.FailureThreshold;
        _duration = options.BreakDuration;
        _time = options.TimeProvider;
    }

    /// <summary>The current state. An open circuit reports <see cref="RuvioCircuitState.Open"/> until a call asks to go through.</summary>
    public RuvioCircuitState State => (RuvioCircuitState)_state;

    /// <summary>
    /// Raised after each change of <see cref="State"/>: closed to open, open to half-open (the trial call), and
    /// half-open to closed or back to open. Runs outside the breaker's lock, on the thread of the call that caused the
    /// change; keep it short. An exception from a handler is ignored, so it never fails that call.
    /// </summary>
    public event EventHandler<RuvioCircuitStateChangedEventArgs>? StateChanged;

    /// <summary>How long until a trial call is let through; zero when the circuit is not open.</summary>
    public TimeSpan RetryAfter
    {
        get
        {
            if (_state != (int)RuvioCircuitState.Open)
                return TimeSpan.Zero;

            lock (_gate)
            {
                TimeSpan left = _duration - _time.GetElapsedTime(_openedAt);

                return left > TimeSpan.Zero ? left : TimeSpan.Zero;
            }
        }
    }

    /// <summary>
    /// Asks to make a call. True when the call may go to Ruvio (every call while closed, one trial call once the break
    /// is over); the caller then reports its outcome with <see cref="RecordSuccess"/>, <see cref="RecordFailure"/> or <see cref="RecordAbandoned"/>.
    /// </summary>
    public bool TryAcquire()
    {
        if (_state == (int)RuvioCircuitState.Closed)
            return true;

        lock (_gate)
        {
            switch ((RuvioCircuitState)_state)
            {
                case RuvioCircuitState.Closed:
                    return true;

                case RuvioCircuitState.Open:
                    if (_time.GetElapsedTime(_openedAt) < _duration)
                        return false;

                    _state = (int)RuvioCircuitState.HalfOpen;
                    _probing = true;

                    break;

                default:
                    if (_probing)
                        return false;

                    _probing = true;

                    return true;
            }
        }

        Changed(RuvioCircuitState.Open, RuvioCircuitState.HalfOpen);

        return true;
    }

    /// <summary>A call got an answer from Ruvio (including an error reply): the outage is over.</summary>
    public void RecordSuccess()
    {
        if (_state == (int)RuvioCircuitState.Closed && Volatile.Read(ref _failures) == 0)
            return;

        RuvioCircuitState previous;

        lock (_gate)
        {
            // A late answer from a call that began before the circuit opened proves nothing.
            if (_state == (int)RuvioCircuitState.Open)
                return;

            previous = (RuvioCircuitState)_state;
            _failures = 0;
            _probing = false;
            _state = (int)RuvioCircuitState.Closed;
        }

        if (previous != RuvioCircuitState.Closed)
            Changed(previous, RuvioCircuitState.Closed);
    }

    /// <summary>A call failed because Ruvio could not be reached.</summary>
    public void RecordFailure()
    {
        RuvioCircuitState previous;

        lock (_gate)
        {
            previous = (RuvioCircuitState)_state;

            switch (previous)
            {
                case RuvioCircuitState.Closed:
                    if (++_failures < _threshold)
                        return;

                    Open();

                    break;

                case RuvioCircuitState.HalfOpen:
                    Open();

                    break;

                default:
                    return;
            }
        }

        Changed(previous, RuvioCircuitState.Open);
    }

    /// <summary>A call ended without telling anything about Ruvio (the caller cancelled it): a trial call's slot is freed.</summary>
    public void RecordAbandoned()
    {
        lock (_gate)
        {
            if (_state == (int)RuvioCircuitState.HalfOpen)
                _probing = false;
        }
    }

    // Called after the lock is released: counts the change and tells the listeners.
    private void Changed(RuvioCircuitState previous, RuvioCircuitState current)
    {
        Transitions.Add(1, new KeyValuePair<string, object?>("state", current switch
        {
            RuvioCircuitState.Open => "open",
            RuvioCircuitState.HalfOpen => "half_open",
            _ => "closed",
        }));

        if (StateChanged is not { } handlers)
            return;

        var change = new RuvioCircuitStateChangedEventArgs(previous, current,
            current == RuvioCircuitState.Open ? _duration : TimeSpan.Zero);

        try
        {
            handlers(this, change);
        }
        catch (Exception)
        {
            // A listener (a log sink, a metric) must not fail the Ruvio call that moved the circuit.
        }
    }

    private void Open()
    {
        _failures = 0;
        _probing = false;
        _openedAt = _time.GetTimestamp();
        _state = (int)RuvioCircuitState.Open;
    }
}
