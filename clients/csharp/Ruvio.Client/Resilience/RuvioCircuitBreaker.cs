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

                    return true;

                default:
                    if (_probing)
                        return false;

                    _probing = true;

                    return true;
            }
        }
    }

    /// <summary>A call got an answer from Ruvio (including an error reply): the outage is over.</summary>
    public void RecordSuccess()
    {
        if (_state == (int)RuvioCircuitState.Closed && Volatile.Read(ref _failures) == 0)
            return;

        lock (_gate)
        {
            // A late answer from a call that began before the circuit opened proves nothing.
            if (_state == (int)RuvioCircuitState.Open)
                return;

            _failures = 0;
            _probing = false;
            _state = (int)RuvioCircuitState.Closed;
        }
    }

    /// <summary>A call failed because Ruvio could not be reached.</summary>
    public void RecordFailure()
    {
        lock (_gate)
        {
            switch ((RuvioCircuitState)_state)
            {
                case RuvioCircuitState.Closed:
                    if (++_failures >= _threshold)
                        Open();

                    break;

                case RuvioCircuitState.HalfOpen:
                    Open();

                    break;
            }
        }
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

    private void Open()
    {
        _failures = 0;
        _probing = false;
        _openedAt = _time.GetTimestamp();
        _state = (int)RuvioCircuitState.Open;
    }
}
