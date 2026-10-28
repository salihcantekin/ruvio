using System.Diagnostics.Metrics;
using System.Net.Sockets;
using Xunit;

namespace Ruvio.Client.Tests;

public class CircuitBreakerTests
{
    private sealed class ManualTime : TimeProvider
    {
        private long _ticks;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override long GetTimestamp() => Interlocked.Read(ref _ticks);

        public void Advance(TimeSpan by) => Interlocked.Add(ref _ticks, by.Ticks);
    }

    private static RuvioCircuitBreaker Create(ManualTime time, int threshold = 3) => new(new RuvioCircuitBreakerOptions
    {
        FailureThreshold = threshold,
        BreakDuration = TimeSpan.FromSeconds(10),
        TimeProvider = time,
    });

    [Fact]
    public void StateChangedReportsEveryTransitionOnceWithTheBreakDuration()
    {
        var time = new ManualTime();
        var breaker = Create(time, threshold: 2);
        var changes = new List<(RuvioCircuitState From, RuvioCircuitState To, TimeSpan RetryAfter)>();

        breaker.StateChanged += (sender, change) =>
        {
            Assert.Same(breaker, sender);
            changes.Add((change.Previous, change.Current, change.RetryAfter));
        };

        breaker.RecordFailure();
        breaker.RecordSuccess();                          // closed, failures reset: no transition
        breaker.RecordFailure();
        breaker.RecordFailure();                          // threshold: opens
        Assert.False(breaker.TryAcquire());
        breaker.RecordFailure();                          // already open: nothing
        time.Advance(TimeSpan.FromSeconds(10));
        Assert.True(breaker.TryAcquire());                // trial call
        breaker.RecordFailure();                          // trial failed: open again
        time.Advance(TimeSpan.FromSeconds(10));
        Assert.True(breaker.TryAcquire());
        breaker.RecordSuccess();                          // trial passed: closed

        Assert.Equal(
        [
            (RuvioCircuitState.Closed, RuvioCircuitState.Open, TimeSpan.FromSeconds(10)),
            (RuvioCircuitState.Open, RuvioCircuitState.HalfOpen, TimeSpan.Zero),
            (RuvioCircuitState.HalfOpen, RuvioCircuitState.Open, TimeSpan.FromSeconds(10)),
            (RuvioCircuitState.Open, RuvioCircuitState.HalfOpen, TimeSpan.Zero),
            (RuvioCircuitState.HalfOpen, RuvioCircuitState.Closed, TimeSpan.Zero),
        ], changes);
    }

    [Fact]
    public void AThrowingListenerDoesNotFailTheCallOrStopTheTransition()
    {
        var breaker = Create(new ManualTime(), threshold: 1);

        breaker.StateChanged += (_, _) => throw new InvalidOperationException("listener");
        breaker.RecordFailure();

        Assert.Equal(RuvioCircuitState.Open, breaker.State);
    }

    [Fact]
    public void TransitionsAreCountedOnTheClientMeterByNewState()
    {
        var breaker = Create(new ManualTime(), threshold: 1);
        var states = new List<string>();
        using var listener = new MeterListener();

        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == "Ruvio.Client" && instrument.Name == "ruvio_client_circuit_transitions")
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            foreach (KeyValuePair<string, object?> tag in tags)
            {
                if (tag.Key == "state")
                    lock (states)
                        states.Add($"{tag.Value}:{value}");
            }
        });
        listener.Start();

        breaker.RecordFailure();

        lock (states)
            Assert.Contains("open:1", states);
    }

    [Fact]
    public void ClosedCircuitAdmitsEveryCallAndOpensAfterTheThresholdOfFailuresInARow()
    {
        var breaker = Create(new ManualTime());

        for (int i = 0; i < 2; i++)
        {
            Assert.True(breaker.TryAcquire());
            breaker.RecordFailure();
        }

        Assert.Equal(RuvioCircuitState.Closed, breaker.State);
        Assert.True(breaker.TryAcquire());
        breaker.RecordFailure();
        Assert.Equal(RuvioCircuitState.Open, breaker.State);
        Assert.False(breaker.TryAcquire());
        Assert.Equal(TimeSpan.FromSeconds(10), breaker.RetryAfter);
    }

    [Fact]
    public void ASuccessResetsTheCountOfFailuresInARow()
    {
        var breaker = Create(new ManualTime());

        for (int round = 0; round < 5; round++)
        {
            breaker.RecordFailure();
            breaker.RecordFailure();
            breaker.RecordSuccess();
        }

        Assert.Equal(RuvioCircuitState.Closed, breaker.State);
        Assert.True(breaker.TryAcquire());
    }

    [Fact]
    public void AfterTheBreakOneTrialCallGoesThroughAndItsAnswerDecides()
    {
        var time = new ManualTime();
        var breaker = Create(time, threshold: 1);

        breaker.RecordFailure();
        time.Advance(TimeSpan.FromSeconds(9));
        Assert.False(breaker.TryAcquire());
        Assert.Equal(TimeSpan.FromSeconds(1), breaker.RetryAfter);
        time.Advance(TimeSpan.FromSeconds(1));

        Assert.True(breaker.TryAcquire());
        Assert.Equal(RuvioCircuitState.HalfOpen, breaker.State);
        Assert.False(breaker.TryAcquire()); // the others wait for the trial call

        breaker.RecordFailure();
        Assert.Equal(RuvioCircuitState.Open, breaker.State);
        Assert.False(breaker.TryAcquire()); // a new break of the full length
        time.Advance(TimeSpan.FromSeconds(10));
        Assert.True(breaker.TryAcquire());

        breaker.RecordSuccess();
        Assert.Equal(RuvioCircuitState.Closed, breaker.State);
        Assert.True(breaker.TryAcquire());
        Assert.True(breaker.TryAcquire());
    }

    [Fact]
    public void AnAbandonedTrialCallFreesTheSlotForTheNextCaller()
    {
        var time = new ManualTime();
        var breaker = Create(time, threshold: 1);

        breaker.RecordFailure();
        time.Advance(TimeSpan.FromSeconds(10));
        Assert.True(breaker.TryAcquire());
        Assert.False(breaker.TryAcquire());

        breaker.RecordAbandoned();
        Assert.Equal(RuvioCircuitState.HalfOpen, breaker.State);
        Assert.True(breaker.TryAcquire());
    }

    [Fact]
    public void ALateSuccessFromBeforeTheCircuitOpenedDoesNotCloseIt()
    {
        var breaker = Create(new ManualTime(), threshold: 1);

        breaker.RecordFailure();
        breaker.RecordSuccess();
        Assert.Equal(RuvioCircuitState.Open, breaker.State);
    }

    [Fact]
    public async Task ManyThreadsOpenTheCircuitOnceAndOnlyOneGetsTheTrialCall()
    {
        var time = new ManualTime();
        var breaker = Create(time, threshold: 5);

        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(() =>
        {
            for (int i = 0; i < 100; i++)
                breaker.RecordFailure();
        })));
        Assert.Equal(RuvioCircuitState.Open, breaker.State);

        time.Advance(TimeSpan.FromSeconds(10));
        int admitted = 0;

        await Task.WhenAll(Enumerable.Range(0, 16).Select(_ => Task.Run(() =>
        {
            if (breaker.TryAcquire())
                Interlocked.Increment(ref admitted);
        })));
        Assert.Equal(1, admitted);
    }

    [Theory]
    [InlineData(0, 1000)]
    [InlineData(1, 0)]
    public void InvalidSettingsAreRejected(int threshold, int milliseconds)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new RuvioCircuitBreaker(new RuvioCircuitBreakerOptions
        {
            FailureThreshold = threshold == 0 ? 0 : 1,
            BreakDuration = TimeSpan.FromMilliseconds(milliseconds),
        }));
    }

    [Fact]
    public void OnlyOutagesAreClassifiedAsOutages()
    {
        var none = CancellationToken.None;

        Assert.True(RuvioOutageOptions.IsOutage(new SocketException(), none));
        Assert.True(RuvioOutageOptions.IsOutage(new IOException(), none));
        Assert.True(RuvioOutageOptions.IsOutage(new TimeoutException(), none));
        Assert.True(RuvioOutageOptions.IsOutage(new RuvioCircuitOpenException(TimeSpan.Zero), none));
        Assert.True(RuvioOutageOptions.IsOutage(new RuvioException("closed", closedBeforeReply: true), none));
        Assert.True(RuvioOutageOptions.IsOutage(new RuvioException("LOADING Ruvio is loading", isServerReply: true), none));
        Assert.True(RuvioOutageOptions.IsOutage(new RuvioException("MASTERDOWN x", isServerReply: true), none));
        Assert.True(RuvioOutageOptions.IsOutage(new RuvioException("CLUSTERDOWN x", isServerReply: true), none));
        Assert.True(RuvioOutageOptions.IsOutage(new OperationCanceledException(), none));

        Assert.False(RuvioOutageOptions.IsOutage(new RuvioException("WRONGTYPE x", isServerReply: true), none));
        Assert.False(RuvioOutageOptions.IsOutage(new RuvioException("ERR syntax", isServerReply: true), none));
        Assert.False(RuvioOutageOptions.IsOutage(new InvalidOperationException(), none));
        Assert.False(RuvioOutageOptions.IsOutage(new ArgumentException("x"), none));

        using var cancelled = new CancellationTokenSource();

        cancelled.Cancel();
        Assert.False(RuvioOutageOptions.IsOutage(new OperationCanceledException(cancelled.Token), cancelled.Token));
    }

    [Fact]
    public void OutageOptionsWithoutABreakerAreInert()
    {
        var options = new RuvioOutageOptions();

        Assert.Equal(RuvioOutageMode.Throw, options.Mode);
        Assert.Null(options.CircuitBreaker);
        Assert.Null(options.Breaker);
        Assert.True(options.TryEnter());
        Assert.Equal(TimeSpan.Zero, options.RetryAfter);
        options.Succeeded();
        Assert.True(options.Failed(new IOException(), default));
        Assert.True(options.TryEnter());
        Assert.False(new RuvioOutageOptions { Mode = (RuvioOutageMode)9 }.IsValid());
    }

    [Fact]
    public void OutageOptionsDriveTheBreakerFromReportedErrors()
    {
        var time = new ManualTime();
        var options = new RuvioOutageOptions(RuvioOutageMode.FailOpen)
        {
            CircuitBreaker = new RuvioCircuitBreakerOptions { FailureThreshold = 2, BreakDuration = TimeSpan.FromSeconds(5), TimeProvider = time },
        };

        // A server error reply proves Ruvio is up; it is no outage and never counts.
        for (int i = 0; i < 5; i++)
        {
            Assert.True(options.TryEnter());
            Assert.False(options.Failed(new RuvioException("WRONGTYPE", isServerReply: true), default));
        }

        Assert.True(options.TryEnter());
        Assert.True(options.Failed(new IOException(), default));
        Assert.True(options.TryEnter());
        Assert.True(options.Failed(new TimeoutException(), default));
        Assert.False(options.TryEnter());
        Assert.Equal(TimeSpan.FromSeconds(5), options.RetryAfter);

        time.Advance(TimeSpan.FromSeconds(5));
        Assert.True(options.TryEnter());
        options.Succeeded();
        Assert.True(options.TryEnter());
        Assert.Equal(RuvioCircuitState.Closed, options.Breaker!.State);
    }

    [Fact]
    public void ACallerCancellationNeverCountsAgainstTheCircuit()
    {
        var options = new RuvioOutageOptions
        {
            CircuitBreaker = new RuvioCircuitBreakerOptions { FailureThreshold = 1 },
        };
        using var cancelled = new CancellationTokenSource();

        cancelled.Cancel();

        for (int i = 0; i < 5; i++)
        {
            Assert.True(options.TryEnter());
            Assert.False(options.Failed(new OperationCanceledException(cancelled.Token), cancelled.Token));
        }

        Assert.Equal(RuvioCircuitState.Closed, options.Breaker!.State);
    }
}
