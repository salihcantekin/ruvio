using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Ruvio.AspNetCore.DistributedLock;

/// <summary>
/// Metrics and traces of the lock middleware, in the same shape as the rate-limiting package so one
/// dashboard can show both. Subscribe with <c>AddMeter("Ruvio.AspNetCore.DistributedLock")</c> and
/// <c>AddSource("Ruvio.AspNetCore.DistributedLock")</c> (OpenTelemetry), or <c>dotnet-counters</c>.
/// </summary>
/// <remarks>
/// Instruments: <c>ruvio.lock.acquired</c>, <c>ruvio.lock.rejected</c> (lock stayed taken, a sign of
/// contention), <c>ruvio.lock.lost</c> (a lease was lost mid-request, a sign of an unhealthy server
/// link) and <c>ruvio.lock.errors</c> (storage failures), all tagged with <c>resource</c> only when
/// the endpoint uses a fixed name (templated names are not tagged, to keep cardinality bounded), plus
/// the histogram <c>ruvio.lock.wait_duration</c> in milliseconds: how long requests waited to get or
/// give up the lock. <c>ruvio.lock.outage</c> counts requests that met an unreachable Ruvio or an open circuit
/// breaker, tagged with <c>mode</c> and <c>reason</c>.
/// </remarks>
public static class RuvioLockTelemetry
{
    /// <summary>The meter and activity source name.</summary>
    public const string Name = "Ruvio.AspNetCore.DistributedLock";

    internal static readonly ActivitySource Source = new(Name);
    private static readonly Meter Meter = new(Name);

    internal static readonly Counter<long> Acquired = Meter.CreateCounter<long>("ruvio.lock.acquired", description: "Requests that got the lock.");
    internal static readonly Counter<long> Rejected = Meter.CreateCounter<long>("ruvio.lock.rejected", description: "Requests rejected because the lock stayed taken.");
    internal static readonly Counter<long> Lost = Meter.CreateCounter<long>("ruvio.lock.lost", description: "Locks lost while the request was running.");
    internal static readonly Counter<long> Errors = Meter.CreateCounter<long>("ruvio.lock.errors", description: "Lock storage failures.");
    internal static readonly Counter<long> Outage = Meter.CreateCounter<long>("ruvio.lock.outage", description: "Requests that met an unreachable Ruvio or an open circuit.");
    internal static readonly Histogram<double> WaitDuration = Meter.CreateHistogram<double>("ruvio.lock.wait_duration", unit: "ms", description: "Time spent acquiring (or giving up on) the lock.");
}
