using Microsoft.AspNetCore.Http;
using Ruvio.Client;

namespace Ruvio.AspNetCore.DistributedLock;

/// <summary>Settings shared by every lock a <see cref="RuvioLockProvider"/> hands out.</summary>
public sealed class RuvioLockOptions
{
    /// <summary>Prefix for lock keys. Use one per application so resources never collide.</summary>
    public string KeyPrefix { get; set; } = "lock:";

    /// <summary>First pause between acquisition attempts. It doubles up to <see cref="RetryMax"/>, with jitter.</summary>
    public TimeSpan RetryMin { get; set; } = TimeSpan.FromMilliseconds(10);

    /// <summary>Longest pause between acquisition attempts.</summary>
    public TimeSpan RetryMax { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Renew the lease in the background, every third of the lease, while the lock is held.
    /// When a renewal proves the lock was lost, <see cref="IRuvioLock.LockLost"/> is cancelled.
    /// </summary>
    public bool AutoRenew { get; set; } = true;

    /// <summary>
    /// Take locks with the server's <c>LEASE</c> (renewed with <c>RENEW</c>, freed with <c>RELEASE</c>) so each lock carries a
    /// <see cref="IRuvioLock.FencingToken"/>. Off by default. Turn it on for every instance that shares a key prefix: while a
    /// plain lock (from an instance with it off) holds a resource, a fenced instance sees it as taken, and the other way round.
    /// </summary>
    public bool Fenced { get; set; }

    /// <summary>
    /// How long an HTTP request may hold its lock before the lease expires, unless the endpoint sets its own.
    /// With <see cref="AutoRenew"/> on, the lease is renewed while the request runs, so this is only how long a
    /// crashed process keeps other requests out: keep it short (the default is 30 seconds).
    /// </summary>
    public TimeSpan DefaultLease { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How long an HTTP request waits for a taken lock before giving up with <see cref="ConflictStatusCode"/>,
    /// unless the endpoint sets its own. Zero (the default) tries once and answers immediately, so a double click
    /// or a client retry never queues requests behind each other.
    /// </summary>
    public TimeSpan DefaultWait { get; set; } = TimeSpan.Zero;

    /// <summary>
    /// The status code sent when the lock stayed taken. 409 (Conflict) by default; 423 (Locked) or
    /// 429 (Too Many Requests) fit some APIs better. The request body is never read and the handler never runs.
    /// </summary>
    public int ConflictStatusCode { get; set; } = StatusCodes.Status409Conflict;

    /// <summary>
    /// The clock behind the renewal timer and the acquisition wait. The system clock by default;
    /// tests can pass a fake one to run a lease through its life without waiting.
    /// </summary>
    public TimeProvider TimeProvider { get; set; } = TimeProvider.System;

    /// <summary>
    /// What an HTTP request does when Ruvio cannot be reached. The default, <see cref="RuvioOutageMode.Throw"/>, keeps
    /// today's behavior: the <see cref="RuvioLockAttribute"/> middleware answers 503 and the request does not run, while
    /// <c>WithDistributedLock</c> endpoints let the storage exception reach the application.
    /// <see cref="RuvioOutageMode.FailClosed"/> makes both answer 503 (with <c>Retry-After</c> while a circuit is open).
    /// <see cref="RuvioOutageMode.FailOpen"/> runs the request with no lock, so it is not exclusive while Ruvio is away.
    /// Add a circuit breaker with <see cref="RuvioOutageOptions.CircuitBreaker"/> to stop waiting on a Ruvio that is down.
    /// Applies to the HTTP integrations only; <see cref="IRuvioLock"/> calls made in code still throw.
    /// </summary>
    public RuvioOutageOptions Outage { get; } = new();

    internal void Validate()
    {
        Outage.Validate();

        ArgumentNullException.ThrowIfNull(TimeProvider);

        if (string.IsNullOrEmpty(KeyPrefix))
        {
            throw new ArgumentException("KeyPrefix must not be empty.", nameof(KeyPrefix));
        }

        if (DefaultLease < TimeSpan.FromMilliseconds(1))
        {
            throw new ArgumentOutOfRangeException(nameof(DefaultLease), "DefaultLease must be at least one millisecond.");
        }

        if (DefaultWait < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(DefaultWait), "DefaultWait must not be negative.");
        }

        if (ConflictStatusCode is < 400 or > 599)
        {
            throw new ArgumentOutOfRangeException(nameof(ConflictStatusCode), "ConflictStatusCode must be a 4xx or 5xx status.");
        }

        if (RetryMin <= TimeSpan.Zero || RetryMax < RetryMin)
        {
            throw new ArgumentOutOfRangeException(nameof(RetryMin), "RetryMin must be positive and not above RetryMax.");
        }
    }
}
