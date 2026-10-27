using System.Globalization;
using Ruvio.Client;

namespace Ruvio.AspNetCore.DistributedLock;

/// <summary>
/// Acquires distributed locks on one Ruvio client. A lock is a key holding a random owner token:
/// <c>SET key token NX PX lease</c> takes it, <c>SET key token IFEQ token PX lease</c> renews it and
/// <c>DELEX key IFEQ token</c> releases it, each in one command and only for the owner. The provider
/// neither owns nor disposes the client.
/// </summary>
/// <remarks>
/// Needs a Ruvio server with <c>SET … IFEQ</c> and <c>DELEX</c>. The lock lives on one server, so a
/// failover that loses the key can let two owners overlap; guard writes that must never overlap with
/// the resource's own versioning as well.
/// </remarks>
public sealed class RuvioLockProvider
{
    private readonly IRuvioClient client;
    private readonly RuvioLockOptions options;

    internal RuvioLockOptions Options => options;

    /// <summary>Creates a provider.</summary>
    public RuvioLockProvider(IRuvioClient client, RuvioLockOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(client);
        this.client = client;
        this.options = options ?? new RuvioLockOptions();
        this.options.Validate();
    }

    /// <summary>
    /// Takes the lock for <paramref name="lease"/>, retrying for up to <paramref name="wait"/>.
    /// Returns null when it stayed taken. A zero wait tries exactly once.
    /// </summary>
    public async ValueTask<IRuvioLock?> TryAcquireAsync(
        string resource,
        TimeSpan lease,
        TimeSpan wait = default,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(resource);
        string leaseText = LeaseMilliseconds(lease).ToString(CultureInfo.InvariantCulture);
        string key = options.KeyPrefix + resource;
        // Random per acquisition: IFEQ and DELEX match on it, so a holder whose lease expired cannot
        // renew or release a lock that another owner now holds. A fenced lock gets its token from LEASE.
        bool fenced = options.Fenced;
        string? token = fenced ? null : Guid.NewGuid().ToString("N");
        IReadOnlyList<string> command = fenced ? ["LEASE", key, leaseText] : ["SET", key, token!, "NX", "PX", leaseText];
        long start = options.TimeProvider.GetTimestamp();
        TimeSpan pause = options.RetryMin;

        while (true)
        {
            RespValue? reply = null;

            try
            {
                reply = await client.ExecuteAsync(command, cancellationToken).ConfigureAwait(false);
                Check(reply);
            }
            catch (RuvioException exception) when (fenced && exception.IsServerReply && IsWrongType(exception.Message))
            {
                // A plain lock (a string) taken by an instance without fencing answers WRONGTYPE to LEASE: it is
                // held, like a null reply.
                reply = null;
            }

            if (reply is not null && reply.Kind != RespKind.Null)
            {
                return fenced
                    ? new RuvioLock(client, options, resource, key, reply.Integer.ToString(CultureInfo.InvariantCulture), lease, reply.Integer)
                    : new RuvioLock(client, options, resource, key, token!, lease);
            }

            TimeSpan remaining = wait - options.TimeProvider.GetElapsedTime(start);

            if (remaining <= TimeSpan.Zero)
            {
                return null;
            }

            double jitter = 0.5 + Random.Shared.NextDouble();
            TimeSpan delay = TimeSpan.FromTicks((long)(pause.Ticks * jitter));

            await Task.Delay(delay < remaining ? delay : remaining, options.TimeProvider, cancellationToken).ConfigureAwait(false);
            pause = pause + pause > options.RetryMax ? options.RetryMax : pause + pause;
        }
    }

    /// <summary>Like <see cref="TryAcquireAsync"/>, but throws <see cref="TimeoutException"/> when the lock stayed taken.</summary>
    public async ValueTask<IRuvioLock> AcquireAsync(
        string resource,
        TimeSpan lease,
        TimeSpan wait,
        CancellationToken cancellationToken = default)
    {
        return await TryAcquireAsync(resource, lease, wait, cancellationToken).ConfigureAwait(false)
            ?? throw new TimeoutException($"The lock '{resource}' stayed taken for {wait}.");
    }

    /// <summary>
    /// Runs <paramref name="work"/> at most once per <paramref name="period"/> across every instance, with the server's
    /// <c>ONCE</c>: the first caller in a period sets a marker and runs the work; the others get false and skip it.
    /// The period starts at that first call (it is not aligned to the clock), and the marker is set before the work
    /// runs, so work that fails is not run again in the same period.
    /// </summary>
    /// <param name="name">The job name; the marker key is the key prefix, <c>once:</c> and the name.</param>
    /// <param name="period">At least one millisecond.</param>
    /// <param name="work">The work; it receives <paramref name="cancellationToken"/>.</param>
    /// <param name="cancellationToken">Cancels the request and the work.</param>
    /// <returns>True when this call ran the work.</returns>
    public async ValueTask<bool> RunOncePerPeriodAsync(
        string name,
        TimeSpan period,
        Func<CancellationToken, Task> work,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(work);
        long milliseconds = (long)period.TotalMilliseconds;

        if (milliseconds < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(period), "The period must be at least one millisecond.");
        }

        IReadOnlyList<string> command = ["ONCE", options.KeyPrefix + "once:" + name, milliseconds.ToString(CultureInfo.InvariantCulture)];
        RespValue reply = await client.ExecuteAsync(command, cancellationToken).ConfigureAwait(false);

        Check(reply);

        if (reply.Kind != RespKind.Integer || reply.Integer != 1)
        {
            return false;
        }

        await work(cancellationToken).ConfigureAwait(false);

        return true;
    }

    internal static long LeaseMilliseconds(TimeSpan lease)
    {
        long milliseconds = (long)lease.TotalMilliseconds;

        if (milliseconds < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(lease), "The lease must be at least one millisecond.");
        }

        return milliseconds;
    }

    private static bool IsWrongType(string message) => message.StartsWith("WRONGTYPE", StringComparison.Ordinal);

    internal static void Check(RespValue reply)
    {
        if (reply.Kind == RespKind.Error)
        {
            throw new RuvioException(reply.Text ?? "The server rejected the lock command.", isServerReply: true);
        }
    }
}
