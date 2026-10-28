using System.Globalization;
using Ruvio.Client;

namespace Ruvio.AspNetCore.DistributedLock;

internal sealed class RuvioLock : IRuvioLock
{
    private readonly IRuvioClient client;
    private readonly string key;
    private readonly string token;
    private readonly long? fencingToken;
    private readonly TimeSpan lease;
    private readonly TimeProvider time;
    private readonly CancellationTokenSource lost = new();
    private readonly CancellationTokenSource stopRenewing = new();
    private readonly Task renewer;
    private long renewedAt;
    private TimeSpan currentLease;
    private int state; // 0 held, 1 lost, 2 released

    internal RuvioLock(IRuvioClient client, RuvioLockOptions options, string resource, string key, string token, TimeSpan lease,
        long? fencingToken = null)
    {
        this.client = client;
        this.key = key;
        this.token = token;
        this.fencingToken = fencingToken;
        this.lease = lease;
        time = options.TimeProvider;
        renewedAt = time.GetTimestamp();
        currentLease = lease;
        Resource = resource;
        renewer = options.AutoRenew ? Task.Run(RenewLoopAsync) : Task.CompletedTask;
    }

    public string Resource { get; }

    public bool IsHeld => Volatile.Read(ref state) == 0;

    public CancellationToken LockLost => lost.Token;

    public long? FencingToken => fencingToken;

    public async ValueTask<bool> ExtendAsync(TimeSpan? newLease = null, CancellationToken cancellationToken = default)
    {
        if (!IsHeld)
        {
            return false;
        }

        TimeSpan requested = newLease ?? lease;
        string leaseText = RuvioLockProvider.LeaseMilliseconds(requested).ToString(CultureInfo.InvariantCulture);
        // A fenced lock is a server lease: RENEW answers 0 once the token no longer holds it.
        IReadOnlyList<string> command = fencingToken is null
            ? ["SET", key, token, "IFEQ", token, "PX", leaseText]
            : ["RENEW", key, token, leaseText];
        RespValue reply = await client.ExecuteAsync(command, cancellationToken).ConfigureAwait(false);

        RuvioLockProvider.Check(reply);

        if (reply.Kind == RespKind.Null || (fencingToken is not null && reply.Integer == 0))
        {
            MarkLost();

            return false;
        }

        currentLease = requested;
        Volatile.Write(ref renewedAt, time.GetTimestamp());

        return true;
    }

    public async ValueTask<bool> ReleaseAsync(CancellationToken cancellationToken = default)
    {
        int before = Interlocked.Exchange(ref state, 2);

        await stopRenewing.CancelAsync().ConfigureAwait(false);
        await renewer.ConfigureAwait(false);

        if (before == 2)
        {
            return false;
        }

        // DELEX (or RELEASE for a lease) removes the lock only while it still holds our token, so a lock
        // that expired and was taken by another owner is left alone.
        IReadOnlyList<string> command = fencingToken is null ? ["DELEX", key, "IFEQ", token] : ["RELEASE", key, token];
        RespValue reply = await client.ExecuteAsync(command, cancellationToken).ConfigureAwait(false);

        RuvioLockProvider.Check(reply);

        return reply.Kind == RespKind.Integer && reply.Integer > 0;
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await ReleaseAsync().ConfigureAwait(false);
        }

        catch (RuvioException)
        {
            // The lease expires on its own; a failed release must not hide the guarded work's result.
        }

        stopRenewing.Dispose();
        lost.Dispose();
    }

    private void MarkLost()
    {
        if (Interlocked.CompareExchange(ref state, 1, 0) == 0)
        {
            lost.Cancel();
        }
    }

    // Renews every third of the lease. A failed attempt is retried at the next tick; once a whole
    // lease passed without a successful renewal the lock may be someone else's, so it is reported lost.
    private async Task RenewLoopAsync()
    {
        CancellationToken stop = stopRenewing.Token;

        try
        {
            while (IsHeld)
            {
                TimeSpan interval = currentLease / 3;

                // A lease under 3 ms gives a zero interval, which would fire renewals back to back; keep a 1 ms floor.
                await Task.Delay(interval < TimeSpan.FromMilliseconds(1) ? TimeSpan.FromMilliseconds(1) : interval, time, stop).ConfigureAwait(false);

                try
                {
                    await ExtendAsync(null, stop).ConfigureAwait(false);
                }

                catch (Exception exception) when (exception is RuvioException or IOException or ObjectDisposedException)
                {
                    if (time.GetElapsedTime(Volatile.Read(ref renewedAt)) >= currentLease)
                    {
                        MarkLost();
                    }
                }
            }
        }

        catch (OperationCanceledException)
        {
            // Released: nothing left to renew.
        }
    }
}
