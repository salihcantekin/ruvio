using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.DistributedLock.Tests;

/// <summary>Run against a real server: set RUVIO_TEST_ADDR to host:port, otherwise each test returns at once.</summary>
public class LiveTests
{
    internal static Task<(RuvioConnection Connection, IRuvioClient Client)?> TryConnectAsync() => ConnectAsync();

    private static async Task<(RuvioConnection Connection, IRuvioClient Client)?> ConnectAsync()
    {
        string? address = Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR");

        if (string.IsNullOrWhiteSpace(address))
            return null;

        var uri = new Uri("tcp://" + address);
        RuvioConnection connection = await RuvioConnection.ConnectAsync(uri.Host, uri.Port);

        return (connection, await connection.GetDefaultDatabaseAsync());
    }

    private static RuvioLockProvider Provider(IRuvioClient client) =>
        new(client, new RuvioLockOptions { KeyPrefix = "test:lock:" + Guid.NewGuid().ToString("N") + ":" });

    [Fact(Timeout = 30000)]
    public async Task OnlyOneOwnerHoldsAResourceAndReleaseHandsItOver()
    {
        if (await ConnectAsync() is not { } connected)
            return;

        await using RuvioConnection connection = connected.Connection;
        IRuvioClient client = connected.Client;
        RuvioLockProvider provider = Provider(client);

        await using IRuvioLock first = await provider.AcquireAsync("job", TimeSpan.FromSeconds(5), TimeSpan.Zero);

        Assert.Null(await provider.TryAcquireAsync("job", TimeSpan.FromSeconds(5)));
        Assert.True(await first.ReleaseAsync());
        Assert.False(await first.ReleaseAsync());

        await using IRuvioLock? second = await provider.TryAcquireAsync("job", TimeSpan.FromSeconds(5));

        Assert.NotNull(second);
    }

    [Fact(Timeout = 30000)]
    public async Task AWaitingCallerGetsTheLockWhenTheHolderReleases()
    {
        if (await ConnectAsync() is not { } connected)
            return;

        await using RuvioConnection connection = connected.Connection;
        IRuvioClient client = connected.Client;
        RuvioLockProvider provider = Provider(client);
        IRuvioLock holder = await provider.AcquireAsync("queue", TimeSpan.FromSeconds(5), TimeSpan.Zero);
        Task<IRuvioLock> waiter = provider.AcquireAsync("queue", TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(3)).AsTask();

        await Task.Delay(100);
        Assert.False(waiter.IsCompleted);
        await holder.ReleaseAsync();

        await using IRuvioLock next = await waiter;

        Assert.True(next.IsHeld);
    }

    [Fact(Timeout = 30000)]
    public async Task ALockThatExpiredIsNotReleasedForANewOwner()
    {
        if (await ConnectAsync() is not { } connected)
            return;

        await using RuvioConnection connection = connected.Connection;
        IRuvioClient client = connected.Client;
        var options = new RuvioLockOptions { KeyPrefix = "test:lock:" + Guid.NewGuid().ToString("N") + ":", AutoRenew = false };
        var provider = new RuvioLockProvider(client, options);
        IRuvioLock stale = await provider.AcquireAsync("expiring", TimeSpan.FromMilliseconds(100), TimeSpan.Zero);

        await Task.Delay(300);

        await using IRuvioLock? fresh = await provider.TryAcquireAsync("expiring", TimeSpan.FromSeconds(5));

        Assert.NotNull(fresh);
        Assert.False(await stale.ReleaseAsync());
        Assert.False(await stale.ExtendAsync());
        Assert.True(fresh!.IsHeld);
    }

    [Fact(Timeout = 30000)]
    public async Task AutoRenewalKeepsAShortLeaseAlive()
    {
        if (await ConnectAsync() is not { } connected)
            return;

        await using RuvioConnection connection = connected.Connection;
        IRuvioClient client = connected.Client;
        RuvioLockProvider provider = Provider(client);

        // A 300 ms lease loses to a stalled shared CI runner (a renewal tick can be delayed past a whole lease).
        await using IRuvioLock held = await provider.AcquireAsync("renewed", TimeSpan.FromSeconds(2), TimeSpan.Zero);

        await Task.Delay(3000);

        Assert.True(held.IsHeld);
        Assert.False(held.LockLost.IsCancellationRequested);
        Assert.Null(await provider.TryAcquireAsync("renewed", TimeSpan.FromSeconds(1)));
    }

    [Fact(Timeout = 30000)]
    public async Task LockLostFiresWhenAnotherOwnerTakesTheKey()
    {
        if (await ConnectAsync() is not { } connected)
            return;

        await using RuvioConnection connection = connected.Connection;
        IRuvioClient client = connected.Client;
        string prefix = "test:lock:" + Guid.NewGuid().ToString("N") + ":";
        var provider = new RuvioLockProvider(client, new RuvioLockOptions { KeyPrefix = prefix });

        await using IRuvioLock held = await provider.AcquireAsync("stolen", TimeSpan.FromMilliseconds(300), TimeSpan.Zero);

        await client.SetAsync(prefix + "stolen", "someone-else");

        // The renewal tick notices the takeover; a shared CI runner can delay it well past one lease.
        for (int attempt = 0; attempt < 100 && !held.LockLost.IsCancellationRequested; attempt++)
            await Task.Delay(100);

        Assert.True(held.LockLost.IsCancellationRequested);
        Assert.False(held.IsHeld);
    }
}
