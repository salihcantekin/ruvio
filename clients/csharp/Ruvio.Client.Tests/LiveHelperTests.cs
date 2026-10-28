using System.Text.Json.Serialization;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

/// <summary>The convenience helpers against a live server (database 12).</summary>
[Collection("Live")]
public sealed class LiveHelperTests
{
    private const int Db = 12;

    [LiveFact(Timeout = 30000)]
    public async Task SetDurabilityIsRefusedOnAServerWithoutReplication()
    {
        await using RuvioConnection connection = await LiveServer.ConnectAsync(Db);
        RuvioClient db = await connection.GetDefaultDatabaseAsync();

        RuvioException error = await Assert.ThrowsAsync<RuvioException>(async () => await db.SetDurabilityAsync(1));
        Assert.Contains("needs replication", error.Message);
    }

    [LiveFact(Timeout = 30000)]
    public async Task Lock_is_exclusive_renewable_and_never_releases_another_owner()
    {
        await using RuvioConnection connection = await LiveServer.ConnectAsync(Db);
        RuvioClient db = await connection.GetDefaultDatabaseAsync();

        await using (RuvioLock? held = await db.TryAcquireLockAsync("lock", TimeSpan.FromSeconds(10)))
        {
            Assert.NotNull(held);
            Assert.Equal(held.Token, await db.GetStringAsync("lock"));
            Assert.Null(await db.TryAcquireLockAsync("lock", TimeSpan.FromSeconds(10)));
            Assert.True(await held.RenewAsync(TimeSpan.FromSeconds(30)));
            Assert.InRange(await db.GetTimeToLiveMillisecondsAsync("lock"), 10001, 30000);
        }

        Assert.Null(await db.GetStringAsync("lock"));

        // An expired lock taken by someone else is left alone by the first owner's release and renew.
        RuvioLock first = (await db.TryAcquireLockAsync("lock", TimeSpan.FromMilliseconds(50)))!;
        await Task.Delay(120);
        RuvioLock second = (await db.TryAcquireLockAsync("lock", TimeSpan.FromSeconds(10)))!;
        Assert.NotNull(second);
        Assert.False(await first.RenewAsync());
        Assert.False(await first.ReleaseAsync());
        Assert.Equal(second.Token, await db.GetStringAsync("lock"));
        Assert.True(await second.ReleaseAsync());
        Assert.False(await second.ReleaseAsync());
        Assert.True(second.IsReleased);
    }

    [LiveFact(Timeout = 30000)]
    public async Task AcquireLock_waits_for_the_holder()
    {
        await using RuvioConnection connection = await LiveServer.ConnectAsync(Db);
        RuvioClient db = await connection.GetDefaultDatabaseAsync();
        RuvioLock holder = (await db.TryAcquireLockAsync("wait-lock", TimeSpan.FromSeconds(10)))!;

        Assert.Null(await db.AcquireLockAsync("wait-lock", TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(60)));
        Task<RuvioLock?> waiting = db.AcquireLockAsync("wait-lock", TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5)).AsTask();
        await Task.Delay(30);
        await holder.DisposeAsync();
        await using RuvioLock? next = await waiting;
        Assert.NotNull(next);
    }

    [LiveFact(Timeout = 30000)]
    public async Task Lease_semaphore_and_rate_limit_helpers()
    {
        await using RuvioConnection connection = await LiveServer.ConnectAsync(Db);
        RuvioClient db = await connection.GetDefaultDatabaseAsync();
        long token;

        await using (RuvioLease? lease = await db.TryAcquireLeaseAsync("lease", TimeSpan.FromSeconds(10)))
        {
            Assert.NotNull(lease);
            token = lease.Token;
            Assert.Null(await db.TryAcquireLeaseAsync("lease", TimeSpan.FromSeconds(10)));
        }

        await using (RuvioLease? next = await db.TryAcquireLeaseAsync("lease", TimeSpan.FromSeconds(10)))
        {
            Assert.True(next!.Token > token);
        }

        RuvioLease a = (await db.TryAcquireSemaphoreAsync("sem", 2, TimeSpan.FromSeconds(10)))!;
        RuvioLease b = (await db.TryAcquireSemaphoreAsync("sem", 2, TimeSpan.FromSeconds(10)))!;
        Assert.Null(await db.TryAcquireSemaphoreAsync("sem", 2, TimeSpan.FromSeconds(10)));
        Assert.True(await a.ReleaseAsync());
        Assert.False(await a.ReleaseAsync());
        await using (RuvioLease? c = await db.TryAcquireSemaphoreAsync("sem", 2, TimeSpan.FromSeconds(10)))
        {
            Assert.NotNull(c);
        }

        await b.DisposeAsync();

        Assert.Equal(new RateLimitResult(true, 1), await db.RateLimitAsync("rl", 2, TimeSpan.FromSeconds(10)));
        Assert.Equal(new RateLimitResult(true, 0), await db.RateLimitAsync("rl", 2, TimeSpan.FromSeconds(10)));
        Assert.Equal(new RateLimitResult(false, 0), await db.RateLimitAsync("rl", 2, TimeSpan.FromSeconds(10)));
        Assert.Equal(1, await db.LimitSlidingAsync("sl", 2, 60_000));
        Assert.Equal(0, await db.LimitSlidingAsync("sl", 2, 60_000));

        LimitOutcome refused = await db.LimitSlidingWithRetryAsync("sl", 2, 60_000);

        Assert.False(refused.Allowed);
        Assert.True(refused.RetryAfterMs > 0);
        Assert.Equal(3, await db.LimitTokensAsync("tk", 5, 60_000, 2));
        Assert.Equal(new LimitOutcome(true, 0, 0), await db.LimitTokensWithRetryAsync("tk", 5, 60_000, 3));
        Assert.Null(await db.LimitTokensAsync("tk", 5, 60_000));
    }

    [LiveFact(Timeout = 30000)]
    public async Task GetOrCreate_runs_the_factory_once_for_concurrent_callers()
    {
        await using RuvioConnection connection = await LiveServer.ConnectAsync(Db);
        RuvioClient db = await connection.GetDefaultDatabaseAsync();
        int calls = 0;

        async ValueTask<string> Build(CancellationToken token)
        {
            Interlocked.Increment(ref calls);
            await Task.Delay(100, token);

            return "built";
        }

        string[] values = await Task.WhenAll(Enumerable.Range(0, 20)
            .Select(_ => db.GetOrCreateAsync("cache", TimeSpan.FromSeconds(30), Build).AsTask()));

        Assert.All(values, value => Assert.Equal("built", value));
        Assert.Equal(1, calls);
        Assert.Equal("built", await db.GetOrCreateAsync("cache", TimeSpan.FromSeconds(30), Build));
        Assert.Equal(1, calls);

        // Typed form with a source-generated JsonTypeInfo.
        Item item = await db.GetOrCreateAsync("cache:item", TimeSpan.FromSeconds(30),
            _ => new ValueTask<Item>(new Item("a", 2)), HelperJson.Default.Item);
        Assert.Equal(new Item("a", 2), item);
        Item cached = await db.GetOrCreateAsync("cache:item", TimeSpan.FromSeconds(30),
            _ => throw new InvalidOperationException("must hit"), HelperJson.Default.Item);
        Assert.Equal(item, cached);

        // A waiter that outlasts WaitTimeout builds the value itself.
        FillResult marker = await db.FillGetAsync("stuck", TimeSpan.FromSeconds(10));
        Assert.Equal(FillState.Fill, marker.State);
        string own = await db.GetOrCreateAsync("stuck", TimeSpan.FromSeconds(30), _ => new ValueTask<string>("own"),
            new GetOrCreateOptions { WaitTimeout = TimeSpan.FromMilliseconds(50) });
        Assert.Equal("own", own);
        Assert.Equal("own", await db.GetStringAsync("stuck"));
    }
}

public sealed record Item(string Name, int Count);

[JsonSerializable(typeof(Item))]
internal sealed partial class HelperJson : JsonSerializerContext;
