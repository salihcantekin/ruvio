using Xunit;
using System.Globalization;
using Microsoft.Extensions.Caching.Distributed;
using Ruvio.Client;
using Ruvio.Extensions.Caching;
using ZiggyCreatures.Caching.Fusion;
using ZiggyCreatures.Caching.Fusion.Serialization.SystemTextJson;

namespace Ruvio.Extensions.Caching.FusionCache.Tests;

/// <summary>FusionCache uses Ruvio as its second level (L2) through <see cref="IDistributedCache"/>.</summary>
public sealed class FusionCacheL2Tests
{
    private static async Task<RuvioConnection> Connect()
    {
        string[] address = Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR")!.Split(':');

        return await RuvioConnection.ConnectAsync(address[0], int.Parse(address[1], CultureInfo.InvariantCulture));
    }

    private static (IFusionCache Cache, RuvioDistributedCache L2) Node(IRuvioClient client, string prefix)
    {
        var l2 = new RuvioDistributedCache(client, Microsoft.Extensions.Options.Options.Create(
            new RuvioCacheOptions { InstanceName = prefix }));
        var cache = new ZiggyCreatures.Caching.Fusion.FusionCache(new FusionCacheOptions { CacheName = "node" + Guid.NewGuid().ToString("N") });

        cache.SetupDistributedCache(l2, new FusionCacheSystemTextJsonSerializer());

        return (cache, l2);
    }

    [LiveFact]
    public async Task A_value_written_by_one_node_is_read_by_another_through_ruvio()
    {
        await using RuvioConnection connection = await Connect();
        IRuvioClient client = await connection.GetDefaultDatabaseAsync();
        string prefix = "fc-" + Guid.NewGuid().ToString("N") + ":";
        var (a, _) = Node(client, prefix);
        var (b, _) = Node(client, prefix);

        await a.SetAsync("k", "hello", new FusionCacheEntryOptions(TimeSpan.FromMinutes(1)));

        Assert.Equal("hello", await b.GetOrDefaultAsync<string>("k"));
    }

    [LiveFact]
    public async Task Remove_reaches_the_shared_level()
    {
        await using RuvioConnection connection = await Connect();
        IRuvioClient client = await connection.GetDefaultDatabaseAsync();
        string prefix = "fc-" + Guid.NewGuid().ToString("N") + ":";
        var (a, _) = Node(client, prefix);
        var (b, _) = Node(client, prefix);

        await a.SetAsync("k", 7, new FusionCacheEntryOptions(TimeSpan.FromMinutes(1)));
        await a.RemoveAsync("k");

        Assert.False((await b.TryGetAsync<int>("k")).HasValue);
    }

    [LiveFact]
    public async Task GetOrSet_runs_the_factory_once_and_the_second_node_reuses_the_stored_value()
    {
        await using RuvioConnection connection = await Connect();
        IRuvioClient client = await connection.GetDefaultDatabaseAsync();
        string prefix = "fc-" + Guid.NewGuid().ToString("N") + ":";
        var (a, _) = Node(client, prefix);
        var (b, _) = Node(client, prefix);
        int calls = 0;

        var first = await a.GetOrSetAsync("k", async _ => { calls++; await Task.Yield(); return new Item("x", 3); }, TimeSpan.FromMinutes(1));
        var second = await b.GetOrSetAsync("k", async _ => { calls++; await Task.Yield(); return new Item("y", 4); }, TimeSpan.FromMinutes(1));

        Assert.Equal(1, calls);
        Assert.Equal(first, second);
    }

    [LiveFact]
    public async Task An_entry_expires_in_the_shared_level()
    {
        await using RuvioConnection connection = await Connect();
        IRuvioClient client = await connection.GetDefaultDatabaseAsync();
        string prefix = "fc-" + Guid.NewGuid().ToString("N") + ":";
        var (a, _) = Node(client, prefix);
        var (b, _) = Node(client, prefix);

        await a.SetAsync("k", "v", new FusionCacheEntryOptions(TimeSpan.FromMilliseconds(400)));
        await Task.Delay(900);

        Assert.False((await b.TryGetAsync<string>("k")).HasValue);
    }

    private sealed record Item(string Name, int Count);
}

public sealed class LiveFactAttribute : FactAttribute
{
    public LiveFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR")))
            Skip = "Set RUVIO_TEST_ADDR to run against an isolated live Ruvio server.";
    }
}
