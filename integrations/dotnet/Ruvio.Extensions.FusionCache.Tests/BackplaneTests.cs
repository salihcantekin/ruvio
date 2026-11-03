using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Ruvio.Extensions.Caching;
using Xunit;
using ZiggyCreatures.Caching.Fusion;
using ZiggyCreatures.Caching.Fusion.Serialization.SystemTextJson;

namespace Ruvio.Extensions.FusionCache.Tests;

public sealed class BackplaneTests
{
    private static readonly TimeSpan Deadline = TimeSpan.FromSeconds(10);

    [Fact]
    public void An_empty_channel_prefix_is_refused_when_the_backplane_is_registered()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentException>(() =>
            services.AddFusionCache().WithRuvioBackplane(options => options.ChannelPrefix = " "));
    }

    [Fact]
    public void An_empty_client_key_is_refused_when_the_backplane_is_registered()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentException>(() =>
            services.AddFusionCache().WithRuvioBackplane(options => options.ClientKey = ""));
    }

    [LiveFact]
    public async Task A_set_on_one_node_replaces_the_in_memory_copy_of_another()
    {
        await using Pair pair = await Pair.Open();
        var entry = new FusionCacheEntryOptions(TimeSpan.FromMinutes(5));

        await pair.A.SetAsync("k", "v1", entry);
        Assert.Equal("v1", await pair.B.GetOrDefaultAsync<string>("k"));   // B now holds "v1" in memory

        await pair.A.SetAsync("k", "v2", entry);

        await Eventually(async () => await pair.B.GetOrDefaultAsync<string>("k") == "v2");
    }

    [LiveFact]
    public async Task A_remove_on_one_node_drops_the_in_memory_copy_of_another()
    {
        await using Pair pair = await Pair.Open();
        var entry = new FusionCacheEntryOptions(TimeSpan.FromMinutes(5));

        await pair.A.SetAsync("k", 1, entry);
        Assert.Equal(1, await pair.B.GetOrDefaultAsync<int>("k"));

        await pair.A.RemoveAsync("k");

        await Eventually(async () => !(await pair.B.TryGetAsync<int>("k")).HasValue);
    }

    [LiveFact]
    public async Task A_message_that_is_not_from_fusioncache_is_ignored()
    {
        await using Pair pair = await Pair.Open();
        var entry = new FusionCacheEntryOptions(TimeSpan.FromMinutes(5));

        await pair.A.SetAsync("k", "v1", entry);
        Assert.Equal("v1", await pair.B.GetOrDefaultAsync<string>("k"));
        await pair.Client.PublishShardedAsync(pair.Channel, "not base64 !!");
        await pair.Client.PublishShardedAsync(pair.Channel, Convert.ToBase64String([1, 2, 3]));
        await pair.A.SetAsync("k", "v2", entry);

        await Eventually(async () => await pair.B.GetOrDefaultAsync<string>("k") == "v2");
    }

    private static async Task Eventually(Func<Task<bool>> condition)
    {
        DateTime end = DateTime.UtcNow + Deadline;

        while (!await condition())
        {
            Assert.True(DateTime.UtcNow < end, "The other node did not see the change in time.");
            await Task.Delay(50);
        }
    }

    private sealed class Pair : IAsyncDisposable
    {
        private readonly RuvioConnection _connection;

        private Pair(RuvioConnection connection, IRuvioClient client, string channel, IFusionCache a, IFusionCache b)
        {
            _connection = connection;
            Client = client;
            Channel = channel;
            A = a;
            B = b;
        }

        public IRuvioClient Client { get; }
        public string Channel { get; }
        public IFusionCache A { get; }
        public IFusionCache B { get; }

        public static async Task<Pair> Open()
        {
            string[] address = Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR")!.Split(':');
            RuvioConnection connection = await RuvioConnection.ConnectAsync(address[0], int.Parse(address[1], CultureInfo.InvariantCulture));
            IRuvioClient client = await connection.GetDefaultDatabaseAsync();
            string id = Guid.NewGuid().ToString("N");
            string l2Prefix = "fcbp-" + id + ":";
            string cacheName = "cache" + id;
            var backplaneOptions = new RuvioBackplaneOptions { ChannelPrefix = "test:" + id + ":" };

            // Each node has its own connection, as separate app nodes would.
            IFusionCache a = await Node(client, l2Prefix, cacheName, backplaneOptions);
            RuvioConnection connectionB = await RuvioConnection.ConnectAsync(address[0], int.Parse(address[1], CultureInfo.InvariantCulture));
            IFusionCache b = await Node(await connectionB.GetDefaultDatabaseAsync(), l2Prefix, cacheName, backplaneOptions);

            return new Pair(connection, client, backplaneOptions.ChannelPrefix + cacheName, a, b);
        }

        private static async Task<IFusionCache> Node(IRuvioClient client, string l2Prefix, string cacheName, RuvioBackplaneOptions backplaneOptions)
        {
            var cache = new ZiggyCreatures.Caching.Fusion.FusionCache(new FusionCacheOptions { CacheName = cacheName });
            var l2 = new RuvioDistributedCache(client, Options.Create(new RuvioCacheOptions { InstanceName = l2Prefix }));

            cache.SetupDistributedCache(l2, new FusionCacheSystemTextJsonSerializer());
            cache.SetupBackplane(new RuvioBackplane(client, backplaneOptions));
            await Task.Yield();

            return cache;
        }

        public ValueTask DisposeAsync() => _connection.DisposeAsync();
    }
}

public sealed class LiveFactAttribute : FactAttribute
{
    public LiveFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR")))
            Skip = "Set RUVIO_TEST_ADDR to run against an isolated live Ruvio server.";
    }
}
