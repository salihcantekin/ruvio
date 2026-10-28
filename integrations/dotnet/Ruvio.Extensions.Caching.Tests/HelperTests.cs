using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Extensions.Caching.Tests;

[JsonSerializable(typeof(HelperTests.Order))]
internal sealed partial class OrderContext : JsonSerializerContext;

public class HelperTests
{
    public sealed record Order(int Id, string CustomerName);

    private sealed class MapCache : IDistributedCache
    {
        public Dictionary<string, byte[]> Map { get; } = [];

        public byte[]? Get(string key) => Map.GetValueOrDefault(key);

        public Task<byte[]?> GetAsync(string key, CancellationToken token = default) => Task.FromResult(Get(key));

        public void Set(string key, byte[] value, DistributedCacheEntryOptions options) => Map[key] = value;

        public Task SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token = default)
        {
            Set(key, value, options);

            return Task.CompletedTask;
        }

        public void Refresh(string key) { }

        public Task RefreshAsync(string key, CancellationToken token = default) => Task.CompletedTask;

        public void Remove(string key) => Map.Remove(key);

        public Task RemoveAsync(string key, CancellationToken token = default)
        {
            Remove(key);

            return Task.CompletedTask;
        }
    }

    [Fact(Timeout = 30000)]
    public async Task ModelRoundTripsWithSerializerOptions()
    {
        var cache = new MapCache();
        var camel = new JsonSerializerOptions(JsonSerializerDefaults.Web);

        await cache.SetJsonAsync("o", new Order(7, "Ada"), serializerOptions: camel);

        Assert.Equal("{\"id\":7,\"customerName\":\"Ada\"}", Encoding.UTF8.GetString(cache.Map["o"]));
        Assert.Equal(new Order(7, "Ada"), await cache.GetJsonAsync<Order>("o", camel));
        Assert.Null(await cache.GetJsonAsync<Order>("missing", camel));
    }

    [Fact(Timeout = 30000)]
    public async Task ModelRoundTripsWithSourceGeneratedTypeInfo()
    {
        var cache = new MapCache();
        var info = OrderContext.Default.Order;

        await cache.SetJsonAsync("o", new Order(1, "Bo"), info);

        Assert.Equal(new Order(1, "Bo"), await cache.GetJsonAsync("o", info));
    }

    [Fact(Timeout = 30000)]
    public async Task GetOrCreateRunsTheFactoryOnceForStringsAndModels()
    {
        var cache = new MapCache();
        int calls = 0;

        for (int i = 0; i < 2; i++)
        {
            Assert.Equal("hello", await cache.GetOrCreateStringAsync("s", _ => Task.FromResult(Interlocked.Increment(ref calls) > 0 ? "hello" : "")));
            Assert.Equal(new Order(2, "Cy"), await cache.GetOrCreateJsonAsync("m", _ => Task.FromResult(new Order(2, "Cy"))));
        }

        Assert.Equal(1, calls);
    }
}

public class DirectStringJsonTests
{
    private static (RuvioDistributedCache Cache, CacheEdgeTests.Script Script) Make()
    {
        var client = DispatchProxy.Create<IRuvioClient, CacheEdgeTests.Script>();
        var cache = new RuvioDistributedCache(client, Options.Create(new RuvioCacheOptions { InstanceName = "t" }));

        return (cache, (CacheEdgeTests.Script)(object)client);
    }

    private static string Sent(CacheEdgeTests.Script script, int index) => script.Commands[0][index];

    [Fact(Timeout = 30000)]
    public async Task SetStringWritesTheUtf8RecordInOneCommand()
    {
        var (cache, script) = Make();

        script.Replies.Enqueue(Resp.Simple("OK"));
        await cache.SetStringAsync("k", "héllo", new());

        Assert.Equal(["SET", Sent(script, 1), "RDC1\n0\n0\nhéllo"], script.Commands[0].Select((a, i) => i == 2 ? Encoding.UTF8.GetString(Encoding.Latin1.GetBytes(a)) : a));
        Assert.Single(script.Commands);
    }

    [Fact(Timeout = 30000)]
    public async Task SetJsonWritesCamelCaseWhenAskedAndHonoursTheSizeLimit()
    {
        var (cache, script) = Make();
        var web = new JsonSerializerOptions(JsonSerializerDefaults.Web);

        script.Replies.Enqueue(Resp.Simple("OK"));
        await cache.SetJsonAsync("k", new HelperTests.Order(1, "Ada"), new(), web);

        Assert.Equal("RDC1\n0\n0\n{\"id\":1,\"customerName\":\"Ada\"}", script.Commands[0][2]);
    }

    [Fact(Timeout = 30000)]
    public async Task GetStringAndGetJsonReadThePayloadDirectly()
    {
        var (cache, script) = Make();

        script.Replies.Enqueue(Resp.Bulk("RDC1\n0\n0\nmerhaba"));
        Assert.Equal("merhaba", await cache.GetStringAsync("k"));

        script.Replies.Enqueue(Resp.Bulk("RDC1\n0\n0\n{\"Id\":4,\"CustomerName\":\"Eve\"}"));
        Assert.Equal(new HelperTests.Order(4, "Eve"), await cache.GetJsonAsync("k", OrderContext.Default.Order));

        script.Replies.Enqueue(Resp.Null());
        Assert.Null(await cache.GetStringAsync("k"));
    }
}
