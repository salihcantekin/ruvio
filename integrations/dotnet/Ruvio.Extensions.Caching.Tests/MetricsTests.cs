using System.Diagnostics.Metrics;
using System.Reflection;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Extensions.Caching.Tests;

// Meter listeners see every cache in the process, so these tests run alone.
[CollectionDefinition("Metrics", DisableParallelization = true)]
public sealed class MetricsCollection;

[Collection("Metrics")]
public class MetricsTests
{
    private static (RuvioDistributedCache Cache, CacheEdgeTests.Script Script) Make()
    {
        var client = DispatchProxy.Create<IRuvioClient, CacheEdgeTests.Script>();

        return (new RuvioDistributedCache(client, Options.Create(new RuvioCacheOptions { InstanceName = "m" })),
            (CacheEdgeTests.Script)(object)client);
    }

    private static MeterListener Listen(List<(string Name, string Operation)> seen)
    {
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == "Ruvio.Extensions.Caching" && instrument.Name is "ruvio.cache.hit" or "ruvio.cache.miss")
                    l.EnableMeasurementEvents(instrument);
            },
        };

        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            string operation = "";

            foreach (var tag in tags)
            {
                if (tag.Key == "operation")
                    operation = (string)tag.Value!;
            }

            lock (seen)
                seen.Add((instrument.Name, operation));
        });
        listener.Start();

        return listener;
    }

    [Fact(Timeout = 30000)]
    public async Task GetCountsHitsAndMissesIncludingExpiredEntries()
    {
        var seen = new List<(string, string)>();
        using var listener = Listen(seen);
        var (cache, script) = Make();
        long past = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 1000;

        script.Replies.Enqueue(Resp.Bulk("RDC1\n0\n0\nvalue"));
        script.Replies.Enqueue(Resp.Null());
        script.Replies.Enqueue(Resp.Bulk($"RDC1\n{past}\n0\nold"));
        script.Fallback = _ => Resp.Int(1); // the DEL of the expired entry

        Assert.NotNull(await cache.GetAsync("a"));
        Assert.Null(await cache.GetAsync("b"));
        Assert.Null(await cache.GetStringAsync("c"));

        Assert.Equal([("ruvio.cache.hit", "get"), ("ruvio.cache.miss", "get"), ("ruvio.cache.miss", "get")], seen);
    }

    [Fact(Timeout = 30000)]
    public async Task GetOrCreateCountsAHitOrOneMissPerCall()
    {
        var seen = new List<(string, string)>();
        using var listener = Listen(seen);
        var (cache, script) = Make();

        script.Replies.Enqueue(Resp.Array(Resp.Bulk("HIT"), Resp.Bulk("RDC1\n0\n0\ncached")));
        script.Replies.Enqueue(Resp.Array(Resp.Bulk("FILL"), Resp.Int(7)));
        script.Replies.Enqueue(Resp.Int(1));

        Assert.Equal("cached", await cache.GetOrCreateStringAsync("a", _ => Task.FromResult("built"), new DistributedCacheEntryOptions()));
        Assert.Equal("built", await cache.GetOrCreateStringAsync("b", _ => Task.FromResult("built"),
            new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(1) }));

        Assert.Equal([("ruvio.cache.hit", "get_or_create"), ("ruvio.cache.miss", "get_or_create")], seen);
    }
}
