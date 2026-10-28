using System.Diagnostics.Metrics;
using System.Reflection;
using System.Text;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.OutputCaching.Tests;

// Meter listeners see every store in the process, so these tests run alone.
[CollectionDefinition("Metrics", DisableParallelization = true)]
public sealed class MetricsCollection;

[Collection("Metrics")]
public class MetricsTests
{
    private static readonly string Hash = new('A', 64);
    private static readonly string Version = new('b', 32);

    private static RespValue Record(string count, string tags, string value = "payload") =>
        Resp.Bulk(Encoding.ASCII.GetBytes("ROC1" + count + tags + value));

    [Fact]
    public async Task LookupsCountHitsMissesWithReasonAndTagEvictions()
    {
        var seen = new List<string>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == "Ruvio.AspNetCore.OutputCaching"
                    && instrument.Name is "ruvio.output_cache.hit" or "ruvio.output_cache.miss" or "ruvio.output_cache.tag_evict")
                    l.EnableMeasurementEvents(instrument);
            },
        };

        listener.SetMeasurementEventCallback<long>((instrument, _, tags, _) =>
        {
            string reason = "";

            foreach (var tag in tags)
            {
                if (tag.Key == "reason")
                    reason = ":" + tag.Value;
            }

            lock (seen)
                seen.Add(instrument.Name + reason);
        });
        listener.Start();

        var client = DispatchProxy.Create<IRuvioClient, StoreEdgeTests.StoreClient>();
        var proxy = (StoreEdgeTests.StoreClient)(object)client;
        var store = new RuvioOutputCacheStore(client, Options.Create(new RuvioOutputCacheOptions()));
        var replies = new Queue<RespValue>([
            Record("00", ""),
            Resp.Null(),
            Record("01", Hash + Version),
            Resp.Int(1),
        ]);

        proxy.Binary = _ => replies.Dequeue();
        proxy.GetMany = _ => ["stale-generation-xxxxxxxxxxxxxxx"];

        Assert.NotNull(await store.GetAsync("a", default));
        Assert.Null(await store.GetAsync("b", default));
        Assert.Null(await store.GetAsync("c", default));
        await store.EvictByTagAsync("t", default);

        Assert.Equal([
            "ruvio.output_cache.hit",
            "ruvio.output_cache.miss:absent",
            "ruvio.output_cache.miss:tag_evicted",
            "ruvio.output_cache.tag_evict",
        ], seen);
    }
}
