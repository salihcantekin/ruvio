using System.Reflection;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Extensions.Caching.Tests;

/// <summary>The stale period and the stored format, against a scripted client.</summary>
public class SwrEnvelopeTests
{
    private static (RuvioDistributedCache Cache, CacheEdgeTests.Script Script) Make(Action<RuvioCacheOptions>? configure = null)
    {
        var client = DispatchProxy.Create<IRuvioClient, CacheEdgeTests.Script>();
        var options = new RuvioCacheOptions { InstanceName = "t" };

        configure?.Invoke(options);

        return (new RuvioDistributedCache(client, Options.Create(options)), (CacheEdgeTests.Script)(object)client);
    }

    private static long Now => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private static RespValue Stale(string payload, long freshAgo = 1000)
        => Resp.Bulk($"RDC2\n{Now + 60_000}\n0\n{Now - freshAgo}\n0\n{payload}");

    private static RespValue Fill(string state, params RespValue[] rest) => Resp.Array([Resp.Bulk(state), .. rest]);

    private static RuvioCacheEntryOptions Swr => new()
    {
        AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(10),
        StaleFor = TimeSpan.FromSeconds(50),
    };

    [Fact(Timeout = 30000)]
    public async Task EntriesWithoutExtrasKeepTheOldFormat()
    {
        var (cache, script) = Make();

        script.Fallback = _ => Resp.Ok;
        await cache.SetStringAsync("k", "v", new RuvioCacheEntryOptions { AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(5) });
        await cache.SetStringAsync("k", "v", new DistributedCacheEntryOptions());

        Assert.StartsWith("RDC1\n", script.Commands[0][2]);
        Assert.Equal("RDC1\n0\n0\nv", script.Commands[1][2]);
    }

    [Fact(Timeout = 30000)]
    public async Task AStalePeriodIsStoredAfterTheFreshDeadline()
    {
        var (cache, script) = Make();

        script.Fallback = _ => Resp.Ok;
        long before = Now;
        await cache.SetStringAsync("k", "v", Swr);

        string[] header = script.Commands[0][2].Split('\n');

        Assert.Equal("RDC2", header[0]);
        Assert.Equal(long.Parse(header[3]) + 50_000, long.Parse(header[1]));
        Assert.InRange(long.Parse(header[3]), before + 10_000, Now + 10_000);
        Assert.Equal("0", header[2]);
        Assert.Equal("0", header[4]);
        Assert.Equal("v", header[5]);
        Assert.Equal("PXAT", script.Commands[0][3]);
        Assert.Equal(header[1], script.Commands[0][4]);
    }

    [Fact(Timeout = 30000)]
    public async Task StalePeriodOptionsAreValidatedBeforeAnythingIsSent()
    {
        var (cache, script) = Make();

        await Assert.ThrowsAsync<ArgumentException>(() => cache.SetAsync("k", [1], new RuvioCacheEntryOptions { StaleFor = TimeSpan.FromSeconds(1) }));
        await Assert.ThrowsAsync<ArgumentException>(() => cache.SetAsync("k", [1], new RuvioCacheEntryOptions
        {
            SlidingExpiration = TimeSpan.FromSeconds(1),
            AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(5),
            StaleFor = TimeSpan.FromSeconds(1),
        }));
        await Assert.ThrowsAsync<ArgumentException>(() => cache.SetAsync("k", [1], new RuvioCacheEntryOptions { Tags = ["a", ""] }));
        await Assert.ThrowsAsync<ArgumentException>(() => cache.SetAsync("k", [1],
            new RuvioCacheEntryOptions { Tags = Enumerable.Range(0, 9).Select(i => i.ToString()).ToArray() }));
        Assert.Empty(script.Commands);
        Assert.Throws<ArgumentOutOfRangeException>(() => Make(o => o.MaxTagsPerEntry = 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => Make(o => o.MaxTagsPerEntry = 65));
        await Assert.ThrowsAsync<NotSupportedException>(() =>
            DispatchProxy.Create<IDistributedCache, OtherCache>().RemoveByTagAsync("t"));
        await Assert.ThrowsAsync<ArgumentException>(() => cache.RemoveByTagAsync(""));
    }

    [Fact(Timeout = 30000)]
    public async Task APlainGetMissesAStaleEntryWithoutDeletingIt()
    {
        var (cache, script) = Make();

        script.Replies.Enqueue(Stale("old"));
        Assert.Null(await cache.GetStringAsync("k"));
        Assert.Equal(["GET"], script.Commands.Select(c => c[0]));
    }

    [Fact(Timeout = 30000)]
    public async Task AStaleRefresherStoresWithItsTokenAndTheSwrWindow()
    {
        var (cache, script) = Make();

        script.Replies.Enqueue(Fill("STALE", Stale("old"), Resp.Int(9)));
        script.Replies.Enqueue(Resp.Int(1));
        Assert.Equal("new", await cache.GetOrCreateStringAsync("k", _ => Task.FromResult("new"), Swr));

        string[] fillSet = script.Commands[1];

        Assert.Equal("FILLSET", fillSet[0]);
        Assert.InRange(long.Parse(fillSet[3]), 9_000, 10_000);
        Assert.Equal(["9", "SWR", "50000"], fillSet[4..]);
        Assert.StartsWith("RDC2\n", fillSet[2]);
    }

    [Fact(Timeout = 30000)]
    public async Task AFailingRefreshGetsTheStaleValue()
    {
        var (cache, script) = Make();

        script.Replies.Enqueue(Fill("STALE", Stale("old"), Resp.Int(9)));
        Assert.Equal("old", await cache.GetOrCreateStringAsync("k", _ => throw new TimeoutException("source down"), Swr));
        Assert.Single(script.Commands);

        // A cancelled caller still sees its cancellation, not the stale value.
        using var cancelled = new CancellationTokenSource();

        script.Replies.Enqueue(Fill("STALE", Stale("old"), Resp.Int(10)));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => cache.GetOrCreateStringAsync("k", token =>
        {
            cancelled.Cancel();
            token.ThrowIfCancellationRequested();

            return Task.FromResult("never");
        }, Swr, cancelled.Token));
    }

    [Fact(Timeout = 30000)]
    public async Task WhenTheServerForgotTheSoftExpiryTheEnvelopeDecides()
    {
        var (cache, script) = Make();

        // The server answers HIT (its mark is gone) but the envelope is past its fresh deadline: this caller
        // refreshes with a plain SET, since there is no token to fence a FILLSET.
        script.Replies.Enqueue(Fill("HIT", Stale("old")));
        script.Replies.Enqueue(Resp.Ok);
        Assert.Equal("new", await cache.GetOrCreateStringAsync("k", _ => Task.FromResult("new"), Swr));
        Assert.Equal(["FILLGET", "SET"], script.Commands.Select(c => c[0]));

        script.Replies.Enqueue(Fill("HIT", Stale("old")));
        Assert.Equal("old", await cache.GetOrCreateStringAsync("k", _ => throw new InvalidOperationException("down"), Swr));
    }

    public class OtherCache : DispatchProxy
    {
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) =>
            throw new NotSupportedException(targetMethod!.Name);
    }
}
