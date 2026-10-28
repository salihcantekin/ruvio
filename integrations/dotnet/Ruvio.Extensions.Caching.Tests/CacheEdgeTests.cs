using System.Reflection;
using System.Text;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Extensions.Caching.Tests;

public class CacheEdgeTests
{
    public class Script : DispatchProxy
    {
        public Queue<RespValue> Replies { get; } = new();
        public List<string[]> Commands { get; } = [];
        public Func<string, RespValue>? Fallback { get; set; }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod!.Name != nameof(IRuvioClient.ExecuteBinaryAsync))
                throw new NotSupportedException(targetMethod.Name);

            var arguments = (IReadOnlyList<ReadOnlyMemory<byte>>)args![0]!;

            Commands.Add(arguments.Select(a => Encoding.Latin1.GetString(a.Span)).ToArray());

            return new ValueTask<RespValue>(Replies.Count > 0 ? Replies.Dequeue() : Fallback!(Commands[^1][0]));
        }
    }

    private static (RuvioDistributedCache Cache, Script Script) Make(Action<RuvioCacheOptions>? configure = null)
    {
        var client = DispatchProxy.Create<IRuvioClient, Script>();
        var options = new RuvioCacheOptions { InstanceName = "t" };

        configure?.Invoke(options);

        return (new RuvioDistributedCache(client, Options.Create(options)), (Script)(object)client);
    }

    private static RespValue Envelope(long absolute, long sliding, string payload)
        => Resp.Bulk($"RDC1\n{absolute}\n{sliding}\n{payload}");

    private static long Now => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    [Fact]
    public void OptionsRejectEveryOutOfRangeSetting()
    {
        Assert.Throws<ArgumentException>(() => Make(o => o.InstanceName = " "));
        Assert.Throws<ArgumentOutOfRangeException>(() => Make(o => o.MaxValueBytes = -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => Make(o => o.MaxValueBytes = 64 * 1024 * 1024));
        Assert.Throws<ArgumentOutOfRangeException>(() => Make(o => o.FillTimeout = TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => Make(o => o.FillTimeout = TimeSpan.FromHours(2)));
        Assert.Throws<ArgumentOutOfRangeException>(() => Make(o => o.FillWait = TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => Make(o => o.FillWait = TimeSpan.FromHours(2)));
        Assert.Throws<ArgumentOutOfRangeException>(() => Make(o => o.MaxKeyBytes = 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => Make(o => o.MaxKeyBytes = 5));
        Assert.Throws<ArgumentNullException>(() => new RuvioDistributedCache(null!, Options.Create(new RuvioCacheOptions())));
        Assert.Throws<ArgumentNullException>(() => new RuvioDistributedCache(DispatchProxy.Create<IRuvioClient, Script>(), null!));
    }

    [Fact(Timeout = 30000)]
    public async Task KeysAreValidatedBeforeAnyCommandIsSent()
    {
        var (cache, script) = Make(o => o.MaxKeyBytes = 40);

        await Assert.ThrowsAsync<ArgumentNullException>(() => cache.GetAsync(null!));
        await Assert.ThrowsAsync<ArgumentException>(() => cache.GetAsync(new string('k', 100)));
        await Assert.ThrowsAsync<ArgumentException>(() => cache.RemoveAsync(new string('k', 100)));
        await Assert.ThrowsAsync<OperationCanceledException>(() => cache.GetAsync("k", new CancellationToken(true)));
        await Assert.ThrowsAsync<OperationCanceledException>(() => cache.RemoveAsync("k", new CancellationToken(true)));
        await Assert.ThrowsAsync<OperationCanceledException>(() => cache.SetAsync("k", [], new(), new CancellationToken(true)));
        Assert.Empty(script.Commands);
    }

    [Fact]
    public void SyncWrappersDelegateToTheAsyncPaths()
    {
        var (cache, script) = Make();

        script.Replies.Enqueue(Envelope(0, 0, "abc"));
        Assert.Equal("abc"u8.ToArray(), cache.Get("k"));
        script.Replies.Enqueue(Resp.Ok);
        cache.Set("k", [1], new());
        script.Replies.Enqueue(Resp.Null());
        cache.Refresh("k");
        script.Replies.Enqueue(Resp.Int(1));
        cache.Remove("k");
        Assert.Equal(["GET", "SET", "GET", "DEL"], script.Commands.Select(c => c[0]));
    }

    [Fact(Timeout = 30000)]
    public async Task GetRejectsErrorWrongKindAndMalformedEnvelopes()
    {
        var (cache, script) = Make();

        script.Replies.Enqueue(Resp.Error("ERR boom"));
        await Assert.ThrowsAsync<RuvioException>(() => cache.GetAsync("k"));
        script.Replies.Enqueue(Resp.Int(1));
        await Assert.ThrowsAsync<RuvioException>(() => cache.GetAsync("k"));

        foreach (string bad in new[] { "XXXX\n0\n0\n", "RDC1\n\n0\n", "RDC1\nx\n0\n", "RDC1\n0\n", "RDC1\n0\n\n", "RDC1\n0\nz\n", "RDC1\n-1\n0\n" })
        {
            script.Replies.Enqueue(Resp.Bulk(bad));
            await Assert.ThrowsAsync<RuvioException>(() => cache.GetAsync("k"));
        }
    }

    [Fact(Timeout = 30000)]
    public async Task GetDeletesExpiredEntriesAndRenewsSlidingOnes()
    {
        var (cache, script) = Make();

        script.Replies.Enqueue(Envelope(Now - 1000, 0, "old"));
        script.Replies.Enqueue(Resp.Int(1));
        Assert.Null(await cache.GetAsync("k"));
        Assert.Equal("EVAL", script.Commands[1][0]);
        Assert.Equal("0", script.Commands[1][^1]);

        script.Commands.Clear();
        script.Replies.Enqueue(Envelope(0, 60000, "slide"));
        script.Replies.Enqueue(Resp.Int(1));
        Assert.Equal("slide"u8.ToArray(), await cache.GetAsync("k"));
        Assert.InRange(long.Parse(script.Commands[1][^1]), Now, Now + 60000);

        script.Commands.Clear();
        long absolute = Now + 5000;
        script.Replies.Enqueue(Envelope(absolute, 60000, "capped"));
        script.Replies.Enqueue(Resp.Int(1));
        await cache.GetAsync("k");
        Assert.Equal(absolute.ToString(), script.Commands[1][^1]);

        script.Replies.Enqueue(Envelope(0, 60000, "bad touch"));
        script.Replies.Enqueue(Resp.Bulk("x"));
        await Assert.ThrowsAsync<RuvioException>(() => cache.GetAsync("k"));

        script.Replies.Enqueue(Envelope(0, 60000, "bad touch"));
        script.Replies.Enqueue(Resp.Int(-1));
        await Assert.ThrowsAsync<RuvioException>(() => cache.GetAsync("k"));
    }

    [Fact(Timeout = 30000)]
    public async Task SetValidatesValuesAndExpirations()
    {
        var (cache, script) = Make(o => o.MaxValueBytes = 4);

        await Assert.ThrowsAsync<ArgumentNullException>(() => cache.SetAsync("k", null!, new()));
        await Assert.ThrowsAsync<ArgumentNullException>(() => cache.SetAsync("k", [], null!));
        await Assert.ThrowsAsync<ArgumentException>(() => cache.SetAsync("k", new byte[5], new()));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => cache.SetAsync("k", [], new() { AbsoluteExpiration = DateTimeOffset.UtcNow.AddSeconds(-1) }));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => cache.SetAsync("k", [], new() { SlidingExpiration = TimeSpan.Zero }));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => cache.SetAsync("k", [], new() { AbsoluteExpirationRelativeToNow = TimeSpan.FromTicks(1) }));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => cache.SetAsync("k", [], new() { AbsoluteExpirationRelativeToNow = TimeSpan.MaxValue }));
        Assert.Empty(script.Commands);

        script.Replies.Enqueue(Resp.Simple("QUEUED"));
        await Assert.ThrowsAsync<RuvioException>(() => cache.SetAsync("k", [], new()));
        script.Replies.Enqueue(Resp.Error("ERR no"));
        await Assert.ThrowsAsync<RuvioException>(() => cache.SetAsync("k", [], new()));

        script.Commands.Clear();
        script.Replies.Enqueue(Resp.Ok);
        await cache.SetAsync("k", [], new());
        Assert.Equal(3, script.Commands[0].Length);
    }

    [Fact(Timeout = 30000)]
    public async Task RemoveAcceptsOnlyZeroOrOne()
    {
        var (cache, script) = Make();

        script.Replies.Enqueue(Resp.Int(0));
        await cache.RemoveAsync("k");
        script.Replies.Enqueue(Resp.Int(2));
        await Assert.ThrowsAsync<RuvioException>(() => cache.RemoveAsync("k"));
        script.Replies.Enqueue(Resp.Ok);
        await Assert.ThrowsAsync<RuvioException>(() => cache.RemoveAsync("k"));
        script.Replies.Enqueue(Resp.Error("ERR"));
        await Assert.ThrowsAsync<RuvioException>(() => cache.RemoveAsync("k"));
    }

    private static RespValue Fill(string state, params RespValue[] rest)
        => Resp.Array([Resp.Bulk(state), .. rest]);

    [Fact(Timeout = 30000)]
    public async Task FillGetRepliesMustHaveAKnownShape()
    {
        var (cache, script) = Make();
        Func<CancellationToken, Task<byte[]>> factory = _ => Task.FromResult<byte[]>([9]);

        foreach (RespValue bad in new[] { Resp.Ok, Resp.Array(), Fill("NOPE", Resp.Null()), Fill("HIT"), Fill("BUSY", Resp.Null(), Resp.Null()) })
        {
            script.Replies.Enqueue(bad);
            await Assert.ThrowsAsync<RuvioException>(() => cache.GetOrCreateAsync("k", factory, new()));
        }

        await Assert.ThrowsAsync<ArgumentNullException>(() => cache.GetOrCreateAsync("k", null!, new()));
        await Assert.ThrowsAsync<ArgumentNullException>(() => cache.GetOrCreateAsync("k", factory, null!));
        await Assert.ThrowsAsync<ArgumentNullException>(() => cache.GetOrCreateAsync(null!, factory, new()));
        await Assert.ThrowsAsync<OperationCanceledException>(() => cache.GetOrCreateAsync("k", factory, new(), new CancellationToken(true)));
    }

    [Fact(Timeout = 30000)]
    public async Task FillHitReturnsTheStoredValueAndStaleOrExpiredHitsRebuild()
    {
        var (cache, script) = Make();
        int built = 0;
        Func<CancellationToken, Task<byte[]>> factory = _ => { built++; return Task.FromResult<byte[]>([7]); };

        script.Replies.Enqueue(Fill("HIT", Envelope(0, 0, "hit")));
        Assert.Equal("hit"u8.ToArray(), await cache.GetOrCreateAsync("k", factory, new()));
        Assert.Equal(0, built);

        // STALE without a token: another caller refreshes it, this one gets the stale value at once.
        script.Replies.Enqueue(Fill("STALE", Envelope(0, 0, "stale"), Resp.Null()));
        Assert.Equal("stale"u8.ToArray(), await cache.GetOrCreateAsync("k", factory, new()));
        Assert.Equal(0, built);

        // STALE with a token: this caller refreshes it and stores the new value with FILLSET under that token.
        script.Commands.Clear();
        script.Replies.Enqueue(Fill("STALE", Envelope(0, 0, "stale"), Resp.Int(5)));
        script.Replies.Enqueue(Resp.Int(1));
        script.Replies.Enqueue(Resp.Int(1));
        Assert.Equal(new byte[] { 7 }, await cache.GetOrCreateAsync("k", factory, new()));
        Assert.Equal(1, built);
        Assert.Equal(["FILLGET", "FILLSET", "PERSIST"], script.Commands.Select(c => c[0]));
        Assert.Equal("5", script.Commands[1][4]);
        built = 0;

        // An expired hit falls through; the state is not FILL, so the value is stored with a plain SET.
        script.Commands.Clear();
        script.Replies.Enqueue(Fill("HIT", Envelope(Now - 1000, 0, "gone")));
        script.Replies.Enqueue(Resp.Int(1));
        script.Replies.Enqueue(Resp.Ok);
        Assert.Equal(new byte[] { 7 }, await cache.GetOrCreateAsync("k", factory, new()));
        Assert.Equal(1, built);
        Assert.Equal(["FILLGET", "EVAL", "SET"], script.Commands.Select(c => c[0]));
    }

    [Fact(Timeout = 30000)]
    public async Task FillBuilderStoresWithATtlAndPersistsEntriesWithoutExpiry()
    {
        var (cache, script) = Make();
        Func<CancellationToken, Task<byte[]>> factory = _ => Task.FromResult<byte[]>([1]);

        script.Replies.Enqueue(Fill("FILL", Resp.Int(42)));
        script.Replies.Enqueue(Resp.Int(1));
        script.Replies.Enqueue(Resp.Int(1));
        await cache.GetOrCreateAsync("k", factory, new());
        Assert.Equal(["FILLGET", "FILLSET", "PERSIST"], script.Commands.Select(c => c[0]));
        Assert.Equal("3600000", script.Commands[1][3]);
        Assert.Equal("42", script.Commands[1][4]);

        script.Commands.Clear();
        script.Replies.Enqueue(Fill("FILL", Resp.Int(43)));
        script.Replies.Enqueue(Resp.Int(0));
        await cache.GetOrCreateAsync("k", factory, new() { SlidingExpiration = TimeSpan.FromSeconds(2) });
        Assert.Equal(["FILLGET", "FILLSET"], script.Commands.Select(c => c[0]));
        Assert.Equal("2000", script.Commands[1][3]);

        script.Commands.Clear();
        script.Replies.Enqueue(Fill("FILL", Resp.Int(44)));
        script.Replies.Enqueue(Resp.Int(1));
        await cache.GetOrCreateAsync("k", factory, new() { AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(30) });
        Assert.InRange(long.Parse(script.Commands[1][3]), 1, 30000);
        Assert.Equal(2, script.Commands.Count);

        script.Replies.Enqueue(Fill("FILL", Resp.Int(45)));
        script.Replies.Enqueue(Resp.Bulk("x"));
        await Assert.ThrowsAsync<RuvioException>(() => cache.GetOrCreateAsync("k", factory, new()));
    }

    [Fact(Timeout = 30000)]
    public async Task FillWaitersPollBusyMarkersThenTakeTheResultOrGiveUp()
    {
        var (cache, script) = Make(o => o.FillWait = TimeSpan.FromMilliseconds(30));
        Func<CancellationToken, Task<byte[]>> factory = _ => Task.FromResult<byte[]>([3]);

        script.Replies.Enqueue(Fill("BUSY", Resp.Null()));
        script.Replies.Enqueue(Fill("HIT", Envelope(0, 0, "done")));
        Assert.Equal("done"u8.ToArray(), await cache.GetOrCreateAsync("k", factory, new()));
        Assert.Equal(2, script.Commands.Count);

        // A marker that never clears: the caller stops waiting and builds the value itself with a plain SET.
        script.Commands.Clear();
        script.Fallback = name => name == "FILLGET" ? Fill("BUSY", Resp.Null()) : Resp.Ok;
        Assert.Equal(new byte[] { 3 }, await cache.GetOrCreateAsync("k", factory, new()));
        Assert.True(script.Commands.Count(c => c[0] == "FILLGET") > 1);
        Assert.Equal("SET", script.Commands[^1][0]);
    }

    [Fact(Timeout = 30000)]
    public async Task ExtensionFallsBackToGetThenFactoryThenSetForOtherCaches()
    {
        var fake = new MemoryCache();
        int built = 0;
        Func<CancellationToken, Task<byte[]>> factory = _ => { built++; return Task.FromResult<byte[]>([5]); };

        Assert.Equal(new byte[] { 5 }, await fake.GetOrCreateAsync("k", factory, new()));
        Assert.Equal(new byte[] { 5 }, await fake.GetOrCreateAsync("k", factory, new()));
        Assert.Equal(1, built);

        await Assert.ThrowsAsync<ArgumentNullException>(() => ((IDistributedCache)null!).GetOrCreateAsync("k", factory, new()));
        await Assert.ThrowsAsync<ArgumentNullException>(() => fake.GetOrCreateAsync("k", null!, new()));
        await Assert.ThrowsAsync<ArgumentNullException>(() => fake.GetOrCreateAsync("k", factory, null!));

        var (cache, script) = Make();
        script.Replies.Enqueue(Fill("HIT", Envelope(0, 0, "r")));
        Assert.Equal("r"u8.ToArray(), await ((IDistributedCache)cache).GetOrCreateAsync("k", factory, new()));
    }

    private sealed class MemoryCache : IDistributedCache
    {
        private readonly Dictionary<string, byte[]> _items = [];

        public byte[]? Get(string key) => _items.GetValueOrDefault(key);

        public Task<byte[]?> GetAsync(string key, CancellationToken token = default) => Task.FromResult(Get(key));

        public void Set(string key, byte[] value, DistributedCacheEntryOptions options) => _items[key] = value;

        public Task SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token = default)
        {
            Set(key, value, options);

            return Task.CompletedTask;
        }

        public void Refresh(string key) { }

        public Task RefreshAsync(string key, CancellationToken token = default) => Task.CompletedTask;

        public void Remove(string key) => _items.Remove(key);

        public Task RemoveAsync(string key, CancellationToken token = default) => Task.CompletedTask;
    }
}
