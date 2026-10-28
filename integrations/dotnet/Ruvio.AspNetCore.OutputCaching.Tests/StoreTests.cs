using System.Globalization;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.AspNetCore.OutputCaching;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.OutputCaching.Tests;

public class StoreTests
{
    [Fact]
    public void RegistrationReplacesTheFrameworkStoreWithoutResolvingAClient()
    {
        var services = new ServiceCollection();

        services.AddSingleton<IRuvioClient>(_ => throw new InvalidOperationException("Should stay lazy."));
        services.AddOutputCache();
        services.AddRuvioOutputCache();
        using var provider = services.BuildServiceProvider();

        Assert.Single(services, s => s.ServiceType == typeof(IOutputCacheStore));
        Assert.IsType<RuvioOutputCacheStore>(provider.GetRequiredService<IOutputCacheStore>());
        Assert.Throws<ArgumentException>(() => new ServiceCollection().AddRuvioOutputCache(o => o.InstanceName = ""));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ServiceCollection().AddRuvioOutputCache(o => o.MaxTagsPerEntry = 33));
    }

    [Fact]
    public async Task InvalidInputsAndCancellationDoNotResolveTheClient()
    {
        var cache = new RuvioOutputCacheStore(() => throw new InvalidOperationException("Unexpected IO."),
            Options.Create(new RuvioOutputCacheOptions { MaxValueBytes = 1, MaxKeyBytes = 3, MaxTagsPerEntry = 1 }));

        await Assert.ThrowsAsync<ArgumentNullException>(() => cache.GetAsync(null!, default).AsTask());
        // A key over MaxKeyBytes is a miss and is not stored; neither touches the client.
        Assert.Null(await cache.GetAsync("long", default));
        await cache.SetAsync("long", [], null, TimeSpan.FromSeconds(1), default);
        await Assert.ThrowsAsync<System.Text.EncoderFallbackException>(() => cache.GetAsync("\ud800", default).AsTask());
        await Assert.ThrowsAsync<ArgumentNullException>(() => cache.SetAsync("", null!, null, TimeSpan.FromSeconds(1), default).AsTask());
        await Assert.ThrowsAsync<ArgumentException>(() => cache.SetAsync("", [1, 2], [], TimeSpan.FromSeconds(1), default).AsTask());
        await Assert.ThrowsAsync<ArgumentException>(() => cache.SetAsync("", [], ["a", "b"], TimeSpan.FromSeconds(1), default).AsTask());
        await Assert.ThrowsAsync<ArgumentException>(() => cache.SetAsync("", [], ["long"], TimeSpan.FromSeconds(1), default).AsTask());
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => cache.SetAsync("", [], null, TimeSpan.Zero, default).AsTask());
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => cache.SetAsync("", [], null, TimeSpan.MaxValue, default).AsTask());
        using var source = new CancellationTokenSource();

        source.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => cache.GetAsync("", source.Token).AsTask());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => cache.SetAsync("", [], [], TimeSpan.FromSeconds(1), source.Token).AsTask());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => cache.EvictByTagAsync("", source.Token).AsTask());
    }

    [Fact]
    public async Task AFiveKilobyteQueryStringIsAMissAndIsNotStored()
    {
        var cache = new RuvioOutputCacheStore(() => throw new InvalidOperationException("Unexpected IO."),
            Options.Create(new RuvioOutputCacheOptions { Outage = { Mode = RuvioOutageMode.FailOpen } }));
        string key = "GET\u001Fhost\u001F/items?q=" + new string('x', 5 * 1024);

        Assert.Null(await cache.GetAsync(key, default));
        await cache.SetAsync(key, [1], null, TimeSpan.FromSeconds(1), default);
    }

    [Fact]
    public async Task TransportErrorsAreNotRetriedAndTokensAreForwarded()
    {
        var client = DispatchProxy.Create<IRuvioClient, RejectingClient>();
        var cache = new RuvioOutputCacheStore(client, Options.Create(new RuvioOutputCacheOptions()));
        using var source = new CancellationTokenSource();

        await Assert.ThrowsAsync<IOException>(() => cache.SetAsync("k", [], ["t"], TimeSpan.FromSeconds(1), source.Token).AsTask());
        Assert.Equal(1, ((RejectingClient)client).Calls);
        Assert.Equal(source.Token, ((RejectingClient)client).Token);
    }

    [LiveFact]
    public async Task Binary_null_tags_empty_values_and_namespaces_round_trip()
    {
        await using var scope = await LiveScope.Open();
        byte[] bytes = Enumerable.Range(0, 256).Select(i => (byte)i).ToArray();

        await scope.Store.SetAsync("", bytes, null, TimeSpan.FromSeconds(5), default);
        Assert.Equal(bytes, await scope.Store.GetAsync("", default));
        await scope.Store.SetAsync("empty", [], [], TimeSpan.FromSeconds(5), default);
        Assert.Empty((await scope.Store.GetAsync("empty", default))!);
        Assert.Null(await scope.Store.GetAsync("missing", default));
        var other = new RuvioOutputCacheStore(scope.Client,
            Options.Create(new RuvioOutputCacheOptions { InstanceName = scope.Name + "-other" }));

        Assert.Null(await other.GetAsync("", default));
    }

    [LiveFact]
    public async Task Tag_eviction_is_shared_and_does_not_resurrect_after_generation_recreation()
    {
        await using var scope = await LiveScope.Open();
        await using var secondConnection = await LiveScope.Connect();
        RuvioClient second = await secondConnection.GetDefaultDatabaseAsync();
        var other = new RuvioOutputCacheStore(second, Options.Create(new RuvioOutputCacheOptions { InstanceName = scope.Name }));

        await scope.Store.SetAsync("old", [1], ["shared", "other"], TimeSpan.FromSeconds(10), default);
        await other.SetAsync("unrelated", [2], ["other"], TimeSpan.FromSeconds(10), default);
        Assert.Equal(new byte[] { 1 }, await other.GetAsync("old", default));

        await other.EvictByTagAsync("shared", default);
        Assert.Null(await scope.Store.GetAsync("old", default));
        Assert.Equal(new byte[] { 2 }, await scope.Store.GetAsync("unrelated", default));

        await other.SetAsync("new", [3], ["shared", "shared"], TimeSpan.FromSeconds(10), default);
        Assert.Null(await scope.Store.GetAsync("old", default));
        Assert.Equal(new byte[] { 3 }, await scope.Store.GetAsync("new", default));

        await other.EvictByTagAsync("absent", default);
        Assert.Equal(-2, (await second.ExecuteAsync("PTTL", other.TagKey("absent"))).Integer);
    }

    [LiveFact]
    public async Task Replacement_changes_tag_membership_without_stale_invalidation()
    {
        await using var scope = await LiveScope.Open();
        await scope.Store.SetAsync("replace", [1], ["old"], TimeSpan.FromSeconds(5), default);
        await scope.Store.SetAsync("replace", [2], ["new"], TimeSpan.FromSeconds(5), default);
        await scope.Store.EvictByTagAsync("old", default);
        Assert.Equal(new byte[] { 2 }, await scope.Store.GetAsync("replace", default));

        await scope.Store.SetAsync("replace", [3], null, TimeSpan.FromSeconds(5), default);
        await scope.Store.EvictByTagAsync("new", default);
        Assert.Equal(new byte[] { 3 }, await scope.Store.GetAsync("replace", default));
    }

    [LiveFact]
    public async Task Entry_and_tag_metadata_expire_and_shorter_entries_do_not_shorten_shared_tags()
    {
        await using var scope = await LiveScope.Open();
        await scope.Store.SetAsync("long", [1], ["t"], TimeSpan.FromSeconds(30), default);
        await scope.Store.SetAsync("short", [2], ["t"], TimeSpan.FromMilliseconds(100), default);
        Assert.InRange((await scope.Client.ExecuteAsync("PTTL", scope.Store.TagKey("t"))).Integer, 20_000, 30_000);
        await Task.Delay(180);
        Assert.Null(await scope.Store.GetAsync("short", default));
        Assert.Equal(new byte[] { 1 }, await scope.Store.GetAsync("long", default));

        await scope.Store.SetAsync("expire", [], ["expire"], TimeSpan.FromMilliseconds(100), default);
        await Task.Delay(180);
        Assert.Null(await scope.Store.GetAsync("expire", default));
        Assert.Equal(-2, (await scope.Client.ExecuteAsync("PTTL", scope.Store.TagKey("expire"))).Integer);
    }

    [LiveFact]
    public async Task All_supported_tags_fit_script_limits_and_eviction_invalidates_each_record()
    {
        await using var scope = await LiveScope.Open();
        string[] tags = Enumerable.Range(0, 32).Select(i => "tag" + i).ToArray();
        byte[] payload = new byte[1024 * 1024];

        Array.Fill(payload, (byte)255);
        await scope.Store.SetAsync("many", payload, tags, TimeSpan.FromSeconds(5), default);
        Assert.Equal(payload, await scope.Store.GetAsync("many", default));
        await scope.Store.EvictByTagAsync(tags[^1], default);
        Assert.Null(await scope.Store.GetAsync("many", default));
    }

    [LiveFact]
    public async Task Concurrent_publish_and_evict_leaves_no_old_generation_visible()
    {
        await using var scope = await LiveScope.Open();
        await using var secondConnection = await LiveScope.Connect();
        RuvioClient second = await secondConnection.GetDefaultDatabaseAsync();
        var other = new RuvioOutputCacheStore(second, Options.Create(new RuvioOutputCacheOptions { InstanceName = scope.Name }));

        for (int i = 0; i < 30; i++)
        {
            await scope.Store.SetAsync("before", [1], ["race"], TimeSpan.FromSeconds(5), default);
            await Task.WhenAll(
                other.SetAsync("racing", [2], ["race"], TimeSpan.FromSeconds(5), default).AsTask(),
                scope.Store.EvictByTagAsync("race", default).AsTask());
            Assert.Null(await other.GetAsync("before", default));
            await other.SetAsync("after", [3], ["race"], TimeSpan.FromSeconds(5), default);
            Assert.Equal(new byte[] { 3 }, await scope.Store.GetAsync("after", default));

            Assert.Null(await scope.Store.GetAsync("before", default));
        }
    }

    [LiveFact]
    public async Task Corrupt_records_wrong_types_and_corrupt_tags_fail_loudly()
    {
        await using var scope = await LiveScope.Open();
        await scope.Client.ExecuteAsync("SET", scope.Store.EntryKey("bad"), "ROC132short");
        await Assert.ThrowsAsync<RuvioException>(() => scope.Store.GetAsync("bad", default).AsTask());
        await scope.Client.ExecuteAsync("HSET", scope.Store.EntryKey("hash"), "f", "v");
        await Assert.ThrowsAsync<RuvioException>(() => scope.Store.GetAsync("hash", default).AsTask());
        await scope.Client.ExecuteAsync("SET", scope.Store.TagKey("bad"), "not-a-generation");
        await Assert.ThrowsAsync<RuvioException>(() =>
            scope.Store.SetAsync("untouched", [1], ["good", "bad"], TimeSpan.FromSeconds(5), default).AsTask());
        Assert.Null(await scope.Store.GetAsync("untouched", default));
        Assert.Equal(-2, (await scope.Client.ExecuteAsync("PTTL", scope.Store.TagKey("good"))).Integer);
    }

    public class RejectingClient : DispatchProxy
    {
        public int Calls { get; private set; }
        public CancellationToken Token { get; private set; }
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            Calls++;
            Token = (CancellationToken)args![1]!;
            var error = new IOException("Ambiguous write.");

            if (targetMethod!.ReturnType == typeof(ValueTask<IReadOnlyList<RespValue>>))
                return ValueTask.FromException<IReadOnlyList<RespValue>>(error);

            return ValueTask.FromException<RespValue>(error);
        }
    }
}

internal sealed class LiveScope : IAsyncDisposable
{
    public string Name { get; } = "output-test-" + Guid.NewGuid().ToString("N");

    public RuvioClient Client { get; }
    public RuvioConnection Connection { get; }
    public RuvioOutputCacheStore Store { get; }
    private LiveScope(RuvioConnection connection, RuvioClient client)
    {
        Connection = connection;
        Client = client;
        Store = new(client, Options.Create(new RuvioOutputCacheOptions { InstanceName = Name }));
    }

    public static async Task<LiveScope> Open()
    {
        RuvioConnection connection = await Connect();

        return new LiveScope(connection, await connection.GetDefaultDatabaseAsync());
    }

    public static Task<RuvioConnection> Connect()
    {
        var address = Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR")!.Split(':');

        return RuvioConnection.ConnectAsync(address[0], int.Parse(address[1], CultureInfo.InvariantCulture));
    }

    public ValueTask DisposeAsync() => Connection.DisposeAsync();
}

public sealed class LiveFactAttribute : FactAttribute
{
    public LiveFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR")))
            Skip = "Set RUVIO_TEST_ADDR to run against an isolated Ruvio server.";

        Timeout = 30000;
    }
}
