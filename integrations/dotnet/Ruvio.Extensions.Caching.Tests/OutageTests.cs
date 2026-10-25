using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Ruvio.Integrations.Tests;
using Xunit;

namespace Ruvio.Extensions.Caching.Tests;

/// <summary>The cache against a live server that a proxy can take away.</summary>
public class OutageTests
{
    private static readonly DistributedCacheEntryOptions Minutes = new() { AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5) };

    private sealed class Scope : IAsyncDisposable
    {
        public required OutageProxy Proxy { get; init; }
        public required RuvioConnection Connection { get; init; }
        public required IRuvioClient Client { get; init; }
        public required RuvioDistributedCache Cache { get; init; }
        public required ManualTime Time { get; init; }
        public required string Instance { get; init; }

        public async ValueTask DisposeAsync()
        {
            await Connection.DisposeAsync();
            Proxy.Dispose();
        }
    }

    private static async Task<Scope> OpenAsync(RuvioOutageMode mode, int? breakerThreshold = null)
    {
        var proxy = OutageProxy.ForLiveServer();
        var (connection, client) = await OutageClient.ConnectAsync(proxy);
        var time = new ManualTime();
        string instance = "outage-" + Guid.NewGuid().ToString("N");
        var options = new RuvioCacheOptions { InstanceName = instance };

        options.Outage.Mode = mode;

        if (breakerThreshold is int threshold)
        {
            options.Outage.CircuitBreaker = new RuvioCircuitBreakerOptions
            {
                FailureThreshold = threshold,
                BreakDuration = TimeSpan.FromSeconds(30),
                TimeProvider = time,
            };
        }

        return new Scope
        {
            Proxy = proxy,
            Connection = connection,
            Client = client,
            Cache = new RuvioDistributedCache(client, Options.Create(options)),
            Time = time,
            Instance = instance,
        };
    }

    [OutageLiveFact]
    public async Task TheDefaultStillThrowsOnEveryOperationWhenRuvioIsAway()
    {
        await using var scope = await OpenAsync(RuvioOutageMode.Throw);

        await scope.Cache.SetStringAsync("k", "v", Minutes);
        scope.Proxy.Cut();

        await Assert.ThrowsAnyAsync<Exception>(() => scope.Cache.GetStringAsync("k"));
        await Assert.ThrowsAnyAsync<Exception>(() => scope.Cache.SetStringAsync("k", "w", Minutes));
        await Assert.ThrowsAnyAsync<Exception>(() => scope.Cache.RemoveAsync("k"));
        await Assert.ThrowsAnyAsync<Exception>(() => scope.Cache.GetOrCreateStringAsync("k", _ => Task.FromResult("x"), Minutes));
    }

    [OutageLiveFact]
    public async Task FailClosedBehavesAsThrowBecauseACacheHasNoResponseToRefuse()
    {
        await using var scope = await OpenAsync(RuvioOutageMode.FailClosed);

        scope.Proxy.Cut();
        await Assert.ThrowsAnyAsync<Exception>(() => scope.Cache.GetStringAsync("k"));
    }

    [OutageLiveFact]
    public async Task FailOpenTreatsReadsAsMissesAndDropsWritesAndRemovals()
    {
        await using var scope = await OpenAsync(RuvioOutageMode.FailOpen);

        await scope.Cache.SetStringAsync("k", "before", Minutes);
        scope.Proxy.Cut();

        Assert.Null(await scope.Cache.GetStringAsync("k"));
        Assert.Null(await scope.Cache.GetAsync("k"));
        await scope.Cache.SetStringAsync("k", "during", Minutes);
        await scope.Cache.SetAsync("k2", [1, 2, 3], Minutes);
        await scope.Cache.RemoveAsync("k");
        await scope.Cache.RefreshAsync("k");
        scope.Cache.Set("k3", [1], Minutes);
        Assert.Null(scope.Cache.Get("k3"));

        scope.Proxy.Restore();
        await OutageClient.WaitUntilServingAsync(scope.Client);

        // The write and the removal made during the outage never happened.
        Assert.Equal("before", await scope.Cache.GetStringAsync("k"));
        Assert.Null(await scope.Cache.GetAsync("k2"));
        await scope.Cache.SetStringAsync("k", "after", Minutes);
        Assert.Equal("after", await scope.Cache.GetStringAsync("k"));
    }

    [OutageLiveFact]
    public async Task FailOpenGetOrCreateRunsTheFactoryOnceWhetherRuvioIsAwayAtTheStartOrAfterTheFactoryRan()
    {
        await using var scope = await OpenAsync(RuvioOutageMode.FailOpen);
        int calls = 0;

        scope.Proxy.Cut();
        Assert.Equal("built-1", await scope.Cache.GetOrCreateStringAsync("a",
            _ => Task.FromResult("built-" + Interlocked.Increment(ref calls)), Minutes));
        Assert.Equal(1, calls);

        scope.Proxy.Restore();
        await OutageClient.WaitUntilServingAsync(scope.Client);

        // Ruvio goes away while the factory runs, so only storing the value fails.
        string value = await scope.Cache.GetOrCreateStringAsync("b", _ =>
        {
            scope.Proxy.Cut();

            return Task.FromResult("built-" + Interlocked.Increment(ref calls));
        }, Minutes);

        Assert.Equal("built-2", value);
        Assert.Equal(2, calls);
    }

    [OutageLiveFact]
    public async Task AnErrorFromTheFactoryIsNeverTakenForAnOutage()
    {
        await using var scope = await OpenAsync(RuvioOutageMode.FailOpen, breakerThreshold: 1);
        int calls = 0;

        // One key per call: a failed fill leaves its marker on the key for the fill timeout.
        for (int i = 0; i < 3; i++)
        {
            await Assert.ThrowsAsync<IOException>(() => scope.Cache.GetOrCreateStringAsync("c" + i, _ =>
            {
                Interlocked.Increment(ref calls);

                throw new IOException("the factory's own network error");
            }, Minutes));
        }

        Assert.Equal(3, calls); // run once per call, and the circuit did not open
        Assert.Equal("ok", await scope.Cache.GetOrCreateStringAsync("c-ok", _ => Task.FromResult("ok"), Minutes));
    }

    [OutageLiveFact]
    public async Task AServerErrorReplyIsNotAnOutageAndIsNeverSwallowedEvenWhenFailingOpen()
    {
        await using var scope = await OpenAsync(RuvioOutageMode.FailOpen);
        string key = scope.Instance + ":ruvio:cache:v1:wrong";

        await scope.Client.ExecuteAsync("LPUSH", key, "x");

        try
        {
            var error = await Assert.ThrowsAsync<RuvioException>(() => scope.Cache.GetStringAsync("wrong"));

            Assert.True(error.IsServerReply);
            Assert.StartsWith("WRONGTYPE", error.Message);
        }
        finally
        {
            await scope.Client.DeleteAsync(key);
        }
    }

    [OutageLiveFact]
    public async Task AnOpenCircuitStopsCallingRuvioAndAProbeClosesItAgain()
    {
        await using var scope = await OpenAsync(RuvioOutageMode.FailOpen, breakerThreshold: 2);

        await scope.Cache.SetStringAsync("k", "v", Minutes);
        scope.Proxy.Cut();

        for (int i = 0; i < 2; i++)
            Assert.Null(await scope.Cache.GetStringAsync("k"));

        long bytes = scope.Proxy.BytesToServer;
        long connections = scope.Proxy.Connections;

        for (int i = 0; i < 5; i++)
        {
            Assert.Null(await scope.Cache.GetStringAsync("k"));
            await scope.Cache.SetStringAsync("x", "y", Minutes);
        }

        Assert.Equal(bytes, scope.Proxy.BytesToServer);
        Assert.Equal(connections, scope.Proxy.Connections);

        scope.Proxy.Restore();
        await OutageClient.WaitUntilServingAsync(scope.Client);
        bytes = scope.Proxy.BytesToServer;

        Assert.Null(await scope.Cache.GetStringAsync("k")); // Ruvio is back but the circuit is still open
        Assert.Equal(bytes, scope.Proxy.BytesToServer);

        scope.Time.Advance(TimeSpan.FromSeconds(30));
        Assert.Equal("v", await scope.Cache.GetStringAsync("k"));
        Assert.Equal("v", await scope.Cache.GetStringAsync("k"));
    }

    [OutageLiveFact]
    public async Task ThrowModeWithACircuitRaisesTheCircuitOpenErrorWithoutCallingRuvio()
    {
        await using var scope = await OpenAsync(RuvioOutageMode.Throw, breakerThreshold: 1);

        scope.Proxy.Cut();
        await Assert.ThrowsAnyAsync<Exception>(() => scope.Cache.GetStringAsync("k"));

        long bytes = scope.Proxy.BytesToServer;
        long connections = scope.Proxy.Connections;
        var open = await Assert.ThrowsAsync<RuvioCircuitOpenException>(() => scope.Cache.GetStringAsync("k"));

        Assert.True(open.RetryAfter > TimeSpan.Zero);
        await Assert.ThrowsAsync<RuvioCircuitOpenException>(() => scope.Cache.SetStringAsync("k", "v", Minutes));
        await Assert.ThrowsAsync<RuvioCircuitOpenException>(() => scope.Cache.RemoveAsync("k"));
        await Assert.ThrowsAsync<RuvioCircuitOpenException>(() => scope.Cache.GetOrCreateStringAsync("k", _ => Task.FromResult("v"), Minutes));
        Assert.Equal(bytes, scope.Proxy.BytesToServer);
        Assert.Equal(connections, scope.Proxy.Connections);
    }

    [Fact]
    public void InvalidOutageSettingsAreRejectedWhenTheCacheIsCreated()
    {
        var options = new RuvioCacheOptions();

        options.Outage.CircuitBreaker = new RuvioCircuitBreakerOptions { FailureThreshold = 0 };

        Assert.Throws<ArgumentOutOfRangeException>(() => options.Validate());
    }
}
