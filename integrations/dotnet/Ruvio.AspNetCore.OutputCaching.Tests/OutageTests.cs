using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.OutputCaching;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Ruvio.Client;
using Ruvio.Integrations.Tests;
using Xunit;

namespace Ruvio.AspNetCore.OutputCaching.Tests;

/// <summary>Real HTTP requests through the output-cache middleware to a live server that a proxy can take away.</summary>
public class OutageTests
{
    private sealed class Host : IAsyncDisposable
    {
        public required WebApplication App { get; init; }
        public required HttpClient Http { get; init; }
        public required OutageProxy Proxy { get; init; }
        public required RuvioConnection Connection { get; init; }
        public required IRuvioClient Client { get; init; }
        public required ManualTime Time { get; init; }
        public required int[] Counter { get; init; }
        public int Handled => Volatile.Read(ref Counter[0]);

        public async ValueTask DisposeAsync()
        {
            Http.Dispose();
            await App.StopAsync();
            await App.DisposeAsync();
            await Connection.DisposeAsync();
            Proxy.Dispose();
        }
    }

    private static async Task<Host> StartAsync(RuvioOutageMode mode, int? breakerThreshold = null)
    {
        var proxy = OutageProxy.ForLiveServer();
        var (connection, client) = await OutageClient.ConnectAsync(proxy);
        var time = new ManualTime();
        int[] counter = new int[1];
        var builder = WebApplication.CreateBuilder();

        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(client);
        builder.Services.AddRuvioOutputCache(options =>
        {
            options.InstanceName = "outage-" + Guid.NewGuid().ToString("N");
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
        });

        var app = builder.Build();

        app.UseOutputCache();
        app.MapGet("/cached", () => "page-" + Interlocked.Increment(ref counter[0]))
            .CacheOutput(policy => policy.Expire(TimeSpan.FromMinutes(5)).Tag("catalog"));
        await app.StartAsync();

        string address = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.Single();
        var http = new HttpClient { BaseAddress = new Uri(address), Timeout = TimeSpan.FromSeconds(15) };

        http.DefaultRequestHeaders.Host = "application.example";

        return new Host
        {
            App = app,
            Http = http,
            Proxy = proxy,
            Connection = connection,
            Client = client,
            Time = time,
            Counter = counter,
        };
    }

    private static async Task<(HttpStatusCode Status, string Body)> GetAsync(Host host)
    {
        using var cancel = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        using var response = await host.Http.GetAsync("/cached", cancel.Token);

        return (response.StatusCode, await response.Content.ReadAsStringAsync(cancel.Token));
    }

    [OutageLiveFact]
    public async Task TheFrameworkAlreadyTreatsStoreErrorsAsAMissButAnEvictionStillThrowsByDefault()
    {
        await using var host = await StartAsync(RuvioOutageMode.Throw);
        var store = host.App.Services.GetRequiredService<IOutputCacheStore>();

        Assert.Equal((HttpStatusCode.OK, "page-1"), await GetAsync(host));
        host.Proxy.Cut();

        // ASP.NET Core's output-cache middleware catches store errors on reads and writes.
        Assert.Equal((HttpStatusCode.OK, "page-2"), await GetAsync(host));
        await Assert.ThrowsAnyAsync<Exception>(async () => await store.EvictByTagAsync("catalog", default));
    }

    [OutageLiveFact]
    public async Task FailOpenServesUncachedPagesWhileRuvioIsAwayAndCachesAgainAfterwards()
    {
        await using var host = await StartAsync(RuvioOutageMode.FailOpen);

        Assert.Equal((HttpStatusCode.OK, "page-1"), await GetAsync(host));
        Assert.Equal((HttpStatusCode.OK, "page-1"), await GetAsync(host)); // cached

        host.Proxy.Cut();
        Assert.Equal((HttpStatusCode.OK, "page-2"), await GetAsync(host)); // a miss: the endpoint runs
        Assert.Equal((HttpStatusCode.OK, "page-3"), await GetAsync(host));

        host.Proxy.Restore();
        await OutageClient.WaitUntilServingAsync(host.Client);

        // The entry written before the outage is still there; the pages built during it were never stored.
        Assert.Equal((HttpStatusCode.OK, "page-1"), await GetAsync(host));
        Assert.Equal(3, host.Handled);
    }

    [OutageLiveFact]
    public async Task FailOpenDropsAnEvictionSoTheStaleEntryComesBackWhenRuvioReturns()
    {
        await using var host = await StartAsync(RuvioOutageMode.FailOpen);
        var store = host.App.Services.GetRequiredService<IOutputCacheStore>();

        Assert.Equal((HttpStatusCode.OK, "page-1"), await GetAsync(host));

        host.Proxy.Cut();
        await store.EvictByTagAsync("catalog", default); // does not throw, and evicts nothing
        host.Proxy.Restore();
        await OutageClient.WaitUntilServingAsync(host.Client);

        Assert.Equal((HttpStatusCode.OK, "page-1"), await GetAsync(host)); // the documented cost of failing open

        await store.EvictByTagAsync("catalog", default);
        Assert.Equal((HttpStatusCode.OK, "page-2"), await GetAsync(host));
    }

    [OutageLiveFact]
    public async Task AnOpenCircuitStopsCallingRuvioAndAProbeClosesItAgain()
    {
        await using var host = await StartAsync(RuvioOutageMode.FailOpen, breakerThreshold: 2);

        Assert.Equal((HttpStatusCode.OK, "page-1"), await GetAsync(host));
        host.Proxy.Cut();

        for (int i = 0; i < 2; i++)
            Assert.Equal(HttpStatusCode.OK, (await GetAsync(host)).Status);

        long bytes = host.Proxy.BytesToServer;
        long connections = host.Proxy.Connections;

        for (int i = 0; i < 5; i++)
            Assert.Equal(HttpStatusCode.OK, (await GetAsync(host)).Status);

        Assert.Equal(bytes, host.Proxy.BytesToServer);
        Assert.Equal(connections, host.Proxy.Connections);

        host.Proxy.Restore();
        await OutageClient.WaitUntilServingAsync(host.Client);
        bytes = host.Proxy.BytesToServer;

        // Ruvio is back, but the circuit stays open until its break is over.
        await GetAsync(host);
        Assert.Equal(bytes, host.Proxy.BytesToServer);

        host.Time.Advance(TimeSpan.FromSeconds(30));
        Assert.Equal((HttpStatusCode.OK, "page-1"), await GetAsync(host)); // the probe: the cached entry is served again
        Assert.True(host.Proxy.BytesToServer > bytes);
    }

    [OutageLiveFact]
    public async Task ThrowModeWithACircuitRaisesTheCircuitOpenErrorWithoutCallingRuvio()
    {
        await using var host = await StartAsync(RuvioOutageMode.Throw, breakerThreshold: 1);
        var store = host.App.Services.GetRequiredService<IOutputCacheStore>();

        host.Proxy.Cut();
        await Assert.ThrowsAnyAsync<Exception>(async () => await store.EvictByTagAsync("catalog", default));

        long bytes = host.Proxy.BytesToServer;
        long connections = host.Proxy.Connections;

        await Assert.ThrowsAsync<RuvioCircuitOpenException>(async () => await store.EvictByTagAsync("catalog", default));
        Assert.Equal(HttpStatusCode.OK, (await GetAsync(host)).Status); // the framework still answers, uncached
        Assert.Equal(bytes, host.Proxy.BytesToServer);
        Assert.Equal(connections, host.Proxy.Connections);
    }

    [Fact]
    public void InvalidOutageSettingsAreRejectedAtRegistration()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentOutOfRangeException>(() => services.AddRuvioOutputCache(options =>
            options.Outage.CircuitBreaker = new RuvioCircuitBreakerOptions { FailureThreshold = 0 }));
    }
}
