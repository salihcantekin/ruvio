using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Ruvio.Client;
using Ruvio.Integrations.Tests;
using Xunit;

namespace Ruvio.AspNetCore.DistributedLock.Tests;

/// <summary>
/// Real HTTP requests to both lock integrations (the attribute middleware and the endpoint filter) with a live
/// server that a proxy can take away.
/// </summary>
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
        builder.Services.AddRuvioDistributedLock(options =>
        {
            options.KeyPrefix = "test:outage:" + Guid.NewGuid().ToString("N") + ":";
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

        app.UseRouting();
        app.UseRuvioLock();
        app.MapGet("/middleware", () =>
        {
            Interlocked.Increment(ref counter[0]);

            return "ok";
        }).WithMetadata(new RuvioLockAttribute("outage-middleware"));
        app.MapGet("/filter", () =>
        {
            Interlocked.Increment(ref counter[0]);

            return "ok";
        }).WithDistributedLock("outage-filter");
        await app.StartAsync();

        string address = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.Single();

        return new Host
        {
            App = app,
            Http = new HttpClient { BaseAddress = new Uri(address), Timeout = TimeSpan.FromSeconds(15) },
            Proxy = proxy,
            Connection = connection,
            Client = client,
            Time = time,
            Counter = counter,
        };
    }

    private static async Task<HttpResponseMessage> GetAsync(Host host, string path)
    {
        using var cancel = new CancellationTokenSource(TimeSpan.FromSeconds(20));

        return await host.Http.GetAsync(path, cancel.Token);
    }

    [OutageLiveTheory]
    [InlineData("/middleware")]
    [InlineData("/filter")]
    public async Task FailOpenRunsTheRequestWithoutALockWhileRuvioIsAwayAndLocksAgainWhenItReturns(string path)
    {
        await using var host = await StartAsync(RuvioOutageMode.FailOpen);

        Assert.Equal(HttpStatusCode.OK, (await GetAsync(host, path)).StatusCode);
        host.Proxy.Cut();
        Assert.Equal(HttpStatusCode.OK, (await GetAsync(host, path)).StatusCode);
        Assert.Equal(2, host.Handled);

        host.Proxy.Restore();
        await OutageClient.WaitUntilServingAsync(host.Client);

        Assert.Equal(HttpStatusCode.OK, (await GetAsync(host, path)).StatusCode);
        Assert.Equal(3, host.Handled);

        // The lock is real again: a held lock of the same name makes the next request answer 409.
        var provider = host.App.Services.GetRequiredService<RuvioLockProvider>();
        await using var held = await provider.TryAcquireAsync(path == "/middleware" ? "outage-middleware" : "outage-filter",
            TimeSpan.FromSeconds(30), TimeSpan.Zero);

        Assert.NotNull(held);
        Assert.Equal(HttpStatusCode.Conflict, (await GetAsync(host, path)).StatusCode);
        Assert.Equal(3, host.Handled);
    }

    [OutageLiveTheory]
    [InlineData("/middleware")]
    [InlineData("/filter")]
    public async Task FailClosedAnswers503AndTheHandlerDoesNotRun(string path)
    {
        await using var host = await StartAsync(RuvioOutageMode.FailClosed);

        host.Proxy.Cut();

        using var response = await GetAsync(host, path);

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.Equal(0, host.Handled);
    }

    [OutageLiveFact]
    public async Task TheDefaultKeepsTodaysBehaviorMiddleware503AndFilterException()
    {
        await using var host = await StartAsync(RuvioOutageMode.Throw);

        host.Proxy.Cut();
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await GetAsync(host, "/middleware")).StatusCode);
        Assert.Equal(HttpStatusCode.InternalServerError, (await GetAsync(host, "/filter")).StatusCode);
        Assert.Equal(0, host.Handled);
    }

    [OutageLiveTheory]
    [InlineData("/middleware")]
    [InlineData("/filter")]
    public async Task AnOpenCircuitStopsCallingRuvioAndAProbeClosesItAgain(string path)
    {
        await using var host = await StartAsync(RuvioOutageMode.FailClosed, breakerThreshold: 2);

        host.Proxy.Cut();
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await GetAsync(host, path)).StatusCode);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await GetAsync(host, path)).StatusCode);

        long bytes = host.Proxy.BytesToServer;
        long connections = host.Proxy.Connections;

        for (int i = 0; i < 5; i++)
        {
            using var response = await GetAsync(host, path);

            Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
            Assert.Equal("30", response.Headers.GetValues("Retry-After").Single());
        }

        Assert.Equal(bytes, host.Proxy.BytesToServer);
        Assert.Equal(connections, host.Proxy.Connections);

        host.Proxy.Restore();
        await OutageClient.WaitUntilServingAsync(host.Client);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await GetAsync(host, path)).StatusCode);

        host.Time.Advance(TimeSpan.FromSeconds(30));
        Assert.Equal(HttpStatusCode.OK, (await GetAsync(host, path)).StatusCode);
        Assert.Equal(HttpStatusCode.OK, (await GetAsync(host, path)).StatusCode);
        Assert.Equal(2, host.Handled);
    }

    [OutageLiveTheory]
    [InlineData("/middleware")]
    [InlineData("/filter")]
    public async Task FailOpenWithACircuitSkipsRuvioWhileTheCircuitIsOpen(string path)
    {
        await using var host = await StartAsync(RuvioOutageMode.FailOpen, breakerThreshold: 1);

        host.Proxy.Cut();
        Assert.Equal(HttpStatusCode.OK, (await GetAsync(host, path)).StatusCode);

        long bytes = host.Proxy.BytesToServer;
        long connections = host.Proxy.Connections;

        for (int i = 0; i < 5; i++)
            Assert.Equal(HttpStatusCode.OK, (await GetAsync(host, path)).StatusCode);

        Assert.Equal(6, host.Handled);
        Assert.Equal(bytes, host.Proxy.BytesToServer);
        Assert.Equal(connections, host.Proxy.Connections);
    }

    [Fact]
    public void InvalidOutageSettingsAreRejectedAtRegistration()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentOutOfRangeException>(() => services.AddRuvioDistributedLock(options =>
            options.Outage.CircuitBreaker = new RuvioCircuitBreakerOptions { BreakDuration = TimeSpan.Zero }));
    }
}

public sealed class OutageLiveTheoryAttribute : TheoryAttribute
{
    public OutageLiveTheoryAttribute()
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR")))
            Skip = "Set RUVIO_TEST_ADDR to run against live Ruvio.";
    }
}
