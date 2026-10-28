using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Ruvio.Client;
using Ruvio.Integrations.Tests;
using Xunit;

namespace Ruvio.AspNetCore.RateLimiting.Tests;

/// <summary>
/// Real HTTP requests through the middleware to a live server that a proxy can take away, so Ruvio really is
/// unreachable in the middle of a test.
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

    private static async Task<Host> StartAsync(RuvioOutageMode mode, int? breakerThreshold = null, long permits = 1000)
    {
        var proxy = OutageProxy.ForLiveServer();
        var (connection, client) = await OutageClient.ConnectAsync(proxy);
        var time = new ManualTime();
        var builder = WebApplication.CreateBuilder();

        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(client);
        builder.Services.AddRuvioRateLimiting(options =>
        {
            options.KeyPrefix = "test:outage:" + Guid.NewGuid().ToString("N") + ":";
            options.PartitionKey = _ => "tenant";
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
        int[] counter = new int[1];

        app.UseRouting();
        app.UseRuvioRateLimiting();
        app.MapGet("/limited", () =>
        {
            Interlocked.Increment(ref counter[0]);

            return "ok";
        }).WithRuvioRateLimit("outage", permits, TimeSpan.FromMinutes(5));
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

    private static async Task<HttpResponseMessage> GetAsync(Host host)
    {
        using var cancel = new CancellationTokenSource(TimeSpan.FromSeconds(20));

        return await host.Http.GetAsync("/limited", cancel.Token);
    }

    [OutageLiveFact]
    public async Task WithRuvioAwayTheDefaultStillThrowsAndAnswers500()
    {
        await using var host = await StartAsync(RuvioOutageMode.Throw);

        Assert.Equal(HttpStatusCode.OK, (await GetAsync(host)).StatusCode);
        host.Proxy.Cut();
        Assert.Equal(HttpStatusCode.InternalServerError, (await GetAsync(host)).StatusCode);
        Assert.Equal(1, host.Handled);
    }

    [OutageLiveFact]
    public async Task FailClosedAnswers503AndTheHandlerDoesNotRun()
    {
        await using var host = await StartAsync(RuvioOutageMode.FailClosed);

        Assert.Equal(HttpStatusCode.OK, (await GetAsync(host)).StatusCode);
        host.Proxy.Cut();

        using var response = await GetAsync(host);

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.Equal(1, host.Handled);
    }

    [OutageLiveFact]
    public async Task FailOpenLetsTheRequestThroughAndLimitingResumesWhenRuvioReturns()
    {
        await using var host = await StartAsync(RuvioOutageMode.FailOpen, permits: 2);

        Assert.Equal(HttpStatusCode.OK, (await GetAsync(host)).StatusCode);
        Assert.Equal(HttpStatusCode.OK, (await GetAsync(host)).StatusCode);
        Assert.Equal((HttpStatusCode)429, (await GetAsync(host)).StatusCode);

        host.Proxy.Cut();

        for (int i = 0; i < 3; i++)
            Assert.Equal(HttpStatusCode.OK, (await GetAsync(host)).StatusCode);

        Assert.Equal(5, host.Handled);

        host.Proxy.Restore();
        await OutageClient.WaitUntilServingAsync(host.Client);

        // The window is still the one from before the outage: the budget is spent.
        Assert.Equal((HttpStatusCode)429, (await GetAsync(host)).StatusCode);
    }

    [OutageLiveFact]
    public async Task AnErrorReplyIsNotAnOutageAndIsNeverSwallowedEvenWhenFailingOpen()
    {
        await using var host = await StartAsync(RuvioOutageMode.FailOpen);
        var options = host.App.Services.GetRequiredService<Microsoft.Extensions.Options.IOptions<RuvioRateLimitingOptions>>().Value;
        string key = RuvioRateLimitingMiddleware.StorageKey(options.KeyPrefix, "outage", "tenant");

        // A list sits where the counter should be, so LIMIT answers WRONGTYPE.
        await host.Client.ExecuteAsync("LPUSH", key, "x");

        try
        {
            Assert.Equal(HttpStatusCode.InternalServerError, (await GetAsync(host)).StatusCode);
            Assert.Equal(0, host.Handled);
        }
        finally
        {
            await host.Client.DeleteAsync(key);
        }
    }

    [OutageLiveFact]
    public async Task AnOpenCircuitStopsCallingRuvioAndAProbeClosesItAgain()
    {
        await using var host = await StartAsync(RuvioOutageMode.FailClosed, breakerThreshold: 2);

        Assert.Equal(HttpStatusCode.OK, (await GetAsync(host)).StatusCode);
        host.Proxy.Cut();

        // Two failures in a row open the circuit.
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await GetAsync(host)).StatusCode);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await GetAsync(host)).StatusCode);

        long bytes = host.Proxy.BytesToServer;
        long connections = host.Proxy.Connections;

        for (int i = 0; i < 5; i++)
        {
            using var response = await GetAsync(host);

            Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
            Assert.Equal("30", response.Headers.GetValues("Retry-After").Single());
        }

        Assert.Equal(bytes, host.Proxy.BytesToServer);
        Assert.Equal(connections, host.Proxy.Connections);

        host.Proxy.Restore();
        await OutageClient.WaitUntilServingAsync(host.Client);

        // Ruvio is back, but the circuit stays open until its break is over.
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await GetAsync(host)).StatusCode);

        host.Time.Advance(TimeSpan.FromSeconds(30));
        Assert.Equal(HttpStatusCode.OK, (await GetAsync(host)).StatusCode);
        Assert.Equal(HttpStatusCode.OK, (await GetAsync(host)).StatusCode);
        Assert.Equal(3, host.Handled);
    }

    [OutageLiveFact]
    public async Task AFailedProbeKeepsTheCircuitOpenForAnotherBreak()
    {
        await using var host = await StartAsync(RuvioOutageMode.FailClosed, breakerThreshold: 1);

        host.Proxy.Cut();
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await GetAsync(host)).StatusCode);

        host.Time.Advance(TimeSpan.FromSeconds(30));
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await GetAsync(host)).StatusCode); // the probe, still down

        long bytes = host.Proxy.BytesToServer;
        long connections = host.Proxy.Connections;

        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await GetAsync(host)).StatusCode);
        Assert.Equal(bytes, host.Proxy.BytesToServer);
        Assert.Equal(connections, host.Proxy.Connections);
    }

    [OutageLiveFact]
    public async Task FailOpenWithACircuitRunsRequestsWithoutTouchingRuvioWhileItIsOpen()
    {
        await using var host = await StartAsync(RuvioOutageMode.FailOpen, breakerThreshold: 1);

        host.Proxy.Cut();
        Assert.Equal(HttpStatusCode.OK, (await GetAsync(host)).StatusCode);

        long bytes = host.Proxy.BytesToServer;
        long connections = host.Proxy.Connections;

        for (int i = 0; i < 5; i++)
            Assert.Equal(HttpStatusCode.OK, (await GetAsync(host)).StatusCode);

        Assert.Equal(6, host.Handled);
        Assert.Equal(bytes, host.Proxy.BytesToServer);
        Assert.Equal(connections, host.Proxy.Connections);
    }

    [OutageLiveFact]
    public async Task ThrowModeWithACircuitRaisesTheCircuitOpenErrorWithoutCallingRuvio()
    {
        await using var host = await StartAsync(RuvioOutageMode.Throw, breakerThreshold: 1);

        host.Proxy.Cut();
        Assert.Equal(HttpStatusCode.InternalServerError, (await GetAsync(host)).StatusCode);

        long bytes = host.Proxy.BytesToServer;

        Assert.Equal(HttpStatusCode.InternalServerError, (await GetAsync(host)).StatusCode);
        Assert.Equal(bytes, host.Proxy.BytesToServer);
    }

    [Fact]
    public void InvalidOutageSettingsFailWhenTheOptionsAreFirstUsed()
    {
        var services = new ServiceCollection();

        services.AddRuvioRateLimiting(options =>
        {
            options.PartitionKey = _ => "tenant";
            options.Outage.CircuitBreaker = new RuvioCircuitBreakerOptions { FailureThreshold = 0 };
        });

        using var provider = services.BuildServiceProvider();

        Assert.Throws<Microsoft.Extensions.Options.OptionsValidationException>(
            () => provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<RuvioRateLimitingOptions>>().Value);
    }
}
