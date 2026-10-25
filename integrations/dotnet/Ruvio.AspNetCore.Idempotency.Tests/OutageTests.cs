using System.Net;
using System.Security.Claims;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Ruvio.Client;
using Ruvio.Integrations.Tests;
using Xunit;

namespace Ruvio.AspNetCore.Idempotency.Tests;

/// <summary>Real HTTP requests through the middleware to a live server that a proxy can take away.</summary>
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
        builder.Services.AddRuvioIdempotency(options =>
        {
            options.Namespace = "outage-" + Guid.NewGuid().ToString("N");
            options.ScopeSelector = _ => "tenant";
            options.Retention = TimeSpan.FromMinutes(5);
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

        app.Use(async (context, next) =>
        {
            context.User = new ClaimsPrincipal(new ClaimsIdentity([new Claim("sub", "user")], "test"));
            await next();
        });
        app.UseRouting();
        app.UseRuvioIdempotency();
        app.MapPost("/pay", () =>
        {
            Interlocked.Increment(ref counter[0]);

            return new { paid = true };
        }).WithRuvioIdempotency();
        // Ruvio goes away while the handler runs, so only recording the result fails.
        app.MapPost("/pay-then-outage", () =>
        {
            Interlocked.Increment(ref counter[0]);
            proxy.Cut();

            return new { paid = true };
        }).WithRuvioIdempotency();
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

    private static async Task<HttpResponseMessage> PostAsync(Host host, string key, string path = "/pay")
    {
        using var cancel = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        using var request = new HttpRequestMessage(HttpMethod.Post, path) { Content = new StringContent("{}") };

        request.Headers.Add("Idempotency-Key", key);

        return await host.Http.SendAsync(request, cancel.Token);
    }

    [OutageLiveFact]
    public async Task TheDefaultStillThrowsWhenRuvioIsAway()
    {
        await using var host = await StartAsync(RuvioOutageMode.Throw);

        Assert.Equal(HttpStatusCode.OK, (await PostAsync(host, "a")).StatusCode);
        host.Proxy.Cut();
        Assert.Equal(HttpStatusCode.InternalServerError, (await PostAsync(host, "b")).StatusCode);
        Assert.Equal(1, host.Handled);
    }

    [OutageLiveFact]
    public async Task FailClosedAnswers503WithoutRunningTheHandler()
    {
        await using var host = await StartAsync(RuvioOutageMode.FailClosed);

        host.Proxy.Cut();
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await PostAsync(host, "a")).StatusCode);
        Assert.Equal(0, host.Handled);
    }

    [OutageLiveFact]
    public async Task FailOpenRunsTheHandlerUncheckedAndReplayWorksAgainWhenRuvioReturns()
    {
        await using var host = await StartAsync(RuvioOutageMode.FailOpen);

        host.Proxy.Cut();

        // No idempotency while Ruvio is away: the same key runs the handler each time. That is the accepted risk.
        using var first = await PostAsync(host, "k");
        using var second = await PostAsync(host, "k");

        Assert.Equal(HttpStatusCode.OK, first.StatusCode);
        Assert.Equal(HttpStatusCode.OK, second.StatusCode);
        Assert.Equal("""{"paid":true}""", await first.Content.ReadAsStringAsync());
        Assert.Equal(2, host.Handled);

        host.Proxy.Restore();
        await OutageClient.WaitUntilServingAsync(host.Client);

        using var third = await PostAsync(host, "k");
        using var replay = await PostAsync(host, "k");

        Assert.Equal(HttpStatusCode.OK, third.StatusCode);
        Assert.Equal(HttpStatusCode.OK, replay.StatusCode);
        Assert.Equal(3, host.Handled); // the fourth request was a replay
    }

    [OutageLiveFact]
    public async Task FailOpenDeliversTheResponseWhenRuvioDisappearsAfterTheHandlerRan()
    {
        await using var host = await StartAsync(RuvioOutageMode.FailOpen);

        using var response = await PostAsync(host, "late", "/pay-then-outage");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("""{"paid":true}""", await response.Content.ReadAsStringAsync());
        Assert.Equal(1, host.Handled);

        host.Proxy.Restore();
        await OutageClient.WaitUntilServingAsync(host.Client);

        // The result was never recorded: the key is still pending, so a retry cannot run the handler twice.
        Assert.Equal(HttpStatusCode.Conflict, (await PostAsync(host, "late", "/pay-then-outage")).StatusCode);
        Assert.Equal(1, host.Handled);
    }

    [OutageLiveFact]
    public async Task FailClosedAnswers503WhenRuvioDisappearsAfterTheHandlerRan()
    {
        await using var host = await StartAsync(RuvioOutageMode.FailClosed);

        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await PostAsync(host, "late", "/pay-then-outage")).StatusCode);
        Assert.Equal(1, host.Handled);
    }

    [OutageLiveFact]
    public async Task AnOpenCircuitStopsCallingRuvioAndAProbeClosesItAgain()
    {
        await using var host = await StartAsync(RuvioOutageMode.FailClosed, breakerThreshold: 2);

        host.Proxy.Cut();
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await PostAsync(host, "a")).StatusCode);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await PostAsync(host, "b")).StatusCode);

        long bytes = host.Proxy.BytesToServer;
        long connections = host.Proxy.Connections;

        for (int i = 0; i < 5; i++)
        {
            using var response = await PostAsync(host, "c" + i);

            Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
            Assert.Equal("30", response.Headers.GetValues("Retry-After").Single());
        }

        Assert.Equal(bytes, host.Proxy.BytesToServer);
        Assert.Equal(connections, host.Proxy.Connections);

        host.Proxy.Restore();
        await OutageClient.WaitUntilServingAsync(host.Client);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await PostAsync(host, "d")).StatusCode);

        host.Time.Advance(TimeSpan.FromSeconds(30));
        Assert.Equal(HttpStatusCode.OK, (await PostAsync(host, "e")).StatusCode);
        Assert.Equal(HttpStatusCode.OK, (await PostAsync(host, "f")).StatusCode);
        Assert.Equal(2, host.Handled);
    }

    [OutageLiveFact]
    public async Task FailOpenWithACircuitSkipsRuvioWhileTheCircuitIsOpen()
    {
        await using var host = await StartAsync(RuvioOutageMode.FailOpen, breakerThreshold: 1);

        host.Proxy.Cut();
        Assert.Equal(HttpStatusCode.OK, (await PostAsync(host, "a")).StatusCode);

        long bytes = host.Proxy.BytesToServer;
        long connections = host.Proxy.Connections;

        for (int i = 0; i < 5; i++)
            Assert.Equal(HttpStatusCode.OK, (await PostAsync(host, "b" + i)).StatusCode);

        Assert.Equal(6, host.Handled);
        Assert.Equal(bytes, host.Proxy.BytesToServer);
        Assert.Equal(connections, host.Proxy.Connections);
    }

    [Fact]
    public void InvalidOutageSettingsFailTheOptionsValidation()
    {
        var options = new IdempotencyOptions
        {
            Namespace = "n",
            ScopeSelector = _ => "s",
        };

        Assert.True(options.IsValid());
        options.Outage.CircuitBreaker = new RuvioCircuitBreakerOptions { FailureThreshold = 0 };
        Assert.False(options.IsValid());
    }
}
