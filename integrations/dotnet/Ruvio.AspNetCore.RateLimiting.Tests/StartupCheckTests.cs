using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.RateLimiting.Tests;

/// <summary>What the package checks before the server takes a request.</summary>
public sealed class StartupCheckTests
{
    [Fact(Timeout = 30000)]
    public async Task MissingClientFailsStartAndNamesTheFix()
    {
        await using WebApplication app = App(services => { });

        var error = await Assert.ThrowsAsync<OptionsValidationException>(() => app.StartAsync());

        Assert.Contains("services.AddRuvioClient", error.Message);
    }

    [Fact(Timeout = 30000)]
    public async Task MissingKeyedClientFailsStartAndNamesTheKey()
    {
        await using WebApplication app = App(Client, clientKey: "limits");

        var error = await Assert.ThrowsAsync<OptionsValidationException>(() => app.StartAsync());

        Assert.Contains("AddKeyedRuvioClient(\"limits\"", error.Message);
    }

    [Fact(Timeout = 30000)]
    public async Task RegisteredClientIsNotResolvedAtStart()
    {
        await using WebApplication app = App(services =>
            services.AddSingleton<IRuvioClient>(_ => throw new InvalidOperationException("The client was resolved at start.")));

        await app.StartAsync();
        await app.StopAsync();
    }

    [Fact(Timeout = 30000)]
    public async Task UnknownNamedPolicyOnAnEndpointFailsStart()
    {
        await using WebApplication app = App(Client, app =>
        {
            app.MapGet("/ok", () => "ok").WithRuvioRateLimit("api");
            app.MapGet("/typo", () => "typo").WithRuvioRateLimit("apii");
        });

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => app.StartAsync());

        Assert.Contains("'apii'", error.Message);
        Assert.Contains("/typo", error.Message);
        Assert.DoesNotContain("/ok", error.Message);
    }

    [Fact(Timeout = 30000)]
    public async Task ExemptedEndpointIsNotChecked()
    {
        await using WebApplication app = App(Client, app =>
            app.MapGet("/free", () => "free").WithRuvioRateLimit("apii").DisableRuvioRateLimit());

        await app.StartAsync();
        await app.StopAsync();
    }

    [Fact(Timeout = 30000)]
    public async Task InlineLimitWithoutPartitionKeyFailsStart()
    {
        await using WebApplication app = App(Client, app =>
            app.MapGet("/inline", () => "inline").WithRuvioRateLimit("inline", 5, TimeSpan.FromSeconds(1)),
            partitionKey: false);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => app.StartAsync());

        Assert.Contains("PartitionKey", error.Message);
    }

    [Fact(Timeout = 30000)]
    public async Task UseWithoutAddFailsStart()
    {
        var builder = WebApplication.CreateBuilder();

        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();
        await using WebApplication app = builder.Build();

        var error = Assert.Throws<InvalidOperationException>(() => app.UseRuvioRateLimiting());

        Assert.Contains("AddRuvioRateLimiting", error.Message);
    }

    private static void Client(IServiceCollection services)
        => services.AddSingleton<IRuvioClient>(_ => throw new InvalidOperationException("The client was resolved at start."));

    private static WebApplication App(
        Action<IServiceCollection> services, Action<WebApplication>? map = null, bool partitionKey = true, string? clientKey = null)
    {
        var builder = WebApplication.CreateBuilder();

        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();
        services(builder.Services);
        builder.Services.AddRuvioRateLimiting(options =>
        {
            options.ClientKey = clientKey;

            // Without the shared key the named policy brings its own, so only an inline limit lacks one.
            if (partitionKey)
                options.PartitionKey = static context => "p";

            options.AddPolicy("api", policy => policy.Limit(10, TimeSpan.FromSeconds(1), static context => "own"));
        });

        WebApplication app = builder.Build();

        app.UseRuvioRateLimiting();
        map?.Invoke(app);

        return app;
    }
}
