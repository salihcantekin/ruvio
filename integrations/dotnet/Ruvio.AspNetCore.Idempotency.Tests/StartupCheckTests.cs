using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.Idempotency.Tests;

/// <summary>What the package checks before the server takes a request.</summary>
public sealed class StartupCheckTests
{
    [Fact(Timeout = 30000)]
    public async Task MissingClientFailsStartAndNamesTheFix()
    {
        await using WebApplication app = App(static services => { });

        var error = await Assert.ThrowsAsync<OptionsValidationException>(() => app.StartAsync());

        Assert.Contains("services.AddRuvioClient", error.Message);
    }

    [Fact(Timeout = 30000)]
    public async Task MissingKeyedClientFailsStartAndNamesTheKey()
    {
        await using WebApplication app = App(Client, clientKey: "orders");

        var error = await Assert.ThrowsAsync<OptionsValidationException>(() => app.StartAsync());

        Assert.Contains("AddKeyedRuvioClient(\"orders\"", error.Message);
    }

    [Fact(Timeout = 30000)]
    public async Task RegisteredKeyedClientIsNotResolvedAtStart()
    {
        await using WebApplication app = App(static services => services.AddKeyedSingleton<IRuvioClient>("orders",
            static (_, _) => throw new InvalidOperationException("The client was resolved at start.")), clientKey: "orders");

        await app.StartAsync();
        await app.StopAsync();
    }

    [Fact(Timeout = 30000)]
    public async Task OutOfRangeEndpointSettingFailsStart()
    {
        await using WebApplication app = App(Client, map: static app =>
        {
            app.MapPost("/ok", () => "ok").WithRuvioIdempotency();
            app.MapPost("/long", () => "long").WithRuvioIdempotency(static a => a.RetentionSeconds = 1_000_000);
        });

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => app.StartAsync());

        Assert.Contains("RetentionSeconds", error.Message);
        Assert.Contains("/long", error.Message);
        Assert.DoesNotContain("/ok", error.Message);
    }

    [Fact(Timeout = 30000)]
    public async Task UseWithoutAddFails()
    {
        var builder = WebApplication.CreateBuilder();

        builder.Logging.ClearProviders();
        await using WebApplication app = builder.Build();

        var error = Assert.Throws<InvalidOperationException>(() => app.UseRuvioIdempotency());

        Assert.Contains("AddRuvioIdempotency", error.Message);
    }

    private static void Client(IServiceCollection services)
        => services.AddSingleton<IRuvioClient>(static _ => throw new InvalidOperationException("The client was resolved at start."));

    private static WebApplication App(Action<IServiceCollection> services, string? clientKey = null, Action<WebApplication>? map = null)
    {
        var builder = WebApplication.CreateBuilder();

        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();
        services(builder.Services);
        builder.Services.AddRuvioIdempotency(o =>
        {
            o.Namespace = "startup";
            o.ScopeSelector = static _ => "tenant";
            o.ClientKey = clientKey;
        });

        WebApplication app = builder.Build();

        app.UseRuvioIdempotency();
        map?.Invoke(app);

        return app;
    }
}
