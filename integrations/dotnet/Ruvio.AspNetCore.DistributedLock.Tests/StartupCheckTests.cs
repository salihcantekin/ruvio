using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.DistributedLock.Tests;

/// <summary>What the package checks before the server takes a request.</summary>
public sealed class StartupCheckTests
{
    [Fact]
    public async Task MissingClientFailsStartAndNamesTheFix()
    {
        await using WebApplication app = App(static services => { });

        var error = await Assert.ThrowsAsync<OptionsValidationException>(() => app.StartAsync());

        Assert.Contains("services.AddRuvioClient", error.Message);
    }

    [Fact]
    public async Task RegisteredClientIsNotResolvedAtStart()
    {
        await using WebApplication app = App(Client, map: static app => app.MapPost("/ok", [RuvioLock("job")] () => "ok"));

        await app.StartAsync();
        await app.StopAsync();
    }

    [Fact]
    public async Task OutOfRangeAttributeFailsStart()
    {
        await using WebApplication app = App(Client, map: static app =>
        {
            app.MapPost("/status", [RuvioLock("job", RejectStatusCode = 200)] () => "status");
            app.MapPost("/wait", [RuvioLock("job", WaitMilliseconds = -5)] () => "wait");
        });

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => app.StartAsync());

        Assert.Contains("RejectStatusCode 200", error.Message);
        Assert.Contains("WaitMilliseconds -5", error.Message);
    }

    [Fact]
    public void OutOfRangeFilterSettingFailsWhenMapped()
    {
        var builder = WebApplication.CreateBuilder();

        builder.Logging.ClearProviders();
        using WebApplication app = builder.Build();

        Assert.Throws<ArgumentOutOfRangeException>(() => app.MapPost("/a", () => "a").WithDistributedLock("a", conflictStatusCode: 200));
        Assert.Throws<ArgumentOutOfRangeException>(() => app.MapPost("/b", () => "b").WithDistributedLock("b", wait: TimeSpan.FromSeconds(-1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => app.MapPost("/c", () => "c").WithDistributedLock("c", lease: TimeSpan.Zero));
    }

    [Fact]
    public void UseWithoutAddFails()
    {
        var builder = WebApplication.CreateBuilder();

        builder.Logging.ClearProviders();
        using WebApplication app = builder.Build();

        var error = Assert.Throws<InvalidOperationException>(() => app.UseRuvioLock());

        Assert.Contains("AddRuvioDistributedLock", error.Message);
    }

    private static void Client(IServiceCollection services)
        => services.AddSingleton<IRuvioClient>(static _ => throw new InvalidOperationException("The client was resolved at start."));

    private static WebApplication App(Action<IServiceCollection> services, Action<WebApplication>? map = null)
    {
        var builder = WebApplication.CreateBuilder();

        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();
        services(builder.Services);
        builder.Services.AddRuvioDistributedLock();

        WebApplication app = builder.Build();

        app.UseRuvioLock();
        map?.Invoke(app);

        return app;
    }
}
