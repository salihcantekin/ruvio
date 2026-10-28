using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.OutputCaching.Tests;

/// <summary>What the package checks before the server takes a request.</summary>
public sealed class StartupCheckTests
{
    [Fact(Timeout = 30000)]
    public async Task MissingKeyedClientFailsStartAndNamesTheKey()
    {
        await using WebApplication app = App(static services =>
            services.AddSingleton<IRuvioClient>(static _ => throw new InvalidOperationException("Unkeyed client.")), "pages");

        var error = await Assert.ThrowsAsync<OptionsValidationException>(() => app.StartAsync());

        Assert.Contains("AddKeyedRuvioClient(\"pages\"", error.Message);
    }

    [Fact(Timeout = 30000)]
    public async Task RegisteredClientIsNotResolvedAtStart()
    {
        await using WebApplication app = App(static services =>
            services.AddSingleton<IRuvioClient>(static _ => throw new InvalidOperationException("The client was resolved at start.")));

        await app.StartAsync();
        await app.StopAsync();
    }

    private static WebApplication App(Action<IServiceCollection> services, string? clientKey = null)
    {
        var builder = WebApplication.CreateBuilder();

        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();
        services(builder.Services);
        builder.Services.AddRuvioOutputCache(o => o.ClientKey = clientKey);

        return builder.Build();
    }
}
