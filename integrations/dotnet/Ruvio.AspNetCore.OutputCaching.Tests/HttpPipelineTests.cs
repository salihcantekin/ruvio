using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.OutputCaching;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.OutputCaching.Tests;

public class HttpPipelineTests
{
    [Fact]
    public async Task UnselectedEndpointDoesNotCreateAClient()
    {
        var builder = Builder();

        builder.Services.AddSingleton<IRuvioClient>(_ => throw new InvalidOperationException("Unexpected connection."));
        builder.Services.AddRuvioOutputCache();
        await using var app = builder.Build();
        app.UseOutputCache();
        app.MapGet("/plain", () => "uncached");
        await app.StartAsync();
        try
        {
            using var http = Http(app);

            Assert.Equal("uncached", await http.GetStringAsync("/plain"));
            Assert.Equal("uncached", await http.GetStringAsync("/plain"));
        }

        finally { await app.StopAsync(); }
    }

    [LiveFact]
    public async Task Framework_cache_shares_responses_between_hosts_varies_by_query_and_evicts_by_tag()
    {
        await using var scope = await LiveScope.Open();
        await using var secondConnection = await LiveScope.Connect();
        RuvioClient secondClient = await secondConnection.GetDefaultDatabaseAsync();
        await using var first = App(scope.Client, scope.Name, "first");
        await using var second = App(secondClient, scope.Name, "second");
        await first.StartAsync();
        await second.StartAsync();
        try
        {
            using var a = Http(first);
            using var b = Http(second);

            Assert.Equal("first-1", await a.GetStringAsync("/cached?variant=1"));
            Assert.Equal("first-1", await b.GetStringAsync("/cached?variant=1"));
            Assert.Equal("second-1", await b.GetStringAsync("/cached?variant=2"));
            await first.Services.GetRequiredService<IOutputCacheStore>().EvictByTagAsync("catalog", default);
            Assert.Equal("second-2", await b.GetStringAsync("/cached?variant=1"));
            Assert.Equal("second-2", await a.GetStringAsync("/cached?variant=1"));
            Assert.Equal("first-2", await a.GetStringAsync("/cookies"));
            Assert.Equal("first-3", await a.GetStringAsync("/cookies"));
        }

        finally
        {
            await first.StopAsync();
            await second.StopAsync();
        }
    }

    private static WebApplication App(IRuvioClient client, string name, string identity)
    {
        var builder = Builder();

        builder.Services.AddSingleton(client);
        builder.Services.AddRuvioOutputCache(o => o.InstanceName = name);
        var app = builder.Build();

        app.UseOutputCache();
        int calls = 0;

        app.MapGet("/cached", () => identity + "-" + Interlocked.Increment(ref calls))
            .CacheOutput(policy => policy.Expire(TimeSpan.FromSeconds(10)).Tag("catalog"));
        app.MapGet("/cookies", (HttpContext context) =>
        {
            context.Response.Cookies.Append("private", "value", new CookieOptions { Secure = true });

            return identity + "-" + Interlocked.Increment(ref calls);
        }).CacheOutput();

        return app;
    }

    private static WebApplicationBuilder Builder()
    {
        var builder = WebApplication.CreateBuilder();

        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();

        return builder;
    }

    private static HttpClient Http(WebApplication app)
    {
        var client = new HttpClient
        {
            BaseAddress = new Uri(app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.Single()),
            Timeout = TimeSpan.FromSeconds(10),
        };

        client.DefaultRequestHeaders.Host = "application.example";

        return client;
    }
}
