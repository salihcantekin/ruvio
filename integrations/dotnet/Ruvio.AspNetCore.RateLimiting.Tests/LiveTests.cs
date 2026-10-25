using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.RateLimiting.Tests;

public class LiveTests
{
    [Fact]
    public async Task InstancesShareABudgetAndExpiredWindowsReset()
    {
        string? address = Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR");

        if (string.IsNullOrWhiteSpace(address))
            return;

        var uri = new Uri("tcp://" + address);

        await using var firstConnection = await RuvioConnection.ConnectAsync(uri.Host, uri.Port);
        await using var secondConnection = await RuvioConnection.ConnectAsync(uri.Host, uri.Port);
        IRuvioClient first = await firstConnection.GetDefaultDatabaseAsync();
        IRuvioClient second = await secondConnection.GetDefaultDatabaseAsync();
        string prefix = "test:rate:" + Guid.NewGuid().ToString("N") + ":";
        string key = prefix + Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes("4:livetenant")));
        using var firstServices = Services(first, prefix);
        using var secondServices = Services(second, prefix);
        var middleware = MiddlewareTests.Create(_ => Task.CompletedTask, firstServices);

        try
        {
            var contexts = Enumerable.Range(0, 10).Select(index =>
                Context(index % 2 == 0 ? firstServices : secondServices)).ToArray();

            await Task.WhenAll(contexts.Select(middleware.InvokeAsync));
            Assert.Equal(3, contexts.Count(context => context.Response.StatusCode == 200));
            Assert.Equal(7, contexts.Count(context => context.Response.StatusCode == 429));

            // The window lasts 30 s, so the ten requests above always share it. Expiring the key
            // stands in for the window running out.
            Assert.Equal(1, (await first.ExecuteAsync("PEXPIRE", key, "1")).Integer);

            DateTime deadline = DateTime.UtcNow.AddSeconds(10);

            while ((await first.ExecuteAsync("EXISTS", key)).Integer != 0)
            {
                Assert.True(DateTime.UtcNow < deadline, "The expired window key never went away.");
                await Task.Delay(10);
            }

            var afterExpiry = Context(firstServices);

            await middleware.InvokeAsync(afterExpiry);
            Assert.Equal(200, afterExpiry.Response.StatusCode);
        }

        finally
        {
            await first.DeleteAsync(key);
        }
    }

    private static ServiceProvider Services(IRuvioClient client, string prefix)
    {
        var services = new ServiceCollection();

        services.AddSingleton(client);
        services.AddRuvioRateLimiting(options =>
        {
            options.KeyPrefix = prefix;
            options.PartitionKey = _ => "tenant";
        });

        return services.BuildServiceProvider();
    }

    private static HttpContext Context(IServiceProvider provider)
    {
        var context = new DefaultHttpContext { RequestServices = provider };

        context.SetEndpoint(new Endpoint(_ => Task.CompletedTask,
            new EndpointMetadataCollection(new RuvioRateLimitAttribute("live", 3, 30)), "live"));

        return context;
    }
}
