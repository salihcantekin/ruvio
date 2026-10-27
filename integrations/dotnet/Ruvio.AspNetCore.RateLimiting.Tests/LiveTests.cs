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

    [Fact]
    public async Task NamedPolicyLimitsAgainstARealServer()
    {
        string? address = Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR");

        if (string.IsNullOrWhiteSpace(address))
            return;

        var uri = new Uri("tcp://" + address);

        await using var connection = await RuvioConnection.ConnectAsync(uri.Host, uri.Port);
        IRuvioClient client = await connection.GetDefaultDatabaseAsync();
        string prefix = "test:rate:" + Guid.NewGuid().ToString("N") + ":";
        string key = prefix + Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes("5:named" + "livetenant")));
        var collection = new ServiceCollection();

        collection.AddSingleton(client);
        collection.AddRuvioRateLimiting(options =>
        {
            options.KeyPrefix = prefix;
            options.PartitionKey = _ => "livetenant";
            options.AddPolicy("named", 2, TimeSpan.FromSeconds(30));
        });

        using var services = collection.BuildServiceProvider();
        var middleware = MiddlewareTests.Create(_ => Task.CompletedTask, services);

        try
        {
            var statuses = new List<int>();

            for (int i = 0; i < 4; i++)
            {
                var context = new DefaultHttpContext { RequestServices = services };

                context.SetEndpoint(new Endpoint(_ => Task.CompletedTask,
                    new EndpointMetadataCollection(new RuvioRateLimitAttribute("named")), "named"));
                await middleware.InvokeAsync(context);
                statuses.Add(context.Response.StatusCode);
            }

            Assert.Equal(new[] { 200, 200, 429, 429 }, statuses);
        }

        finally
        {
            await client.DeleteAsync(key);
        }
    }

    [Fact]
    public async Task ChainedLimitsConcurrencyAndOnRejectedWorkAgainstARealServer()
    {
        string? address = Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR");

        if (string.IsNullOrWhiteSpace(address))
            return;

        var uri = new Uri("tcp://" + address);

        await using var connection = await RuvioConnection.ConnectAsync(uri.Host, uri.Port);
        IRuvioClient client = await connection.GetDefaultDatabaseAsync();
        string prefix = "test:rate:" + Guid.NewGuid().ToString("N") + ":";
        var collection = new ServiceCollection();
        var rejections = new List<RuvioRejectionReason>();

        collection.AddSingleton(client);
        collection.AddRuvioRateLimiting(options =>
        {
            options.KeyPrefix = prefix;
            options.PartitionKey = _ => "tenant";
            options.OnRejected = (_, rejection) =>
            {
                rejections.Add(rejection.Reason);

                return ValueTask.CompletedTask;
            };
            options.AddPolicy("chained", p => p
                .Limit(3, TimeSpan.FromSeconds(30), _ => "ip-a")
                .Limit(2, TimeSpan.FromSeconds(30), _ => "tenant-a", RuvioRateLimitAlgorithm.SlidingWindow));
            options.AddPolicy("slots", p => p.Concurrency(1, TimeSpan.FromSeconds(20)));
        });

        using var services = collection.BuildServiceProvider();
        var release = new TaskCompletionSource();
        var entered = new TaskCompletionSource();
        var holder = MiddlewareTests.Create(async _ => { entered.SetResult(); await release.Task; }, services);
        var quick = MiddlewareTests.Create(_ => Task.CompletedTask, services);

        // The second limit (2 per window) refuses the third call; the first limit has counted it.
        var statuses = new List<int>();

        for (int i = 0; i < 3; i++)
        {
            var context = Context(services, "chained");

            await quick.InvokeAsync(context);
            statuses.Add(context.Response.StatusCode);
        }

        Assert.Equal(new[] { 200, 200, 429 }, statuses);
        Assert.Equal(new[] { RuvioRejectionReason.RateLimit }, rejections);

        // One slot: a second request is refused while the first runs, and admitted after it finishes.
        var running = holder.InvokeAsync(Context(services, "slots"));

        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        var refused = Context(services, "slots");

        await quick.InvokeAsync(refused);
        Assert.Equal(429, refused.Response.StatusCode);
        Assert.Equal(RuvioRejectionReason.Concurrency, rejections[^1]);

        release.SetResult();
        await running.WaitAsync(TimeSpan.FromSeconds(10));

        var after = Context(services, "slots");

        await quick.InvokeAsync(after);
        Assert.Equal(200, after.Response.StatusCode);
    }

    private static HttpContext Context(IServiceProvider provider, string policy)
    {
        var context = new DefaultHttpContext { RequestServices = provider };

        context.SetEndpoint(new Endpoint(_ => Task.CompletedTask,
            new EndpointMetadataCollection(new RuvioRateLimitAttribute(policy)), policy));

        return context;
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
