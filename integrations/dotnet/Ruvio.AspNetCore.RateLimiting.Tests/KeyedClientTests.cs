using System.Reflection;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.RateLimiting.Tests;

public class KeyedClientTests
{
    [Fact]
    public async Task ClientKeySelectsTheKeyedClientPerRequest()
    {
        var keyed = DispatchProxy.Create<IRuvioClient, MiddlewareTests.Stub>();
        var unkeyed = DispatchProxy.Create<IRuvioClient, MiddlewareTests.Stub>();
        var services = new ServiceCollection();

        services.AddSingleton(unkeyed);
        services.AddKeyedSingleton("limits", keyed);
        services.AddRuvioRateLimiting(options =>
        {
            options.ClientKey = "limits";
            options.PartitionKey = static _ => "tenant";
        });
        await using ServiceProvider provider = services.BuildServiceProvider();
        var context = new DefaultHttpContext { RequestServices = provider };

        context.SetEndpoint(new Endpoint(_ => Task.CompletedTask,
            new EndpointMetadataCollection(new RuvioRateLimitAttribute("test", 5, 60)), "test"));

        await MiddlewareTests.Create(static _ => Task.CompletedTask, provider).InvokeAsync(context);

        Assert.Equal(1, ((MiddlewareTests.Stub)(object)keyed).Calls);
        Assert.Equal(0, ((MiddlewareTests.Stub)(object)unkeyed).Calls);
    }
}
