using System.Reflection;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Extensions.Caching.Tests;

public class KeyedClientTests
{
    [Fact]
    public void ClientKeySelectsTheKeyedClient()
    {
        var keyed = DispatchProxy.Create<IRuvioClient, ValidationTests.RejectingClient>();
        var services = new ServiceCollection();

        services.AddKeyedSingleton("cache", keyed);
        services.AddRuvioDistributedCache(o => o.ClientKey = "cache");
        using ServiceProvider provider = services.BuildServiceProvider();
        IDistributedCache cache = provider.GetRequiredService<IDistributedCache>();

        Assert.Throws<IOException>(() => cache.Get("k"));
        Assert.Equal(1, ((ValidationTests.RejectingClient)(object)keyed).Calls);
    }

    [Fact]
    public void WithoutClientKeyTheUnkeyedClientIsUsed()
    {
        var keyed = DispatchProxy.Create<IRuvioClient, ValidationTests.RejectingClient>();
        var services = new ServiceCollection();

        services.AddKeyedSingleton("cache", keyed);
        services.AddRuvioDistributedCache();
        using ServiceProvider provider = services.BuildServiceProvider();

        Assert.Throws<InvalidOperationException>(() => provider.GetRequiredService<IDistributedCache>());
        Assert.Equal(0, ((ValidationTests.RejectingClient)(object)keyed).Calls);
    }
}
