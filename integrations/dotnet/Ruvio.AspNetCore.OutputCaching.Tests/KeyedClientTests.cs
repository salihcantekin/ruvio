using System.Reflection;
using Microsoft.AspNetCore.OutputCaching;
using Microsoft.Extensions.DependencyInjection;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.OutputCaching.Tests;

public class KeyedClientTests
{
    [Fact]
    public async Task ClientKeySelectsTheKeyedClient()
    {
        var keyed = DispatchProxy.Create<IRuvioClient, StoreTests.RejectingClient>();
        var services = new ServiceCollection();

        services.AddKeyedSingleton("pages", keyed);
        services.AddRuvioOutputCache(o => o.ClientKey = "pages");
        await using ServiceProvider provider = services.BuildServiceProvider();
        IOutputCacheStore store = provider.GetRequiredService<IOutputCacheStore>();

        await Assert.ThrowsAsync<IOException>(() => store.GetAsync("k", default).AsTask());
        Assert.Equal(1, ((StoreTests.RejectingClient)(object)keyed).Calls);
    }
}
