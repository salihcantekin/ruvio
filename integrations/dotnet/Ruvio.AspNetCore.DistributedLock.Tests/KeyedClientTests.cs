using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.DistributedLock.Tests;

public class KeyedClientTests
{
    [Fact(Timeout = 30000)]
    public async Task ClientKeySelectsTheKeyedClient()
    {
        var keyed = DispatchProxy.Create<IRuvioClient, OfflineLockTests.ScriptClient>();
        var services = new ServiceCollection();

        services.AddKeyedSingleton("locks", keyed);
        services.AddRuvioDistributedLock(o => o.ClientKey = "locks");
        await using ServiceProvider provider = services.BuildServiceProvider();
        RuvioLockProvider locks = provider.GetRequiredService<RuvioLockProvider>();

        await using IRuvioLock? held = await locks.TryAcquireAsync("job", TimeSpan.FromSeconds(5));

        Assert.NotNull(held);
        Assert.Equal("SET", ((OfflineLockTests.ScriptClient)(object)keyed).Sent[0][0]);
    }

    [Fact]
    public void WithoutClientKeyTheUnkeyedClientIsRequired()
    {
        var services = new ServiceCollection();

        services.AddKeyedSingleton("locks", DispatchProxy.Create<IRuvioClient, OfflineLockTests.ScriptClient>());
        services.AddRuvioDistributedLock();
        using ServiceProvider provider = services.BuildServiceProvider();

        Assert.Throws<InvalidOperationException>(() => provider.GetRequiredService<RuvioLockProvider>());
    }
}
