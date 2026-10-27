using System.Reflection;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.DataProtection.Tests;

public class KeyedClientTests
{
    [Fact]
    public void ClientKeySelectsTheKeyedConnection()
    {
        var client = DispatchProxy.Create<IRuvioClient, RepositoryEdgeTests.HashClient>();
        var connection = DispatchProxy.Create<IRuvioConnection, RepositoryEdgeTests.ConnectionStub>();
        var services = new ServiceCollection();

        ((RepositoryEdgeTests.ConnectionStub)(object)connection).Client = client;
        services.AddKeyedSingleton("key-ring", connection);
        services.AddDataProtection().PersistKeysToRuvio(o =>
        {
            o.ClientKey = "key-ring";
            o.RequireEncryption = false;
        });
        using ServiceProvider provider = services.BuildServiceProvider();
        var repository = Assert.IsType<RuvioXmlRepository>(provider.GetRequiredService<IOptions<KeyManagementOptions>>().Value.XmlRepository);

        Assert.Empty(repository.GetAllElements());
        Assert.Contains("GetHashAllAsync", ((RepositoryEdgeTests.HashClient)(object)client).Calls);
    }

    [Fact]
    public void WithoutClientKeyTheUnkeyedConnectionIsRequired()
    {
        var services = new ServiceCollection();

        services.AddKeyedSingleton("key-ring", DispatchProxy.Create<IRuvioConnection, RepositoryEdgeTests.ConnectionStub>());
        services.AddDataProtection().PersistKeysToRuvio(o => o.RequireEncryption = false);
        using ServiceProvider provider = services.BuildServiceProvider();

        Assert.Throws<InvalidOperationException>(() => provider.GetRequiredService<IOptions<KeyManagementOptions>>().Value);
    }
}
