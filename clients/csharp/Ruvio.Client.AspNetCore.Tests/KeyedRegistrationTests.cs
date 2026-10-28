using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Ruvio.Client.AspNetCore;
using Xunit;

namespace Ruvio.Client.AspNetCore.Tests;

public class KeyedRegistrationTests
{
    [Fact]
    public void KeyedOptionsDoNotChangeTheDefaultOnes()
    {
        var services = new ServiceCollection();

        services.AddRuvioClient("10.0.0.1", 6379);
        services.AddKeyedRuvioClient("durable", "10.0.0.2", 6390, database: 3);
        using ServiceProvider provider = services.BuildServiceProvider();
        IOptionsMonitor<RuvioClientOptions> monitor = provider.GetRequiredService<IOptionsMonitor<RuvioClientOptions>>();
        RuvioClientOptions keyed = monitor.Get("durable");
        RuvioClientOptions unkeyed = provider.GetRequiredService<IOptions<RuvioClientOptions>>().Value;

        Assert.Equal("10.0.0.2", keyed.Host);
        Assert.Equal(6390, keyed.Port);
        Assert.Equal(3, keyed.Database);
        Assert.Equal("10.0.0.1", unkeyed.Host);
        Assert.Equal(6379, unkeyed.Port);
    }

    [Fact]
    public void KeyedClientBindsAConfigurationSection()
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Host"] = "10.4.4.4",
                ["Port"] = "6391",
            })
            .Build();
        var services = new ServiceCollection();

        services.AddKeyedRuvioClient("ring", configuration);
        using ServiceProvider provider = services.BuildServiceProvider();
        RuvioClientOptions options = provider.GetRequiredService<IOptionsMonitor<RuvioClientOptions>>().Get("ring");

        Assert.Equal("10.4.4.4", options.Host);
        Assert.Equal(6391, options.Port);
    }

    [Fact]
    public void KeyedClientRegistersOnlyKeyedServices()
    {
        var services = new ServiceCollection();

        services.AddKeyedRuvioClient("ring", options => options.Host = "127.0.0.1");

        Assert.All(services.Where(descriptor =>
                descriptor.ServiceType == typeof(IRuvioClient)
                || descriptor.ServiceType == typeof(RuvioClient)
                || descriptor.ServiceType == typeof(IRuvioConnection)
                || descriptor.ServiceType == typeof(RuvioConnection)),
            descriptor => Assert.Equal("ring", descriptor.ServiceKey));
        Assert.Equal(4, services.Count(descriptor => "ring".Equals(descriptor.ServiceKey)));
    }

    [Fact]
    public void KeyedClientRefusesAnEmptyKey()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentException>(() => services.AddKeyedRuvioClient("", options => { }));
        Assert.Throws<ArgumentNullException>(() => services.AddKeyedRuvioClient(null!, options => { }));
    }

    [Fact(Timeout = 15000)]
    public async Task EachKeyOpensItsOwnConnection()
    {
        using var first = new TcpListener(IPAddress.Loopback, 0);
        using var second = new TcpListener(IPAddress.Loopback, 0);

        first.Start();
        second.Start();
        var services = new ServiceCollection();

        services.AddKeyedRuvioClient("a", Endpoint(first));
        services.AddKeyedRuvioClient("b", Endpoint(second));
        await using ServiceProvider provider = services.BuildServiceProvider();
        using var acceptLimit = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        Task<IRuvioClient> resolvingA = Task.Run(() => provider.GetRequiredKeyedService<IRuvioClient>("a"));
        using TcpClient acceptedA = await first.AcceptTcpClientAsync(acceptLimit.Token);
        IRuvioClient a = await resolvingA;
        Task<IRuvioClient> resolvingB = Task.Run(() => provider.GetRequiredKeyedService<IRuvioClient>("b"));
        using TcpClient acceptedB = await second.AcceptTcpClientAsync(acceptLimit.Token);
        IRuvioClient b = await resolvingB;

        Assert.NotSame(a, b);
        Assert.Same(a, provider.GetRequiredKeyedService<RuvioClient>("a"));
        Assert.Same(provider.GetRequiredKeyedService<RuvioConnection>("b"), provider.GetRequiredKeyedService<IRuvioConnection>("b"));
        Assert.Null(provider.GetService<IRuvioClient>());
    }

    private static Action<RuvioClientOptions> Endpoint(TcpListener listener)
    {
        int port = ((IPEndPoint)listener.LocalEndpoint!).Port;

        return options =>
        {
            options.Host = "127.0.0.1";
            options.Port = port;
            options.DiscoverCluster = false;
            options.Reconnect = false;
        };
    }
}
