using System.Net;
using System.Net.Sockets;
using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Ruvio.Client.AspNetCore;
using Xunit;

namespace Ruvio.Client.AspNetCore.Tests;

public class RegistrationTests
{
    [Fact]
    public void AddRuvioClientKeepsTheConfiguredEndpoint()
    {
        var services = new ServiceCollection();

        services.AddRuvioClient(options =>
        {
            options.Host = "10.0.0.8";
            options.Port = 6380;
            options.Password = "secret";
        });

        using ServiceProvider provider = services.BuildServiceProvider();
        RuvioClientOptions options = provider.GetRequiredService<IOptions<RuvioClientOptions>>().Value;

        Assert.Equal("10.0.0.8", options.Host);
        Assert.Equal(6380, options.Port);
        Assert.Equal("secret", options.Password);
    }

    [Fact]
    public void AddRuvioClientTakesHostPortAndDatabase()
    {
        var services = new ServiceCollection();

        services.AddRuvioClient("10.0.0.9", 6390, database: 4);
        using ServiceProvider provider = services.BuildServiceProvider();
        RuvioClientOptions options = provider.GetRequiredService<IOptions<RuvioClientOptions>>().Value;

        Assert.Equal("10.0.0.9", options.Host);
        Assert.Equal(6390, options.Port);
        Assert.Equal(4, options.Database);
    }

    [Fact]
    public void AddRuvioClientBindsAConfigurationSection()
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Host"] = "10.1.2.3",
                ["Port"] = "6381",
            })
            .Build();
        var services = new ServiceCollection();

        services.AddRuvioClient(configuration);
        using ServiceProvider provider = services.BuildServiceProvider();
        RuvioClientOptions options = provider.GetRequiredService<IOptions<RuvioClientOptions>>().Value;

        Assert.Equal("10.1.2.3", options.Host);
        Assert.Equal(6381, options.Port);
    }

    [Fact]
    public void AddRuvioClientKeepsTlsOffUnlessAskedFor()
    {
        RuvioClientOptions options = Bind(new Dictionary<string, string?>
        {
            ["TlsServerName"] = "cache.internal",
        });

        Assert.Null(options.Tls);
    }

    [Fact]
    public void AddRuvioClientBindsTheTlsSettings()
    {
        string caFile = Path.Combine(Path.GetTempPath(), $"ruvio-ca-{Guid.NewGuid():N}.pem");

        try
        {
            using var key = System.Security.Cryptography.ECDsa.Create();
            var request = new System.Security.Cryptography.X509Certificates.CertificateRequest(
                "CN=test ca",
                key,
                System.Security.Cryptography.HashAlgorithmName.SHA256);
            using var ca = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));

            File.WriteAllText(caFile, ca.ExportCertificatePem());

            RuvioClientOptions options = Bind(new Dictionary<string, string?>
            {
                ["Tls"] = "true",
                ["TlsServerName"] = "cache.internal",
                ["TlsCaFile"] = caFile,
                ["TlsInsecureSkipVerify"] = "false",
            });

            Assert.NotNull(options.Tls);
            Assert.Equal("cache.internal", options.Tls.ServerName);
            Assert.False(options.Tls.InsecureSkipVerify);
            Assert.Single(options.Tls.TrustedCertificates!);
            Assert.Null(options.Tls.ClientCertificates);
        }
        finally
        {
            File.Delete(caFile);
        }
    }

    private static RuvioClientOptions Bind(Dictionary<string, string?> values)
    {
        IConfiguration configuration = new ConfigurationBuilder().AddInMemoryCollection(values).Build();
        var services = new ServiceCollection();

        services.AddRuvioClient(configuration);
        using ServiceProvider provider = services.BuildServiceProvider();

        return provider.GetRequiredService<IOptions<RuvioClientOptions>>().Value;
    }

    [Fact]
    public void AddRuvioClientBindsTheSeedList()
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Seeds:0"] = "10.1.2.3:6379",
                ["Seeds:1"] = "[::1]:6380",
            })
            .Build();
        var services = new ServiceCollection();

        services.AddRuvioClient(configuration);
        using ServiceProvider provider = services.BuildServiceProvider();
        RuvioClientOptions options = provider.GetRequiredService<IOptions<RuvioClientOptions>>().Value;

        Assert.Equal(["10.1.2.3:6379", "[::1]:6380"], options.Seeds);
    }

    [Fact]
    public void AddRuvioClientBindsReconnectSettings()
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Host"] = "10.1.2.3",
                ["Port"] = "6381",
                ["Reconnect"] = "false",
                ["MaxReconnectAttempts"] = "3",
                ["ReconnectBaseDelay"] = "00:00:00.050",
                ["ReconnectMaxDelay"] = "00:00:01",
            })
            .Build();
        var services = new ServiceCollection();

        services.AddRuvioClient(configuration);
        using ServiceProvider provider = services.BuildServiceProvider();
        RuvioClientOptions options = provider.GetRequiredService<IOptions<RuvioClientOptions>>().Value;

        Assert.False(options.Reconnect);
        Assert.Equal(3, options.MaxReconnectAttempts);
        Assert.Equal(TimeSpan.FromMilliseconds(50), options.ReconnectBaseDelay);
        Assert.Equal(TimeSpan.FromSeconds(1), options.ReconnectMaxDelay);
    }

    [Fact(Timeout = 15000)]
    public async Task ResolvingTheClientOpensOneOwnedConnectionAndSendsNothing()
    {
        using var server = new TcpListener(IPAddress.Loopback, 0);

        server.Start();
        int port = ((IPEndPoint)server.LocalEndpoint!).Port;
        var services = new ServiceCollection();

        services.AddRuvioClient(options =>
        {
            options.Host = "127.0.0.1";
            options.Port = port;
            options.DiscoverCluster = false;
        });

        await using ServiceProvider provider = services.BuildServiceProvider();
        Task<RuvioClient> resolving = Task.Run(() => provider.GetRequiredService<RuvioClient>());
        using var acceptLimit = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        using TcpClient accepted = await server.AcceptTcpClientAsync(acceptLimit.Token);
        RuvioClient client = await resolving;

        await Task.Delay(50);
        Assert.Equal(0, accepted.Available);

        byte[] expected = Encoding.ASCII.GetBytes("*1\r\n$4\r\nPING\r\n");
        NetworkStream stream = accepted.GetStream();
        ValueTask<string> ping = client.PingAsync();
        var got = new byte[expected.Length];
        using var readLimit = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await stream.ReadExactlyAsync(got, readLimit.Token);
        Assert.Equal(expected, got);
        await stream.WriteAsync(Encoding.ASCII.GetBytes("+PONG\r\n"));
        Assert.Equal("PONG", await ping);
    }

    [Fact(Timeout = 15000)]
    public async Task ConfigureClientRunsAfterTheSocketOpens()
    {
        using var server = new TcpListener(IPAddress.Loopback, 0);

        server.Start();
        int port = ((IPEndPoint)server.LocalEndpoint!).Port;
        var wired = false;
        var services = new ServiceCollection();

        services.AddRuvioClient(options =>
        {
            options.Host = "127.0.0.1";
            options.Port = port;
            options.DiscoverCluster = false;
            options.Reconnect = false;
        }, connection =>
        {
            wired = connection.IsConnected;
            connection.ConnectionLost += (_, _) => { };
        });

        await using ServiceProvider provider = services.BuildServiceProvider();
        Task<RuvioConnection> resolving = Task.Run(() => provider.GetRequiredService<RuvioConnection>());
        using var acceptLimit = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        using TcpClient accepted = await server.AcceptTcpClientAsync(acceptLimit.Token);
        RuvioConnection connection = await resolving;
        RuvioClient client = (RuvioClient)provider.GetRequiredService<IRuvioClient>();

        Assert.True(wired);
        Assert.False(provider.GetRequiredService<IOptions<RuvioClientOptions>>().Value.Reconnect);
        Assert.Same(connection, provider.GetRequiredService<IRuvioConnection>());
        Assert.Same(client, provider.GetRequiredService<RuvioClient>());
        Assert.Same(client, provider.GetRequiredService<IRuvioClient>());
        Assert.NotSame(client, connection);
    }

    [Fact]
    public void AddRuvioClientRegistersTheConnectionOwnerAndDatabaseClient()
    {
        var services = new ServiceCollection();

        services.AddRuvioClient(options => options.Host = "127.0.0.1");

        Assert.Single(services, descriptor => descriptor.ServiceType == typeof(RuvioClient));
        Assert.Single(services, descriptor => descriptor.ServiceType == typeof(IRuvioClient));
        Assert.Single(services, descriptor => descriptor.ServiceType == typeof(IRuvioConnection));
    }
}
