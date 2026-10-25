using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

/// <summary>
/// A test that needs the TLS servers <c>scripts/run_with_servers.sh</c> starts: <c>RUVIO_TLS_ADDR</c>,
/// <c>RUVIO_TLS_CLIENT_AUTH_ADDR</c> and the PEM files <c>RUVIO_TLS_CA</c>, <c>RUVIO_TLS_CLIENT_CERT</c> and
/// <c>RUVIO_TLS_CLIENT_KEY</c>. Skipped when they are not set.
/// </summary>
public sealed class TlsFactAttribute : FactAttribute
{
    public TlsFactAttribute()
    {
        if (TlsServer.Address is null || TlsServer.CaFile is null)
        {
            Skip = "RUVIO_TLS_ADDR and RUVIO_TLS_CA are not set";
        }
    }
}

internal static class TlsServer
{
    public static (string Host, int Port)? Address => Split("RUVIO_TLS_ADDR");

    public static (string Host, int Port)? ClientAuthAddress => Split("RUVIO_TLS_CLIENT_AUTH_ADDR");

    public static string? CaFile => Environment.GetEnvironmentVariable("RUVIO_TLS_CA");

    public static X509Certificate2Collection Ca()
    {
        var roots = new X509Certificate2Collection();

        roots.ImportFromPemFile(CaFile!);

        return roots;
    }

    public static X509CertificateCollection ClientCertificate()
    {
        using X509Certificate2 pem = X509Certificate2.CreateFromPemFile(
            Environment.GetEnvironmentVariable("RUVIO_TLS_CLIENT_CERT")!,
            Environment.GetEnvironmentVariable("RUVIO_TLS_CLIENT_KEY")!);

        // Windows needs the key in a persisted form for the handshake; the round trip gives it.
        return [new X509Certificate2(pem.Export(X509ContentType.Pfx))];
    }

    public static RuvioClientOptions Options((string Host, int Port) address, RuvioTlsOptions? tls) => new()
    {
        Host = address.Host,
        Port = address.Port,
        DiscoverCluster = false,
        CommandTimeout = TimeSpan.FromSeconds(10),
        Tls = tls,
    };

    private static (string Host, int Port)? Split(string variable)
    {
        string? value = Environment.GetEnvironmentVariable(variable);
        int colon = value?.LastIndexOf(':') ?? -1;

        return colon > 0 && int.TryParse(value![(colon + 1)..], out int port) ? (value[..colon], port) : null;
    }
}

/// <summary>TLS against live servers: verification on by default, a private CA, mutual TLS and the explicit opt-out.</summary>
public sealed class TlsLiveTests
{
    [TlsFact(Timeout = 30000)]
    public async Task A_verified_connection_runs_commands()
    {
        var tls = new RuvioTlsOptions { TrustedCertificates = TlsServer.Ca() };
        await using RuvioConnection connection = await RuvioConnection.ConnectAsync(TlsServer.Options(TlsServer.Address!.Value, tls));
        RuvioClient db = await connection.GetDefaultDatabaseAsync();

        Assert.True(await db.SetAsync("tls:key", "value"));
        Assert.Equal("value", await db.GetStringAsync("tls:key"));

        string big = new('x', 300_000);

        Assert.True(await db.SetAsync("tls:big", big));
        Assert.Equal(big, await db.GetStringAsync("tls:big"));
    }

    [TlsFact(Timeout = 30000)]
    public async Task A_subscriber_socket_runs_over_tls()
    {
        var tls = new RuvioTlsOptions { TrustedCertificates = TlsServer.Ca() };
        await using RuvioConnection connection = await RuvioConnection.ConnectAsync(TlsServer.Options(TlsServer.Address!.Value, tls));
        RuvioClient db = await connection.GetDefaultDatabaseAsync();
        var got = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        await db.SubscribeAsync("tls-ch", message => got.TrySetResult(message.Payload));
        Assert.True(await db.PublishAsync("tls-ch", "hi") >= 1);
        Assert.Equal("hi", await got.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [TlsFact(Timeout = 30000)]
    public async Task An_untrusted_certificate_is_refused()
    {
        // The system store does not know the test CA.
        await Assert.ThrowsAsync<AuthenticationException>(async () =>
            await RuvioConnection.ConnectAsync(TlsServer.Options(TlsServer.Address!.Value, new RuvioTlsOptions())));
    }

    [TlsFact(Timeout = 30000)]
    public async Task A_name_the_certificate_lacks_is_refused()
    {
        var tls = new RuvioTlsOptions { TrustedCertificates = TlsServer.Ca(), ServerName = "elsewhere.example" };

        await Assert.ThrowsAsync<AuthenticationException>(async () =>
            await RuvioConnection.ConnectAsync(TlsServer.Options(TlsServer.Address!.Value, tls)));
    }

    [TlsFact(Timeout = 30000)]
    public async Task A_dns_name_can_be_checked_while_connecting_by_address()
    {
        var tls = new RuvioTlsOptions { TrustedCertificates = TlsServer.Ca(), ServerName = "localhost" };
        await using RuvioConnection connection = await RuvioConnection.ConnectAsync(TlsServer.Options(TlsServer.Address!.Value, tls));

        Assert.Equal("PONG", await (await connection.GetDefaultDatabaseAsync()).PingAsync());
    }

    [TlsFact(Timeout = 30000)]
    public async Task Skipping_verification_is_an_explicit_choice()
    {
        var tls = new RuvioTlsOptions { InsecureSkipVerify = true };
        await using RuvioConnection connection = await RuvioConnection.ConnectAsync(TlsServer.Options(TlsServer.Address!.Value, tls));

        Assert.Equal("hi", await (await connection.GetDefaultDatabaseAsync()).PingAsync("hi"));
    }

    [TlsFact(Timeout = 30000)]
    public async Task A_server_that_wants_a_client_certificate_gets_one()
    {
        var tls = new RuvioTlsOptions { TrustedCertificates = TlsServer.Ca(), ClientCertificates = TlsServer.ClientCertificate() };
        await using RuvioConnection connection = await RuvioConnection.ConnectAsync(TlsServer.Options(TlsServer.ClientAuthAddress!.Value, tls));

        Assert.Equal("mtls", await (await connection.GetDefaultDatabaseAsync()).PingAsync("mtls"));
    }

    [TlsFact(Timeout = 30000)]
    public async Task A_missing_client_certificate_is_refused()
    {
        var tls = new RuvioTlsOptions { TrustedCertificates = TlsServer.Ca() };

        // With TLS 1.3 the refusal can arrive with the first read, still inside the connect (CLUSTER SLOTS is off here, so PING).
        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using RuvioConnection connection = await RuvioConnection.ConnectAsync(TlsServer.Options(TlsServer.ClientAuthAddress!.Value, tls));

            await (await connection.GetDefaultDatabaseAsync()).PingAsync();
        });
    }

    [TlsFact(Timeout = 30000)]
    public async Task Plain_tcp_to_a_tls_port_does_not_work()
    {
        var options = TlsServer.Options(TlsServer.Address!.Value, tls: null);

        options.CommandTimeout = TimeSpan.FromSeconds(2);
        options.Reconnect = false;

        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using RuvioConnection connection = await RuvioConnection.ConnectAsync(options);

            await (await connection.GetDefaultDatabaseAsync()).PingAsync();
        });
    }
}
