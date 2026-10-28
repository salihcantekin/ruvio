using System.Globalization;
using System.Reflection;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Xml;
using System.Xml.Linq;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.DataProtection.Tests;

public class RepositoryTests
{
    [Fact]
    public void RegistrationIsLazyAndMissingEncryptionIsRejected()
    {
        var services = new ServiceCollection();
        var client = DispatchProxy.Create<IRuvioClient, RejectingClient>();

        services.AddSingleton(client);
        services.AddDataProtection().PersistKeysToRuvio();
        using var provider = services.BuildServiceProvider();

        Assert.Equal(0, ((RejectingClient)client).Calls);
        Assert.Throws<InvalidOperationException>(() => provider.GetRequiredService<IOptions<KeyManagementOptions>>().Value);
        Assert.Equal(0, ((RejectingClient)client).Calls);
    }

    [Fact]
    public void InvalidConfigurationAndPlaintextSecretsAreRejectedBeforeIo()
    {
        var client = DispatchProxy.Create<IRuvioClient, RejectingClient>();

        Assert.Throws<ArgumentException>(() => new RuvioXmlRepository(client, Options.Create(new RuvioDataProtectionOptions { Key = "" })));

        Assert.Throws<ArgumentOutOfRangeException>(() => new RuvioXmlRepository(client,
            Options.Create(new RuvioDataProtectionOptions { OperationTimeout = TimeSpan.Zero })));

        Assert.Throws<ArgumentOutOfRangeException>(() => new RuvioXmlRepository(client,
            Options.Create(new RuvioDataProtectionOptions { MaxElements = 4096 })));

        var repository = new RuvioXmlRepository(client, Options.Create(new RuvioDataProtectionOptions { MaxElementBytes = 64 }));

        Assert.Throws<ArgumentNullException>(() => repository.StoreElement(null!, "unused"));
        Assert.Throws<ArgumentException>(() => repository.StoreElement(new XElement("entry", new string('x', 65)), "unused"));
        Assert.Throws<InvalidOperationException>(() =>
            repository.StoreElement(XElement.Parse("<masterKey requiresEncryption=\"true\">secret</masterKey>"), "unused"));
        Assert.Equal(0, ((RejectingClient)client).Calls);
    }

    [Fact]
    public void SynchronousContractAvoidsCallersContextPropagatesErrorsAndNeverRetries()
    {
        var client = DispatchProxy.Create<IRuvioClient, RejectingClient>();
        var fake = (RejectingClient)client;
        var repository = new RuvioXmlRepository(client, Options.Create(new RuvioDataProtectionOptions()));
        SynchronizationContext? previous = SynchronizationContext.Current;

        try
        {
            SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());
            Assert.Throws<IOException>(() => repository.StoreElement(new XElement("revocation"), "name"));
            Assert.Equal(1, fake.Calls);
            Assert.Null(fake.Context);
            Assert.True(fake.Token.CanBeCanceled);
            Assert.Throws<IOException>(() => repository.GetAllElements());
            Assert.Equal(2, fake.Calls);
            Assert.Null(fake.Context);
        }

        finally { SynchronizationContext.SetSynchronizationContext(previous); }
    }

    [Fact]
    public void TimeoutCancelsAnUnresponsiveClient()
    {
        var client = DispatchProxy.Create<IRuvioClient, RejectingClient>();

        ((RejectingClient)client).WaitForCancellation = true;
        var repository = new RuvioXmlRepository(client,
            Options.Create(new RuvioDataProtectionOptions { OperationTimeout = TimeSpan.FromMilliseconds(20) }));

        Assert.ThrowsAny<OperationCanceledException>(() => repository.GetAllElements());
        Assert.Equal(1, ((RejectingClient)client).Calls);
    }

    [LiveFact]
    public async Task Require_durable_storage_follows_the_live_servers_profile()
    {
        await using var scope = await Scope.Open();
        string info = await scope.Client.GetInfoAsync("server");
        string? profile = RuvioXmlRepository.DurabilityProfile(info);
        var strict = new RuvioXmlRepository(scope.Client,
            Options.Create(new RuvioDataProtectionOptions { Key = scope.Key, RequireDurableStorage = true }));

        Assert.NotNull(profile);

        if (profile == "memory")
            Assert.Throws<RuvioException>(() => strict.GetAllElements());
        else
            Assert.Empty(strict.GetAllElements());
    }

    [LiveFact]
    public async Task Append_is_immutable_deduplicates_content_and_preserves_revocations_without_ttl()
    {
        await using var scope = await Scope.Open();
        Assert.Empty(scope.Repository.GetAllElements());
        var first = new XElement("key", new XAttribute("id", "first"), "one");

        scope.Repository.StoreElement(first, "same-friendly-name");
        scope.Repository.StoreElement(first, "another-name");
        scope.Repository.StoreElement(new XElement("revocation", "second"), "same-friendly-name");
        var entries = scope.Repository.GetAllElements();

        Assert.Equal(2, entries.Count);
        Assert.Contains(entries, e => XNode.DeepEquals(first, e));
        Assert.Contains(entries, e => e.Name == "revocation");
        Assert.Equal(-1, (await scope.Client.ExecuteAsync("PTTL", scope.Key)).Integer);
        Assert.Equal(2, (await scope.Client.ExecuteAsync("HLEN", scope.Key)).Integer);
    }

    [LiveFact]
    public async Task Concurrent_writers_preserve_all_distinct_elements()
    {
        await using var scope = await Scope.Open();
        await using var secondConnection = await Scope.Connect();
        IRuvioClient second = await secondConnection.GetDefaultDatabaseAsync();
        var other = new RuvioXmlRepository(second, Options.Create(new RuvioDataProtectionOptions { Key = scope.Key }));

        // Each writer blocks a pool thread (IXmlRepository is synchronous), so give the pool room on small CI machines.
        ThreadPool.SetMinThreads(64, 64);
        await Task.WhenAll(Enumerable.Range(0, 20).Select(i => Task.Run(() =>
            (i % 2 == 0 ? scope.Repository : other).StoreElement(new XElement("entry", i), "same-name"))));
        Assert.Equal(20, scope.Repository.GetAllElements().Count);
    }

    [LiveFact]
    public async Task Capacity_corruption_and_expiring_key_are_errors_not_empty_rings()
    {
        await using var scope = await Scope.Open();
        var small = new RuvioXmlRepository(scope.Client,
            Options.Create(new RuvioDataProtectionOptions { Key = scope.Key, MaxElements = 1 }));

        small.StoreElement(new XElement("entry", "one"), "one");
        Assert.Throws<RuvioException>(() => small.StoreElement(new XElement("entry", "two"), "two"));
        Assert.Single(small.GetAllElements());
        await scope.Client.ExecuteAsync("HSET", scope.Key, "manual", "<entry/>");
        Assert.Throws<RuvioException>(() => small.GetAllElements());
        await scope.Client.ExecuteAsync("HDEL", scope.Key, "manual");
        await scope.Client.ExecuteAsync("PEXPIRE", scope.Key, "10000");
        Assert.Throws<RuvioException>(() => scope.Repository.GetAllElements());
        Assert.Throws<RuvioException>(() => scope.Repository.StoreElement(new XElement("entry"), "entry"));
    }

    [LiveFact]
    public async Task Invalid_xml_and_external_entities_are_never_loaded()
    {
        await using var scope = await Scope.Open();
        await scope.Client.ExecuteAsync("HSET", scope.Key, "bad",
            "<!DOCTYPE key [<!ENTITY x SYSTEM \"file:///must-not-be-read\">]><key>&x;</key>");
        Assert.Throws<XmlException>(() => scope.Repository.GetAllElements());
        await scope.Client.ExecuteAsync("HSET", scope.Key, "bad", "<unclosed>");
        Assert.Throws<XmlException>(() => scope.Repository.GetAllElements());
        await scope.Client.DeleteAsync(scope.Key);
        await scope.Client.ExecuteAsync("SET", scope.Key, "wrongtype");
        Assert.Throws<RuvioException>(() => scope.Repository.GetAllElements());
    }

    [LiveFact]
    public async Task Real_data_protection_shares_encrypted_keys_and_separates_applications()
    {
        await using var scope = await Scope.Open();
        await using var secondConnection = await Scope.Connect();
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest("CN=ruvio-test", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddHours(1));
        using var first = Services(scope.Connection, scope.Key, "application", certificate, generate: true);
        string protectedValue = first.GetRequiredService<IDataProtectionProvider>().CreateProtector("cookies").Protect("payload");
        var xml = scope.Repository.GetAllElements();

        Assert.Contains(xml, e => e.Descendants().Any(child => child.Name.LocalName == "encryptedSecret"));
        Assert.DoesNotContain(xml, e => e.Descendants().Any(child => child.Name.LocalName == "masterKey"));
        using var second = Services(secondConnection, scope.Key, "application", certificate, generate: false);

        Assert.Equal("payload", second.GetRequiredService<IDataProtectionProvider>().CreateProtector("cookies").Unprotect(protectedValue));
        using var isolated = Services(secondConnection, scope.Key, "different-application", certificate, generate: false);

        Assert.Throws<CryptographicException>(() =>
            isolated.GetRequiredService<IDataProtectionProvider>().CreateProtector("cookies").Unprotect(protectedValue));

        first.GetRequiredService<IKeyManager>().RevokeAllKeys(DateTimeOffset.UtcNow.AddSeconds(1), "test revocation");
        using var afterRevocation = Services(secondConnection, scope.Key, "application", certificate, generate: false);

        Assert.Throws<CryptographicException>(() =>
            afterRevocation.GetRequiredService<IDataProtectionProvider>().CreateProtector("cookies").Unprotect(protectedValue));
    }

    private static ServiceProvider Services(IRuvioConnection connection, string key, string application,
        X509Certificate2 certificate, bool generate)
    {
        var services = new ServiceCollection();

        services.AddLogging();
        services.AddSingleton(connection);
        services.AddSingleton<IRuvioConnection>(connection);
        services.AddSingleton<IRuvioClient>(_ => connection.GetDefaultDatabaseAsync().GetAwaiter().GetResult());
        var builder = services.AddDataProtection().SetApplicationName(application)
            .PersistKeysToRuvio(o => o.Key = key).ProtectKeysWithCertificate(certificate);

        if (!generate)
            builder.DisableAutomaticKeyGeneration();

        return services.BuildServiceProvider();
    }

    public class RejectingClient : DispatchProxy
    {
        public int Calls { get; private set; }
        public CancellationToken Token { get; private set; }
        public SynchronizationContext? Context { get; private set; }
        public bool WaitForCancellation { get; set; }
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            Calls++;
            Token = args?.OfType<CancellationToken>().LastOrDefault() ?? default;
            Context = SynchronizationContext.Current;
            Type result = targetMethod!.ReturnType.GetGenericArguments()[0];

            object task = WaitForCancellation
                ? typeof(RejectingClient).GetMethod(nameof(Wait), BindingFlags.NonPublic | BindingFlags.Static)!
                    .MakeGenericMethod(result).Invoke(null, [Token])!
                : typeof(Task).GetMethods().Single(method => method.Name == nameof(Task.FromException) && method.IsGenericMethod)
                    .MakeGenericMethod(result).Invoke(null, [new IOException("Ambiguous write.")])!;

            return Activator.CreateInstance(targetMethod.ReturnType, task);
        }

        private static async Task<T> Wait<T>(CancellationToken token)
        {
            await Task.Delay(Timeout.Infinite, token);
            throw new InvalidOperationException("Cancellation was expected.");
        }
    }

    private sealed class Scope : IAsyncDisposable
    {
        public string Key { get; } = "dp-test-" + Guid.NewGuid().ToString("N");

        public RuvioConnection Connection { get; }
        public RuvioClient Client { get; }
        public RuvioXmlRepository Repository { get; }
        private Scope(RuvioConnection connection, RuvioClient client)
        {
            Connection = connection;
            Client = client;
            Repository = new(client, Options.Create(new RuvioDataProtectionOptions { Key = Key }));
        }

        public static async Task<Scope> Open()
        {
            RuvioConnection connection = await Connect();

            return new Scope(connection, await connection.GetDefaultDatabaseAsync());
        }

        public static Task<RuvioConnection> Connect()
        {
            var address = Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR")!.Split(':');

            return RuvioConnection.ConnectAsync(address[0], int.Parse(address[1], CultureInfo.InvariantCulture));
        }

        public async ValueTask DisposeAsync()
        {
            try { await Client.DeleteAsync(Key); }
            finally { await Connection.DisposeAsync(); }
        }
    }
}

public sealed class LiveFactAttribute : FactAttribute
{
    public LiveFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR")))
            Skip = "Set RUVIO_TEST_ADDR to run against an isolated Ruvio server.";

        Timeout = 30000;
    }
}
