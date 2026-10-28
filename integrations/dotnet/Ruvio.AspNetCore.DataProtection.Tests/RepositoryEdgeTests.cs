using System.Reflection;
using System.Text;
using System.Xml;
using System.Xml.Linq;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Microsoft.AspNetCore.DataProtection.XmlEncryption;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.DataProtection.Tests;

public class RepositoryEdgeTests
{
    public class HashClient : DispatchProxy
    {
        public long Ttl { get; set; } = -1;
        public Dictionary<string, string> Fields { get; set; } = [];
        public bool Exists { get; set; }
        public long Length { get; set; }
        public long Added { get; set; } = 1;
        public string Info { get; set; } = "# Server\r\nruvio_durability:everysec\r\n";
        public bool RefuseInfo { get; set; }
        public List<string> Calls { get; } = [];

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            string name = targetMethod!.Name;

            Calls.Add(name);

            if (name == "GetInfoAsync" && RefuseInfo)
                return new ValueTask<string>(Task.FromException<string>(new RuvioException("NOPERM", isServerReply: true)));

            object? value = name switch
            {
                "GetTimeToLiveMillisecondsAsync" => Ttl,
                "GetHashAllAsync" => (IReadOnlyDictionary<string, string>)Fields,
                "HashFieldExistsAsync" => Exists,
                "GetHashLengthAsync" => Length,
                "SetHashFieldsAsync" => Added,
                "GetInfoAsync" => Info,
                _ => throw new NotSupportedException(name),
            };

            return Activator.CreateInstance(targetMethod.ReturnType, value);
        }
    }

    private static (RuvioXmlRepository Repository, HashClient Client) Make(Action<RuvioDataProtectionOptions>? configure = null)
    {
        var client = DispatchProxy.Create<IRuvioClient, HashClient>();
        var options = new RuvioDataProtectionOptions { RequireEncryption = false };

        configure?.Invoke(options);

        return (new RuvioXmlRepository(client, Options.Create(options)), (HashClient)(object)client);
    }

    [Fact]
    public void OptionsRejectEveryOutOfRangeValue()
    {
        var client = DispatchProxy.Create<IRuvioClient, HashClient>();

        void Build(Action<RuvioDataProtectionOptions> configure)
        {
            var options = new RuvioDataProtectionOptions();

            configure(options);
            _ = new RuvioXmlRepository(client, Options.Create(options));
        }

        Assert.Throws<ArgumentException>(() => Build(o => o.Key = "   "));
        Assert.Throws<ArgumentException>(() => Build(o => o.Key = null!));
        Assert.Throws<ArgumentException>(() => Build(o => o.Key = new string('k', 1025)));
        Build(o => o.Key = new string('k', 1024));
        Assert.ThrowsAny<EncoderFallbackException>(() => Build(o => o.Key = "bad\ud800key"));
        Assert.Throws<ArgumentOutOfRangeException>(() => Build(o => o.OperationTimeout = TimeSpan.FromTicks(1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => Build(o => o.OperationTimeout = TimeSpan.FromMinutes(5) + TimeSpan.FromTicks(1)));
        Build(o => o.OperationTimeout = TimeSpan.FromMilliseconds(1));
        Build(o => o.OperationTimeout = TimeSpan.FromMinutes(5));
        Assert.Throws<ArgumentOutOfRangeException>(() => Build(o => o.MaxElementBytes = 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => Build(o => o.MaxElementBytes = 1024 * 1024 + 1));
        Build(o => { o.MaxElementBytes = 1024 * 1024; o.MaxElements = 8; });
        Assert.Throws<ArgumentOutOfRangeException>(() => Build(o => { o.MaxElementBytes = 1024 * 1024; o.MaxElements = 9; }));
        Assert.Throws<ArgumentOutOfRangeException>(() => Build(o => o.MaxElements = 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => Build(o => o.MaxElements = 4097));
        Assert.Throws<ArgumentNullException>(() => new RuvioXmlRepository(null!, Options.Create(new RuvioDataProtectionOptions())));
        Assert.Throws<ArgumentNullException>(() => new RuvioXmlRepository(client, null!));
    }

    [Fact]
    public void ReadingRejectsAnExpiringOversizedOrHostileKeyRing()
    {
        (RuvioXmlRepository repository, HashClient client) = Make(o => { o.MaxElements = 2; o.MaxElementBytes = 64; });

        client.Ttl = 0;
        Assert.Contains("expire", Assert.Throws<RuvioException>(() => repository.GetAllElements()).Message);
        client.Ttl = 5000;
        Assert.Throws<RuvioException>(() => repository.GetAllElements());
        client.Ttl = -2;
        Assert.Empty(repository.GetAllElements());

        client.Fields = new() { ["a"] = "<a/>", ["b"] = "<b/>", ["c"] = "<c/>" };
        Assert.Contains("MaxElements", Assert.Throws<RuvioException>(() => repository.GetAllElements()).Message);

        client.Fields = new() { ["a"] = "<a>" + new string('x', 70) + "</a>" };
        Assert.Contains("MaxElementBytes", Assert.Throws<RuvioException>(() => repository.GetAllElements()).Message);

        client.Fields = new() { ["a"] = "<!DOCTYPE a [<!ENTITY x \"y\">]><a>&x;</a>" };
        Assert.ThrowsAny<XmlException>(() => repository.GetAllElements());

        client.Fields = new() { ["a"] = "<not-xml" };
        Assert.ThrowsAny<XmlException>(() => repository.GetAllElements());

        client.Fields = new() { ["a"] = "<a/>", ["b"] = "<b x=\"1\"/>" };
        Assert.Equal(["a", "b"], repository.GetAllElements().Select(e => e.Name.LocalName).Order());
    }

    [Fact]
    public void WritingChecksExpiryDuplicatesCapacityAndTheServerReply()
    {
        (RuvioXmlRepository repository, HashClient client) = Make(o => o.MaxElements = 2);
        var element = new XElement("key", "v");

        client.Ttl = 10;
        Assert.Contains("expire", Assert.Throws<RuvioException>(() => repository.StoreElement(element, "n")).Message);
        Assert.DoesNotContain("SetHashFieldsAsync", client.Calls);

        client.Ttl = -1;
        client.Exists = true;
        client.Calls.Clear();
        repository.StoreElement(element, "n");
        Assert.DoesNotContain("SetHashFieldsAsync", client.Calls);
        Assert.DoesNotContain("GetHashLengthAsync", client.Calls);

        client.Exists = false;
        client.Length = 2;
        Assert.Contains("MaxElements", Assert.Throws<RuvioException>(() => repository.StoreElement(element, "n")).Message);

        client.Length = 1;
        client.Added = 1;
        repository.StoreElement(element, "n");
        client.Added = 0;
        repository.StoreElement(element, "n");
        client.Added = 2;
        Assert.Contains("Invalid", Assert.Throws<RuvioException>(() => repository.StoreElement(element, "n")).Message);
        client.Added = -1;
        Assert.Throws<RuvioException>(() => repository.StoreElement(element, "n"));
    }

    [Fact]
    public void EncryptionIsEnforcedOnlyForElementsThatDeclareIt()
    {
        (RuvioXmlRepository strict, HashClient client) = Make(o => o.RequireEncryption = true);

        Assert.Throws<InvalidOperationException>(() =>
            strict.StoreElement(XElement.Parse("<outer><inner requiresEncryption=\"TRUE\"/></outer>"), "n"));
        strict.StoreElement(XElement.Parse("<outer><inner requiresEncryption=\"false\"/></outer>"), "n");
        strict.StoreElement(XElement.Parse("<outer other=\"true\"/>"), "n");
        Assert.Contains("SetHashFieldsAsync", client.Calls);

        (RuvioXmlRepository lax, _) = Make(o => o.RequireEncryption = false);

        lax.StoreElement(XElement.Parse("<k requiresEncryption=\"true\"/>"), "n");
    }

    [Fact]
    public void PersistKeysValidatesArgumentsAndWiresTheRepository()
    {
        var services = new ServiceCollection();
        IDataProtectionBuilder builder = services.AddDataProtection();

        Assert.Throws<ArgumentNullException>(() => ((IDataProtectionBuilder)null!).PersistKeysToRuvio());
        Assert.Throws<ArgumentNullException>(() => builder.PersistKeysToRuvio((Func<IServiceProvider, IRuvioConnection>)null!));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.PersistKeysToRuvio(_ => null!, o => o.MaxElements = 0));

        var client = DispatchProxy.Create<IRuvioClient, HashClient>();
        var connection = DispatchProxy.Create<IRuvioConnection, ConnectionStub>();

        ((ConnectionStub)(object)connection).Client = client;
        builder.PersistKeysToRuvio(_ => connection, o => o.RequireEncryption = false);
        using ServiceProvider provider = services.BuildServiceProvider();
        KeyManagementOptions keys = provider.GetRequiredService<IOptions<KeyManagementOptions>>().Value;

        Assert.IsType<RuvioXmlRepository>(keys.XmlRepository);
    }

    [Fact]
    public void AnExistingEncryptorSatisfiesTheEncryptionRequirement()
    {
        var services = new ServiceCollection();
        var client = DispatchProxy.Create<IRuvioClient, HashClient>();
        var connection = DispatchProxy.Create<IRuvioConnection, ConnectionStub>();

        ((ConnectionStub)(object)connection).Client = client;
        services.AddDataProtection().PersistKeysToRuvio(_ => connection);
        services.Configure<KeyManagementOptions>(o => o.XmlEncryptor = new NullXmlEncryptor());
        using ServiceProvider provider = services.BuildServiceProvider();

        Assert.IsType<RuvioXmlRepository>(provider.GetRequiredService<IOptions<KeyManagementOptions>>().Value.XmlRepository);
    }

    public class ConnectionStub : DispatchProxy
    {
        public IRuvioClient? Client { get; set; }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod!.Name == nameof(IRuvioConnection.GetDefaultDatabaseAsync))
            {
                return Task.FromResult(Client!);
            }

            throw new NotSupportedException(targetMethod.Name);
        }
    }
}
