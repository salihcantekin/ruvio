using System.Reflection;
using System.Xml.Linq;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.DataProtection.Tests;

public class DurabilityTests
{
    private sealed class Warnings : ILogger<RuvioXmlRepository>
    {
        public List<string> Messages { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            Assert.Equal(LogLevel.Warning, logLevel);
            Messages.Add(formatter(state, exception));
        }
    }

    private static (RuvioXmlRepository Repository, RepositoryEdgeTests.HashClient Client, Warnings Log) Make(
        string? info, bool require = false, bool refuse = false)
    {
        var client = DispatchProxy.Create<IRuvioClient, RepositoryEdgeTests.HashClient>();
        var fake = (RepositoryEdgeTests.HashClient)(object)client;
        var log = new Warnings();

        fake.RefuseInfo = refuse;

        if (info is not null)
            fake.Info = info;

        var options = new RuvioDataProtectionOptions { RequireEncryption = false, RequireDurableStorage = require };

        return (new RuvioXmlRepository(client, Options.Create(options), log), fake, log);
    }

    [Fact]
    public void AMemoryOnlyServerIsReportedOnceOnTheFirstAccess()
    {
        var (repository, client, log) = Make("# Server\r\nruvio_version:1\r\nruvio_durability:memory\r\nrun_id:x\r\n");

        repository.GetAllElements();
        repository.StoreElement(new XElement("revocation"), "r");
        repository.GetAllElements();

        Assert.Single(log.Messages);
        Assert.Contains("memory", log.Messages[0]);
        Assert.Single(client.Calls, "GetInfoAsync");
        Assert.Equal("GetInfoAsync", client.Calls[0]);
    }

    [Fact]
    public void ADurableServerLogsNothing()
    {
        var (repository, _, log) = Make(null, require: true);

        repository.GetAllElements();
        repository.StoreElement(new XElement("revocation"), "r");

        Assert.Empty(log.Messages);
    }

    [Fact]
    public void RequireDurableStorageRefusesAMemoryOnlyServerBeforeTouchingTheRing()
    {
        var (repository, client, _) = Make("ruvio_durability:memory\r\n", require: true);

        Assert.Throws<RuvioException>(() => repository.GetAllElements());
        Assert.Throws<RuvioException>(() => repository.StoreElement(new XElement("revocation"), "r"));
        Assert.DoesNotContain("GetHashAllAsync", client.Calls);
        Assert.DoesNotContain("SetHashFieldsAsync", client.Calls);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void AnUnknownProfileWarnsOrIsRefused(bool refuse)
    {
        var (repository, _, log) = Make(refuse ? null : "# Server\r\nredis_version:7\r\n", refuse: refuse);

        repository.GetAllElements();
        Assert.Single(log.Messages);
        Assert.Contains("Could not read", log.Messages[0]);

        var (strict, _, _) = Make(refuse ? null : "# Server\r\n", require: true, refuse: refuse);

        Assert.Throws<RuvioException>(() => strict.GetAllElements());
    }

    [Theory]
    [InlineData("ruvio_durability:memory\r\n", "memory")]
    [InlineData("a:b\nruvio_durability:everysec", "everysec")]
    [InlineData("", null)]
    public void TheProfileIsReadFromInfo(string info, string? profile)
        => Assert.Equal(profile, RuvioXmlRepository.DurabilityProfile(info));
}
