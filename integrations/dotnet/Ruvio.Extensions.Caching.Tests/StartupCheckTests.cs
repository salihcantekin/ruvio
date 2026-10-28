using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Extensions.Caching.Tests;

/// <summary>The registration check the host runs on start (the same IStartupValidator the generic host calls).</summary>
public sealed class StartupCheckTests
{
    [Fact]
    public void MissingClientFailsStartAndNamesTheFix()
    {
        var services = new ServiceCollection();

        services.AddRuvioDistributedCache();
        using ServiceProvider provider = services.BuildServiceProvider();

        var error = Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IStartupValidator>().Validate());

        Assert.Contains("services.AddRuvioClient", error.Message);
    }

    [Fact]
    public void MissingKeyedClientFailsStartAndNamesTheKey()
    {
        var services = new ServiceCollection();

        services.AddSingleton<IRuvioClient>(static _ => throw new InvalidOperationException("Unkeyed client."));
        services.AddRuvioDistributedCache(static o => o.ClientKey = "sessions");
        using ServiceProvider provider = services.BuildServiceProvider();

        var error = Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IStartupValidator>().Validate());

        Assert.Contains("AddKeyedRuvioClient(\"sessions\"", error.Message);
    }

    [Fact]
    public void RegisteredClientIsNotResolvedAtStart()
    {
        var services = new ServiceCollection();

        services.AddSingleton<IRuvioClient>(static _ => throw new InvalidOperationException("The client was resolved at start."));
        services.AddRuvioDistributedCache();
        using ServiceProvider provider = services.BuildServiceProvider();

        provider.GetRequiredService<IStartupValidator>().Validate();
    }

    [Fact]
    public void ExistingCacheRegistrationNeedsNoClient()
    {
        var services = new ServiceCollection();

        services.AddSingleton<IDistributedCache>(static _ => throw new InvalidOperationException("Another cache."));
        services.AddRuvioDistributedCache();
        using ServiceProvider provider = services.BuildServiceProvider();

        Assert.Null(provider.GetService<IStartupValidator>());
    }
}
