using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Extensions.Idempotency.Tests;

/// <summary>The registration check the host runs on start (the same IStartupValidator the generic host calls).</summary>
public sealed class StartupCheckTests
{
    [Fact]
    public void MissingClientFailsStartAndNamesTheFix()
    {
        var services = new ServiceCollection();

        services.AddRuvioIdempotentExecutor();
        using ServiceProvider provider = services.BuildServiceProvider();

        var error = Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IStartupValidator>().Validate());

        Assert.Contains("services.AddRuvioClient", error.Message);
    }

    [Fact]
    public void RegisteredKeyedClientIsNotResolvedAtStart()
    {
        var services = new ServiceCollection();

        services.AddKeyedSingleton<IRuvioClient>("jobs", static (_, _) => throw new InvalidOperationException("The client was resolved at start."));
        services.AddRuvioIdempotentExecutor(static o => o.ClientKey = "jobs");
        using ServiceProvider provider = services.BuildServiceProvider();

        provider.GetRequiredService<IStartupValidator>().Validate();
    }
}
