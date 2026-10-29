using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Ruvio.AspNetCore.DataProtection.Tests;

/// <summary>What the package checks before the server takes a request.</summary>
public sealed class StartupCheckTests
{
    [Fact(Timeout = 30000)]
    public async Task MissingConnectionFailsStartAndNamesTheFix()
    {
        await using WebApplication app = App(static builder => builder.PersistKeysToRuvio(static o => o.RequireEncryption = false));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => app.StartAsync());

        Assert.Contains("IRuvioConnection", error.Message);
        Assert.Contains("services.AddRuvioClient", error.Message);
    }

    [Fact(Timeout = 30000)]
    public async Task MissingKeyedConnectionFailsStartAndNamesTheKey()
    {
        await using WebApplication app = App(static builder => builder.PersistKeysToRuvio(static o =>
        {
            o.RequireEncryption = false;
            o.ClientKey = "key-ring";
        }));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => app.StartAsync());

        Assert.Contains("AddKeyedRuvioClient(\"key-ring\"", error.Message);
    }

    private static WebApplication App(Action<IDataProtectionBuilder> persist)
    {
        var builder = WebApplication.CreateBuilder();

        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();
        persist(builder.Services.AddDataProtection());

        return builder.Build();
    }
}
