using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.RateLimiting.Tests;

public class EdgeTests
{
    [Fact]
    public void AttributeBoundsPolicyLengthPermitsAndWindow()
    {
        Assert.Throws<ArgumentNullException>(() => new RuvioRateLimitAttribute(null!, 1, 1));
        Assert.Throws<ArgumentException>(() => new RuvioRateLimitAttribute("", 1, 1));
        Assert.Throws<ArgumentException>(() => new RuvioRateLimitAttribute("   ", 1, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RuvioRateLimitAttribute(new string('p', 129), 1, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RuvioRateLimitAttribute("p", -1, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RuvioRateLimitAttribute("p", 1, -1));

        var edge = new RuvioRateLimitAttribute(new string('p', 128), long.MaxValue, int.MaxValue);

        Assert.Equal(128, edge.Policy.Length);
        Assert.Equal(long.MaxValue, edge.Permits);
        Assert.Equal(int.MaxValue, edge.WindowSeconds);
    }

    [Fact]
    public void RegistrationValidatesThePartitionSelectorAndTheKeyPrefix()
    {
        static void Resolve(Action<RuvioRateLimitingOptions> configure)
        {
            var services = new ServiceCollection();

            services.AddRuvioRateLimiting(configure);
            using ServiceProvider provider = services.BuildServiceProvider();

            _ = provider.GetRequiredService<IOptions<RuvioRateLimitingOptions>>().Value;
        }

        static void Valid(RuvioRateLimitingOptions o) => o.PartitionKey = _ => "p";

        Resolve(Valid);
        Resolve(o => { Valid(o); o.KeyPrefix = new string('k', 128); });
        Assert.Throws<OptionsValidationException>(() => Resolve(o => { Valid(o); o.KeyPrefix = ""; }));
        Assert.Throws<OptionsValidationException>(() => Resolve(o => { Valid(o); o.KeyPrefix = "  "; }));
        Assert.Throws<OptionsValidationException>(() => Resolve(o => { Valid(o); o.KeyPrefix = new string('k', 129); }));
        Assert.Throws<OptionsValidationException>(() => Resolve(o => { Valid(o); o.KeyPrefix = "a{b"; }));
        Assert.Throws<OptionsValidationException>(() => Resolve(o => { Valid(o); o.KeyPrefix = "a}b"; }));
        Assert.Throws<OptionsValidationException>(() => Resolve(o => o.PartitionKey = null));
        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddRuvioRateLimiting(_ => { }));
        Assert.Throws<ArgumentNullException>(() => new ServiceCollection().AddRuvioRateLimiting(null!));
    }

    [Theory]
    [InlineData(0, 1024)]
    [InlineData(1000, 1)]
    [InlineData(100, 1024)]
    public void StorageKeyHandlesLongPrefixesAndMultiBytePartitions(int prefixLength, int partitionLength)
    {
        string prefix = new('k', prefixLength);
        string partition = string.Concat(Enumerable.Repeat("çğü", partitionLength / 3 + 1))[..partitionLength];
        string expected = prefix + Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes($"3:api{partition}")));

        Assert.Equal(expected, RuvioRateLimitingMiddleware.StorageKey(prefix, "api", partition));

        string big = new('€', 1024);

        Assert.Equal(
            prefix + Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes($"3:api{big}"))),
            RuvioRateLimitingMiddleware.StorageKey(prefix, "api", big));
    }

    [Fact]
    public void StorageKeyBindsThePolicyLengthSoPolicyAndPartitionCannotCollide()
    {
        Assert.NotEqual(
            RuvioRateLimitingMiddleware.StorageKey("p:", "ab", "c"),
            RuvioRateLimitingMiddleware.StorageKey("p:", "a", "bc"));
    }

    [Theory]
    [InlineData(" ")]
    [InlineData("")]
    public async Task BlankPartitionsAndOversizedPartitionsAreConfigurationErrors(string partition)
    {
        var client = DispatchProxy.Create<IRuvioClient, MiddlewareTests.Stub>();

        await RejectPartition(client, partition);
        await RejectPartition(client, new string('x', 1025));
        Assert.Equal(0, ((MiddlewareTests.Stub)client).Calls);

        // The longest accepted partition is exactly 1024 characters.
        var services = new ServiceCollection();

        services.AddSingleton(client);
        services.AddRuvioRateLimiting(o => o.PartitionKey = _ => new string('x', 1024));
        await using ServiceProvider provider = services.BuildServiceProvider();
        var context = new DefaultHttpContext { RequestServices = provider };

        context.SetEndpoint(new Endpoint(_ => Task.CompletedTask,
            new EndpointMetadataCollection(new RuvioRateLimitAttribute("p", 5, 60)), "t"));
        await MiddlewareTests.Create(_ => Task.CompletedTask, provider).InvokeAsync(context);
        Assert.Equal(1, ((MiddlewareTests.Stub)client).Calls);
    }

    private static async Task RejectPartition(IRuvioClient client, string partition)
    {
        var services = new ServiceCollection();

        services.AddSingleton(client);
        services.AddRuvioRateLimiting(o => o.PartitionKey = _ => partition);
        await using ServiceProvider provider = services.BuildServiceProvider();
        var context = new DefaultHttpContext { RequestServices = provider };

        context.SetEndpoint(new Endpoint(_ => Task.CompletedTask,
            new EndpointMetadataCollection(new RuvioRateLimitAttribute("p", 5, 60)), "t"));
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => MiddlewareTests.Create(_ => throw new Exception("must not run"), provider).InvokeAsync(context));
    }

    [Fact]
    public async Task TheRetryDelayIsTheWholeWindowAndTheAllowanceIsCheckedAtItsEdges()
    {
        var client = DispatchProxy.Create<IRuvioClient, MiddlewareTests.Stub>();
        var stub = (MiddlewareTests.Stub)client;
        var services = new ServiceCollection();

        services.AddSingleton(client);
        services.AddRuvioRateLimiting(o => o.PartitionKey = _ => "t");
        await using ServiceProvider provider = services.BuildServiceProvider();

        async Task<DefaultHttpContext> Run(long? remaining, long permits = 5, int window = 3600)
        {
            stub.Result = Task.FromResult(remaining);
            var context = new DefaultHttpContext { RequestServices = provider };

            context.SetEndpoint(new Endpoint(_ => Task.CompletedTask,
                new EndpointMetadataCollection(new RuvioRateLimitAttribute("p", permits, window)), "t"));
            await MiddlewareTests.Create(_ => Task.CompletedTask, provider).InvokeAsync(context);

            return context;
        }

        DefaultHttpContext rejected = await Run(null, window: 3600);

        Assert.Equal(429, rejected.Response.StatusCode);
        Assert.Equal("3600", rejected.Response.Headers.RetryAfter.ToString());
        Assert.Equal(3_600_000, stub.Window);
        Assert.Equal(200, (await Run(4)).Response.StatusCode);
        Assert.Equal(200, (await Run(0, permits: 1)).Response.StatusCode);
        await Assert.ThrowsAsync<InvalidOperationException>(() => Run(1, permits: 1));
        await Assert.ThrowsAsync<InvalidOperationException>(() => Run(-1));

        // A window near the int limit must not overflow the millisecond conversion.
        await Run(0, permits: 1, window: int.MaxValue);
        Assert.Equal((long)int.MaxValue * 1000, stub.Window);
    }
}
