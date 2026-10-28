using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using System.Reflection;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.RateLimiting.Tests;

public class NamedPolicyTests
{
    [Fact]
    public void AddPolicyValidatesItsArguments()
    {
        var options = new RuvioRateLimitingOptions();

        Assert.Throws<ArgumentNullException>(() => options.AddPolicy(null!, 1, TimeSpan.FromSeconds(1)));
        Assert.Throws<ArgumentException>(() => options.AddPolicy(" ", 1, TimeSpan.FromSeconds(1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => options.AddPolicy(new string('p', 129), 1, TimeSpan.FromSeconds(1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => options.AddPolicy("p", 0, TimeSpan.FromSeconds(1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => options.AddPolicy("p", 1, TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => options.AddPolicy("p", 1, TimeSpan.FromSeconds(1), (RuvioRateLimitAlgorithm)9));

        Assert.Same(options, options.AddPolicy("p", 7, TimeSpan.FromMilliseconds(1500), RuvioRateLimitAlgorithm.TokenBucket));
        Assert.True(options.TryGetPolicy("p", out var limits));

        var policy = Assert.Single(limits);

        Assert.Equal((7L, 2, RuvioRateLimitAlgorithm.TokenBucket), (policy.Permits, policy.WindowSeconds, policy.Algorithm));
        Assert.Throws<ArgumentException>(() => options.AddPolicy("p", 1, TimeSpan.FromSeconds(1)));
        Assert.False(options.TryGetPolicy("P", out _));
    }

    [Fact]
    public void NameOnlyAttributeAndExtensionCarryNoSettings()
    {
        var attribute = new RuvioRateLimitAttribute("api");

        Assert.Equal(("api", 0L, 0, true), (attribute.Policy, attribute.Permits, attribute.WindowSeconds, attribute.IsNamed));
        Assert.False(new RuvioRateLimitAttribute("api", 1, 1).IsNamed);
        Assert.Throws<ArgumentException>(() => new RuvioRateLimitAttribute(""));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RuvioRateLimitAttribute(new string('p', 129)));

        var app = WebApplication.CreateBuilder().Build();

        app.MapGet("/x", () => "ok").WithRuvioRateLimit("api");

        var found = ((IEndpointRouteBuilder)app).DataSources.SelectMany(d => d.Endpoints)
            .Single().Metadata.GetMetadata<RuvioRateLimitAttribute>();

        Assert.True(found!.IsNamed);
        Assert.Equal("api", found.Policy);
    }

    [Theory(Timeout = 30000)]
    [InlineData(RuvioRateLimitAlgorithm.FixedWindow, nameof(IRuvioClient.LimitAsync))]
    [InlineData(RuvioRateLimitAlgorithm.SlidingWindow, nameof(IRuvioClient.LimitSlidingWithRetryAsync))]
    [InlineData(RuvioRateLimitAlgorithm.TokenBucket, nameof(IRuvioClient.LimitTokensWithRetryAsync))]
    public async Task RegisteredSettingsDriveTheCommand(RuvioRateLimitAlgorithm algorithm, string method)
    {
        var client = DispatchProxy.Create<IRuvioClient, MiddlewareTests.Stub>();
        var stub = (MiddlewareTests.Stub)client;

        stub.Result = Task.FromResult<long?>(null);
        stub.Outcome = new LimitOutcome(false, -1, 1500);
        using var services = Services(client, o => o.AddPolicy("api", 9, TimeSpan.FromSeconds(30), algorithm));
        var context = Context(services, new RuvioRateLimitAttribute("api"));
        var middleware = MiddlewareTests.Create(_ => throw new Exception("must not run"), services);

        await middleware.InvokeAsync(context);
        Assert.Equal(429, context.Response.StatusCode);
        Assert.Equal(method, stub.Method);
        Assert.Equal((9L, 30_000L), (stub.Permits, stub.Window));
        Assert.Equal(algorithm == RuvioRateLimitAlgorithm.FixedWindow ? "30" : "2", context.Response.Headers.RetryAfter.ToString());
    }

    [Fact(Timeout = 30000)]
    public async Task NamedAndInlinePoliciesWithTheSameNameShareAKey()
    {
        var client = DispatchProxy.Create<IRuvioClient, MiddlewareTests.Stub>();
        var stub = (MiddlewareTests.Stub)client;

        using var services = Services(client, o => o.AddPolicy("api", 5, TimeSpan.FromSeconds(60)));
        var middleware = MiddlewareTests.Create(_ => Task.CompletedTask, services);

        await middleware.InvokeAsync(Context(services, new RuvioRateLimitAttribute("api")));
        string? namedKey = stub.Key;
        await middleware.InvokeAsync(Context(services, new RuvioRateLimitAttribute("api", 5, 60)));

        Assert.Equal(namedKey, stub.Key);
        Assert.Equal(2, stub.Calls);
    }

    [Fact(Timeout = 30000)]
    public async Task UnknownPolicyFailsBeforeCallingRuvio()
    {
        var client = DispatchProxy.Create<IRuvioClient, MiddlewareTests.Stub>();
        var stub = (MiddlewareTests.Stub)client;

        using var services = Services(client, o => o.AddPolicy("other", 5, TimeSpan.FromSeconds(60)));
        var middleware = MiddlewareTests.Create(_ => throw new Exception("must not run"), services);
        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => middleware.InvokeAsync(Context(services, new RuvioRateLimitAttribute("api"))));

        Assert.Contains("'api'", error.Message);
        Assert.Equal(0, stub.Calls);
    }

    private static ServiceProvider Services(IRuvioClient client, Action<RuvioRateLimitingOptions> configure)
    {
        var services = new ServiceCollection();

        services.AddSingleton(client);
        services.AddRuvioRateLimiting(options =>
        {
            options.PartitionKey = _ => "tenant";
            configure(options);
        });

        return services.BuildServiceProvider();
    }

    private static DefaultHttpContext Context(IServiceProvider services, RuvioRateLimitAttribute attribute)
    {
        var context = new DefaultHttpContext { RequestServices = services };

        context.SetEndpoint(new Endpoint(_ => Task.CompletedTask, new EndpointMetadataCollection(attribute), "named"));

        return context;
    }
}
