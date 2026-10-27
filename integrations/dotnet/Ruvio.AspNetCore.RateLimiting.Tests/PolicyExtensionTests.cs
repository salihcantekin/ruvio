using System.Reflection;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.RateLimiting.Tests;

public class PolicyExtensionTests
{
    [Fact]
    public async Task EachPolicyUsesItsOwnPartitionAndFallsBackToTheDefault()
    {
        var (stub, client) = NewClient();
        using var services = Services(client, o =>
        {
            o.PartitionKey = _ => "tenant-1";
            o.AddPolicy("by-ip", 5, TimeSpan.FromMinutes(1), _ => "10.0.0.1");
            o.AddPolicy("by-tenant", 5, TimeSpan.FromMinutes(1));
        });
        var middleware = MiddlewareTests.Create(_ => Task.CompletedTask, services);

        await middleware.InvokeAsync(Context(services, new RuvioRateLimitAttribute("by-ip")));
        Assert.Equal(RuvioRateLimitingMiddleware.StorageKey("rl:", "by-ip", "10.0.0.1"), stub.Keys.Last());
        await middleware.InvokeAsync(Context(services, new RuvioRateLimitAttribute("by-tenant")));
        Assert.Equal(RuvioRateLimitingMiddleware.StorageKey("rl:", "by-tenant", "tenant-1"), stub.Keys.Last());
    }

    [Fact]
    public void GlobalPartitionIsOptionalWhenEveryLimitHasItsOwn()
    {
        Resolve(o => o.AddPolicy("a", 1, TimeSpan.FromSeconds(1), _ => "x"));
        Assert.Throws<OptionsValidationException>(() => Resolve(o => o.AddPolicy("a", 1, TimeSpan.FromSeconds(1))));
        Assert.Throws<OptionsValidationException>(() => Resolve(_ => { }));
        Assert.Throws<OptionsValidationException>(() => Resolve(o =>
        {
            o.AddPolicy("a", 1, TimeSpan.FromSeconds(1), _ => "x");
            o.DefaultPolicy = "missing";
        }));
    }

    [Fact]
    public async Task InlineEndpointWithoutAnyPartitionKeyFailsBeforeCallingRuvio()
    {
        var (stub, client) = NewClient();
        using var services = Services(client, o => o.AddPolicy("a", 1, TimeSpan.FromSeconds(1), _ => "x"), partition: false);
        var middleware = MiddlewareTests.Create(_ => Task.CompletedTask, services);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => middleware.InvokeAsync(Context(services, new RuvioRateLimitAttribute("inline", 5, 60))));
        Assert.Empty(stub.Keys);
    }

    [Fact]
    public async Task DefaultPolicyAppliesToEndpointsWithoutMetadataAndCanBeDisabled()
    {
        var (stub, client) = NewClient();
        using var services = Services(client, o =>
        {
            o.AddPolicy("global", 5, TimeSpan.FromMinutes(1));
            o.DefaultPolicy = "global";
        });
        var middleware = MiddlewareTests.Create(_ => Task.CompletedTask, services);

        await middleware.InvokeAsync(Context(services));
        Assert.Single(stub.Keys);
        await middleware.InvokeAsync(Context(services, new DisableRuvioRateLimitAttribute()));
        Assert.Single(stub.Keys);

        // The metadata closest to the endpoint wins.
        await middleware.InvokeAsync(Context(services, new RuvioRateLimitAttribute("global"), new DisableRuvioRateLimitAttribute()));
        Assert.Single(stub.Keys);
        await middleware.InvokeAsync(Context(services, new DisableRuvioRateLimitAttribute(), new RuvioRateLimitAttribute("global")));
        Assert.Equal(2, stub.Keys.Count);

        // No endpoint (for example a 404) is never limited.
        var none = new DefaultHttpContext { RequestServices = services };

        await middleware.InvokeAsync(none);
        Assert.Equal(2, stub.Keys.Count);
    }

    [Fact]
    public async Task WithoutADefaultPolicyEndpointsWithoutMetadataAreNotLimited()
    {
        var (stub, client) = NewClient();
        using var services = Services(client, o => o.AddPolicy("a", 5, TimeSpan.FromMinutes(1)));
        var middleware = MiddlewareTests.Create(_ => Task.CompletedTask, services);

        await middleware.InvokeAsync(Context(services));
        Assert.Empty(stub.Keys);
    }

    [Fact]
    public async Task BlankPartitionThrowsByDefaultAndSharesOneBucketOnRequest()
    {
        var (stub, client) = NewClient();
        string partition = "";

        using var strict = Services(client, o => o.PartitionKey = _ => partition, partition: false);

        await Assert.ThrowsAsync<InvalidOperationException>(() => MiddlewareTests.Create(_ => Task.CompletedTask, strict)
            .InvokeAsync(Context(strict, new RuvioRateLimitAttribute("p", 5, 60))));
        Assert.Empty(stub.Keys);

        using var shared = Services(client, o =>
        {
            o.PartitionKey = _ => partition;
            o.EmptyPartition = RuvioEmptyPartitionMode.SharedBucket;
        }, partition: false);
        var middleware = MiddlewareTests.Create(_ => Task.CompletedTask, shared);

        await middleware.InvokeAsync(Context(shared, new RuvioRateLimitAttribute("p", 5, 60)));
        partition = "   ";
        await middleware.InvokeAsync(Context(shared, new RuvioRateLimitAttribute("p", 5, 60)));
        Assert.Equal(2, stub.Keys.Count);
        Assert.Equal(stub.Keys[0], stub.Keys[1]);

        // Still a bounded value when it is too long.
        partition = new string('x', 1025);
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => middleware.InvokeAsync(Context(shared, new RuvioRateLimitAttribute("p", 5, 60))));
    }

    [Fact]
    public async Task ChainedLimitsEachUseTheirOwnKeyAndStopAtTheFirstRejection()
    {
        var (stub, client) = NewClient();
        using var services = Services(client, o => o.AddPolicy("api", p => p
            .Limit(10, TimeSpan.FromMinutes(1), _ => "tenant-1")
            .Limit(3, TimeSpan.FromSeconds(10), _ => "10.0.0.1", RuvioRateLimitAlgorithm.SlidingWindow)));
        var middleware = MiddlewareTests.Create(_ => Task.CompletedTask, services);

        await middleware.InvokeAsync(Context(services, new RuvioRateLimitAttribute("api")));
        Assert.Equal(2, stub.Keys.Count);
        Assert.NotEqual(stub.Keys[0], stub.Keys[1]);
        Assert.Equal(RuvioRateLimitingMiddleware.StorageKey("rl:", "api", "tenant-1"), stub.Keys[0]);
        Assert.Equal(new long[] { 10, 3 }, stub.Permits);

        // The first limit rejects: the second is not asked.
        stub.Keys.Clear();
        stub.Permits.Clear();
        stub.RejectCall = 0;

        var context = Context(services, new RuvioRateLimitAttribute("api"));

        await middleware.InvokeAsync(context);
        Assert.Equal(429, context.Response.StatusCode);
        Assert.Single(stub.Keys);
        Assert.Equal("60", context.Response.Headers.RetryAfter.ToString());

        // The second limit rejects with its exact wait.
        stub.Keys.Clear();
        stub.RejectCall = 1;
        stub.SlidingRetryMs = 2500;
        context = Context(services, new RuvioRateLimitAttribute("api"));
        await middleware.InvokeAsync(context);
        Assert.Equal(429, context.Response.StatusCode);
        Assert.Equal(2, stub.Keys.Count);
        Assert.Equal("3", context.Response.Headers.RetryAfter.ToString());
    }

    [Fact]
    public void PolicyBuilderValidatesItsLimits()
    {
        var options = new RuvioRateLimitingOptions();

        Assert.Throws<InvalidOperationException>(() => options.AddPolicy("empty", _ => { }));
        Assert.Throws<ArgumentOutOfRangeException>(() => options.AddPolicy("a", p => p.Limit(0, TimeSpan.FromSeconds(1))));
        Assert.Throws<ArgumentOutOfRangeException>(() => options.AddPolicy("a", p => p.Limit(1, TimeSpan.Zero)));
        Assert.Throws<ArgumentOutOfRangeException>(() => options.AddPolicy("a", p => p.Concurrency(0)));
        Assert.Throws<ArgumentOutOfRangeException>(() => options.AddPolicy("a", p => p.Concurrency(1, TimeSpan.Zero)));
        Assert.Throws<InvalidOperationException>(() => options.AddPolicy("a", p => p.Concurrency(1).Concurrency(2)));
        Assert.Throws<ArgumentException>(() => options.AddPolicy("a\0b", p => p.Limit(1, TimeSpan.FromSeconds(1))));
        Assert.Throws<ArgumentNullException>(() => options.AddPolicy("a", (Action<RuvioPolicyBuilder>)null!));
        Assert.Throws<ArgumentNullException>(() => options.AddPolicy("a", 1, TimeSpan.FromSeconds(1), (Func<HttpContext, string>)null!));
    }

    [Fact]
    public async Task ConcurrencySlotIsHeldWhileTheEndpointRunsAndReleased()
    {
        var (stub, client) = NewClient();
        using var services = Services(client, o => o.AddPolicy("uploads", p => p.Concurrency(2, TimeSpan.FromSeconds(45))));
        bool heldInside = false;
        var middleware = MiddlewareTests.Create(_ =>
        {
            heldInside = stub.Released.Count == 0;

            return Task.CompletedTask;
        }, services);

        await middleware.InvokeAsync(Context(services, new RuvioRateLimitAttribute("uploads")));

        Assert.True(heldInside);
        Assert.Equal((2L, 45_000L), (stub.SemaphoreMax, stub.SemaphoreTtl));
        Assert.Equal((stub.Keys[0], 77L), Assert.Single(stub.Released));
    }

    [Fact]
    public async Task ConcurrencySlotIsReleasedWhenTheEndpointThrows()
    {
        var (stub, client) = NewClient();
        using var services = Services(client, o => o.AddPolicy("uploads", p => p.Concurrency(2)));
        var middleware = MiddlewareTests.Create(_ => throw new InvalidOperationException("boom"), services);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => middleware.InvokeAsync(Context(services, new RuvioRateLimitAttribute("uploads"))));
        Assert.Single(stub.Released);
    }

    [Fact]
    public async Task FullConcurrencyAnswers429WithoutRunningTheEndpoint()
    {
        var (stub, client) = NewClient();

        stub.SlotFull = true;
        using var services = Services(client, o => o.AddPolicy("uploads", p => p.Concurrency(1)));
        var middleware = MiddlewareTests.Create(_ => throw new Exception("must not run"), services);
        var context = Context(services, new RuvioRateLimitAttribute("uploads"));

        await middleware.InvokeAsync(context);
        Assert.Equal(429, context.Response.StatusCode);
        Assert.Equal("1", context.Response.Headers.RetryAfter.ToString());
        Assert.Empty(stub.Released);
    }

    [Fact]
    public async Task SlotIsReleasedWhenALaterLimitRejects()
    {
        var (stub, client) = NewClient();

        stub.RejectCall = 1;
        using var services = Services(client, o => o.AddPolicy("p", b => b.Concurrency(3).Limit(5, TimeSpan.FromMinutes(1))));
        var middleware = MiddlewareTests.Create(_ => throw new Exception("must not run"), services);
        var context = Context(services, new RuvioRateLimitAttribute("p"));

        await middleware.InvokeAsync(context);
        Assert.Equal(429, context.Response.StatusCode);
        Assert.Single(stub.Released);
    }

    [Fact]
    public async Task FailedReleaseDoesNotFailTheRequest()
    {
        var (stub, client) = NewClient();

        stub.ReleaseFails = true;
        using var services = Services(client, o => o.AddPolicy("uploads", p => p.Concurrency(2)));
        var middleware = MiddlewareTests.Create(_ => Task.CompletedTask, services);

        await middleware.InvokeAsync(Context(services, new RuvioRateLimitAttribute("uploads")));
    }

    [Fact]
    public async Task OnRejectedRunsAfterTheDefaultAnswerAndMayReplaceIt()
    {
        var (stub, client) = NewClient();

        stub.RejectCall = 0;
        RuvioRateLimitRejection seen = default;
        int statusSeen = 0;
        using var services = Services(client, o =>
        {
            o.AddPolicy("api", 5, TimeSpan.FromSeconds(30));
            o.OnRejected = (context, rejection) =>
            {
                seen = rejection;
                statusSeen = context.Response.StatusCode;
                context.Response.StatusCode = 503;

                return ValueTask.CompletedTask;
            };
        });
        var middleware = MiddlewareTests.Create(_ => throw new Exception("must not run"), services);
        var context = Context(services, new RuvioRateLimitAttribute("api"));

        await middleware.InvokeAsync(context);
        Assert.Equal(429, statusSeen);
        Assert.Equal(503, context.Response.StatusCode);
        Assert.Equal(("api", RuvioRejectionReason.RateLimit, 0, TimeSpan.FromSeconds(30)),
            (seen.Policy, seen.Reason, seen.LimitIndex, seen.RetryAfter));
        Assert.Equal("30", context.Response.Headers.RetryAfter.ToString());
    }

    private static (PolicyStub Stub, IRuvioClient Client) NewClient()
    {
        var client = DispatchProxy.Create<IRuvioClient, PolicyStub>();

        return ((PolicyStub)client, client);
    }

    private static void Resolve(Action<RuvioRateLimitingOptions> configure)
    {
        var services = new ServiceCollection();

        services.AddRuvioRateLimiting(configure);

        using var provider = services.BuildServiceProvider();

        _ = provider.GetRequiredService<IOptions<RuvioRateLimitingOptions>>().Value;
    }

    private static ServiceProvider Services(IRuvioClient client, Action<RuvioRateLimitingOptions> configure, bool partition = true)
    {
        var services = new ServiceCollection();

        services.AddSingleton(client);
        services.AddRuvioRateLimiting(options =>
        {
            options.KeyPrefix = "rl:";

            if (partition)
                options.PartitionKey = _ => "tenant";

            configure(options);
        });

        return services.BuildServiceProvider();
    }

    private static DefaultHttpContext Context(IServiceProvider services, params object[] metadata)
    {
        var context = new DefaultHttpContext { RequestServices = services };

        context.SetEndpoint(new Endpoint(_ => Task.CompletedTask, new EndpointMetadataCollection(metadata), "ep"));

        return context;
    }

    /// <summary>Counts storage calls; call number <see cref="RejectCall"/> (0-based, over rate limits and slots) is refused.</summary>
    public class PolicyStub : DispatchProxy
    {
        public readonly List<string> Keys = [];
        public readonly List<long> Permits = [];
        public readonly List<(string Key, long Token)> Released = [];
        public int RejectCall = -1;
        public long SlidingRetryMs = 1000;
        public bool SlotFull;
        public bool ReleaseFails;
        public long SemaphoreMax;
        public long SemaphoreTtl;

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            string name = targetMethod!.Name;

            if (name == nameof(IRuvioClient.ReleaseAsync))
            {
                if (ReleaseFails)
                    throw new RuvioException("release failed");

                Released.Add(((string)args![0]!, (long)args[1]!));

                return new ValueTask<long>(1);
            }

            int call = Keys.Count;

            Keys.Add((string)args![0]!);

            if (name == nameof(IRuvioClient.AcquireSemaphoreAsync))
            {
                SemaphoreMax = (long)args[1]!;
                SemaphoreTtl = (long)args[2]!;

                return new ValueTask<long?>(SlotFull || call == RejectCall ? null : 77L);
            }

            Permits.Add((long)args[1]!);

            if (name == nameof(IRuvioClient.LimitAsync))
                return new ValueTask<long?>(call == RejectCall ? null : 1L);

            return new ValueTask<LimitOutcome>(call == RejectCall
                ? new LimitOutcome(false, -1, SlidingRetryMs)
                : new LimitOutcome(true, 1, 0));
        }
    }
}
