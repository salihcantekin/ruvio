using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.RateLimiting.Tests;

public class MiddlewareTests
{
    [Theory]
    [InlineData("rl:", "api", "tenant-1")]
    [InlineData("", "çalışan", "kullanıcı-ğüşiöç-😀")]
    [InlineData("ruvio:ratelimit:", "p", null)]
    public void StorageKeyMatchesTheDocumentedHashFormat(string prefix, string policy, string? partition)
    {
        partition ??= new string('x', 1024);
        string identity = $"{policy.Length}:{policy}{partition}";
        string expected = prefix + Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(identity)));

        Assert.Equal(expected, RuvioRateLimitingMiddleware.StorageKey(prefix, policy, partition));
        Assert.Equal(expected, RuvioRateLimitingMiddleware.StorageKey(prefix, policy, partition));
    }

    [Fact]
    public void StorageKeyAllocatesOnlyTheKey()
    {
        RuvioRateLimitingMiddleware.StorageKey("rl:", "api", "tenant-1");
        long before = GC.GetAllocatedBytesForCurrentThread();
        string key = RuvioRateLimitingMiddleware.StorageKey("rl:", "api", "tenant-1");
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(allocated <= 22 + (key.Length * 2) + 8, $"allocated {allocated} bytes");
    }

    [Fact]
    public async Task BypassHasNoServicesCommandsOrAllocations()
    {
        var context = new DefaultHttpContext();
        var middleware = new RuvioRateLimitingMiddleware(static _ => Task.CompletedTask, Options.Create(new RuvioRateLimitingOptions()));

        for (int i = 0; i < 100; i++)
        {
            Assert.Same(Task.CompletedTask, middleware.InvokeAsync(context));
            await middleware.InvokeAsync(context);
        }

        long start = GC.GetAllocatedBytesForCurrentThread();

        for (int i = 0; i < 1000; i++)
            Assert.Same(Task.CompletedTask, middleware.InvokeAsync(context));

        Assert.Equal(0, GC.GetAllocatedBytesForCurrentThread() - start);
    }

    [Fact]
    public void OptionsAreReadOnceWhenTheMiddlewareIsBuilt()
    {
        var options = new CountingOptions();

        _ = new RuvioRateLimitingMiddleware(static _ => Task.CompletedTask, options);
        Assert.Equal(1, options.Reads);
    }

    private sealed class CountingOptions : IOptions<RuvioRateLimitingOptions>
    {
        public int Reads { get; private set; }

        public RuvioRateLimitingOptions Value
        {
            get
            {
                Reads++;

                return new RuvioRateLimitingOptions();
            }
        }
    }

    [Theory]
    [InlineData(0L, 200, 1)]
    [InlineData(4L, 200, 1)]
    [InlineData(null, 429, 0)]
    public async Task OneAtomicLimitCommand(long? remaining, int status, int expectedNext)
    {
        var client = DispatchProxy.Create<IRuvioClient, Stub>();
        var stub = (Stub)client;

        stub.Result = Task.FromResult(remaining);
        using var services = Services(client);
        var context = Context(services, "tenant");
        int calls = 0;
        var middleware = Create(_ => { calls++; return Task.CompletedTask; }, services);

        await middleware.InvokeAsync(context);
        Assert.Equal(status, context.Response.StatusCode);
        Assert.Equal(expectedNext, calls);
        Assert.Equal(1, stub.Calls);
        Assert.DoesNotContain("tenant", stub.Key);
        Assert.Equal(5, stub.Permits);
        Assert.Equal(60_000, stub.Window);

        if (remaining is null)
            Assert.Equal("60", context.Response.Headers.RetryAfter.ToString());
    }

    [Theory]
    [InlineData(RuvioRateLimitAlgorithm.SlidingWindow, nameof(IRuvioClient.LimitSlidingWithRetryAsync))]
    [InlineData(RuvioRateLimitAlgorithm.TokenBucket, nameof(IRuvioClient.LimitTokensWithRetryAsync))]
    public async Task OtherAlgorithmsUseTheirModeAndExactRetryAfter(RuvioRateLimitAlgorithm algorithm, string method)
    {
        var client = DispatchProxy.Create<IRuvioClient, Stub>();
        var stub = (Stub)client;

        using var services = Services(client);
        int calls = 0;
        var middleware = Create(_ => { calls++; return Task.CompletedTask; }, services);

        stub.Outcome = new LimitOutcome(true, 3, 0);
        var allowed = Context(services, "tenant", algorithm);
        await middleware.InvokeAsync(allowed);
        Assert.Equal(200, allowed.Response.StatusCode);
        Assert.Equal(1, calls);

        stub.Outcome = new LimitOutcome(false, -1, 1500);
        var refused = Context(services, "tenant", algorithm);
        await middleware.InvokeAsync(refused);
        Assert.Equal(429, refused.Response.StatusCode);
        Assert.Equal("2", refused.Response.Headers.RetryAfter.ToString());
        Assert.Equal(1, calls);
        Assert.Equal(method, stub.Method);
        Assert.Equal(5, stub.Permits);
        Assert.Equal(60_000, stub.Window);
    }

    [Fact]
    public async Task FailuresPropagateWithoutRetryOrExecution()
    {
        var client = DispatchProxy.Create<IRuvioClient, Stub>();
        var stub = (Stub)client;

        stub.Result = Task.FromException<long?>(new IOException("unavailable"));
        using var services = Services(client);
        var middleware = Create(_ => throw new Exception("must not run"), services);

        await Assert.ThrowsAsync<IOException>(() => middleware.InvokeAsync(Context(services, "tenant")));
        Assert.Equal(1, stub.Calls);
    }

    [Fact]
    public async Task PartitionsAreIsolatedAndCancellationIsForwarded()
    {
        var client = DispatchProxy.Create<IRuvioClient, Stub>();
        var stub = (Stub)client;
        using var services = Services(client);
        var middleware = Create(_ => Task.CompletedTask, services);
        var first = Context(services, "one");
        using var cancel = new CancellationTokenSource();

        first.RequestAborted = cancel.Token;
        await middleware.InvokeAsync(first);
        string? key = stub.Key;

        Assert.Equal(cancel.Token, stub.Token);
        await middleware.InvokeAsync(Context(services, "two"));
        Assert.NotEqual(key, stub.Key);
    }

    [Fact]
    public async Task MissingPartitionDoesNotConsumeABudget()
    {
        var client = DispatchProxy.Create<IRuvioClient, Stub>();
        using var services = Services(client);
        var middleware = Create(_ => Task.CompletedTask, services);

        await Assert.ThrowsAsync<InvalidOperationException>(() => middleware.InvokeAsync(Context(services, "")));
        Assert.Equal(0, ((Stub)client).Calls);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(5)]
    public async Task InvalidServerAllowanceNeverAdmitsARequest(long remaining)
    {
        var client = DispatchProxy.Create<IRuvioClient, Stub>();

        ((Stub)client).Result = Task.FromResult<long?>(remaining);
        using var services = Services(client);
        var middleware = Create(_ => throw new Exception("must not run"), services);

        await Assert.ThrowsAsync<RuvioException>(() => middleware.InvokeAsync(Context(services, "tenant")));
    }

    [Fact]
    public void InvalidOptionsAndPolicyAreRejected()
    {
        var services = new ServiceCollection();

        services.AddRuvioRateLimiting(_ => { });

        using var provider = services.BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() =>
            provider.GetRequiredService<IOptions<RuvioRateLimitingOptions>>().Value);
        Assert.Throws<ArgumentOutOfRangeException>(() => new RuvioRateLimitAttribute("x", 0, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RuvioRateLimitAttribute("x", 1, 0));
    }

    internal static RuvioRateLimitingMiddleware Create(RequestDelegate next, IServiceProvider services)
        => new(next, services.GetRequiredService<IOptions<RuvioRateLimitingOptions>>());

    private static ServiceProvider Services(IRuvioClient client)
    {
        var services = new ServiceCollection();

        services.AddSingleton(client);
        services.AddRuvioRateLimiting(options => options.PartitionKey = context => (string)context.Items["tenant"]!);

        return services.BuildServiceProvider();
    }

    private static DefaultHttpContext Context(
        IServiceProvider services, string tenant, RuvioRateLimitAlgorithm algorithm = RuvioRateLimitAlgorithm.FixedWindow)
    {
        var context = new DefaultHttpContext { RequestServices = services };

        context.Items["tenant"] = tenant;
        context.SetEndpoint(new Endpoint(_ => Task.CompletedTask,
            new EndpointMetadataCollection(new RuvioRateLimitAttribute("test", 5, 60) { Algorithm = algorithm }), "test"));

        return context;
    }

    public class Stub : DispatchProxy
    {
        public Task<long?> Result = Task.FromResult<long?>(4);
        public LimitOutcome Outcome;
        public string? Method;
        public int Calls;
        public string? Key;
        public long Permits;
        public long Window;
        public CancellationToken Token;

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod?.Name is nameof(IRuvioClient.LimitSlidingWithRetryAsync) or nameof(IRuvioClient.LimitTokensWithRetryAsync))
            {
                Calls++;
                Method = targetMethod.Name;
                Key = (string)args![0]!;
                Permits = (long)args[1]!;
                Window = (long)args[2]!;

                return new ValueTask<LimitOutcome>(Outcome);
            }

            if (targetMethod?.Name != nameof(IRuvioClient.LimitAsync))
                throw new NotSupportedException();

            Calls++;
            Method = targetMethod.Name;
            Key = (string)args![0]!;
            Permits = (long)args[1]!;
            Window = (long)args[2]!;
            Token = (CancellationToken)args[3]!;

            return new ValueTask<long?>(Result);
        }
    }
}
