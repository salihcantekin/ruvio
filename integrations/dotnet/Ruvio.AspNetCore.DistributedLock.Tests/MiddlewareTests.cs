using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.DistributedLock.Tests;

/// <summary>The bypass test runs anywhere; the rest need RUVIO_TEST_ADDR (host:port) and otherwise return at once.</summary>
public class MiddlewareTests
{
    [Fact]
    public void EndpointsWithoutTheAttributeCostNothing()
    {
        var context = new DefaultHttpContext();
        var middleware = new RuvioLockMiddleware(static _ => Task.CompletedTask);

        for (int i = 0; i < 100; i++)
            Assert.Same(Task.CompletedTask, middleware.InvokeAsync(context));

        long start = GC.GetAllocatedBytesForCurrentThread();

        for (int i = 0; i < 1000; i++)
            Assert.Same(Task.CompletedTask, middleware.InvokeAsync(context));

        Assert.Equal(0, GC.GetAllocatedBytesForCurrentThread() - start);
    }

    [Fact]
    public void AttributeRejectsInvalidArguments()
    {
        Assert.Throws<ArgumentException>(() => new RuvioLockAttribute(" "));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RuvioLockAttribute("x", 0));
    }

    [Fact(Timeout = 30000)]
    public async Task SecondRequestIsRejectedWith409WhileTheFirstRunsAndTheLockIsFreedAfter()
    {
        if (await Live.ConnectAsync() is not { } live)
            return;

        await using RuvioConnection connection = live.Connection;
        using ServiceProvider services = live.Services();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int runs = 0;
        var middleware = new RuvioLockMiddleware(async _ => { runs++; started.TrySetResult(); await gate.Task.WaitAsync(TimeSpan.FromSeconds(20)); });

        Task first = middleware.InvokeAsync(live.Context(services, "export"));
        await started.Task.WaitAsync(TimeSpan.FromSeconds(10));
        DefaultHttpContext second = live.Context(services, "export");
        await middleware.InvokeAsync(second);

        Assert.Equal(409, second.Response.StatusCode);
        Assert.False(string.IsNullOrEmpty(second.Response.Headers.RetryAfter.ToString()));
        Assert.Equal(1, runs);

        gate.SetResult();
        await first;

        DefaultHttpContext third = live.Context(services, "export");
        await new RuvioLockMiddleware(_ => Task.CompletedTask).InvokeAsync(third);
        Assert.Equal(200, third.Response.StatusCode);
    }

    [Fact(Timeout = 30000)]
    public async Task TheLockIsReleasedWhenTheEndpointThrows()
    {
        if (await Live.ConnectAsync() is not { } live)
            return;

        await using RuvioConnection connection = live.Connection;
        using ServiceProvider services = live.Services();
        var failing = new RuvioLockMiddleware(_ => throw new InvalidOperationException("boom"));

        await Assert.ThrowsAsync<InvalidOperationException>(() => failing.InvokeAsync(live.Context(services, "job")));

        DefaultHttpContext next = live.Context(services, "job");
        await new RuvioLockMiddleware(_ => Task.CompletedTask).InvokeAsync(next);
        Assert.Equal(200, next.Response.StatusCode);
    }

    [Fact(Timeout = 30000)]
    public async Task TemplatedNamesLockPerValueAndAMissingValueIsA400()
    {
        if (await Live.ConnectAsync() is not { } live)
            return;

        await using RuvioConnection connection = live.Connection;
        using ServiceProvider services = live.Services();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var otherStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var middleware = new RuvioLockMiddleware(async context =>
        {
            (context.Request.RouteValues["id"] as string == "1" ? firstStarted : otherStarted).TrySetResult();
            await gate.Task.WaitAsync(TimeSpan.FromSeconds(20));
        });

        DefaultHttpContext a = live.Context(services, "order:{id}", route: ("id", "1"));
        Task first = middleware.InvokeAsync(a);
        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(10));

        DefaultHttpContext same = live.Context(services, "order:{id}", route: ("id", "1"));
        await middleware.InvokeAsync(same);
        Assert.Equal(409, same.Response.StatusCode);

        DefaultHttpContext other = live.Context(services, "order:{id}", route: ("id", "2"));
        Task otherRun = middleware.InvokeAsync(other);
        await otherStarted.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.False(otherRun.IsCompleted); // got its own lock and is running

        DefaultHttpContext missing = live.Context(services, "order:{id}");
        await middleware.InvokeAsync(missing);
        Assert.Equal(400, missing.Response.StatusCode);

        gate.SetResult();
        await Task.WhenAll(first, otherRun);
    }

    [Fact(Timeout = 30000)]
    public async Task RequestAbortedTokenFiresWhenTheClientDisconnectsAndIsRestoredAfterwards()
    {
        if (await Live.ConnectAsync() is not { } live)
            return;

        await using RuvioConnection connection = live.Connection;
        using ServiceProvider services = live.Services();
        using var disconnect = new CancellationTokenSource();
        DefaultHttpContext context = live.Context(services, "abort");

        context.RequestAborted = disconnect.Token;
        CancellationToken inside = default;
        var middleware = new RuvioLockMiddleware(_ =>
        {
            inside = context.RequestAborted;
            disconnect.Cancel();

            return Task.CompletedTask;
        });

        await middleware.InvokeAsync(context);

        Assert.NotEqual(disconnect.Token, inside); // downstream saw the linked token
        Assert.True(inside.IsCancellationRequested);
        Assert.Equal(disconnect.Token, context.RequestAborted);
    }

    [Fact(Timeout = 30000)]
    public async Task LockLossCancelsTheRequestToken()
    {
        if (await Live.ConnectAsync() is not { } live)
            return;

        await using RuvioConnection connection = live.Connection;
        // A 1 s lease; deleting the key under it makes the next renewal find another owner (none).
        using ServiceProvider services = live.Services();
        DefaultHttpContext context = live.Context(services, "lost", leaseSeconds: 1);
        string key = live.Prefix + "lost";
        var middleware = new RuvioLockMiddleware(async http =>
        {
            await live.Client.ExecuteAsync(["DEL", key]);
            // Ends as soon as the renewal notices the loss; the bound only stops a missed cancellation from hanging.
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => Task.Delay(Timeout.Infinite, http.RequestAborted).WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.True(http.RequestAborted.IsCancellationRequested);
        });

        await middleware.InvokeAsync(context);
    }

    private sealed class Live
    {
        public required RuvioConnection Connection { get; init; }
        public required IRuvioClient Client { get; init; }
        public string Prefix { get; } = "test:mw:" + Guid.NewGuid().ToString("N") + ":";

        public static async Task<Live?> ConnectAsync()
        {
            string? address = Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR");

            if (string.IsNullOrWhiteSpace(address))
                return null;

            var uri = new Uri("tcp://" + address);
            RuvioConnection connection = await RuvioConnection.ConnectAsync(uri.Host, uri.Port);

            return new Live { Connection = connection, Client = await connection.GetDefaultDatabaseAsync() };
        }

        public ServiceProvider Services()
        {
            var services = new ServiceCollection();

            services.AddSingleton(Client);
            services.AddRuvioDistributedLock(options => options.KeyPrefix = Prefix);

            return services.BuildServiceProvider();
        }

        public DefaultHttpContext Context(IServiceProvider services, string resource, (string, string)? route = null, int leaseSeconds = 30)
        {
            var context = new DefaultHttpContext { RequestServices = services };

            if (route is { } r)
                context.Request.RouteValues[r.Item1] = r.Item2;

            context.SetEndpoint(new Endpoint(_ => Task.CompletedTask,
                new EndpointMetadataCollection(new RuvioLockAttribute(resource, leaseSeconds)), "test"));

            return context;
        }
    }
}
