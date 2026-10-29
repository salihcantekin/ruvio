using System.Security.Claims;
using System.Text;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.Idempotency.Tests;

public class HttpOptionsTests
{
    private static IdempotencyOptions Defaults(Action<IdempotencyOptions>? configure = null)
    {
        var options = new IdempotencyOptions { Namespace = "tests", ScopeSelector = _ => "tenant" };

        configure?.Invoke(options);

        return options;
    }

    private static DefaultHttpContext Context(IRuvioClient client, string body = "hello", string? key = "key-1",
        string header = "Idempotency-Key", RuvioIdempotencyAttribute? endpoint = null)
    {
        var c = new DefaultHttpContext();

        c.User = new ClaimsPrincipal(new ClaimsIdentity([new Claim(ClaimTypes.NameIdentifier, "user")], "test"));
        c.Request.Method = "POST";
        c.Request.Path = "/orders";
        c.Request.ContentType = "application/json";

        if (key is not null)
            c.Request.Headers[header] = key;

        c.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes(body));
        c.Response.Body = new MemoryStream();
        c.SetEndpoint(new Endpoint(_ => Task.CompletedTask,
            new EndpointMetadataCollection(endpoint ?? new RuvioIdempotencyAttribute()), "test"));
        c.RequestServices = new ServiceCollection().AddSingleton(client).BuildServiceProvider();

        return c;
    }

    private static IdempotencyMiddleware Middleware(RequestDelegate next, IdempotencyOptions? options = null)
        => new(next, Options.Create(options ?? Defaults()));

    private static string Body(HttpContext c) => Encoding.UTF8.GetString(((MemoryStream)c.Response.Body).ToArray());

    [Fact(Timeout = 30000)]
    public async Task ARunningRequestAnswers409WithRetryAfterAndAMismatchUsesTheConfiguredStatus()
    {
        var (client, _) = StubClient.Create();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var middleware = Middleware(async _ => { entered.SetResult(); await release.Task.WaitAsync(TimeSpan.FromSeconds(20)); },
            Defaults(o => o.MismatchStatusCode = 422));

        Task first = middleware.InvokeAsync(Context(client));

        await entered.Task;
        var running = Context(client);
        var different = Context(client, body: "other");

        await middleware.InvokeAsync(running);
        await middleware.InvokeAsync(different);
        release.SetResult();
        await first;

        Assert.Equal(409, running.Response.StatusCode);
        Assert.Equal("1", running.Response.Headers.RetryAfter.ToString());
        Assert.Equal(422, different.Response.StatusCode);
        Assert.False(different.Response.Headers.ContainsKey("Retry-After"));
    }

    [Fact(Timeout = 30000)]
    public async Task TheMismatchStatusStays409ByDefault()
    {
        var (client, _) = StubClient.Create();
        var middleware = Middleware(_ => Task.CompletedTask);

        await middleware.InvokeAsync(Context(client));
        var different = Context(client, body: "other");

        await middleware.InvokeAsync(different);
        Assert.Equal(409, different.Response.StatusCode);
    }

    [Fact(Timeout = 30000)]
    public async Task AReplayIsMarkedAndCarriesLocationOnlyWhenConfigured()
    {
        foreach (bool withLocation in new[] { false, true })
        {
            var (client, _) = StubClient.Create();
            var options = Defaults(o =>
            {
                if (withLocation)
                    o.ReplayHeaders.Add("Location");
            });
            var middleware = Middleware(async c =>
            {
                c.Response.StatusCode = 201;
                c.Response.Headers.Location = "/orders/7";
                await c.Response.WriteAsync("created");
            }, options);

            var first = Context(client);

            await middleware.InvokeAsync(first);
            Assert.False(first.Response.Headers.ContainsKey("Idempotent-Replayed"));
            Assert.Equal("/orders/7", first.Response.Headers.Location.ToString());

            var replay = Context(client);

            await middleware.InvokeAsync(replay);
            Assert.Equal(201, replay.Response.StatusCode);
            Assert.Equal("created", Body(replay));
            Assert.Equal("true", replay.Response.Headers["Idempotent-Replayed"].ToString());
            Assert.Equal(withLocation, replay.Response.Headers.ContainsKey("Location"));
        }
    }

    [Theory]
    [InlineData("Set-Cookie")]
    [InlineData("authorization")]
    [InlineData("Content-Length")]
    [InlineData("Bad Header")]
    [InlineData("")]
    public void RefusedOrMalformedReplayHeadersFailValidation(string header)
    {
        var options = Defaults(o => o.ReplayHeaders.Add(header));

        Assert.Throws<OptionsValidationException>(() => Middleware(_ => Task.CompletedTask, options));
    }

    [Fact]
    public void OutOfRangeOptionsFailValidation()
    {
        Assert.Throws<OptionsValidationException>(() => Middleware(_ => Task.CompletedTask, Defaults(o => o.MismatchStatusCode = 500)));
        Assert.Throws<OptionsValidationException>(() => Middleware(_ => Task.CompletedTask, Defaults(o => o.PendingWait = TimeSpan.FromSeconds(31))));
        Assert.Throws<OptionsValidationException>(() => Middleware(_ => Task.CompletedTask, Defaults(o => o.PendingWait = TimeSpan.FromSeconds(-1))));
        Assert.Throws<OptionsValidationException>(() => Middleware(_ => Task.CompletedTask, Defaults(o => o.KeyHeader = "X Key")));
        Middleware(_ => Task.CompletedTask, Defaults(o => { o.MismatchStatusCode = 422; o.PendingWait = TimeSpan.FromSeconds(30); }));
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);

        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "The condition was not reached in time.");
            await Task.Yield();
        }
    }

    [Fact(Timeout = 30000)]
    public async Task AWaitingRequestReplaysTheFirstResultAndTheHandlerRunsOnce()
    {
        var (client, stub) = StubClient.Create();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int calls = 0;
        var middleware = Middleware(async c =>
        {
            Interlocked.Increment(ref calls);
            entered.SetResult();
            await release.Task.WaitAsync(TimeSpan.FromSeconds(20));
            await c.Response.WriteAsync("done");
        }, Defaults(o => o.PendingWait = TimeSpan.FromSeconds(10)));

        Task first = middleware.InvokeAsync(Context(client));

        await entered.Task;
        var waiting = Context(client);
        Task second = middleware.InvokeAsync(waiting);

        // The waiter's own BEGIN has been answered once the stub counts a second acquisition.
        await WaitUntilAsync(() => stub.Acquisitions >= 2);
        Assert.False(second.IsCompleted);
        int pollsWhileRunning = stub.Acquisitions;
        release.SetResult();
        await first;
        await second.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(1, calls);
        Assert.Equal(200, waiting.Response.StatusCode);
        Assert.Equal("done", Body(waiting));
        Assert.Equal("true", waiting.Response.Headers["Idempotent-Replayed"].ToString());
        Assert.InRange(pollsWhileRunning, 2, 60); // the first BEGIN, the waiter's own and its polls (growing delay, capped at 25 ms)
    }

    [Fact(Timeout = 30000)]
    public async Task AWaitThatRunsOutAnswers409()
    {
        var (client, _) = StubClient.Create();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var middleware = Middleware(async _ => { entered.SetResult(); await release.Task.WaitAsync(TimeSpan.FromSeconds(20)); });

        Task first = middleware.InvokeAsync(Context(client));

        await entered.Task;
        var waiting = Context(client, endpoint: new RuvioIdempotencyAttribute { PendingWaitMilliseconds = 40 });

        await middleware.InvokeAsync(waiting);
        release.SetResult();
        await first;

        Assert.Equal(409, waiting.Response.StatusCode);
        Assert.Equal("1", waiting.Response.Headers.RetryAfter.ToString());
    }

    [Fact(Timeout = 30000)]
    public async Task AWaitingRequestStopsWhenTheCallerGoesAway()
    {
        var (client, _) = StubClient.Create();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var middleware = Middleware(async _ => { entered.SetResult(); await release.Task.WaitAsync(TimeSpan.FromSeconds(20)); },
            Defaults(o => o.PendingWait = TimeSpan.FromSeconds(30)));

        Task first = middleware.InvokeAsync(Context(client));

        await entered.Task;
        using var gone = new CancellationTokenSource(TimeSpan.FromMilliseconds(30));
        var waiting = Context(client);

        waiting.RequestAborted = gone.Token;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => middleware.InvokeAsync(waiting).WaitAsync(TimeSpan.FromSeconds(5)));
        release.SetResult();
        await first;
    }

    [Fact(Timeout = 30000)]
    public async Task EndpointSettingsOverrideTheGlobalOnes()
    {
        var (client, stub) = StubClient.Create();
        int calls = 0;
        var middleware = Middleware(_ => { calls++; return Task.CompletedTask; });
        var endpoint = new RuvioIdempotencyAttribute { RetentionSeconds = 300, KeyHeader = "X-Request-Id" };

        var named = Context(client, header: "X-Request-Id", endpoint: endpoint);

        await middleware.InvokeAsync(named);
        Assert.Equal(200, named.Response.StatusCode);
        Assert.Equal(300_000, stub.LastTtl);

        // The default header is not read on that endpoint.
        var wrongHeader = Context(client, endpoint: endpoint);

        await middleware.InvokeAsync(wrongHeader);
        Assert.Equal(400, wrongHeader.Response.StatusCode);
        Assert.Equal(1, calls);
    }

    [Fact(Timeout = 30000)]
    public async Task AnOptionalKeyLetsARequestWithoutOneRunUnchecked()
    {
        var (client, stub) = StubClient.Create();
        int calls = 0;
        var middleware = Middleware(_ => { calls++; return Task.CompletedTask; });
        var optional = new RuvioIdempotencyAttribute { KeyRequired = false };

        await middleware.InvokeAsync(Context(client, key: null, endpoint: optional));
        await middleware.InvokeAsync(Context(client, key: null, endpoint: optional));
        Assert.Equal(2, calls);
        Assert.Empty(stub.Records);

        // With the key, the same endpoint is checked.
        await middleware.InvokeAsync(Context(client, endpoint: optional));
        await middleware.InvokeAsync(Context(client, endpoint: optional));
        Assert.Equal(3, calls);
        Assert.Single(stub.Records);

        var required = Context(client, key: null);

        await middleware.InvokeAsync(required);
        Assert.Equal(400, required.Response.StatusCode);
    }

    [Theory(Timeout = 30000)]
    [InlineData(-1, null, -1)]
    [InlineData(604_801, null, -1)]
    [InlineData(0, "bad header", -1)]
    [InlineData(0, null, 30_001)]
    [InlineData(0, null, -2)]
    public async Task InvalidEndpointSettingsThrow(int retention, string? header, int wait)
    {
        var (client, _) = StubClient.Create();
        var middleware = Middleware(_ => Task.CompletedTask);
        var endpoint = new RuvioIdempotencyAttribute { RetentionSeconds = retention, KeyHeader = header, PendingWaitMilliseconds = wait };

        await Assert.ThrowsAsync<InvalidOperationException>(() => middleware.InvokeAsync(Context(client, endpoint: endpoint)));
    }

    [Fact]
    public void TheConfiguredEndpointExtensionAddsTheAttribute()
    {
        var builder = new TestBuilder();

        builder.WithRuvioIdempotency(a => a.RetentionSeconds = 60);
        var attribute = Assert.IsType<RuvioIdempotencyAttribute>(Assert.Single(builder.Metadata));

        Assert.Equal(60, attribute.RetentionSeconds);
    }

    private sealed class TestBuilder : Microsoft.AspNetCore.Builder.IEndpointConventionBuilder
    {
        public List<object> Metadata { get; } = [];

        public void Add(Action<Microsoft.AspNetCore.Builder.EndpointBuilder> convention)
        {
            var endpoint = new Microsoft.AspNetCore.Routing.RouteEndpointBuilder(_ => Task.CompletedTask,
                Microsoft.AspNetCore.Routing.Patterns.RoutePatternFactory.Parse("/"), 0);

            convention(endpoint);
            Metadata.AddRange(endpoint.Metadata);
        }
    }
}
