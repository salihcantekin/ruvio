using System.Reflection;
using System.Diagnostics;
using System.Security.Claims;
using System.Text;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.Idempotency.Tests;

public class IdempotencyTests
{
    private static IdempotencyOptions Options() => new()
    {
        Namespace = "tests",
        ScopeSelector = c => c.User.FindFirst("tenant")?.Value,
        MaxRequestBodyBytes = 32,
        MaxResponseBodyBytes = 32
    };

    private static DefaultHttpContext Context(IRuvioClient? client = null, string body = "hello",
        string user = "user-a", string tenant = "tenant-a", bool selected = true)
    {
        var c = new DefaultHttpContext();

        c.User = new ClaimsPrincipal(new ClaimsIdentity(
            [new Claim(ClaimTypes.NameIdentifier, user), new Claim("tenant", tenant)], "test"));
        c.Request.Method = "POST";
        c.Request.Path = "/orders";
        c.Request.ContentType = "application/json";
        c.Request.Headers["Idempotency-Key"] = "key-1";
        c.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes(body));
        c.Response.Body = new MemoryStream();
        c.SetEndpoint(new Endpoint(_ => Task.CompletedTask,
            new EndpointMetadataCollection(selected ? [new RuvioIdempotencyAttribute()] : []), "test"));
        var services = new ServiceCollection();

        if (client is not null)
            services.AddSingleton(client);

        c.RequestServices = services.BuildServiceProvider();

        return c;
    }

    private static IdempotencyMiddleware Middleware(RequestDelegate next, IdempotencyOptions? options = null) =>
        new(next, Microsoft.Extensions.Options.Options.Create(options ?? Options()));

    [Fact]
    public async Task UnselectedEndpointDoesNotReadResolveValidateOrWrap()
    {
        var c = Context(selected: false);
        var untouched = new ThrowingStream();

        c.Request.Body = untouched;
        c.RequestServices = new ThrowingProvider();
        c.User = new ClaimsPrincipal();
        c.Request.Method = "GET";
        bool called = false;
        var middleware = Middleware(ctx =>
        {
            called = true;
            Assert.Same(untouched, ctx.Request.Body);

            return Task.CompletedTask;
        });

        await middleware.InvokeAsync(c);
        Assert.True(called);
    }

    [Fact]
    public async Task CompletedResultReplaysOnceWithoutSensitiveHeaders()
    {
        var (client, stub) = StubClient.Create();
        int calls = 0;
        var middleware = Middleware(async c =>
        {
            calls++;
            Assert.Equal("hello", await new StreamReader(c.Request.Body).ReadToEndAsync());
            c.Response.StatusCode = 201;
            c.Response.ContentType = "text/plain";
            c.Response.Headers["ETag"] = "\"abc\"";
            c.Response.Headers["Set-Cookie"] = "secret=1";
            c.Response.Headers["Authorization"] = "secret";
            c.Response.Headers["Content-Security-Policy"] = "default-src 'none'";
            c.Response.OnStarting(() =>
            {
                c.Response.Headers["Content-Language"] = "en";

                return Task.CompletedTask;
            });

            await c.Response.Body.WriteAsync("result"u8.ToArray());
        });

        var first = Context(client);
        Stream originalRequest = first.Request.Body, originalResponse = first.Response.Body;

        await middleware.InvokeAsync(first);
        Assert.Same(originalRequest, first.Request.Body);
        Assert.Same(originalResponse, first.Response.Body);
        var second = Context(client);

        await middleware.InvokeAsync(second);
        Assert.Equal(1, calls);
        Assert.Equal(201, second.Response.StatusCode);
        Assert.Equal("result", Encoding.UTF8.GetString(((MemoryStream)second.Response.Body).ToArray()));
        Assert.Equal("en", second.Response.Headers.ContentLanguage);
        Assert.Equal("\"abc\"", second.Response.Headers.ETag);
        Assert.False(second.Response.Headers.ContainsKey("Set-Cookie"));
        Assert.False(second.Response.Headers.ContainsKey("Authorization"));
        Assert.False(second.Response.Headers.ContainsKey("Content-Security-Policy"));
        Assert.Equal(6, second.Response.ContentLength);
        Assert.Single(stub.Records);
        Assert.DoesNotContain("secret", stub.Records.Values.Single().Result!);
        Assert.Matches("^ruvio:http-idempotency:v1:\\{[A-F0-9]{64}\\}$", stub.Records.Keys.Single());
    }

    [Theory]
    [InlineData("body")]
    [InlineData("method")]
    [InlineData("path")]
    [InlineData("pathbase")]
    [InlineData("query")]
    [InlineData("type")]
    public async Task ChangedRequestFingerprintConflicts(string change)
    {
        var (client, _) = StubClient.Create();
        int calls = 0;
        var middleware = Middleware(_ => { calls++; return Task.CompletedTask; });

        await middleware.InvokeAsync(Context(client));
        var changed = Context(client, body: change == "body" ? "other" : "hello");

        switch (change)
        {
            case "method": changed.Request.Method = "PUT"; break;
            case "path": changed.Request.Path = "/other"; break;
            case "pathbase": changed.Request.PathBase = "/v2"; break;
            case "query": changed.Request.QueryString = new QueryString("?x=1"); break;
            case "type": changed.Request.ContentType = "text/plain"; break;
        }

        await middleware.InvokeAsync(changed);
        Assert.Equal(409, changed.Response.StatusCode);
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task SameKeyIsIsolatedByPrincipalTenantAndNamespace()
    {
        var (client, stub) = StubClient.Create();
        int calls = 0;
        RequestDelegate handler = _ => { calls++; return Task.CompletedTask; };

        var middleware = Middleware(handler);

        await middleware.InvokeAsync(Context(client));
        await middleware.InvokeAsync(Context(client, user: "user-b"));
        await middleware.InvokeAsync(Context(client, tenant: "tenant-b"));
        var otherOptions = Options();

        otherOptions.Namespace = "other-app";
        await Middleware(handler, otherOptions).InvokeAsync(Context(client));
        Assert.Equal(4, calls);
        Assert.Equal(4, stub.Records.Count);
    }

    [Fact]
    public async Task ConcurrentPendingRequestConflictsAndNeverRunsHandler()
    {
        var (client, _) = StubClient.Create();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int calls = 0;
        var middleware = Middleware(async _ => { calls++; entered.SetResult(); await release.Task; });

        Task first = middleware.InvokeAsync(Context(client));

        await entered.Task;
        var duplicate = Context(client);

        await middleware.InvokeAsync(duplicate);
        Assert.Equal(409, duplicate.Response.StatusCode);
        release.SetResult();
        await first;
        Assert.Equal(1, calls);
    }

    [Theory]
    [InlineData("handler")]
    [InlineData("store")]
    [InlineData("cancellation")]
    [InlineData("ownership")]
    [InlineData("overflow")]
    [InlineData("caught-overflow")]
    [InlineData("writer-overflow")]
    [InlineData("headers")]
    [InlineData("starting")]
    public async Task FailurePropagatesRestoresBodiesAndLeavesPending(string failure)
    {
        var (client, stub) = StubClient.Create();

        if (failure == "store")
            stub.FailCompletion = true;

        if (failure == "ownership")
            stub.LoseOwnership = true;

        using var cts = new CancellationTokenSource();
        var c = Context(client);

        c.RequestAborted = cts.Token;
        var requestBody = c.Request.Body;
        var responseBody = c.Response.Body;
        var responseFeature = c.Features.Get<IHttpResponseFeature>();
        var middleware = Middleware(async ctx =>
        {
            if (failure == "handler")
                throw new InvalidOperationException("handler failed");

            if (failure == "cancellation") { cts.Cancel(); ctx.RequestAborted.ThrowIfCancellationRequested(); }

            if (failure == "headers")
                ctx.Response.ContentType = new string('a', 8193);

            if (failure == "starting")
                ctx.Response.OnStarting(() => throw new InvalidOperationException("starting"));

            if (failure == "writer-overflow")
                ctx.Response.BodyWriter.GetMemory(33);

            if (failure is "overflow" or "caught-overflow")
            {
                try { await ctx.Response.Body.WriteAsync(new byte[33]); }
                catch (IOException) when (failure == "caught-overflow") { }
            }
            else
                await ctx.Response.Body.WriteAsync("ok"u8.ToArray());
        });

        var error = await Record.ExceptionAsync(() => middleware.InvokeAsync(c));

        Assert.NotNull(error);

        if (failure == "cancellation")
            Assert.IsAssignableFrom<OperationCanceledException>(error);

        Assert.Same(requestBody, c.Request.Body);
        Assert.Same(responseBody, c.Response.Body);
        Assert.Same(responseFeature, c.Features.Get<IHttpResponseFeature>());
        Assert.Equal(0, responseBody.Length);
        Assert.Equal("pending", stub.Records.Values.Single().State);
        var duplicate = Context(client);

        await Middleware(_ => throw new Exception("Must not execute")).InvokeAsync(duplicate);
        Assert.Equal(409, duplicate.Response.StatusCode);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OversizedRequestNeverAcquiresOrExecutes(bool contentLengthKnown)
    {
        var (client, stub) = StubClient.Create();
        var c = Context(client, new string('x', 33));

        if (contentLengthKnown)
            c.Request.ContentLength = 33;

        var original = c.Request.Body;

        await Middleware(_ => throw new Exception("Must not execute")).InvokeAsync(c);
        Assert.Equal(413, c.Response.StatusCode);
        Assert.Same(original, c.Request.Body);
        Assert.Empty(stub.Records);
        Assert.Equal(contentLengthKnown ? 0 : 33, original.Position);
    }

    [Fact]
    public async Task ExactLimitsAndResponseWriterAreSupported()
    {
        var (client, _) = StubClient.Create();
        var c = Context(client, new string('x', 32));
        var original = c.Response.Body;

        await Middleware(async ctx =>
        {
            var memory = ctx.Response.BodyWriter.GetMemory(32);

            memory.Span[..32].Fill((byte)'x');
            ctx.Response.BodyWriter.Advance(32);
            await ctx.Response.BodyWriter.FlushAsync();
            await ctx.Response.StartAsync();
            Assert.Equal(0, original.Length);
        }).InvokeAsync(c);

        Assert.Equal(32, c.Response.Body.Length);
    }

    [Theory]
    [InlineData("GET", "valid", 405)]
    [InlineData("HEAD", "valid", 405)]
    [InlineData("POST", "", 400)]
    [InlineData("POST", "has space", 400)]
    [InlineData("POST", "nonascii-ü", 400)]
    public async Task InvalidMethodOrKeyIsRejectedBeforeStore(string method, string key, int status)
    {
        var c = Context();

        c.Request.Method = method;
        c.Request.Headers["Idempotency-Key"] = key;
        await Middleware(_ => throw new Exception("Must not execute")).InvokeAsync(c);
        Assert.Equal(status, c.Response.StatusCode);
    }

    [Fact]
    public async Task MissingSubjectOrScopeFailsClosed()
    {
        var c = Context();

        c.User = new ClaimsPrincipal();
        await Middleware(_ => throw new Exception("Must not execute")).InvokeAsync(c);
        Assert.Equal(401, c.Response.StatusCode);
        c = Context(tenant: "");
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            Middleware(_ => throw new Exception("Must not execute")).InvokeAsync(c));
    }

    [Fact]
    public async Task CustomIdentitySelectorSupportsUnauthenticatedCallersAndIsolatesThem()
    {
        var (client, stub) = StubClient.Create();
        var options = Options();

        options.IdentitySelector = context => context.Items["client"] as string;
        int calls = 0;
        var middleware = Middleware(_ => { calls++; return Task.CompletedTask; }, options);
        var first = Context(client);

        first.User = new ClaimsPrincipal(new ClaimsIdentity([new Claim("tenant", "tenant-a")]));
        first.Items["client"] = "client-a";
        await middleware.InvokeAsync(first);
        var replay = Context(client);

        replay.User = new ClaimsPrincipal(new ClaimsIdentity([new Claim("tenant", "tenant-a")]));
        replay.Items["client"] = "client-a";
        await middleware.InvokeAsync(replay);
        var otherCaller = Context(client);

        otherCaller.User = new ClaimsPrincipal(new ClaimsIdentity([new Claim("tenant", "tenant-a")]));
        otherCaller.Items["client"] = "client-b";
        await middleware.InvokeAsync(otherCaller);

        Assert.Equal(2, calls);
        Assert.Equal(2, stub.Records.Count);
        Assert.Equal(200, replay.Response.StatusCode);
    }

    [Fact]
    public async Task AnonymousIdentityRequiresExplicitOptIn()
    {
        var (client, stub) = StubClient.Create();
        var options = Options();
        options.AllowAnonymous = true;
        int calls = 0;
        var middleware = Middleware(_ => { calls++; return Task.CompletedTask; }, options);
        var first = Context(client);

        first.User = new ClaimsPrincipal(new ClaimsIdentity([new Claim("tenant", "tenant-a")]));
        await middleware.InvokeAsync(first);
        var replay = Context(client);

        replay.User = new ClaimsPrincipal(new ClaimsIdentity([new Claim("tenant", "tenant-a")]));
        await middleware.InvokeAsync(replay);

        Assert.Equal(1, calls);
        Assert.Single(stub.Records);
        Assert.Equal(200, replay.Response.StatusCode);
    }

    [Fact]
    public async Task AuthorizationIsRecheckedBeforeReplay()
    {
        var (client, stub) = StubClient.Create();
        var services = new ServiceCollection();

        services.AddLogging();
        services.AddRouting();
        services.AddSingleton(client);
        services.AddRuvioIdempotency(o => { o.Namespace = "tests"; o.ScopeSelector = _ => "tenant"; });

        services.AddAuthentication("test").AddScheme<AuthenticationSchemeOptions, TestAuthenticationHandler>("test", _ => { });

        services.AddAuthorization();
        using var provider = services.BuildServiceProvider();
        var app = new ApplicationBuilder(provider);

        app.UseAuthentication();
        app.UseAuthorization();
        app.UseRuvioIdempotency();
        int calls = 0;

        app.Run(async c => { calls++; await c.Response.Body.WriteAsync("allowed"u8.ToArray()); });

        RequestDelegate pipeline = app.Build();
        using var allowedScope = provider.CreateScope();
        using var deniedScope = provider.CreateScope();
        var allowed = Context(client);

        ((ClaimsIdentity)allowed.User.Identity!).AddClaim(new Claim(ClaimTypes.Role, "writer"));
        allowed.RequestServices = allowedScope.ServiceProvider;
        var metadata = new EndpointMetadataCollection(new RuvioIdempotencyAttribute(), new AuthorizeAttribute { Roles = "writer" });

        allowed.SetEndpoint(new Endpoint(_ => Task.CompletedTask, metadata, "authorized"));
        await pipeline(allowed);
        Assert.Equal(1, calls);
        var denied = Context(client);

        denied.RequestServices = deniedScope.ServiceProvider;
        denied.SetEndpoint(new Endpoint(_ => Task.CompletedTask, metadata, "authorized"));
        await pipeline(denied);
        Assert.Equal(403, denied.Response.StatusCode);
        Assert.Equal(0, denied.Response.Body.Length);
        Assert.Equal(1, calls);
        Assert.Equal(1, stub.Acquisitions);
    }

    [Fact]
    public void OptionsRejectOutOfBoundsConfiguration()
    {
        foreach (TimeSpan ttl in new[] { TimeSpan.Zero, TimeSpan.FromTicks(1), TimeSpan.FromMilliseconds(-1),
                     TimeSpan.MaxValue, TimeSpan.FromDays(8) })
        {
            var o = Options();

            o.Retention = ttl;
            Assert.Throws<OptionsValidationException>(() => Middleware(_ => Task.CompletedTask, o));
        }

        foreach (int limit in new[] { 0, -1, IdempotencyOptions.MaximumPayloadBytes + 1 })
        {
            var o = Options();

            o.MaxRequestBodyBytes = limit;
            Assert.False(o.IsValid());
            o = Options();
            o.MaxResponseBodyBytes = limit;
            Assert.False(o.IsValid());
        }

        var missing = Options();

        missing.ScopeSelector = null;
        Assert.False(missing.IsValid());
        missing = Options();
        missing.Namespace = "";
        Assert.False(missing.IsValid());
    }

    [Fact]
    public async Task ReturnedErrorResponseIsReplayedButNotReexecuted()
    {
        var (client, _) = StubClient.Create();
        int calls = 0;
        var middleware = Middleware(async c =>
        {
            calls++;
            c.Response.StatusCode = 422;
            await c.Response.Body.WriteAsync("invalid"u8.ToArray());
        });

        await middleware.InvokeAsync(Context(client));
        var replay = Context(client);

        await middleware.InvokeAsync(replay);
        Assert.Equal(422, replay.Response.StatusCode);
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task CorruptRecordIsNotReplayed()
    {
        var (client, stub) = StubClient.Create();
        var middleware = Middleware(_ => Task.CompletedTask);

        await middleware.InvokeAsync(Context(client));
        var entry = stub.Records.Values.Single();

        entry.Result = """{"Status":200,"Headers":{"Set-Cookie":"secret"},"Body":""}""";
        var c = Context(client);

        await Assert.ThrowsAsync<IOException>(() => middleware.InvokeAsync(c));
        Assert.Equal(0, c.Response.Body.Length);
        Assert.False(c.Response.Headers.ContainsKey("Set-Cookie"));
    }

    [Fact]
    public async Task InstrumentationIsOptInAndContainsNoRequestData()
    {
        var seen = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "Ruvio.AspNetCore.Idempotency",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = activity => seen.Add(activity)
        };

        ActivitySource.AddActivityListener(listener);
        var (client, _) = StubClient.Create();
        await Middleware(_ => Task.CompletedTask).InvokeAsync(Context(client));
        Assert.Empty(seen);
        var options = Options();

        options.EnableInstrumentation = true;
        await Middleware(_ => Task.CompletedTask, options).InvokeAsync(Context(client, selected: false));
        Assert.Empty(seen);
        await Middleware(_ => Task.CompletedTask, options).InvokeAsync(Context(client));
        Assert.Equal("completed", Assert.Single(Assert.Single(seen).Tags).Value);
    }

    [Fact]
    public void UnselectedPathHasNoPerRequestAllocations()
    {
        var c = Context(selected: false);
        var middleware = Middleware(_ => Task.CompletedTask);

        for (int i = 0; i < 100; i++)
            _ = middleware.InvokeAsync(c);

        long before = GC.GetAllocatedBytesForCurrentThread();

        for (int i = 0; i < 100; i++)
            _ = middleware.InvokeAsync(c);

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Equal(0, allocated);
    }

    [Fact]
    public async Task MaximumBinaryPayloadRoundtripsWithBoundedSerialization()
    {
        var (client, stub) = StubClient.Create();
        var options = Options();

        options.MaxResponseBodyBytes = IdempotencyOptions.MaximumPayloadBytes;
        var bytes = Enumerable.Repeat((byte)251, options.MaxResponseBodyBytes).ToArray();
        var middleware = Middleware(c => c.Response.Body.WriteAsync(bytes).AsTask(), options);

        await middleware.InvokeAsync(Context(client));
        var replay = Context(client);

        await middleware.InvokeAsync(replay);
        Assert.Equal(bytes, ((MemoryStream)replay.Response.Body).ToArray());
        Assert.True(stub.Records.Values.Single().Result!.Length < 1_500_000);
    }

    [Fact]
    public async Task SendFileChecksLimitBeforeWrite()
    {
        var response = new BoundedResponseBody(32);

        // This existing test assembly is much larger than the response limit.
        await Assert.ThrowsAsync<IOException>(() => response.SendFileAsync(typeof(IdempotencyTests).Assembly.Location, 0, null));
        Assert.True(response.Buffer.Failed);
        Assert.Equal(0, response.Buffer.Length);
    }

    [Fact]
    public async Task LostCompletionAcknowledgmentDoesNotRetryAndCanReplayCommittedResult()
    {
        var (client, stub) = StubClient.Create();
        stub.FailAfterCompletion = true;
        int calls = 0;
        var middleware = Middleware(async c =>
        {
            calls++;
            await c.Response.Body.WriteAsync("saved"u8.ToArray());
        });

        var failed = Context(client);

        await Assert.ThrowsAsync<IOException>(() => middleware.InvokeAsync(failed));
        Assert.Equal(0, failed.Response.Body.Length);
        Assert.Equal("completed", stub.Records.Values.Single().State);
        var replay = Context(client);

        await middleware.InvokeAsync(replay);
        Assert.Equal("saved", Encoding.UTF8.GetString(((MemoryStream)replay.Response.Body).ToArray()));
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task AmbiguousAcquisitionErrorPropagatesWithoutRunningHandler()
    {
        var (client, stub) = StubClient.Create();
        stub.FailAfterAcquisition = true;
        var middleware = Middleware(_ => throw new Exception("Must not execute"));
        var failed = Context(client);

        await Assert.ThrowsAsync<IOException>(() => middleware.InvokeAsync(failed));
        Assert.Equal(0, failed.Response.Body.Length);
        Assert.Equal("pending", stub.Records.Values.Single().State);
        var retry = Context(client);

        await middleware.InvokeAsync(retry);
        Assert.Equal(409, retry.Response.StatusCode);
    }

    [Fact]
    public async Task MultipleAndLongKeysFailBeforeAcquisition()
    {
        var (client, stub) = StubClient.Create();
        var middleware = Middleware(_ => throw new Exception("Must not execute"));
        var c = Context(client);

        c.Request.Headers["Idempotency-Key"] = new[] { "one", "two" };

        await middleware.InvokeAsync(c);
        Assert.Equal(400, c.Response.StatusCode);
        c = Context(client);
        c.Request.Headers["Idempotency-Key"] = new string('x', 129);
        await middleware.InvokeAsync(c);
        Assert.Equal(400, c.Response.StatusCode);
        Assert.Equal(0, stub.Acquisitions);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task StartFlushAndCompleteCannotPublishBeforePersistence(bool persist)
    {
        var (client, stub) = StubClient.Create();
        var reachedStore = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var persistence = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        stub.BeforeCompletion = async () =>
        {
            reachedStore.SetResult();
            await persistence.Task;
        };

        var c = Context(client);
        var outgoing = new ObservedResponseBody();

        c.Features.Set<IHttpResponseBodyFeature>(outgoing);
        var originalMetadata = c.Features.Get<IHttpResponseFeature>()!;

        originalMetadata.StatusCode = 202;
        originalMetadata.ReasonPhrase = "Original";
        c.Response.Headers["X-Outer"] = "original";
        c.Response.ContentLength = 99;
        var middleware = Middleware(async ctx =>
        {
            ctx.Response.StatusCode = 201;
            ctx.Features.Get<IHttpResponseFeature>()!.ReasonPhrase = "Handler";
            ctx.Response.Headers["X-Outer"] = "changed";
            ctx.Response.Headers["X-Handler"] = "success";
            ctx.Response.ContentLength = 10;
            await ctx.Response.StartAsync();
            await ctx.Response.Body.WriteAsync("stream"u8.ToArray());
            ctx.Response.Body.Flush();
            await ctx.Response.Body.FlushAsync();
            var memory = ctx.Response.BodyWriter.GetMemory(4);
            "pipe"u8.CopyTo(memory.Span);
            ctx.Response.BodyWriter.Advance(4);
            await ctx.Response.BodyWriter.FlushAsync();
            await ctx.Response.CompleteAsync();
        });

        Task request = middleware.InvokeAsync(c);

        await reachedStore.Task;
        Assert.False(request.IsCompleted);
        Assert.Equal("pending", stub.Records.Values.Single().State);
        Assert.Equal(0, outgoing.Buffer.Length);
        Assert.Equal(0, outgoing.Buffer.FlushCalls);
        Assert.Equal(0, outgoing.StartCalls);
        Assert.Equal(0, outgoing.CompleteCalls);
        Assert.Equal(202, originalMetadata.StatusCode);
        Assert.Equal("Original", originalMetadata.ReasonPhrase);
        Assert.Equal("original", originalMetadata.Headers["X-Outer"]);
        Assert.Equal(99, originalMetadata.Headers.ContentLength);
        Assert.False(originalMetadata.Headers.ContainsKey("X-Handler"));

        if (persist)
        {
            persistence.SetResult();
            await request;
            Assert.Equal("streampipe", Encoding.UTF8.GetString(outgoing.Buffer.ToArray()));
            Assert.Equal(201, c.Response.StatusCode);
            Assert.Equal("Handler", originalMetadata.ReasonPhrase);
            Assert.Equal("changed", c.Response.Headers["X-Outer"]);
            Assert.Equal(10, c.Response.ContentLength);
        }
        else
        {
            persistence.SetException(new IOException("Persistence failed"));
            await Assert.ThrowsAsync<IOException>(() => request);
            Assert.Equal(0, outgoing.Buffer.Length);
            Assert.Equal("pending", stub.Records.Values.Single().State);
            Assert.Equal(202, c.Response.StatusCode);
            Assert.Equal("Original", originalMetadata.ReasonPhrase);
            Assert.Equal("original", c.Response.Headers["X-Outer"]);
            Assert.Equal(99, c.Response.ContentLength);
            Assert.False(c.Response.Headers.ContainsKey("X-Handler"));
        }

        Assert.Same(outgoing, c.Features.Get<IHttpResponseBodyFeature>());
    }

    [Fact]
    public async Task HandlerExceptionPreservesOriginalResponseMetadata()
    {
        var (client, _) = StubClient.Create();
        var c = Context(client);

        c.Response.StatusCode = 202;
        c.Response.Headers["X-Original"] = "keep";
        c.Response.ContentLength = 123;
        var original = c.Features.Get<IHttpResponseFeature>()!;

        original.ReasonPhrase = "Original";
        await Assert.ThrowsAsync<IOException>(() => Middleware(ctx =>
        {
            ctx.Response.StatusCode = 201;
            ctx.Response.Headers.Clear();
            ctx.Response.Headers["Set-Cookie"] = "do-not-leak";
            ctx.Response.ContentLength = 500;
            ctx.Features.Get<IHttpResponseFeature>()!.ReasonPhrase = "Wrong";
            throw new IOException("Handler failed");
        }).InvokeAsync(c));

        Assert.Same(original, c.Features.Get<IHttpResponseFeature>());
        Assert.Equal(202, c.Response.StatusCode);
        Assert.Equal(123, c.Response.ContentLength);
        Assert.Equal("Original", original.ReasonPhrase);
        Assert.Equal("keep", c.Response.Headers["X-Original"]);
        Assert.False(c.Response.Headers.ContainsKey("Set-Cookie"));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(7)]
    [InlineData(8193)]
    public async Task KnownLengthRequestIsReadIntoPooledStorage(int length)
    {
        var input = new MemoryStream(new byte[length]);
        var result = await RequestBuffer.ReadAsync(input, length, IdempotencyOptions.MaximumPayloadBytes, default);

        Assert.Null(result.RejectionStatus);
        Assert.Equal(length, result.Length);
        // The pool may hand out a larger array; only the first Length bytes belong to the request.
        Assert.True(result.Bytes.Length >= length);
        RequestBuffer.Release(result.Bytes);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(4097)]
    [InlineData(8193)]
    public async Task UnknownLengthRequestGrowsOnlyAsNeeded(int length)
    {
        var input = new MemoryStream(new byte[length]);
        var result = await RequestBuffer.ReadAsync(input, null, IdempotencyOptions.MaximumPayloadBytes, default);

        Assert.Null(result.RejectionStatus);
        Assert.Equal(length, result.Length);
        Assert.True(result.Bytes.Length >= length);
        Assert.Equal(new byte[length], result.Bytes.AsSpan(0, length).ToArray());
        RequestBuffer.Release(result.Bytes);
    }

    [Fact]
    public async Task UnknownLengthRequestKeepsEveryByteAcrossGrowth()
    {
        byte[] body = Enumerable.Range(0, 20_000).Select(i => (byte)(i * 31)).ToArray();
        var result = await RequestBuffer.ReadAsync(new MemoryStream(body), null, 1_000_000, default);

        Assert.Null(result.RejectionStatus);
        Assert.Equal(body, result.Bytes.AsSpan(0, result.Length).ToArray());
        RequestBuffer.Release(result.Bytes);
    }

    [Fact]
    public async Task RejectedRequestReturnsNoStorage()
    {
        var result = await RequestBuffer.ReadAsync(new MemoryStream(new byte[50]), null, 10, default);

        Assert.Equal(413, result.RejectionStatus);
        Assert.Empty(result.Bytes);
    }

    [Fact]
    public async Task ResponseBufferGrowsPastItsInitialStorageWithoutLosingBytes()
    {
        var body = new BoundedResponseBody(100_000);
        byte[] expected = Enumerable.Range(0, 70_000).Select(i => (byte)(i * 7)).ToArray();

        // Mix every write API across several growth steps.
        await body.Stream.WriteAsync(expected.AsMemory(0, 10));
        body.Stream.WriteByte(expected[10]);
        body.Stream.Write(expected, 11, 5_000);
        expected.AsMemory(5_011, 30_000).CopyTo(body.Writer.GetMemory(30_000));
        body.Writer.Advance(30_000);
        await body.Stream.WriteAsync(expected.AsMemory(35_011));

        Assert.Equal(expected, body.Buffer.Written.ToArray());
        Assert.Equal(expected.Length, body.Buffer.Length);

        body.Release();
        Assert.Equal(0, body.Buffer.Length);
        Assert.Equal(0, body.Buffer.Written.Length);
    }

    [Fact]
    public void ResponseBufferRejectsAHintBeyondItsLimitEvenWhenStorageIsLarger()
    {
        var body = new BoundedResponseBody(10);

        body.Buffer.Write(new byte[4]);
        // The pool may rent more than 10 bytes; the limit, not the array, decides.
        Assert.Throws<IOException>(() => body.Buffer.Reserve(7));
        Assert.True(body.Buffer.Failed);
        body.Release();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(3)]
    [InlineData(7)]
    public async Task MismatchedKnownRequestLengthFailsBeforeAcquisition(int declared)
    {
        var (client, stub) = StubClient.Create();
        var c = Context(client);

        c.Request.ContentLength = declared;
        await Middleware(_ => throw new Exception("Must not execute")).InvokeAsync(c);
        Assert.Equal(400, c.Response.StatusCode);
        Assert.Equal(0, stub.Acquisitions);
    }

    [Theory]
    [InlineData("byte")]
    [InlineData("span")]
    [InlineData("array")]
    [InlineData("async-array")]
    [InlineData("async-memory")]
    [InlineData("writer-memory")]
    [InlineData("writer-span")]
    [InlineData("writer-advance")]
    public async Task AllResponseWriteApisEnforceCapacity(string api)
    {
        var body = new BoundedResponseBody(8);

        body.Buffer.Write(new byte[8]);
        var exception = await Record.ExceptionAsync(async () =>
        {
            switch (api)
            {
                case "byte": body.Stream.WriteByte(1); break;
                case "span": body.Stream.Write("a"u8); break;
                case "array": body.Stream.Write(new byte[1], 0, 1); break;
                case "async-array": await body.Stream.WriteAsync(new byte[1], 0, 1); break;
                case "async-memory": await body.Stream.WriteAsync(new byte[1].AsMemory()); break;
                case "writer-memory": body.Writer.GetMemory(1); break;
                case "writer-span": body.Writer.GetSpan(1); break;
                case "writer-advance": body.Writer.Advance(1); break;
            }
        });

        Assert.IsType<IOException>(exception);
        Assert.True(body.Buffer.Failed);
        Assert.Equal(8, body.Buffer.Length);
    }

    private sealed class ObservedResponseBody : IHttpResponseBodyFeature
    {
        public ObservedStream Buffer { get; } = new();

        public Stream Stream => Buffer;
        public System.IO.Pipelines.PipeWriter Writer => throw new Exception("Original writer used before persistence");

        public int StartCalls { get; private set; }
        public int CompleteCalls { get; private set; }
        public void DisableBuffering() => throw new Exception("Original buffering disabled");
        public Task StartAsync(CancellationToken cancellationToken = default) { StartCalls++; return Task.CompletedTask; }
        public Task CompleteAsync() { CompleteCalls++; return Task.CompletedTask; }
        public Task SendFileAsync(string path, long offset, long? count, CancellationToken cancellationToken = default) =>
            throw new Exception("Original send-file used before persistence");
    }

    private sealed class ObservedStream : MemoryStream
    {
        public int FlushCalls { get; private set; }
        public override void Flush() => FlushCalls++;
        public override Task FlushAsync(CancellationToken cancellationToken) { FlushCalls++; return Task.CompletedTask; }
    }

    private sealed class ThrowingProvider : IServiceProvider
    {
        public object? GetService(Type serviceType) => throw new Exception("Service resolution on unselected request");
    }

    private sealed class ThrowingStream : MemoryStream
    {
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            throw new Exception("Body read on unselected request");
    }
}

public class TestAuthenticationHandler(IOptionsMonitor<AuthenticationSchemeOptions> options,
    ILoggerFactory logger, UrlEncoder encoder) : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
{
    protected override Task<AuthenticateResult> HandleAuthenticateAsync() =>
        Task.FromResult(AuthenticateResult.Success(new AuthenticationTicket(Context.User, Scheme.Name)));
}

public class StubClient : DispatchProxy
{
    internal sealed class Entry(string fingerprint, string owner)
    {
        internal string Fingerprint = fingerprint;
        internal string Owner = owner;
        internal string State = "pending";
        internal string? Result;
    }

    internal readonly Dictionary<string, Entry> Records = new();
    internal bool FailCompletion;
    internal bool FailAfterCompletion;
    internal bool FailAfterAcquisition;
    internal bool LoseOwnership;
    internal Func<Task>? BeforeCompletion;
    internal int Acquisitions;

    internal static (IRuvioClient Client, StubClient Stub) Create()
    {
        var client = Create<IRuvioClient, StubClient>();

        return (client, (StubClient)client);
    }

    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) =>
        Activator.CreateInstance(targetMethod!.ReturnType, InvokeTask(targetMethod, args));

    private object InvokeTask(MethodInfo targetMethod, object?[]? args)
    {
        string operation = targetMethod.Name;

        Assert.Contains(operation, new[] { nameof(IRuvioClient.BeginIdempotencyAsync), nameof(IRuvioClient.CompleteIdempotencyAsync) });

        ((CancellationToken)args![4]!).ThrowIfCancellationRequested();
        string key = (string)args[0]!;
        string fingerprint = (string)args[1]!;
        string owner = (string)args[2]!;

        lock (Records)
        {
            if (operation == nameof(IRuvioClient.BeginIdempotencyAsync))
            {
                Acquisitions++;

                if (!Records.TryGetValue(key, out var entry))
                {
                    Records[key] = new Entry(fingerprint, owner);

                    if (FailAfterAcquisition)
                        throw new IOException("Acquisition reply lost");

                    return Task.FromResult(Array("acquired"));
                }

                if (entry.Fingerprint != fingerprint)
                    return Task.FromResult(Array("conflict"));

                return Task.FromResult(entry.State == "completed" ? Array("completed", entry.Result!) : Array("pending"));
            }

            if (FailCompletion)
                throw new IOException("Store failed");

            if (LoseOwnership)
                return Task.FromResult(false);

            if (!Records.TryGetValue(key, out var existing) || existing.Owner != owner ||
                existing.Fingerprint != fingerprint)
                return Task.FromResult(false);

            string result = (string)args[3]!;

            if (existing.State == "completed")
                return Task.FromResult(existing.Result == result);

            return CompleteAsync(existing, result);
        }
    }

    private async Task<bool> CompleteAsync(Entry existing, string result)
    {
        if (BeforeCompletion is not null)
            await BeforeCompletion();

        lock (Records)
        {
            existing.State = "completed";
            existing.Result = result;

            if (FailAfterCompletion)
                throw new IOException("Completion reply lost");

            return true;
        }
    }

    private static RespValue Integer(long n) => Factory("FromInteger", n);
    private static RespValue Array(params string[] values) =>
        Factory("Array", values.Select(s => Factory("Bulk", Encoding.UTF8.GetBytes(s))).ToArray());
    private static RespValue Factory(string method, object value) =>
        (RespValue)typeof(RespValue).GetMethod(method, BindingFlags.NonPublic | BindingFlags.Static)!.Invoke(null, [value])!;
}
