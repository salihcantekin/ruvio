using System.Reflection;
using System.Security.Claims;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.DistributedLock.Tests;

/// <summary>Lock behaviour against a scripted client, so error and timing paths run without a server.</summary>
public class OfflineLockTests
{
    public class ScriptClient : DispatchProxy
    {
        public Func<IReadOnlyList<string>, CancellationToken, RespValue> Handler { get; set; } = (_, _) => Resp.Simple("OK");
        public List<string[]> Sent { get; } = [];

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod!.Name != nameof(IRuvioClient.ExecuteAsync) || args![0] is not IReadOnlyList<string> command)
            {
                throw new NotSupportedException(targetMethod.Name);
            }

            lock (Sent)
            {
                Sent.Add([.. command]);
            }

            var token = (CancellationToken)args[1]!;

            try
            {
                return new ValueTask<RespValue>(Handler(command, token));
            }
            catch (Exception exception)
            {
                return ValueTask.FromException<RespValue>(exception);
            }
        }
    }

    private static (IRuvioClient Client, ScriptClient Script) Fake(Func<IReadOnlyList<string>, CancellationToken, RespValue>? handler = null)
    {
        var client = DispatchProxy.Create<IRuvioClient, ScriptClient>();
        var script = (ScriptClient)(object)client;

        if (handler is not null)
        {
            script.Handler = handler;
        }

        return (client, script);
    }

    private static RespValue Ok => Resp.Simple("OK");

    [Fact]
    public void OptionsValidationNamesTheBadSetting()
    {
        var (client, _) = Fake();

        Assert.Throws<ArgumentNullException>(() => new RuvioLockProvider(null!));
        Assert.Throws<ArgumentException>(() => new RuvioLockProvider(client, new RuvioLockOptions { KeyPrefix = "" }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RuvioLockProvider(client, new RuvioLockOptions { DefaultLease = TimeSpan.Zero }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RuvioLockProvider(client, new RuvioLockOptions { DefaultWait = TimeSpan.FromSeconds(-1) }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RuvioLockProvider(client, new RuvioLockOptions { ConflictStatusCode = 399 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RuvioLockProvider(client, new RuvioLockOptions { ConflictStatusCode = 600 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RuvioLockProvider(client, new RuvioLockOptions { RetryMin = TimeSpan.Zero }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RuvioLockProvider(client,
            new RuvioLockOptions { RetryMin = TimeSpan.FromSeconds(2), RetryMax = TimeSpan.FromSeconds(1) }));
        _ = new RuvioLockProvider(client, new RuvioLockOptions { ConflictStatusCode = 400, DefaultLease = TimeSpan.FromMilliseconds(1), RetryMin = TimeSpan.FromMilliseconds(5), RetryMax = TimeSpan.FromMilliseconds(5) });
        _ = new RuvioLockProvider(client, new RuvioLockOptions { ConflictStatusCode = 599 });
        Assert.Throws<ArgumentNullException>(() => new RuvioLockMiddleware(null!));
        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddRuvioDistributedLock());
        Assert.Throws<ArgumentOutOfRangeException>(() => new ServiceCollection().AddRuvioDistributedLock(o => o.DefaultWait = TimeSpan.FromTicks(-1)));
        Assert.Throws<ArgumentNullException>(() => ((Microsoft.AspNetCore.Builder.IApplicationBuilder)null!).UseRuvioLock());
        Assert.Throws<ArgumentNullException>(() => ((HttpContext)null!).GetRuvioLock());
    }

    [Fact]
    public async Task AcquireValidatesInputAndSurfacesServerErrors()
    {
        var (client, script) = Fake();
        var provider = new RuvioLockProvider(client);

        await Assert.ThrowsAsync<ArgumentNullException>(async () => await provider.TryAcquireAsync(null!, TimeSpan.FromSeconds(1)));
        await Assert.ThrowsAsync<ArgumentException>(async () => await provider.TryAcquireAsync("", TimeSpan.FromSeconds(1)));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await provider.TryAcquireAsync("r", TimeSpan.Zero));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await provider.TryAcquireAsync("r", TimeSpan.FromTicks(9999)));
        Assert.Empty(script.Sent);

        script.Handler = (_, _) => Resp.Error("ERR unknown subcommand");
        RuvioException error = await Assert.ThrowsAsync<RuvioException>(async () => await provider.TryAcquireAsync("r", TimeSpan.FromSeconds(1)));
        Assert.True(error.IsServerReply);

        script.Handler = (_, _) => Resp.Error("");
        await Assert.ThrowsAsync<RuvioException>(async () => await provider.AcquireAsync("r", TimeSpan.FromSeconds(1), TimeSpan.Zero));

        script.Handler = (_, _) => Resp.Null();
        TimeoutException timeout = await Assert.ThrowsAsync<TimeoutException>(
            async () => await provider.AcquireAsync("busy", TimeSpan.FromSeconds(1), TimeSpan.Zero));
        Assert.Contains("busy", timeout.Message);
    }

    [Fact]
    public async Task AcquireRetriesWithGrowingPausesUntilTheWaitRunsOutOrTheKeyFrees()
    {
        int attempts = 0;
        var (client, script) = Fake((_, _) => Interlocked.Increment(ref attempts) < 4 ? Resp.Null() : Ok);
        var provider = new RuvioLockProvider(client, new RuvioLockOptions
        {
            KeyPrefix = "p:", RetryMin = TimeSpan.FromMilliseconds(1), RetryMax = TimeSpan.FromMilliseconds(3), AutoRenew = false,
        });

        await using IRuvioLock? held = await provider.TryAcquireAsync("r", TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));

        Assert.NotNull(held);
        Assert.Equal(4, attempts);
        Assert.Equal("r", held!.Resource);
        Assert.Equal("SET", script.Sent[0][0]);
        Assert.Equal("p:r", script.Sent[0][1]);
        Assert.Equal(["NX", "PX", "5000"], script.Sent[0][3..]);

        script.Handler = (_, _) => Resp.Null();
        Assert.Null(await provider.TryAcquireAsync("r", TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(30)));
        using var cancel = new CancellationTokenSource(30);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await provider.TryAcquireAsync("r", TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30), cancel.Token));
    }

    [Fact]
    public async Task ExtendAndReleaseFollowTheOwnerToken()
    {
        RespValue extendReply = Ok;
        RespValue releaseReply = Resp.Int(1);
        var (client, script) = Fake((command, _) => command[0] switch
        {
            "SET" when command.Contains("NX") => Ok,
            "SET" => extendReply,
            _ => releaseReply,
        });
        var provider = new RuvioLockProvider(client, new RuvioLockOptions { AutoRenew = false });
        IRuvioLock held = (await provider.TryAcquireAsync("r", TimeSpan.FromSeconds(10)))!;

        Assert.True(held.IsHeld);
        Assert.True(await held.ExtendAsync());
        Assert.True(await held.ExtendAsync(TimeSpan.FromSeconds(20)));
        string[] renewal = script.Sent[^1];
        Assert.Equal(["IFEQ", renewal[2], "PX", "20000"], renewal[3..]);

        extendReply = Resp.Error("ERR down");
        await Assert.ThrowsAsync<RuvioException>(async () => await held.ExtendAsync());
        Assert.True(held.IsHeld);

        extendReply = Resp.Null();
        Assert.False(await held.ExtendAsync());
        Assert.False(held.IsHeld);
        Assert.True(held.LockLost.IsCancellationRequested);
        Assert.False(await held.ExtendAsync());

        releaseReply = Resp.Int(0);
        Assert.False(await held.ReleaseAsync());
        Assert.False(await held.ReleaseAsync());

        IRuvioLock second = (await provider.TryAcquireAsync("r", TimeSpan.FromSeconds(10)))!;

        releaseReply = Resp.Null();
        Assert.False(await second.ReleaseAsync());
        IRuvioLock third = (await provider.TryAcquireAsync("r", TimeSpan.FromSeconds(10)))!;

        releaseReply = Resp.Error("ERR no");
        await Assert.ThrowsAsync<RuvioException>(async () => await third.ReleaseAsync());
        await third.DisposeAsync();
        await held.DisposeAsync();
        await second.DisposeAsync();
    }

    [Fact]
    public async Task DisposeSwallowsAFailedReleaseAndReleasesAHeldLock()
    {
        bool failRelease = true;
        var (client, script) = Fake((command, _) => command[0] == "DELEX"
            ? failRelease ? throw new RuvioException("socket closed") : Resp.Int(1)
            : Ok);
        var provider = new RuvioLockProvider(client, new RuvioLockOptions { AutoRenew = false });

        await (await provider.TryAcquireAsync("a", TimeSpan.FromSeconds(1)))!.DisposeAsync();
        failRelease = false;
        await (await provider.TryAcquireAsync("b", TimeSpan.FromSeconds(1)))!.DisposeAsync();
        Assert.Equal(2, script.Sent.Count(c => c[0] == "DELEX"));
    }

    [Fact]
    public async Task AutoRenewKeepsTheLeaseAndReportsLossAfterAWholeLeaseOfFailures()
    {
        int renewals = 0;
        bool failing = false;
        var (client, _) = Fake((command, _) =>
        {
            if (command[0] == "SET" && command.Contains("IFEQ"))
            {
                Interlocked.Increment(ref renewals);

                if (failing)
                {
                    throw new RuvioException("renewal failed");
                }
            }

            return command[0] == "DELEX" ? Resp.Int(1) : Ok;
        });
        var time = new FakeTimeProvider();
        var provider = new RuvioLockProvider(client, new RuvioLockOptions { TimeProvider = time });
        await using IRuvioLock held = (await provider.TryAcquireAsync("r", TimeSpan.FromMilliseconds(90)))!;

        await AdvanceUntil(time, TimeSpan.FromMilliseconds(30), () => Volatile.Read(ref renewals) >= 2);
        Assert.True(held.IsHeld);

        failing = true;
        await AdvanceUntil(time, TimeSpan.FromMilliseconds(30), () => !held.IsHeld);
        Assert.False(held.IsHeld);
        Assert.True(held.LockLost.IsCancellationRequested);

        // A lease below 3 ms is renewed at the 1 ms floor instead of spinning: with the clock stopped
        // no renewal is sent, and each millisecond of clock sends at most one.
        failing = false;
        await using IRuvioLock tiny = (await provider.TryAcquireAsync("tiny", TimeSpan.FromMilliseconds(2)))!;

        int beforeTiny = Volatile.Read(ref renewals);

        await Task.Delay(30);
        Assert.Equal(beforeTiny, Volatile.Read(ref renewals));

        await AdvanceUntil(time, TimeSpan.FromMilliseconds(1), () => Volatile.Read(ref renewals) >= beforeTiny + 3);
        Assert.True(tiny.IsHeld);
    }

    [Fact]
    public async Task AutoRenewStopsWhenTheLockIsReleased()
    {
        int renewals = 0;
        var (client, _) = Fake((command, _) =>
        {
            if (command[0] == "SET" && command.Contains("IFEQ"))
            {
                Interlocked.Increment(ref renewals);
            }

            return command[0] == "DELEX" ? Resp.Int(1) : Ok;
        });
        var time = new FakeTimeProvider();
        var provider = new RuvioLockProvider(client, new RuvioLockOptions { TimeProvider = time });
        IRuvioLock held = (await provider.TryAcquireAsync("r", TimeSpan.FromMilliseconds(60)))!;

        await AdvanceUntil(time, TimeSpan.FromMilliseconds(20), () => Volatile.Read(ref renewals) >= 1);
        Assert.True(await held.ReleaseAsync());
        int atRelease = Volatile.Read(ref renewals);

        time.Advance(TimeSpan.FromMilliseconds(150));
        await Task.Delay(30);
        Assert.Equal(atRelease, Volatile.Read(ref renewals));
        Assert.False(held.IsHeld);
        await held.DisposeAsync();
    }

    // The renewal loop runs on its own thread and re-arms its timer after each tick, so the clock is
    // moved a step at a time until the condition holds instead of once by a fixed amount.
    private static async Task AdvanceUntil(FakeTimeProvider time, TimeSpan step, Func<bool> condition)
    {
        for (int attempt = 0; attempt < 2000; attempt++)
        {
            if (condition())
            {
                return;
            }

            time.Advance(step);
            await Task.Delay(1);
        }

        Assert.True(condition(), "The condition never held while the clock advanced.");
    }

    private static DefaultHttpContext Context(
        ScriptClient? script, RuvioLockAttribute? attribute, Action<HttpContext>? configure = null, Action<RuvioLockOptions>? options = null)
    {
        var (client, _) = script is null ? Fake() : (DispatchProxy.Create<IRuvioClient, ScriptClient>(), script);
        var services = new ServiceCollection();

        services.AddSingleton(client);
        services.AddRuvioDistributedLock(o => { o.AutoRenew = false; options?.Invoke(o); });
        var context = new DefaultHttpContext { RequestServices = services.BuildServiceProvider() };

        if (attribute is not null)
        {
            context.SetEndpoint(new Endpoint(_ => Task.CompletedTask, new EndpointMetadataCollection(attribute), "t"));
        }

        configure?.Invoke(context);

        return context;
    }

    private static (DefaultHttpContext Context, ScriptClient Script) Locked(
        RuvioLockAttribute attribute,
        Func<IReadOnlyList<string>, CancellationToken, RespValue>? handler = null,
        Action<HttpContext>? configure = null)
    {
        var client = DispatchProxy.Create<IRuvioClient, ScriptClient>();
        var script = (ScriptClient)(object)client;

        if (handler is not null)
        {
            script.Handler = handler;
        }

        var services = new ServiceCollection();

        services.AddSingleton(client);
        services.AddRuvioDistributedLock(o => o.AutoRenew = false);
        var context = new DefaultHttpContext { RequestServices = services.BuildServiceProvider() };

        context.SetEndpoint(new Endpoint(_ => Task.CompletedTask, new EndpointMetadataCollection(attribute), "t"));
        configure?.Invoke(context);

        return (context, script);
    }

    [Fact]
    public async Task MiddlewareAnswers503WhenTheLockServiceFailsAndStaysSilentWhenTheClientLeaves()
    {
        bool ran = false;
        var middleware = new RuvioLockMiddleware(_ => { ran = true; return Task.CompletedTask; });

        (DefaultHttpContext broken, _) = Locked(new RuvioLockAttribute("r"), (_, _) => throw new RuvioException("down"));
        await middleware.InvokeAsync(broken);
        Assert.Equal(503, broken.Response.StatusCode);

        (DefaultHttpContext serverError, _) = Locked(new RuvioLockAttribute("r"), (_, _) => Resp.Error("ERR x"));
        await middleware.InvokeAsync(serverError);
        Assert.Equal(503, serverError.Response.StatusCode);

        using var gone = new CancellationTokenSource();
        (DefaultHttpContext aborted, _) = Locked(new RuvioLockAttribute("r") { WaitMilliseconds = 5000 }, (_, _) => Resp.Null(),
            context => context.RequestAborted = gone.Token);

        gone.CancelAfter(30);
        await middleware.InvokeAsync(aborted);
        Assert.Equal(200, aborted.Response.StatusCode);
        Assert.False(ran);
    }

    [Fact]
    public async Task MiddlewareRejectsWithTheConfiguredStatusAndRetryAfter()
    {
        var middleware = new RuvioLockMiddleware(_ => throw new InvalidOperationException("must not run"));

        (DefaultHttpContext context, _) = Locked(new RuvioLockAttribute("r", 7) { RejectStatusCode = 423 }, (_, _) => Resp.Null());
        await middleware.InvokeAsync(context);

        Assert.Equal(423, context.Response.StatusCode);
        Assert.Equal("7", context.Response.Headers.RetryAfter.ToString());
    }

    [Fact]
    public async Task MiddlewareResolvesTemplatesFromRouteQueryAndUserOrAnswers400()
    {
        var seen = new List<string>();
        var (client, script) = Fake();
        var middleware = new RuvioLockMiddleware(_ => Task.CompletedTask);

        async Task<int> Run(string template, Action<HttpContext>? configure = null)
        {
            (DefaultHttpContext context, ScriptClient s) = Locked(new RuvioLockAttribute(template), (command, _) =>
            {
                if (command[0] == "SET")
                {
                    seen.Add(command[1]);
                }

                return command[0] == "DELEX" ? Resp.Int(1) : Ok;
            }, configure);

            await middleware.InvokeAsync(context);

            return context.Response.StatusCode;
        }

        Assert.Equal(200, await Run("fixed"));
        Assert.Equal(200, await Run("a:{id}:b", c => c.Request.RouteValues["id"] = 42));
        Assert.Equal(200, await Run("q:{name}", c => c.Request.QueryString = new QueryString("?name=zed")));
        Assert.Equal(200, await Run("u:{user}", c => c.User = new ClaimsPrincipal(new ClaimsIdentity([new Claim(ClaimTypes.Name, "ann")], "t"))));
        Assert.Equal(200, await Run("{a}{b}", c => { c.Request.RouteValues["a"] = "x"; c.Request.RouteValues["b"] = "y"; }));
        Assert.Equal(400, await Run("u:{user}"));
        Assert.Equal(400, await Run("q:{name}"));
        Assert.Equal(400, await Run("open:{id"));
        Assert.Equal(400, await Run("r:{id}", c => c.Request.RouteValues["id"] = ""));
        Assert.Equal(["lock:fixed", "lock:a:42:b", "lock:q:zed", "lock:u:ann", "lock:xy"], seen);
    }

    [Fact]
    public async Task MiddlewareFlagsALostLockAndSurvivesAFailingRelease()
    {
        bool renewReturnsNull = false;
        var middleware = new RuvioLockMiddleware(async context =>
        {
            renewReturnsNull = true;
            IRuvioLock? lockInContext = context.GetRuvioLock();

            Assert.Null(lockInContext);
            await Task.Delay(10);
        });

        (DefaultHttpContext context, ScriptClient script) = Locked(new RuvioLockAttribute("r"), (command, _) =>
            command[0] == "DELEX" ? throw new RuvioException("release failed") : Ok);

        await middleware.InvokeAsync(context);
        Assert.True(renewReturnsNull);
        Assert.Equal(200, context.Response.StatusCode);
        Assert.Contains(script.Sent, c => c[0] == "DELEX");
    }

    [Fact]
    public void ResourceTemplatesRejectMalformedBracesAndMissingRouteValues()
    {
        Assert.Throws<ArgumentNullException>(() => new RuvioLockHttp.Template(null!));
        Assert.Throws<ArgumentException>(() => new RuvioLockHttp.Template(" "));
        Assert.Throws<ArgumentException>(() => new RuvioLockHttp.Template("a{b"));
        Assert.Throws<ArgumentException>(() => new RuvioLockHttp.Template("a{}b"));

        var template = new RuvioLockHttp.Template("pay:{id}:{kind}");
        var context = new DefaultHttpContext();

        context.Request.RouteValues["id"] = 7;
        Assert.Throws<InvalidOperationException>(() => template.Resolve(context));
        context.Request.RouteValues["kind"] = "";
        Assert.Throws<InvalidOperationException>(() => template.Resolve(context));
        context.Request.RouteValues["kind"] = "card";
        Assert.Equal("pay:7:card", template.Resolve(context));
        Assert.Equal("plain", new RuvioLockHttp.Template("plain").Resolve(context));
        Assert.Equal("tail:7:x", new RuvioLockHttp.Template("tail:{id}:x").Resolve(context));
    }

    [Fact]
    public async Task EndpointHelpersTakeTheLockExposeItAndBuildTheConflictResponse()
    {
        (DefaultHttpContext context, ScriptClient script) = Locked(new RuvioLockAttribute("r"));
        RespValue reply = Resp.Simple("OK");

        script.Handler = (_, _) => reply;
        IRuvioLock? held = (await RuvioLockHttp.TryTakeAsync(context, "res", TimeSpan.FromSeconds(3), TimeSpan.Zero)).Held;

        Assert.NotNull(held);
        Assert.Same(held, context.GetRuvioLock());
        Assert.Equal("3000", script.Sent[0][^1]);
        await held!.DisposeAsync();

        reply = Resp.Null();
        var other = new DefaultHttpContext { RequestServices = context.RequestServices };

        Assert.Null((await RuvioLockHttp.TryTakeAsync(other, "res", null, null)).Held);
        Assert.Null(other.GetRuvioLock());
        Assert.Equal(409, RuvioLockHttp.ConflictStatus(other, null));
        Assert.Equal(429, RuvioLockHttp.ConflictStatus(other, 429));
        Assert.NotNull(RuvioLockHttp.Conflict(other, 423));

        other.Items["Ruvio.AspNetCore.DistributedLock.Lock"] = "not a lock";
        Assert.Null(other.GetRuvioLock());
    }
}
