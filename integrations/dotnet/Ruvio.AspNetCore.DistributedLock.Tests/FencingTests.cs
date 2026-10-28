using System.Reflection;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.DistributedLock.Tests;

/// <summary>Fenced locks (LEASE, RENEW, RELEASE). The live tests need RUVIO_TEST_ADDR and otherwise return at once.</summary>
public class FencingTests
{
    private static (IRuvioClient Client, OfflineLockTests.ScriptClient Script) Fake(Func<IReadOnlyList<string>, CancellationToken, RespValue> handler)
    {
        var client = DispatchProxy.Create<IRuvioClient, OfflineLockTests.ScriptClient>();
        var script = (OfflineLockTests.ScriptClient)(object)client;

        script.Handler = handler;

        return (client, script);
    }

    private static RuvioLockOptions Fenced(string prefix = "p:") => new() { KeyPrefix = prefix, Fenced = true, AutoRenew = false };

    [Fact(Timeout = 30000)]
    public async Task FencedLockUsesLeaseRenewAndReleaseWithItsToken()
    {
        long renewReply = 1;
        var (client, script) = Fake((command, _) => command[0] switch
        {
            "LEASE" => Resp.Int(42),
            "RENEW" => Resp.Int(renewReply),
            "RELEASE" => Resp.Int(1),
            _ => throw new InvalidOperationException(command[0]),
        });
        var provider = new RuvioLockProvider(client, Fenced());
        IRuvioLock held = (await provider.TryAcquireAsync("job", TimeSpan.FromSeconds(5)))!;

        Assert.Equal(42, held.FencingToken);
        Assert.True(await held.ExtendAsync(TimeSpan.FromSeconds(7)));
        Assert.True(await held.ReleaseAsync());
        Assert.Equal(["LEASE", "p:job", "5000"], script.Sent[0]);
        Assert.Equal(["RENEW", "p:job", "42", "7000"], script.Sent[1]);
        Assert.Equal(["RELEASE", "p:job", "42"], script.Sent[2]);

        IRuvioLock second = (await provider.TryAcquireAsync("job", TimeSpan.FromSeconds(5)))!;

        renewReply = 0;
        Assert.False(await second.ExtendAsync());
        Assert.False(second.IsHeld);
        Assert.True(second.LockLost.IsCancellationRequested);
        await second.DisposeAsync();
    }

    [Fact(Timeout = 30000)]
    public async Task FencedAcquireTreatsAPlainLockAsTakenAndOtherErrorsAsErrors()
    {
        var (client, _) = Fake((_, _) => Resp.Error("WRONGTYPE Operation against a key holding the wrong kind of value"));
        var provider = new RuvioLockProvider(client, Fenced());

        Assert.Null(await provider.TryAcquireAsync("job", TimeSpan.FromSeconds(5)));
        await Assert.ThrowsAsync<TimeoutException>(async () => await provider.AcquireAsync("job", TimeSpan.FromSeconds(5), TimeSpan.Zero));

        var (failing, _) = Fake((_, _) => Resp.Error("ERR unknown command 'LEASE'"));

        await Assert.ThrowsAsync<RuvioException>(async () =>
            await new RuvioLockProvider(failing, Fenced()).TryAcquireAsync("job", TimeSpan.FromSeconds(5)));

        // Without fencing the same reply is an error, as before.
        await Assert.ThrowsAsync<RuvioException>(async () =>
            await new RuvioLockProvider(client, new RuvioLockOptions { AutoRenew = false }).TryAcquireAsync("job", TimeSpan.FromSeconds(5)));
    }

    [Fact(Timeout = 30000)]
    public async Task PlainLocksAndOtherImplementationsHaveNoFencingToken()
    {
        var (client, _) = Fake((command, _) => command[0] == "DELEX" ? Resp.Int(1) : Resp.Ok);
        await using IRuvioLock? plain = await new RuvioLockProvider(client, new RuvioLockOptions { AutoRenew = false })
            .TryAcquireAsync("job", TimeSpan.FromSeconds(5));

        Assert.Null(plain!.FencingToken);
        Assert.Null(((IRuvioLock)new OtherLock()).FencingToken);
    }

    [Fact(Timeout = 30000)]
    public async Task TheAttributeMiddlewareHandsTheLockToTheHandler()
    {
        var (client, _) = Fake((command, _) => command[0] == "LEASE" ? Resp.Int(7) : Resp.Int(1));
        var services = new ServiceCollection();

        services.AddSingleton(client);
        services.AddRuvioDistributedLock(o => { o.Fenced = true; o.AutoRenew = false; });
        var context = new DefaultHttpContext { RequestServices = services.BuildServiceProvider() };

        context.SetEndpoint(new Endpoint(_ => Task.CompletedTask, new EndpointMetadataCollection(new RuvioLockAttribute("job")), "t"));
        long? seen = null;
        var middleware = new RuvioLockMiddleware(c =>
        {
            seen = c.GetRuvioLock()?.FencingToken;

            return Task.CompletedTask;
        });

        await middleware.InvokeAsync(context);

        Assert.Equal(7, seen);
    }

    [Fact(Timeout = 30000)]
    public async Task LiveTokensGrowAndPlainAndFencedLocksExcludeEachOther()
    {
        if (await LiveTests.TryConnectAsync() is not { } connected)
        {
            return;
        }

        await using RuvioConnection connection = connected.Connection;
        string prefix = "test:fence:" + Guid.NewGuid().ToString("N") + ":";
        var fenced = new RuvioLockProvider(connected.Client, Fenced(prefix));
        var plain = new RuvioLockProvider(connected.Client, new RuvioLockOptions { KeyPrefix = prefix, AutoRenew = false });

        IRuvioLock first = (await fenced.TryAcquireAsync("job", TimeSpan.FromSeconds(5)))!;

        Assert.NotNull(first.FencingToken);
        Assert.Null(await fenced.TryAcquireAsync("job", TimeSpan.FromSeconds(5)));
        Assert.Null(await plain.TryAcquireAsync("job", TimeSpan.FromSeconds(5)));
        Assert.True(await first.ExtendAsync(TimeSpan.FromSeconds(5)));
        Assert.True(await first.ReleaseAsync());

        await using IRuvioLock second = (await fenced.TryAcquireAsync("job", TimeSpan.FromSeconds(5)))!;

        Assert.True(second.FencingToken > first.FencingToken);
        Assert.False(await first.ExtendAsync());
        Assert.True(await second.ReleaseAsync());

        // A released lease takes its key with it, so a plain lock can take the resource again.
        await using (IRuvioLock? plainAfter = await plain.TryAcquireAsync("job", TimeSpan.FromSeconds(5)))
        {
            Assert.NotNull(plainAfter);
        }

        // A resource a plain lock holds is taken for a fenced one (LEASE answers WRONGTYPE).
        await using IRuvioLock? plainHolder = await plain.TryAcquireAsync("other", TimeSpan.FromSeconds(5));

        Assert.NotNull(plainHolder);
        Assert.Null(await fenced.TryAcquireAsync("other", TimeSpan.FromSeconds(5)));
        await connected.Client.ExecuteAsync(["DEL", prefix + "job"]);
    }

    private sealed class OtherLock : IRuvioLock
    {
        public string Resource => "r";

        public bool IsHeld => true;

        public CancellationToken LockLost => default;

        public ValueTask<bool> ExtendAsync(TimeSpan? lease = null, CancellationToken cancellationToken = default) => new(true);

        public ValueTask<bool> ReleaseAsync(CancellationToken cancellationToken = default) => new(true);

        public ValueTask DisposeAsync() => default;
    }
}
