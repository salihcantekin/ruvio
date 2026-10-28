using System.Reflection;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.DistributedLock.Tests;

/// <summary>A stalled link makes the client throw TimeoutException; renewal and release must treat it as a failed call.</summary>
public class StalledLinkTests
{
    [Fact]
    public async Task TimeoutOnRenewMarksTheLockLostAndDisposeDoesNotThrow()
    {
        var client = DispatchProxy.Create<IRuvioClient, OfflineLockTests.ScriptClient>();
        var script = (OfflineLockTests.ScriptClient)(object)client;

        script.Handler = (command, _) => command[0] switch
        {
            "SET" when command.Contains("IFEQ") => throw new TimeoutException("stalled"),
            "DELEX" => throw new TimeoutException("stalled"),
            _ => Resp.Ok,
        };

        var provider = new RuvioLockProvider(client, new RuvioLockOptions { KeyPrefix = "p:", AutoRenew = true });
        IRuvioLock held = (await provider.TryAcquireAsync("job", TimeSpan.FromMilliseconds(150)))!;
        using var lostSignal = new ManualResetEventSlim();

        held.LockLost.Register(lostSignal.Set);

        Assert.True(lostSignal.Wait(TimeSpan.FromSeconds(10)));
        Assert.False(held.IsHeld);
        await held.DisposeAsync();
    }

    [Fact]
    public async Task TimeoutOnReleaseIsSwallowedByDispose()
    {
        var client = DispatchProxy.Create<IRuvioClient, OfflineLockTests.ScriptClient>();
        var script = (OfflineLockTests.ScriptClient)(object)client;

        script.Handler = (command, _) => command[0] == "DELEX" ? throw new TimeoutException("stalled") : Resp.Ok;

        var provider = new RuvioLockProvider(client, new RuvioLockOptions { AutoRenew = false });
        IRuvioLock held = (await provider.TryAcquireAsync("job", TimeSpan.FromSeconds(5)))!;

        await held.DisposeAsync();
    }
}
