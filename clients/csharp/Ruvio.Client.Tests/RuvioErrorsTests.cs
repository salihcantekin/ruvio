using System.Net.Sockets;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

public class RuvioErrorsTests
{
    [Theory]
    [InlineData("TRYAGAIN the slot is moving to another node", true, false)]
    [InlineData("READONLY This primary was replaced", true, false)]
    [InlineData("LOADING replica catching up", true, false)]
    [InlineData("MASTERDOWN incomplete replica", true, false)]
    [InlineData("CLUSTERDOWN Hash slot not served", true, false)]
    [InlineData("NOREPLICAS Not enough good replicas to write.", true, true)]
    [InlineData("WRONGTYPE Operation against a key holding the wrong kind of value", false, false)]
    [InlineData("OOM command not allowed when used memory > 'maxmemory'.", false, false)]
    [InlineData("ERR syntax error", false, false)]
    public void ServerRepliesAreSorted(string reply, bool transient, bool mayHaveRun)
    {
        var error = new RuvioException(reply, isServerReply: true);

        Assert.Equal(transient, RuvioErrors.IsTransient(error));
        Assert.Equal(mayHaveRun, RuvioErrors.MayHaveRun(error));
    }

    [Fact]
    public void ConnectionFailuresAreTransientAndMayHaveRun()
    {
        Exception[] failures =
        [
            new RuvioException("connection closed", closedBeforeReply: true),
            new SocketException((int)SocketError.ConnectionReset),
            new IOException("reset"),
            new TimeoutException(),
            new OperationCanceledException(),
        ];

        foreach (Exception failure in failures)
        {
            Assert.True(RuvioErrors.IsTransient(failure));
            Assert.True(RuvioErrors.MayHaveRun(failure));
        }
    }

    [Fact]
    public void OpenCircuitIsTransientAndDidNotRun()
    {
        var open = new RuvioCircuitOpenException(TimeSpan.FromSeconds(1));

        Assert.True(RuvioErrors.IsTransient(open));
        Assert.False(RuvioErrors.MayHaveRun(open));
    }

    [Fact]
    public void TheCallersOwnCancellationIsNotTransient()
    {
        using var cancelled = new CancellationTokenSource();

        cancelled.Cancel();

        Assert.False(RuvioErrors.IsTransient(new OperationCanceledException(cancelled.Token), cancelled.Token));
        Assert.False(RuvioErrors.IsTransient(new ArgumentException("bad")));
        Assert.Throws<ArgumentNullException>(() => RuvioErrors.IsTransient(null!));
        Assert.Throws<ArgumentNullException>(() => RuvioErrors.MayHaveRun(null!));
    }

    [Fact(Timeout = 15000)]
    public async Task ExceptionsTheClientThrowsAreSortedTheSameWay()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        RuvioClient db = session.Client;

        ValueTask<long> wrongType = db.IncrementAsync("k");
        await session.Expect("INCR", "k");
        await session.Reply("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
        RuvioException refused = await Assert.ThrowsAsync<RuvioException>(async () => await wrongType);
        Assert.False(RuvioErrors.IsTransient(refused));
        Assert.False(RuvioErrors.MayHaveRun(refused));

        ValueTask<long> durable = db.IncrementAsync("k");
        await session.Expect("INCR", "k");
        await session.Reply("-NOREPLICAS Not enough good replicas to write.\r\n");
        RuvioException unconfirmed = await Assert.ThrowsAsync<RuvioException>(async () => await durable);
        Assert.True(RuvioErrors.IsTransient(unconfirmed));
        Assert.True(RuvioErrors.MayHaveRun(unconfirmed));
    }
}
