using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

/// <summary>
/// A standalone client with several seeds finds the primary again after a failover, against scripted nodes that
/// answer <c>ROLE</c>, refuse writes with <c>READONLY</c> when they are replicas and (for the coordinator case) name a
/// primary with <c>SENTINEL get-master-addr-by-name</c>. <c>FailoverLiveTests</c> does the same against real servers.
/// </summary>
public class FollowFailoverTests
{
    private static readonly NodeMode Master = new(NodeKind.Master);

    private static NodeMode Replica(int primary = 0) => new(NodeKind.Replica, primary);

    private static RuvioClientOptions Options(params int[] ports) => new()
    {
        Seeds = [.. ports.Select(port => $"127.0.0.1:{port}")],
        DiscoverCluster = false,
        ConnectTimeout = TimeSpan.FromSeconds(2),
        CommandTimeout = TimeSpan.FromSeconds(5),
        MaxReconnectAttempts = 3,
        ReconnectBaseDelay = TimeSpan.FromMilliseconds(10),
        ReconnectMaxDelay = TimeSpan.FromMilliseconds(50),
    };

    private static async Task<(RuvioConnection Connection, RuvioClient Client)> OpenAsync(RuvioClientOptions options)
    {
        RuvioConnection connection = await RuvioConnection.ConnectAsync(options);

        return (connection, await connection.GetDefaultDatabaseAsync());
    }

    private static async Task<RuvioException> ReadOnlyAsync(Func<ValueTask> send)
    {
        RuvioException error = await Assert.ThrowsAsync<RuvioException>(async () => await send());

        Assert.True(error.IsServerReply);
        Assert.StartsWith("READONLY", error.Message, StringComparison.Ordinal);

        return error;
    }

    [Fact(Timeout = 30000)]
    public async Task AReadOnlyWriteFollowsTheNewPrimaryAndIsSentOnceMore()
    {
        using var old = new FailoverNode(Master);
        using var next = new FailoverNode(Replica());
        (RuvioConnection connection, RuvioClient client) = await OpenAsync(Options(old.Port, next.Port));

        await using RuvioConnection owner = connection;

        await client.SetAsync("k", "1");
        Assert.Equal((1, 0), (old.Seen("SET"), next.Seen("SET")));

        old.BecomeMode(Replica(next.Port));
        next.BecomeMode(Master);

        await client.SetAsync("k", "2");

        // The refused write was sent once to the old node, then once to the new primary.
        Assert.Equal((2, 1), (old.Seen("SET"), next.Seen("SET")));

        await client.SetAsync("k", "3");
        Assert.Equal((2, 2), (old.Seen("SET"), next.Seen("SET")));
    }

    [Fact(Timeout = 30000)]
    public async Task TheMovedConnectionIsReportedAsLostAndRestored()
    {
        using var old = new FailoverNode(Master);
        using var next = new FailoverNode(Replica());
        (RuvioConnection connection, RuvioClient client) = await OpenAsync(Options(old.Port, next.Port));

        await using RuvioConnection owner = connection;

        var lost = new List<RuvioConnectionNotice>();
        var restored = new List<RuvioConnectionNotice>();

        connection.ConnectionLost += (_, notice) => lost.Add(notice);
        connection.ConnectionRestored += (_, notice) => restored.Add(notice);
        old.BecomeMode(Replica(next.Port));
        next.BecomeMode(Master);

        await client.SetAsync("k", "1");

        Assert.Equal(old.Port, Assert.Single(lost).Port);
        Assert.Equal(next.Port, Assert.Single(restored).Port);
    }

    [Fact(Timeout = 30000)]
    public async Task ABinaryCommandIsFollowedToo()
    {
        using var old = new FailoverNode(Master);
        using var next = new FailoverNode(Replica());
        (RuvioConnection connection, RuvioClient client) = await OpenAsync(Options(old.Port, next.Port));

        await using RuvioConnection owner = connection;

        old.BecomeMode(Replica(next.Port));
        next.BecomeMode(Master);

        RespValue reply = await client.ExecuteBinaryAsync(
            [new byte[] { (byte)'S', (byte)'E', (byte)'T' }, new byte[] { (byte)'k' }, new byte[] { (byte)'v' }]);

        Assert.Equal("OK", reply.AsString());
        Assert.Equal((1, 1), (old.Seen("SET"), next.Seen("SET")));
    }

    [Fact(Timeout = 30000)]
    public async Task AHealthyConnectionSendsNothingExtra()
    {
        using var node = new FailoverNode(Master);
        using var other = new FailoverNode(Replica());
        (RuvioConnection connection, RuvioClient client) = await OpenAsync(Options(node.Port, other.Port));

        await using RuvioConnection owner = connection;

        for (int count = 0; count < 50; count++)
        {
            await client.SetAsync("k", "v");
        }

        Assert.Equal(50, node.Commands.Count);
        Assert.Empty(other.Commands);
    }

    [Fact(Timeout = 30000)]
    public async Task AReplicaNamesItsPrimaryWhenThatIsNoSeed()
    {
        using var replica = new FailoverNode(Master);
        using var hidden = new FailoverNode(Master);
        using var listener = new TcpListener(IPAddress.Loopback, 0);

        listener.Start();

        int dead = ((IPEndPoint)listener.LocalEndpoint).Port;
        using Socket? held = ClosedPort.Stop(listener);
        (RuvioConnection connection, RuvioClient client) = await OpenAsync(Options(replica.Port, dead));

        await using RuvioConnection owner = connection;

        replica.BecomeMode(Replica(hidden.Port));

        await client.SetAsync("k", "v");

        Assert.Equal((1, 1), (replica.Seen("SET"), hidden.Seen("SET")));
    }

    [Fact(Timeout = 30000)]
    public async Task FollowPrimaryOffReturnsReadOnlyAndSendsNoRole()
    {
        using var node = new FailoverNode(Master);
        using var other = new FailoverNode(Master);
        RuvioClientOptions options = Options(node.Port, other.Port);

        options.FollowPrimary = false;

        (RuvioConnection connection, RuvioClient client) = await OpenAsync(options);

        await using RuvioConnection owner = connection;

        node.BecomeMode(Replica(other.Port));

        await ReadOnlyAsync(async () => await client.SetAsync("k", "v"));
        Assert.Equal((0, 0), (node.Seen("ROLE"), other.Commands.Count));
    }

    [Fact(Timeout = 30000)]
    public async Task AReconnectTakesTheNodeThatSaysMasterNotTheNextSeed()
    {
        using var first = new FailoverNode(Master);
        using var second = new FailoverNode(Master);
        using var third = new FailoverNode(Master);
        (RuvioConnection connection, RuvioClient client) = await OpenAsync(Options(first.Port, second.Port, third.Port));

        await using RuvioConnection owner = connection;

        await client.PingAsync();

        // The second seed became a replica while the connection was down.
        second.BecomeMode(Replica(third.Port));
        first.BecomeMode(Replica(third.Port));
        first.DropConnections();

        // The command that hits the drop fails and is not sent again; the next one reconnects.
        await Assert.ThrowsAnyAsync<Exception>(async () => await client.PingAsync());
        Assert.False(connection.IsConnected);

        await client.SetAsync("k", "v");

        Assert.Equal((0, 0, 1), (first.Seen("SET"), second.Seen("SET"), third.Seen("SET")));
    }

    [Fact(Timeout = 30000)]
    public async Task AReconnectWithoutAnyPrimaryStillConnectsToTheFirstSeedThatAnswers()
    {
        using var first = new FailoverNode(Master);
        using var second = new FailoverNode(Master);
        (RuvioConnection connection, RuvioClient client) = await OpenAsync(Options(first.Port, second.Port));

        await using RuvioConnection owner = connection;

        await client.PingAsync();
        first.BecomeMode(Replica());
        second.BecomeMode(Replica());
        first.DropConnections();

        // The command that hits the drop fails and is not sent again; the next one reconnects.
        await Assert.ThrowsAnyAsync<Exception>(async () => await client.PingAsync());
        Assert.False(connection.IsConnected);

        Assert.Equal("PONG", await client.PingAsync());
        Assert.Equal(1, second.Seen("PING"));
    }

    [Fact(Timeout = 30000)]
    public async Task WithoutAnyPrimaryTheReadOnlyErrorComesBackWithinTheBackoff()
    {
        using var first = new FailoverNode(Master);
        using var second = new FailoverNode(Master);
        (RuvioConnection connection, RuvioClient client) = await OpenAsync(Options(first.Port, second.Port));

        await using RuvioConnection owner = connection;

        first.BecomeMode(Replica());
        second.BecomeMode(Replica());

        var clock = Stopwatch.StartNew();

        await ReadOnlyAsync(async () => await client.SetAsync("k", "v"));
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(5), clock.Elapsed.ToString());
        Assert.Equal((1, 0), (first.Seen("SET"), second.Seen("SET")));

        // The connection stayed where it was.
        Assert.True(connection.IsConnected);
    }

    [Fact(Timeout = 30000)]
    public async Task WithReconnectOffOneRoundRunsAndTheReadOnlyErrorComesBack()
    {
        using var first = new FailoverNode(Master);
        using var second = new FailoverNode(Master);
        RuvioClientOptions options = Options(first.Port, second.Port);

        options.Reconnect = false;

        (RuvioConnection connection, RuvioClient client) = await OpenAsync(options);

        await using RuvioConnection owner = connection;

        first.BecomeMode(Replica());
        second.BecomeMode(Replica());

        await ReadOnlyAsync(async () => await client.SetAsync("k", "v"));
        Assert.Equal(1, first.Seen("ROLE"));
        Assert.Equal(1, second.Seen("ROLE"));
    }

    [Fact(Timeout = 30000)]
    public async Task AWriteRefusedTwiceIsNotSentAThirdTime()
    {
        using var old = new FailoverNode(Master);
        using var liar = new FailoverNode(Master);
        (RuvioConnection connection, RuvioClient client) = await OpenAsync(Options(old.Port, liar.Port));

        await using RuvioConnection owner = connection;

        old.BecomeMode(Replica());
        liar.BecomeMode(new NodeMode(NodeKind.Liar));

        await ReadOnlyAsync(async () => await client.SetAsync("k", "v"));
        Assert.Equal((1, 1), (old.Seen("SET"), liar.Seen("SET")));
    }

    [Fact(Timeout = 30000)]
    public async Task AWriteRefusedInsideATransactionFollowsButIsNotSentAgain()
    {
        using var old = new FailoverNode(Master);
        using var next = new FailoverNode(Replica());
        (RuvioConnection connection, RuvioClient client) = await OpenAsync(Options(old.Port, next.Port));

        await using RuvioConnection owner = connection;

        await client.ExecuteAsync("MULTI");
        old.BecomeMode(Replica(next.Port));
        next.BecomeMode(Master);

        await ReadOnlyAsync(async () => await client.ExecuteAsync("SET", "k", "v"));
        Assert.Equal(0, next.Seen("SET"));

        // The client moved and left the lost transaction: the next write is sent, and sent once.
        await client.SetAsync("k", "v");
        Assert.Equal(1, next.Seen("SET"));
    }

    [Fact(Timeout = 30000)]
    public async Task AWriteAfterAnEndedTransactionIsSentAgain()
    {
        using var old = new FailoverNode(Master);
        using var next = new FailoverNode(Replica());
        (RuvioConnection connection, RuvioClient client) = await OpenAsync(Options(old.Port, next.Port));

        await using RuvioConnection owner = connection;

        await client.ExecuteAsync("MULTI");
        await client.ExecuteAsync("EXEC");
        old.BecomeMode(Replica(next.Port));
        next.BecomeMode(Master);

        await client.ExecuteAsync("SET", "k", "v");
        Assert.Equal(1, next.Seen("SET"));
    }

    [Fact(Timeout = 30000)]
    public async Task APipelineReturnsReadOnlyAtItsIndexAndSendsNothingMore()
    {
        using var old = new FailoverNode(Master);
        using var next = new FailoverNode(Master);
        (RuvioConnection connection, RuvioClient client) = await OpenAsync(Options(old.Port, next.Port));

        await using RuvioConnection owner = connection;

        old.BecomeMode(Replica(next.Port));

        IReadOnlyList<RespValue> replies = await client.ExecuteManyAsync([["PING"], ["SET", "k", "v"]]);

        Assert.Equal("PONG", replies[0].AsString());
        Assert.Equal(RespKind.Error, replies[1].Kind);
        Assert.StartsWith("READONLY", replies[1].Text, StringComparison.Ordinal);
        Assert.Equal((1, 0, 0), (old.Seen("SET"), next.Seen("SET"), old.Seen("ROLE") + next.Seen("ROLE")));
    }

    [Fact(Timeout = 30000)]
    public async Task AnotherErrorIsNotFollowed()
    {
        using var node = new FailoverNode(Master);
        using var other = new FailoverNode(Master);
        (RuvioConnection connection, RuvioClient client) = await OpenAsync(Options(node.Port, other.Port));

        await using RuvioConnection owner = connection;

        // An error that is not READONLY never starts a round: only the command itself arrives.
        RuvioException error = await Assert.ThrowsAsync<RuvioException>(async () => await client.ExecuteAsync("NOPE"));

        Assert.False(error.Message.StartsWith("READONLY", StringComparison.Ordinal));
        Assert.Equal(0, node.Seen("ROLE") + other.Seen("ROLE"));
    }

    [Fact(Timeout = 30000)]
    public async Task ACoordinatorNameFindsThePrimaryAtConnectAndAfterAFailover()
    {
        using var coordinator = new FailoverNode(new NodeMode(NodeKind.Names));
        using var one = new FailoverNode(Master);
        using var two = new FailoverNode(Replica());
        RuvioClientOptions options = Options(coordinator.Port);

        coordinator.BecomeMode(new NodeMode(NodeKind.Names, one.Port));
        options.SentinelMaster = "main";

        (RuvioConnection connection, RuvioClient client) = await OpenAsync(options);

        await using RuvioConnection owner = connection;

        await client.SetAsync("k", "1");
        Assert.Equal((1, 0), (one.Seen("SET"), two.Seen("SET")));
        Assert.Equal(1, coordinator.Seen("SENTINEL"));

        one.BecomeMode(Replica());
        two.BecomeMode(Master);
        coordinator.BecomeMode(new NodeMode(NodeKind.Names, two.Port));

        await client.SetAsync("k", "2");
        Assert.Equal((2, 1), (one.Seen("SET"), two.Seen("SET")));

        // After a dropped socket the coordinator is asked again.
        one.BecomeMode(Master);
        two.BecomeMode(Replica());
        coordinator.BecomeMode(new NodeMode(NodeKind.Names, one.Port));
        two.DropConnections();

        // The command that hits the drop fails and is not sent again; the next one reconnects.
        await Assert.ThrowsAnyAsync<Exception>(async () => await client.PingAsync());
        Assert.False(connection.IsConnected);

        await client.SetAsync("k", "3");
        Assert.Equal((3, 1), (one.Seen("SET"), two.Seen("SET")));
    }

    [Fact(Timeout = 30000)]
    public async Task ACoordinatorThatNamesNoPrimaryFailsTheConnect()
    {
        using var coordinator = new FailoverNode(Master);
        RuvioClientOptions options = Options(coordinator.Port);

        options.SentinelMaster = "main";

        RuvioException error = await Assert.ThrowsAsync<RuvioException>(async () => await RuvioConnection.ConnectAsync(options));

        Assert.False(error.IsServerReply);
        Assert.Contains("main", error.Message, StringComparison.Ordinal);
    }

    [Fact(Timeout = 30000)]
    public async Task AHungSeedCostsOneBoundedWait()
    {
        using var live = new FailoverNode(Master);
        using var silent = new FailoverNode(new NodeMode(NodeKind.Silent));
        using var other = new FailoverNode(Replica());
        RuvioClientOptions options = Options(live.Port, silent.Port, other.Port);

        options.ConnectTimeout = TimeSpan.FromMilliseconds(300);

        (RuvioConnection connection, RuvioClient client) = await OpenAsync(options);

        await using RuvioConnection owner = connection;

        live.BecomeMode(Replica(other.Port));
        other.BecomeMode(Master);

        var clock = Stopwatch.StartNew();

        await client.SetAsync("k", "v");
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(3), clock.Elapsed.ToString());
        Assert.Equal(1, other.Seen("SET"));
    }

    [Fact(Timeout = 30000)]
    public async Task AProbeSendsAuthAndOneCommandAndNothingElse()
    {
        using var old = new FailoverNode(Master);
        using var next = new FailoverNode(Replica());
        RuvioClientOptions options = Options(old.Port, next.Port);

        options.Password = "secret";
        options.Database = 2;
        options.DurabilityReplicas = 1;

        (RuvioConnection connection, RuvioClient client) = await OpenAsync(options);

        await using RuvioConnection owner = connection;

        old.BecomeMode(Replica(next.Port));
        next.BecomeMode(Master);

        await client.SetAsync("k", "v");

        // Each probe connection carried AUTH and ROLE, and no SELECT or CLIENT.
        int probes = 0;

        foreach (FailoverNode node in new[] { old, next })
        {
            foreach (IGrouping<int, SeenCommand> connectionCommands in node.Commands.GroupBy(command => command.Connection))
            {
                string[] names = [.. connectionCommands.Select(command => command.Arguments[0])];

                if (names.Contains("ROLE"))
                {
                    probes++;
                    Assert.Equal(["AUTH", "ROLE"], names);
                }
            }
        }

        Assert.True(probes >= 1, $"probes: {probes}");
        Assert.Equal(1, next.Seen("SET"));
    }
}
