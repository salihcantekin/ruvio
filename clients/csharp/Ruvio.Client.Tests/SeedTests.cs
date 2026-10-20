using System.Net;
using System.Net.Sockets;
using System.Text;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

public class SeedTests
{
    [Fact(Timeout = 15000)]
    public async Task ConnectSkipsASeedThatCannotBeReached()
    {
        (int refusedPort, Socket? held) = Refused();
        using var live = Listen();
        using Socket? holder = held;
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync(Options(
            [Address(refusedPort), Address(live.Port)]));
        using TcpClient accepted = await live.AcceptTcpClientAsync();
        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        ValueTask<string> ping = client.PingAsync();

        await ExpectAsync(accepted.GetStream(), "PING");
        await ReplyAsync(accepted.GetStream(), "+PONG\r\n");
        Assert.Equal("PONG", await ping);
    }

    [Fact(Timeout = 15000)]
    public async Task ConnectReturnsTheLastErrorWhenNoSeedAnswers()
    {
        (int firstPort, Socket? heldFirst) = Refused();
        (int secondPort, Socket? heldSecond) = Refused();
        using Socket? first = heldFirst;
        using Socket? second = heldSecond;

        // A refused connect and a timed-out one are both connect errors; which one the OS gives varies.
        Exception error = await Assert.ThrowsAnyAsync<Exception>(async () => await RuvioConnection.ConnectAsync(Options(
            [Address(firstPort), Address(secondPort)])));

        Assert.True(error is SocketException or OperationCanceledException, error.ToString());
    }

    [Fact(Timeout = 15000)]
    public async Task ADroppedConnectionMovesOnToTheNextSeed()
    {
        using var first = Listen();
        using var second = Listen();
        var restored = new List<RuvioConnectionNotice>();
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync(Options(
            [Address(first.Port), Address(second.Port)]));
        TcpClient accepted = await first.AcceptTcpClientAsync();
        await using RuvioConnection connection = await connecting;

        connection.ConnectionRestored += (_, notice) => restored.Add(notice);
        RuvioClient client = await connection.GetDefaultDatabaseAsync();

        accepted.Dispose();
        await Assert.ThrowsAnyAsync<Exception>(async () => await client.GetStringAsync("key"));

        ValueTask<string> again = client.PingAsync();
        using TcpClient moved = await second.AcceptTcpClientAsync();

        await ExpectAsync(moved.GetStream(), "PING");
        await ReplyAsync(moved.GetStream(), "+PONG\r\n");
        Assert.Equal("PONG", await again);
        Assert.Equal(second.Port, Assert.Single(restored).Port);
    }

    [Fact(Timeout = 15000)]
    public async Task AReconnectEndsWithTheAddressThatDropped()
    {
        using var first = Listen();
        using var second = Listen();
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync(Options(
            [Address(first.Port), Address(second.Port)]));
        TcpClient accepted = await first.AcceptTcpClientAsync();
        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        // The second seed is gone, so the attempt wraps around to the one that dropped.
        accepted.Dispose();
        using Socket? held = ClosedPort.Stop(second.Listener);
        await Assert.ThrowsAnyAsync<Exception>(async () => await client.GetStringAsync("key"));

        ValueTask<string> again = client.PingAsync();
        using TcpClient back = await first.AcceptTcpClientAsync();

        await ExpectAsync(back.GetStream(), "PING");
        await ReplyAsync(back.GetStream(), "+PONG\r\n");
        Assert.Equal("PONG", await again);
    }

    [Fact]
    public async Task AnEmptySeedHostIsRefused()
    {
        await Assert.ThrowsAsync<ArgumentException>(async () => await RuvioConnection.ConnectAsync(Options([":6379"])));
        await Assert.ThrowsAsync<ArgumentException>(async () => await RuvioConnection.ConnectAsync(Options(["[]:6379"])));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await RuvioConnection.ConnectAsync(Options(["host:0"])));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await RuvioConnection.ConnectAsync(Options(["host:99999"])));
        await Assert.ThrowsAsync<ArgumentException>(async () => await RuvioConnection.ConnectAsync(Options(["nohostport"])));
    }

    [Fact]
    public void SeedsAcceptBracketedIPv6Hosts()
    {
        (string Host, int Port)[] parsed = SeedAddresses.Parse(Options(["[::1]:6380", "node-a:6379"]));

        Assert.Equal([("::1", 6380), ("node-a", 6379)], parsed);
    }

    [Fact(Timeout = 15000)]
    public async Task ALostNodeReloadsTheSlotMapFromASeed()
    {
        using var gone = Listen();
        using var live = Listen();
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync(ClusterOptions(
            [Address(gone.Port), Address(live.Port)]));
        TcpClient seed = await gone.AcceptTcpClientAsync();

        await ExpectAsync(seed.GetStream(), "CLUSTER", "SLOTS");
        await ReplyAsync(seed.GetStream(), OneNodeSlots(gone.Port));
        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();

        seed.Dispose();
        using Socket? held = ClosedPort.Stop(gone.Listener);
        await Assert.ThrowsAnyAsync<Exception>(async () => await client.GetStringAsync("key"));

        ValueTask<string?> get = client.GetStringAsync("key");
        using TcpClient reloaded = await live.AcceptTcpClientAsync();
        NetworkStream stream = reloaded.GetStream();

        await ExpectAsync(stream, "CLUSTER", "SLOTS");
        await ReplyAsync(stream, OneNodeSlots(live.Port));
        await ExpectAsync(stream, "GET", "key");
        await ReplyAsync(stream, "$5\r\nvalue\r\n");
        Assert.Equal("value", await get);
        Assert.Equal(1, client.ShardCount);
    }

    [Fact(Timeout = 15000)]
    public async Task AKeylessCommandGoesToTheFirstReachableNodeOfTheNewMap()
    {
        using var gone = Listen();
        using var live = Listen();
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync(ClusterOptions(
            [Address(gone.Port), Address(live.Port)]));
        TcpClient seed = await gone.AcceptTcpClientAsync();

        await ExpectAsync(seed.GetStream(), "CLUSTER", "SLOTS");
        await ReplyAsync(seed.GetStream(), OneNodeSlots(gone.Port));
        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();

        seed.Dispose();
        using Socket? held = ClosedPort.Stop(gone.Listener);
        await Assert.ThrowsAnyAsync<Exception>(async () => await client.GetStringAsync("key"));

        ValueTask<string> ping = client.PingAsync();
        using TcpClient reloaded = await live.AcceptTcpClientAsync();
        NetworkStream stream = reloaded.GetStream();

        await ExpectAsync(stream, "CLUSTER", "SLOTS");
        await ReplyAsync(stream, OneNodeSlots(live.Port));
        await ExpectAsync(stream, "PING");
        await ReplyAsync(stream, "+PONG\r\n");
        Assert.Equal("PONG", await ping);
    }

    [Fact(Timeout = 15000)]
    public async Task AFailedReloadLeavesNoSeedEndpointsBehind()
    {
        using var gone = Listen();
        (int unreachablePort, Socket? heldPort) = Refused();
        using Socket? heldSeed = heldPort;
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync(ClusterOptions(
            [Address(gone.Port), Address(unreachablePort)]));
        TcpClient seed = await gone.AcceptTcpClientAsync();

        await ExpectAsync(seed.GetStream(), "CLUSTER", "SLOTS");
        await ReplyAsync(seed.GetStream(), OneNodeSlots(gone.Port));
        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();

        seed.Dispose();
        using Socket? held = ClosedPort.Stop(gone.Listener);
        await Assert.ThrowsAnyAsync<Exception>(async () => await client.GetStringAsync("key"));

        await Assert.ThrowsAnyAsync<Exception>(async () => await client.GetStringAsync("key"));
        Assert.Equal(1, client.Router!.EndpointCount);
    }

    private static RuvioClientOptions Options(List<string> seeds) => new()
    {
        Seeds = seeds,
        DiscoverCluster = false,

        // The scripted seeds only answer PING; the rotation is what is checked.
        FollowPrimary = false,
        ReconnectBaseDelay = TimeSpan.Zero,
        ReconnectMaxDelay = TimeSpan.Zero,
        ConnectTimeout = TimeSpan.FromMilliseconds(500),
    };

    private static RuvioClientOptions ClusterOptions(List<string> seeds)
    {
        RuvioClientOptions options = Options(seeds);

        options.DiscoverCluster = true;
        options.MaxReconnectAttempts = 1;

        return options;
    }

    private static string Address(int port) => $"127.0.0.1:{port}";

    /// <summary>A port that refuses connects; the socket returned (when not null) keeps it that way.</summary>
    private static (int Port, Socket? Holder) Refused()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);

        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;

        return (port, ClosedPort.Stop(listener));
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        DateTime deadline = DateTime.UtcNow + TestDeadlines.Bound;

        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "the condition did not become true in time");
            await Task.Delay(10);
        }
    }

    private static ScriptedListener Listen()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);

        listener.Start();

        return new ScriptedListener(listener);
    }

    private static async Task ExpectAsync(NetworkStream stream, params string[] arguments)
    {
        byte[] expected = RespCodec.Encode(arguments);
        var got = new byte[expected.Length];

        await TestDeadlines.ReadExactlyAsync(stream, got);
        Assert.Equal(Encoding.ASCII.GetString(expected), Encoding.ASCII.GetString(got));
    }

    private static Task ReplyAsync(NetworkStream stream, string resp) =>
        stream.WriteAsync(Encoding.ASCII.GetBytes(resp)).AsTask();

    private static string OneNodeSlots(int port)
    {
        string id = 1.ToString("x40");

        return $"*1\r\n*3\r\n:0\r\n:16383\r\n*3\r\n$9\r\n127.0.0.1\r\n:{port}\r\n${id.Length}\r\n{id}\r\n";
    }

    private sealed class ScriptedListener(TcpListener listener) : IDisposable
    {
        public TcpListener Listener => listener;

        public int Port => ((IPEndPoint)listener.LocalEndpoint).Port;

        public Task<TcpClient> AcceptTcpClientAsync() => TestDeadlines.AcceptAsync(listener);

        public void Dispose() => listener.Stop();
    }
}
