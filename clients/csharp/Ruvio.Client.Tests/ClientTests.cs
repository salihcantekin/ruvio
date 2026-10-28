using System.Net;
using System.Net.Sockets;
using System.Text;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

public class ClientTests
{
    [Fact]
    public void EncodeWritesAResp2ArrayOfBulkStrings()
    {
        byte[] payload = RespCodec.Encode(["GET", "key"]);

        Assert.Equal("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n", Encoding.ASCII.GetString(payload));
    }

    [Fact]
    public void GeneratedVerbsMatchTheStringEncoder()
    {
        Assert.Equal("GET"u8.ToArray(), RespVerbs.GET.ToArray());
        Assert.Equal("INCR"u8.ToArray(), RespVerbs.INCR.ToArray());
        Assert.Equal("GEOADD"u8.ToArray(), RespVerbs.GEOADD.ToArray());

        using (PooledFrame get = RespCodec.WriteVerb(RespVerbs.GET, "session"))
        {
            Assert.Equal(RespCodec.Encode(["GET", "session"]), get.Buffer.AsSpan(0, get.Length).ToArray());
        }

        string[] places = ["cities", "13", "38", "Palermo"];
        using PooledFrame geo = RespCodec.WriteVerb(RespVerbs.GEOADD, places);

        Assert.Equal(
            RespCodec.Encode(["GEOADD", "cities", "13", "38", "Palermo"]),
            geo.Buffer.AsSpan(0, geo.Length).ToArray());
    }

    [Fact(Timeout = 15000)]
    public async Task StandaloneGetSendsTheGeneratedVerbFrame()
    {
        using var server = new TcpListener(IPAddress.Loopback, 0);

        server.Start();
        int port = ((IPEndPoint)server.LocalEndpoint!).Port;
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync(new RuvioClientOptions
        {
            Host = "127.0.0.1",
            Port = port,
            DiscoverCluster = false,
        });

        using TcpClient accepted = await TestDeadlines.AcceptAsync(server);

        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        NetworkStream stream = accepted.GetStream();
        byte[] expected = RespCodec.Encode(["GET", "session"]);
        ValueTask<string?> get = client.GetStringAsync("session");
        var got = new byte[expected.Length];

        await TestDeadlines.ReadExactlyAsync(stream, got);
        Assert.Equal(expected, got);
        await stream.WriteAsync(Encoding.ASCII.GetBytes("$5\r\nhello\r\n"));
        Assert.Equal("hello", await get);
    }

    [Fact(Timeout = 15000)]
    public async Task ReaderDecodesTheReplyKindsRuvioSends()
    {
        byte[] payload = Encoding.ASCII.GetBytes(
            "+OK\r\n-ERR nope\r\n:7\r\n$4\r\nping\r\n$-1\r\n*2\r\n$1\r\na\r\n:2\r\n");
        var reader = new RespReader(new MemoryStream(payload));

        Assert.Equal("OK", (await reader.ReadAsync(CancellationToken.None)).AsString());
        RespValue error = await reader.ReadAsync(CancellationToken.None);

        Assert.Equal(RespKind.Error, error.Kind);
        Assert.Equal("ERR nope", error.Text);
        Assert.Equal(7, (await reader.ReadAsync(CancellationToken.None)).Integer);
        RespValue bulk = await reader.ReadAsync(CancellationToken.None);

        Assert.Equal(Encoding.ASCII.GetBytes("ping"), bulk.Bytes);
        Assert.Equal(RespKind.Null, (await reader.ReadAsync(CancellationToken.None)).Kind);
        RespValue array = await reader.ReadAsync(CancellationToken.None);

        Assert.Equal(2, array.Items!.Count);
        Assert.Equal("a", array.Items[0].AsString());
        Assert.Equal(2, array.Items[1].Integer);
    }

    [Fact(Timeout = 15000)]
    public async Task ReaderKeepsCrlfInsideABulkString()
    {
        byte[] body = Encoding.ASCII.GetBytes("a\r\nb");
        var framed = new MemoryStream();

        framed.Write(Encoding.ASCII.GetBytes($"${body.Length}\r\n"));
        framed.Write(body);
        framed.Write(Encoding.ASCII.GetBytes("\r\n"));
        var reader = new RespReader(new MemoryStream(framed.ToArray()));

        RespValue value = await reader.ReadAsync(CancellationToken.None);

        Assert.Equal(body, value.Bytes);
    }

    [Fact]
    public async Task ReaderHandlesAsynchronouslyFragmentedNestedReplies()
    {
        using var stream = new FragmentedReplyStream(Encoding.ASCII.GetBytes(
            "*3\r\n+OK\r\n$4\r\na\r\nb\r\n*2\r\n:12345\r\n$-1\r\n"));
        using var reader = new RespReader(stream);
        RespValue reply = await reader.ReadAsync(CancellationToken.None);

        Assert.Equal(3, reply.Items!.Count);
        Assert.Equal("OK", reply.Items[0].AsString());
        Assert.Equal("a\r\nb", reply.Items[1].AsString());
        Assert.Equal(12345, reply.Items[2].Items![0].Integer);
        Assert.Equal(RespKind.Null, reply.Items[2].Items![1].Kind);
    }

    [Theory]
    [InlineData("", true)]
    [InlineData("$256\r\n", false)]
    [InlineData("$1\r\na\rX", false)]
    [InlineData("+OK\rX", false)]
    [InlineData("?", false)]
    public async Task ReaderPreservesFailureAndRetryClassification(string payload, bool closedBeforeReply)
    {
        using var reader = new RespReader(new MemoryStream(Encoding.ASCII.GetBytes(payload)));
        reader.PrepareReply();
        RuvioException error = await Assert.ThrowsAsync<RuvioException>(
            async () => await reader.ReadAsync(CancellationToken.None));

        Assert.Equal(closedBeforeReply, error.ClosedBeforeReply);
    }

    [Fact]
    public async Task ReaderBufferedBulkKeepsOwnedResponseBytes()
    {
        using var reader = new RespReader(new MemoryStream(Encoding.ASCII.GetBytes(
            "$3\r\none\r\n$3\r\ntwo\r\n")));
        RespValue first = await reader.ReadAsync(CancellationToken.None);
        RespValue second = await reader.ReadAsync(CancellationToken.None);
        first.Bytes![0] = (byte)'X';

        Assert.Equal("Xne", first.AsString());
        Assert.Equal("two", second.AsString());
        Assert.NotSame(first.Bytes, second.Bytes);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReaderFirstReadAndBufferedRepliesPreserveMixedFrames(bool fragmented)
    {
        byte[] payload = Encoding.ASCII.GetBytes(
            "+OK\r\n:9223372036854775807\r\n$0\r\n\r\n$3\r\none\r\n*1\r\n:7\r\n");
        using MemoryStream stream = fragmented ? new FragmentedReplyStream(payload) : new MemoryStream(payload);
        using var reader = new RespReader(stream);
        reader.PrepareReply();
        Assert.Equal("OK", (await reader.ReadAsync(CancellationToken.None)).AsString());
        Assert.True(reader.ReplyStarted);
        reader.PrepareReply();
        Assert.Equal(long.MaxValue, (await reader.ReadAsync(CancellationToken.None)).Integer);
        Assert.Equal("", (await reader.ReadAsync(CancellationToken.None)).AsString());
        Assert.Equal("one", (await reader.ReadAsync(CancellationToken.None)).AsString());
        Assert.Equal(7, (await reader.ReadAsync(CancellationToken.None)).Items![0].Integer);
        reader.PrepareReply();
        RuvioException closed = await Assert.ThrowsAsync<RuvioException>(
            async () => await reader.ReadAsync(CancellationToken.None));
        Assert.True(closed.ClosedBeforeReply);
    }

    private sealed class FragmentedReplyStream(byte[] payload) : MemoryStream(payload)
    {
        public override async ValueTask<int> ReadAsync(
            Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            cancellationToken.ThrowIfCancellationRequested();

            return Read(buffer.Span[..Math.Min(1, buffer.Length)]);
        }
    }

    [Fact(Timeout = 15000)]
    public async Task ConnectAsksForClusterSlotsThenIsQuiet()
    {
        using var server = new TcpListener(IPAddress.Loopback, 0);

        server.Start();
        int port = ((IPEndPoint)server.LocalEndpoint!).Port;
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync("127.0.0.1", port);
        using TcpClient accepted = await TestDeadlines.AcceptAsync(server);
        NetworkStream stream = accepted.GetStream();

        await ReplyStandaloneClusterAsync(stream);
        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        Assert.False(client.IsCluster);

        await Task.Delay(100);
        Assert.Equal(0, accepted.Available);

        byte[] expected = Encoding.ASCII.GetBytes("*1\r\n$4\r\nPING\r\n");
        ValueTask<string> ping = client.PingAsync();
        var got = new byte[expected.Length];

        await TestDeadlines.ReadExactlyAsync(stream, got);
        Assert.Equal(expected, got);
        await stream.WriteAsync(Encoding.ASCII.GetBytes("+PONG\r\n"));
        Assert.Equal("PONG", await ping);
    }

    [Fact(Timeout = 15000)]
    public async Task DiscoverClusterOffSendsNothingUntilTheFirstCommand()
    {
        using var server = new TcpListener(IPAddress.Loopback, 0);

        server.Start();
        int port = ((IPEndPoint)server.LocalEndpoint!).Port;
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync(new RuvioClientOptions
        {
            Host = "127.0.0.1",
            Port = port,
            DiscoverCluster = false,
        });

        using TcpClient accepted = await TestDeadlines.AcceptAsync(server);

        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();

        await Task.Delay(100);
        Assert.Equal(0, accepted.Available);

        byte[] expected = Encoding.ASCII.GetBytes("*1\r\n$4\r\nPING\r\n");
        NetworkStream stream = accepted.GetStream();
        ValueTask<string> ping = client.PingAsync();
        var got = new byte[expected.Length];

        await TestDeadlines.ReadExactlyAsync(stream, got);
        Assert.Equal(expected, got);
        await stream.WriteAsync(Encoding.ASCII.GetBytes("+PONG\r\n"));
        Assert.Equal("PONG", await ping);
    }

    [Fact(Timeout = 15000)]
    public async Task AServerErrorLeavesTheNextCommandUsable()
    {
        using var server = new TcpListener(IPAddress.Loopback, 0);

        server.Start();
        int port = ((IPEndPoint)server.LocalEndpoint!).Port;
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync("127.0.0.1", port);
        using TcpClient accepted = await TestDeadlines.AcceptAsync(server);
        NetworkStream stream = accepted.GetStream();

        await ReplyStandaloneClusterAsync(stream);
        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();

        ValueTask<string?> get = client.GetStringAsync("missing");

        await ReadCommandAsync(stream);
        await stream.WriteAsync(Encoding.ASCII.GetBytes("-ERR no such key\r\n"));
        RuvioException error = await Assert.ThrowsAsync<RuvioException>(async () => await get);

        Assert.True(error.IsServerReply);

        ValueTask<string> ping = client.PingAsync();

        await ReadCommandAsync(stream);
        await stream.WriteAsync(Encoding.ASCII.GetBytes("+PONG\r\n"));
        Assert.Equal("PONG", await ping);
    }

    [Fact(Timeout = 15000)]
    public async Task PasswordIsSentAsAuthAndNotAsClientSetinfo()
    {
        using var server = new TcpListener(IPAddress.Loopback, 0);

        server.Start();
        int port = ((IPEndPoint)server.LocalEndpoint!).Port;
        var connecting = RuvioConnection.ConnectAsync(new RuvioClientOptions
        {
            Host = "127.0.0.1",
            Port = port,
            Password = "secret",
        });

        using TcpClient accepted = await TestDeadlines.AcceptAsync(server);
        NetworkStream stream = accepted.GetStream();
        byte[] expected = Encoding.ASCII.GetBytes("*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n");
        var got = new byte[expected.Length];

        await TestDeadlines.ReadExactlyAsync(stream, got);
        Assert.Equal(expected, got);
        await stream.WriteAsync(Encoding.ASCII.GetBytes("+OK\r\n"));
        await ReplyStandaloneClusterAsync(stream);
        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        Assert.NotNull(client);
    }

    [Fact(Timeout = 15000)]
    public async Task DatabaseOptionIsSelectedAfterAuth()
    {
        using var server = new TcpListener(IPAddress.Loopback, 0);

        server.Start();
        int port = ((IPEndPoint)server.LocalEndpoint!).Port;
        var connecting = RuvioConnection.ConnectAsync(new RuvioClientOptions
        {
            Host = "127.0.0.1",
            Port = port,
            Password = "secret",
            Database = 3,
        });

        using TcpClient accepted = await TestDeadlines.AcceptAsync(server);
        NetworkStream stream = accepted.GetStream();
        byte[] expected = Encoding.ASCII.GetBytes(
            "*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n*2\r\n$6\r\nSELECT\r\n$1\r\n3\r\n");
        var got = new byte[expected.Length];

        await TestDeadlines.ReadExactlyAsync(stream, got.AsMemory(0, 26));
        await stream.WriteAsync(Encoding.ASCII.GetBytes("+OK\r\n"));
        await ReplyStandaloneClusterAsync(stream);
        await TestDeadlines.ReadExactlyAsync(stream, got.AsMemory(26));
        await stream.WriteAsync(Encoding.ASCII.GetBytes("+OK\r\n"));
        Assert.Equal(expected, got);
        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        Assert.NotNull(client);
    }

    [Theory(Timeout = 15000)]
    [InlineData(null, "*4\r\n$6\r\nCLIENT\r\n$10\r\nDURABILITY\r\n$8\r\nREPLICAS\r\n$1\r\n2\r\n")]
    [InlineData(250, "*6\r\n$6\r\nCLIENT\r\n$10\r\nDURABILITY\r\n$8\r\nREPLICAS\r\n$1\r\n2\r\n$7\r\nTIMEOUT\r\n$3\r\n250\r\n")]
    public async Task DurabilityOptionIsSentAfterAuthOnConnect(int? timeoutMilliseconds, string command)
    {
        using var server = new TcpListener(IPAddress.Loopback, 0);

        server.Start();
        int port = ((IPEndPoint)server.LocalEndpoint!).Port;
        var connecting = RuvioConnection.ConnectAsync(new RuvioClientOptions
        {
            Host = "127.0.0.1",
            Port = port,
            Password = "secret",
            DurabilityReplicas = 2,
            DurabilityTimeout = timeoutMilliseconds is { } ms ? TimeSpan.FromMilliseconds(ms) : null,
        });

        using TcpClient accepted = await TestDeadlines.AcceptAsync(server);
        NetworkStream stream = accepted.GetStream();
        byte[] auth = Encoding.ASCII.GetBytes("*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n");
        byte[] expected = Encoding.ASCII.GetBytes(command);
        var got = new byte[auth.Length];

        await TestDeadlines.ReadExactlyAsync(stream, got);
        Assert.Equal(auth, got);
        await stream.WriteAsync(Encoding.ASCII.GetBytes("+OK\r\n"));
        got = new byte[expected.Length];
        await TestDeadlines.ReadExactlyAsync(stream, got);
        Assert.Equal(expected, got);
        await stream.WriteAsync(Encoding.ASCII.GetBytes("+OK\r\n"));
        await ReplyStandaloneClusterAsync(stream);
        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        Assert.NotNull(client);
    }

    [Fact(Timeout = 15000)]
    public async Task RefusedDurabilityOptionFailsTheConnect()
    {
        using var server = new TcpListener(IPAddress.Loopback, 0);

        server.Start();
        int port = ((IPEndPoint)server.LocalEndpoint!).Port;
        var connecting = RuvioConnection.ConnectAsync(new RuvioClientOptions
        {
            Host = "127.0.0.1",
            Port = port,
            DurabilityReplicas = 1,
        });

        using TcpClient accepted = await TestDeadlines.AcceptAsync(server);
        NetworkStream stream = accepted.GetStream();
        byte[] expected = Encoding.ASCII.GetBytes("*4\r\n$6\r\nCLIENT\r\n$10\r\nDURABILITY\r\n$8\r\nREPLICAS\r\n$1\r\n1\r\n");
        var got = new byte[expected.Length];

        await TestDeadlines.ReadExactlyAsync(stream, got);
        Assert.Equal(expected, got);
        await stream.WriteAsync(Encoding.ASCII.GetBytes("-ERR CLIENT DURABILITY needs replication on this server\r\n"));

        RuvioException error = await Assert.ThrowsAsync<RuvioException>(() => connecting);
        Assert.Contains("needs replication", error.Message);
    }

    [Fact(Timeout = 15000)]
    public async Task DatabaseOptionIsNotSentForDatabaseZero()
    {
        using var server = new TcpListener(IPAddress.Loopback, 0);

        server.Start();
        int port = ((IPEndPoint)server.LocalEndpoint!).Port;
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync(new RuvioClientOptions
        {
            Host = "127.0.0.1",
            Port = port,
        });

        using TcpClient accepted = await TestDeadlines.AcceptAsync(server);
        NetworkStream stream = accepted.GetStream();

        await ReplyStandaloneClusterAsync(stream);
        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        ValueTask<string> ping = client.PingAsync();
        byte[] expected = Encoding.ASCII.GetBytes("*1\r\n$4\r\nPING\r\n");
        var got = new byte[expected.Length];

        await TestDeadlines.ReadExactlyAsync(stream, got);
        Assert.Equal(expected, got);
        await stream.WriteAsync(Encoding.ASCII.GetBytes("+PONG\r\n"));
        Assert.Equal("PONG", await ping);
    }

    [Theory]
    [InlineData("host")]
    [InlineData("address")]
    [InlineData("endpoint")]
    public async Task ConnectOverloadsSelectTheDatabase(string overload)
    {
        using var server = new TcpListener(IPAddress.Loopback, 0);

        server.Start();
        int port = ((IPEndPoint)server.LocalEndpoint!).Port;
        Task<RuvioConnection> connecting = overload switch
        {
            "host" => RuvioConnection.ConnectAsync("127.0.0.1", port, 3),
            "address" => RuvioConnection.ConnectAsync(IPAddress.Loopback, port, 3),
            _ => RuvioConnection.ConnectAsync(new IPEndPoint(IPAddress.Loopback, port), 3),
        };

        using TcpClient accepted = await TestDeadlines.AcceptAsync(server);
        NetworkStream stream = accepted.GetStream();

        await ReplyStandaloneClusterAsync(stream);
        byte[] expected = Encoding.ASCII.GetBytes("*2\r\n$6\r\nSELECT\r\n$1\r\n3\r\n");
        var got = new byte[expected.Length];

        await TestDeadlines.ReadExactlyAsync(stream, got);
        await stream.WriteAsync(Encoding.ASCII.GetBytes("+OK\r\n"));
        Assert.Equal(expected, got);
        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
    }

    [Fact(Timeout = 15000)]
    public async Task ConnectToAnEndpointWithoutADatabaseSendsNothing()
    {
        using var server = new TcpListener(IPAddress.Loopback, 0);

        server.Start();
        var endPoint = (IPEndPoint)server.LocalEndpoint!;
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync(endPoint);
        using TcpClient accepted = await TestDeadlines.AcceptAsync(server);
        NetworkStream stream = accepted.GetStream();

        await ReplyStandaloneClusterAsync(stream);
        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        ValueTask<string> ping = client.PingAsync();
        byte[] expected = Encoding.ASCII.GetBytes("*1\r\n$4\r\nPING\r\n");
        var got = new byte[expected.Length];

        await TestDeadlines.ReadExactlyAsync(stream, got);
        Assert.Equal(expected, got);
        await stream.WriteAsync(Encoding.ASCII.GetBytes("+PONG\r\n"));
        Assert.Equal("PONG", await ping);
    }

    [Fact(Timeout = 15000)]
    public async Task DatabaseOptionRejectsANegativeIndex()
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await RuvioConnection.ConnectAsync(new RuvioClientOptions { Database = -1 }));
    }

    [Fact(Timeout = 15000)]
    public async Task DisposingTheConnectionInvalidatesEveryDatabaseClient()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        Task<RuvioClient> creating = session.Connection.GetDatabaseAsync(1);

        await using SubscriberPipe database = await session.AcceptDatabaseAsync();
        await database.Expect("SELECT", "1");
        await database.Reply("+OK\r\n");
        RuvioClient db1 = await creating;

        Assert.Equal(0, session.Client.Database);
        Assert.Equal(1, db1.Database);
        Assert.NotSame(session.Client, db1);
        Assert.Same(db1, await session.Connection.GetDatabaseAsync(1));
        Assert.Same(session.Client,
            await ((IRuvioConnection)session.Connection).GetDefaultDatabaseAsync());
        await Assert.ThrowsAsync<RuvioException>(async () => await db1.ExecuteAsync("sElEcT", "2"));
        await Assert.ThrowsAsync<RuvioException>(async () => await db1.ExecuteBinaryAsync(
            new ReadOnlyMemory<byte>[] { "select"u8.ToArray(), "2"u8.ToArray() }));

        session.Connection.Dispose();

        Assert.False(session.Connection.IsConnected);
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await session.Client.PingAsync());
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await db1.PingAsync());
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await session.Connection.GetDatabaseAsync(0));
    }

    [Fact]
    public void ConvertParsesDecimalIntegersAndLeavesAMissNull()
    {
        Assert.Null(RespConvert.FromBulk<long?>(null));
        Assert.Null(RespConvert.FromBulk<int?>(null));
        Assert.Equal(42, RespConvert.FromBulk<long>(Encoding.ASCII.GetBytes("42")));
        Assert.Equal(42, RespConvert.FromBulk<int>(Encoding.ASCII.GetBytes("42")));
        Assert.Equal("42", RespConvert.FromBulk<string>(Encoding.ASCII.GetBytes("42")));
        Assert.Equal(new byte[] { (byte)'1' }, RespConvert.FromBulk<byte[]>(Encoding.ASCII.GetBytes("1")));

        Assert.Throws<RuvioException>(() => RespConvert.FromBulk<long>(Encoding.ASCII.GetBytes("hello")));
        Assert.Throws<RuvioException>(() => RespConvert.FromBulk<Guid>(Encoding.ASCII.GetBytes("1")));
    }

    [Fact(Timeout = 15000)]
    public async Task GetInt64ReadsTheDecimalBulkFromIncr()
    {
        using var server = new TcpListener(IPAddress.Loopback, 0);

        server.Start();
        int port = ((IPEndPoint)server.LocalEndpoint!).Port;
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync("127.0.0.1", port);
        using TcpClient accepted = await TestDeadlines.AcceptAsync(server);
        NetworkStream stream = accepted.GetStream();

        await ReplyStandaloneClusterAsync(stream);
        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();

        ValueTask<long?> get = client.GetInt64Async("visits");

        await ReadCommandAsync(stream);
        await stream.WriteAsync(Encoding.ASCII.GetBytes("$1\r\n1\r\n"));
        Assert.Equal(1, await get);

        ValueTask<int?> typed = client.GetAsync<int?>("visits");

        await ReadCommandAsync(stream);
        await stream.WriteAsync(Encoding.ASCII.GetBytes("$1\r\n1\r\n"));
        Assert.Equal(1, await typed);

        ValueTask<int?> asInt = client.GetInt32Async("visits");

        await ReadCommandAsync(stream);
        await stream.WriteAsync(Encoding.ASCII.GetBytes("$1\r\n1\r\n"));
        Assert.Equal(1, await asInt);
    }

    internal static async Task ReplyStandaloneClusterAsync(NetworkStream stream)
    {
        byte[] expected = RespCodec.Encode(["CLUSTER", "SLOTS"]);
        var got = new byte[expected.Length];

        await TestDeadlines.ReadExactlyAsync(stream, got);
        Assert.Equal(expected, got);
        await stream.WriteAsync(Encoding.ASCII.GetBytes("-ERR cluster mode is disabled\r\n"));
    }

    private static async Task ReadCommandAsync(NetworkStream stream)
    {
        var buffer = new byte[256];
        int read = 0;
        using var timeout = new CancellationTokenSource(TestDeadlines.Bound);

        while (read < 4)
        {
            read += await stream.ReadAsync(buffer.AsMemory(read), timeout.Token);
        }

        string header = Encoding.ASCII.GetString(buffer, 0, read);

        Assert.StartsWith("*", header);
    }
}
