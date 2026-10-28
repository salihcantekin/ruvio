using System.Net;
using System.Net.Sockets;
using System.Text;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Extensions.Caching.Tests;

public class WireTests
{
    [Fact(Timeout = 30000)]
    public async Task BinarySetUsesOneEvalWithOneNamespacedKeyAndNoBase64()
    {
        await using var wire = await Wire.Open();
        byte[] payload = [0, 255, 128, 13, 10];
        long before = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        Task setting = wire.Cache.SetAsync("", payload, new()
        {
            SlidingExpiration = TimeSpan.FromTicks(15001),
            AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(3),
        });

        long after = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        byte[][] command = await wire.Command();
        Assert.Equal("SET", Text(command[0]));
        Assert.Equal("unit:ruvio:cache:v1:", Text(command[1]));
        Assert.Equal("PX", Text(command[3]));
        Assert.Equal("1", Text(command[4]));
        Assert.StartsWith("RDC1\n", Text(command[2]));
        Assert.InRange(long.Parse(Text(command[2]).Split('\n')[1]), before + 3000, after + 3000);
        Assert.EndsWith("\n1\n" + Encoding.Latin1.GetString(payload), Encoding.Latin1.GetString(command[2]));
        await wire.Reply("+OK\r\n"u8.ToArray());
        await setting;
    }

    [Theory(Timeout = 30000)]
    [InlineData(true)]
    [InlineData(false)]
    public async Task SetComputesTheEarlierAbsoluteDeadlineBeforeDispatch(bool explicitEarlier)
    {
        await using var wire = await Wire.Open();
        DateTimeOffset explicitDeadline = DateTimeOffset.UtcNow.AddSeconds(explicitEarlier ? 10 : 60);
        long before = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        Task setting = wire.Cache.SetAsync("k", [], new()
        {
            AbsoluteExpiration = explicitDeadline,
            AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(30),
        });

        long after = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        byte[][] command = await wire.Command();
        Assert.Equal("PXAT", Text(command[3]));
        long sentDeadline = long.Parse(Text(command[4]));

        if (explicitEarlier)
            Assert.Equal(explicitDeadline.ToUnixTimeMilliseconds(), sentDeadline);
        else
            Assert.InRange(sentDeadline, before + 30000, after + 30000);

        Assert.Contains("\n0\n", Text(command[2]));
        await wire.Reply("+OK\r\n"u8.ToArray());
        await setting;
    }

    [Fact(Timeout = 30000)]
    public async Task GetPreservesInvalidUtf8EmptyAndNullAndRefreshDoesNotReturnValue()
    {
        await using var wire = await Wire.Open();
        Task<byte[]?> get = wire.Cache.GetAsync("k");

        Assert.Equal("GET", Text((await wire.Command())[0]));
        await wire.Reply(Bulk(Envelope(0, 0, [0, 255, 128])));
        Assert.Equal(new byte[] { 0, 255, 128 }, await get);

        get = wire.Cache.GetAsync("k");
        await wire.Command();
        await wire.Reply(Bulk(Envelope(0, 0, [])));
        Assert.Empty((await get)!);
        get = wire.Cache.GetAsync("k");
        await wire.Command();
        await wire.Reply("$-1\r\n"u8.ToArray());
        Assert.Null(await get);
        Task refreshing = wire.Cache.RefreshAsync("k");

        Assert.Equal("GET", Text((await wire.Command())[0]));
        await wire.Reply("$-1\r\n"u8.ToArray());
        await refreshing;
        Task removing = wire.Cache.RemoveAsync("k");

        Assert.Equal("DEL", Text((await wire.Command())[0]));
        await wire.Reply(":0\r\n"u8.ToArray());
        await removing;
    }

    [Theory(Timeout = 30000)]
    [InlineData("-ERR script failure\r\n")]
    [InlineData("+not-a-cache-value\r\n")]
    public async Task ErrorsAndUnexpectedRepliesNeverBecomeMisses(string response)
    {
        await using var wire = await Wire.Open();
        Task<byte[]?> get = wire.Cache.GetAsync("k");

        await wire.Command();
        await wire.Reply(Encoding.ASCII.GetBytes(response));
        await Assert.ThrowsAsync<RuvioException>(() => get);
    }

    private static string Text(byte[] bytes) => Encoding.Latin1.GetString(bytes);

    private static byte[] Envelope(long absolute, long sliding, byte[] payload)
    {
        byte[] prefix = Encoding.ASCII.GetBytes($"RDC1\n{absolute}\n{sliding}\n");

        return prefix.Concat(payload).ToArray();
    }

    private static byte[] Bulk(byte[] payload)
    {
        byte[] header = Encoding.ASCII.GetBytes($"${payload.Length}\r\n");

        return header.Concat(payload).Concat("\r\n"u8.ToArray()).ToArray();
    }

    private sealed class Wire : IAsyncDisposable
    {
        private readonly TcpListener _listener;
        private readonly TcpClient _peer;
        private readonly RuvioConnection _connection;
        private readonly CancellationTokenSource _deadline = new(TimeSpan.FromSeconds(10));

        public RuvioDistributedCache Cache { get; }

        private Wire(TcpListener listener, TcpClient peer, RuvioConnection connection, RuvioClient client)
        {
            _listener = listener;
            _peer = peer;
            _connection = connection;
            Cache = new(client, Options.Create(new RuvioCacheOptions { InstanceName = "unit" }));
        }

        public static async Task<Wire> Open()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);

            listener.Start();
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync(new RuvioClientOptions
            {
                Host = "127.0.0.1",
                Port = ((IPEndPoint)listener.LocalEndpoint).Port,
                DiscoverCluster = false,
            }, deadline.Token);

            TcpClient peer = await listener.AcceptTcpClientAsync(deadline.Token);

            RuvioConnection connection = await connecting;
            RuvioClient client = await connection.GetDefaultDatabaseAsync();

            return new(listener, peer, connection, client);
        }

        public async Task<byte[][]> Command()
        {
            string header = await Line();

            Assert.StartsWith("*", header);
            var args = new byte[int.Parse(header[1..])][];

            for (int i = 0; i < args.Length; i++)
            {
                string bulk = await Line();

                Assert.StartsWith("$", bulk);
                args[i] = new byte[int.Parse(bulk[1..])];
                await _peer.GetStream().ReadExactlyAsync(args[i], _deadline.Token);
                Assert.Equal("", await Line());
            }

            return args;
        }

        private async Task<string> Line()
        {
            var bytes = new List<byte>();
            var single = new byte[1];

            while (true)
            {
                await _peer.GetStream().ReadExactlyAsync(single, _deadline.Token);

                if (single[0] == '\n')
                {
                    Assert.Equal((byte)'\r', bytes[^1]);

                    return Encoding.ASCII.GetString(bytes.ToArray(), 0, bytes.Count - 1);
                }

                bytes.Add(single[0]);
            }
        }

        public Task Reply(byte[] bytes) => _peer.GetStream().WriteAsync(bytes, _deadline.Token).AsTask();

        public ValueTask DisposeAsync()
        {
            _connection.Dispose();
            _peer.Dispose();
            _listener.Stop();
            _deadline.Dispose();

            return ValueTask.CompletedTask;
        }
    }
}
