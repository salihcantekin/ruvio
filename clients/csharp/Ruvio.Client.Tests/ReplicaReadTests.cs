using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

public class ReplicaReadTests
{
    [Fact(Timeout = 15000)]
    public async Task ReadsGoToTheReplicaAndWritesAndMixedPipelinesToThePrimary()
    {
        using var primary = new FakeNode("p");
        using var replica = new FakeNode("r");

        primary.Slots = OneShard(primary.Port, replica.Port);
        await using RuvioConnection connection = await ConnectAsync(primary, ReadPolicy.PreferReplica);
        RuvioClient client = await connection.GetDefaultDatabaseAsync();

        Assert.Equal("r", await client.GetStringAsync("k"));
        Assert.Equal("r", (await client.ExecuteAsync(["STRLEN", "k"])).AsString());
        Assert.True(await client.SetAsync("k", "v"));
        Assert.Equal("r", (await client.ExecuteBinaryAsync(new ReadOnlyMemory<byte>[] { "GET"u8.ToArray(), "k"u8.ToArray() })).AsString());

        // A pipeline with a write goes to the primary; one that only reads goes to the replica.
        IReadOnlyList<RespValue> mixed = await client.ExecuteManyAsync([["SET", "a", "1"], ["GET", "a"]]);
        Assert.Equal("p", mixed[1].AsString());
        IReadOnlyList<RespValue> reads = await client.ExecuteManyAsync([["GET", "a"], ["STRLEN", "b"]]);
        Assert.Equal(["r", "r"], reads.Select(reply => reply.AsString()));

        Assert.Equal("READONLY", replica.Log[0]);
        Assert.Equal(["READONLY", "GET k", "STRLEN k", "GET k", "GET a", "STRLEN b"], replica.Log);
        Assert.Equal(["CLUSTER SLOTS", "SET k v", "SET a 1", "GET a"], primary.Log);
    }

    [Fact(Timeout = 15000)]
    public async Task ThePrimaryPolicyNeverOpensTheReplica()
    {
        using var primary = new FakeNode("p");
        using var replica = new FakeNode("r");

        primary.Slots = OneShard(primary.Port, replica.Port);
        await using RuvioConnection connection = await ConnectAsync(primary, ReadPolicy.Primary);
        RuvioClient client = await connection.GetDefaultDatabaseAsync();

        Assert.Equal("p", await client.GetStringAsync("k"));
        Assert.Equal("p", (await client.ExecuteManyAsync([["GET", "a"]]))[0].AsString());
        Assert.Equal(0, replica.Connections);
    }

    [Fact(Timeout = 15000)]
    public async Task MultiKeyReadsSplitToReplicasAndMultiKeyWritesToPrimaries()
    {
        using var primary0 = new FakeNode("p0");
        using var primary1 = new FakeNode("p1");
        using var replica0 = new FakeNode("r0");
        using var replica1 = new FakeNode("r1");

        primary0.Slots = TwoShards(primary0.Port, replica0.Port, primary1.Port, replica1.Port);
        await using RuvioConnection connection = await ConnectAsync(primary0, ReadPolicy.PreferReplica);
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        (string low, string high) = KeysOnBothShards();

        RespValue values = await client.ExecuteAsync(["MGET", low, high]);
        Assert.Equal(["r0", "r1"], values.Items!.Select(item => item.AsString()));
        Assert.Equal(2, (await client.ExecuteAsync(["DEL", low, high])).Integer);

        Assert.Equal($"MGET {low}", replica0.Log[1]);
        Assert.Equal($"MGET {high}", replica1.Log[1]);
        Assert.Contains($"DEL {low}", primary0.Log);
        Assert.Contains($"DEL {high}", primary1.Log);
        Assert.DoesNotContain(replica0.Log.Concat(replica1.Log), line => line.StartsWith("DEL", StringComparison.Ordinal));

        // The same keys in a pipeline of reads: each goes to its slot's replica.
        IReadOnlyList<RespValue> pipeline = await client.ExecuteManyAsync([["GET", low], ["GET", high], ["MGET", low, high]]);
        Assert.Equal(new[] { "r0", "r1" }, new[] { pipeline[0].AsString(), pipeline[1].AsString() });
        Assert.Equal(["r0", "r1"], pipeline[2].Items!.Select(item => item.AsString()));
    }

    [Fact(Timeout = 15000)]
    public async Task AnUnreachableReplicaFallsBackToThePrimaryOrFailsByPolicy()
    {
        using var primary = new FakeNode("p");
        using var down = new FakeNode("r");
        int port = down.Port;

        down.Close();
        primary.Slots = OneShard(primary.Port, port);

        await using (RuvioConnection connection = await ConnectAsync(primary, ReadPolicy.PreferReplica))
        {
            RuvioClient client = await connection.GetDefaultDatabaseAsync();

            Assert.Equal("p", await client.GetStringAsync("k"));
            Assert.Equal("p", await client.GetStringAsync("k"));
        }

        await using (RuvioConnection connection = await ConnectAsync(primary, ReadPolicy.Replica))
        {
            RuvioClient client = await connection.GetDefaultDatabaseAsync();

            await Assert.ThrowsAnyAsync<Exception>(async () => await client.GetStringAsync("k"));
        }
    }

    [Fact(Timeout = 15000)]
    public async Task ALoadingReplicaIsSkippedAndTheReadGoesToThePrimary()
    {
        using var primary = new FakeNode("p");
        using var replica = new FakeNode("r") { Loading = true };

        primary.Slots = OneShard(primary.Port, replica.Port);

        await using (RuvioConnection connection = await ConnectAsync(primary, ReadPolicy.PreferReplica))
        {
            RuvioClient client = await connection.GetDefaultDatabaseAsync();

            Assert.Equal("p", await client.GetStringAsync("k"));
            Assert.Equal("p", await client.GetStringAsync("k"));
            Assert.Equal("p", (await client.ExecuteManyAsync([["GET", "k"]]))[0].AsString());
            Assert.Equal(["READONLY", "GET k"], replica.Log);
        }

        await using (RuvioConnection connection = await ConnectAsync(primary, ReadPolicy.Replica))
        {
            RuvioClient client = await connection.GetDefaultDatabaseAsync();
            RuvioException error = await Assert.ThrowsAsync<RuvioException>(async () => await client.GetStringAsync("k"));

            Assert.Contains("LOADING", error.Message, StringComparison.Ordinal);
        }
    }

    [Fact(Timeout = 15000)]
    public async Task TheReplicaPolicyWithoutAReplicaFails()
    {
        using var primary = new FakeNode("p");

        primary.Slots = OneShard(primary.Port);
        await using RuvioConnection connection = await ConnectAsync(primary, ReadPolicy.Replica);
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        RuvioException error = await Assert.ThrowsAsync<RuvioException>(async () => await client.GetStringAsync("k"));

        Assert.Contains("no replica", error.Message, StringComparison.Ordinal);
        Assert.Equal("OK", (await client.ExecuteAsync(["SET", "k", "v"])).AsString());
    }

    [Fact]
    public void SlotsParsingKeepsReplicasAndSkipsMalformedOnes()
    {
        RespValue Node(string host, long port) => RespValue.Array(
            [RespValue.Bulk(Encoding.ASCII.GetBytes(host)), RespValue.FromInteger(port), RespValue.Bulk("id"u8.ToArray())]);

        RespValue range = RespValue.Array(
        [
            RespValue.FromInteger(0), RespValue.FromInteger(16383), Node("primary", 1),
            Node("replica-a", 2), RespValue.FromInteger(7), Node("replica-b", 99999), Node("replica-a", 2), Node("replica-c", 3),
        ]);
        ClusterTopology topology = ClusterTopology.Parse(RespValue.Array([range]))!;

        Assert.Equal([("primary", 1)], topology.Nodes);
        Assert.Equal([("replica-a", 2), ("replica-c", 3)], topology.Replicas[0]);
        Assert.Null(topology.ReadReplica(0));

        topology.PickReplicas(3);
        Assert.Equal(("replica-c", 3), topology.ReadReplica(0));
        topology.SkipReplica(0);
        Assert.Null(topology.ReadReplica(0));
    }

    [Fact]
    public void OnlyNonBlockingReadonlyCommandsCountAsReads()
    {
        Assert.True(ClusterKeys.Reads("GET"));
        Assert.True(ClusterKeys.Reads("HGETALL"));
        Assert.True(ClusterKeys.Reads("MGET"));
        Assert.False(ClusterKeys.Reads("SET"));
        Assert.False(ClusterKeys.Reads("DEL"));
        Assert.False(ClusterKeys.Reads("MSET"));
        Assert.False(ClusterKeys.Reads("BLPOP"));
        Assert.False(ClusterKeys.Reads("EVAL"));
        Assert.False(ClusterKeys.Reads("NOSUCH"));
        Assert.Equal(ReadPolicy.Primary, new RuvioClientOptions().ReadPolicy);
    }

    private static Task<RuvioConnection> ConnectAsync(FakeNode seed, ReadPolicy policy) =>
        RuvioConnection.ConnectAsync(new RuvioClientOptions
        {
            Host = "127.0.0.1",
            Port = seed.Port,
            ReadPolicy = policy,
            MaxReconnectAttempts = 1,
            ConnectTimeout = TimeSpan.FromSeconds(1),
        });

    private static string Node(int port) => $"*3\r\n$9\r\n127.0.0.1\r\n:{port}\r\n$2\r\nid\r\n";

    private static string OneShard(int primary, params int[] replicas) =>
        $"*1\r\n*{3 + replicas.Length}\r\n:0\r\n:16383\r\n{Node(primary)}{string.Concat(replicas.Select(Node))}";

    private static string TwoShards(int primary0, int replica0, int primary1, int replica1) =>
        $"*2\r\n*4\r\n:0\r\n:8191\r\n{Node(primary0)}{Node(replica0)}*4\r\n:8192\r\n:16383\r\n{Node(primary1)}{Node(replica1)}";

    private static (string Low, string High) KeysOnBothShards()
    {
        string low = Enumerable.Range(0, 100).Select(i => $"low{i}").First(key => RuvioClient.GetHashSlot(key) < 8192);
        string high = Enumerable.Range(0, 100).Select(i => $"high{i}").First(key => RuvioClient.GetHashSlot(key) >= 8192);

        return (low, high);
    }

    /// <summary>A scripted node: records every command as one line and answers by name, tagging replies with its own name.</summary>
    private sealed class FakeNode : IDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly string _name;
        private readonly ConcurrentQueue<string> _log = new();
        private readonly List<TcpClient> _clients = [];
        private int _connections;

        public FakeNode(string name)
        {
            _name = name;
            _listener.Start();
            Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
            _ = Task.Run(AcceptAsync);
        }

        public int Port { get; }

        public string Slots { get; set; } = "*0\r\n";

        public bool Loading { get; init; }

        public int Connections => Volatile.Read(ref _connections);

        public List<string> Log => [.. _log];

        public void Close() => _listener.Stop();

        public void Dispose()
        {
            _listener.Stop();

            lock (_clients)
            {
                foreach (TcpClient client in _clients)
                {
                    client.Dispose();
                }
            }
        }

        private async Task AcceptAsync()
        {
            try
            {
                while (true)
                {
                    TcpClient client = await _listener.AcceptTcpClientAsync();

                    lock (_clients)
                    {
                        _clients.Add(client);
                    }

                    Interlocked.Increment(ref _connections);
                    _ = Task.Run(() => ServeAsync(client));
                }
            }

            catch (Exception)
            {
                // The listener was stopped.
            }
        }

        private async Task ServeAsync(TcpClient client)
        {
            try
            {
                var stream = new BufferedStream(client.GetStream());

                while (true)
                {
                    string[] command = await ReadCommandAsync(stream);

                    _log.Enqueue(string.Join(' ', command));
                    await client.GetStream().WriteAsync(Encoding.ASCII.GetBytes(Reply(command)));
                }
            }

            catch (Exception)
            {
                // The client went away.
            }
        }

        private string Reply(string[] command)
        {
            string tag = $"${_name.Length}\r\n{_name}\r\n";

            switch (command[0].ToUpperInvariant())
            {
                case "CLUSTER":
                    return Slots;
                case "READONLY":
                    return "+OK\r\n";
                case "SET":
                    return "+OK\r\n";
                case "DEL":
                    return $":{command.Length - 1}\r\n";
                case "MGET":
                    return $"*{command.Length - 1}\r\n{string.Concat(Enumerable.Repeat(tag, command.Length - 1))}";
                default:
                    return Loading ? "-LOADING Ruvio is loading the dataset in memory\r\n" : tag;
            }
        }

        private static async Task<string[]> ReadCommandAsync(Stream stream)
        {
            string header = await ReadLineAsync(stream);
            var command = new string[int.Parse(header[1..])];

            for (int index = 0; index < command.Length; index++)
            {
                var bytes = new byte[int.Parse((await ReadLineAsync(stream))[1..]) + 2];

                await stream.ReadExactlyAsync(bytes);
                command[index] = Encoding.ASCII.GetString(bytes, 0, bytes.Length - 2);
            }

            return command;
        }

        private static async Task<string> ReadLineAsync(Stream stream)
        {
            var line = new StringBuilder();
            var one = new byte[1];

            while (true)
            {
                await stream.ReadExactlyAsync(one);

                if (one[0] == '\n')
                {
                    return line.ToString().TrimEnd('\r');
                }

                line.Append((char)one[0]);
            }
        }
    }
}
