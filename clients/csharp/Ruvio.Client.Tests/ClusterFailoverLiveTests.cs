using System.Diagnostics;
using System.Net.Sockets;
using System.Text;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

/// <summary>
/// A cluster client leaves a demoted primary after its <c>READONLY</c> reply, on real servers.
/// <c>scripts/run_with_servers.sh</c> starts two 2-shard nodes that can each be primary or replica
/// (<c>RUVIO_CLUSTER_FAILOVER_ADDRS</c>, shard 0 of each; their replication addresses in
/// <c>RUVIO_CLUSTER_FAILOVER_REPLICATION</c>; comma separated, primary first). No coordinator runs: the test swaps
/// the roles itself with <c>REPLICAOF</c>, as a coordinator does. Returns early without the variables.
/// </summary>
[Collection("Live")]
public class ClusterFailoverLiveTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    [Fact(Timeout = 60000)]
    public async Task AClusterClientReloadsItsSlotsAfterReadonlyFromADemotedPrimary()
    {
        if (Split("RUVIO_CLUSTER_FAILOVER_ADDRS") is not { } clients
            || Split("RUVIO_CLUSTER_FAILOVER_REPLICATION") is not { } replication)
        {
            return;
        }

        // Whichever node is the primary now, so the test can run again on the same servers.
        (int old, int next) = await IsMasterAsync(clients[0]) ? (0, 1) : (1, 0);

        // One key on each shard: {a} hashes to slot 15495 (shard 1), {b} to slot 3300 (shard 0).
        string[] keys = [$"cluster-failover:csharp:{Environment.ProcessId}:{{a}}", $"cluster-failover:csharp:{Environment.ProcessId}:{{b}}"];

        await using RuvioConnection connection = await RuvioConnection.ConnectAsync(new RuvioClientOptions
        {
            Host = clients[old].Host,
            Port = clients[old].Port,
            DiscoverCluster = true,
            FollowPrimary = false,
            CommandTimeout = TimeSpan.FromSeconds(5),
        });
        await using RuvioConnection onNext = await DirectAsync(clients[next]);
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        RuvioClient nextClient = await onNext.GetDefaultDatabaseAsync();

        foreach (string key in keys)
        {
            await client.SetAsync(key, "before");
        }

        // A replica that still copies refuses to be promoted without FORCE.
        await UntilAsync("the replica to hold the first writes", async () =>
        {
            foreach (string key in keys)
            {
                if (await nextClient.GetStringAsync(key) != "before")
                {
                    return false;
                }
            }

            return true;
        });

        await RawAsync(clients[next], "REPLICAOF", "NO", "ONE");
        await RawAsync(clients[old], "REPLICAOF", replication[next].Host, replication[next].Port.ToString());
        await UntilAsync("the old primary to become a replica", async () => !await IsMasterAsync(clients[old]));

        // The demoted node names its new primary once that primary has accepted it.
        var clock = Stopwatch.StartNew();

        while (true)
        {
            try
            {
                await client.SetAsync(keys[0], "after");

                break;
            }
            catch (RuvioException refused) when (refused.Message.StartsWith("READONLY", StringComparison.Ordinal))
            {
                Assert.True(clock.Elapsed < TimeSpan.FromSeconds(5), $"still refused after {clock.Elapsed}: {refused.Message}");
                await Task.Delay(20);
            }
        }

        Assert.Equal("after", await nextClient.GetStringAsync(keys[0]));

        // The other shard's slots moved too: no write is refused once the map is reloaded.
        for (int round = 0; round < 10; round++)
        {
            foreach (string key in keys)
            {
                await client.SetAsync(key, $"v{round}");
            }
        }

        foreach (string key in keys)
        {
            Assert.Equal("v9", await nextClient.GetStringAsync(key));
        }
    }

    private static Task<RuvioConnection> DirectAsync((string Host, int Port) address) =>
        RuvioConnection.ConnectAsync(new RuvioClientOptions
        {
            Host = address.Host,
            Port = address.Port,
            DiscoverCluster = false,
            FollowPrimary = false,
            CommandTimeout = TimeSpan.FromSeconds(5),
        });

    private static async Task<bool> IsMasterAsync((string Host, int Port) address)
    {
        await using RuvioConnection connection = await DirectAsync(address);
        RespValue role = await (await connection.GetDefaultDatabaseAsync()).GetRoleAsync();

        return string.Equals(role.Items?[0].AsString(), "master", StringComparison.OrdinalIgnoreCase);
    }

    private static async Task UntilAsync(string what, Func<Task<bool>> done)
    {
        var clock = Stopwatch.StartNew();

        while (!await done())
        {
            Assert.True(clock.Elapsed < Patience, $"gave up waiting for {what}");
            await Task.Delay(20);
        }
    }

    /// <summary>One command over a raw RESP socket; the reply must not be an error.</summary>
    private static async Task RawAsync((string Host, int Port) address, params string[] arguments)
    {
        using var timeout = new CancellationTokenSource(Patience);
        using var socket = new TcpClient();

        await socket.ConnectAsync(address.Host, address.Port, timeout.Token);

        NetworkStream stream = socket.GetStream();

        await stream.WriteAsync(RespCodec.Encode(arguments), timeout.Token);

        var line = new List<byte>();
        var one = new byte[1];

        while (!(line.Count >= 2 && line[^2] == '\r' && line[^1] == '\n'))
        {
            await stream.ReadExactlyAsync(one, timeout.Token);
            line.Add(one[0]);
        }

        string reply = Encoding.ASCII.GetString([.. line]);

        Assert.True(reply.StartsWith('+'), $"{string.Join(' ', arguments)} answered {reply.TrimEnd()}");
    }

    private static (string Host, int Port)[]? Split(string name)
    {
        string[]? parts = Environment.GetEnvironmentVariable(name)?.Split(',');

        if (parts is not { Length: 2 })
        {
            return null;
        }

        var addresses = new (string Host, int Port)[2];

        for (int index = 0; index < 2; index++)
        {
            int colon = parts[index].LastIndexOf(':');

            if (colon <= 0 || !int.TryParse(parts[index][(colon + 1)..], out int port))
            {
                return null;
            }

            addresses[index] = (parts[index][..colon], port);
        }

        return addresses;
    }
}
