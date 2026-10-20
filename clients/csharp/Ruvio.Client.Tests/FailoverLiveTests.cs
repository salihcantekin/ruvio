using System.Diagnostics;
using System.Net.Sockets;
using System.Text;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

/// <summary>
/// A client with two seeds follows a failover on real servers. <c>scripts/run_with_servers.sh</c> starts a pair that can
/// each be primary or replica (<c>RUVIO_FAILOVER_ADDRS</c>, their replication addresses in
/// <c>RUVIO_FAILOVER_REPLICATION</c>, comma separated, primary first); no coordinator runs, the test swaps the roles
/// itself with <c>REPLICAOF</c>, as a coordinator does. Returns early without the variables.
/// </summary>
[Collection("Live")]
public class FailoverLiveTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    [Fact(Timeout = 60000)]
    public async Task AClientFollowsThePrimaryAfterTheRolesAreSwapped()
    {
        if (Pair() is not { } pair)
        {
            return;
        }

        // Whichever node is the primary now, so the test can run again on the same servers.
        (int old, int next) = await IsMasterAsync(pair.Clients[0]) ? (0, 1) : (1, 0);
        RuvioClientOptions options = Options([pair.Clients[old], pair.Clients[next]]);
        string key = $"failover:{Environment.ProcessId}";

        await using RuvioConnection connection = await RuvioConnection.ConnectAsync(options);
        await using RuvioConnection onOld = await DirectAsync(pair.Clients[old]);
        await using RuvioConnection onNext = await DirectAsync(pair.Clients[next]);
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        RuvioClient oldClient = await onOld.GetDefaultDatabaseAsync();
        RuvioClient nextClient = await onNext.GetDefaultDatabaseAsync();

        await client.SetAsync(key, "before");

        // A replica that still copies refuses to be promoted without FORCE.
        await UntilAsync("the replica to hold the first write", async () => await nextClient.GetStringAsync(key) == "before");

        // The roles are swapped over raw sockets, as an operator or coordinator would.
        await RawAsync(pair.Clients[next], "REPLICAOF", "NO", "ONE");
        await RawAsync(pair.Clients[old], "REPLICAOF", pair.Replication[next].Host, pair.Replication[next].Port.ToString());

        var clock = Stopwatch.StartNew();

        await client.SetAsync(key, "after");
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(5), clock.Elapsed.ToString());
        Assert.Equal("after", await nextClient.GetStringAsync(key));

        // Without the option the same situation is the READONLY error.
        RuvioClientOptions stuckOptions = Options([pair.Clients[old], pair.Clients[next]]);

        stuckOptions.FollowPrimary = false;

        await using RuvioConnection stuck = await RuvioConnection.ConnectAsync(stuckOptions);
        RuvioClient stuckClient = await stuck.GetDefaultDatabaseAsync();
        RuvioException refused = await Assert.ThrowsAsync<RuvioException>(async () => await stuckClient.SetAsync(key, "never"));

        Assert.StartsWith("READONLY", refused.Message, StringComparison.Ordinal);

        // A dropped socket: the reconnect starts at the old primary (now a replica) but takes the node that
        // answers master, before any write.
        long id = await client.GetClientIdAsync();

        await nextClient.ExecuteAsync("CLIENT", "KILL", "ID", id.ToString());

        // The command that hits the drop fails; the next one reconnects.
        await UntilAsync("the client to see its socket closed", async () =>
        {
            try
            {
                await client.PingAsync();

                return false;
            }
            catch (Exception error) when (error is RuvioException or IOException or SocketException)
            {
                return true;
            }
        });

        Assert.True(await IsMasterAsync(client), "the reconnect did not land on the primary");

        await client.SetAsync(key, "again");
        Assert.Equal("again", await nextClient.GetStringAsync(key));
        Assert.False(await IsMasterAsync(oldClient));
    }

    private static RuvioClientOptions Options(IReadOnlyList<(string Host, int Port)> seeds) => new()
    {
        Seeds = [.. seeds.Select(seed => $"{seed.Host}:{seed.Port}")],
        DiscoverCluster = false,
        CommandTimeout = TimeSpan.FromSeconds(5),
    };

    private static Task<RuvioConnection> DirectAsync((string Host, int Port) address) =>
        RuvioConnection.ConnectAsync(new RuvioClientOptions
        {
            Host = address.Host,
            Port = address.Port,
            DiscoverCluster = false,
            CommandTimeout = TimeSpan.FromSeconds(5),
        });

    private static async Task<bool> IsMasterAsync((string Host, int Port) address)
    {
        await using RuvioConnection connection = await DirectAsync(address);

        return await IsMasterAsync(await connection.GetDefaultDatabaseAsync());
    }

    private static async Task<bool> IsMasterAsync(RuvioClient client)
    {
        RespValue role = await client.GetRoleAsync();

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

    private static ((string Host, int Port)[] Clients, (string Host, int Port)[] Replication)? Pair()
    {
        (string Host, int Port)[]? clients = Split("RUVIO_FAILOVER_ADDRS");
        (string Host, int Port)[]? replication = Split("RUVIO_FAILOVER_REPLICATION");

        return clients is null || replication is null ? null : (clients, replication);
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
