using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

/// <summary>A test that needs the replicated pair <c>scripts/run_with_servers.sh</c> starts; skipped without it.</summary>
public sealed class ReplicatedFactAttribute : FactAttribute
{
    public ReplicatedFactAttribute()
    {
        if (ReplicaReadLiveTests.Primary is null || ReplicaReadLiveTests.Replica is null)
        {
            Skip = "RUVIO_REPLICATED_ADDR / RUVIO_REPLICA_ADDR are not set";
        }
    }
}

/// <summary>
/// Runs against a 2-shard primary on <c>RUVIO_REPLICATED_ADDR</c> and its 2-shard replica on
/// <c>RUVIO_REPLICA_ADDR</c> (replica shard i is on port + i).
/// </summary>
[Collection("Live")]
public class ReplicaReadLiveTests
{
    internal static (string Host, int Port)? Primary => Address("RUVIO_REPLICATED_ADDR");

    internal static (string Host, int Port)? Replica => Address("RUVIO_REPLICA_ADDR");

    [ReplicatedFact(Timeout = 30000)]
    public Task PreferReplicaReadsFromTheReplicaAndWritesToThePrimary() =>
        ReadsCount(ReadPolicy.PreferReplica, "csharp:replica-reads:prefer", expected: 1);

    [ReplicatedFact(Timeout = 30000)]
    public Task ReplicaPolicyReadsFromTheReplica() =>
        ReadsCount(ReadPolicy.Replica, "csharp:replica-reads:only", expected: 1);

    [ReplicatedFact(Timeout = 30000)]
    public Task PrimaryPolicyKeepsReadsOnThePrimary() =>
        ReadsCount(ReadPolicy.Primary, "csharp:replica-reads:primary", expected: 0);

    [ReplicatedFact(Timeout = 30000)]
    public async Task DurabilityOptionLetsWritesThroughOnAReplicatedPrimary()
    {
        (string host, int port) = Primary!.Value;
        await using RuvioConnection connection = await RuvioConnection.ConnectAsync(new RuvioClientOptions
        {
            Host = host,
            Port = port,
            DurabilityReplicas = 1,
            DurabilityTimeout = TimeSpan.FromMilliseconds(5000),
        });
        RuvioClient db = await connection.GetDefaultDatabaseAsync();

        Assert.True(await db.SetAsync("csharp:durability", "v1"));
        Assert.Equal("v1", await db.GetStringAsync("csharp:durability"));
    }

    private static async Task ReadsCount(ReadPolicy policy, string key, int expected)
    {
        (string host, int port) = Primary!.Value;
        await using RuvioConnection connection = await RuvioConnection.ConnectAsync(
            new RuvioClientOptions { Host = host, Port = port, ReadPolicy = policy });
        RuvioClient db = await connection.GetDefaultDatabaseAsync();
        await using RuvioConnection shard = await ShardAsync(key);
        RuvioClient onReplica = await shard.GetDefaultDatabaseAsync();

        Assert.True(await db.SetAsync(key, "v1"));
        await WaitForCopyAsync(onReplica, key, "v1");
        long before = await GetCommandsAsync(onReplica);

        Assert.Equal("v1", await db.GetStringAsync(key));
        Assert.Equal(before + expected, await GetCommandsAsync(onReplica));
    }

    /// <summary>One shard of the replica, without cluster routing.</summary>
    private static Task<RuvioConnection> ShardAsync(string key)
    {
        (string host, int port) = Replica!.Value;

        return RuvioConnection.ConnectAsync(new RuvioClientOptions
        {
            Host = host,
            Port = port + (RuvioClient.GetHashSlot(key) >= 8192 ? 1 : 0),
            DiscoverCluster = false,
        });
    }

    private static async Task<long> GetCommandsAsync(RuvioClient shard)
    {
        string info = await shard.GetInfoAsync();
        string line = info.Split('\n').First(line => line.StartsWith("get_commands:", StringComparison.Ordinal));

        return long.Parse(line["get_commands:".Length..].Trim());
    }

    private static async Task WaitForCopyAsync(RuvioClient shard, string key, string value)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(10);

        while (await shard.GetStringAsync(key) != value)
        {
            Assert.True(DateTime.UtcNow < deadline, $"the replica did not copy {key}");
            await Task.Delay(20);
        }
    }

    private static (string Host, int Port)? Address(string variable)
    {
        string? value = Environment.GetEnvironmentVariable(variable);
        int colon = value?.LastIndexOf(':') ?? -1;

        return colon > 0 && int.TryParse(value![(colon + 1)..], out int port) ? (value[..colon], port) : null;
    }
}
