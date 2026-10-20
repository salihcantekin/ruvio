using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

/// <summary>A test that needs the multi-node cluster <c>scripts/run_with_servers.sh</c> starts; skipped without it.</summary>
public sealed class MultiNodeFactAttribute : FactAttribute
{
    public MultiNodeFactAttribute()
    {
        if (MultiNodeLiveTests.Stale is null || MultiNodeLiveTests.NodeA is null || MultiNodeLiveTests.NodeB is null)
        {
            Skip = "RUVIO_MULTINODE_A_ADDR / RUVIO_MULTINODE_B_ADDR / RUVIO_MULTINODE_STALE_ADDR are not set";
        }
    }
}

/// <summary>
/// Runs against node a (<c>RUVIO_MULTINODE_A_ADDR</c>, slots 0-8191), node b
/// (<c>RUVIO_MULTINODE_B_ADDR</c>, slots 8192-16383) and a node with an older map
/// (<c>RUVIO_MULTINODE_STALE_ADDR</c>) that gives every slot to node a.
/// </summary>
[Collection("Live")]
public class MultiNodeLiveTests
{
    internal static (string Host, int Port)? NodeA => Address("RUVIO_MULTINODE_A_ADDR");

    internal static (string Host, int Port)? NodeB => Address("RUVIO_MULTINODE_B_ADDR");

    internal static (string Host, int Port)? Stale => Address("RUVIO_MULTINODE_STALE_ADDR");

    [MultiNodeFact(Timeout = 30000)]
    public async Task FollowsMovedFromANodeWithAnOlderMap()
    {
        // Node b's slot (12182) and node a's (5061).
        const string onB = "{foo}csharp:multinode";
        const string onA = "{bar}csharp:multinode";

        Assert.Equal(12182, RuvioClient.GetHashSlot(onB));
        Assert.Equal(5061, RuvioClient.GetHashSlot(onA));

        // Seeded with the stale node, the client first sends node b's key to node a, which
        // answers MOVED.
        (string host, int port) = Stale!.Value;
        await using RuvioConnection connection = await RuvioConnection.ConnectAsync(
            new RuvioClientOptions { Host = host, Port = port });
        RuvioClient db = await connection.GetDefaultDatabaseAsync();

        Assert.True(await db.SetAsync(onB, "b"));
        Assert.True(await db.SetAsync(onA, "a"));
        Assert.Equal("b", await db.GetStringAsync(onB));
        Assert.Equal("a", await db.GetStringAsync(onA));

        // Each key is on the node that owns it.
        Assert.Equal("b", await GetOnNodeAsync(NodeB!.Value, onB));
        Assert.Equal("a", await GetOnNodeAsync(NodeA!.Value, onA));
    }

    /// <summary>The key's value read from one node, without cluster routing.</summary>
    private static async Task<string?> GetOnNodeAsync((string Host, int Port) node, string key)
    {
        await using RuvioConnection connection = await RuvioConnection.ConnectAsync(
            new RuvioClientOptions { Host = node.Host, Port = node.Port, DiscoverCluster = false });
        RuvioClient db = await connection.GetDefaultDatabaseAsync();

        return await db.GetStringAsync(key);
    }

    private static (string Host, int Port)? Address(string variable)
    {
        string? value = Environment.GetEnvironmentVariable(variable);
        int colon = value?.LastIndexOf(':') ?? -1;

        return colon > 0 && int.TryParse(value![(colon + 1)..], out int port) ? (value[..colon], port) : null;
    }
}
