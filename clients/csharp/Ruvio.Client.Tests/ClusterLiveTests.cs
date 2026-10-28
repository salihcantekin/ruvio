using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

// It flushes the cluster and counts its keys, so it must not run beside another cluster test.
[Collection("Live")]
public class ClusterLiveTests
{
    [Fact(Timeout = 15000)]
    public async Task ReportsSlotMovesAndRefusesARangeAlreadyInPlace()
    {
        string? address = Environment.GetEnvironmentVariable("RUVIO_CLUSTER_ADDR");

        if (string.IsNullOrWhiteSpace(address))
        {
            return;
        }

        string[] parts = address.Split(':');

        await using RuvioConnection connection = await RuvioConnection.ConnectAsync(parts[0], int.Parse(parts[1]));
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        IReadOnlyDictionary<string, string> status = await client.GetMoveSlotsStatusAsync();

        Assert.True(status.ContainsKey("state"));
        // Slot 0 is on shard 0: nothing to move, and the server says why.
        RuvioException refused = await Assert.ThrowsAnyAsync<RuvioException>(async () => await client.MoveSlotsAsync(0, 10, 0));
        Assert.Contains("already on that shard", refused.Message);
    }

    [Fact(Timeout = 15000)]
    public async Task RoutesKeysAndSplitsMgetAgainstALiveCluster()
    {
        string? address = Environment.GetEnvironmentVariable("RUVIO_CLUSTER_ADDR");

        if (string.IsNullOrWhiteSpace(address))
        {
            return;
        }

        string[] parts = address.Split(':');

        await using RuvioConnection connection = await RuvioConnection.ConnectAsync(parts[0], int.Parse(parts[1]));
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        Assert.True(client.IsCluster);
        Assert.True(client.ShardCount >= 2);
        Assert.Equal(11058, RuvioClient.GetHashSlot("somekey"));
        await client.FlushDatabaseAsync();
        await client.SetAsync("{user1000}.following", "left");
        await client.SetAsync("somekey", "right");
        Assert.Equal(
            ["left", "right"],
            await client.GetManyAsync("{user1000}.following", "somekey"));
        Assert.Equal(11058, await client.GetClusterKeySlotAsync("somekey"));
        Assert.Contains("cluster_state:ok", await client.GetClusterInfoAsync());
        Assert.Equal(2, await client.GetDatabaseSizeAsync());
        await client.ReadOnlyAsync();
        await client.ReadWriteAsync();
    }

    [Fact(Timeout = 15000)]
    public async Task RenameOntoAnotherSlotIsRefused()
    {
        string? address = Environment.GetEnvironmentVariable("RUVIO_CLUSTER_ADDR");

        if (string.IsNullOrWhiteSpace(address))
        {
            return;
        }

        string[] parts = address.Split(':');

        await using RuvioConnection connection = await RuvioConnection.ConnectAsync(parts[0], int.Parse(parts[1]));
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        const string source = "portal-rename-cs";
        const string destination = "key1-rename-cs";

        Assert.NotEqual(RuvioClient.GetHashSlot(source), RuvioClient.GetHashSlot(destination));
        await client.DeleteAsync([source, destination]);
        await client.SetAsync(source, "from-screen");
        RuvioException error = await Assert.ThrowsAsync<RuvioException>(async () => await client.RenameAsync(source, destination));
        Assert.StartsWith("CROSSSLOT", error.Message);
        Assert.Equal("from-screen", await client.GetStringAsync(source));
        Assert.Null(await client.GetStringAsync(destination));
        await client.DeleteAsync(source);
    }
}
