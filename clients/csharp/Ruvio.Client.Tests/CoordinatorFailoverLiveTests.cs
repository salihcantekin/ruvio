using System.Diagnostics;
using Ruvio.Client;
using Xunit;
using Xunit.Abstractions;

namespace Ruvio.Client.Tests;

/// <summary>
/// Clients follow a failover that real coordinators carry out. <c>scripts/run_with_servers.sh</c> starts a group of two
/// data nodes (<c>RUVIO_COORDINATOR_GROUP_ADDRS</c>, their pids in <c>RUVIO_COORDINATOR_GROUP_PIDS</c>) watched by three
/// <c>ruvio coordinator</c> processes (<c>RUVIO_COORDINATOR_ADDRS</c>, the group named
/// <c>RUVIO_COORDINATOR_MASTER</c>). The test kills the primary with SIGKILL, so the group serves one run of the script.
/// Returns early without the variables.
/// </summary>
[Collection("Live")]
public class CoordinatorFailoverLiveTests(ITestOutputHelper output)
{
    // The coordinators notice the kill after down_after_ms (1 s) and promote within a few ticks.
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(15);

    [Fact(Timeout = 60000)]
    public async Task ClientsReachTheNewPrimaryAfterTheCoordinatorsFailOver()
    {
        string[]? coordinators = List("RUVIO_COORDINATOR_ADDRS");
        string? master = Environment.GetEnvironmentVariable("RUVIO_COORDINATOR_MASTER");
        string[]? members = List("RUVIO_COORDINATOR_GROUP_ADDRS");
        string[]? pids = List("RUVIO_COORDINATOR_GROUP_PIDS");

        if (coordinators is null || string.IsNullOrEmpty(master) || members is not { Length: 2 } || pids is not { Length: 2 })
        {
            return;
        }

        await using RuvioConnection first = await DirectAsync(members[0]);
        await using RuvioConnection second = await DirectAsync(members[1]);
        RuvioClient[] onMember = [await first.GetDefaultDatabaseAsync(), await second.GetDefaultDatabaseAsync()];
        (int old, int next) = await IsMasterAsync(onMember[0]) ? (0, 1) : (1, 0);

        Assert.True(await IsMasterAsync(onMember[old]) && !await IsMasterAsync(onMember[next]), "the group does not have exactly one primary");

        // One client asks the coordinators for the primary, the other has the data nodes as seeds.
        RuvioClientOptions throughCoordinators = Options(coordinators);

        throughCoordinators.SentinelMaster = master;

        await using RuvioConnection sentinelConnection = await RuvioConnection.ConnectAsync(throughCoordinators);
        await using RuvioConnection seedsConnection = await RuvioConnection.ConnectAsync(Options(members));
        var clients = new Dictionary<string, RuvioClient>
        {
            ["SentinelMaster"] = await sentinelConnection.GetDefaultDatabaseAsync(),
            ["Seeds"] = await seedsConnection.GetDefaultDatabaseAsync(),
        };
        string Key(string name) => $"coordinated:csharp:{Environment.ProcessId}:{name}";

        foreach ((string name, RuvioClient client) in clients)
        {
            await client.SetAsync(Key(name), "before");
        }

        // Only a replica that holds every acknowledged write is promoted.
        var waiting = Stopwatch.StartNew();

        foreach (string name in clients.Keys)
        {
            while (await onMember[next].GetStringAsync(Key(name)) != "before")
            {
                Assert.True(waiting.Elapsed < Patience, "the replica did not hold the first writes");
                await Task.Delay(20);
            }
        }

        using (Process primary = Process.GetProcessById(int.Parse(pids[old])))
        {
            primary.Kill();
        }

        var clock = Stopwatch.StartNew();

        // Every client writes at once, so none waits for another.
        async Task<TimeSpan> WriteUntilItSucceedsAsync(string name, RuvioClient client)
        {
            while (true)
            {
                try
                {
                    await client.SetAsync(Key(name), "after");

                    return clock.Elapsed;
                }
                catch (Exception error) when (error is RuvioException or IOException or System.Net.Sockets.SocketException or TimeoutException)
                {
                    Assert.True(clock.Elapsed < Patience, $"{name}: no write succeeded: {error}");
                }

                await Task.Delay(50);
            }
        }

        TimeSpan[] took = await Task.WhenAll(clients.Select(pair => WriteUntilItSucceedsAsync(pair.Key, pair.Value)));

        output.WriteLine($"first write after the kill: {string.Join(", ", clients.Keys.Zip(took, (name, time) => $"{name} {time.TotalMilliseconds:F0} ms"))}");

        Assert.True(await IsMasterAsync(onMember[next]), "the surviving member is not the primary");

        foreach (string name in clients.Keys)
        {
            Assert.Equal("after", await onMember[next].GetStringAsync(Key(name)));
        }
    }

    private static RuvioClientOptions Options(IEnumerable<string> seeds) => new()
    {
        Seeds = [.. seeds],
        DiscoverCluster = false,
        ConnectTimeout = TimeSpan.FromSeconds(1),
        CommandTimeout = TimeSpan.FromSeconds(5),
    };

    private static Task<RuvioConnection> DirectAsync(string address) =>
        RuvioConnection.ConnectAsync(Options([address]));

    private static async Task<bool> IsMasterAsync(RuvioClient client)
    {
        RespValue role = await client.GetRoleAsync();

        return string.Equals(role.Items?[0].AsString(), "master", StringComparison.OrdinalIgnoreCase);
    }

    private static string[]? List(string name) =>
        Environment.GetEnvironmentVariable(name) is { Length: > 0 } value ? value.Split(',') : null;
}
