using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

/// <summary>
/// A client that follows a failover (two seeds, <c>FollowPrimary</c> on) allocates what a single-seed client allocates
/// per command while the connection is healthy, and keeps no state that grows with the number of commands. Runs against
/// the failover pair that <c>scripts/run_with_servers.sh</c> starts (<c>RUVIO_FAILOVER_ADDRS</c>); returns early without
/// it. The commands complete on socket threads, so the whole process is counted, not the test thread; the tests of this
/// collection run alone, and each measurement keeps the best of three trials as <see cref="AllocationTests"/> does.
/// </summary>
[Collection("Allocation")]
public class FollowAllocationTests
{
    private const int Trials = 5;
    private const int Warmup = 30;
    private const int Rounds = 100;

    // Bytes per round (a SET and a GET, or 16 pipelined commands) a following client may allocate beyond a single-seed
    // one. Measured over 10 runs: the two differ by at most ten bytes per round in either direction, from pool and timer noise in the
    // process-wide count. The smallest object, one per command, is 24 bytes, so a per-command allocation by following
    // mode shows up as at least 48 bytes per round and fails.
    private const double Slack = 16;

    [Fact(Timeout = 120000)]
    public async Task AFollowingClientAllocatesLikeASingleSeedClientPerCommand()
    {
        if (await ConnectPairAsync() is not { } pair)
        {
            return;
        }

        await using (pair)
        {
            string value = new('v', 32);
            string key = $"follow:alloc:{Environment.ProcessId}";
            IReadOnlyList<IReadOnlyList<string>> batch =
            [
                .. Enumerable.Range(0, 16).Select(index =>
                    index % 2 == 0 ? (IReadOnlyList<string>)["SET", key, value] : ["GET", key]),
            ];

            async Task SetGet(RuvioClient client)
            {
                await client.SetAsync(key, value);
                Assert.Equal(value, await client.GetStringAsync(key));
            }

            async Task Pipelined(RuvioClient client) =>
                Assert.Equal(16, (await client.ExecuteManyAsync(batch)).Count);

            long singleLoop = long.MaxValue, followingLoop = long.MaxValue;
            long singleBatch = long.MaxValue, followingBatch = long.MaxValue;

            for (int trial = 0; trial < Trials; trial++)
            {
                singleLoop = Math.Min(singleLoop, await MeasureAsync(() => SetGet(pair.Single)));
                followingLoop = Math.Min(followingLoop, await MeasureAsync(() => SetGet(pair.Following)));
                singleBatch = Math.Min(singleBatch, await MeasureAsync(() => Pipelined(pair.Single)));
                followingBatch = Math.Min(followingBatch, await MeasureAsync(() => Pipelined(pair.Following)));
            }

            Console.WriteLine(
                $"follow alloc: SET+GET single {singleLoop / (double)Rounds:F1} B, following {followingLoop / (double)Rounds:F1} B; " +
                $"pipeline of 16 single {singleBatch / (double)Rounds:F1} B, following {followingBatch / (double)Rounds:F1} B");

            Assert.True(singleLoop > 0, "the measurement counts nothing");
            Assert.InRange((followingLoop - singleLoop) / (double)Rounds, double.NegativeInfinity, Slack);
            Assert.InRange((followingBatch - singleBatch) / (double)Rounds, double.NegativeInfinity, Slack);
        }
    }

    [Fact(Timeout = 120000)]
    public async Task AFollowingClientRetainsNothingPerCommand()
    {
        if (await ConnectPairAsync() is not { } pair)
        {
            return;
        }

        await using (pair)
        {
            string key = $"follow:retain:{Environment.ProcessId}";
            IReadOnlyList<IReadOnlyList<string>> batch =
            [
                .. Enumerable.Range(0, 16).Select(index =>
                    index % 2 == 0 ? (IReadOnlyList<string>)["SET", key, "v"] : ["GET", key]),
            ];

            for (int index = 0; index < 100; index++)
            {
                await pair.Following.ExecuteManyAsync(batch);
            }

            long before = Retained();

            // 20000 commands in 1250 round trips.
            for (int index = 0; index < 1250; index++)
            {
                await pair.Following.ExecuteManyAsync(batch);
            }

            long grown = Retained() - before;

            Console.WriteLine($"follow alloc: retained bytes grew by {grown} over 20000 commands");
            // Noise measured up to about 90 KB (pool trimming); one retained 24-byte object per command is 480 KB.
            // Only growth is a leak: a collection that frees more than it held before is not.
            Assert.InRange(grown, long.MinValue, 196608);
        }
    }

    private static long Retained() => GC.GetTotalMemory(forceFullCollection: true);

    /// <summary>Bytes allocated per <see cref="Rounds"/> rounds of <paramref name="round"/>, after a warm-up.</summary>
    private static async Task<long> MeasureAsync(Func<Task> round)
    {
        for (int index = 0; index < Warmup; index++)
        {
            await round();
        }

        long before = GC.GetTotalAllocatedBytes(precise: true);

        for (int index = 0; index < Rounds; index++)
        {
            await round();
        }

        return GC.GetTotalAllocatedBytes(precise: true) - before;
    }

    private static async Task<Pair?> ConnectPairAsync()
    {
        string[]? parts = Environment.GetEnvironmentVariable("RUVIO_FAILOVER_ADDRS")?.Split(',');

        if (parts is not { Length: 2 })
        {
            return null;
        }

        // The failover tests may have swapped the roles on these servers: the primary is whichever says so now.
        RuvioClientOptions probe = Options([parts[0]]);

        await using RuvioConnection first = await RuvioConnection.ConnectAsync(probe);
        RespValue role = await (await first.GetDefaultDatabaseAsync()).GetRoleAsync();
        bool firstIsPrimary = string.Equals(role.Items?[0].AsString(), "master", StringComparison.OrdinalIgnoreCase);
        string primary = firstIsPrimary ? parts[0] : parts[1];
        string other = firstIsPrimary ? parts[1] : parts[0];

        RuvioConnection single = await RuvioConnection.ConnectAsync(Options([primary]));
        RuvioConnection following = await RuvioConnection.ConnectAsync(Options([primary, other]));

        return new Pair(single, following, await single.GetDefaultDatabaseAsync(), await following.GetDefaultDatabaseAsync());
    }

    private static RuvioClientOptions Options(IEnumerable<string> seeds) => new()
    {
        Seeds = [.. seeds],
        DiscoverCluster = false,
        CommandTimeout = TimeSpan.FromSeconds(5),
    };

    private sealed class Pair(RuvioConnection singleConnection, RuvioConnection followingConnection, RuvioClient single, RuvioClient following)
        : IAsyncDisposable
    {
        public RuvioClient Single { get; } = single;

        public RuvioClient Following { get; } = following;

        public async ValueTask DisposeAsync()
        {
            await singleConnection.DisposeAsync();
            await followingConnection.DisposeAsync();
        }
    }
}
