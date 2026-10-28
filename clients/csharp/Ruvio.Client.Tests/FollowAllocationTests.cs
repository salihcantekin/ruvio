using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

/// <summary>
/// A client that follows a failover (two seeds, <c>FollowPrimary</c> on) allocates what a single-seed client allocates
/// per command while the connection is healthy, and keeps no state that grows with the number of commands. Runs against
/// the failover pair that <c>scripts/run_with_servers.sh</c> starts (<c>RUVIO_FAILOVER_ADDRS</c>); returns early without
/// it. The commands complete on socket threads, so the whole process is counted, not the test thread. The two clients
/// take turns round by round and each is judged by its median round, so an allocation by an unrelated thread (pool,
/// timer, runner) lands on a few rounds of either client and moves neither median.
/// </summary>
[Collection("Allocation")]
public class FollowAllocationTests
{
    private const int Trials = 3;
    private const int Warmup = 30;
    private const int Rounds = 500;

    // Bytes per round (a SET and a GET, or 16 pipelined commands) a following client may allocate beyond a single-seed
    // one, median against median. Summing whole runs let a few KB of unrelated process-wide allocation through as 24-209
    // bytes per round on busy CI runners; the medians of the two clients agree to a few bytes. The smallest object, one
    // per command, is 24 bytes, so a per-command allocation by following mode shows up in every round as at least 48
    // bytes (384 for the pipeline of 16) and still fails above this slack.
    private const double Slack = 32;

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

            double loopGap = double.MaxValue, batchGap = double.MaxValue;
            string report = "";

            for (int trial = 0; trial < Trials && (loopGap > Slack || batchGap > Slack); trial++)
            {
                (long singleLoop, long followingLoop) =
                    await MedianRoundsAsync(() => SetGet(pair.Single), () => SetGet(pair.Following));
                (long singleBatch, long followingBatch) =
                    await MedianRoundsAsync(() => Pipelined(pair.Single), () => Pipelined(pair.Following));

                Assert.True(singleLoop > 0, "the measurement counts nothing");
                loopGap = Math.Min(loopGap, followingLoop - singleLoop);
                batchGap = Math.Min(batchGap, followingBatch - singleBatch);
                report +=
                    $"SET+GET single {singleLoop} B, following {followingLoop} B; " +
                    $"pipeline of 16 single {singleBatch} B, following {followingBatch} B. ";
            }

            Assert.True(loopGap <= Slack, $"SET+GET gap {loopGap} B per round; {report}");
            Assert.True(batchGap <= Slack, $"pipeline gap {batchGap} B per round; {report}");
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

    /// <summary>
    /// Median bytes allocated by one round of <paramref name="single"/> and of <paramref name="following"/>, after a
    /// warm-up. The rounds alternate, so noise from other threads falls on both clients alike.
    /// </summary>
    private static async Task<(long Single, long Following)> MedianRoundsAsync(Func<Task> single, Func<Task> following)
    {
        long[] singleRounds = new long[Rounds];
        long[] followingRounds = new long[Rounds];

        for (int index = 0; index < Warmup; index++)
        {
            await single();
            await following();
        }

        for (int index = 0; index < Rounds; index++)
        {
            long before = GC.GetTotalAllocatedBytes(precise: true);

            await single();

            long middle = GC.GetTotalAllocatedBytes(precise: true);

            await following();

            long after = GC.GetTotalAllocatedBytes(precise: true);

            singleRounds[index] = middle - before;
            followingRounds[index] = after - middle;
        }

        Array.Sort(singleRounds);
        Array.Sort(followingRounds);

        return (singleRounds[Rounds / 2], followingRounds[Rounds / 2]);
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
