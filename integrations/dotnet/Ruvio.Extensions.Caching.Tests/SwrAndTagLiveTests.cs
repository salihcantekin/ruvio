using System.Text;
using Microsoft.Extensions.Caching.Distributed;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Extensions.Caching.Tests;

public partial class LiveTests
{
    private static RuvioCacheEntryOptions Swr(int freshMilliseconds) => new()
    {
        AbsoluteExpirationRelativeToNow = TimeSpan.FromMilliseconds(freshMilliseconds),
        StaleFor = TimeSpan.FromSeconds(20),
    };

    // Waits until the entry's fresh period is over by the server's own answer, so the test needs no fixed sleep margin.
    private static async Task WaitUntilStale(Scope scope, string key)
    {
        for (int attempt = 0; attempt < 200; attempt++)
        {
            if (await scope.Cache.GetAsync(key) is null)
                return;

            await Task.Delay(20);
        }

        Assert.Fail("The entry never went stale.");
    }

    [LiveFact]
    public async Task Stale_entries_are_refreshed_by_one_caller_while_the_others_get_the_stale_value()
    {
        await using var scope = await Scope.Open();
        string key = "swr";

        scope.Key(key);
        await scope.Cache.GetOrCreateStringAsync(key, _ => Task.FromResult("v1"), Swr(200));
        Assert.StartsWith("RDC2\n", Encoding.ASCII.GetString((await scope.Client.ExecuteBinaryAsync(["GET"u8.ToArray(), Encoding.UTF8.GetBytes(scope.Key(key))])).Bytes!, 0, 5));
        await WaitUntilStale(scope, key);

        // A plain Get misses a stale entry but leaves it stored for GetOrCreate.
        Assert.Equal(1, (await scope.Client.ExecuteAsync("EXISTS", scope.Key(key))).Integer);

        int builds = 0;
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        async Task<string> Build(CancellationToken token)
        {
            Interlocked.Increment(ref builds);
            await release.Task.WaitAsync(TimeSpan.FromSeconds(10), token);

            return "v2";
        }

        Task<string>[] callers = Enumerable.Range(0, 8).Select(_ => scope.Cache.GetOrCreateStringAsync(key, Build, Swr(60_000))).ToArray();

        // Everyone but the refresher answers with the stale value while the factory is still running.
        for (int attempt = 0; attempt < 1000 && callers.Count(c => c.IsCompleted) < 7; attempt++)
            await Task.Delay(10);

        Assert.Equal(7, callers.Count(c => c.IsCompleted));
        release.SetResult();
        string[] all = await Task.WhenAll(callers).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(1, builds);
        Assert.Equal(7, all.Count(value => value == "v1"));
        Assert.Equal(1, all.Count(value => value == "v2"));
        Assert.Equal("v2", await scope.Cache.GetStringAsync(key));
        Assert.Equal("v2", await scope.Cache.GetOrCreateStringAsync(key, Build, Swr(60_000)));
        Assert.Equal(1, builds);
    }

    [LiveFact]
    public async Task A_failing_factory_gets_the_stale_value()
    {
        await using var scope = await Scope.Open();
        string key = "fail-safe";

        scope.Key(key);
        await scope.Cache.GetOrCreateStringAsync(key, _ => Task.FromResult("old"), Swr(200));
        await WaitUntilStale(scope, key);

        Assert.Equal("old", await scope.Cache.GetOrCreateStringAsync(key, _ => throw new InvalidOperationException("source down"), Swr(60_000)));

        // Without a stale value the error reaches the caller as before.
        scope.Key("none");
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            scope.Cache.GetOrCreateStringAsync("none", _ => throw new InvalidOperationException("down"), Swr(60_000)));
    }

    [LiveFact]
    public async Task Removing_a_tag_hides_every_entry_that_carries_it()
    {
        await using var scope = await Scope.Open();
        RuvioCacheEntryOptions Tagged(params string[] tags) => new() { AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(1), Tags = tags };

        foreach (string name in new[] { "a", "b", "c" })
            scope.Key(name);

        foreach (string tag in new[] { "product:1", "category:9" })
            scope.TagKey(tag);

        await scope.Cache.SetStringAsync("a", "A", Tagged("product:1", "category:9"));
        await scope.Cache.SetStringAsync("b", "B", Tagged("category:9"));
        await scope.Cache.SetStringAsync("c", "C", new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(1) });

        Assert.Equal("A", await scope.Cache.GetStringAsync("a"));
        Assert.Equal("B", await scope.Cache.GetStringAsync("b"));
        Assert.InRange((await scope.Client.ExecuteAsync("PTTL", scope.TagKey("category:9"))).Integer, 50_000, 60_000);
        // An entry without tags is still the format older versions read.
        Assert.StartsWith("RDC1\n", Encoding.ASCII.GetString((await scope.Client.GetAsync(scope.Key("c")))!));

        await ((IDistributedCache)scope.Cache).RemoveByTagAsync("category:9");

        Assert.Null(await scope.Cache.GetStringAsync("a"));
        Assert.Null(await scope.Cache.GetStringAsync("b"));
        Assert.Equal("C", await scope.Cache.GetStringAsync("c"));

        // Writing under the tag again starts a new generation, which the new entry matches.
        await scope.Cache.SetStringAsync("b", "B2", Tagged("category:9"));
        Assert.Equal("B2", await scope.Cache.GetStringAsync("b"));
        Assert.Null(await scope.Cache.GetStringAsync("a"));
    }
}
