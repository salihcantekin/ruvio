using System.Text;
using System.Text.Json;
using Ruvio.Client;
using Xunit;
using Xunit.Abstractions;

namespace Ruvio.Client.Tests;

/// <summary>
/// Runs every command family against a real server (<c>RUVIO_TEST_ADDR</c>) through a recording proxy, then checks
/// that every command in docs/compatibility-matrix.json was really sent by its typed method. Uses databases 13 and 14
/// (FLUSHDB), database 0 only for the JSON.INDEX keys it creates and deletes, and ends with FLUSHALL.
/// </summary>
[Collection("Live")]
public sealed class LiveCommandTests(ITestOutputHelper output)
{
    private const int Db = 13;
    private const int Other = 14;

    [LiveFact(Timeout = 120000)]
    public async Task Every_matrix_command_runs_against_a_live_server()
    {
        await using var proxy = new RecordingProxy();
        await using RuvioConnection connection = await LiveServer.ConnectAsync(Db, proxy);
        RuvioClient db = await connection.GetDefaultDatabaseAsync();

        await Strings(db);
        await Ruvio(db);
        await Keys(connection, db);
        await Lists(db);
        await Sets(db);
        await Hashes(db);
        await SortedSets(db);
        await Geo(db);
        await Bits(db);
        await Bloom(db);
        await Json(connection, db);
        await Streams(db);
        await Transactions(db);
        await Scripts(db);
        await PubSub(db);
        await Admin(connection, db, proxy);

        using JsonDocument matrix = JsonDocument.Parse(File.ReadAllText(MatrixCoverageTests.FindMatrix()));
        List<string> commands = matrix.RootElement.GetProperty("commands").EnumerateArray()
            .Select(command => command.GetProperty("name").GetString()!)
            .ToList();
        List<string> unsent = commands
            .Where(command => !proxy.Seen.ContainsKey(command))
            .Where(command => RewritesConfig || command != "CONFIG REWRITE")
            .ToList();

        output.WriteLine($"live: {commands.Count - unsent.Count}/{commands.Count} matrix commands sent and answered");
        Assert.True(unsent.Count == 0, "Matrix commands never sent live: " + string.Join(", ", unsent));
    }

    /// <summary><c>scripts/run_with_servers.sh</c> runs the servers on throwaway config files.</summary>
    private static bool RewritesConfig => Environment.GetEnvironmentVariable("RUVIO_TEST_CONFIG_REWRITE") == "1";

    private static async Task Strings(RuvioClient db)
    {
        Assert.True(await db.SetAsync("s", "v"));
        Assert.Equal("v", await db.GetStringAsync("s"));
        Assert.Equal("v", await db.GetSetAsync("s", "w"));
        Assert.Equal(3, await db.AppendAsync("s", "xy"));
        Assert.Equal(3, await db.GetStringLengthAsync("s"));
        Assert.Equal(1, await db.IncrementAsync("n"));
        Assert.Equal(0, await db.DecrementAsync("n"));
        Assert.Equal(5, await db.IncrementByAsync("n", 5));
        Assert.Equal(3, await db.DecrementByAsync("n", 2));
        Assert.Null(await db.IncrementWithLimitAsync("n", 10, 5));
        Assert.Equal(5, await db.IncrementWithLimitAsync("n", 2, 5));
        await db.SetManyAsync(("m1", "a"), ("m2", "b"));
        Assert.Equal(new string?[] { "a", "b", null }, await db.GetManyAsync("m1", "m2", "m3"));
        Assert.False(await db.SetManyIfNotExistsAsync(("m1", "x"), ("m9", "y")));
        Assert.True(await db.SetManyIfNotExistsAsync(("m8", "x"), ("m9", "y")));

        // SET ... IFEQ and DELEX: compare-and-set and compare-and-delete.
        Assert.False(await db.SetIfEqualAsync("cas", "1", "0"));
        Assert.True(await db.SetAsync("cas", "1"));
        Assert.True(await db.SetIfEqualAsync("cas", "2", "1", TimeSpan.FromSeconds(30)));
        Assert.InRange(await db.GetTimeToLiveMillisecondsAsync("cas"), 1, 30000);
        Assert.True(await db.SetIfEqualKeepingTimeToLiveAsync("cas", "3", "2"));
        Assert.InRange(await db.GetTimeToLiveMillisecondsAsync("cas"), 1, 30000);
        Assert.True(await db.SetIfGreaterAsync("wm", 10));
        Assert.False(await db.SetIfGreaterAsync("wm", 10));
        Assert.True(await db.SetIfGreaterAsync("wm", 11, TimeSpan.FromSeconds(30)));
        Assert.False(await db.SetIfLessAsync("wm", 12));
        Assert.True(await db.SetIfLessAsync("wm", -1));
        Assert.False(await db.DeleteIfEqualAsync("cas", "2"));
        Assert.True(await db.DeleteIfEqualAsync("cas", "3"));
        Assert.False(await db.DeleteIfEqualAsync("cas", "3"));

        await db.SetExpireAsync("ex", 10, "v");
        Assert.InRange(await db.GetTimeToLiveAsync("ex"), 1, 10);
        await db.SetExpireMillisecondsAsync("pex", 5000, "v");
        Assert.InRange(await db.GetTimeToLiveMillisecondsAsync("pex"), 1, 5000);
        Assert.True(await db.SetIfNotExistsAsync("nx", "1"));
        Assert.False(await db.SetIfNotExistsAsync("nx", "2"));
        Assert.Equal("1", await db.GetDeleteAsync("nx"));
        Assert.Null(await db.GetDeleteAsync("nx"));
        Assert.Equal("v", await db.GetExpireAsync("ex", 60000));
        Assert.Equal(1.5, await db.IncrementByFloatAsync("f", 1.5));
        Assert.Equal(1.0, await db.IncrementByFloatAsync("f", -0.5));
        Assert.True(await db.SetAsync("r", "foobar"));
        Assert.Equal("foo", await db.GetRangeAsync("r", 0, 2));
        Assert.Equal("bar", await db.GetSubstringAsync("r", -3, -1));
        Assert.Equal(6, await db.SetRangeAsync("r", 1, "OO"));
        Assert.Equal("fOObar", await db.GetStringAsync("r"));
        Assert.Equal("fOObar", await db.SetAndGetAsync("r", "next"));
        Assert.True(await db.SetKeepingTimeToLiveAsync("ex", "kept"));
        Assert.True(await db.SetAsync("at", "v", DateTimeOffset.UtcNow.AddMinutes(1)));
    }

    private static async Task Ruvio(RuvioClient db)
    {
        Assert.Equal(1, await db.SetVersionAsync("ver", "a"));
        Assert.Equal(2, await db.SetVersionAsync("ver", "b", 1));
        Assert.True(await db.SetAsync("stock", "5"));
        Assert.Equal(2, await db.TakeAsync("stock", 3));
        Assert.Null(await db.TakeAsync("stock", 3));

        Assert.True(await db.SetAsync("{w}a", "10"));
        Assert.Equal((6L, 4L), await db.TransferAsync("{w}a", "{w}b", 4));
        Assert.Null(await db.TransferAsync("{w}a", "{w}b", 7));
        Assert.Null(await db.TransferAsync("{w}a", "{w}b", 3, 6));
        Assert.Equal((4L, 6L), await db.TransferAsync("{w}a", "{w}b", 2, 6));
        Assert.Equal(1, await db.SetHashFieldsAsync("th", ("f", "10")));
        Assert.Equal((6L, 4L), await db.TransferHashFieldAsync("th", "f", "g", 4));
        Assert.Null(await db.TransferHashFieldAsync("th", "f", "g", 3, 5));

        Assert.Equal(1, await db.OnceAsync("once", 10000));
        Assert.Equal(0, await db.OnceAsync("once", 10000));
        Assert.Equal(1, await db.LimitAsync("lim", 2, 10000));
        Assert.Equal(0, await db.LimitAsync("lim", 2, 10000));
        Assert.Null(await db.LimitAsync("lim", 2, 10000));
        long lease = (await db.LeaseAsync("lease", 10000))!.Value;
        Assert.Null(await db.LeaseAsync("lease", 10000));
        Assert.Equal(0, await db.RenewAsync("lease", lease + 100, 20000));
        Assert.Equal(1, await db.RenewAsync("lease", lease, 20000));
        Assert.Equal(1, await db.ReleaseAsync("lease", lease));
        Assert.Equal(0, await db.RenewAsync("lease", lease, 20000));
        Assert.True(await db.LeaseAsync("lease", 10000) > lease);
        long slot = (await db.AcquireSemaphoreAsync("sem", 1, 10000))!.Value;
        Assert.Null(await db.AcquireSemaphoreAsync("sem", 1, 10000));
        Assert.Equal(1, await db.RenewAsync("sem", slot, 20000));
        Assert.Null(await db.AcquireSemaphoreAsync("sem", 1, 10000));
        Assert.Equal(1, await db.ReleaseAsync("sem", slot));

        RespValue begin = await db.BeginIdempotencyAsync("idem", "fp", "me", 10000);
        Assert.Equal("acquired", begin.Items![0].AsString());
        Assert.True(await db.CompleteIdempotencyAsync("idem", "fp", "me", "done"));
        IdempotencyBegin replay = await db.TryBeginIdempotencyAsync("idem", "fp", "you", TimeSpan.FromSeconds(10));
        Assert.Equal(IdempotencyState.Completed, replay.State);
        Assert.Equal("done"u8.ToArray(), replay.Result);
        Assert.Equal(IdempotencyState.Conflict, (await db.TryBeginIdempotencyAsync("idem", "other", "you", TimeSpan.FromSeconds(10))).State);
        Assert.False(await db.AbortIdempotencyAsync("idem", "fp", "me"));
        Assert.Equal(IdempotencyState.Acquired, (await db.TryBeginIdempotencyAsync("idem-abort", "fp", "me", TimeSpan.FromSeconds(10))).State);
        Assert.False(await db.AbortIdempotencyAsync("idem-abort", "fp", "you"));
        Assert.True(await db.AbortIdempotencyAsync("idem-abort", "fp", "me"));
        Assert.Equal(IdempotencyState.Acquired, (await db.TryBeginIdempotencyAsync("idem-abort", "fp", "retry", TimeSpan.FromSeconds(10))).State);

        long cursor = await db.StartChangesAsync();
        Assert.True(await db.SetAsync("changed", "1"));
        Assert.Contains(await db.ReadChangesAsync(cursor), note => note.Text.Contains("changed", StringComparison.Ordinal));

        FillResult fill = await db.FillGetAsync("fill", TimeSpan.FromSeconds(5));
        Assert.Equal(FillState.Fill, fill.State);
        Assert.Equal(FillState.Busy, (await db.FillGetAsync("fill", TimeSpan.FromSeconds(5))).State);
        Assert.True(await db.FillSetAsync("fill", "built", TimeSpan.FromSeconds(10), fill.Token!.Value, TimeSpan.FromSeconds(5)));
        FillResult hit = await db.FillGetAsync("fill", TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(10));
        Assert.Equal(FillState.Hit, hit.State);
        Assert.Equal("built", Encoding.UTF8.GetString(hit.Value!));
    }

    private static async Task Keys(RuvioConnection connection, RuvioClient db)
    {
        Assert.True(await db.SetAsync("k1", "v"));
        Assert.True(await db.SetAsync("k2", "v"));
        Assert.Equal(2, await db.ExistsAsync("k1", "k2"));
        Assert.Equal("string", await db.GetKeyTypeAsync("k1"));
        await db.RenameAsync("k2", "k3");
        Assert.False(await db.RenameIfNotExistsAsync("k3", "k1"));
        Assert.True(await db.ExpireAsync("k1", TimeSpan.FromSeconds(100)));
        Assert.True(await db.ExpireAsync("k1", TimeSpan.FromMilliseconds(100500)));
        Assert.True(await db.ExpireAtAsync("k1", DateTimeOffset.FromUnixTimeSeconds(DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 100)));
        // A whole second goes out as EXPIREAT, so the milliseconds part must not be zero.
        Assert.True(await db.ExpireAtAsync("k1", DateTimeOffset.FromUnixTimeMilliseconds((DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 100) * 1000 + 250)));
        Assert.True(await db.GetExpireTimeAsync("k1") > 0);
        Assert.True(await db.GetExpireTimeMillisecondsAsync("k1") > 0);
        Assert.True(await db.PersistAsync("k1"));
        Assert.Equal(-1, await db.GetTimeToLiveAsync("k1"));
        Assert.Equal(2, await db.TouchAsync("k1", "k3"));
        Assert.True(await db.CopyAsync("k1", "k4"));
        Assert.Contains("k4", await db.GetKeysAsync("k*"));
        Assert.NotNull(await db.GetRandomKeyAsync());
        Assert.True(await db.GetDatabaseSizeAsync() > 3);
        ScanPage page = await db.ScanAsync(0, "k*", 100);
        Assert.Contains("k1", page.Keys);
        Assert.Contains("k1", (await db.ScanAsync(0, "k*", 100, "string")).Keys);
        var walked = new List<string>();

        await foreach (string key in db.ScanKeysAsync("k*", 1))
        {
            walked.Add(key);
        }

        Assert.Equal(["k1", "k3", "k4"], walked.Order().ToArray());
        Assert.Equal(1, await db.UnlinkAsync("k4"));
        Assert.Equal(1, await db.DeleteAsync("k3"));
        Assert.Equal("embstr", await db.GetObjectEncodingAsync("k1"));
        await ServerAnswers(() => db.GetObjectIdleSecondsAsync("k1").AsTask()); // needs an LRU maxmemory-policy
        Assert.NotNull(await db.GetMemoryUsageAsync("k1"));
        byte[] dump = (await db.DumpAsync("k1"))!;
        await db.RestoreAsync("k1", dump, replace: true);
        Assert.Equal("v", await db.GetStringAsync("k1"));

        // MOVE and SWAPDB use database 14.
        RuvioClient other = await connection.GetDatabaseAsync(Other);
        await other.FlushDatabaseAsync();
        Assert.True(await db.MoveKeyAsync("k1", Other));
        Assert.Equal("v", await other.GetStringAsync("k1"));
        await db.SwapDatabasesAsync(Db, Other);
        Assert.Equal("v", await db.GetStringAsync("k1"));
        await db.SwapDatabasesAsync(Db, Other);
    }

    private static async Task Lists(RuvioClient db)
    {
        Assert.Equal(2, await db.PushListLeftAsync("l", "b", "a"));
        Assert.Equal(3, await db.PushListRightAsync("l", "c"));
        Assert.Equal(3, await db.GetListLengthAsync("l"));
        Assert.Equal("a", await db.GetListItemAsync("l", 0));
        Assert.Equal(["a", "b", "c"], await db.GetListRangeAsync("l", 0, -1));
        Assert.Equal(4, await db.InsertListAsync("l", before: true, "c", "bb"));
        await db.SetListItemAsync("l", 0, "A");
        Assert.Equal(1, await db.RemoveListItemsAsync("l", 0, "bb"));
        Assert.Equal(1, await db.GetListPositionAsync("l", "b"));
        Assert.Equal(4, await db.PushListLeftIfExistsAsync("l", "z"));
        Assert.Equal(0, await db.PushListRightIfExistsAsync("nolist", "z"));
        Assert.Equal("z", await db.PopListLeftAsync("l"));
        Assert.Equal("c", await db.PopListRightAsync("l"));
        Assert.Equal(["A"], await db.PopListLeftAsync("l", 1));
        Assert.Equal(["b"], await db.PopListRightAsync("l", 1));
        Assert.Equal(2, await db.PushListRightAsync("l", "1", "2"));
        await db.TrimListAsync("l", 0, 0);
        Assert.Equal("1", (await db.PopListLeftBlockingAsync("l", TimeSpan.FromSeconds(1)))!.Value.Value);
        Assert.Equal(1, await db.PushListRightAsync("l", "x"));
        Assert.Equal("x", (await db.PopListRightBlockingAsync("l", TimeSpan.FromSeconds(1)))!.Value.Value);
        Assert.Equal(2, await db.PushListRightAsync("src", "a", "b"));
        Assert.Equal("a", await db.MoveListItemAsync("src", "dst", fromLeft: true, toLeft: true));
        Assert.Equal("b", await db.PopListRightPushLeftAsync("src", "dst"));
        Assert.Equal("b", await db.MoveListItemBlockingAsync("dst", "src", true, false, TimeSpan.FromSeconds(1)));
        Assert.Equal("src", (await db.PopListManyAsync(["none", "src"], fromLeft: true))!.Value.Key);
        Assert.Equal(["a"], (await db.PopListManyBlockingAsync(["none", "dst"], true, TimeSpan.FromSeconds(1), 5))!.Value.Value);

        // LPUSHCAP / RPUSHCAP keep a bounded list.
        Assert.Equal(2, await db.PushListLeftCappedAsync("cap", 2, "a", "b", "c"));
        Assert.Equal(["c", "b"], await db.GetListRangeAsync("cap", 0, -1));
        Assert.Equal(2, await db.PushListRightCappedAsync("cap", 2, "d"));
        Assert.Equal(["b", "d"], await db.GetListRangeAsync("cap", 0, -1));
        Assert.Equal(1, await db.PushRecentAsync("recent", "1", 3));
        Assert.Equal(2, await db.PushRecentAsync("recent", "2", 3));
        Assert.Equal(3, await db.PushRecentAsync("recent", "3", 3));
        Assert.Equal(3, await db.PushRecentAsync("recent", "4", 3));
        Assert.Equal(["4", "3"], await db.GetRecentAsync("recent", 2));
        Assert.Equal(3, await db.GetListLengthAsync("recent"));
    }

    private static async Task Sets(RuvioClient db)
    {
        Assert.Equal(3, await db.AddSetMembersAsync("s1", "a", "b", "c"));
        Assert.Equal(2, await db.AddSetMembersAsync("s2", "b", "c"));
        Assert.Equal(1, await db.RemoveSetMembersAsync("s1", "c"));
        Assert.True(await db.SetContainsAsync("s1", "a"));
        Assert.Equal([true, false], await db.SetContainsManyAsync("s1", "a", "z"));
        Assert.Equal(2, await db.GetSetCountAsync("s1"));
        Assert.Equal(["a", "b"], (await db.GetSetMembersAsync("s1")).Order());
        Assert.Equal(["b"], await db.GetSetIntersectionAsync("s1", "s2"));
        Assert.Equal(["a", "b", "c"], (await db.GetSetUnionAsync("s1", "s2")).Order());
        Assert.Equal(["a"], await db.GetSetDifferenceAsync("s1", "s2"));
        Assert.Equal(1, await db.StoreSetIntersectionAsync("si", "s1", "s2"));
        Assert.Equal(3, await db.StoreSetUnionAsync("su", "s1", "s2"));
        Assert.Equal(1, await db.StoreSetDifferenceAsync("sd", "s1", "s2"));
        Assert.Equal(1, await db.GetSetIntersectionCountAsync(["s1", "s2"]));
        Assert.Equal(1, await db.GetSetIntersectionCountAsync(["su", "s1"], 1));
        Assert.True(await db.MoveSetMemberAsync("su", "s3", "c"));
        Assert.NotNull(await db.GetSetRandomMemberAsync("su"));
        Assert.Equal(2, (await db.GetSetRandomMembersAsync("su", 2)).Count);
        Assert.NotNull(await db.PopSetMemberAsync("su"));
        Assert.Single(await db.PopSetMembersAsync("su", 1));
        Assert.Equal(15, await db.AddSetMembersAsync("big", Enumerable.Range(0, 15).Select(index => "m" + index).ToArray()));
        SetScanPage page = await db.ScanSetAsync("big", 0, "m1*", 100);
        Assert.Equal(6, page.Members.Count);
        var members = new List<string>();

        await foreach (string member in db.ScanSetMembersAsync("big", count: 4))
        {
            members.Add(member);
        }

        Assert.Equal(15, members.Distinct().Count());
    }

    private static async Task Hashes(RuvioClient db)
    {
        Assert.Equal(2, await db.SetHashFieldsAsync("h", ("f", "1"), ("g", "2")));
        Assert.Equal("1", await db.GetHashFieldAsync("h", "f"));
        Assert.Equal(new string?[] { "1", null }, await db.GetHashFieldsAsync("h", "f", "zz"));
        Assert.Equal(2, await db.GetHashLengthAsync("h"));
        Assert.Equal("2", (await db.GetHashAllAsync("h"))["g"]);
        Assert.True(await db.HashFieldExistsAsync("h", "g"));
        Assert.Equal(["f", "g"], await db.GetHashFieldNamesAsync("h"));
        Assert.Equal(["1", "2"], await db.GetHashFieldValuesAsync("h"));
        Assert.Equal(3, await db.IncrementHashFieldAsync("h", "f", 2));
        Assert.Equal(3.5, await db.IncrementHashFieldByFloatAsync("h", "f", 0.5));
        Assert.False(await db.SetHashFieldIfAbsentAsync("h", "f", "x"));
        await db.SetHashManyAsync("hm", ("a", "1"), ("b", "2"));
        Assert.Equal(new string?[] { "1", "2" }, await db.GetHashFieldsAsync("hm", "a", "b"));
        Assert.Equal(3, await db.GetHashFieldLengthAsync("h", "f"));
        Assert.Contains(await db.GetHashRandomFieldAsync("h"), new[] { "f", "g" });
        Assert.Equal(2, (await db.GetHashRandomFieldsAsync("h", 2)).Count);
        Assert.Contains(new KeyValuePair<string, string>("g", "2"), await db.GetHashRandomFieldsWithValuesAsync("h", 2));
        Assert.Single((await db.ScanHashAsync("h", 0)).Fields, field => field.Key == "g");
        Assert.Single((await db.ScanHashAsync("h", 0, "g*")).Fields);
        var fields = new List<KeyValuePair<string, string>>();

        await foreach (KeyValuePair<string, string> field in db.ScanHashEntriesAsync("h", count: 1))
        {
            fields.Add(field);
        }

        Assert.Equal(2, fields.Count);
        Assert.Equal([1L], await db.ExpireHashFieldsAsync("h", TimeSpan.FromSeconds(100), "g"));
        Assert.Equal([1L], await db.ExpireHashFieldsAsync("h", TimeSpan.FromMilliseconds(100500), FieldExpireCondition.Always, "g"));
        Assert.Equal([1L], await db.ExpireHashFieldsAtAsync("h", DateTimeOffset.FromUnixTimeSeconds(DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 100), "g"));
        Assert.Equal([1L], await db.ExpireHashFieldsAtAsync("h", DateTimeOffset.FromUnixTimeMilliseconds((DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 100) * 1000 + 250), "g"));
        Assert.InRange((await db.GetHashFieldTimeToLiveAsync("h", "g"))[0], 1, 101);
        Assert.InRange((await db.GetHashFieldTimeToLiveMillisecondsAsync("h", "g"))[0], 1, 101000);
        Assert.True((await db.GetHashFieldExpireTimeAsync("h", "g"))[0] > 0);
        Assert.True((await db.GetHashFieldExpireTimeMillisecondsAsync("h", "g"))[0] > 0);
        Assert.Equal([1L], await db.PersistHashFieldsAsync("h", "g"));
        Assert.Equal(1, await db.DeleteHashFieldsAsync("h", "g"));
    }

    private static async Task SortedSets(RuvioClient db)
    {
        Assert.Equal(3, await db.AddSortedSetAsync("z", ("a", 1), ("b", 2), ("c", 3)));
        Assert.Equal(2, await db.AddSortedSetAsync("z2", ("b", 3), ("d", 4)));
        Assert.Equal(["a", "b"], await db.GetSortedSetRangeByRankAsync("z", 0, 1));
        Assert.Equal(["c", "b"], await db.GetSortedSetRangeByRankAsync("z", 0, 1, descending: true));
        Assert.Equal(new SortedSetEntry("a", 1), (await db.GetSortedSetRangeByRankWithScoresAsync("z", 0, 0))[0]);
        Assert.Equal(2, (await db.GetSortedSetRangeByScoreWithScoresAsync("z", "1", "(3")).Count);
        // A one-point range: the prebuilt test binary predates the server fix that reads REV bounds high-first
        // (src/protocol/parse/zset.rs); the scripted test pins the Redis order the client sends.
        Assert.Equal(["b"], await db.GetSortedSetRangeByScoreAsync("z", "2", "2", descending: true));
        Assert.Equal(["b"], await db.GetSortedSetRangeByScoreAsync("z", "-inf", "+inf", false, 1, 1));
        Assert.Equal(["a", "b", "c"], await db.GetSortedSetRangeByLexAsync("z", "-", "+"));
        Assert.Equal(["a", "b"], await db.GetSortedSetRangeByLexAsync("z", "-", "+", false, 0, 2));
        Assert.Equal(["c", "b"], await db.GetSortedSetRangeByLexAsync("z", "[b", "+", descending: true));
        Assert.Equal(2, await db.CountSortedSetByScoreAsync("z", "-inf", "(3"));
        Assert.Equal(2, await db.CountSortedSetByLexAsync("z", "[b", "+"));
        Assert.Equal(new double?[] { 1, null }, await db.GetSortedSetScoresAsync("z", "a", "zz"));
        Assert.Contains(await db.GetSortedSetRandomMemberAsync("z"), new[] { "a", "b", "c" });
        Assert.Equal(2, (await db.GetSortedSetRandomMembersAsync("z", 2)).Count);
        Assert.Equal(3, (await db.GetSortedSetRandomMembersWithScoresAsync("z", 3)).Count);
        Assert.Equal(3, (await db.ScanSortedSetAsync("z", 0)).Entries.Count);
        var entries = new List<SortedSetEntry>();

        await foreach (SortedSetEntry entry in db.ScanSortedSetEntriesAsync("z", count: 1))
        {
            entries.Add(entry);
        }

        Assert.Equal(3, entries.Count);
        Assert.Equal([new SortedSetEntry("a", 1), new SortedSetEntry("c", 3), new SortedSetEntry("b", 6), new SortedSetEntry("d", 8)],
            await db.GetSortedSetUnionAsync(["z", "z2"], [1, 2], SortedSetAggregate.Max));
        Assert.Equal([new SortedSetEntry("b", 2)], await db.GetSortedSetIntersectionAsync(["z", "z2"], aggregate: SortedSetAggregate.Min));
        Assert.Equal(2, (await db.GetSortedSetDifferenceAsync(["z", "z2"])).Count);
        Assert.Equal(4, await db.StoreSortedSetUnionAsync("zu", ["z", "z2"]));
        Assert.Equal(1, await db.StoreSortedSetIntersectionAsync("zi", ["z", "z2"], [1, 1], SortedSetAggregate.Sum));
        Assert.Equal(2, await db.StoreSortedSetDifferenceAsync("zd", ["z", "z2"]));
        Assert.Equal(5.0, (await db.GetSortedSetScoreAsync("zi", "b")));
        Assert.Equal(2.5, await db.IncrementSortedSetAsync("z", "a", 1.5));
        Assert.Equal(3.5, await db.IncrementSortedSetAsync("z", "a", 1, SortedSetAddFlags.IfExists));
        Assert.Equal(2, await db.GetSortedSetRankAsync("z", "a"));
        Assert.Equal(0, await db.GetSortedSetRankAsync("z", "a", descending: true));
        Assert.Equal(3, await db.GetSortedSetCountAsync("z"));
        Assert.Equal(1, await db.RemoveSortedSetMembersAsync("z", "c"));
        Assert.Equal("b", (await db.PopSortedSetMinAsync("z"))[0].Member);
        Assert.Equal("a", (await db.PopSortedSetMaxAsync("z"))[0].Member);
        Assert.Equal(4, await db.AddSortedSetAsync("due", ("a", 1), ("b", 2), ("c", 3), ("d", 9)));
        Assert.Equal("a", (await db.PopSortedSetByScoreAsync("due", "-inf", "1", 5))[0].Member);
        Assert.Equal(2, await db.MoveDueSortedSetAsync("due", "ready", "3", 5));
        Assert.Equal(["b", "c"], await db.GetListRangeAsync("ready", 0, -1));
        Assert.Empty(await db.PopSortedSetByScoreAsync("due", "-inf", "3", 5));
        Assert.True(await db.AddSortedSetAsync("bz", "m", 1));
        Assert.Equal("m", (await db.PopSortedSetMinBlockingAsync("bz", TimeSpan.FromSeconds(1)))!.Value.Entry.Member);
        Assert.True(await db.AddSortedSetAsync("bz", "n", 1));
        Assert.Equal("n", (await db.PopSortedSetMaxBlockingAsync("bz", TimeSpan.FromSeconds(1)))!.Value.Entry.Member);
        Assert.Equal(4, await db.AddSortedSetAsync("zr", ("a", 1), ("b", 2), ("c", 3), ("d", 4)));
        Assert.Equal(1, await db.RemoveSortedSetRangeByRankAsync("zr", 0, 0));
        Assert.Equal(1, await db.RemoveSortedSetRangeByScoreAsync("zr", "2", "2"));
        Assert.Equal(1, await db.RemoveSortedSetRangeByLexAsync("zr", "[c", "[c"));
    }

    private static async Task Geo(RuvioClient db)
    {
        Assert.Equal(2, await db.AddGeoAsync("geo", (13.361389, 38.115556, "Palermo"), (15.087269, 37.502669, "Catania")));
        Assert.InRange((await db.GeoDistanceAsync("geo", "Palermo", "Catania", "km"))!.Value, 160, 170);
        Assert.Equal(11, (await db.GeoHashAsync("geo", "Palermo"))[0]!.Length);
        Assert.NotNull((await db.GeoPositionAsync("geo", "Palermo"))[0]);
        Assert.Equal(2, (await db.GeoSearchAsync("geo", 15, 37, 200, "km")).Count);
        Assert.Equal(2, await db.GeoSearchStoreAsync("geo2", "geo", "Palermo", 200, "km"));
        Assert.Equal(2, (await db.GeoRadiusAsync("geo", 15, 37, 200, "km")).Count);
        Assert.Equal(2, (await db.GeoRadiusByMemberAsync("geo", "Palermo", 200, "km")).Count);
    }

    private static async Task Bits(RuvioClient db)
    {
        Assert.False(await db.SetBitAsync("bits", 7, true));
        Assert.True(await db.GetBitAsync("bits", 7));
        Assert.True(await db.SetAsync("bs", "foobar"));
        Assert.Equal(26, await db.BitCountAsync("bs"));
        Assert.Equal(6, await db.BitCountAsync("bs", 1, 1));
        Assert.Equal(4, await db.BitCountAsync("bs", 0, 7, BitUnit.Bit));
        Assert.Equal(1, await db.BitPositionAsync("bs", true));
        Assert.Equal(8, await db.BitPositionAsync("bs", false, 1, 2));
        Assert.Equal(6, await db.BitOperationAsync(BitOperation.And, "band", ["bs", "bits"]));
        Assert.Equal(1, await db.BitOperationAsync(BitOperation.Not, "bnot", ["bits"]));
        Assert.Equal(new long?[] { 0, 100 }, await db.BitFieldAsync("bf8", "SET", "u8", "0", "100", "GET", "u8", "0"));
        Assert.Equal(new long?[] { 100 }, await db.BitFieldReadOnlyAsync("bf8", "GET", "u8", "0"));
        Assert.True(await db.HyperLogLogAddAsync("hll", "a", "b", "c"));
        Assert.False(await db.HyperLogLogAddAsync("hll", "a"));
        Assert.True(await db.HyperLogLogAddAsync("hll2", "d"));
        Assert.Equal(4, await db.HyperLogLogCountAsync("hll", "hll2"));
        await db.HyperLogLogMergeAsync("hll3", "hll", "hll2");
        Assert.Equal(4, await db.HyperLogLogCountAsync("hll3"));
    }

    private static async Task Bloom(RuvioClient db)
    {
        await db.ReserveBloomFilterAsync("bf", 0.01, 100);
        await db.ReserveBloomFilterAsync("bfx", 0.01, 100, 4);
        await db.ReserveBloomFilterAsync("bfn", 0.01, 100, null, nonScaling: true);
        Assert.True(await db.AddBloomFilterAsync("bf", "a"));
        Assert.True(await db.BloomFilterContainsAsync("bf", "a"));
        Assert.Equal([true, false], await db.AddBloomFilterManyAsync("bf", "b", "a"));
        Assert.Equal([true, false], await db.BloomFilterContainsManyAsync("bf", "b", "zz"));
        Assert.Equal([true, true], await db.InsertBloomFilterAsync("bfi", ["x", "y"], new BloomInsertOptions { Capacity = 50, ErrorRate = 0.001 }));
        RuvioException missing = await Assert.ThrowsAsync<RuvioException>(async () =>
            await db.InsertBloomFilterAsync("nofilter", ["x"], new BloomInsertOptions { NoCreate = true }));
        Assert.Contains("not found", missing.Message, StringComparison.Ordinal);
        BloomFilterInfo info = await db.GetBloomFilterInfoAsync("bfx");
        Assert.Equal(new BloomFilterInfo(100, info.Size, 1, 0, 4), info);
        Assert.Null((await db.GetBloomFilterInfoAsync("bfn")).ExpansionRate);
        Assert.Equal(2, await db.GetBloomFilterCardinalityAsync("bf"));
        Assert.Equal(0, await db.GetBloomFilterCardinalityAsync("nofilter"));
        Assert.Equal([false, true], await db.SeenBeforeAsync("seen", "new", "new"));
        Assert.Equal([true, false], await db.MaybeSeenAsync("seen", "new", "other"));
    }

    private static async Task Json(RuvioConnection connection, RuvioClient db)
    {
        Assert.True(await db.JsonSetAsync("doc", "$", """{"n":3,"s":"ab","a":[1,2,3],"o":{"x":1},"t":true}"""));
        Assert.Equal(3.0, JsonDocument.Parse((await db.JsonGetAsync("doc", "$.n"))!).RootElement.GetDouble());
        Assert.Equal(new string?[] { "3", null }, await db.JsonGetManyAsync("$.n", "doc", "nodoc"));
        Assert.False(await db.JsonSetAsync("doc", "$.n", "9", SetMode.IfNotExists));
        Assert.True(await db.JsonSetAsync("doc", "$.n", "4", SetMode.IfExists));
        Assert.True(await db.JsonMergeAsync("doc", "$", """{"m":1}"""));
        Assert.True(await db.JMergeAsync("doc", "$", """{"m":2}"""));
        Assert.Equal("integer", await db.JsonTypeAsync("doc", "$.n"));
        Assert.Equal(2, await db.JsonStringLengthAsync("doc", "$.s"));
        Assert.Equal(3, await db.JsonArrayLengthAsync("doc", "$.a"));
        Assert.Equal(["x"], await db.JsonObjectKeysAsync("doc", "$.o"));
        Assert.Equal(1, await db.JsonObjectLengthAsync("doc", "$.o"));
        Assert.Null(await db.JsonObjectLengthAsync("doc", "$.zz"));
        Assert.Equal("6", await db.JsonIncrementAsync("doc", "$.n", 2));
        Assert.Equal("15.0", await db.JsonMultiplyAsync("doc", "$.n", 2.5));
        Assert.False(await db.JsonToggleAsync("doc", "$.t"));
        Assert.Equal(4, await db.JsonStringAppendAsync("doc", "$.s", "\"cd\""));
        Assert.Equal(4, await db.JsonArrayAppendAsync("doc", "$.a", "4"));
        Assert.Equal(6, await db.JsonArrayInsertAsync("doc", "$.a", 0, "0", "\"q\""));
        Assert.Equal(1, await db.JsonArrayIndexAsync("doc", "$.a", "\"q\""));
        Assert.Equal(-1, await db.JsonArrayIndexAsync("doc", "$.a", "\"q\"", 2, 0));
        Assert.Equal("4", await db.JsonArrayPopAsync("doc", "$.a"));
        Assert.Equal("0", await db.JsonArrayPopAsync("doc", "$.a", 0));
        Assert.Equal(2, await db.JsonArrayTrimAsync("doc", "$.a", 1, 2));
        Assert.Equal(1, await db.JsonClearAsync("doc", "$.o"));
        Assert.Equal(1, await db.JsonForgetAsync("doc", "$.o"));
        Assert.Equal(1, await db.JsonDeleteAsync("doc", "$.m"));
        Assert.Equal(15.0, (await db.JsonGetValueAsync<Dictionary<string, JsonElement>>("doc"))!["n"].GetDouble());

        // JSON.INDEX / JSON.FIND work on database 0 only: unique prefix, removed afterwards.
        RuvioClient zero = await connection.GetDatabaseAsync(0);
        string prefix = "live-json-" + Guid.NewGuid().ToString("N") + ":";
        string index = "live" + Guid.NewGuid().ToString("N");
        Assert.True(await zero.JsonSetAsync(prefix + "1", "$", """{"city":"ankara"}"""));
        Assert.True(await zero.JsonSetAsync(prefix + "2", "$", """{"city":"ankara"}"""));
        Assert.True(await zero.JsonSetAsync(prefix + "3", "$", """{"city":"izmir"}"""));

        try
        {
            await zero.JsonCreateIndexAsync(index, prefix, "$.city");
            Assert.Contains(await zero.JsonListIndexesAsync(), line => line.StartsWith(index + " ", StringComparison.Ordinal));
            IReadOnlyList<string> found = [];

            // The first build runs in the background; 15 s is far beyond what it takes.
            for (DateTime deadline = DateTime.UtcNow.AddSeconds(15); DateTime.UtcNow < deadline;)
            {
                try
                {
                    found = await zero.JsonFindAsync(index, "ankara");
                    break;
                }
                catch (RuvioException building) when (building.Message.Contains("building", StringComparison.Ordinal))
                {
                    await Task.Delay(10);
                }
            }

            Assert.Equal([prefix + "1", prefix + "2"], found);
            Assert.Equal([prefix + "2"], await zero.JsonFindAsync(index, "ankara", 5, prefix + "1"));
        }
        finally
        {
            Assert.True(await zero.JsonDropIndexAsync(index));
            Assert.False(await zero.JsonDropIndexAsync(index));
            await zero.DeleteAsync([prefix + "1", prefix + "2", prefix + "3"]);
        }
    }

    private static async Task Streams(RuvioClient db)
    {
        string first = await db.AddStreamEntryAsync("st", "*", ("f", "1"));
        string second = await db.AddStreamEntryAsync("st", "*", 10, ("f", "2"));
        Assert.Null(await db.AddStreamEntryIfExistsAsync("nostream", "*", ("f", "1")));
        _ = await db.AddStreamEntryDelayedAsync("st", 1, "*", ("f", "3"));
        Assert.Equal(3, await db.GetStreamLengthAsync("st"));
        Assert.Equal(first, (await db.GetStreamRangeAsync("st", "-", "+"))[0].Id);
        Assert.Single(await db.GetStreamRangeAsync("st", "+", "-", 1, reverse: true));
        Assert.Equal(2, (await db.GetStreamUpdatesAsync(("st", first)))[0].Entries.Count);
        await db.CreateStreamGroupAsync("st", "g", "0");
        await db.CreateStreamGroupAsync("st2", "g", "$", createStream: true);
        Assert.Equal(1, await db.CreateStreamConsumerAsync("st", "g", "c0"));
        await Task.Delay(5);
        Assert.Equal(3, (await db.GetStreamGroupUpdatesAsync("g", "c1", "st", ">"))[0].Entries.Count);
        Assert.Equal(RespKind.Array, (await db.GetStreamPendingAsync("st", "g")).Kind);
        Assert.Equal(3, (await db.GetStreamPendingAsync("st", "g", "-", "+", 10)).Count);
        Assert.Single(await db.ClaimStreamEntriesAsync("st", "g", "c2", 0, first));
        Assert.Single(await db.ClaimStreamEntryIdsAsync("st", "g", "c1", 0, null, first));
        Assert.Equal(RespKind.Array, (await db.AutoClaimStreamAsync("st", "g", "c2", 0, "0-0")).Kind);
        Assert.Equal(1, await db.AcknowledgeStreamAsync("st", "g", second));
        Assert.Equal(RespKind.Array, (await db.GetStreamInfoAsync("st")).Kind);
        Assert.Equal(RespKind.Array, (await db.GetStreamGroupsAsync("st")).Kind);
        Assert.Equal(RespKind.Array, (await db.GetStreamConsumersAsync("st", "g")).Kind);
        await db.SetStreamGroupIdAsync("st", "g", "0");
        Assert.True(await db.DeleteStreamGroupConsumerAsync("st", "g", "c2") >= 0);
        Assert.Equal(1, await db.DestroyStreamGroupAsync("st", "g"));
        Assert.Equal(1, await db.DeleteStreamEntriesAsync("st", first));
        Assert.Equal(1, await db.TrimStreamAsync("st", 1));
        Assert.Equal(0, await db.TrimStreamMinIdAsync("st", "0-1"));
        await db.SetStreamIdAsync("st", "99999999999999-0");
    }

    private static async Task Transactions(RuvioClient db)
    {
        await db.WatchAsync("tx");
        await db.UnwatchAsync();
        await db.BeginTransactionAsync();
        await db.ExecuteAsync("SET", "tx", "1");
        await db.ExecuteAsync("INCR", "tx");
        IReadOnlyList<RespValue> replies = await db.ExecuteTransactionAsync();
        Assert.Equal(2, replies[1].Integer);
        await db.BeginTransactionAsync();
        await db.ExecuteAsync("SET", "tx", "9");
        await db.DiscardTransactionAsync();
        Assert.Equal("2", await db.GetStringAsync("tx"));
    }

    private static async Task Scripts(RuvioClient db)
    {
        Assert.Equal(1, (await db.EvaluateAsync("return 1", 0)).Integer);
        string sha = await db.LoadScriptAsync("return 2");
        Assert.Equal(2, (await db.EvaluateShaAsync(sha, 0)).Integer);
        Assert.Equal([true], await db.ScriptExistsAsync(sha));
        await db.FlushScriptsAsync();
        Assert.Equal("again", (await db.EvaluateCachedAsync("return ARGV[1]", 0, "again")).AsString());
        Assert.Equal("again", (await db.EvaluateCachedAsync("return ARGV[1]", 0, "again")).AsString());
        RuvioException notBusy = await Assert.ThrowsAsync<RuvioException>(async () => await db.KillScriptAsync());
        Assert.Contains("NOTBUSY", notBusy.Message, StringComparison.Ordinal);
        await db.LoadFunctionAsync("livefn", "return 7");
        Assert.Equal(7, (await db.CallFunctionAsync("livefn", 0)).Integer);
        Assert.Equal(RespKind.Array, (await db.GetFunctionListAsync()).Kind);
        Assert.True(await db.DeleteFunctionAsync("livefn"));
    }

    private static async Task PubSub(RuvioClient db)
    {
        var got = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var pattern = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var sharded = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        await db.SubscribeAsync("live-ch", message => got.TrySetResult(message.Payload));
        await db.SubscribePatternsAsync("live-p*", message => pattern.TrySetResult(message.Payload));
        await db.SubscribeShardedAsync("live-sh", message => sharded.TrySetResult(message.Payload));
        Assert.True(await db.PublishAsync("live-ch", "hi") >= 1);
        Assert.True(await db.PublishAsync("live-pat", "pat") >= 1);
        Assert.True(await db.PublishShardedAsync("live-sh", "sh") >= 1);
        Assert.Equal("hi", await got.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal("pat", await pattern.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal("sh", await sharded.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        await db.UnsubscribeAsync("live-ch");
        await db.UnsubscribePatternsAsync("live-p*");
        await db.UnsubscribeShardedAsync("live-sh");
    }

    private static async Task Admin(RuvioConnection connection, RuvioClient db, RecordingProxy proxy)
    {
        Assert.Equal("PONG", await db.PingAsync());
        Assert.Equal("hi", await db.PingAsync("hi"));
        Assert.Equal("hi", await db.EchoAsync("hi"));
        Assert.Equal(RespKind.Array, (await db.HelloAsync(2)).Kind);
        Assert.Equal("PONG is taken", await db.RuvioAsync());
        Assert.NotEmpty(await db.GetRuvioOwnerAsync());
        Assert.Equal(RespKind.Array, (await db.GetCommandsAsync()).Kind);
        Assert.True(await db.GetCommandCountAsync() > 200);
        Assert.Single((await db.GetCommandInfoAsync("get")).Items!);
        Assert.Equal(RespKind.Array, (await db.GetCommandDocsAsync("get")).Kind);
        Assert.Empty(await db.GetLatencyLatestAsync());
        Assert.Equal(0, await db.WaitForReplicasAsync(0, TimeSpan.FromMilliseconds(10)));
        Assert.Contains("# Server", await db.GetInfoAsync(), StringComparison.Ordinal);
        Assert.Contains("ruvio_", await db.GetInfoAsync("server"), StringComparison.Ordinal);
        Assert.Equal(RespKind.Array, (await db.GetConfigAsync("maxmemory")).Kind);
        await db.SetConfigAsync([("slowlog-max-len", "128")]);
        Assert.Equal("128", (await db.GetConfigAsync("slowlog-max-len")).Items![1].AsString());
        await Assert.ThrowsAsync<RuvioException>(async () => await db.SetConfigAsync([("shards", "2")]));
        Assert.Equal("master", (await db.GetRoleAsync()).Items![0].AsString());
        // The live server keeps no WAL, so it cannot replicate: these are refused unchanged.
        await Assert.ThrowsAsync<RuvioException>(async () => await db.ReplicaOfAsync("127.0.0.1", 1));
        await Assert.ThrowsAsync<RuvioException>(async () => await db.ReplicaOfNoOneAsync());
        await Assert.ThrowsAsync<RuvioException>(async () => await db.SetDurabilityAsync(1));

        // Only on a throwaway server: CONFIG REWRITE rewrites the server's config file.
        if (RewritesConfig)
        {
            await db.RewriteConfigAsync();
        }

        await db.SaveAsync();
        await db.SaveInBackgroundAsync();
        Assert.True(await db.GetSlowLogLengthAsync() >= 0);
        Assert.NotNull(await db.GetSlowLogAsync(5));
        await db.ResetSlowLogAsync();
        await ServerAnswers(() => db.GetObjectFrequencyAsync("nokey").AsTask()); // needs an LFU maxmemory-policy

        // ACL: create a user, AUTH as it on a second connection, then inspect.
        await db.SetAclUserAsync("livetest", "on", ">pw", "~*", "+@all");
        Assert.Contains("livetest", await db.GetAclUsersAsync());
        Assert.Contains(await db.GetAclListAsync(), line => line.Contains("livetest", StringComparison.Ordinal));
        Assert.Equal(RespKind.Array, (await db.GetAclUserAsync("livetest")).Kind);
        Assert.Contains("string", await db.GetAclCategoriesAsync());
        Assert.Equal("default", await db.GetAclUserNameAsync());
        await using (RuvioConnection second = await LiveServer.ConnectAsync(Db, proxy, flush: false))
        {
            RuvioClient other = await second.GetDefaultDatabaseAsync();
            await other.AuthenticateAsync("livetest", "pw");
            Assert.Equal("livetest", await other.GetAclUserNameAsync());
            await ServerAnswers(() => other.AuthenticateAsync("pw").AsTask()); // the default user: OK or an error, by setup
            await other.SetConnectionNameAsync("live-second");
            Assert.Equal("live-second", await other.GetConnectionNameAsync());
            await other.SetTrackingAsync(false);
            long id = await other.GetClientIdAsync();
            string info = await other.GetClientInfoAsync();
            Assert.Contains("live-second", await db.GetClientListAsync(), StringComparison.Ordinal);
            string address = info.Split(' ').First(part => part.StartsWith("addr=", StringComparison.Ordinal))["addr=".Length..];
            Assert.Equal(1, await db.KillClientAsync(address));
            Assert.Equal(0, await db.KillClientAsync(id));
        }

        // Not configured or not a cluster in this setup: the server must still answer each one.
        await ServerAnswers(() => db.LoadAclAsync().AsTask());
        await ServerAnswers(() => db.SaveAclAsync().AsTask());
        await ServerAnswers(() => db.GetClusterSlotsAsync().AsTask());
        await ServerAnswers(() => db.GetClusterNodesAsync().AsTask());
        await ServerAnswers(() => db.GetClusterShardsAsync().AsTask());
        await ServerAnswers(() => db.GetClusterInfoAsync().AsTask());
        await ServerAnswers(() => db.GetClusterMyIdAsync().AsTask());
        await ServerAnswers(() => db.ReadOnlyAsync().AsTask());
        await ServerAnswers(() => db.ReadWriteAsync().AsTask());
        Assert.Equal(12182, await db.GetClusterKeySlotAsync("foo"));
        Assert.True((await db.GetMoveSlotsStatusAsync()).ContainsKey("state"));

        Assert.True(await db.SetAsync("flush-me", "1"));
        await db.FlushAllDatabasesAsync();
        Assert.Equal(0, await db.GetDatabaseSizeAsync());
        _ = await connection.GetDatabaseAsync(Db);
    }

    [LiveFact(Timeout = 60000)]
    public async Task The_deadline_is_set_only_when_the_call_creates_the_key()
    {
        await using RuvioConnection connection = await LiveServer.ConnectAsync(Db);
        RuvioClient db = await connection.GetDefaultDatabaseAsync();
        TimeSpan ttl = TimeSpan.FromSeconds(100);

        Assert.Equal(1, await db.IncrementAsync("ttl:incr", ttl));
        Assert.InRange(await db.GetTimeToLiveAsync("ttl:incr"), 90, 100);
        Assert.Equal(2, await db.IncrementAsync("ttl:incr", TimeSpan.FromSeconds(5000)));
        Assert.InRange(await db.GetTimeToLiveAsync("ttl:incr"), 90, 100);

        Assert.Equal(-1, await db.DecrementAsync("ttl:decr", ttl));
        Assert.Equal(5, await db.IncrementByAsync("ttl:incrby", 5, ttl));
        Assert.Equal(-5, await db.DecrementByAsync("ttl:decrby", 5, ttl));
        Assert.Equal(1, await db.IncrementWithLimitAsync("ttl:max", 1, 2, ttl));
        Assert.Equal(2, await db.IncrementWithLimitAsync("ttl:max", 1, 2, ttl));
        Assert.Null(await db.IncrementWithLimitAsync("ttl:max", 1, 2, ttl));
        Assert.Equal(1.5, await db.IncrementByFloatAsync("ttl:float", 1.5, ttl));
        Assert.Equal(3, await db.IncrementHashFieldAsync("ttl:hash", "n", 3, ttl));
        Assert.Equal(2.5, await db.IncrementSortedSetAsync("ttl:zset", "m", 2.5, ttl));
        Assert.False(await db.SetBitAsync("ttl:bit", 7, true, ttl));
        Assert.Equal(1, await db.AddSortedSetAsync("ttl:zadd", ttl, SortedSetAddFlags.None, ("a", 1)));
        Assert.Equal(1, await db.AddGeoAsync("ttl:geo", ttl, (13.361389, 38.115556, "palermo")));
        Assert.NotEmpty(await db.AddStreamEntryAsync("ttl:stream", ttl, "*", ("f", "v")));
        Assert.True(await db.JsonSetAsync("ttl:json", "$", "{}", SetMode.IfNotExists, ttl));
        Assert.Equal([true], await db.InsertBloomFilterAsync("ttl:bloom", ["x"], new BloomInsertOptions { Ttl = ttl }));

        foreach (string key in new[] { "ttl:decr", "ttl:incrby", "ttl:decrby", "ttl:max", "ttl:float", "ttl:hash", "ttl:zset", "ttl:bit", "ttl:zadd", "ttl:geo", "ttl:stream", "ttl:json", "ttl:bloom" })
        {
            Assert.InRange(await db.GetTimeToLiveAsync(key), 90, 100);
        }

        Assert.Equal(1, await db.IncrementAsync("ttl:ms", TimeSpan.FromMilliseconds(90_500)));
        Assert.InRange(await db.GetTimeToLiveMillisecondsAsync("ttl:ms"), 80_000, 90_500);
    }

    [LiveFact(Timeout = 60000)]
    public async Task Expire_conditions_decide_whether_the_deadline_changes()
    {
        await using RuvioConnection connection = await LiveServer.ConnectAsync(Db);
        RuvioClient db = await connection.GetDefaultDatabaseAsync();

        Assert.True(await db.SetAsync("exp:key", "v"));
        Assert.False(await db.ExpireAsync("exp:key", TimeSpan.FromSeconds(100), ExpireCondition.Xx));
        Assert.True(await db.ExpireAsync("exp:key", TimeSpan.FromSeconds(100), ExpireCondition.Nx));
        Assert.False(await db.ExpireAsync("exp:key", TimeSpan.FromSeconds(200), ExpireCondition.Nx));
        Assert.False(await db.ExpireAsync("exp:key", TimeSpan.FromSeconds(50), ExpireCondition.Gt));
        Assert.True(await db.ExpireAsync("exp:key", TimeSpan.FromSeconds(200), ExpireCondition.Gt));
        Assert.False(await db.ExpireAsync("exp:key", TimeSpan.FromSeconds(300), ExpireCondition.Lt));
        Assert.True(await db.ExpireAsync("exp:key", TimeSpan.FromSeconds(150), ExpireCondition.Lt));
        Assert.InRange(await db.GetTimeToLiveAsync("exp:key"), 140, 150);

        Assert.True(await db.SetAsync("exp:at", "v"));
        DateTimeOffset soon = DateTimeOffset.UtcNow.AddSeconds(100);
        Assert.True(await db.ExpireAtAsync("exp:at", soon, ExpireCondition.Nx));
        Assert.False(await db.ExpireAtAsync("exp:at", soon.AddSeconds(100), ExpireCondition.Lt));
        Assert.True(await db.ExpireAtAsync("exp:at", soon.AddSeconds(100), ExpireCondition.Gt));
    }

    private static async Task ServerAnswers(Func<Task> call)
    {
        try
        {
            await call();
        }
        catch (RuvioException exception) when (exception.IsServerReply)
        {
            // The command reached the server and was answered with an error, which is expected here.
        }
    }
}
