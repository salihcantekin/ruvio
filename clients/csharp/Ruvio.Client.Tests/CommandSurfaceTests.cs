using System.Text.Json;
using System.Text.Json.Serialization;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

public partial class CommandSurfaceTests
{
    [Fact(Timeout = 15000)]
    public async Task JsonValueCommandsMapRepliesAndAbsentPaths()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<string?> type = session.Client.JsonTypeAsync("doc", ".n");

        await session.Expect("JSON.TYPE", "doc", ".n");
        await session.Reply("$7\r\ninteger\r\n");
        Assert.Equal("integer", await type);

        ValueTask<long?> strlen = session.Client.JsonStringLengthAsync("doc", "$.s");

        await session.Expect("JSON.STRLEN", "doc", "$.s");
        await session.Reply(":6\r\n");
        Assert.Equal(6L, await strlen);

        ValueTask<long?> missing = session.Client.JsonArrayLengthAsync("doc", "$.zz");

        await session.Expect("JSON.ARRLEN", "doc", "$.zz");
        await session.Reply("$-1\r\n");
        Assert.Null(await missing);

        ValueTask<IReadOnlyList<string>> keys = session.Client.JsonObjectKeysAsync("doc", "$.o");

        await session.Expect("JSON.OBJKEYS", "doc", "$.o");
        await session.Reply("*2\r\n$1\r\na\r\n$1\r\nb\r\n");
        Assert.Equal(["a", "b"], await keys);

        ValueTask<string?> increment = session.Client.JsonIncrementAsync("doc", "$.n", 2.5);

        await session.Expect("JSON.NUMINCRBY", "doc", "$.n", "2.5");
        await session.Reply("$3\r\n7.5\r\n");
        Assert.Equal("7.5", await increment);

        ValueTask<bool?> toggle = session.Client.JsonToggleAsync("doc", "$.t");

        await session.Expect("JSON.TOGGLE", "doc", "$.t");
        await session.Reply(":1\r\n");
        Assert.True(await toggle);

        ValueTask<long?> append = session.Client.JsonArrayAppendAsync("doc", "$.l", "2");

        await session.Expect("JSON.ARRAPPEND", "doc", "$.l", "2");
        await session.Reply(":2\r\n");
        Assert.Equal(2L, await append);

        ValueTask<long> clear = session.Client.JsonClearAsync("doc", "$.o");

        await session.Expect("JSON.CLEAR", "doc", "$.o");
        await session.Reply(":1\r\n");
        Assert.Equal(1L, await clear);
    }

    [Fact(Timeout = 15000)]
    public async Task JsonCommandsSendNativeArgumentsAndMapNullReplies()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<bool> set = session.Client.JsonSetAsync("doc", "$", "{\"a\":1}");

        await session.Expect("JSON.SET", "doc", "$", "{\"a\":1}");
        await session.Reply("+OK\r\n");
        Assert.True(await set);

        ValueTask<bool> missing = session.Client.JsonSetAsync("doc", "$.x.y", "1");

        await session.Expect("JSON.SET", "doc", "$.x.y", "1");
        await session.Reply("$-1\r\n");
        Assert.False(await missing);

        ValueTask<bool> merge = session.Client.JsonMergeAsync("doc", "$", "{\"b\":2}");

        await session.Expect("JSON.MERGE", "doc", "$", "{\"b\":2}");
        await session.Reply("+OK\r\n");
        Assert.True(await merge);

        ValueTask<string?> get = session.Client.JsonGetAsync("doc");

        await session.Expect("JSON.GET", "doc", "$");
        await session.Reply("$13\r\n{\"a\":1,\"b\":2}\r\n");
        Assert.Equal("{\"a\":1,\"b\":2}", await get);

        ValueTask<IReadOnlyList<string?>> many = session.Client.JsonGetManyAsync("$.a", "doc", "none");

        await session.Expect("JSON.MGET", "doc", "none", "$.a");
        await session.Reply("*2\r\n$1\r\n1\r\n$-1\r\n");
        Assert.Equal(new string?[] { "1", null }, await many);

        ValueTask<long> delete = session.Client.JsonDeleteAsync("doc", "$.a");

        await session.Expect("JSON.DEL", "doc", "$.a");
        await session.Reply(":1\r\n");
        Assert.Equal(1, await delete);
    }

    [Fact(Timeout = 15000)]
    public async Task IdempotencyUsesNativeCommandsAndPreservesResultBytes()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<RespValue> acquire = session.Client.BeginIdempotencyAsync("request", "fp", "owner", 1000);

        await session.Expect("IDEM", "request", "BEGIN", "fp", "owner", "1000");
        await session.Reply("*1\r\n$8\r\nacquired\r\n");
        Assert.Equal("acquired", (await acquire).Items![0].AsString());
        ValueTask<bool> complete = session.Client.CompleteIdempotencyAsync("request", "fp", "owner", "saved");

        await session.Expect("IDEM", "request", "COMPLETE", "fp", "owner", "saved");
        await session.Reply(":1\r\n");
        Assert.True(await complete);
        complete = session.Client.CompleteIdempotencyAsync("request", "fp", "other", "saved");
        await session.Expect("IDEM", "request", "COMPLETE", "fp", "other", "saved");
        await session.Reply(":0\r\n");
        Assert.False(await complete);
        acquire = session.Client.BeginIdempotencyAsync("request", "fp", "retry", 1000);
        await session.Expect("IDEM", "request", "BEGIN", "fp", "retry", "1000");
        await session.Reply("*2\r\n$9\r\ncompleted\r\n$0\r\n\r\n");
        Assert.Empty((await acquire).Items![1].Bytes!);
    }

    [Fact(Timeout = 15000)]
    public async Task DeleteSendsDelForOneKey()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<long> pending = session.Client.DeleteAsync("session");

        await session.Expect("DEL", "session");
        await session.Reply(":1\r\n");
        Assert.Equal(1, await pending);
    }

    [Fact(Timeout = 15000)]
    public async Task PopListLeftBlockingSendsBlpopForOneKey()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<KeyValuePair<string, string>?> pending =
            session.Client.PopListLeftBlockingAsync("jobs", TimeSpan.FromSeconds(1));

        await session.Expect("BLPOP", "jobs", "1");
        await session.Reply("*2\r\n$4\r\njobs\r\n$1\r\na\r\n");
        KeyValuePair<string, string>? popped = await pending;

        Assert.Equal("jobs", popped!.Value.Key);
        Assert.Equal("a", popped.Value.Value);
    }

    [Fact(Timeout = 15000)]
    public async Task AddSortedSetSendsZaddPairsAndReturnsTheNewMemberCount()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<long> pending = session.Client.AddSortedSetAsync(
            "leaderboard",
            new SortedSetEntry("player2", 50),
            new SortedSetEntry("player1", 100));

        await session.Expect("ZADD", "leaderboard", "50", "player2", "100", "player1");
        await session.Reply(":2\r\n");
        Assert.Equal(2, await pending);
    }

    [Fact(Timeout = 15000)]
    public async Task GetSortedSetRangeByRankWithScoresSendsZrevrangeWhenDescending()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<IReadOnlyList<SortedSetEntry>> pending =
            session.Client.GetSortedSetRangeByRankWithScoresAsync("leaderboard", 0, -1, descending: true);

        await session.Expect("ZREVRANGE", "leaderboard", "0", "-1", "WITHSCORES");
        await session.Reply("*4\r\n$7\r\nplayer1\r\n$3\r\n100\r\n$7\r\nplayer2\r\n$2\r\n50\r\n");
        IReadOnlyList<SortedSetEntry> rows = await pending;

        Assert.Equal(2, rows.Count);
        Assert.Equal("player1", rows[0].Member);
        Assert.Equal(100, rows[0].Score);
        Assert.Equal("player2", rows[1].Member);
        Assert.Equal(50, rows[1].Score);
    }

    [Fact(Timeout = 15000)]
    public async Task GetSortedSetRangeByRankSendsZrange()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<IReadOnlyList<string>> pending = session.Client.GetSortedSetRangeByRankAsync("z", 0, -1);

        await session.Expect("ZRANGE", "z", "0", "-1");
        await session.Reply("*2\r\n$1\r\na\r\n$1\r\nb\r\n");
        Assert.Equal(["a", "b"], await pending);
    }

    [Fact(Timeout = 15000)]
    public async Task SaveAclSendsAclSave()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask pending = session.Client.SaveAclAsync();

        await session.Expect("ACL", "SAVE");
        await session.Reply("+OK\r\n");
        await pending;
        ValueTask load = session.Client.LoadAclAsync();

        await session.Expect("ACL", "LOAD");
        await session.Reply("+OK\r\n");
        await load;
    }

    [Fact(Timeout = 15000)]
    public async Task GetSortedSetRangeByLexSendsZrangeBylex()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<IReadOnlyList<string>> pending =
            session.Client.GetSortedSetRangeByLexAsync("names", "[b", "+");

        await session.Expect("ZRANGE", "names", "[b", "+", "BYLEX");
        await session.Reply("*2\r\n$1\r\nb\r\n$1\r\nc\r\n");
        Assert.Equal(["b", "c"], await pending);
    }

    [Fact(Timeout = 15000)]
    public async Task GetSortedSetRangeByScoreWithScoresSendsZrangebyscore()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<IReadOnlyList<SortedSetEntry>> pending =
            session.Client.GetSortedSetRangeByScoreWithScoresAsync("z", "-inf", "+inf");

        await session.Expect("ZRANGEBYSCORE", "z", "-inf", "+inf", "WITHSCORES");
        await session.Reply("*2\r\n$1\r\na\r\n$1\r\n1\r\n");
        IReadOnlyList<SortedSetEntry> rows = await pending;

        Assert.Equal("a", rows[0].Member);
        Assert.Equal(1, rows[0].Score);
    }

    [Fact(Timeout = 15000)]
    public async Task RemoveSortedSetMembersSendsZrem()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<long> pending = session.Client.RemoveSortedSetMembersAsync("z", "a", "b");

        await session.Expect("ZREM", "z", "a", "b");
        await session.Reply(":1\r\n");
        Assert.Equal(1, await pending);
    }

    [Fact(Timeout = 15000)]
    public async Task IncrementSortedSetSendsZincrbyAndParsesTheScore()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<double> pending = session.Client.IncrementSortedSetAsync("z", "a", 1.5);

        await session.Expect("ZINCRBY", "z", "1.5", "a");
        await session.Reply("$3\r\n2.5\r\n");
        Assert.Equal(2.5, await pending);
    }

    [Fact(Timeout = 15000)]
    public async Task GetSortedSetCountAndScoreAndRank()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<long> card = session.Client.GetSortedSetCountAsync("z");

        await session.Expect("ZCARD", "z");
        await session.Reply(":3\r\n");
        Assert.Equal(3, await card);

        ValueTask<double?> score = session.Client.GetSortedSetScoreAsync("z", "a");

        await session.Expect("ZSCORE", "z", "a");
        await session.Reply("$3\r\n1.5\r\n");
        Assert.Equal(1.5, await score);

        ValueTask<long?> rank = session.Client.GetSortedSetRankAsync("z", "a");

        await session.Expect("ZRANK", "z", "a");
        await session.Reply(":0\r\n");
        Assert.Equal(0, await rank);

        ValueTask<long?> rev = session.Client.GetSortedSetRankAsync("z", "a", descending: true);

        await session.Expect("ZREVRANK", "z", "a");
        await session.Reply("$-1\r\n");
        Assert.Null(await rev);
    }

    [Fact(Timeout = 15000)]
    public async Task StringFamilySendsTheSupportedCommands()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<IReadOnlyList<string?>> mget = session.Client.GetManyAsync("a", "b");

        await session.Expect("MGET", "a", "b");
        await session.Reply("*2\r\n$1\r\n1\r\n$-1\r\n");
        Assert.Equal(["1", null], await mget);

        ValueTask pendingSet = session.Client.SetManyAsync(("a", "1"), ("b", "2"));

        await session.Expect("MSET", "a", "1", "b", "2");
        await session.Reply("+OK\r\n");
        await pendingSet;

        ValueTask<string?> getSet = session.Client.GetSetAsync("a", "next");

        await session.Expect("GETSET", "a", "next");
        await session.Reply("$3\r\nold\r\n");
        Assert.Equal("old", await getSet);

        ValueTask<long> append = session.Client.AppendAsync("a", "!");

        await session.Expect("APPEND", "a", "!");
        await session.Reply(":4\r\n");
        Assert.Equal(4, await append);

        ValueTask<long> len = session.Client.GetStringLengthAsync("a");

        await session.Expect("STRLEN", "a");
        await session.Reply(":3\r\n");
        Assert.Equal(3, await len);

        ValueTask<long> decr = session.Client.DecrementAsync("n");

        await session.Expect("DECR", "n");
        await session.Reply(":0\r\n");
        Assert.Equal(0, await decr);

        ValueTask<long> incrBy = session.Client.IncrementByAsync("n", 5);

        await session.Expect("INCRBY", "n", "5");
        await session.Reply(":5\r\n");
        Assert.Equal(5, await incrBy);

        ValueTask<long> decrBy = session.Client.DecrementByAsync("n", 2);

        await session.Expect("DECRBY", "n", "2");
        await session.Reply(":3\r\n");
        Assert.Equal(3, await decrBy);

        ValueTask<long> incr = session.Client.IncrementAsync("n");

        await session.Expect("INCR", "n");
        await session.Reply(":4\r\n");
        Assert.Equal(4, await incr);

        ValueTask<RespValue> raw = session.Client.ExecuteAsync("ECHO", "hi");

        await session.Expect("ECHO", "hi");
        await session.Reply("$2\r\nhi\r\n");
        Assert.Equal("hi", (await raw).AsString());

        ValueTask<IReadOnlyList<RespValue>> piped = session.Client.ExecuteManyAsync([["PING"], ["PING"]]);

        await session.Expect("PING");
        await session.Expect("PING");
        await session.Reply("+PONG\r\n+PONG\r\n");
        Assert.Equal(["PONG", "PONG"], (await piped).Select(value => value.AsString()!).ToArray());
    }

    [Fact(Timeout = 15000)]
    public async Task KeyFamilySendsTheSupportedCommands()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<long> unlink = session.Client.UnlinkAsync("a", "b");

        await session.Expect("UNLINK", "a", "b");
        await session.Reply(":1\r\n");
        Assert.Equal(1, await unlink);

        ValueTask<long> exists = session.Client.ExistsAsync("a", "b");

        await session.Expect("EXISTS", "a", "b");
        await session.Reply(":1\r\n");
        Assert.Equal(1, await exists);

        ValueTask<string> type = session.Client.GetKeyTypeAsync("a");

        await session.Expect("TYPE", "a");
        await session.Reply("+zset\r\n");
        Assert.Equal("zset", await type);

        ValueTask rename = session.Client.RenameAsync("a", "b");

        await session.Expect("RENAME", "a", "b");
        await session.Reply("+OK\r\n");
        await rename;

        ValueTask<ScanPage> scan = session.Client.ScanAsync(0, "lb:*", 10);

        await session.Expect("SCAN", "0", "MATCH", "lb:*", "COUNT", "10");
        await session.Reply("*2\r\n$1\r\n0\r\n*1\r\n$2\r\nlb\r\n");
        ScanPage page = await scan;

        Assert.Equal(0, page.Cursor);
        Assert.Equal(["lb"], page.Keys);

        ValueTask<long> size = session.Client.GetDatabaseSizeAsync();

        await session.Expect("DBSIZE");
        await session.Reply(":4\r\n");
        Assert.Equal(4, await size);

        ValueTask<bool> pexpire = session.Client.ExpireAsync("a", TimeSpan.FromMilliseconds(1500));

        await session.Expect("PEXPIRE", "a", "1500");
        await session.Reply(":1\r\n");
        Assert.True(await pexpire);

        ValueTask<bool> expireAt = session.Client.ExpireAtAsync("a", DateTimeOffset.FromUnixTimeSeconds(10));

        await session.Expect("EXPIREAT", "a", "10");
        await session.Reply(":1\r\n");
        Assert.True(await expireAt);

        ValueTask<bool> pexpireAt = session.Client.ExpireAtAsync("a", DateTimeOffset.FromUnixTimeMilliseconds(1500));

        await session.Expect("PEXPIREAT", "a", "1500");
        await session.Reply(":1\r\n");
        Assert.True(await pexpireAt);

        ValueTask<long> ttl = session.Client.GetTimeToLiveAsync("a");

        await session.Expect("TTL", "a");
        await session.Reply(":9\r\n");
        Assert.Equal(9, await ttl);

        ValueTask<long> pttl = session.Client.GetTimeToLiveMillisecondsAsync("a");

        await session.Expect("PTTL", "a");
        await session.Reply(":900\r\n");
        Assert.Equal(900, await pttl);

        ValueTask<bool> persist = session.Client.PersistAsync("a");

        await session.Expect("PERSIST", "a");
        await session.Reply(":1\r\n");
        Assert.True(await persist);
    }

    [Fact(Timeout = 15000)]
    public async Task ListFamilySendsTheSupportedCommands()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<long> left = session.Client.PushListLeftAsync("q", "a", "b");

        await session.Expect("LPUSH", "q", "a", "b");
        await session.Reply(":2\r\n");
        Assert.Equal(2, await left);

        ValueTask<long> right = session.Client.PushListRightAsync("q", "c");

        await session.Expect("RPUSH", "q", "c");
        await session.Reply(":3\r\n");
        Assert.Equal(3, await right);

        ValueTask<string?> popL = session.Client.PopListLeftAsync("q");

        await session.Expect("LPOP", "q");
        await session.Reply("$1\r\nb\r\n");
        Assert.Equal("b", await popL);

        ValueTask<IReadOnlyList<string>> popMany = session.Client.PopListRightAsync("q", 2);

        await session.Expect("RPOP", "q", "2");
        await session.Reply("*2\r\n$1\r\nc\r\n$1\r\na\r\n");
        Assert.Equal(["c", "a"], await popMany);

        ValueTask<long> llen = session.Client.GetListLengthAsync("q");

        await session.Expect("LLEN", "q");
        await session.Reply(":0\r\n");
        Assert.Equal(0, await llen);

        ValueTask<string?> index = session.Client.GetListItemAsync("q", 0);

        await session.Expect("LINDEX", "q", "0");
        await session.Reply("$-1\r\n");
        Assert.Null(await index);

        ValueTask<IReadOnlyList<string>> range = session.Client.GetListRangeAsync("q", 0, -1);

        await session.Expect("LRANGE", "q", "0", "-1");
        await session.Reply("*0\r\n");
        Assert.Empty(await range);

        ValueTask trim = session.Client.TrimListAsync("q", 0, 1);

        await session.Expect("LTRIM", "q", "0", "1");
        await session.Reply("+OK\r\n");
        await trim;

        ValueTask<KeyValuePair<string, string>?> block = session.Client.PopListLeftBlockingAsync(["q"], TimeSpan.FromSeconds(1));

        await session.Expect("BLPOP", "q", "1");
        await session.Reply("*2\r\n$1\r\nq\r\n$1\r\nv\r\n");
        Assert.Equal(new KeyValuePair<string, string>("q", "v"), await block);

        ValueTask<KeyValuePair<string, string>?> blockR = session.Client.PopListRightBlockingAsync(["q"], TimeSpan.Zero);

        await session.Expect("BRPOP", "q", "0");
        await session.Reply("$-1\r\n");
        Assert.Null(await blockR);
    }

    [Fact(Timeout = 15000)]
    public async Task SetFamilySendsTheSupportedCommands()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<long> add = session.Client.AddSetMembersAsync("s", "a", "b");

        await session.Expect("SADD", "s", "a", "b");
        await session.Reply(":2\r\n");
        Assert.Equal(2, await add);

        ValueTask<long> rem = session.Client.RemoveSetMembersAsync("s", "a");

        await session.Expect("SREM", "s", "a");
        await session.Reply(":1\r\n");
        Assert.Equal(1, await rem);

        ValueTask<bool> member = session.Client.SetContainsAsync("s", "b");

        await session.Expect("SISMEMBER", "s", "b");
        await session.Reply(":1\r\n");
        Assert.True(await member);

        ValueTask<long> card = session.Client.GetSetCountAsync("s");

        await session.Expect("SCARD", "s");
        await session.Reply(":1\r\n");
        Assert.Equal(1, await card);

        ValueTask<IReadOnlyList<string>> members = session.Client.GetSetMembersAsync("s");

        await session.Expect("SMEMBERS", "s");
        await session.Reply("*1\r\n$1\r\nb\r\n");
        Assert.Equal(["b"], await members);

        ValueTask<IReadOnlyList<string>> inter = session.Client.GetSetIntersectionAsync("a", "b");

        await session.Expect("SINTER", "a", "b");
        await session.Reply("*0\r\n");
        Assert.Empty(await inter);

        ValueTask<IReadOnlyList<string>> union = session.Client.GetSetUnionAsync("a");

        await session.Expect("SUNION", "a");
        await session.Reply("*0\r\n");
        Assert.Empty(await union);

        ValueTask<IReadOnlyList<string>> diff = session.Client.GetSetDifferenceAsync("a", "b");

        await session.Expect("SDIFF", "a", "b");
        await session.Reply("*0\r\n");
        Assert.Empty(await diff);

        ValueTask<long> storeI = session.Client.StoreSetIntersectionAsync("d", "a", "b");

        await session.Expect("SINTERSTORE", "d", "a", "b");
        await session.Reply(":0\r\n");
        Assert.Equal(0, await storeI);

        ValueTask<long> storeU = session.Client.StoreSetUnionAsync("d", "a");

        await session.Expect("SUNIONSTORE", "d", "a");
        await session.Reply(":0\r\n");
        Assert.Equal(0, await storeU);

        ValueTask<long> storeD = session.Client.StoreSetDifferenceAsync("d", "a", "b");

        await session.Expect("SDIFFSTORE", "d", "a", "b");
        await session.Reply(":0\r\n");
        Assert.Equal(0, await storeD);

        ValueTask<bool> move = session.Client.MoveSetMemberAsync("a", "b", "x");

        await session.Expect("SMOVE", "a", "b", "x");
        await session.Reply(":0\r\n");
        Assert.False(await move);

        ValueTask<string?> pop = session.Client.PopSetMemberAsync("s");

        await session.Expect("SPOP", "s");
        await session.Reply("$-1\r\n");
        Assert.Null(await pop);

        ValueTask<string?> rand = session.Client.GetSetRandomMemberAsync("s");

        await session.Expect("SRANDMEMBER", "s");
        await session.Reply("$-1\r\n");
        Assert.Null(await rand);
    }

    [Fact(Timeout = 15000)]
    public async Task HashFamilySendsTheSupportedCommands()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<long> hset = session.Client.SetHashFieldsAsync("h", ("f", "1"), ("g", "2"));

        await session.Expect("HSET", "h", "f", "1", "g", "2");
        await session.Reply(":2\r\n");
        Assert.Equal(2, await hset);

        ValueTask hmset = session.Client.SetHashManyAsync("h", ("f", "1"));

        await session.Expect("HMSET", "h", "f", "1");
        await session.Reply("+OK\r\n");
        await hmset;

        ValueTask<string?> hget = session.Client.GetHashFieldAsync("h", "f");

        await session.Expect("HGET", "h", "f");
        await session.Reply("$1\r\n1\r\n");
        Assert.Equal("1", await hget);

        ValueTask<long> hdel = session.Client.DeleteHashFieldsAsync("h", "g");

        await session.Expect("HDEL", "h", "g");
        await session.Reply(":1\r\n");
        Assert.Equal(1, await hdel);

        ValueTask<long> hlen = session.Client.GetHashLengthAsync("h");

        await session.Expect("HLEN", "h");
        await session.Reply(":1\r\n");
        Assert.Equal(1, await hlen);

        ValueTask<IReadOnlyDictionary<string, string>> all = session.Client.GetHashAllAsync("h");

        await session.Expect("HGETALL", "h");
        await session.Reply("*2\r\n$1\r\nf\r\n$1\r\n1\r\n");
        Assert.Equal("1", (await all)["f"]);

        ValueTask<IReadOnlyList<string?>> hmget = session.Client.GetHashFieldsAsync("h", "f", "x");

        await session.Expect("HMGET", "h", "f", "x");
        await session.Reply("*2\r\n$1\r\n1\r\n$-1\r\n");
        Assert.Equal(["1", null], await hmget);

        ValueTask<bool> hex = session.Client.HashFieldExistsAsync("h", "f");

        await session.Expect("HEXISTS", "h", "f");
        await session.Reply(":1\r\n");
        Assert.True(await hex);

        ValueTask<IReadOnlyList<string>> keys = session.Client.GetHashFieldNamesAsync("h");

        await session.Expect("HKEYS", "h");
        await session.Reply("*1\r\n$1\r\nf\r\n");
        Assert.Equal(["f"], await keys);

        ValueTask<IReadOnlyList<string>> vals = session.Client.GetHashFieldValuesAsync("h");

        await session.Expect("HVALS", "h");
        await session.Reply("*1\r\n$1\r\n1\r\n");
        Assert.Equal(["1"], await vals);

        ValueTask<long> incr = session.Client.IncrementHashFieldAsync("h", "f", 2);

        await session.Expect("HINCRBY", "h", "f", "2");
        await session.Reply(":3\r\n");
        Assert.Equal(3, await incr);

        ValueTask<bool> nx = session.Client.SetHashFieldIfAbsentAsync("h", "z", "9");

        await session.Expect("HSETNX", "h", "z", "9");
        await session.Reply(":1\r\n");
        Assert.True(await nx);

        ValueTask<long> hstrlen = session.Client.GetHashFieldLengthAsync("h", "f");

        await session.Expect("HSTRLEN", "h", "f");
        await session.Reply(":1\r\n");
        Assert.Equal(1, await hstrlen);

        ValueTask<HashScanPage> hscan = session.Client.ScanHashAsync("h", 0);

        await session.Expect("HSCAN", "h", "0");
        await session.Reply("*2\r\n$1\r\n0\r\n*2\r\n$1\r\nf\r\n$1\r\n1\r\n");
        HashScanPage page = await hscan;

        Assert.Equal(0, page.Cursor);
        Assert.Equal("1", page.Fields["f"]);
    }

    [Fact(Timeout = 15000)]
    public async Task BloomStreamAndAdminCommandsUseTheSupportedNames()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask reserve = session.Client.ReserveBloomFilterAsync("bf", 0.01, 100);

        await session.Expect("BF.RESERVE", "bf", "0.01", "100");
        await session.Reply("+OK\r\n");
        await reserve;

        ValueTask<bool> added = session.Client.AddBloomFilterAsync("bf", "x");

        await session.Expect("BF.ADD", "bf", "x");
        await session.Reply(":1\r\n");
        Assert.True(await added);

        ValueTask<bool> exists = session.Client.BloomFilterContainsAsync("bf", "x");

        await session.Expect("BF.EXISTS", "bf", "x");
        await session.Reply(":1\r\n");
        Assert.True(await exists);

        ValueTask<string?> xaddMissing = session.Client.AddStreamEntryIfExistsAsync("s", "*", ("f", "1"));

        await session.Expect("XADD", "s", "NOMKSTREAM", "*", "f", "1");
        await session.Reply("$-1\r\n");
        Assert.Null(await xaddMissing);

        ValueTask<long?> taken = session.Client.TakeAsync("seats", 1);

        await session.Expect("TAKE", "seats", "1");
        await session.Reply(":4\r\n");
        Assert.Equal(4, await taken);

        ValueTask<(long Source, long Destination)?> moved = session.Client.TransferAsync("a", "b", 4);

        await session.Expect("TRANSFER", "a", "b", "4");
        await session.Reply("*2\r\n:6\r\n:4\r\n");
        Assert.Equal((6L, 4L), await moved);

        ValueTask<(long Source, long Destination)?> capped = session.Client.TransferAsync("a", "b", 4, 9);

        await session.Expect("TRANSFER", "a", "b", "4", "MAX", "9");
        await session.Reply("$-1\r\n");
        Assert.Null(await capped);

        ValueTask<(long From, long To)?> fields = session.Client.TransferHashFieldAsync("h", "f", "g", 1, 5);

        await session.Expect("HTRANSFER", "h", "f", "g", "1", "MAX", "5");
        await session.Reply("*2\r\n:1\r\n:2\r\n");
        Assert.Equal((1L, 2L), await fields);

        ValueTask<string?> slid = session.Client.GetExpireAsync("session", 5000);

        await session.Expect("GETEX", "session", "5000");
        await session.Reply("$2\r\nhi\r\n");
        Assert.Equal("hi", await slid);

        ValueTask<long?> limited = session.Client.LimitAsync("login", 5, 60000);

        await session.Expect("LIMIT", "login", "5", "60000");
        await session.Reply(":4\r\n");
        Assert.Equal(4, await limited);

        ValueTask<long?> sliding = session.Client.LimitSlidingAsync("login", 5, 60000);

        await session.Expect("LIMIT", "login", "5", "60000", "SLIDING");
        await session.Reply(":3\r\n");
        Assert.Equal(3, await sliding);

        ValueTask<long?> tokens = session.Client.LimitTokensAsync("api", 10, 2000, 2);

        await session.Expect("LIMIT", "api", "10", "2000", "TOKENS", "COST", "2");
        await session.Reply("$-1\r\n");
        Assert.Null(await tokens);

        ValueTask<LimitOutcome> slidingRetry = session.Client.LimitSlidingWithRetryAsync("login", 5, 60000);

        await session.Expect("LIMIT", "login", "5", "60000", "SLIDING", "WITHRETRY");
        await session.Reply("*2\r\n:2\r\n:0\r\n");
        Assert.Equal(new LimitOutcome(true, 2, 0), await slidingRetry);

        ValueTask<LimitOutcome> tokensRetry = session.Client.LimitTokensWithRetryAsync("api", 10, 2000);

        await session.Expect("LIMIT", "api", "10", "2000", "TOKENS", "COST", "1", "WITHRETRY");
        await session.Reply("*2\r\n:-1\r\n:250\r\n");
        Assert.Equal(new LimitOutcome(false, 0, 250), await tokensRetry);

        ValueTask<long> once = session.Client.OnceAsync("pay", 60000);

        await session.Expect("ONCE", "pay", "60000");
        await session.Reply(":1\r\n");
        Assert.Equal(1, await once);

        ValueTask<long?> lease = session.Client.LeaseAsync("lock", 5000);

        await session.Expect("LEASE", "lock", "5000");
        await session.Reply(":1\r\n");
        Assert.Equal(1, await lease);

        ValueTask<long> setv = session.Client.SetVersionAsync("doc", "12");

        await session.Expect("SETV", "doc", "12");
        await session.Reply(":1\r\n");
        Assert.Equal(1, await setv);

        ValueTask<long> changes = session.Client.StartChangesAsync();

        await session.Expect("CHANGES", "START");
        await session.Reply(":0\r\n");
        Assert.Equal(0, await changes);

        ValueTask<IReadOnlyList<ChangeNote>> notes = session.Client.ReadChangesAsync(0);

        await session.Expect("CHANGES", "0");
        await session.Reply("*1\r\n*2\r\n:7\r\n$8\r\nSET note\r\n");
        ChangeNote note = Assert.Single(await notes);

        Assert.Equal(7, note.Sequence);
        Assert.Equal("SET note", note.Text);

        ValueTask<string> delayed = session.Client.AddStreamEntryDelayedAsync("s", 5000, "*", ("f", "1"));

        await session.Expect("XADD", "s", "DELAY", "5000", "*", "f", "1");
        await session.Reply("$3\r\n1-0\r\n");
        Assert.Equal("1-0", await delayed);

        ValueTask<string> xadd = session.Client.AddStreamEntryAsync("s", "*", ("f", "1"));

        await session.Expect("XADD", "s", "*", "f", "1");
        await session.Reply("$3\r\n1-0\r\n");
        Assert.Equal("1-0", await xadd);

        ValueTask<long> xlen = session.Client.GetStreamLengthAsync("s");

        await session.Expect("XLEN", "s");
        await session.Reply(":1\r\n");
        Assert.Equal(1, await xlen);

        ValueTask<RespValue> xinfo = session.Client.GetStreamInfoAsync("s");

        await session.Expect("XINFO", "STREAM", "s");
        await session.Reply("*2\r\n$6\r\nlength\r\n:1\r\n");
        Assert.Equal(2, (await xinfo).Items!.Count);

        ValueTask<RespValue> groups = session.Client.GetStreamGroupsAsync("s");

        await session.Expect("XINFO", "GROUPS", "s");
        await session.Reply("*0\r\n");
        Assert.Empty((await groups).Items!);

        ValueTask<RespValue> consumers = session.Client.GetStreamConsumersAsync("s", "workers");

        await session.Expect("XINFO", "CONSUMERS", "s", "workers");
        await session.Reply("*0\r\n");
        Assert.Empty((await consumers).Items!);

        ValueTask setId = session.Client.SetStreamIdAsync("s", "2-0");

        await session.Expect("XSETID", "s", "2-0");
        await session.Reply("+OK\r\n");
        await setId;

        ValueTask<RespValue> autoclaim = session.Client.AutoClaimStreamAsync("s", "g", "c2", 400, "0-0");

        await session.Expect("XAUTOCLAIM", "s", "g", "c2", "400", "0-0");
        await session.Reply("*3\r\n$3\r\n0-0\r\n*0\r\n*0\r\n");
        Assert.Equal(3, (await autoclaim).Items!.Count);

        ValueTask<IReadOnlyList<StreamEntry>> xrange = session.Client.GetStreamRangeAsync("s", "-", "+");

        await session.Expect("XRANGE", "s", "-", "+");
        await session.Reply("*1\r\n*2\r\n$3\r\n1-0\r\n*2\r\n$1\r\nf\r\n$1\r\n1\r\n");
        Assert.Equal("1-0", (await xrange)[0].Id);

        ValueTask<IReadOnlyList<StreamEntry>> xrev = session.Client.GetStreamRangeAsync("s", "+", "-", reverse: true);

        await session.Expect("XREVRANGE", "s", "+", "-");
        await session.Reply("*0\r\n");
        Assert.Empty(await xrev);

        ValueTask<long> xdel = session.Client.DeleteStreamEntriesAsync("s", "1-0");

        await session.Expect("XDEL", "s", "1-0");
        await session.Reply(":1\r\n");
        Assert.Equal(1, await xdel);

        ValueTask<long> xtrim = session.Client.TrimStreamAsync("s", 10);

        await session.Expect("XTRIM", "s", "MAXLEN", "10");
        await session.Reply(":0\r\n");
        Assert.Equal(0, await xtrim);

        ValueTask<long> xminid = session.Client.TrimStreamMinIdAsync("s", "2-0");

        await session.Expect("XTRIM", "s", "MINID", "2-0");
        await session.Reply(":1\r\n");
        Assert.Equal(1, await xminid);

        ValueTask<string> info = session.Client.GetInfoAsync();

        await session.Expect("INFO");
        await session.Reply("$4\r\ninfo\r\n");
        Assert.Equal("info", await info);

        ValueTask save = session.Client.SaveAsync();

        await session.Expect("SAVE");
        await session.Reply("+OK\r\n");
        await save;

        ValueTask bgsave = session.Client.SaveInBackgroundAsync();

        await session.Expect("BGSAVE");
        await session.Reply("+OK\r\n");
        await bgsave;

        ValueTask<long> pub = session.Client.PublishAsync("c", "m");

        await session.Expect("PUBLISH", "c", "m");
        await session.Reply(":0\r\n");
        Assert.Equal(0, await pub);

        ValueTask<long> spub = session.Client.PublishShardedAsync("c", "m");

        await session.Expect("SPUBLISH", "c", "m");
        await session.Reply(":0\r\n");
        Assert.Equal(0, await spub);
    }

    [Fact(Timeout = 15000)]
    public async Task StreamGroupsScriptsAclAndClusterUseTheSupportedNames()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<IReadOnlyList<StreamReadResult>> xread = session.Client.GetStreamUpdatesAsync(("s", "0-0"));

        await session.Expect("XREAD", "STREAMS", "s", "0-0");
        await session.Reply("*1\r\n*2\r\n$1\r\ns\r\n*1\r\n*2\r\n$3\r\n1-0\r\n*0\r\n");
        Assert.Equal("s", (await xread)[0].Key);

        ValueTask create = session.Client.CreateStreamGroupAsync("s", "g", "$");

        await session.Expect("XGROUP", "CREATE", "s", "g", "$");
        await session.Reply("+OK\r\n");
        await create;

        ValueTask<long> createConsumer = session.Client.CreateStreamConsumerAsync("s", "g", "c");

        await session.Expect("XGROUP", "CREATECONSUMER", "s", "g", "c");
        await session.Reply(":1\r\n");
        Assert.Equal(1, await createConsumer);

        ValueTask<IReadOnlyList<StreamReadResult>> xrg = session.Client.GetStreamGroupUpdatesAsync("g", "c", "s", ">");

        await session.Expect("XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">");
        await session.Reply("*0\r\n");
        Assert.Empty(await xrg);

        ValueTask<long> destroy = session.Client.DestroyStreamGroupAsync("s", "g");

        await session.Expect("XGROUP", "DESTROY", "s", "g");
        await session.Reply(":1\r\n");
        Assert.Equal(1, await destroy);

        ValueTask setid = session.Client.SetStreamGroupIdAsync("s", "g", "0-0");

        await session.Expect("XGROUP", "SETID", "s", "g", "0-0");
        await session.Reply("+OK\r\n");
        await setid;

        ValueTask<long> deleteConsumer = session.Client.DeleteStreamGroupConsumerAsync("s", "g", "c");

        await session.Expect("XGROUP", "DELCONSUMER", "s", "g", "c");
        await session.Reply(":0\r\n");
        Assert.Equal(0L, await deleteConsumer);

        ValueTask<long> ack = session.Client.AcknowledgeStreamAsync("s", "g", "1-0");

        await session.Expect("XACK", "s", "g", "1-0");
        await session.Reply(":1\r\n");
        Assert.Equal(1, await ack);

        ValueTask<RespValue> pending = session.Client.GetStreamPendingAsync("s", "g");

        await session.Expect("XPENDING", "s", "g");
        await session.Reply("*4\r\n:0\r\n$-1\r\n$-1\r\n*0\r\n");
        Assert.Equal(RespKind.Array, (await pending).Kind);

        ValueTask<IReadOnlyList<StreamEntry>> claim = session.Client.ClaimStreamEntriesAsync("s", "g", "c", 0, "1-0");

        await session.Expect("XCLAIM", "s", "g", "c", "0", "1-0");
        await session.Reply("*0\r\n");
        Assert.Empty(await claim);

        ValueTask<RespValue> eval = session.Client.EvaluateAsync("return 1", 0);

        await session.Expect("EVAL", "return 1", "0");
        await session.Reply(":1\r\n");
        Assert.Equal(1, (await eval).Integer);

        ValueTask multi = session.Client.BeginTransactionAsync();

        await session.Expect("MULTI");
        await session.Reply("+OK\r\n");
        await multi;

        ValueTask<IReadOnlyList<RespValue>> exec = session.Client.ExecuteTransactionAsync();

        await session.Expect("EXEC");
        await session.Reply("*0\r\n");
        Assert.Empty(await exec);

        ValueTask discard = session.Client.DiscardTransactionAsync();

        await session.Expect("DISCARD");
        await session.Reply("+OK\r\n");
        await discard;

        ValueTask watch = session.Client.WatchAsync("a");

        await session.Expect("WATCH", "a");
        await session.Reply("+OK\r\n");
        await watch;

        ValueTask unwatch = session.Client.UnwatchAsync();

        await session.Expect("UNWATCH");
        await session.Reply("+OK\r\n");
        await unwatch;

        ValueTask<string> who = session.Client.GetAclUserNameAsync();

        await session.Expect("ACL", "WHOAMI");
        await session.Reply("$7\r\ndefault\r\n");
        Assert.Equal("default", await who);

        ValueTask<RespValue> slots = session.Client.GetClusterSlotsAsync();

        await session.Expect("CLUSTER", "SLOTS");
        await session.Reply("*0\r\n");
        Assert.Equal(RespKind.Array, (await slots).Kind);

        ValueTask<string> nodes = session.Client.GetClusterNodesAsync();

        await session.Expect("CLUSTER", "NODES");
        await session.Reply("$1\r\nn\r\n");
        Assert.Equal("n", await nodes);

        ValueTask<RespValue> shards = session.Client.GetClusterShardsAsync();

        await session.Expect("CLUSTER", "SHARDS");
        await session.Reply("*0\r\n");
        Assert.Equal(RespKind.Array, (await shards).Kind);

        ValueTask<string> myid = session.Client.GetClusterMyIdAsync();

        await session.Expect("CLUSTER", "MYID");
        await session.Reply("$4\r\nnode\r\n");
        Assert.Equal("node", await myid);

        ValueTask<string> loaded = session.Client.LoadScriptAsync("return 1");

        await session.Expect("SCRIPT", "LOAD", "return 1");
        await session.Reply("$6\r\nabcdef\r\n");
        Assert.Equal("abcdef", await loaded);

        ValueTask<RespValue> evalSha = session.Client.EvaluateShaAsync("abcdef", 0);

        await session.Expect("EVALSHA", "abcdef", "0");
        await session.Reply(":1\r\n");
        Assert.Equal(1, (await evalSha).Integer);

        ValueTask<IReadOnlyList<bool>> exists = session.Client.ScriptExistsAsync("abcdef");

        await session.Expect("SCRIPT", "EXISTS", "abcdef");
        await session.Reply("*1\r\n:1\r\n");
        Assert.True((await exists)[0]);

        ValueTask flush = session.Client.FlushScriptsAsync();

        await session.Expect("SCRIPT", "FLUSH");
        await session.Reply("+OK\r\n");
        await flush;

        ValueTask kill = session.Client.KillScriptAsync();

        await session.Expect("SCRIPT", "KILL");
        await session.Reply("+OK\r\n");
        await kill;

        ValueTask<RespValue> fcall = session.Client.CallFunctionAsync("fn", 0);

        await session.Expect("FCALL", "fn", "0");
        await session.Reply(":1\r\n");
        Assert.Equal(1, (await fcall).Integer);

        ValueTask loadFn = session.Client.LoadFunctionAsync("fn", "return 1");

        await session.Expect("FUNCTION", "LOAD", "fn", "return 1");
        await session.Reply("+OK\r\n");
        await loadFn;

        ValueTask<RespValue> listFn = session.Client.GetFunctionListAsync();

        await session.Expect("FUNCTION", "LIST");
        await session.Reply("*0\r\n");
        Assert.Equal(RespKind.Array, (await listFn).Kind);

        ValueTask<bool> deleteFn = session.Client.DeleteFunctionAsync("fn");

        await session.Expect("FUNCTION", "DELETE", "fn");
        await session.Reply(":1\r\n");
        Assert.True(await deleteFn);

        ValueTask<IReadOnlyList<string>> users = session.Client.GetAclUsersAsync();

        await session.Expect("ACL", "USERS");
        await session.Reply("*1\r\n$7\r\ndefault\r\n");
        Assert.Equal(["default"], await users);

        ValueTask<string?> name = session.Client.GetConnectionNameAsync();

        await session.Expect("CLIENT", "GETNAME");
        await session.Reply("$-1\r\n");
        Assert.Null(await name);

        ValueTask setName = session.Client.SetConnectionNameAsync("app");

        await session.Expect("CLIENT", "SETNAME", "app");
        await session.Reply("+OK\r\n");
        await setName;

        ValueTask tracking = session.Client.SetTrackingAsync(true);

        await session.Expect("CLIENT", "TRACKING", "ON");
        await session.Reply("+OK\r\n");
        await tracking;

        ValueTask durability = session.Client.SetDurabilityAsync(2);

        await session.Expect("CLIENT", "DURABILITY", "REPLICAS", "2");
        await session.Reply("+OK\r\n");
        await durability;

        ValueTask durabilityWithTimeout = session.Client.SetDurabilityAsync(1, TimeSpan.FromMilliseconds(250));

        await session.Expect("CLIENT", "DURABILITY", "REPLICAS", "1", "TIMEOUT", "250");
        await session.Reply("+OK\r\n");
        await durabilityWithTimeout;

        ValueTask<RespValue> config = session.Client.GetConfigAsync("requirepass");

        await session.Expect("CONFIG", "GET", "requirepass");
        await session.Reply("*2\r\n$11\r\nrequirepass\r\n$0\r\n\r\n");
        Assert.Equal(RespKind.Array, (await config).Kind);

        ValueTask setConfig = session.Client.SetConfigAsync([("maxmemory", "100"), ("slowlog-max-len", "5")]);

        await session.Expect("CONFIG", "SET", "maxmemory", "100", "slowlog-max-len", "5");
        await session.Reply("+OK\r\n");
        await setConfig;

        ValueTask rewrite = session.Client.RewriteConfigAsync();

        await session.Expect("CONFIG", "REWRITE");
        await session.Reply("+OK\r\n");
        await rewrite;
    }

    [Fact(Timeout = 15000)]
    public async Task RemainingAclHelloAndPubsubCommandsUseTheSupportedNames()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<RespValue> hello = session.Client.HelloAsync(2);

        await session.Expect("HELLO", "2");
        await session.Reply("*0\r\n");
        Assert.Equal(RespKind.Array, (await hello).Kind);

        ValueTask<IReadOnlyList<string>> list = session.Client.GetAclListAsync();

        await session.Expect("ACL", "LIST");
        await session.Reply("*1\r\n$7\r\ndefault\r\n");
        Assert.Equal(["default"], await list);

        ValueTask<RespValue> user = session.Client.GetAclUserAsync("default");

        await session.Expect("ACL", "GETUSER", "default");
        await session.Reply("*0\r\n");
        Assert.Equal(RespKind.Array, (await user).Kind);

        ValueTask<IReadOnlyList<string>> cats = session.Client.GetAclCategoriesAsync();

        await session.Expect("ACL", "CAT");
        await session.Reply("*1\r\n$6\r\nstring\r\n");
        Assert.Equal(["string"], await cats);

        ValueTask setUser = session.Client.SetAclUserAsync("app", "on", ">secret", "+@all", "~*");

        await session.Expect("ACL", "SETUSER", "app", "on", ">secret", "+@all", "~*");
        await session.Reply("+OK\r\n");
        await setUser;

        ValueTask<IReadOnlyList<RespValue>> sub = session.Client.SubscribeAsync("news");

        await using SubscriberPipe subscriber = await session.AcceptSubscriberAsync();
        await subscriber.Expect("SUBSCRIBE", "news");
        await subscriber.Reply("*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n");
        Assert.Equal("subscribe", (await sub)[0].Items![0].AsString());

        ValueTask<RespValue> raw = session.Client.ReadMessageAsync();

        await subscriber.Reply("*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$2\r\nhi\r\n");
        RespValue message = await raw;

        Assert.Null(message.AsString());
        Assert.Equal("hi", message.Items![2].AsString());
        Assert.True(PubSubMessage.TryParse(message, out PubSubMessage parsed));
        Assert.Equal("hi", parsed.Payload);

        ValueTask<IReadOnlyList<RespValue>> unsub = session.Client.UnsubscribeAsync("news");

        await subscriber.Expect("UNSUBSCRIBE", "news");
        await subscriber.Reply("*3\r\n$11\r\nunsubscribe\r\n$4\r\nnews\r\n:0\r\n");
        Assert.Equal(0, (await unsub)[0].Items![2].Integer);

        ValueTask<IReadOnlyList<RespValue>> psub = session.Client.SubscribePatternsAsync("n*");

        await subscriber.Expect("PSUBSCRIBE", "n*");
        await subscriber.Reply("*3\r\n$10\r\npsubscribe\r\n$2\r\nn*\r\n:1\r\n");
        Assert.Equal("psubscribe", (await psub)[0].Items![0].AsString());

        ValueTask<IReadOnlyList<RespValue>> punsub = session.Client.UnsubscribePatternsAsync("n*");

        await subscriber.Expect("PUNSUBSCRIBE", "n*");
        await subscriber.Reply("*3\r\n$12\r\npunsubscribe\r\n$2\r\nn*\r\n:0\r\n");
        Assert.Equal(0, (await punsub)[0].Items![2].Integer);

        ValueTask<IReadOnlyList<RespValue>> ssub = session.Client.SubscribeShardedAsync("slot");

        await subscriber.Expect("SSUBSCRIBE", "slot");
        await subscriber.Reply("*3\r\n$10\r\nssubscribe\r\n$4\r\nslot\r\n:1\r\n");
        Assert.Equal("ssubscribe", (await ssub)[0].Items![0].AsString());

        ValueTask<IReadOnlyList<RespValue>> sunsub = session.Client.UnsubscribeShardedAsync("slot");

        await subscriber.Expect("SUNSUBSCRIBE", "slot");
        await subscriber.Reply("*3\r\n$12\r\nsunsubscribe\r\n$4\r\nslot\r\n:0\r\n");
        Assert.Equal(0, (await sunsub)[0].Items![2].Integer);
    }

    [Fact(Timeout = 15000)]
    public async Task ConnectionCreatesASeparateClientForEachDatabase()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        Task<RuvioClient> created = session.Connection.GetDatabaseAsync(1);

        await using SubscriberPipe database = await session.AcceptDatabaseAsync();
        await database.Expect("SELECT", "1");
        await database.Reply("+OK\r\n");
        RuvioClient db1 = await created;

        Assert.Equal(1, db1.Database);
        Assert.NotSame(session.Client, db1);
        Assert.Same(session.Client, await session.Connection.GetDatabaseAsync(0));
    }

    [Fact(Timeout = 15000)]
    public async Task DatabaseWideCommandsSendTheirVerbs()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask flush = session.Client.FlushDatabaseAsync();

        await session.Expect("FLUSHDB");
        await session.Reply("+OK\r\n");
        await flush;

        ValueTask flushAll = session.Client.FlushAllDatabasesAsync();

        await session.Expect("FLUSHALL");
        await session.Reply("+OK\r\n");
        await flushAll;

        ValueTask swap = session.Client.SwapDatabasesAsync(0, 1);

        await session.Expect("SWAPDB", "0", "1");
        await session.Reply("+OK\r\n");
        await swap;

        ValueTask<bool> moved = session.Client.MoveKeyAsync("k", 2);

        await session.Expect("MOVE", "k", "2");
        await session.Reply(":1\r\n");
        Assert.True(await moved);

        ValueTask<bool> blocked = session.Client.MoveKeyAsync("k", 3);

        await session.Expect("MOVE", "k", "3");
        await session.Reply(":0\r\n");
        Assert.False(await blocked);
    }

    [Fact(Timeout = 15000)]
    public async Task SetOptionsSendPxatKeepttlAndGet()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        DateTimeOffset at = DateTimeOffset.FromUnixTimeMilliseconds(1_900_000_000_123);
        ValueTask<bool> absolute = session.Client.SetAsync("k", "v", at, SetMode.IfExists);

        await session.Expect("SET", "k", "v", "PXAT", "1900000000123", "XX");
        await session.Reply("+OK\r\n");
        Assert.True(await absolute);

        ValueTask<bool> keep = session.Client.SetKeepingTimeToLiveAsync("k", "w");

        await session.Expect("SET", "k", "w", "KEEPTTL");
        await session.Reply("+OK\r\n");
        Assert.True(await keep);

        ValueTask<string?> previous = session.Client.SetAndGetAsync("k", "x", TimeSpan.FromSeconds(2));

        await session.Expect("SET", "k", "x", "PX", "2000", "GET");
        await session.Reply("$1\r\nw\r\n");
        Assert.Equal("w", await previous);

        ValueTask<string?> missing = session.Client.SetAndGetAsync("n", "x", mode: SetMode.IfNotExists);

        await session.Expect("SET", "n", "x", "NX", "GET");
        await session.Reply("$-1\r\n");
        Assert.Null(await missing);

        ValueTask<string> ping = session.Client.PingAsync("hello");

        await session.Expect("PING", "hello");
        await session.Reply("$5\r\nhello\r\n");
        Assert.Equal("hello", await ping);
    }

    [Fact(Timeout = 15000)]
    public async Task CountedPopsAndHashScanOptionsAreSent()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<IReadOnlyList<string>> left = session.Client.PopListLeftAsync("l", 2);

        await session.Expect("LPOP", "l", "2");
        await session.Reply("*2\r\n$1\r\na\r\n$1\r\nb\r\n");
        Assert.Equal(["a", "b"], await left);

        ValueTask<string?> right = session.Client.PopListRightAsync("l");

        await session.Expect("RPOP", "l");
        await session.Reply("$-1\r\n");
        Assert.Null(await right);

        ValueTask<IReadOnlyList<string>> popped = session.Client.PopSetMembersAsync("s", 2);

        await session.Expect("SPOP", "s", "2");
        await session.Reply("*1\r\n$1\r\nm\r\n");
        Assert.Equal(["m"], await popped);

        ValueTask<IReadOnlyList<string>> random = session.Client.GetSetRandomMembersAsync("s", -3);

        await session.Expect("SRANDMEMBER", "s", "-3");
        await session.Reply("*3\r\n$1\r\nm\r\n$1\r\nm\r\n$1\r\nm\r\n");
        Assert.Equal(3, (await random).Count);

        ValueTask<HashScanPage> page = session.Client.ScanHashAsync("h", 0, "f*", 10);

        await session.Expect("HSCAN", "h", "0", "MATCH", "f*", "COUNT", "10");
        await session.Reply("*2\r\n$1\r\n0\r\n*2\r\n$2\r\nf1\r\n$1\r\n1\r\n");
        Assert.Equal("1", (await page).Fields["f1"]);

        ValueTask<string> info = session.Client.GetInfoAsync("prometheus");

        await session.Expect("INFO", "prometheus");
        await session.Reply("$3\r\nx 1\r\n");
        Assert.Equal("x 1", await info);
    }

    [Fact(Timeout = 15000)]
    public async Task SortedSetAddTakesTuplesAndASingleMember()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<long> pairs = session.Client.AddSortedSetAsync("board", ("ada", 100), ("bob", 50.5));

        await session.Expect("ZADD", "board", "100", "ada", "50.5", "bob");
        await session.Reply(":2\r\n");
        Assert.Equal(2, await pairs);

        ValueTask<long> flagged = session.Client.AddSortedSetAsync("board", SortedSetAddFlags.GreaterThan, ("ada", 120));

        await session.Expect("ZADD", "board", "GT", "120", "ada");
        await session.Reply(":0\r\n");
        Assert.Equal(0, await flagged);

        ValueTask<bool> one = session.Client.AddSortedSetAsync("board", "cem", 75);

        await session.Expect("ZADD", "board", "75", "cem");
        await session.Reply(":1\r\n");
        Assert.True(await one);
    }

    [Fact(Timeout = 15000)]
    public async Task SortedSetAddFlagsAreSentInOrder()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<long> changed = session.Client.AddSortedSetAsync(
            "z",
            SortedSetAddFlags.IfExists | SortedSetAddFlags.GreaterThan | SortedSetAddFlags.Changed,
            new SortedSetEntry("a", 5));

        await session.Expect("ZADD", "z", "XX", "GT", "CH", "5", "a");
        await session.Reply(":1\r\n");
        Assert.Equal(1, await changed);

        ValueTask<double?> incremented = session.Client.IncrementSortedSetAsync("z", "a", 2, SortedSetAddFlags.LessThan);

        await session.Expect("ZADD", "z", "LT", "INCR", "2", "a");
        await session.Reply("$-1\r\n");
        Assert.Null(await incremented);
    }

    [Fact(Timeout = 15000)]
    public async Task StreamOptionsAreSent()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<string> added = session.Client.AddStreamEntryAsync("s", "*", 100, ("f", "v"));

        await session.Expect("XADD", "s", "MAXLEN", "100", "*", "f", "v");
        await session.Reply("$3\r\n1-0\r\n");
        Assert.Equal("1-0", await added);

        ValueTask<IReadOnlyList<StreamEntry>> range = session.Client.GetStreamRangeAsync("s", "+", "-", 1, reverse: true);

        await session.Expect("XREVRANGE", "s", "+", "-", "COUNT", "1");
        await session.Reply("*0\r\n");
        Assert.Empty(await range);

        ValueTask<IReadOnlyList<StreamReadResult>> read = session.Client.GetStreamUpdatesAsync(
            new StreamReadOptions { Count = 5, Block = TimeSpan.FromMilliseconds(250) },
            ("s", "$"));

        await session.Expect("XREAD", "COUNT", "5", "BLOCK", "250", "STREAMS", "s", "$");
        await session.Reply("*-1\r\n");
        Assert.Empty(await read);

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await session.Client.GetStreamUpdatesAsync(new StreamReadOptions { NoAcknowledge = true }, ("s", "$")));

        ValueTask create = session.Client.CreateStreamGroupAsync("s", "g", "$", createStream: true);

        await session.Expect("XGROUP", "CREATE", "s", "g", "$", "MKSTREAM");
        await session.Reply("+OK\r\n");
        await create;

        ValueTask<IReadOnlyList<StreamReadResult>> group = session.Client.GetStreamGroupUpdatesAsync(
            "g",
            "c",
            "s",
            ">",
            new StreamReadOptions { Count = 1, Block = TimeSpan.Zero, NoAcknowledge = true });

        await session.Expect("XREADGROUP", "GROUP", "g", "c", "COUNT", "1", "BLOCK", "0", "NOACK", "STREAMS", "s", ">");
        await session.Reply("*-1\r\n");
        Assert.Empty(await group);

        ValueTask<IReadOnlyList<StreamPendingEntry>> pending = session.Client.GetStreamPendingAsync(
            "s", "g", "-", "+", 10, consumer: "c", minIdle: TimeSpan.FromSeconds(1));

        await session.Expect("XPENDING", "s", "g", "IDLE", "1000", "-", "+", "10", "c");
        await session.Reply("*1\r\n*4\r\n$3\r\n1-0\r\n$1\r\nc\r\n:1500\r\n:2\r\n");
        Assert.Equal(new StreamPendingEntry("1-0", "c", 1500, 2), (await pending)[0]);

        ValueTask<IReadOnlyList<StreamEntry>> claimed = session.Client.ClaimStreamEntriesAsync(
            "s",
            "g",
            "c",
            0,
            new StreamClaimOptions { Idle = TimeSpan.Zero, RetryCount = 3, Force = true, LastId = "5-0" },
            "1-0");

        await session.Expect("XCLAIM", "s", "g", "c", "0", "1-0", "IDLE", "0", "RETRYCOUNT", "3", "FORCE", "LASTID", "5-0");
        await session.Reply("*0\r\n");
        Assert.Empty(await claimed);

        ValueTask<IReadOnlyList<string>> ids = session.Client.ClaimStreamEntryIdsAsync("s", "g", "c", 0, null, "1-0");

        await session.Expect("XCLAIM", "s", "g", "c", "0", "1-0", "JUSTID");
        await session.Reply("*1\r\n$3\r\n1-0\r\n");
        Assert.Equal(["1-0"], await ids);

        await Assert.ThrowsAsync<ArgumentException>(async () => await session.Client.ClaimStreamEntriesAsync(
            "s",
            "g",
            "c",
            0,
            new StreamClaimOptions { Idle = TimeSpan.Zero, Time = DateTimeOffset.UnixEpoch },
            "1-0"));
    }

    [Fact]
    public void GeoHelpersArePartOfThePublicClient()
    {
        Assert.NotNull(typeof(RuvioClient).GetMethods().FirstOrDefault(m => m.Name == nameof(RuvioClient.AddGeoAsync)));
        Assert.NotNull(typeof(RuvioClient).GetMethods().FirstOrDefault(m => m.Name == nameof(RuvioClient.GeoDistanceAsync)));
        Assert.NotNull(typeof(RuvioClient).GetMethods().FirstOrDefault(m => m.Name == nameof(RuvioClient.GeoHashAsync)));
        Assert.NotNull(typeof(RuvioClient).GetMethods().FirstOrDefault(m => m.Name == nameof(RuvioClient.GeoPositionAsync)));
        Assert.NotNull(typeof(RuvioClient).GetMethods().FirstOrDefault(m => m.Name == nameof(RuvioClient.GeoSearchAsync)));
        Assert.NotNull(typeof(RuvioClient).GetMethods().FirstOrDefault(m => m.Name == nameof(RuvioClient.GeoSearchStoreAsync)));
        Assert.NotNull(typeof(RuvioClient).GetMethods().FirstOrDefault(m => m.Name == nameof(RuvioClient.GeoRadiusAsync)));
        Assert.NotNull(typeof(RuvioClient).GetMethods().FirstOrDefault(m => m.Name == nameof(RuvioClient.GeoRadiusByMemberAsync)));
    }

    [Fact(Timeout = 15000)]
    public async Task HashFieldTtlMethodsSendTheFieldsBlock()
    {
        await using ScriptedSession session = await ScriptedSession.Open();

        ValueTask<IReadOnlyList<long>> expire = session.Client.ExpireHashFieldsAsync("h", TimeSpan.FromSeconds(60), "a", "x");

        await session.Expect("HEXPIRE", "h", "60", "FIELDS", "2", "a", "x");
        await session.Reply("*2\r\n:1\r\n:-2\r\n");
        Assert.Equal([1L, -2L], await expire);

        ValueTask<IReadOnlyList<long>> gt = session.Client.ExpireHashFieldsAsync(
            "h", TimeSpan.FromMilliseconds(1500), FieldExpireCondition.IfGreater, "a");

        await session.Expect("HPEXPIRE", "h", "1500", "GT", "FIELDS", "1", "a");
        await session.Reply("*1\r\n:0\r\n");
        Assert.Equal([0L], await gt);

        ValueTask<IReadOnlyList<long>> at = session.Client.ExpireHashFieldsAtAsync(
            "h", DateTimeOffset.FromUnixTimeSeconds(4_102_444_800), "a");

        await session.Expect("HEXPIREAT", "h", "4102444800", "FIELDS", "1", "a");
        await session.Reply("*1\r\n:1\r\n");
        Assert.Equal([1L], await at);

        ValueTask<IReadOnlyList<long>> pat = session.Client.ExpireHashFieldsAtAsync(
            "h", DateTimeOffset.FromUnixTimeMilliseconds(4_102_444_800_250), "a");

        await session.Expect("HPEXPIREAT", "h", "4102444800250", "FIELDS", "1", "a");
        await session.Reply("*1\r\n:1\r\n");
        Assert.Equal([1L], await pat);

        ValueTask<IReadOnlyList<long>> ttl = session.Client.GetHashFieldTimeToLiveAsync("h", "a", "b");

        await session.Expect("HTTL", "h", "FIELDS", "2", "a", "b");
        await session.Reply("*2\r\n:59\r\n:-1\r\n");
        Assert.Equal([59L, -1L], await ttl);

        ValueTask<IReadOnlyList<long>> pttl = session.Client.GetHashFieldTimeToLiveMillisecondsAsync("h", "a");

        await session.Expect("HPTTL", "h", "FIELDS", "1", "a");
        await session.Reply("*1\r\n:59000\r\n");
        Assert.Equal([59000L], await pttl);

        ValueTask<IReadOnlyList<long>> time = session.Client.GetHashFieldExpireTimeAsync("h", "a");

        await session.Expect("HEXPIRETIME", "h", "FIELDS", "1", "a");
        await session.Reply("*1\r\n:4102444800\r\n");
        Assert.Equal([4_102_444_800L], await time);

        ValueTask<IReadOnlyList<long>> ptime = session.Client.GetHashFieldExpireTimeMillisecondsAsync("h", "a");

        await session.Expect("HPEXPIRETIME", "h", "FIELDS", "1", "a");
        await session.Reply("*1\r\n:4102444800250\r\n");
        Assert.Equal([4_102_444_800_250L], await ptime);

        ValueTask<IReadOnlyList<long>> persist = session.Client.PersistHashFieldsAsync("h", "a", "x");

        await session.Expect("HPERSIST", "h", "FIELDS", "2", "a", "x");
        await session.Reply("*2\r\n:1\r\n:-2\r\n");
        Assert.Equal([1L, -2L], await persist);
    }

    [Fact(Timeout = 15000)]
    public async Task DueSortedSetCommandsSendTheirOptions()
    {
        await using ScriptedSession session = await ScriptedSession.Open();

        ValueTask<IReadOnlyList<SortedSetEntry>> due = session.Client.PopSortedSetByScoreAsync("z", "-inf", "(5", 3);

        await session.Expect("ZPOPBYSCORE", "z", "(5", "MIN", "-inf", "COUNT", "3", "WITHSCORES");
        await session.Reply("*2\r\n$1\r\nm\r\n$1\r\n2\r\n");
        Assert.Equal("m", (await due)[0].Member);

        ValueTask<long> moved = session.Client.MoveDueSortedSetAsync("{t}z", "{t}l", "+inf", 4);

        await session.Expect("ZMOVEDUE", "{t}z", "{t}l", "+inf", "COUNT", "4");
        await session.Reply(":2\r\n");
        Assert.Equal(2, await moved);
    }

    [Fact(Timeout = 15000)]
    public async Task SortedSetPopsAndRangeRemovalsSendTheirCommands()
    {
        await using ScriptedSession session = await ScriptedSession.Open();

        ValueTask<IReadOnlyList<SortedSetEntry>> min = session.Client.PopSortedSetMinAsync("z");

        await session.Expect("ZPOPMIN", "z", "1");
        await session.Reply("*2\r\n$1\r\na\r\n$1\r\n1\r\n");
        Assert.Equal([new SortedSetEntry("a", 1)], await min);

        ValueTask<IReadOnlyList<SortedSetEntry>> max = session.Client.PopSortedSetMaxAsync("z", 2);

        await session.Expect("ZPOPMAX", "z", "2");
        await session.Reply("*0\r\n");
        Assert.Empty(await max);

        ValueTask<(string Key, SortedSetEntry Entry)?> bmin = session.Client.PopSortedSetMinBlockingAsync("z", TimeSpan.FromSeconds(1));

        await session.Expect("BZPOPMIN", "z", "1");
        await session.Reply("*3\r\n$1\r\nz\r\n$1\r\nb\r\n$1\r\n2\r\n");
        Assert.Equal(("z", new SortedSetEntry("b", 2)), await bmin);

        ValueTask<(string Key, SortedSetEntry Entry)?> bmax = session.Client.PopSortedSetMaxBlockingAsync(["q", "z"], TimeSpan.FromSeconds(1));

        await session.Expect("BZPOPMAX", "q", "z", "1");
        await session.Reply("*-1\r\n");
        Assert.Null(await bmax);

        ValueTask<(string Key, SortedSetEntry Entry)?> bmaxOne = session.Client.PopSortedSetMaxBlockingAsync("z", TimeSpan.FromSeconds(2));

        await session.Expect("BZPOPMAX", "z", "2");
        await session.Reply("*3\r\n$1\r\nz\r\n$1\r\nc\r\n$1\r\n3\r\n");
        Assert.Equal(("z", new SortedSetEntry("c", 3)), await bmaxOne);

        ValueTask<(string Key, SortedSetEntry Entry)?> bminMany = session.Client.PopSortedSetMinBlockingAsync(["q", "z"], TimeSpan.FromSeconds(1));

        await session.Expect("BZPOPMIN", "q", "z", "1");
        await session.Reply("*-1\r\n");
        Assert.Null(await bminMany);

        ValueTask<long> rank = session.Client.RemoveSortedSetRangeByRankAsync("z", 0, -11);

        await session.Expect("ZREMRANGEBYRANK", "z", "0", "-11");
        await session.Reply(":3\r\n");
        Assert.Equal(3, await rank);

        ValueTask<long> score = session.Client.RemoveSortedSetRangeByScoreAsync("z", "-inf", "(5");

        await session.Expect("ZREMRANGEBYSCORE", "z", "-inf", "(5");
        await session.Reply(":2\r\n");
        Assert.Equal(2, await score);

        ValueTask<long> lex = session.Client.RemoveSortedSetRangeByLexAsync("z", "[a", "(c");

        await session.Expect("ZREMRANGEBYLEX", "z", "[a", "(c");
        await session.Reply(":1\r\n");
        Assert.Equal(1, await lex);
    }

    [Fact(Timeout = 15000)]
    public async Task ReleaseSendsReleaseWithTheToken()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<long> pending = session.Client.ReleaseAsync("lock", 7);

        await session.Expect("RELEASE", "lock", "7");
        await session.Reply(":1\r\n");
        Assert.Equal(1, await pending);
    }

    [Fact(Timeout = 15000)]
    public async Task RenewSendsRenewWithTheTokenAndTtl()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<long> pending = session.Client.RenewAsync("lock", 7, 30000);

        await session.Expect("RENEW", "lock", "7", "30000");
        await session.Reply(":1\r\n");
        Assert.Equal(1, await pending);
    }

    [Fact(Timeout = 15000)]
    public async Task SemaphoreAndLimitedIncrementSendTheirCommands()
    {
        await using ScriptedSession session = await ScriptedSession.Open();

        ValueTask<long?> permit = session.Client.AcquireSemaphoreAsync("jobs", 2, 30000);

        await session.Expect("SEMAPHORE", "jobs", "2", "30000");
        await session.Reply(":1048576000\r\n");
        Assert.Equal(1048576000L, await permit);

        ValueTask<long?> full = session.Client.AcquireSemaphoreAsync("jobs", 2, 30000);

        await session.Expect("SEMAPHORE", "jobs", "2", "30000");
        await session.Reply("$-1\r\n");
        Assert.Null(await full);

        ValueTask<long?> seats = session.Client.IncrementWithLimitAsync("seats", 3, 4);

        await session.Expect("INCRBY", "seats", "3", "MAX", "4");
        await session.Reply(":3\r\n");
        Assert.Equal(3L, await seats);

        ValueTask<long?> over = session.Client.IncrementWithLimitAsync("seats", 2, 4);

        await session.Expect("INCRBY", "seats", "2", "MAX", "4");
        await session.Reply("$-1\r\n");
        Assert.Null(await over);
    }

    public sealed record Address(string City, int Zip);

    [JsonSerializable(typeof(Address))]
    private sealed partial class AddressContext : JsonSerializerContext;

    [Fact(Timeout = 15000)]
    public async Task TypedJsonValuesAreSerializedWithTheChosenSerializer()
    {
        await using ScriptedSession session = await ScriptedSession.Open();

        ValueTask<bool> text = session.Client.JsonSetValueAsync("doc", "$.city", "izmir");

        await session.Expect("JSON.SET", "doc", "$.city", "\"izmir\"");
        await session.Reply("+OK\r\n");
        Assert.True(await text);

        ValueTask<bool> number = session.Client.JsonSetValueAsync("doc", "$.zip", 35000);

        await session.Expect("JSON.SET", "doc", "$.zip", "35000");
        await session.Reply("+OK\r\n");
        Assert.True(await number);

        JsonSerializerOptions camel = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
        ValueTask<bool> obj = session.Client.JsonSetValueAsync("doc", "$.addr", new Address("Izmir", 35000), camel);

        await session.Expect("JSON.SET", "doc", "$.addr", "{\"city\":\"Izmir\",\"zip\":35000}");
        await session.Reply("+OK\r\n");
        Assert.True(await obj);

        ValueTask<bool> generated = session.Client.JsonSetValueAsync(
            "doc", "$.addr", new Address("Izmir", 35000), AddressContext.Default.Address);

        await session.Expect("JSON.SET", "doc", "$.addr", "{\"City\":\"Izmir\",\"Zip\":35000}");
        await session.Reply("+OK\r\n");
        Assert.True(await generated);

        ValueTask<bool> merge = session.Client.JsonMergeValueAsync("doc", "$", new { age = 31, tag = (string?)null });

        await session.Expect("JSON.MERGE", "doc", "$", "{\"age\":31,\"tag\":null}");
        await session.Reply("+OK\r\n");
        Assert.True(await merge);

        ValueTask<long?> append = session.Client.JsonArrayAppendValueAsync("doc", "$.l", "a\"b");

        await session.Expect("JSON.ARRAPPEND", "doc", "$.l", "\"a\\u0022b\"");
        await session.Reply(":2\r\n");
        Assert.Equal(2L, await append);

        ValueTask<Address?> read = session.Client.JsonGetValueAsync("doc", "$.addr", AddressContext.Default.Address);

        await session.Expect("JSON.GET", "doc", "$.addr");
        await session.Reply("$28\r\n{\"City\":\"Izmir\",\"Zip\":35000}\r\n");
        Assert.Equal(new Address("Izmir", 35000), await read);

        ValueTask<string?> absent = session.Client.JsonGetValueAsync<string>("doc", "$.zz");

        await session.Expect("JSON.GET", "doc", "$.zz");
        await session.Reply("$-1\r\n");
        Assert.Null(await absent);
    }
}
