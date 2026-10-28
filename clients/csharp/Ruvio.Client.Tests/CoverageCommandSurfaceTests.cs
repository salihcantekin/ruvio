using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

/// <summary>Wire shapes of the commands added in the coverage pass, against a scripted server.</summary>
public partial class CommandSurfaceTests
{
    private static async Task<T> Exchange<T>(ScriptedSession session, ValueTask<T> call, string reply, params string[] expected)
    {
        await session.Expect(expected);
        await session.Reply(reply);

        return await call;
    }

    private static async Task Exchange(ScriptedSession session, ValueTask call, string reply, params string[] expected)
    {
        await session.Expect(expected);
        await session.Reply(reply);
        await call;
    }

    [Fact(Timeout = 15000)]
    public async Task CoverageStringCommandsSendRuvioOptions()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        RuvioClient db = session.Client;

        Assert.True(await Exchange(session, db.SetIfEqualAsync("k", "new", "old"), "+OK\r\n", "SET", "k", "new", "IFEQ", "old"));
        Assert.False(await Exchange(session, db.SetIfEqualAsync("k", "new", "old", TimeSpan.FromSeconds(2)), "$-1\r\n",
            "SET", "k", "new", "IFEQ", "old", "PX", "2000"));
        Assert.True(await Exchange(session, db.SetIfGreaterAsync("k", -7), "+OK\r\n", "SET", "k", "-7", "IFGT"));
        Assert.False(await Exchange(session, db.SetIfLessAsync("k", 9, TimeSpan.FromSeconds(2)), "$-1\r\n",
            "SET", "k", "9", "IFLT", "PX", "2000"));
        Assert.True(await Exchange(session, db.SetIfEqualKeepingTimeToLiveAsync("k", "n", "o"), "+OK\r\n", "SET", "k", "n", "IFEQ", "o", "KEEPTTL"));
        Assert.True(await Exchange(session, db.DeleteIfEqualAsync("k", "tok"), ":1\r\n", "DELEX", "k", "IFEQ", "tok"));
        await Exchange(session, db.SetExpireAsync("k", 10, "v"), "+OK\r\n", "SETEX", "k", "10", "v");
        await Exchange(session, db.SetExpireMillisecondsAsync("k", 1500, "v"), "+OK\r\n", "PSETEX", "k", "1500", "v");
        Assert.False(await Exchange(session, db.SetIfNotExistsAsync("k", "v"), ":0\r\n", "SETNX", "k", "v"));
        Assert.Equal("v", await Exchange(session, db.GetDeleteAsync("k"), "$1\r\nv\r\n", "GETDEL", "k"));
        Assert.Equal(3.5, await Exchange(session, db.IncrementByFloatAsync("f", 1.5), "$3\r\n3.5\r\n", "INCRBYFLOAT", "f", "1.5"));
        Assert.Equal("foo", await Exchange(session, db.GetRangeAsync("s", 0, 2), "$3\r\nfoo\r\n", "GETRANGE", "s", "0", "2"));
        Assert.Equal("oo", await Exchange(session, db.GetSubstringAsync("s", 1, -1), "$2\r\noo\r\n", "SUBSTR", "s", "1", "-1"));
        Assert.Equal(6, await Exchange(session, db.SetRangeAsync("s", 1, "XY"), ":6\r\n", "SETRANGE", "s", "1", "XY"));
        Assert.True(await Exchange(session, db.SetManyIfNotExistsAsync(("a", "1"), ("b", "2")), ":1\r\n", "MSETNX", "a", "1", "b", "2"));
    }

    [Fact(Timeout = 15000)]
    public async Task CoverageBitAndHyperloglogCommands()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        RuvioClient db = session.Client;

        Assert.False(await Exchange(session, db.SetBitAsync("b", 7, true), ":0\r\n", "SETBIT", "b", "7", "1"));
        Assert.True(await Exchange(session, db.GetBitAsync("b", 7), ":1\r\n", "GETBIT", "b", "7"));
        Assert.Equal(26, await Exchange(session, db.BitCountAsync("b"), ":26\r\n", "BITCOUNT", "b"));
        Assert.Equal(4, await Exchange(session, db.BitCountAsync("b", 0, 7, BitUnit.Bit), ":4\r\n", "BITCOUNT", "b", "0", "7", "BIT"));
        Assert.Equal(1, await Exchange(session, db.BitPositionAsync("b", true), ":1\r\n", "BITPOS", "b", "1"));
        Assert.Equal(8, await Exchange(session, db.BitPositionAsync("b", false, 1, 2), ":8\r\n", "BITPOS", "b", "0", "1", "2", "BYTE"));
        Assert.Equal(6, await Exchange(session, db.BitOperationAsync(BitOperation.Xor, "d", ["a", "b"]), ":6\r\n", "BITOP", "XOR", "d", "a", "b"));
        Assert.True(await Exchange(session, db.HyperLogLogAddAsync("h", "x", "y"), ":1\r\n", "PFADD", "h", "x", "y"));
        Assert.Equal(2, await Exchange(session, db.HyperLogLogCountAsync("h", "g"), ":2\r\n", "PFCOUNT", "h", "g"));
        await Exchange(session, db.HyperLogLogMergeAsync("m", "h", "g"), "+OK\r\n", "PFMERGE", "m", "h", "g");
        Assert.Throws<ArgumentException>(() => { _ = db.BitCountAsync("b", 0); });
    }

    [Fact(Timeout = 15000)]
    public async Task CoverageListAndSetCommands()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        RuvioClient db = session.Client;

        Assert.Equal(2, await Exchange(session, db.PushListLeftCappedAsync("l", 2, "a", "b", "c"), ":2\r\n", "LPUSHCAP", "l", "2", "a", "b", "c"));
        Assert.Equal(2, await Exchange(session, db.PushListRightCappedAsync("l", 2, "a", "b", "c", "d"), ":2\r\n", "RPUSHCAP", "l", "2", "a", "b", "c", "d"));
        KeyValuePair<string, IReadOnlyList<string>>? popped = await Exchange(session,
            db.PopListManyBlockingAsync(["x", "l"], fromLeft: false, TimeSpan.FromMilliseconds(1500), 2),
            "*2\r\n$1\r\nl\r\n*1\r\n$1\r\nd\r\n", "BLMPOP", "2", "2", "x", "l", "RIGHT", "COUNT", "2");
        Assert.Equal("l", popped!.Value.Key);
        Assert.Equal(["d"], popped.Value.Value);
        Assert.Null(await Exchange(session, db.PopListManyBlockingAsync(["x"], true, TimeSpan.FromSeconds(1)), "*-1\r\n",
            "BLMPOP", "1", "1", "x", "LEFT", "COUNT", "1"));
        Assert.Equal([true, false], await Exchange(session, db.SetContainsManyAsync("s", "a", "z"), "*2\r\n:1\r\n:0\r\n", "SMISMEMBER", "s", "a", "z"));
        Assert.Equal(2, await Exchange(session, db.GetSetIntersectionCountAsync(["s", "t"], 5), ":2\r\n", "SINTERCARD", "2", "s", "t", "LIMIT", "5"));
        Assert.Equal(1, await Exchange(session, db.GetSetIntersectionCountAsync(["s"]), ":1\r\n", "SINTERCARD", "1", "s"));
        SetScanPage page = await Exchange(session, db.ScanSetAsync("s", 0, "a*", 10), "*2\r\n$1\r\n7\r\n*1\r\n$1\r\na\r\n",
            "SSCAN", "s", "0", "MATCH", "a*", "COUNT", "10");
        Assert.Equal(7, page.Cursor);
        Assert.Equal(["a"], page.Members);
        ScanPage keys = await Exchange(session, db.ScanAsync(0, null, null, "string"), "*2\r\n$1\r\n0\r\n*1\r\n$1\r\nk\r\n", "SCAN", "0", "TYPE", "string");
        Assert.Equal(["k"], keys.Keys);
    }

    [Fact(Timeout = 15000)]
    public async Task CoverageSortedSetCommands()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        RuvioClient db = session.Client;

        Assert.Equal(["a", "b"], await Exchange(session, db.GetSortedSetRangeByLexAsync("z", "-", "+", false, 0, 2), "*2\r\n$1\r\na\r\n$1\r\nb\r\n",
            "ZRANGEBYLEX", "z", "-", "+", "LIMIT", "0", "2"));
        Assert.Equal(["c"], await Exchange(session, db.GetSortedSetRangeByLexAsync("z", "[b", "+", descending: true), "*1\r\n$1\r\nc\r\n",
            "ZREVRANGEBYLEX", "z", "+", "[b"));
        Assert.Equal(["c", "b"], await Exchange(session, db.GetSortedSetRangeByScoreAsync("z", "1", "+inf", true, 0, 2), "*2\r\n$1\r\nc\r\n$1\r\nb\r\n",
            "ZRANGE", "z", "+inf", "1", "BYSCORE", "REV", "LIMIT", "0", "2"));
        Assert.Equal(2, await Exchange(session, db.CountSortedSetByScoreAsync("z", "-inf", "(3"), ":2\r\n", "ZCOUNT", "z", "-inf", "(3"));
        Assert.Equal(3, await Exchange(session, db.CountSortedSetByLexAsync("z", "-", "+"), ":3\r\n", "ZLEXCOUNT", "z", "-", "+"));
        Assert.Equal(new double?[] { 1, null }, await Exchange(session, db.GetSortedSetScoresAsync("z", "a", "q"), "*2\r\n$1\r\n1\r\n$-1\r\n", "ZMSCORE", "z", "a", "q"));
        Assert.Equal("c", await Exchange(session, db.GetSortedSetRandomMemberAsync("z"), "$1\r\nc\r\n", "ZRANDMEMBER", "z"));
        Assert.Equal(["a"], await Exchange(session, db.GetSortedSetRandomMembersAsync("z", 1), "*1\r\n$1\r\na\r\n", "ZRANDMEMBER", "z", "1"));
        Assert.Equal([new SortedSetEntry("b", 2)], await Exchange(session, db.GetSortedSetRandomMembersWithScoresAsync("z", 1),
            "*2\r\n$1\r\nb\r\n$1\r\n2\r\n", "ZRANDMEMBER", "z", "1", "WITHSCORES"));
        SortedSetScanPage page = await Exchange(session, db.ScanSortedSetAsync("z", 0), "*2\r\n$1\r\n0\r\n*2\r\n$1\r\na\r\n$1\r\n1\r\n", "ZSCAN", "z", "0");
        Assert.Equal([new SortedSetEntry("a", 1)], page.Entries);
        Assert.Equal([new SortedSetEntry("b", 6)], await Exchange(session, db.GetSortedSetUnionAsync(["x", "y"], [1, 2], SortedSetAggregate.Max),
            "*2\r\n$1\r\nb\r\n$1\r\n6\r\n", "ZUNION", "2", "x", "y", "WEIGHTS", "1", "2", "AGGREGATE", "MAX", "WITHSCORES"));
        Assert.Empty(await Exchange(session, db.GetSortedSetIntersectionAsync(["x", "y"]), "*0\r\n", "ZINTER", "2", "x", "y", "WITHSCORES"));
        Assert.Empty(await Exchange(session, db.GetSortedSetDifferenceAsync(["x", "y"]), "*0\r\n", "ZDIFF", "2", "x", "y", "WITHSCORES"));
        Assert.Equal(3, await Exchange(session, db.StoreSortedSetUnionAsync("d", ["x", "y"], aggregate: SortedSetAggregate.Sum), ":3\r\n",
            "ZUNIONSTORE", "d", "2", "x", "y", "AGGREGATE", "SUM"));
        Assert.Equal(1, await Exchange(session, db.StoreSortedSetIntersectionAsync("d", ["x", "y"], [2, 3]), ":1\r\n",
            "ZINTERSTORE", "d", "2", "x", "y", "WEIGHTS", "2", "3"));
        Assert.Equal(1, await Exchange(session, db.StoreSortedSetDifferenceAsync("d", ["x", "y"]), ":1\r\n", "ZDIFFSTORE", "d", "2", "x", "y"));
        Assert.Throws<ArgumentException>(() => { _ = db.GetSortedSetUnionAsync(["x", "y"], [1]); });
    }

    [Fact(Timeout = 15000)]
    public async Task CoverageHashAndBloomCommands()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        RuvioClient db = session.Client;

        Assert.Equal("f", await Exchange(session, db.GetHashRandomFieldAsync("h"), "$1\r\nf\r\n", "HRANDFIELD", "h"));
        Assert.Null(await Exchange(session, db.GetHashRandomFieldAsync("none"), "$-1\r\n", "HRANDFIELD", "none"));
        Assert.Equal(["f", "g"], await Exchange(session, db.GetHashRandomFieldsAsync("h", 2), "*2\r\n$1\r\nf\r\n$1\r\ng\r\n", "HRANDFIELD", "h", "2"));
        Assert.Equal([new KeyValuePair<string, string>("f", "1")], await Exchange(session, db.GetHashRandomFieldsWithValuesAsync("h", 1),
            "*2\r\n$1\r\nf\r\n$1\r\n1\r\n", "HRANDFIELD", "h", "1", "WITHVALUES"));
        Assert.Equal(1.5, await Exchange(session, db.IncrementHashFieldByFloatAsync("h", "f", 0.5), "$3\r\n1.5\r\n", "HINCRBYFLOAT", "h", "f", "0.5"));

        await Exchange(session, db.ReserveBloomFilterAsync("bf", 0.01, 100, 4), "+OK\r\n", "BF.RESERVE", "bf", "0.01", "100", "EXPANSION", "4");
        await Exchange(session, db.ReserveBloomFilterAsync("bf", 0.01, 100, null, nonScaling: true), "+OK\r\n", "BF.RESERVE", "bf", "0.01", "100", "NONSCALING");
        Assert.Equal([true, false], await Exchange(session, db.AddBloomFilterManyAsync("bf", "a", "a"), "*2\r\n:1\r\n:0\r\n", "BF.MADD", "bf", "a", "a"));
        Assert.Equal([true, false], await Exchange(session, db.BloomFilterContainsManyAsync("bf", "a", "z"), "*2\r\n:1\r\n:0\r\n", "BF.MEXISTS", "bf", "a", "z"));
        Assert.Equal([true], await Exchange(session,
            db.InsertBloomFilterAsync("bf", ["x"], new BloomInsertOptions { Capacity = 50, ErrorRate = 0.001, Expansion = 2, NoCreate = true, NonScaling = true }),
            "*1\r\n:1\r\n", "BF.INSERT", "bf", "CAPACITY", "50", "ERROR", "0.001", "EXPANSION", "2", "NOCREATE", "NONSCALING", "ITEMS", "x"));
        BloomFilterInfo info = await Exchange(session, db.GetBloomFilterInfoAsync("bf"),
            "*10\r\n$8\r\nCapacity\r\n:100\r\n$4\r\nSize\r\n:160\r\n$17\r\nNumber of filters\r\n:1\r\n$24\r\nNumber of items inserted\r\n:2\r\n$14\r\nExpansion rate\r\n$-1\r\n",
            "BF.INFO", "bf");
        Assert.Equal(new BloomFilterInfo(100, 160, 1, 2, null), info);
        Assert.Equal(2, await Exchange(session, db.GetBloomFilterCardinalityAsync("bf"), ":2\r\n", "BF.CARD", "bf"));

        ValueTask<IReadOnlyList<bool>> full = db.AddBloomFilterManyAsync("bf", "q");
        await session.Expect("BF.MADD", "bf", "q");
        await session.Reply("*1\r\n-ERR filter is full\r\n");
        RuvioException error = await Assert.ThrowsAsync<RuvioException>(async () => await full);
        Assert.Contains("full", error.Message, StringComparison.Ordinal);
    }

    [Fact(Timeout = 15000)]
    public async Task CoverageJsonCommands()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        RuvioClient db = session.Client;

        Assert.False(await Exchange(session, db.JsonSetAsync("d", "$.z", "5", SetMode.IfNotExists), "$-1\r\n", "JSON.SET", "d", "$.z", "5", "NX"));
        Assert.True(await Exchange(session, db.JsonSetAsync("d", "$.z", "5", SetMode.IfExists), "+OK\r\n", "JSON.SET", "d", "$.z", "5", "XX"));
        Assert.Equal(1, await Exchange(session, db.JsonObjectLengthAsync("d", "$.o"), ":1\r\n", "JSON.OBJLEN", "d", "$.o"));
        Assert.Null(await Exchange(session, db.JsonObjectLengthAsync("d", "$.q"), "$-1\r\n", "JSON.OBJLEN", "d", "$.q"));
        Assert.Equal(4, await Exchange(session, db.JsonStringAppendAsync("d", "$.s", "\"cd\""), ":4\r\n", "JSON.STRAPPEND", "d", "$.s", "\"cd\""));
        Assert.Equal("7.5", await Exchange(session, db.JsonMultiplyAsync("d", "$.n", 2.5), "$3\r\n7.5\r\n", "JSON.NUMMULTBY", "d", "$.n", "2.5"));
        Assert.Equal(5, await Exchange(session, db.JsonArrayInsertAsync("d", "$.a", 0, "0", "\"q\""), ":5\r\n", "JSON.ARRINSERT", "d", "$.a", "0", "0", "\"q\""));
        Assert.Equal("3", await Exchange(session, db.JsonArrayPopAsync("d", "$.a"), "$1\r\n3\r\n", "JSON.ARRPOP", "d", "$.a", "-1"));
        Assert.Equal(1, await Exchange(session, db.JsonArrayTrimAsync("d", "$.a", 0, 0), ":1\r\n", "JSON.ARRTRIM", "d", "$.a", "0", "0"));
        Assert.Equal(-1, await Exchange(session, db.JsonArrayIndexAsync("d", "$.a", "1"), ":-1\r\n", "JSON.ARRINDEX", "d", "$.a", "1"));
        Assert.Equal(0, await Exchange(session, db.JsonArrayIndexAsync("d", "$.a", "\"q\"", 0, 3), ":0\r\n", "JSON.ARRINDEX", "d", "$.a", "\"q\"", "0", "3"));
        Assert.Equal(1, await Exchange(session, db.JsonForgetAsync("d", "$.o"), ":1\r\n", "JSON.FORGET", "d", "$.o"));
        Assert.True(await Exchange(session, db.JMergeAsync("d", "$", "{}"), "+OK\r\n", "JMERGE", "d", "$", "{}"));
        await Exchange(session, db.JsonCreateIndexAsync("city", "u:", "$.city"), "+OK\r\n",
            "JSON.INDEX", "CREATE", "city", "PREFIX", "u:", "PATH", "$.city", "TYPE", "string");
        await Exchange(session, db.JsonCreateIndexAsync("age", "u:", "$.age", JsonIndexType.Number), "+OK\r\n",
            "JSON.INDEX", "CREATE", "age", "PREFIX", "u:", "PATH", "$.age", "TYPE", "number");
        Assert.Equal(["city prefix=u:"], await Exchange(session, db.JsonListIndexesAsync(), "*1\r\n$14\r\ncity prefix=u:\r\n", "JSON.INDEX", "LIST"));
        Assert.Equal(["u:1"], await Exchange(session, db.JsonFindAsync("city", "ankara", 5, "u:0"), "*1\r\n$3\r\nu:1\r\n",
            "JSON.FIND", "city", "VALUE", "ankara", "COUNT", "5", "AFTER", "u:0"));
        Assert.True(await Exchange(session, db.JsonDropIndexAsync("city"), ":1\r\n", "JSON.INDEX", "DROP", "city"));
    }

    [Fact(Timeout = 15000)]
    public async Task CoverageAdminCommands()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        RuvioClient db = session.Client;

        Assert.Equal("PONG is taken", await Exchange(session, db.RuvioAsync(), "+PONG is taken\r\n", "RUVIO"));
        Assert.Equal("rule", await Exchange(session, db.GetRuvioOwnerAsync(), "+rule\r\n", "RUVIO", "OWNER"));
        Assert.Equal(RespKind.Array, (await Exchange(session, db.GetCommandsAsync(), "*0\r\n", "COMMAND")).Kind);
        Assert.Equal(258, await Exchange(session, db.GetCommandCountAsync(), ":258\r\n", "COMMAND", "COUNT"));
        Assert.Equal(RespKind.Array, (await Exchange(session, db.GetCommandInfoAsync("get"), "*0\r\n", "COMMAND", "INFO", "get")).Kind);
        Assert.Equal(RespKind.Array, (await Exchange(session, db.GetCommandDocsAsync("get", "set"), "*0\r\n", "COMMAND", "DOCS", "get", "set")).Kind);
        Assert.Empty(await Exchange(session, db.GetLatencyLatestAsync(), "*0\r\n", "LATENCY", "LATEST"));
        Assert.Equal(0, await Exchange(session, db.WaitForReplicasAsync(0, TimeSpan.FromMilliseconds(10)), ":0\r\n", "WAIT", "0", "10"));
        await Exchange(session, db.AuthenticateAsync("pw"), "+OK\r\n", "AUTH", "pw");
        await Exchange(session, db.AuthenticateAsync("user", "pw"), "+OK\r\n", "AUTH", "user", "pw");
        Assert.Equal(1, await Exchange(session, db.KillClientAsync("127.0.0.1:5000"), ":1\r\n", "CLIENT", "KILL", "ADDR", "127.0.0.1:5000"));
        IdempotencyBegin done = await Exchange(session, db.TryBeginIdempotencyAsync("i", "fp", "me", TimeSpan.FromSeconds(1)),
            "*2\r\n$9\r\ncompleted\r\n$2\r\nok\r\n", "IDEM", "i", "BEGIN", "fp", "me", "1000");
        Assert.Equal(IdempotencyState.Completed, done.State);
        Assert.Equal("ok"u8.ToArray(), done.Result);
        Assert.Equal(IdempotencyState.Pending, (await Exchange(session, db.TryBeginIdempotencyAsync("i", "fp", "you", TimeSpan.FromSeconds(1)),
            "*1\r\n$7\r\npending\r\n", "IDEM", "i", "BEGIN", "fp", "you", "1000")).State);
    }

    [Fact(Timeout = 15000)]
    public async Task InterfaceDefaultsSendTheSameWordsThroughExecuteAsync()
    {
        IRuvioClient db = System.Reflection.DispatchProxy.Create<IRuvioClient, RecordingClient>();
        var recorder = (RecordingClient)(object)db;

        recorder.Reply = RespValue.FromInteger(1);
        Assert.True(await db.DeleteIfEqualAsync("k", "t"));
        Assert.Equal(["DELEX", "k", "IFEQ", "t"], recorder.Last);

        Assert.Equal(1, await db.PushListLeftCappedAsync("l", 3, "a"));
        Assert.Equal(["LPUSHCAP", "l", "3", "a"], recorder.Last);

        recorder.Reply = RespValue.Array([RespValue.FromInteger(1), RespValue.FromInteger(0)]);
        Assert.Equal([true, false], await db.BloomFilterContainsManyAsync("bf", "a", "b"));
        Assert.Equal(["BF.MEXISTS", "bf", "a", "b"], recorder.Last);
    }

    /// <summary>A substitute that implements only ExecuteAsync; every coverage member falls back to its default.</summary>
    public class RecordingClient : System.Reflection.DispatchProxy
    {
        public RespValue Reply { get; set; } = RespValue.Null();

        public IReadOnlyList<string> Last { get; private set; } = [];

        public void Clear() => Last = [];

        protected override object? Invoke(System.Reflection.MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod is { Name: "ExecuteAsync" } && args is [IReadOnlyList<string> arguments, CancellationToken])
            {
                Last = [.. arguments];

                return new ValueTask<RespValue>(Reply);
            }

            if (targetMethod is { IsAbstract: false })
            {
                // Run the interface's default body non-virtually (a plain "call"), as a real implementer would get it.
                return CallDefault(targetMethod)(this, args ?? []);
            }

            throw new NotSupportedException(targetMethod?.Name);
        }

        private static Func<object, object?[], object?> CallDefault(System.Reflection.MethodInfo method)
        {
            var dynamic = new System.Reflection.Emit.DynamicMethod(
                "default_" + method.Name, typeof(object), [typeof(object), typeof(object?[])], typeof(RecordingClient).Module, skipVisibility: true);
            System.Reflection.Emit.ILGenerator il = dynamic.GetILGenerator();
            System.Reflection.ParameterInfo[] parameters = method.GetParameters();

            il.Emit(System.Reflection.Emit.OpCodes.Ldarg_0);
            il.Emit(System.Reflection.Emit.OpCodes.Castclass, method.DeclaringType!);

            for (int index = 0; index < parameters.Length; index++)
            {
                il.Emit(System.Reflection.Emit.OpCodes.Ldarg_1);
                il.Emit(System.Reflection.Emit.OpCodes.Ldc_I4, index);
                il.Emit(System.Reflection.Emit.OpCodes.Ldelem_Ref);
                il.Emit(System.Reflection.Emit.OpCodes.Unbox_Any, parameters[index].ParameterType);
            }

            il.Emit(System.Reflection.Emit.OpCodes.Call, method);

            if (method.ReturnType == typeof(void))
            {
                il.Emit(System.Reflection.Emit.OpCodes.Ldnull);
            }
            else if (method.ReturnType.IsValueType)
            {
                il.Emit(System.Reflection.Emit.OpCodes.Box, method.ReturnType);
            }

            il.Emit(System.Reflection.Emit.OpCodes.Ret);

            return (Func<object, object?[], object?>)dynamic.CreateDelegate(typeof(Func<object, object?[], object?>));
        }
    }
}
