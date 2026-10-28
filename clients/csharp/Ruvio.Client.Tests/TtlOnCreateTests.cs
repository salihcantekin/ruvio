using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

/// <summary>Wire shapes of the deadline set only when a command creates its key, and of the EXPIRE conditions, against a scripted server.</summary>
public partial class CommandSurfaceTests
{
    [Fact(Timeout = 15000)]
    public async Task ExpireConditionsAreSentAfterTheTime()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        RuvioClient db = session.Client;
        DateTimeOffset whole = DateTimeOffset.FromUnixTimeSeconds(1_900_000_000);
        DateTimeOffset fraction = DateTimeOffset.FromUnixTimeMilliseconds(1_900_000_000_500);

        Assert.True(await Exchange(session, db.ExpireAsync("k", TimeSpan.FromSeconds(30), ExpireCondition.Nx), ":1\r\n", "EXPIRE", "k", "30", "NX"));
        Assert.False(await Exchange(session, db.ExpireAsync("k", TimeSpan.FromMilliseconds(1500), ExpireCondition.Xx), ":0\r\n", "PEXPIRE", "k", "1500", "XX"));
        Assert.True(await Exchange(session, db.ExpireAsync("k", TimeSpan.FromMinutes(1), ExpireCondition.Gt), ":1\r\n", "EXPIRE", "k", "60", "GT"));
        Assert.True(await Exchange(session, db.ExpireAsync("k", TimeSpan.FromSeconds(5), ExpireCondition.Lt), ":1\r\n", "EXPIRE", "k", "5", "LT"));
        Assert.True(await Exchange(session, db.ExpireAtAsync("k", whole, ExpireCondition.Nx), ":1\r\n", "EXPIREAT", "k", "1900000000", "NX"));
        Assert.False(await Exchange(session, db.ExpireAtAsync("k", fraction, ExpireCondition.Gt), ":0\r\n", "PEXPIREAT", "k", "1900000000500", "GT"));
        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = db.ExpireAsync("k", TimeSpan.FromSeconds(-1), ExpireCondition.Nx); });
    }

    [Fact(Timeout = 15000)]
    public async Task CounterCommandsSendTheDeadlineAfterTheirArguments()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        RuvioClient db = session.Client;

        Assert.Equal(1, await Exchange(session, db.IncrementAsync("c", TimeSpan.FromSeconds(60)), ":1\r\n", "INCR", "c", "EX", "60"));
        Assert.Equal(2, await Exchange(session, db.IncrementAsync("c", TimeSpan.FromMilliseconds(250)), ":2\r\n", "INCR", "c", "PX", "250"));
        Assert.Equal(-1, await Exchange(session, db.DecrementAsync("c", TimeSpan.FromHours(1)), ":-1\r\n", "DECR", "c", "EX", "3600"));
        Assert.Equal(5, await Exchange(session, db.IncrementByAsync("c", 5, TimeSpan.FromSeconds(10)), ":5\r\n", "INCRBY", "c", "5", "EX", "10"));
        Assert.Equal(-5, await Exchange(session, db.DecrementByAsync("c", 5, TimeSpan.FromMilliseconds(1500)), ":-5\r\n", "DECRBY", "c", "5", "PX", "1500"));
        Assert.Equal(3, await Exchange(session, db.IncrementWithLimitAsync("c", 1, 10, TimeSpan.FromSeconds(20)), ":3\r\n",
            "INCRBY", "c", "1", "EX", "20", "MAX", "10"));
        Assert.Null(await Exchange(session, db.IncrementWithLimitAsync("c", 1, 10, TimeSpan.FromSeconds(20)), "$-1\r\n",
            "INCRBY", "c", "1", "EX", "20", "MAX", "10"));
        Assert.Equal(3.5, await Exchange(session, db.IncrementByFloatAsync("f", 1.5, TimeSpan.FromSeconds(7)), "$3\r\n3.5\r\n", "INCRBYFLOAT", "f", "1.5", "EX", "7"));
        Assert.Equal(4, await Exchange(session, db.IncrementHashFieldAsync("h", "n", 4, TimeSpan.FromMilliseconds(900)), ":4\r\n", "HINCRBY", "h", "n", "4", "PX", "900"));
        Assert.Equal(2.5, await Exchange(session, db.IncrementSortedSetAsync("z", "m", 2.5, TimeSpan.FromSeconds(3)), "$3\r\n2.5\r\n", "ZINCRBY", "z", "2.5", "m", "EX", "3"));
        Assert.False(await Exchange(session, db.SetBitAsync("b", 7, true, TimeSpan.FromSeconds(9)), ":0\r\n", "SETBIT", "b", "7", "1", "EX", "9"));
    }

    [Fact(Timeout = 15000)]
    public async Task CreateDeadlineIsRoundedUpAndMustBePositive()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        RuvioClient db = session.Client;

        Assert.Equal(1, await Exchange(session, db.IncrementAsync("c", TimeSpan.FromTicks(1)), ":1\r\n", "INCR", "c", "PX", "1"));
        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = db.IncrementAsync("c", TimeSpan.Zero); });
        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = db.IncrementAsync("c", TimeSpan.FromSeconds(-1)); });
        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = db.SetBitAsync("b", 1, true, TimeSpan.Zero); });
        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = db.AddGeoAsync("g", TimeSpan.Zero, (1.0, 2.0, "m")); });
    }

    [Fact(Timeout = 15000)]
    public async Task CollectionCommandsSendTheDeadlineWithTheirOptions()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        RuvioClient db = session.Client;

        Assert.Equal(2, await Exchange(session,
            db.AddSortedSetAsync("z", TimeSpan.FromSeconds(60), SortedSetAddFlags.IfNotExists | SortedSetAddFlags.Changed, new SortedSetEntry("a", 1), new SortedSetEntry("b", 2)),
            ":2\r\n", "ZADD", "z", "EX", "60", "NX", "CH", "1", "a", "2", "b"));
        Assert.Equal(1, await Exchange(session,
            db.AddSortedSetAsync("z", TimeSpan.FromMilliseconds(500), SortedSetAddFlags.None, ("a", 1.5)),
            ":1\r\n", "ZADD", "z", "PX", "500", "1.5", "a"));
        Assert.Equal(1, await Exchange(session,
            db.AddGeoAsync("g", TimeSpan.FromMinutes(15), (13.5, 38.25, "palermo")),
            ":1\r\n", "GEOADD", "g", "EX", "900", "13.5", "38.25", "palermo"));
        Assert.Equal("1-1", await Exchange(session,
            db.AddStreamEntryAsync("s", TimeSpan.FromSeconds(30), "*", ("f", "v")),
            "$3\r\n1-1\r\n", "XADD", "s", "EX", "30", "*", "f", "v"));
        Assert.True(await Exchange(session,
            db.JsonSetAsync("j", "$", "{}", SetMode.IfNotExists, TimeSpan.FromSeconds(30)),
            "+OK\r\n", "JSON.SET", "j", "$", "{}", "NX", "EX", "30"));
        Assert.True(await Exchange(session,
            db.JsonSetAsync("j", "$", "{}", SetMode.Always, TimeSpan.FromMilliseconds(1200)),
            "+OK\r\n", "JSON.SET", "j", "$", "{}", "PX", "1200"));
        Assert.Equal([true], await Exchange(session,
            db.InsertBloomFilterAsync("bf", ["x"], new BloomInsertOptions { Ttl = TimeSpan.FromSeconds(45), Capacity = 50 }),
            "*1\r\n:1\r\n", "BF.INSERT", "bf", "EX", "45", "CAPACITY", "50", "ITEMS", "x"));
        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = db.InsertBloomFilterAsync("bf", ["x"], new BloomInsertOptions { Ttl = TimeSpan.Zero }); });
    }
}
