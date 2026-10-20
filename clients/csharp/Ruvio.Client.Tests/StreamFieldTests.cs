using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

[Collection("Live")]
public sealed class StreamFieldTests
{
    private const string EntriesReply = "*1\r\n*2\r\n$3\r\n1-0\r\n*6\r\n$1\r\nf\r\n$1\r\n1\r\n$1\r\ng\r\n$0\r\n\r\n$1\r\nf\r\n$1\r\n3\r\n";

    [Theory(Timeout = 15000)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task RangesPreserveRepeatedFieldsInOrder(bool reverse, bool counted)
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        string start = reverse ? "+" : "-";
        string end = reverse ? "-" : "+";
        string command = reverse ? "XREVRANGE" : "XRANGE";
        ValueTask<IReadOnlyList<StreamEntry>> pending = counted
            ? session.Client.GetStreamRangeAsync("s", start, end, 1, reverse)
            : session.Client.GetStreamRangeAsync("s", start, end, reverse);

        await session.Expect(counted ? [command, "s", start, end, "COUNT", "1"] : [command, "s", start, end]);
        await session.Reply(EntriesReply);
        AssertFields(Assert.Single(await pending));
    }

    [Theory(Timeout = 15000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReadsPreserveRepeatedFieldsInOrder(bool group)
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<IReadOnlyList<StreamReadResult>> pending = group
            ? session.Client.GetStreamGroupUpdatesAsync("g", "c", "s", ">")
            : session.Client.GetStreamUpdatesAsync(("s", "0-0"));

        await session.Expect(group ? ["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"] : ["XREAD", "STREAMS", "s", "0-0"]);
        await session.Reply("*1\r\n*2\r\n$1\r\ns\r\n" + EntriesReply);
        StreamReadResult result = Assert.Single(await pending);

        Assert.Equal("s", result.Key);
        AssertFields(Assert.Single(result.Entries));
    }

    [Fact(Timeout = 15000)]
    public async Task ClaimsPreserveRepeatedFieldsInOrder()
    {
        await using ScriptedSession session = await ScriptedSession.Open();
        ValueTask<IReadOnlyList<StreamEntry>> pending = session.Client.ClaimStreamEntriesAsync("s", "g", "c", 0, "1-0");

        await session.Expect("XCLAIM", "s", "g", "c", "0", "1-0");
        await session.Reply(EntriesReply);
        AssertFields(Assert.Single(await pending));
    }

    [LiveFact(Timeout = 15000)]
    public async Task RepeatedFieldsRoundTripThroughTheServer()
    {
        await using RuvioConnection connection = await LiveServer.ConnectAsync(12);
        RuvioClient db = await connection.GetDefaultDatabaseAsync();

        Assert.Equal("1-0", await db.AddStreamEntryAsync("s", "1-0", ("f", "1"), ("g", ""), ("f", "3")));
        AssertFields(Assert.Single(await db.GetStreamRangeAsync("s", "-", "+")));
        AssertFields(Assert.Single(await db.GetStreamRangeAsync("s", "+", "-", reverse: true)));
        AssertFields(Assert.Single(Assert.Single(await db.GetStreamUpdatesAsync(("s", "0-0"))).Entries));

        await db.CreateStreamGroupAsync("s", "g", "0-0");
        AssertFields(Assert.Single(Assert.Single(await db.GetStreamGroupUpdatesAsync("g", "c", "s", ">")).Entries));
        AssertFields(Assert.Single(await db.ClaimStreamEntriesAsync("s", "g", "other", 0, "1-0")));
    }

    private static void AssertFields(StreamEntry entry)
    {
        Assert.Equal("1-0", entry.Id);
        Assert.Equal(
            [new KeyValuePair<string, string>("f", "1"), new("g", ""), new("f", "3")],
            entry.Fields);
    }
}
