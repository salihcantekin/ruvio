using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

/// <summary>The JSON and text helpers against a live server (database 12).</summary>
[Collection("Live")]
public sealed class JsonHelperLiveTests
{
    private const int Db = 12;

    [LiveFact(Timeout = 30000)]
    public async Task Json_round_trips_through_get_and_set()
    {
        await using RuvioConnection connection = await LiveServer.ConnectAsync(Db);
        RuvioClient db = await connection.GetDefaultDatabaseAsync();

        Assert.True(await db.SetJsonAsync("json:item", new Item("ada", 3), HelperJson.Default.Item));

        Item? read = await db.GetJsonAsync("json:item", HelperJson.Default.Item);
        Assert.Equal(new Item("ada", 3), read);
        Assert.Null(await db.GetJsonAsync("json:missing", HelperJson.Default.Item));
    }

    [LiveFact(Timeout = 30000)]
    public async Task Fill_json_stores_the_value_for_the_next_caller()
    {
        await using RuvioConnection connection = await LiveServer.ConnectAsync(Db);
        RuvioClient db = await connection.GetDefaultDatabaseAsync();
        await db.DeleteAsync("json:fill");

        FillResult<Item> first = await db.FillGetJsonAsync("json:fill", TimeSpan.FromSeconds(10), HelperJson.Default.Item);
        Assert.Equal(FillState.Fill, first.State);
        Assert.True(await db.FillSetJsonAsync("json:fill", new Item("built", 7), TimeSpan.FromMinutes(1), first.Token!.Value, HelperJson.Default.Item));

        FillResult<Item> second = await db.FillGetJsonAsync("json:fill", TimeSpan.FromSeconds(10), HelperJson.Default.Item);
        Assert.Equal(FillState.Hit, second.State);
        Assert.Equal(new Item("built", 7), second.Value);
    }

    [LiveFact(Timeout = 30000)]
    public async Task Text_exposes_the_stored_string()
    {
        await using RuvioConnection connection = await LiveServer.ConnectAsync(Db);
        RuvioClient db = await connection.GetDefaultDatabaseAsync();
        await db.DeleteAsync("json:text");

        FillResult first = await db.FillGetAsync("json:text", TimeSpan.FromSeconds(10));
        Assert.True(await db.FillSetAsync("json:text", "hello", TimeSpan.FromMinutes(1), first.Token!.Value));

        FillResult second = await db.FillGetAsync("json:text", TimeSpan.FromSeconds(10));
        Assert.Equal("hello", second.Text);
    }
}
