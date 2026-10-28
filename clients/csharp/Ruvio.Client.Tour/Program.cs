using Ruvio.Client;

// Calls nearly every Ruvio.Client method.
// dotnet run --project clients/csharp/Ruvio.Client.Tour -- 127.0.0.1 6379
// Standalone: selects database 15, flushes it, and ends with FLUSHALL.
// Cluster: stays on database 0 and does not flush. Point this at a local server.

string host = args.Length > 0 ? args[0] : "127.0.0.1";
int port = args.Length > 1 ? int.Parse(args[1]) : 6379;

await using RuvioConnection connection = await RuvioConnection.ConnectAsync(new RuvioClientOptions
{
    Host = host,
    Port = port,
    Reconnect = true,
    MaxReconnectAttempts = 8,
    ReconnectBaseDelay = TimeSpan.FromMilliseconds(100),
    ReconnectMaxDelay = TimeSpan.FromSeconds(2),
    ReconnectDelay = attempt => attempt >= 8
        ? null
        : TimeSpan.FromMilliseconds(100 * Math.Pow(2, attempt - 1)),
});

IRuvioClient db = await connection.GetDefaultDatabaseAsync();

connection.ConnectionLost += (_, notice) =>
    Console.WriteLine($"connection lost {notice.Host}:{notice.Port} subscriber={notice.Subscriber} {notice.Error?.Message}");
connection.ConnectionRestored += (_, notice) =>
    Console.WriteLine($"connection restored {notice.Host}:{notice.Port} subscriber={notice.Subscriber}");
connection.ConnectionFailed += (_, error) => Console.WriteLine($"connection failed {error.Message}");
Console.WriteLine($"connected cluster={connection.IsCluster} shards={connection.ShardCount} slot={RuvioClient.GetHashSlot("{tour}")} reconnect={connection.IsConnected}");

if (!connection.IsCluster)
{
    db = await connection.GetDatabaseAsync(15);
    await db.FlushDatabaseAsync();
}

await Strings(db);
await Owned(db);
await Lists(db);
await Sets(db);
await Hashes(db);
await SortedSets(db);
await Bloom(db);
await Streams(db);
await Transactions(db, connection.IsCluster);
await Server(db, connection.IsCluster);
await PubSub(db);
Console.WriteLine("tour finished");

static async Task Strings(IRuvioClient db)
{
    const string key = "{tour}:s";

    await db.SetAsync(key, "hello");
    Console.WriteLine($"get {await db.GetStringAsync(key)} bytes={((await db.GetAsync(key))?.Length).ToString() ?? "null"}");
    Console.WriteLine($"typed {await db.GetAsync<string>(key)} int={await db.GetInt32Async("{tour}:missing")} long={await db.GetInt64Async("{tour}:missing")}");

    await db.SetAsync(key, "nx", TimeSpan.FromSeconds(30), SetMode.IfNotExists);
    await db.SetAsync(key, "later", DateTimeOffset.UtcNow.AddMinutes(2));
    await db.SetKeepingTimeToLiveAsync(key, "kept");
    Console.WriteLine($"setAndGet {await db.SetAndGetAsync(key, "now")}");
    await db.SetManyAsync(("{tour}:a", "1"), ("{tour}:b", "2"));
    Console.WriteLine($"mget {string.Join(',', await db.GetManyAsync("{tour}:a", "{tour}:missing", "{tour}:b"))}");

    Console.WriteLine($"getset {await db.GetSetAsync(key, "10")}");
    Console.WriteLine($"append {await db.AppendAsync(key, "0")} strlen {await db.GetStringLengthAsync(key)}");
    Console.WriteLine($"incr {await db.IncrementAsync("{tour}:n")} by {await db.IncrementByAsync("{tour}:n", 5)}");

    Console.WriteLine($"decr {await db.DecrementAsync("{tour}:n")} by {await db.DecrementByAsync("{tour}:n", 2)}");

    Console.WriteLine($"exists {await db.ExistsAsync("{tour}:a", "{tour}:b")} type {await db.GetKeyTypeAsync(key)}");

    await db.RenameAsync("{tour}:b", "{tour}:c");
    Console.WriteLine($"unlink {await db.UnlinkAsync("{tour}:c")}");

    await db.ExpireAsync(key, TimeSpan.FromSeconds(30));
    await db.ExpireAtAsync("{tour}:a", DateTimeOffset.UtcNow.AddMinutes(1));
    Console.WriteLine($"ttl {await db.GetTimeToLiveAsync(key)} pttl {await db.GetTimeToLiveMillisecondsAsync(key)} persist {await db.PersistAsync(key)}");
    ScanPage page = await db.ScanAsync(0, "{tour}:*", 20);

    Console.WriteLine($"scan cursor={page.Cursor} keys={page.Keys.Count} dbsize={await db.GetDatabaseSizeAsync()}");
    Console.WriteLine($"del {await db.DeleteAsync("{tour}:a")} many {await db.DeleteAsync(new[] { "{tour}:n" })}");
}

static async Task Owned(IRuvioClient db)
{
    long? first = await db.LeaseAsync("{tour}:lock", 30_000);
    long? held = await db.LeaseAsync("{tour}:lock", 30_000);

    Console.WriteLine($"lease {first} held {(held is null ? "null" : held.ToString())} release {await db.ReleaseAsync("{tour}:lock", first ?? 0)}");

    long? permit = await db.AcquireSemaphoreAsync("{tour}:workers", 1, 30_000);
    long? full = await db.AcquireSemaphoreAsync("{tour}:workers", 1, 30_000);

    Console.WriteLine($"semaphore {permit} full {(full is null ? "null" : full.ToString())} release {await db.ReleaseAsync("{tour}:workers", permit ?? 0)}");

    long? seats = await db.IncrementWithLimitAsync("{tour}:seats", 3, 4);
    long? over = await db.IncrementWithLimitAsync("{tour}:seats", 2, 4);

    Console.WriteLine($"incrby max {seats} over {(over is null ? "null" : over.ToString())}");
    long created = await db.SetVersionAsync("{tour}:doc", "12");
    long next = await db.SetVersionAsync("{tour}:doc", "11", created);

    Console.WriteLine($"setv {created} then {next}");
    try
    {
        await db.SetVersionAsync("{tour}:doc", "10", created);
        Console.WriteLine("setv mismatch was accepted");
    }

    catch (RuvioException error)
    {
        Console.WriteLine($"setv mismatch {error.Message}");
    }

    long cursor = await db.StartChangesAsync();

    await db.SetAsync("{tour}:note", "paid");
    IReadOnlyList<ChangeNote> notes = await db.ReadChangesAsync(cursor);

    Console.WriteLine($"changes cursor={cursor} notes={notes.Count} {(notes.Count == 0 ? "" : notes[0].Text)}");
    long? allowance = await db.LimitAsync("{tour}:login", 2, 60_000);
    long? again = await db.LimitAsync("{tour}:login", 2, 60_000);

    Console.WriteLine($"limit {allowance} {again}");
    Console.WriteLine($"once {await db.OnceAsync("{tour}:pay", 60_000)} {await db.OnceAsync("{tour}:pay", 60_000)}");

    await db.SetAsync("{tour}:seats", "5");
    Console.WriteLine($"take {await db.TakeAsync("{tour}:seats", 2)} {await db.TakeAsync("{tour}:seats", 9)}");

    Console.WriteLine($"getex {await db.GetExpireAsync("{tour}:seats", 60_000)}");
}

static async Task Lists(IRuvioClient db)
{
    const string key = "{tour}:l";

    Console.WriteLine($"rpush {await db.PushListRightAsync(key, "a", "b", "c", "d")} lpush {await db.PushListLeftAsync(key, "z")}");
    Console.WriteLine($"lpop {await db.PopListLeftAsync(key)} rpop {string.Join(',', await db.PopListRightAsync(key, 2))}");
    Console.WriteLine($"llen {await db.GetListLengthAsync(key)} lindex {await db.GetListItemAsync(key, 0)} lrange {string.Join(',', await db.GetListRangeAsync(key, 0, -1))}");
    await db.TrimListAsync(key, 0, 0);
    Console.WriteLine($"lpop count {string.Join(',', await db.PopListLeftAsync(key, 1))}");
    await db.PushListRightAsync(key, "only");
    KeyValuePair<string, string>? left = await db.PopListLeftBlockingAsync(key, TimeSpan.FromMilliseconds(200));

    Console.WriteLine($"blpop {left?.Value}");
    await db.PushListRightAsync(key, "right");
    KeyValuePair<string, string>? right = await db.PopListRightBlockingAsync(new[] { key }, TimeSpan.FromMilliseconds(200));

    Console.WriteLine($"brpop {right?.Value} empty {await db.PopListRightBlockingAsync(key, TimeSpan.FromMilliseconds(1))}");
}

static async Task Sets(IRuvioClient db)
{
    Console.WriteLine($"sadd {await db.AddSetMembersAsync("{tour}:s1", "a", "b", "c")}");

    await db.AddSetMembersAsync("{tour}:s2", "b", "c", "d");
    Console.WriteLine($"sismember {await db.SetContainsAsync("{tour}:s1", "a")} scard {await db.GetSetCountAsync("{tour}:s1")}");

    Console.WriteLine($"smembers {string.Join(',', await db.GetSetMembersAsync("{tour}:s1"))}");

    Console.WriteLine($"sinter {string.Join(',', await db.GetSetIntersectionAsync("{tour}:s1", "{tour}:s2"))}");

    Console.WriteLine($"sunion {string.Join(',', await db.GetSetUnionAsync("{tour}:s1", "{tour}:s2"))}");

    Console.WriteLine($"sdiff {string.Join(',', await db.GetSetDifferenceAsync("{tour}:s1", "{tour}:s2"))}");

    Console.WriteLine($"stores {await db.StoreSetIntersectionAsync("{tour}:s3", "{tour}:s1", "{tour}:s2")} {await db.StoreSetUnionAsync("{tour}:s4", "{tour}:s1", "{tour}:s2")} {await db.StoreSetDifferenceAsync("{tour}:s5", "{tour}:s1", "{tour}:s2")}");

    Console.WriteLine($"smove {await db.MoveSetMemberAsync("{tour}:s1", "{tour}:s3", "a")} srem {await db.RemoveSetMembersAsync("{tour}:s3", "a")}");

    Console.WriteLine($"srand {await db.GetSetRandomMemberAsync("{tour}:s2")} many {string.Join(',', await db.GetSetRandomMembersAsync("{tour}:s2", 2))}");

    Console.WriteLine($"spop {string.Join(',', await db.PopSetMembersAsync("{tour}:s5", 5))} one {await db.PopSetMemberAsync("{tour}:s4")}");
}

static async Task Hashes(IRuvioClient db)
{
    const string key = "{tour}:h";

    Console.WriteLine($"hset {await db.SetHashFieldsAsync(key, ("f1", "1"), ("f2", "two"))} hget {await db.GetHashFieldAsync(key, "f2")}");
    Console.WriteLine($"hincr {await db.IncrementHashFieldAsync(key, "f1", 4)} hsetnx {await db.SetHashFieldIfAbsentAsync(key, "f1", "9")}");
    Console.WriteLine($"hstrlen {await db.GetHashFieldLengthAsync(key, "f2")} hlen {await db.GetHashLengthAsync(key)} hexists {await db.HashFieldExistsAsync(key, "f1")}");
    Console.WriteLine($"hkeys {string.Join(',', await db.GetHashFieldNamesAsync(key))} hvals {string.Join(',', await db.GetHashFieldValuesAsync(key))}");
    Console.WriteLine($"hgetall {(await db.GetHashAllAsync(key)).Count} hmget {string.Join(',', await db.GetHashFieldsAsync(key, "f2", "nope"))}");
    HashScanPage page = await db.ScanHashAsync(key, 0, "f*", 10);

    Console.WriteLine($"hexpire {string.Join(',', await db.ExpireHashFieldsAsync(key, TimeSpan.FromSeconds(60), "f1", "nope"))} gt {string.Join(',', await db.ExpireHashFieldsAsync(key, TimeSpan.FromSeconds(120), FieldExpireCondition.IfGreater, "f1"))}");
    Console.WriteLine($"hexpireat {string.Join(',', await db.ExpireHashFieldsAtAsync(key, DateTimeOffset.UtcNow.AddMinutes(5), "f2"))} httl {string.Join(',', await db.GetHashFieldTimeToLiveAsync(key, "f1", "f2"))} hpttl {string.Join(',', await db.GetHashFieldTimeToLiveMillisecondsAsync(key, "f1"))}");
    Console.WriteLine($"hexpiretime {string.Join(',', await db.GetHashFieldExpireTimeAsync(key, "f2"))} hpexpiretime {string.Join(',', await db.GetHashFieldExpireTimeMillisecondsAsync(key, "f2"))} hpersist {string.Join(',', await db.PersistHashFieldsAsync(key, "f1", "f2"))}");
    Console.WriteLine($"hscan cursor={page.Cursor} fields={page.Fields.Count} hdel {await db.DeleteHashFieldsAsync(key, "f1")}");
}

static async Task SortedSets(IRuvioClient db)
{
    const string key = "{tour}:z";

    Console.WriteLine($"zadd {await db.AddSortedSetAsync(key, ("ada", 100d), ("bob", 50d))} one {await db.AddSortedSetAsync(key, "cy", 1)}");
    Console.WriteLine($"geoadd {await db.AddGeoAsync(key, (13.361389, 38.115556, "Palermo"))} search {string.Join(',', await db.GeoSearchAsync(key, 13.36, 38.11, 20, "km"))}");
    Console.WriteLine($"flags {await db.AddSortedSetAsync(key, SortedSetAddFlags.GreaterThan | SortedSetAddFlags.Changed, ("bob", 10d))}");
    Console.WriteLine($"entries {await db.AddSortedSetAsync(key, new SortedSetEntry("ada", 100))}");
    Console.WriteLine($"zincr {await db.IncrementSortedSetAsync(key, "bob", 5.5)} flagged {await db.IncrementSortedSetAsync(key, "new", 1, SortedSetAddFlags.IfExists)}");
    Console.WriteLine($"zrange {string.Join(',', await db.GetSortedSetRangeByRankAsync(key, 0, -1))} rev {string.Join(',', await db.GetSortedSetRangeByRankAsync(key, 0, 0, descending: true))}");
    Console.WriteLine($"scores {(await db.GetSortedSetRangeByRankWithScoresAsync(key, 0, 0)).Count} rev {(await db.GetSortedSetRangeByRankWithScoresAsync(key, 0, 0, descending: true)).Count}");
    Console.WriteLine($"byscore {(await db.GetSortedSetRangeByScoreWithScoresAsync(key, "(1", "+inf")).Count}");
    Console.WriteLine($"zscore {await db.GetSortedSetScoreAsync(key, "ada")} rank {await db.GetSortedSetRankAsync(key, "ada")} rev {await db.GetSortedSetRankAsync(key, "ada", descending: true)}");
    Console.WriteLine($"zrem {await db.RemoveSortedSetMembersAsync(key, "cy")} zcard {await db.GetSortedSetCountAsync(key)}");

    string queue = "{tour}:zq";

    await db.AddSortedSetAsync(queue, ("a", 1d), ("b", 2d), ("c", 3d), ("d", 4d), ("e", 5d));
    Console.WriteLine($"zpopmin {(await db.PopSortedSetMinAsync(queue))[0]} zpopmax {(await db.PopSortedSetMaxAsync(queue, 1))[0]}");
    Console.WriteLine($"bzpopmin {await db.PopSortedSetMinBlockingAsync(queue, TimeSpan.FromSeconds(1))} bzpopmax {await db.PopSortedSetMaxBlockingAsync([queue], TimeSpan.FromSeconds(1))}");
    await db.AddSortedSetAsync(queue, ("a", 1d), ("b", 2d), ("c", 3d), ("x", 0d));
    Console.WriteLine($"zremrangebyrank {await db.RemoveSortedSetRangeByRankAsync(queue, 0, 0)} byscore {await db.RemoveSortedSetRangeByScoreAsync(queue, "(1", "2")} bylex {await db.RemoveSortedSetRangeByLexAsync(queue, "-", "+")}");
}

static async Task Bloom(IRuvioClient db)
{
    await db.ReserveBloomFilterAsync("{tour}:bf", 0.01, 1000);
    Console.WriteLine($"bf add {await db.AddBloomFilterAsync("{tour}:bf", "x")} exists {await db.BloomFilterContainsAsync("{tour}:bf", "x")} {await db.BloomFilterContainsAsync("{tour}:bf", "y")}");
}

static async Task Streams(IRuvioClient db)
{
    const string key = "{tour}:stream";
    string first = await db.AddStreamEntryAsync(key, "*", ("f", "1"));

    await db.AddStreamEntryAsync(key, "*", ("f", "2"));
    Console.WriteLine($"xadd maxlen {await db.AddStreamEntryAsync(key, "*", 10, ("f", "3"))} xlen {await db.GetStreamLengthAsync(key)}");
    Console.WriteLine($"xadd delay {await db.AddStreamEntryDelayedAsync(key, 60_000, "*", ("f", "later"))}");
    Console.WriteLine($"xrange {(await db.GetStreamRangeAsync(key, "-", "+")).Count} counted {(await db.GetStreamRangeAsync(key, "+", "-", 1, reverse: true)).Count}");
    Console.WriteLine($"xread {(await db.GetStreamUpdatesAsync((key, "0"))).Count} opts {(await db.GetStreamUpdatesAsync(new StreamReadOptions { Count = 2 }, (key, "0"))).Count}");
    await db.CreateStreamGroupAsync(key, "g", "0");
    await db.CreateStreamGroupAsync("{tour}:fresh", "g", "$", createStream: true);
    Console.WriteLine($"xreadgroup {(await db.GetStreamGroupUpdatesAsync("g", "c1", key, ">")).Count}");
    Console.WriteLine($"xreadgroup opts {(await db.GetStreamGroupUpdatesAsync("g", "c1", key, ">", new StreamReadOptions { Count = 1 })).Count}");
    Console.WriteLine($"xpending {(await db.GetStreamPendingAsync(key, "g", "-", "+", 10, "c1")).Count}");
    Console.WriteLine($"summary {(await db.GetStreamPendingAsync(key, "g")).Kind}");
    Console.WriteLine($"xclaim {(await db.ClaimStreamEntriesAsync(key, "g", "c2", 0, first)).Count}");
    Console.WriteLine($"xclaim opts {string.Join(',', await db.ClaimStreamEntryIdsAsync(key, "g", "c2", 0, new StreamClaimOptions(), first))}");
    Console.WriteLine($"xack {await db.AcknowledgeStreamAsync(key, "g", first)} delconsumer {await db.DeleteStreamGroupConsumerAsync(key, "g", "c1")}");
    await db.SetStreamGroupIdAsync(key, "g", "0");
    Console.WriteLine($"destroy {await db.DestroyStreamGroupAsync(key, "g")} xdel {await db.DeleteStreamEntriesAsync(key, first)} xtrim {await db.TrimStreamAsync(key, 1)}");
}

static async Task Transactions(IRuvioClient db, bool isCluster)
{
    await db.WatchAsync("{tour}:w");
    await db.BeginTransactionAsync();
    await db.ExecuteAsync(["SET", "{tour}:w", "1"]);
    await db.ExecuteAsync("INCR", "{tour}:w");
    Console.WriteLine($"exec {(await db.ExecuteTransactionAsync()).Count}");
    await db.UnwatchAsync();
    await db.BeginTransactionAsync();
    await db.DiscardTransactionAsync();
    Console.WriteLine($"eval {(await db.EvaluateAsync("return ARGV[1]", 0, "hi")).AsString()}");
    string sha = await db.LoadScriptAsync("return ruvio.call('GET', KEYS[1])");

    Console.WriteLine($"exists {string.Join(',', await db.ScriptExistsAsync(sha, "0000"))}");
    Console.WriteLine($"evalsha {(await db.EvaluateShaAsync(sha, 1, "{tour}:w")).AsString()}");

    await db.LoadFunctionAsync("tour_echo", "return ARGV[1]");
    Console.WriteLine($"fcall {(await db.CallFunctionAsync("tour_echo", 0, "x")).AsString()}");
    Console.WriteLine($"functions {(await db.GetFunctionListAsync()).Kind} delete {await db.DeleteFunctionAsync("tour_echo")}");
    await db.FlushScriptsAsync();
    try
    {
        await db.KillScriptAsync();
        Console.WriteLine("script_kill ok");
    }

    catch (RuvioException error)
    {
        Console.WriteLine($"script_kill {error.Message}");
    }

    if (!isCluster)
    {
        await db.SetAsync("{tour}:move", "v");
        Console.WriteLine($"move {await db.MoveKeyAsync("{tour}:move", 14)}");

        await db.SwapDatabasesAsync(14, 15);
        await db.SwapDatabasesAsync(14, 15);
    }

    Console.WriteLine($"pipeline {(await db.ExecuteManyAsync([["PING"], ["PING"]])).Count}");
}

static async Task Server(IRuvioClient db, bool isCluster)
{
    Console.WriteLine($"ping {await db.PingAsync()} {await db.PingAsync("tour")}");
    Console.WriteLine($"info {(await db.GetInfoAsync()).Contains("ruvio")} section {(await db.GetInfoAsync("server")).Length}");
    Console.WriteLine($"config {(await db.GetConfigAsync("databases")).Kind}");
    await db.SetConfigAsync([("slowlog-max-len", "128")]);
    Console.WriteLine("config set slowlog-max-len 128");
    Console.WriteLine($"role {(await db.GetRoleAsync()).Items![0].AsString()}");
    Console.WriteLine($"whoami {await db.GetAclUserNameAsync()} users {string.Join(',', await db.GetAclUsersAsync())}");
    Console.WriteLine($"acl list {(await db.GetAclListAsync()).Count} cat {(await db.GetAclCategoriesAsync()).Count}");
    Console.WriteLine($"getuser {(await db.GetAclUserAsync("default")).Kind}");
    await db.SetAclUserAsync("tour-reader", "on", "~{tour}:*", "+get");
    await db.SetConnectionNameAsync("ruvio-tour");
    Console.WriteLine($"name {await db.GetConnectionNameAsync()}");
    await db.SetTrackingAsync(false);
    Console.WriteLine($"keyslot {await db.GetClusterKeySlotAsync("{tour}")}");

    if (isCluster)
    {
        Console.WriteLine($"nodes {(await db.GetClusterNodesAsync()).Length} slots {(await db.GetClusterSlotsAsync()).Kind}");
        Console.WriteLine($"shards {(await db.GetClusterShardsAsync()).Kind} info {(await db.GetClusterInfoAsync()).Length} myid {await db.GetClusterMyIdAsync()}");
        await db.ReadOnlyAsync();
        await db.ReadWriteAsync();
    }

    await db.SaveAsync();
    await db.SaveInBackgroundAsync();
    Console.WriteLine($"hello {(await db.HelloAsync(2)).Kind}");

    if (!isCluster)
    {
        await db.FlushAllDatabasesAsync();
    }
}

static async Task PubSub(IRuvioClient db)
{
    var seen = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

    db.Message += (_, message) =>
    {
        Console.WriteLine($"event {message.Channel} {message.Payload}");
        seen.TrySetResult(message.Payload);
    };

    await db.SubscribeAsync("{tour}:news", message => Console.WriteLine($"handler {message.Channel} {message.Payload}"));
    await db.SubscribePatternsAsync("{tour}:n*", message => Console.WriteLine($"pattern {message.Pattern} {message.Payload}"));
    Console.WriteLine($"publish {await db.PublishAsync("{tour}:news", "hello-tour")}");

    Console.WriteLine($"wait {await seen.Task.WaitAsync(TimeSpan.FromSeconds(2))}");
    using var stop = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

    try
    {
        await foreach (PubSubMessage message in db.ListenAsync(stop.Token))
        {
            Console.WriteLine($"listen {message.Kind} {message.Payload}");
        }
    }

    catch (OperationCanceledException)
    {
    }

    await db.UnsubscribeAsync("{tour}:news");
    await db.UnsubscribePatternsAsync("{tour}:n*");
    await db.SubscribeShardedAsync("{tour}:shard", message => Console.WriteLine($"sharded {message.Payload}"));
    Console.WriteLine($"spublish {await db.PublishShardedAsync("{tour}:shard", "sharded")}");

    await Task.Delay(100);
    await db.UnsubscribeShardedAsync("{tour}:shard");
    Console.WriteLine($"ping after subscribe {await db.PingAsync()}");
}
