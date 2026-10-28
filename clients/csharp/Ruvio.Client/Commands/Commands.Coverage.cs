// Typed methods for the commands and options added in the coverage pass. Frames are built by
// CoverageCommands; replies are read by RespRead with pooled state machines.

namespace Ruvio.Client;

public sealed partial class RuvioClient
{
    /// <inheritdoc />
    public ValueTask<bool> SetIfEqualAsync(string key, string value, string expected, TimeSpan? expiry = null, CancellationToken cancellationToken = default)
        => RespRead.NotNull(ExecuteCommandAsync(CoverageCommands.SetIfEqual(key, value, expected, expiry), cancellationToken));

    /// <inheritdoc />
    public ValueTask<bool> SetIfGreaterAsync(string key, long value, TimeSpan? expiry = null, CancellationToken cancellationToken = default)
        => RespRead.NotNull(ExecuteCommandAsync(CoverageCommands.SetIfGreater(key, value, expiry), cancellationToken));

    /// <inheritdoc />
    public ValueTask<bool> SetIfLessAsync(string key, long value, TimeSpan? expiry = null, CancellationToken cancellationToken = default)
        => RespRead.NotNull(ExecuteCommandAsync(CoverageCommands.SetIfLess(key, value, expiry), cancellationToken));

    /// <inheritdoc />
    public ValueTask<bool> SetIfEqualKeepingTimeToLiveAsync(string key, string value, string expected, CancellationToken cancellationToken = default)
        => RespRead.NotNull(ExecuteCommandAsync(CoverageCommands.SetIfEqualKeepingTimeToLive(key, value, expected), cancellationToken));

    /// <inheritdoc />
    public ValueTask<bool> DeleteIfEqualAsync(string key, string expected, CancellationToken cancellationToken = default)
        => RespRead.Flag(ExecuteCommandAsync(CoverageCommands.DeleteIfEqual(key, expected), cancellationToken), "DELEX");

    /// <inheritdoc />
    public ValueTask SetExpireAsync(string key, long seconds, string value, CancellationToken cancellationToken = default)
        => RespRead.Ok(ExecuteCommandAsync(CoverageCommands.SetExpire(key, seconds, value), cancellationToken));

    /// <inheritdoc />
    public ValueTask SetExpireMillisecondsAsync(string key, long milliseconds, string value, CancellationToken cancellationToken = default)
        => RespRead.Ok(ExecuteCommandAsync(CoverageCommands.SetExpireMilliseconds(key, milliseconds, value), cancellationToken));

    /// <inheritdoc />
    public ValueTask<bool> SetIfNotExistsAsync(string key, string value, CancellationToken cancellationToken = default)
        => RespRead.Flag(ExecuteCommandAsync(CoverageCommands.SetIfNotExists(key, value), cancellationToken), "SETNX");

    /// <inheritdoc />
    public ValueTask<string?> GetDeleteAsync(string key, CancellationToken cancellationToken = default)
        => RespRead.Text(ExecuteCommandAsync(CoverageCommands.GetDelete(key), cancellationToken));

    /// <inheritdoc />
    public ValueTask<double> IncrementByFloatAsync(string key, double delta, CancellationToken cancellationToken = default)
        => RespRead.Double(ExecuteCommandAsync(CoverageCommands.IncrementByFloat(key, delta), cancellationToken), "INCRBYFLOAT");

    /// <inheritdoc />
    public ValueTask<string> GetRangeAsync(string key, long start, long end, CancellationToken cancellationToken = default)
        => RespRead.RequiredText(ExecuteCommandAsync(CoverageCommands.GetRange("GETRANGE", key, start, end), cancellationToken), "GETRANGE");

    /// <inheritdoc />
    public ValueTask<string> GetSubstringAsync(string key, long start, long end, CancellationToken cancellationToken = default)
        => RespRead.RequiredText(ExecuteCommandAsync(CoverageCommands.GetRange("SUBSTR", key, start, end), cancellationToken), "SUBSTR");

    /// <inheritdoc />
    public ValueTask<long> SetRangeAsync(string key, long offset, string value, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(CoverageCommands.SetRange(key, offset, value), cancellationToken), "SETRANGE");

    /// <inheritdoc />
    public ValueTask<bool> SetManyIfNotExistsAsync(params (string Key, string Value)[] pairs)
        => RespRead.Flag(ExecuteCommandAsync(CoverageCommands.SetManyIfNotExists(pairs), CancellationToken.None), "MSETNX");

    /// <inheritdoc />
    public ValueTask<bool> SetBitAsync(string key, long offset, bool value, CancellationToken cancellationToken = default)
        => RespRead.Flag(ExecuteCommandAsync(CoverageCommands.SetBit(key, offset, value), cancellationToken), "SETBIT");

    /// <inheritdoc />
    public ValueTask<bool> GetBitAsync(string key, long offset, CancellationToken cancellationToken = default)
        => RespRead.Flag(ExecuteCommandAsync(CoverageCommands.GetBit(key, offset), cancellationToken), "GETBIT");

    /// <inheritdoc />
    public ValueTask<long> BitCountAsync(string key, long? start = null, long? end = null, BitUnit unit = BitUnit.Byte, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(CoverageCommands.BitCount(key, start, end, unit), cancellationToken), "BITCOUNT");

    /// <inheritdoc />
    public ValueTask<long> BitPositionAsync(string key, bool bit, long? start = null, long? end = null, BitUnit unit = BitUnit.Byte, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(CoverageCommands.BitPosition(key, bit, start, end, unit), cancellationToken), "BITPOS");

    /// <inheritdoc />
    public ValueTask<long> BitOperationAsync(BitOperation operation, string destination, IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(CoverageCommands.BitOperation(operation, destination, keys), cancellationToken), "BITOP");

    /// <inheritdoc />
    public ValueTask<bool> HyperLogLogAddAsync(string key, params string[] elements)
        => RespRead.Flag(ExecuteCommandAsync(CoverageCommands.Members("PFADD", key, elements), CancellationToken.None), "PFADD");

    /// <inheritdoc />
    public ValueTask<long> HyperLogLogCountAsync(params string[] keys)
        => RespRead.Integer(ExecuteCommandAsync(CoverageCommands.Keys("PFCOUNT", keys), CancellationToken.None), "PFCOUNT");

    /// <inheritdoc />
    public ValueTask HyperLogLogMergeAsync(string destination, params string[] sources)
        => RespRead.Ok(ExecuteCommandAsync(CoverageCommands.Members("PFMERGE", destination, sources), CancellationToken.None));

    /// <inheritdoc />
    public ValueTask<long> PushListLeftCappedAsync(string key, long cap, params string[] values)
        => RespRead.Integer(ExecuteCommandAsync(CoverageCommands.Capped("LPUSHCAP", key, cap, values), CancellationToken.None), "LPUSHCAP");

    /// <inheritdoc />
    public ValueTask<long> PushListRightCappedAsync(string key, long cap, params string[] values)
        => RespRead.Integer(ExecuteCommandAsync(CoverageCommands.Capped("RPUSHCAP", key, cap, values), CancellationToken.None), "RPUSHCAP");

    /// <inheritdoc />
    public ValueTask<KeyValuePair<string, IReadOnlyList<string>>?> PopListManyBlockingAsync(IReadOnlyList<string> keys, bool fromLeft, TimeSpan timeout, long count = 1, CancellationToken cancellationToken = default)
        => RespRead.ListPop(ExecuteCommandAsync(CoverageCommands.PopListManyBlocking(keys, fromLeft, timeout, count), cancellationToken), "BLMPOP");

    /// <inheritdoc />
    public ValueTask<KeyValuePair<string, IReadOnlyList<string>>?> PopListManyBlockingAsync(IReadOnlyList<string> keys, ListSide side, TimeSpan timeout, long count = 1, CancellationToken cancellationToken = default)
        => PopListManyBlockingAsync(keys, side == ListSide.Left, timeout, count, cancellationToken);

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<bool>> SetContainsManyAsync(string key, params string[] members)
        => RespRead.Flags(ExecuteCommandAsync(CoverageCommands.Members("SMISMEMBER", key, members), CancellationToken.None), "SMISMEMBER");

    /// <inheritdoc />
    public ValueTask<long> GetSetIntersectionCountAsync(IReadOnlyList<string> keys, long limit = 0, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(CoverageCommands.SetIntersectionCount(keys, limit), cancellationToken), "SINTERCARD");

    /// <inheritdoc />
    public ValueTask<SetScanPage> ScanSetAsync(string key, long cursor, string? pattern = null, int? count = null, CancellationToken cancellationToken = default)
        => RespRead.SetScan(ExecuteCommandAsync(CoverageCommands.KeyScan("SSCAN", key, cursor, pattern, count), cancellationToken));

    /// <inheritdoc />
    public ValueTask<ScanPage> ScanAsync(long cursor, string? pattern, int? count, KeyType type, CancellationToken cancellationToken = default)
        => ScanAsync(cursor, pattern, count, Keywords.Word(type), cancellationToken);

    /// <inheritdoc />
    public ValueTask<ScanPage> ScanAsync(long cursor, string? pattern, int? count, string? type, CancellationToken cancellationToken = default)
        => ScanPageOf(ExecuteCommandAsync(CoverageCommands.Scan(cursor, pattern, count, type), cancellationToken));

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> GetSortedSetRangeByLexAsync(string key, string min, string max, bool descending, long offset = 0, long count = -1, CancellationToken cancellationToken = default)
        => RespRead.Strings(ExecuteCommandAsync(CoverageCommands.RangeByLex(key, min, max, descending, offset, count), cancellationToken), "ZRANGEBYLEX");

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> GetSortedSetRangeByScoreAsync(string key, string min, string max, bool descending = false, long offset = 0, long count = -1, CancellationToken cancellationToken = default)
        => RespRead.Strings(ExecuteCommandAsync(CoverageCommands.RangeByScore(key, min, max, descending, offset, count), cancellationToken), "ZRANGE");

    /// <inheritdoc />
    public ValueTask<long> CountSortedSetByScoreAsync(string key, string min, string max, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(new RespCommand("ZCOUNT", key, min, max), cancellationToken), "ZCOUNT");

    /// <inheritdoc />
    public ValueTask<long> CountSortedSetByLexAsync(string key, string min, string max, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(new RespCommand("ZLEXCOUNT", key, min, max), cancellationToken), "ZLEXCOUNT");

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<double?>> GetSortedSetScoresAsync(string key, params string[] members)
        => RespRead.Scores(ExecuteCommandAsync(CoverageCommands.Members("ZMSCORE", key, members), CancellationToken.None), "ZMSCORE");

    /// <inheritdoc />
    public ValueTask<string?> GetSortedSetRandomMemberAsync(string key, CancellationToken cancellationToken = default)
        => RespRead.Text(ExecuteCommandAsync(new RespCommand("ZRANDMEMBER", key), cancellationToken));

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> GetSortedSetRandomMembersAsync(string key, long count, CancellationToken cancellationToken = default)
        => RespRead.Strings(ExecuteCommandAsync(CoverageCommands.RandomMembers("ZRANDMEMBER", key, count, null), cancellationToken), "ZRANDMEMBER");

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<SortedSetEntry>> GetSortedSetRandomMembersWithScoresAsync(string key, long count, CancellationToken cancellationToken = default)
        => RespRead.Entries(ExecuteCommandAsync(CoverageCommands.RandomMembers("ZRANDMEMBER", key, count, "WITHSCORES"), cancellationToken), "ZRANDMEMBER");

    /// <inheritdoc />
    public ValueTask<SortedSetScanPage> ScanSortedSetAsync(string key, long cursor, string? pattern = null, int? count = null, CancellationToken cancellationToken = default)
        => RespRead.SortedSetScan(ExecuteCommandAsync(CoverageCommands.KeyScan("ZSCAN", key, cursor, pattern, count), cancellationToken));

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<SortedSetEntry>> GetSortedSetUnionAsync(IReadOnlyList<string> keys, IReadOnlyList<double>? weights = null, SortedSetAggregate? aggregate = null, CancellationToken cancellationToken = default)
        => RespRead.Entries(ExecuteCommandAsync(CoverageCommands.Combine("ZUNION", null, keys, weights, aggregate, true), cancellationToken), "ZUNION");

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<SortedSetEntry>> GetSortedSetIntersectionAsync(IReadOnlyList<string> keys, IReadOnlyList<double>? weights = null, SortedSetAggregate? aggregate = null, CancellationToken cancellationToken = default)
        => RespRead.Entries(ExecuteCommandAsync(CoverageCommands.Combine("ZINTER", null, keys, weights, aggregate, true), cancellationToken), "ZINTER");

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<SortedSetEntry>> GetSortedSetDifferenceAsync(IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
        => RespRead.Entries(ExecuteCommandAsync(CoverageCommands.Combine("ZDIFF", null, keys, null, null, true), cancellationToken), "ZDIFF");

    /// <inheritdoc />
    public ValueTask<long> StoreSortedSetUnionAsync(string destination, IReadOnlyList<string> keys, IReadOnlyList<double>? weights = null, SortedSetAggregate? aggregate = null, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(CoverageCommands.Combine("ZUNIONSTORE", destination, keys, weights, aggregate, false), cancellationToken), "ZUNIONSTORE");

    /// <inheritdoc />
    public ValueTask<long> StoreSortedSetIntersectionAsync(string destination, IReadOnlyList<string> keys, IReadOnlyList<double>? weights = null, SortedSetAggregate? aggregate = null, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(CoverageCommands.Combine("ZINTERSTORE", destination, keys, weights, aggregate, false), cancellationToken), "ZINTERSTORE");

    /// <inheritdoc />
    public ValueTask<long> StoreSortedSetDifferenceAsync(string destination, IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(CoverageCommands.Combine("ZDIFFSTORE", destination, keys, null, null, false), cancellationToken), "ZDIFFSTORE");

    /// <inheritdoc />
    public ValueTask<string?> GetHashRandomFieldAsync(string key, CancellationToken cancellationToken = default)
        => RespRead.Text(ExecuteCommandAsync(new RespCommand("HRANDFIELD", key), cancellationToken));

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> GetHashRandomFieldsAsync(string key, long count, CancellationToken cancellationToken = default)
        => RespRead.Strings(ExecuteCommandAsync(CoverageCommands.RandomMembers("HRANDFIELD", key, count, null), cancellationToken), "HRANDFIELD");

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<KeyValuePair<string, string>>> GetHashRandomFieldsWithValuesAsync(string key, long count, CancellationToken cancellationToken = default)
        => RespRead.Pairs(ExecuteCommandAsync(CoverageCommands.RandomMembers("HRANDFIELD", key, count, "WITHVALUES"), cancellationToken), "HRANDFIELD");

    /// <inheritdoc />
    public ValueTask<double> IncrementHashFieldByFloatAsync(string key, string field, double delta, CancellationToken cancellationToken = default)
        => RespRead.Double(ExecuteCommandAsync(CoverageCommands.IncrementHashFieldByFloat(key, field, delta), cancellationToken), "HINCRBYFLOAT");

    /// <inheritdoc />
    public ValueTask ReserveBloomFilterAsync(string key, double errorRate, long capacity, long? expansion, bool nonScaling = false, CancellationToken cancellationToken = default)
        => RespRead.Ok(ExecuteCommandAsync(CoverageCommands.ReserveBloom(key, errorRate, capacity, expansion, nonScaling), cancellationToken));

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<bool>> AddBloomFilterManyAsync(string key, params string[] items)
        => RespRead.Flags(ExecuteCommandAsync(CoverageCommands.Members("BF.MADD", key, items), CancellationToken.None), "BF.MADD");

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<bool>> BloomFilterContainsManyAsync(string key, params string[] items)
        => RespRead.Flags(ExecuteCommandAsync(CoverageCommands.Members("BF.MEXISTS", key, items), CancellationToken.None), "BF.MEXISTS");

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<bool>> InsertBloomFilterAsync(string key, IReadOnlyList<string> items, BloomInsertOptions? options = null, CancellationToken cancellationToken = default)
        => RespRead.Flags(ExecuteCommandAsync(CoverageCommands.InsertBloom(key, items, options), cancellationToken), "BF.INSERT");

    /// <inheritdoc />
    public ValueTask<BloomFilterInfo> GetBloomFilterInfoAsync(string key, CancellationToken cancellationToken = default)
        => RespRead.BloomInfo(ExecuteCommandAsync(new RespCommand("BF.INFO", key), cancellationToken));

    /// <inheritdoc />
    public ValueTask<long> GetBloomFilterCardinalityAsync(string key, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(new RespCommand("BF.CARD", key), cancellationToken), "BF.CARD");

    /// <inheritdoc />
    public ValueTask<bool> JsonSetAsync(string key, string path, string json, SetMode mode, CancellationToken cancellationToken = default)
        => RespRead.NotNull(ExecuteCommandAsync(CoverageCommands.JsonSet(key, path, json, mode), cancellationToken));

    /// <inheritdoc />
    public ValueTask<long?> JsonObjectLengthAsync(string key, string path = "$", CancellationToken cancellationToken = default)
        => RespRead.OptionalInteger(ExecuteCommandAsync(new RespCommand("JSON.OBJLEN", key, path), cancellationToken), "JSON.OBJLEN");

    /// <inheritdoc />
    public ValueTask<long?> JsonStringAppendAsync(string key, string path, string json, CancellationToken cancellationToken = default)
        => RespRead.OptionalInteger(ExecuteCommandAsync(new RespCommand("JSON.STRAPPEND", key, path, json), cancellationToken), "JSON.STRAPPEND");

    /// <inheritdoc />
    public ValueTask<string?> JsonMultiplyAsync(string key, string path, double by, CancellationToken cancellationToken = default)
        => RespRead.Text(ExecuteCommandAsync(new RespCommand("JSON.NUMMULTBY", key, path, RespRead.Number(by)), cancellationToken));

    /// <inheritdoc />
    public ValueTask<long?> JsonArrayInsertAsync(string key, string path, long index, params string[] json)
        => RespRead.OptionalInteger(ExecuteCommandAsync(CoverageCommands.JsonArrayInsert(key, path, index, json), CancellationToken.None), "JSON.ARRINSERT");

    /// <inheritdoc />
    public ValueTask<string?> JsonArrayPopAsync(string key, string path = "$", long index = -1, CancellationToken cancellationToken = default)
        => RespRead.Text(ExecuteCommandAsync(new RespCommand("JSON.ARRPOP", key, path, index), cancellationToken));

    /// <inheritdoc />
    public ValueTask<long?> JsonArrayTrimAsync(string key, string path, long start, long stop, CancellationToken cancellationToken = default)
        => RespRead.OptionalInteger(ExecuteCommandAsync(new RespCommand("JSON.ARRTRIM", key, path, start, stop), cancellationToken), "JSON.ARRTRIM");

    /// <inheritdoc />
    public ValueTask<long?> JsonArrayIndexAsync(string key, string path, string scalar, long start = 0, long stop = 0, CancellationToken cancellationToken = default)
        => RespRead.OptionalInteger(ExecuteCommandAsync(CoverageCommands.JsonArrayIndex(key, path, scalar, start, stop), cancellationToken), "JSON.ARRINDEX");

    /// <inheritdoc />
    public ValueTask<long> JsonForgetAsync(string key, string path = "$", CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(new RespCommand("JSON.FORGET", key, path), cancellationToken), "JSON.FORGET");

    /// <inheritdoc />
    public ValueTask<bool> JMergeAsync(string key, string path, string patch, CancellationToken cancellationToken = default)
        => RespRead.NotNull(ExecuteCommandAsync(new RespCommand("JMERGE", key, path, patch), cancellationToken));

    /// <inheritdoc />
    public ValueTask JsonCreateIndexAsync(string name, string prefix, string path, JsonIndexType type = JsonIndexType.String, CancellationToken cancellationToken = default)
        => RespRead.Ok(ExecuteCommandAsync(CoverageCommands.JsonCreateIndex(name, prefix, path, type), cancellationToken));

    /// <inheritdoc />
    public ValueTask<bool> JsonDropIndexAsync(string name, CancellationToken cancellationToken = default)
        => RespRead.Flag(ExecuteCommandAsync(new RespCommand("JSON.INDEX", "DROP", name), cancellationToken), "JSON.INDEX");

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> JsonListIndexesAsync(CancellationToken cancellationToken = default)
        => RespRead.Strings(ExecuteCommandAsync(new RespCommand("JSON.INDEX", "LIST"), cancellationToken), "JSON.INDEX");

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> JsonFindAsync(string index, string value, int? count = null, string? after = null, CancellationToken cancellationToken = default)
        => RespRead.Strings(ExecuteCommandAsync(CoverageCommands.JsonFind(index, value, count, after), cancellationToken), "JSON.FIND");

    /// <inheritdoc />
    public ValueTask<string> RuvioAsync(CancellationToken cancellationToken = default)
        => RespRead.RequiredText(ExecuteCommandAsync(new RespCommand("RUVIO"), cancellationToken), "RUVIO");

    /// <inheritdoc />
    public ValueTask<string> GetRuvioOwnerAsync(CancellationToken cancellationToken = default)
        => RespRead.RequiredText(ExecuteCommandAsync(new RespCommand("RUVIO", "OWNER"), cancellationToken), "RUVIO");

    /// <inheritdoc />
    public ValueTask MoveSlotsAsync(int first, int last, int shard, CancellationToken cancellationToken = default)
        => RespRead.Ok(ExecuteCommandAsync(new RespCommand("RUVIO", "MOVESLOTS", RespRead.Number(first) + "-" + RespRead.Number(last), RespRead.Number(shard)), cancellationToken));

    /// <inheritdoc />
    public ValueTask<IReadOnlyDictionary<string, string>> GetMoveSlotsStatusAsync(CancellationToken cancellationToken = default)
        => RespRead.Map(ExecuteCommandAsync(new RespCommand("RUVIO", "MOVESLOTS", "STATUS"), cancellationToken), "RUVIO");

    /// <inheritdoc />
    public ValueTask<RespValue> GetCommandsAsync(CancellationToken cancellationToken = default)
        => ExecuteCommandAsync(new RespCommand("COMMAND"), cancellationToken);

    /// <inheritdoc />
    public ValueTask<long> GetCommandCountAsync(CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(new RespCommand("COMMAND", "COUNT"), cancellationToken), "COMMAND");

    /// <inheritdoc />
    public ValueTask<RespValue> GetCommandInfoAsync(params string[] names)
        => ExecuteCommandAsync(CoverageCommands.Words("COMMAND", "INFO", names), CancellationToken.None);

    /// <inheritdoc />
    public ValueTask<RespValue> GetCommandDocsAsync(params string[] names)
        => ExecuteCommandAsync(CoverageCommands.Words("COMMAND", "DOCS", names), CancellationToken.None);

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<RespValue>> GetLatencyLatestAsync(CancellationToken cancellationToken = default)
        => ItemsOf(ExecuteCommandAsync(new RespCommand("LATENCY", "LATEST"), cancellationToken));

    /// <inheritdoc />
    public ValueTask<long> WaitForReplicasAsync(long replicas, TimeSpan timeout, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(CoverageCommands.Wait(replicas, timeout), cancellationToken), "WAIT");

    /// <inheritdoc />
    public ValueTask AuthenticateAsync(string password, CancellationToken cancellationToken = default)
        => RespRead.Ok(ExecuteCommandAsync(new RespCommand("AUTH", password), cancellationToken));

    /// <inheritdoc />
    public ValueTask AuthenticateAsync(string username, string password, CancellationToken cancellationToken = default)
        => RespRead.Ok(ExecuteCommandAsync(new RespCommand("AUTH", username, password), cancellationToken));

    /// <inheritdoc />
    public ValueTask<long> KillClientAsync(string address, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(new RespCommand("CLIENT", "KILL", "ADDR", address), cancellationToken), "CLIENT");

    /// <inheritdoc />
    public ValueTask<IdempotencyBegin> TryBeginIdempotencyAsync(string key, string fingerprint, string owner, TimeSpan ttl, CancellationToken cancellationToken = default)
        => RespRead.Idempotency(ExecuteCommandAsync(new RespCommand("IDEM", key, "BEGIN", fingerprint, owner, RespRead.Milliseconds(ttl)), cancellationToken));

    /// <inheritdoc />
    public ValueTask<bool> AbortIdempotencyAsync(string key, string fingerprint, string owner, CancellationToken cancellationToken = default)
        => RespRead.Flag(ExecuteCommandAsync(new RespCommand("IDEM", key, "ABORT", fingerprint, owner), cancellationToken), "IDEM");
}
