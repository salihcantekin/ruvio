// Typed methods for the deadline that is set only when a command creates its key (EX / PX), and for the
// EXPIRE family with a condition. Frames are built by TtlCommands.

namespace Ruvio.Client;

public sealed partial class RuvioClient
{
    /// <inheritdoc />
    public ValueTask<bool> ExpireAsync(string key, TimeSpan ttl, ExpireCondition condition, CancellationToken cancellationToken = default)
        => RespRead.Flag(ExecuteCommandAsync(TtlCommands.Expire(key, ttl, condition), cancellationToken), "EXPIRE");

    /// <inheritdoc />
    public ValueTask<bool> ExpireAtAsync(string key, DateTimeOffset when, ExpireCondition condition, CancellationToken cancellationToken = default)
        => RespRead.Flag(ExecuteCommandAsync(TtlCommands.ExpireAt(key, when, condition), cancellationToken), "EXPIREAT");

    /// <inheritdoc />
    public ValueTask<long> IncrementAsync(string key, TimeSpan ttl, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(TtlCommands.Increment(key, ttl), cancellationToken), "INCR");

    /// <inheritdoc />
    public ValueTask<long> DecrementAsync(string key, TimeSpan ttl, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(TtlCommands.Decrement(key, ttl), cancellationToken), "DECR");

    /// <inheritdoc />
    public ValueTask<long> IncrementByAsync(string key, long delta, TimeSpan ttl, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(TtlCommands.IncrementBy(key, delta, ttl), cancellationToken), "INCRBY");

    /// <inheritdoc />
    public ValueTask<long> DecrementByAsync(string key, long delta, TimeSpan ttl, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(TtlCommands.DecrementBy(key, delta, ttl), cancellationToken), "DECRBY");

    /// <inheritdoc />
    public ValueTask<long?> IncrementWithLimitAsync(string key, long delta, long limit, TimeSpan ttl, CancellationToken cancellationToken = default)
        => RespRead.OptionalInteger(ExecuteCommandAsync(TtlCommands.IncrementWithLimit(key, delta, limit, ttl), cancellationToken), "INCRBY");

    /// <inheritdoc />
    public ValueTask<double> IncrementByFloatAsync(string key, double delta, TimeSpan ttl, CancellationToken cancellationToken = default)
        => RespRead.Double(ExecuteCommandAsync(TtlCommands.IncrementByFloat(key, delta, ttl), cancellationToken), "INCRBYFLOAT");

    /// <inheritdoc />
    public ValueTask<long> IncrementHashFieldAsync(string key, string field, long delta, TimeSpan ttl, CancellationToken cancellationToken = default)
        => RespRead.Integer(ExecuteCommandAsync(TtlCommands.IncrementHashField(key, field, delta, ttl), cancellationToken), "HINCRBY");

    /// <inheritdoc />
    public ValueTask<double> IncrementSortedSetAsync(string key, string member, double delta, TimeSpan ttl, CancellationToken cancellationToken = default)
        => RespRead.Double(ExecuteCommandAsync(TtlCommands.IncrementSortedSet(key, member, delta, ttl), cancellationToken), "ZINCRBY");

    /// <inheritdoc />
    public ValueTask<bool> SetBitAsync(string key, long offset, bool value, TimeSpan ttl, CancellationToken cancellationToken = default)
        => RespRead.Flag(ExecuteCommandAsync(TtlCommands.SetBit(key, offset, value, ttl), cancellationToken), "SETBIT");

    /// <inheritdoc />
    public ValueTask<long> AddSortedSetAsync(string key, TimeSpan ttl, SortedSetAddFlags flags, params SortedSetEntry[] entries)
        => RespRead.Integer(ExecuteCommandAsync(TtlCommands.SortedSetAdd(key, ttl, flags, entries), CancellationToken.None), "ZADD");

    /// <inheritdoc />
    public ValueTask<long> AddSortedSetAsync(string key, TimeSpan ttl, SortedSetAddFlags flags, params (string Member, double Score)[] entries)
        => AddSortedSetAsync(key, ttl, flags, Array.ConvertAll(entries, entry => new SortedSetEntry(entry.Member, entry.Score)));

    /// <inheritdoc />
    public ValueTask<long> AddGeoAsync(string key, TimeSpan ttl, params (double Longitude, double Latitude, string Member)[] places)
        => RespRead.Integer(ExecuteCommandAsync(TtlCommands.GeoAdd(key, ttl, places), CancellationToken.None), "GEOADD");

    /// <inheritdoc />
    public ValueTask<string> AddStreamEntryAsync(string key, TimeSpan ttl, string id, params (string Field, string Value)[] fields)
        => RespRead.RequiredText(ExecuteCommandAsync(TtlCommands.StreamAdd(key, ttl, id, fields), CancellationToken.None), "XADD");

    /// <inheritdoc />
    public ValueTask<bool> JsonSetAsync(string key, string path, string json, SetMode mode, TimeSpan ttl, CancellationToken cancellationToken = default)
        => RespRead.NotNull(ExecuteCommandAsync(TtlCommands.JsonSet(key, path, json, mode, ttl), cancellationToken));
}
