using System.Buffers;
using System.Buffers.Text;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using System.Globalization;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Options;
using Ruvio.Client;

namespace Ruvio.Extensions.Caching;

/// <summary>
/// A binary-safe distributed cache. Set and the common get are one native
/// command. A sliding renewal is a second command that expires only when the
/// stored bytes are still the ones just read, so a replacement is left alone.
/// The injected client is not disposed by this provider.
/// </summary>
public sealed class RuvioDistributedCache : IDistributedCache
{
    // One compiled script per shard. It does not parse the cache envelope.
    private const string Renew = """
        if ruvio.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
        if ARGV[2] == '0' then return ruvio.call('DEL', KEYS[1]) end
        return ruvio.call('PEXPIREAT', KEYS[1], ARGV[2])
        """;

    private static readonly ReadOnlyMemory<byte> Eval = "EVAL"u8.ToArray();
    private static readonly ReadOnlyMemory<byte> RenewScript = RuvioCacheOptions.Utf8.GetBytes(Renew);
    private static readonly ReadOnlyMemory<byte> One = "1"u8.ToArray();
    private static readonly ReadOnlyMemory<byte> GetCommand = "GET"u8.ToArray();
    private static readonly ReadOnlyMemory<byte> SetCommand = "SET"u8.ToArray();
    private static readonly ReadOnlyMemory<byte> Px = "PX"u8.ToArray();
    private static readonly ReadOnlyMemory<byte> Pxat = "PXAT"u8.ToArray();
    private static readonly ReadOnlyMemory<byte> Delete = "DEL"u8.ToArray();
    private static readonly ReadOnlyMemory<byte> FillGetCommand = "FILLGET"u8.ToArray();
    private static readonly ReadOnlyMemory<byte> FillSetCommand = "FILLSET"u8.ToArray();
    private static readonly ReadOnlyMemory<byte> PersistCommand = "PERSIST"u8.ToArray();
    // Lenient like Microsoft's SetString/GetString helpers: a value that is not valid UTF-8 decodes with replacement characters.
    private static readonly UTF8Encoding Text = new(false);
    private readonly IRuvioClient _client;
    private readonly byte[] _prefix;
    private readonly int _maxValueBytes;
    private readonly int _maxKeyBytes;
    private readonly long _fillMilliseconds;
    private readonly long _waitMilliseconds;
    private readonly RuvioOutageOptions _outage;
    private static readonly System.Diagnostics.Metrics.Meter Meter = new("Ruvio.Extensions.Caching");
    private static readonly System.Diagnostics.Metrics.Counter<long> Outages = Meter.CreateCounter<long>("ruvio.cache.outage");
    private static readonly System.Diagnostics.Metrics.Counter<long> Hits = Meter.CreateCounter<long>("ruvio.cache.hit");
    private static readonly System.Diagnostics.Metrics.Counter<long> Misses = Meter.CreateCounter<long>("ruvio.cache.miss");

    /// <summary>Constructs a provider with validated, snapshotted options and a borrowed client.</summary>
    public RuvioDistributedCache(IRuvioClient client, IOptions<RuvioCacheOptions> options)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(options);
        RuvioCacheOptions settings = options.Value;

        settings.Validate();
        _client = client;
        _prefix = RuvioCacheOptions.Utf8.GetBytes(settings.InstanceName + RuvioCacheOptions.VersionPrefix);
        _maxValueBytes = settings.MaxValueBytes;
        _maxKeyBytes = settings.MaxKeyBytes;
        _fillMilliseconds = (long)settings.FillTimeout.TotalMilliseconds;
        _waitMilliseconds = (long)settings.FillWait.TotalMilliseconds;
        _outage = settings.Outage;
    }

    // Without an outage mode or a circuit breaker the operations run unwrapped.
    private bool Guarded => _outage.Mode == RuvioOutageMode.FailOpen || _outage.CircuitBreaker is not null;

    // Enabled is one field read, so a cache nobody measures pays no more than that per lookup.
    private static void CountLookup(bool hit, string operation)
    {
        var counter = hit ? Hits : Misses;

        if (counter.Enabled)
            counter.Add(1, new KeyValuePair<string, object?>("operation", operation));
    }

    private void CountOutage(string operation, Exception? cause) =>
        Outages.Add(1, new System.Diagnostics.TagList
        {
            { "operation", operation },
            { "mode", _outage.Mode == RuvioOutageMode.FailOpen ? "fail_open" : "fail_closed" },
            { "reason", cause is null ? "circuit_open" : "error" },
        });

    /// <inheritdoc />
    public byte[]? Get(string key) => Task.Run(() => GetAsync(key)).GetAwaiter().GetResult();

    /// <inheritdoc />
    public async Task<byte[]?> GetAsync(string key, CancellationToken token = default)
        => await ReadAsync(key, 0, static (stored, offset, _) => stored.AsSpan(offset).ToArray(), token).ConfigureAwait(false);

    /// <summary>
    /// Reads the entry as a string, decoding the UTF-8 payload straight from the reply (no intermediate
    /// <c>byte[]</c>). Returns null on a miss. Same expiry handling as <see cref="GetAsync"/>.
    /// </summary>
    public async Task<string?> GetStringAsync(string key, CancellationToken token = default)
        => await ReadAsync(key, 0, static (stored, offset, _) => Text.GetString(stored, offset, stored.Length - offset), token)
            .ConfigureAwait(false);

    /// <summary>Reads and deserializes a JSON entry straight from the reply payload. Returns <c>default</c> on a miss.</summary>
    [RequiresUnreferencedCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    [RequiresDynamicCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    public async Task<T?> GetJsonAsync<T>(string key, JsonSerializerOptions? serializerOptions = null,
        CancellationToken token = default)
        => await ReadAsync(key, serializerOptions,
                static (stored, offset, options) => JsonSerializer.Deserialize<T>(stored.AsSpan(offset), options), token)
            .ConfigureAwait(false);

    /// <summary>Reads and deserializes a JSON entry with a source-generated <paramref name="typeInfo"/>.</summary>
    public async Task<T?> GetJsonAsync<T>(string key, JsonTypeInfo<T> typeInfo, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(typeInfo);

        return await ReadAsync(key, typeInfo,
                static (stored, offset, info) => JsonSerializer.Deserialize(stored.AsSpan(offset), info), token)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// One read plus expiry bookkeeping; <paramref name="convert"/> maps the stored bytes, payload offset and
    /// <paramref name="state"/> to the result. Callers pass a static lambda and hand its inputs in as state, so a read allocates no closure.
    /// </summary>
    private Task<T?> ReadAsync<T, TState>(string key, TState state, Func<byte[], int, TState, T> convert, CancellationToken token)
        => Guarded ? ReadGuardedAsync(key, state, convert, token) : ReadCoreAsync(key, state, convert, token);

    private async Task<T?> ReadGuardedAsync<T, TState>(string key, TState state, Func<byte[], int, TState, T> convert, CancellationToken token)
    {
        if (!_outage.TryEnter())
        {
            CountOutage("get", null);

            // A fail-open cache that cannot be reached has nothing: a miss.
            return _outage.Mode == RuvioOutageMode.FailOpen ? default : throw new RuvioCircuitOpenException(_outage.RetryAfter);
        }

        try
        {
            T? value = await ReadCoreAsync(key, state, convert, token).ConfigureAwait(false);
            _outage.Succeeded();

            return value;
        }
        catch (Exception error) when (_outage.Failed(error, token) && _outage.Mode == RuvioOutageMode.FailOpen)
        {
            CountOutage("get", error);

            return default;
        }
    }

    private async Task<T?> ReadCoreAsync<T, TState>(string key, TState state, Func<byte[], int, TState, T> convert, CancellationToken token)
    {
        ReadOnlyMemory<byte> encoded = Key(key);

        token.ThrowIfCancellationRequested();
        // One async frame on the common path: the read is inlined and the expiry bookkeeping only awaits when it must touch.
        RespValue reply = await _client.ExecuteBinaryAsync([GetCommand, encoded], token).ConfigureAwait(false);

        CheckReply(reply);

        if (reply.Kind == RespKind.Null)
        {
            CountLookup(false, "get");

            return default;
        }

        if (reply.Kind != RespKind.Bulk)
            throw UnexpectedReply();

        byte[] stored = reply.Bytes!;
        int offset = await Decode(encoded, stored, token).ConfigureAwait(false);

        CountLookup(offset >= 0, "get");

        return offset < 0 ? default : convert(stored, offset, state);
    }

    /// <summary>Applies the envelope's absolute and sliding expiry to bytes just read, and returns the payload offset (-1 when expired).</summary>
    private async Task<int> Decode(ReadOnlyMemory<byte> encoded, byte[] stored, CancellationToken token)
    {
        (long absolute, long sliding, int payload) = Parse(stored);
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        if (absolute > 0 && absolute <= now)
        {
            await Touch(encoded, stored, 0, token).ConfigureAwait(false);

            return -1;
        }

        if (sliding > 0)
        {
            long deadline = absolute > 0 ? Math.Min(absolute, now + sliding) : now + sliding;

            await Touch(encoded, stored, deadline <= now ? 0 : deadline, token).ConfigureAwait(false);
        }

        return payload;
    }

    /// <inheritdoc />
    public void Set(string key, byte[] value, DistributedCacheEntryOptions options)
        => Task.Run(() => SetAsync(key, value, options)).GetAwaiter().GetResult();

    /// <inheritdoc />
    public async Task SetAsync(string key, byte[] value, DistributedCacheEntryOptions options,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(value);
        CheckSize(value.Length);

        await WriteAsync(key, options, value, static (absolute, sliding, bytes) => Envelope(absolute, sliding, bytes), token)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Stores a string as UTF-8, encoding it straight into the stored record (no intermediate <c>byte[]</c>).
    /// Readable with <see cref="GetStringAsync"/> and with <see cref="GetAsync"/> (the UTF-8 bytes).
    /// </summary>
    public async Task SetStringAsync(string key, string value, DistributedCacheEntryOptions options,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(value);
        CheckSize(Text.GetByteCount(value));

        await WriteAsync(key, options, value, static (absolute, sliding, text) => EnvelopeString(absolute, sliding, text), token)
            .ConfigureAwait(false);
    }

    /// <summary>Serializes <paramref name="value"/> to JSON directly into the stored record and writes it.</summary>
    [RequiresUnreferencedCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    [RequiresDynamicCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    public async Task SetJsonAsync<T>(string key, T value, DistributedCacheEntryOptions options,
        JsonSerializerOptions? serializerOptions = null, CancellationToken token = default)
        => await WriteAsync(key, options, (cache: this, value, serializerOptions),
            static (absolute, sliding, state) => state.cache.EnvelopeJson(absolute, sliding, (state.value, state.serializerOptions),
                static (w, input) => JsonSerializer.Serialize(w, input.value, input.serializerOptions), state.serializerOptions),
            token).ConfigureAwait(false);

    /// <summary>Same as the options overload, with a source-generated <paramref name="typeInfo"/>.</summary>
    public async Task SetJsonAsync<T>(string key, T value, JsonTypeInfo<T> typeInfo, DistributedCacheEntryOptions options,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(typeInfo);

        await WriteAsync(key, options, (cache: this, value, typeInfo),
            static (absolute, sliding, state) => state.cache.EnvelopeJson(absolute, sliding, (state.value, state.typeInfo),
                static (w, input) => JsonSerializer.Serialize(w, input.value, input.typeInfo), state.typeInfo.Options),
            token).ConfigureAwait(false);
    }

    private Task WriteAsync<TState>(string key, DistributedCacheEntryOptions options, TState state,
        Func<long, long, TState, ReadOnlyMemory<byte>> envelope, CancellationToken token)
        => Guarded ? WriteGuardedAsync(key, options, state, envelope, token) : WriteCoreAsync(key, options, state, envelope, token);

    private async Task WriteGuardedAsync<TState>(string key, DistributedCacheEntryOptions options, TState state,
        Func<long, long, TState, ReadOnlyMemory<byte>> envelope, CancellationToken token)
    {
        if (!_outage.TryEnter())
        {
            CountOutage("set", null);

            if (_outage.Mode == RuvioOutageMode.FailOpen)
                return; // dropped: the value is simply not cached

            throw new RuvioCircuitOpenException(_outage.RetryAfter);
        }

        try
        {
            await WriteCoreAsync(key, options, state, envelope, token).ConfigureAwait(false);
            _outage.Succeeded();
        }
        catch (Exception error) when (_outage.Failed(error, token) && _outage.Mode == RuvioOutageMode.FailOpen)
        {
            CountOutage("set", error);
        }
    }

    private async Task WriteCoreAsync<TState>(string key, DistributedCacheEntryOptions options, TState state,
        Func<long, long, TState, ReadOnlyMemory<byte>> envelope, CancellationToken token)
    {
        ReadOnlyMemory<byte> encoded = Key(key);
        var prepared = Prepare(options, state, envelope);

        token.ThrowIfCancellationRequested();
        ReadOnlyMemory<byte>[] command = prepared.Expiry switch
        {
            (string kind, long when) => [SetCommand, encoded, prepared.Record, kind == "PX" ? Px : Pxat, Number(when)],
            null => [SetCommand, encoded, prepared.Record],
        };

        RespValue reply = await _client.ExecuteBinaryAsync(command, token).ConfigureAwait(false);

        CheckReply(reply);

        if (reply.Kind != RespKind.Simple || reply.Text != "OK")
            throw UnexpectedReply();
    }

    /// <summary>
    /// Returns the cached value or builds it with <paramref name="factory"/>, letting one caller across every
    /// process build it while the others wait for the result (<c>FILLGET</c> / <c>FILLSET</c>). Without this,
    /// every caller that misses runs the factory. The wait is a polling loop that never blocks the shared connection. A caller that waits out <see cref="RuvioCacheOptions.FillWait"/>
    /// builds and stores the value itself. If the factory throws, the marker lapses after
    /// <see cref="RuvioCacheOptions.FillTimeout"/>.
    /// </summary>
    public async Task<byte[]> GetOrCreateAsync(string key, Func<CancellationToken, Task<byte[]>> factory,
        DistributedCacheEntryOptions options, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(factory);

        return await GetOrCreateCore(key, factory, options,
            static (stored, offset) => stored.AsSpan(offset).ToArray(),
            (value, absolute, sliding) =>
            {
                CheckSize(value.Length);

                return Envelope(absolute, sliding, value);
            }, token).ConfigureAwait(false);
    }

    /// <summary>String flavour of <see cref="GetOrCreateAsync"/>: no <c>byte[]</c> conversion on either side.</summary>
    public async Task<string> GetOrCreateStringAsync(string key, Func<CancellationToken, Task<string>> factory,
        DistributedCacheEntryOptions options, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(factory);

        return await GetOrCreateCore(key, factory, options,
            static (stored, offset) => Text.GetString(stored, offset, stored.Length - offset),
            (value, absolute, sliding) =>
            {
                ArgumentNullException.ThrowIfNull(value);
                CheckSize(Text.GetByteCount(value));

                return EnvelopeString(absolute, sliding, value);
            }, token).ConfigureAwait(false);
    }

    /// <summary>JSON flavour of <see cref="GetOrCreateAsync"/>: serializes into and deserializes out of the stored record directly.</summary>
    [RequiresUnreferencedCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    [RequiresDynamicCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    public async Task<T> GetOrCreateJsonAsync<T>(string key, Func<CancellationToken, Task<T>> factory,
        DistributedCacheEntryOptions options, JsonSerializerOptions? serializerOptions = null,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(factory);

        return await GetOrCreateCore(key, factory, options,
            (stored, offset) => JsonSerializer.Deserialize<T>(stored.AsSpan(offset), serializerOptions)!,
            (value, absolute, sliding) => EnvelopeJson(absolute, sliding, (value, serializerOptions),
                static (w, input) => JsonSerializer.Serialize(w, input.value, input.serializerOptions), serializerOptions), token).ConfigureAwait(false);
    }

    /// <summary>Same as the options overload, with a source-generated <paramref name="typeInfo"/>.</summary>
    public async Task<T> GetOrCreateJsonAsync<T>(string key, Func<CancellationToken, Task<T>> factory,
        JsonTypeInfo<T> typeInfo, DistributedCacheEntryOptions options, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentNullException.ThrowIfNull(typeInfo);

        return await GetOrCreateCore(key, factory, options,
            (stored, offset) => JsonSerializer.Deserialize(stored.AsSpan(offset), typeInfo)!,
            (value, absolute, sliding) => EnvelopeJson(absolute, sliding, (value, typeInfo),
                static (w, input) => JsonSerializer.Serialize(w, input.value, input.typeInfo), typeInfo.Options), token).ConfigureAwait(false);
    }

    private Task<T> GetOrCreateCore<T>(string key, Func<CancellationToken, Task<T>> factory,
        DistributedCacheEntryOptions options, Func<byte[], int, T> read,
        Func<T, long, long, ReadOnlyMemory<byte>> envelope, CancellationToken token)
        => Guarded
            ? GetOrCreateGuardedAsync(key, factory, options, read, envelope, token)
            : GetOrCreateStoredAsync(key, factory, options, read, envelope, token);

    private async Task<T> GetOrCreateGuardedAsync<T>(string key, Func<CancellationToken, Task<T>> factory,
        DistributedCacheEntryOptions options, Func<byte[], int, T> read,
        Func<T, long, long, ReadOnlyMemory<byte>> envelope, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (!_outage.TryEnter())
        {
            CountOutage("get_or_create", null);

            if (_outage.Mode != RuvioOutageMode.FailOpen)
                throw new RuvioCircuitOpenException(_outage.RetryAfter);

            return await factory(token).ConfigureAwait(false);
        }

        // A storage outage must not run the factory twice, and an error from the factory is not an outage.
        T built = default!;
        bool factoryRan = false;
        bool factoryFailed = false;

        async Task<T> Tracked(CancellationToken cancellation)
        {
            // The lookup got its answer; whatever the factory throws says nothing about Ruvio.
            _outage.Succeeded();

            try
            {
                built = await factory(cancellation).ConfigureAwait(false);
                factoryRan = true;

                return built;
            }
            catch
            {
                factoryFailed = true;

                throw;
            }
        }

        try
        {
            T value = await GetOrCreateStoredAsync(key, Tracked, options, read, envelope, token).ConfigureAwait(false);
            _outage.Succeeded();

            return value;
        }
        catch (Exception error) when (!factoryFailed && _outage.Failed(error, token) && _outage.Mode == RuvioOutageMode.FailOpen)
        {
            CountOutage("get_or_create", error);

            return factoryRan ? built : await factory(token).ConfigureAwait(false);
        }
    }

    private async Task<T> GetOrCreateStoredAsync<T>(string key, Func<CancellationToken, Task<T>> factory,
        DistributedCacheEntryOptions options, Func<byte[], int, T> read,
        Func<T, long, long, ReadOnlyMemory<byte>> envelope, CancellationToken token)
    {
        ReadOnlyMemory<byte> encoded = Key(key);

        ArgumentNullException.ThrowIfNull(options);
        token.ThrowIfCancellationRequested();

        // FILLGET ... WAIT parks the whole connection on the server, so on this shared multiplexed connection the
        // waiters would block the builder's own FILLSET (and every other caller) until FillWait ran out.
        // Poll the plain FILLGET instead: a lapsed marker turns the next poll into FILL, a finished fill into HIT.
        byte[] fillMilliseconds = Number(_fillMilliseconds);
        long started = Stopwatch.GetTimestamp();
        int delay = 2;
        FillResult fill;

        while (true)
        {
            RespValue reply = await Execute([FillGetCommand, encoded, fillMilliseconds], token).ConfigureAwait(false);

            fill = FillResult.FromReply(reply);

            if (fill.State != FillState.Busy || Stopwatch.GetElapsedTime(started).TotalMilliseconds >= _waitMilliseconds)
                break;

            await Task.Delay(delay, token).ConfigureAwait(false);
            delay = Math.Min(delay * 2, 25);
        }

        if (fill.State is FillState.Hit or FillState.Stale)
        {
            int offset = await Decode(encoded, fill.Value!, token).ConfigureAwait(false);

            if (offset >= 0)
            {
                CountLookup(true, "get_or_create");

                return read(fill.Value!, offset);
            }
        }

        CountLookup(false, "get_or_create");
        T value = await factory(token).ConfigureAwait(false);

        if (fill.State == FillState.Fill)
        {
            var prepared = Prepare(options, (envelope, value),
                static (absolute, sliding, state) => state.envelope(state.value, absolute, sliding));
            // FILLSET needs a positive TTL; an entry without expiry is stored with one and then persisted.
            long ttl = prepared.Expiry switch
            {
                ("PX", long when) => when,
                (_, long when) => Math.Max(1, when - prepared.Now),
                null => 3_600_000,
            };
            bool persistent = prepared.Expiry is null;

            RespValue stored = await Execute(
                [FillSetCommand, encoded, prepared.Record, Number(ttl), Number(fill.Token!.Value)], token).ConfigureAwait(false);

            if (stored.Kind != RespKind.Integer)
                throw UnexpectedReply();

            // 0 means the marker lapsed while the factory ran; the value is still correct for this caller.
            if (stored.Integer == 1 && persistent)
                _ = await Execute([PersistCommand, encoded], token).ConfigureAwait(false);

            return value;
        }

        await WriteAsync(key, options, (envelope, value),
            static (absolute, sliding, state) => state.envelope(state.value, absolute, sliding), token).ConfigureAwait(false);

        return value;
    }

    /// <summary>Validates the expiry options, then builds the stored record with <paramref name="envelope"/>. Returns the current time used.</summary>
    private static (ReadOnlyMemory<byte> Record, (string Kind, long When)? Expiry, long Now) Prepare<TState>(
        DistributedCacheEntryOptions options, TState state, Func<long, long, TState, ReadOnlyMemory<byte>> envelope)
    {
        ArgumentNullException.ThrowIfNull(options);

        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        long absolute = options.AbsoluteExpiration?.ToUnixTimeMilliseconds() ?? 0;

        if (options.AbsoluteExpiration.HasValue && absolute <= now)
            throw new ArgumentOutOfRangeException(nameof(options), "Absolute expiration must be in the future at millisecond precision.");

        long relative = Milliseconds(options.AbsoluteExpirationRelativeToNow);
        long sliding = Milliseconds(options.SlidingExpiration);

        if (relative > DateTimeOffset.MaxValue.ToUnixTimeMilliseconds() - now)
            throw new ArgumentOutOfRangeException(nameof(options), "Relative expiration exceeds the supported date range.");

        if (relative > 0)
            absolute = absolute == 0 ? now + relative : Math.Min(absolute, now + relative);

        return (envelope(absolute, sliding, state), Expiry(now, absolute, sliding), now);
    }

    /// <inheritdoc />
    public void Refresh(string key) => Task.Run(() => RefreshAsync(key)).GetAwaiter().GetResult();

    /// <inheritdoc />
    public async Task RefreshAsync(string key, CancellationToken token = default)
    {
        _ = await GetAsync(key, token).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public void Remove(string key) => Task.Run(() => RemoveAsync(key)).GetAwaiter().GetResult();

    /// <inheritdoc />
    public Task RemoveAsync(string key, CancellationToken token = default)
        => Guarded ? RemoveGuardedAsync(key, token) : RemoveCoreAsync(key, token);

    private async Task RemoveGuardedAsync(string key, CancellationToken token)
    {
        if (!_outage.TryEnter())
        {
            CountOutage("remove", null);

            if (_outage.Mode == RuvioOutageMode.FailOpen)
                return;

            throw new RuvioCircuitOpenException(_outage.RetryAfter);
        }

        try
        {
            await RemoveCoreAsync(key, token).ConfigureAwait(false);
            _outage.Succeeded();
        }
        catch (Exception error) when (_outage.Failed(error, token) && _outage.Mode == RuvioOutageMode.FailOpen)
        {
            // The removal is lost: a stale entry can be read again once Ruvio is back.
            CountOutage("remove", error);
        }
    }

    private async Task RemoveCoreAsync(string key, CancellationToken token)
    {
        ReadOnlyMemory<byte> encoded = Key(key);

        token.ThrowIfCancellationRequested();
        RespValue reply = await Execute([Delete, encoded], token).ConfigureAwait(false);

        if (reply.Kind != RespKind.Integer || reply.Integer is < 0 or > 1)
            throw UnexpectedReply();
    }

    private async Task<byte[]?> Read(ReadOnlyMemory<byte> key, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        RespValue reply = await Execute([GetCommand, key], token).ConfigureAwait(false);

        if (reply.Kind == RespKind.Null)
            return null;

        if (reply.Kind != RespKind.Bulk)
            throw UnexpectedReply();

        return reply.Bytes;
    }

    private async Task Touch(ReadOnlyMemory<byte> key, byte[] stored, long deadline, CancellationToken token)
    {
        RespValue reply = await Execute(
            [Eval, RenewScript, One, key, stored, Number(deadline)], token).ConfigureAwait(false);

        if (reply.Kind != RespKind.Integer || reply.Integer < 0)
            throw UnexpectedReply();
    }

    private static (long Absolute, long Sliding, int Payload) Parse(byte[] stored)
    {
        ReadOnlySpan<byte> body = stored;
        ReadOnlySpan<byte> magic = "RDC1\n"u8;

        if (!body.StartsWith(magic))
            throw UnexpectedReply();

        body = body[magic.Length..];
        int first = body.IndexOf((byte)'\n');

        if (first <= 0 || !long.TryParse(body[..first], NumberStyles.None, CultureInfo.InvariantCulture, out long absolute))
            throw UnexpectedReply();

        body = body[(first + 1)..];
        int second = body.IndexOf((byte)'\n');

        if (second <= 0 || !long.TryParse(body[..second], NumberStyles.None, CultureInfo.InvariantCulture, out long sliding))
            throw UnexpectedReply();

        return (absolute, sliding, magic.Length + first + 1 + second + 1);
    }

    private const int MaxHeaderBytes = 5 + 20 + 1 + 20 + 1;

    /// <summary>Writes <c>RDC1\n{absolute}\n{sliding}\n</c> and returns its length.</summary>
    private static int WriteHeader(Span<byte> header, long absolute, long sliding)
    {
        ReadOnlySpan<byte> magic = "RDC1\n"u8;

        magic.CopyTo(header);
        int written = magic.Length;

        Utf8Formatter.TryFormat(absolute, header[written..], out int digits);
        written += digits;
        header[written++] = (byte)'\n';
        Utf8Formatter.TryFormat(sliding, header[written..], out digits);
        written += digits;
        header[written++] = (byte)'\n';

        return written;
    }

    /// <summary>The header followed by the payload, in one allocation.</summary>
    private static byte[] Envelope(long absolute, long sliding, byte[] value)
    {
        Span<byte> header = stackalloc byte[MaxHeaderBytes];
        int written = WriteHeader(header, absolute, sliding);
        byte[] record = new byte[written + value.Length];

        header[..written].CopyTo(record);
        value.CopyTo(record, written);

        return record;
    }

    /// <summary>The header followed by the string encoded as UTF-8 directly into the record, in one allocation.</summary>
    private static byte[] EnvelopeString(long absolute, long sliding, string value)
    {
        Span<byte> header = stackalloc byte[MaxHeaderBytes];
        int written = WriteHeader(header, absolute, sliding);
        byte[] record = new byte[written + Text.GetByteCount(value)];

        header[..written].CopyTo(record);
        Text.GetBytes(value, record.AsSpan(written));

        return record;
    }

    /// <summary>The header followed by JSON written straight into one growing buffer; no separate payload array.</summary>
    private ReadOnlyMemory<byte> EnvelopeJson<TState>(long absolute, long sliding, TState state,
        Action<Utf8JsonWriter, TState> write, JsonSerializerOptions? serializerOptions)
    {
        var buffer = new ArrayBufferWriter<byte>(256);
        Span<byte> header = buffer.GetSpan(MaxHeaderBytes);

        buffer.Advance(WriteHeader(header, absolute, sliding));
        int headerBytes = buffer.WrittenCount;

        using (var writer = new Utf8JsonWriter(buffer, new JsonWriterOptions
        {
            Encoder = serializerOptions?.Encoder,
            Indented = serializerOptions?.WriteIndented ?? false,
        }))
        {
            write(writer, state);
        }

        CheckSize(buffer.WrittenCount - headerBytes);

        return buffer.WrittenMemory;
    }

    private void CheckSize(int payloadBytes)
    {
        if (payloadBytes > _maxValueBytes)
            throw new ArgumentException("Cache value exceeds MaxValueBytes.", "value");
    }

    /// <summary>PX when the sliding window is sooner, otherwise PXAT for an absolute deadline.</summary>
    private static (string Kind, long When)? Expiry(long now, long absolute, long sliding)
    {
        if (sliding > 0 && (absolute == 0 || now + sliding < absolute))
            return ("PX", sliding);

        if (absolute > 0)
            return ("PXAT", absolute);

        return null;
    }

    private static void CheckReply(RespValue reply)
    {
        if (reply.Kind == RespKind.Error)
            throw new RuvioException(reply.Text ?? "Cache command failed.", isServerReply: true);
    }

    private async Task<RespValue> Execute(IReadOnlyList<ReadOnlyMemory<byte>> arguments, CancellationToken token)
    {
        RespValue reply = await _client.ExecuteBinaryAsync(arguments, token).ConfigureAwait(false);

        if (reply.Kind == RespKind.Error)
            throw new RuvioException(reply.Text ?? "Cache command failed.", isServerReply: true);

        return reply;
    }

    private byte[] Key(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        // The prefix ends in ASCII, so encoding the parts separately equals encoding the concatenation.
        int keyBytes = RuvioCacheOptions.Utf8.GetByteCount(key);

        if (_prefix.Length + keyBytes > _maxKeyBytes)
            throw new ArgumentException("Namespaced cache key exceeds MaxKeyBytes.", nameof(key));

        byte[] encoded = new byte[_prefix.Length + keyBytes];

        _prefix.CopyTo(encoded, 0);
        RuvioCacheOptions.Utf8.GetBytes(key, encoded.AsSpan(_prefix.Length));

        return encoded;
    }

    private static long Milliseconds(TimeSpan? duration)
    {
        if (!duration.HasValue)
            return 0;

        long value = duration.Value.Ticks / TimeSpan.TicksPerMillisecond;

        if (value <= 0 || value > DateTimeOffset.MaxValue.ToUnixTimeMilliseconds())
            throw new ArgumentOutOfRangeException(nameof(duration), "Expiration must be at least one millisecond and within the supported date range.");

        return value;
    }

    private static byte[] Number(long value)
    {
        Span<byte> digits = stackalloc byte[20];
        Utf8Formatter.TryFormat(value, digits, out int written);

        return digits[..written].ToArray();
    }

    private static RuvioException UnexpectedReply() => new("Unexpected cache command response.", isServerReply: true);
}
