using System.Buffers;
using System.Buffers.Text;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using System.Globalization;
using System.Security.Cryptography;
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
    private static readonly ReadOnlyMemory<byte> Swr = "SWR"u8.ToArray();
    // Lenient like Microsoft's SetString/GetString helpers: a value that is not valid UTF-8 decodes with replacement characters.
    private static readonly UTF8Encoding Text = new(false);
    private readonly IRuvioClient _client;
    private readonly byte[] _prefix;
    private readonly string _tagPrefix;
    private readonly int _maxTags;
    private readonly int _maxValueBytes;
    private readonly int _maxKeyBytes;
    private readonly long _fillMilliseconds;
    private readonly long _waitMilliseconds;
    private readonly RuvioOutageOptions _outage;
    private static readonly System.Diagnostics.Metrics.Meter Meter = new("Ruvio.Extensions.Caching");
    private static readonly System.Diagnostics.Metrics.Counter<long> Outages = Meter.CreateCounter<long>("ruvio.cache.outage");
    private static readonly System.Diagnostics.Metrics.Counter<long> FailSafe = Meter.CreateCounter<long>("ruvio.cache.fail_safe");

    /// <summary>Constructs a provider with validated, snapshotted options and a borrowed client.</summary>
    public RuvioDistributedCache(IRuvioClient client, IOptions<RuvioCacheOptions> options)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(options);
        RuvioCacheOptions settings = options.Value;

        settings.Validate();
        _client = client;
        _prefix = RuvioCacheOptions.Utf8.GetBytes(settings.InstanceName + RuvioCacheOptions.VersionPrefix);
        _tagPrefix = settings.InstanceName + RuvioCacheOptions.TagPrefix;
        _maxTags = settings.MaxTagsPerEntry;
        _maxValueBytes = settings.MaxValueBytes;
        _maxKeyBytes = settings.MaxKeyBytes;
        _fillMilliseconds = (long)settings.FillTimeout.TotalMilliseconds;
        _waitMilliseconds = (long)settings.FillWait.TotalMilliseconds;
        _outage = settings.Outage;
    }

    // Without an outage mode or a circuit breaker the operations run unwrapped.
    private bool Guarded => _outage.Mode == RuvioOutageMode.FailOpen || _outage.CircuitBreaker is not null;

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
            return default;

        if (reply.Kind != RespKind.Bulk)
            throw UnexpectedReply();

        byte[] stored = reply.Bytes!;
        int offset = await Decode(encoded, stored, token).ConfigureAwait(false);

        return offset < 0 ? default : convert(stored, offset, state);
    }

    /// <summary>Applies the envelope's expiry to bytes just read, and returns the payload offset (-1 when expired, stale or evicted by a tag).</summary>
    private async Task<int> Decode(ReadOnlyMemory<byte> encoded, byte[] stored, CancellationToken token)
    {
        (Freshness state, int offset) = await Inspect(encoded, stored, token).ConfigureAwait(false);

        return state == Freshness.Fresh ? offset : -1;
    }

    private enum Freshness
    {
        Fresh,
        // Past its fresh period, still within StaleFor: kept for GetOrCreate, a miss for Get.
        Stale,
        Gone,
    }

    /// <summary>
    /// Applies the envelope's absolute and sliding expiry and checks its tags. An entry past its deadline is deleted; a
    /// stale one or one whose tag was removed is left alone (it expires on its own).
    /// </summary>
    private async Task<(Freshness State, int Offset)> Inspect(ReadOnlyMemory<byte> encoded, byte[] stored, CancellationToken token)
    {
        Header header = Parse(stored);
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        if (header.Absolute > 0 && header.Absolute <= now)
        {
            await Touch(encoded, stored, 0, token).ConfigureAwait(false);

            return (Freshness.Gone, -1);
        }

        if (header.TagCount > 0 && !await TagsLive(stored, header, token).ConfigureAwait(false))
            return (Freshness.Gone, -1);

        if (header.Sliding > 0)
        {
            long deadline = header.Absolute > 0 ? Math.Min(header.Absolute, now + header.Sliding) : now + header.Sliding;

            await Touch(encoded, stored, deadline <= now ? 0 : deadline, token).ConfigureAwait(false);
        }

        return (header.Fresh > 0 && header.Fresh <= now ? Freshness.Stale : Freshness.Fresh, header.Payload);
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

        await WriteAsync(key, options, value, static (header, bytes) => Envelope(header, bytes), token)
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

        await WriteAsync(key, options, value, static (header, text) => EnvelopeString(header, text), token)
            .ConfigureAwait(false);
    }

    /// <summary>Serializes <paramref name="value"/> to JSON directly into the stored record and writes it.</summary>
    [RequiresUnreferencedCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    [RequiresDynamicCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    public async Task SetJsonAsync<T>(string key, T value, DistributedCacheEntryOptions options,
        JsonSerializerOptions? serializerOptions = null, CancellationToken token = default)
        => await WriteAsync(key, options, (cache: this, value, serializerOptions),
            static (header, state) => state.cache.EnvelopeJson(header, (state.value, state.serializerOptions),
                static (w, input) => JsonSerializer.Serialize(w, input.value, input.serializerOptions), state.serializerOptions),
            token).ConfigureAwait(false);

    /// <summary>Same as the options overload, with a source-generated <paramref name="typeInfo"/>.</summary>
    public async Task SetJsonAsync<T>(string key, T value, JsonTypeInfo<T> typeInfo, DistributedCacheEntryOptions options,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(typeInfo);

        await WriteAsync(key, options, (cache: this, value, typeInfo),
            static (header, state) => state.cache.EnvelopeJson(header, (state.value, state.typeInfo),
                static (w, input) => JsonSerializer.Serialize(w, input.value, input.typeInfo), state.typeInfo.Options),
            token).ConfigureAwait(false);
    }

    private Task WriteAsync<TState>(string key, DistributedCacheEntryOptions options, TState state,
        Func<Header, TState, ReadOnlyMemory<byte>> envelope, CancellationToken token)
        => Guarded ? WriteGuardedAsync(key, options, state, envelope, token) : WriteCoreAsync(key, options, state, envelope, token);

    private async Task WriteGuardedAsync<TState>(string key, DistributedCacheEntryOptions options, TState state,
        Func<Header, TState, ReadOnlyMemory<byte>> envelope, CancellationToken token)
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
        Func<Header, TState, ReadOnlyMemory<byte>> envelope, CancellationToken token)
    {
        ReadOnlyMemory<byte> encoded = Key(key);
        Plan plan = PlanFor(options);

        token.ThrowIfCancellationRequested();
        ReadOnlyMemory<byte> record = envelope(await HeaderFor(plan, token).ConfigureAwait(false), state);
        ReadOnlyMemory<byte>[] command = plan.Expiry switch
        {
            (string kind, long when) => [SetCommand, encoded, record, kind == "PX" ? Px : Pxat, Number(when)],
            null => [SetCommand, encoded, record],
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
            (value, header) =>
            {
                CheckSize(value.Length);

                return Envelope(header, value);
            }, token).ConfigureAwait(false);
    }

    /// <summary>String flavour of <see cref="GetOrCreateAsync"/>: no <c>byte[]</c> conversion on either side.</summary>
    public async Task<string> GetOrCreateStringAsync(string key, Func<CancellationToken, Task<string>> factory,
        DistributedCacheEntryOptions options, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(factory);

        return await GetOrCreateCore(key, factory, options,
            static (stored, offset) => Text.GetString(stored, offset, stored.Length - offset),
            (value, header) =>
            {
                ArgumentNullException.ThrowIfNull(value);
                CheckSize(Text.GetByteCount(value));

                return EnvelopeString(header, value);
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
            (value, header) => EnvelopeJson(header, (value, serializerOptions),
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
            (value, header) => EnvelopeJson(header, (value, typeInfo),
                static (w, input) => JsonSerializer.Serialize(w, input.value, input.typeInfo), typeInfo.Options), token).ConfigureAwait(false);
    }

    private Task<T> GetOrCreateCore<T>(string key, Func<CancellationToken, Task<T>> factory,
        DistributedCacheEntryOptions options, Func<byte[], int, T> read,
        Func<T, Header, ReadOnlyMemory<byte>> envelope, CancellationToken token)
        => Guarded
            ? GetOrCreateGuardedAsync(key, factory, options, read, envelope, token)
            : GetOrCreateStoredAsync(key, factory, options, read, envelope, token);

    private async Task<T> GetOrCreateGuardedAsync<T>(string key, Func<CancellationToken, Task<T>> factory,
        DistributedCacheEntryOptions options, Func<byte[], int, T> read,
        Func<T, Header, ReadOnlyMemory<byte>> envelope, CancellationToken token)
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
        Func<T, Header, ReadOnlyMemory<byte>> envelope, CancellationToken token)
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

        // The server decides staleness when it remembers the entry's soft expiry (STALE, with a token for the one
        // caller that refreshes). When it does not (a restart, or an entry written by Set), the envelope's fresh
        // deadline decides, and every caller that sees it stale refreshes.
        byte[]? stale = null;
        int staleOffset = -1;

        if (fill.State is FillState.Hit or FillState.Stale)
        {
            (Freshness state, int offset) = await Inspect(encoded, fill.Value!, token).ConfigureAwait(false);

            if (state == Freshness.Fresh && fill.State == FillState.Hit)
                return read(fill.Value!, offset);

            if (state != Freshness.Gone)
            {
                if (fill.State == FillState.Stale && fill.Token is null)
                    return read(fill.Value!, offset);   // another caller is refreshing it

                stale = fill.Value;
                staleOffset = offset;
            }
        }

        T value;

        try
        {
            value = await factory(token).ConfigureAwait(false);
        }
        catch (Exception) when (stale is not null && !token.IsCancellationRequested)
        {
            // Fail-safe: the stale value is better than the error. The refresh token lapses after FillTimeout,
            // so a later caller tries again.
            if (FailSafe.Enabled)
                FailSafe.Add(1);

            return read(stale, staleOffset);
        }

        long? fillToken = fill.State switch
        {
            FillState.Fill => fill.Token,
            FillState.Stale => fill.Token,
            _ => null,
        };

        if (fillToken is { } owned)
        {
            Plan plan = PlanFor(options);
            Header header = await HeaderFor(plan, token).ConfigureAwait(false);
            ReadOnlyMemory<byte> record = envelope(value, header);
            // FILLSET needs a positive TTL; an entry without expiry is stored with one and then persisted. A stale
            // period is FILLSET's SWR window, so the server keeps the end of the fresh period too.
            long ttl = plan.Fresh > 0
                ? Math.Max(1, plan.Fresh - plan.Now)
                : plan.Expiry switch
                {
                    ("PX", long when) => when,
                    (_, long when) => Math.Max(1, when - plan.Now),
                    null => 3_600_000,
                };
            bool persistent = plan.Expiry is null;
            ReadOnlyMemory<byte>[] fillSet = plan.StaleFor > 0
                ? [FillSetCommand, encoded, record, Number(ttl), Number(owned), Swr, Number(plan.StaleFor)]
                : [FillSetCommand, encoded, record, Number(ttl), Number(owned)];

            RespValue stored = await Execute(fillSet, token).ConfigureAwait(false);

            if (stored.Kind != RespKind.Integer)
                throw UnexpectedReply();

            // 0 means the marker lapsed while the factory ran; the value is still correct for this caller.
            if (stored.Integer == 1 && persistent)
                _ = await Execute([PersistCommand, encoded], token).ConfigureAwait(false);

            return value;
        }

        await WriteAsync(key, options, (envelope, value),
            static (header, state) => state.envelope(state.value, header), token).ConfigureAwait(false);

        return value;
    }

    /// <summary>The expiry of one write: deadlines in Unix milliseconds (0 for none), and the server-side expiry.</summary>
    private readonly record struct Plan(long Now, long Absolute, long Sliding, long Fresh, long StaleFor,
        (string Kind, long When)? Expiry, IReadOnlyList<string>? Tags);

    /// <summary>Validates the expiry options and works out the deadlines; with a stale period the stored deadline is its end.</summary>
    private Plan PlanFor(DistributedCacheEntryOptions options)
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

        long fresh = 0;
        long staleFor = 0;
        IReadOnlyList<string>? tags = null;

        if (options is RuvioCacheEntryOptions extra)
        {
            staleFor = Milliseconds(extra.StaleFor);

            if (staleFor > 0)
            {
                if (sliding > 0)
                    throw new ArgumentException("StaleFor cannot be combined with SlidingExpiration.", nameof(options));

                if (absolute == 0)
                    throw new ArgumentException("StaleFor needs an absolute or relative expiration: the end of the fresh period.", nameof(options));

                if (staleFor > DateTimeOffset.MaxValue.ToUnixTimeMilliseconds() - absolute)
                    throw new ArgumentOutOfRangeException(nameof(options), "StaleFor exceeds the supported date range.");

                fresh = absolute;
                absolute += staleFor;
            }

            if (extra.Tags is { Count: > 0 } given)
            {
                if (given.Count > _maxTags)
                    throw new ArgumentException("Too many cache tags; see MaxTagsPerEntry.", nameof(options));

                foreach (string tag in given)
                    ArgumentException.ThrowIfNullOrEmpty(tag, nameof(options));

                tags = given;
            }
        }

        return new Plan(now, absolute, sliding, fresh, staleFor, Expiry(now, absolute, sliding), tags);
    }

    /// <summary>The envelope header of a write; for tags, reads (and creates) their generations first.</summary>
    private async ValueTask<Header> HeaderFor(Plan plan, CancellationToken token)
    {
        if (plan.Tags is null)
            return new Header(plan.Absolute, plan.Sliding, plan.Fresh, null, 0, 0);

        // A tag key must outlive every entry that carries it; zero is an entry without expiry.
        long lifetime = plan.Expiry switch
        {
            ("PX", long when) => when,
            (_, long when) => Math.Max(1, when - plan.Now),
            null => 0,
        };

        return new Header(plan.Absolute, plan.Sliding, plan.Fresh,
            await TagBlock(plan.Tags, lifetime, token).ConfigureAwait(false), plan.Tags.Count, 0);
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

    /// <summary>
    /// The stored header. <c>RDC1\n{absolute}\n{sliding}\n</c> for an entry without a stale period or tags (the
    /// format older versions read); <c>RDC2\n{absolute}\n{sliding}\n{fresh}\n{tags}\n</c> followed by 96 ASCII bytes
    /// per tag (the hex SHA-256 of the tag, then the hex generation it had) otherwise. Times are Unix milliseconds.
    /// </summary>
    private readonly record struct Header(long Absolute, long Sliding, long Fresh, byte[]? Tags, int TagCount, int Payload)
    {
        public bool Extended => Fresh > 0 || Tags is not null;
        public int MaxLength => 5 + 20 + 1 + 20 + 1 + (Extended ? 20 + 1 + 2 + 1 + (Tags?.Length ?? 0) : 0);
    }

    private const int TagBytes = 96;

    private static Header Parse(byte[] stored)
    {
        ReadOnlySpan<byte> body = stored;
        bool extended = body.StartsWith("RDC2\n"u8);

        if (!extended && !body.StartsWith("RDC1\n"u8))
            throw UnexpectedReply();

        int position = 5;
        long absolute = Field(stored, ref position);
        long sliding = Field(stored, ref position);

        if (!extended)
            return new Header(absolute, sliding, 0, null, 0, position);

        long fresh = Field(stored, ref position);
        long tags = Field(stored, ref position);

        if (tags > 64 || stored.Length - position < tags * TagBytes)
            throw UnexpectedReply();

        return new Header(absolute, sliding, fresh, null, (int)tags, position + (int)tags * TagBytes);
    }

    /// <summary>Reads one decimal field ending in <c>\n</c> at <paramref name="position"/> and moves past it.</summary>
    private static long Field(byte[] stored, ref int position)
    {
        ReadOnlySpan<byte> rest = stored.AsSpan(position);
        int end = rest.IndexOf((byte)'\n');

        if (end <= 0 || !long.TryParse(rest[..end], NumberStyles.None, CultureInfo.InvariantCulture, out long value))
            throw UnexpectedReply();

        position += end + 1;

        return value;
    }

    /// <summary>Writes the header and returns its length.</summary>
    private static int WriteHeader(Span<byte> header, Header fields)
    {
        (fields.Extended ? "RDC2\n"u8 : "RDC1\n"u8).CopyTo(header);
        int written = 5;

        WriteField(header, ref written, fields.Absolute);
        WriteField(header, ref written, fields.Sliding);

        if (fields.Extended)
        {
            WriteField(header, ref written, fields.Fresh);
            WriteField(header, ref written, fields.TagCount);

            if (fields.Tags is not null)
            {
                fields.Tags.CopyTo(header[written..]);
                written += fields.Tags.Length;
            }
        }

        return written;
    }

    private static void WriteField(Span<byte> header, ref int written, long value)
    {
        Utf8Formatter.TryFormat(value, header[written..], out int digits);
        written += digits;
        header[written++] = (byte)'\n';
    }

    /// <summary>The header followed by the payload, in one allocation.</summary>
    private static byte[] Envelope(Header fields, byte[] value)
    {
        Span<byte> header = fields.MaxLength <= 256 ? stackalloc byte[256] : new byte[fields.MaxLength];
        int written = WriteHeader(header, fields);
        byte[] record = new byte[written + value.Length];

        header[..written].CopyTo(record);
        value.CopyTo(record, written);

        return record;
    }

    /// <summary>The header followed by the string encoded as UTF-8 directly into the record, in one allocation.</summary>
    private static byte[] EnvelopeString(Header fields, string value)
    {
        Span<byte> header = fields.MaxLength <= 256 ? stackalloc byte[256] : new byte[fields.MaxLength];
        int written = WriteHeader(header, fields);
        byte[] record = new byte[written + Text.GetByteCount(value)];

        header[..written].CopyTo(record);
        Text.GetBytes(value, record.AsSpan(written));

        return record;
    }

    /// <summary>The header followed by JSON written straight into one growing buffer; no separate payload array.</summary>
    private ReadOnlyMemory<byte> EnvelopeJson<TState>(Header fields, TState state,
        Action<Utf8JsonWriter, TState> write, JsonSerializerOptions? serializerOptions)
    {
        var buffer = new ArrayBufferWriter<byte>(256);
        Span<byte> header = buffer.GetSpan(fields.MaxLength);

        buffer.Advance(WriteHeader(header, fields));
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

    /// <summary>
    /// Removes every entry written with <paramref name="tag"/> (<see cref="RuvioCacheEntryOptions.Tags"/>): one <c>DEL</c>
    /// of the tag's generation key, no scan. The entries stay stored until they expire, but no read returns them again.
    /// </summary>
    public Task RemoveByTagAsync(string tag, CancellationToken token = default)
        => Guarded ? RemoveTagGuardedAsync(tag, token) : RemoveTagCoreAsync(tag, token);

    private async Task RemoveTagGuardedAsync(string tag, CancellationToken token)
    {
        if (!_outage.TryEnter())
        {
            CountOutage("remove_tag", null);

            if (_outage.Mode == RuvioOutageMode.FailOpen)
                return;

            throw new RuvioCircuitOpenException(_outage.RetryAfter);
        }

        try
        {
            await RemoveTagCoreAsync(tag, token).ConfigureAwait(false);
            _outage.Succeeded();
        }
        catch (Exception error) when (_outage.Failed(error, token) && _outage.Mode == RuvioOutageMode.FailOpen)
        {
            // The removal is lost: entries with this tag can be read again once Ruvio is back.
            CountOutage("remove_tag", error);
        }
    }

    private async Task RemoveTagCoreAsync(string tag, CancellationToken token)
    {
        ArgumentException.ThrowIfNullOrEmpty(tag);
        token.ThrowIfCancellationRequested();
        RespValue reply = await Execute([Delete, RuvioCacheOptions.Utf8.GetBytes(TagKey(Digest(tag)))], token).ConfigureAwait(false);

        if (reply.Kind != RespKind.Integer || reply.Integer is < 0 or > 1)
            throw UnexpectedReply();
    }

    /// <summary>
    /// The 96-byte-per-tag block of a write: each tag's digest and current generation. A tag without a generation key
    /// gets a new random one, so an entry written under an earlier generation never matches it again. The keys are not
    /// confined to one hash slot, so this is not atomic with the entry: a racing removal can at worst cost a miss.
    /// </summary>
    private async ValueTask<byte[]> TagBlock(IReadOnlyList<string> tags, long lifetime, CancellationToken token)
    {
        var keys = new string[tags.Count];
        var digests = new string[tags.Count];
        var lookup = new IReadOnlyList<string>[tags.Count * 2];

        for (int i = 0; i < tags.Count; i++)
        {
            digests[i] = Digest(tags[i]);
            keys[i] = TagKey(digests[i]);
            lookup[i * 2] = ["GET", keys[i]];
            lookup[i * 2 + 1] = ["PTTL", keys[i]];
        }

        IReadOnlyList<RespValue> found = await _client.ExecuteManyAsync(lookup, token).ConfigureAwait(false);

        if (found.Count != lookup.Length)
            throw UnexpectedReply();

        string created = Guid.NewGuid().ToString("N");
        string? life = lifetime > 0 ? lifetime.ToString(CultureInfo.InvariantCulture) : null;
        var writes = new List<IReadOnlyList<string>>();
        byte[] block = new byte[tags.Count * TagBytes];

        for (int i = 0; i < tags.Count; i++)
        {
            RespValue current = found[i * 2];
            RespValue ttl = found[i * 2 + 1];
            string generation;

            CheckReply(current);
            CheckReply(ttl);

            if (current.Kind == RespKind.Null)
            {
                generation = created;
                writes.Add(life is null ? ["SET", keys[i], created] : ["SET", keys[i], created, "PX", life]);
            }
            else
            {
                generation = current.AsString() ?? "";

                if (current.Kind != RespKind.Bulk || generation.Length != 32 || ttl.Kind != RespKind.Integer)
                    throw UnexpectedReply();

                // -1 is a key without expiry, which outlives anything.
                if (life is null && ttl.Integer >= 0)
                    writes.Add(["PERSIST", keys[i]]);
                else if (life is not null && ttl.Integer >= 0 && ttl.Integer < lifetime)
                    writes.Add(["PEXPIRE", keys[i], life]);
            }

            Encoding.ASCII.GetBytes(digests[i], block.AsSpan(i * TagBytes, 64));
            Encoding.ASCII.GetBytes(generation, block.AsSpan(i * TagBytes + 64, 32));
        }

        if (writes.Count > 0)
        {
            foreach (RespValue reply in await _client.ExecuteManyAsync(writes, token).ConfigureAwait(false))
                CheckReply(reply);
        }

        return block;
    }

    /// <summary>True when every tag of a stored entry still has the generation the entry was written under.</summary>
    private async ValueTask<bool> TagsLive(byte[] stored, Header header, CancellationToken token)
    {
        int start = header.Payload - header.TagCount * TagBytes;
        var keys = new string[header.TagCount];

        for (int i = 0; i < keys.Length; i++)
            keys[i] = TagKey(Encoding.ASCII.GetString(stored, start + i * TagBytes, 64));

        string[] command = new string[keys.Length + 1];

        command[0] = "MGET";
        keys.CopyTo(command, 1);
        // A pipeline splits an MGET over several slots and merges the replies in order.
        IReadOnlyList<RespValue> replies = await _client.ExecuteManyAsync([command], token).ConfigureAwait(false);

        if (replies.Count != 1)
            throw UnexpectedReply();

        CheckReply(replies[0]);

        if (replies[0].Items is not { } live || live.Count != keys.Length)
            throw UnexpectedReply();

        for (int i = 0; i < keys.Length; i++)
        {
            if (live[i].AsString() is not { Length: 32 } generation
                || Encoding.ASCII.GetByteCount(generation) != 32
                || !stored.AsSpan(start + i * TagBytes + 64, 32).SequenceEqual(Encoding.ASCII.GetBytes(generation)))
            {
                return false;
            }
        }

        return true;
    }

    private string TagKey(string digest) => _tagPrefix + digest;

    // Hashing keeps tag keys a fixed length whatever the tag.
    private static string Digest(string tag)
    {
        Span<byte> digest = stackalloc byte[SHA256.HashSizeInBytes];

        SHA256.HashData(RuvioCacheOptions.Utf8.GetBytes(tag), digest);

        return Convert.ToHexString(digest).ToLowerInvariant();
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
