using System.Buffers;
using System.Buffers.Text;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Security.Cryptography;
using Microsoft.AspNetCore.OutputCaching;
using Microsoft.Extensions.Options;
using Ruvio.Client;

namespace Ruvio.AspNetCore.OutputCaching;

/// <summary>
/// Represents an output cache store backed by Ruvio.
/// </summary>
public sealed class RuvioOutputCacheStore : IOutputCacheStore
{
    private static readonly Meter Meter = new("Ruvio.AspNetCore.OutputCaching");
    private static readonly Counter<long> StorageCalls = Meter.CreateCounter<long>("ruvio.output_cache.storage_calls");
    private static readonly Counter<long> StorageCommands = Meter.CreateCounter<long>("ruvio.output_cache.storage_commands");
    private static readonly Histogram<double> StorageDuration = Meter.CreateHistogram<double>(
        "ruvio.output_cache.storage_duration", "ms");
    private static readonly Counter<long> Hits = Meter.CreateCounter<long>("ruvio.output_cache.hit");
    private static readonly Counter<long> Misses = Meter.CreateCounter<long>("ruvio.output_cache.miss");
    private static readonly Counter<long> TagEvictions = Meter.CreateCounter<long>("ruvio.output_cache.tag_evict");
    private static readonly ReadOnlyMemory<byte> Get = "GET"u8.ToArray();
    private static readonly ReadOnlyMemory<byte> Delete = "DEL"u8.ToArray();
    private static readonly ReadOnlyMemory<byte> Set = "SET"u8.ToArray();
    private static readonly ReadOnlyMemory<byte> Px = "PX"u8.ToArray();
    private readonly Lazy<IRuvioClient> client;
    private readonly string prefix;
    private readonly byte[] prefixBytes;
    private readonly int maxValueBytes;
    private readonly int maxKeyBytes;
    private readonly int maxTags;
    private readonly RuvioOutageOptions outage;

    /// <summary>
    /// Initializes a new instance of the <see cref="RuvioOutputCacheStore"/> class.
    /// </summary>
    /// <param name="client">The Ruvio client instance.</param>
    /// <param name="options">The output cache options.</param>
    public RuvioOutputCacheStore(IRuvioClient client, IOptions<RuvioOutputCacheOptions> options)
        : this(() => client, options)
    {
        ArgumentNullException.ThrowIfNull(client);
    }

    internal RuvioOutputCacheStore(Func<IRuvioClient> clientFactory, IOptions<RuvioOutputCacheOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var settings = options.Value;

        settings.Validate();
        client = new Lazy<IRuvioClient>(clientFactory);
        // Hash tag: every key of one InstanceName lands in one slot, so tag reads (MGET) and publication
        // writes can address them together on a cluster. One namespace is not spread across shards.
        prefix = "ruvio:output:v1:{" + Hash(settings.InstanceName) + "}:";
        prefixBytes = Bytes(prefix);
        maxValueBytes = settings.MaxValueBytes;
        maxKeyBytes = settings.MaxKeyBytes;
        maxTags = settings.MaxTagsPerEntry;
        outage = settings.Outage;
    }

    // Without an outage mode or a circuit breaker the public methods return the core task itself.
    private bool Guarded => outage.Mode == RuvioOutageMode.FailOpen || outage.CircuitBreaker is not null;

    private static readonly Counter<long> Outages = Meter.CreateCounter<long>("ruvio.output_cache.outage");

    // Fail open drops the call; every other mode raises, with an open circuit as RuvioCircuitOpenException.
    private void Unavailable(string operation, Exception? cause)
    {
        Outages.Add(1, new TagList
        {
            { "operation", operation },
            { "mode", outage.Mode == RuvioOutageMode.FailOpen ? "fail_open" : "fail_closed" },
            { "reason", cause is null ? "circuit_open" : "error" },
        });
    }

    /// <summary>
    /// Sets a cache entry with the specified key, value, tags, and expiration time.
    /// </summary>
    /// <param name="key">The cache entry key.</param>
    /// <param name="value">The cache entry value.</param>
    /// <param name="tags">The tags associated with the cache entry.</param>
    /// <param name="validFor">The duration for which the cache entry is valid.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    /// <exception cref="ArgumentException"></exception>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    /// <exception cref="RuvioException"></exception>
    public ValueTask SetAsync(string key, byte[] value, string[]? tags, TimeSpan validFor,
        CancellationToken cancellationToken)
        => Guarded ? SetGuardedAsync(key, value, tags, validFor, cancellationToken) : SetCoreAsync(key, value, tags, validFor, cancellationToken);

    private async ValueTask SetGuardedAsync(string key, byte[] value, string[]? tags, TimeSpan validFor,
        CancellationToken cancellationToken)
    {
        if (!outage.TryEnter())
        {
            Unavailable("set", null);

            if (outage.Mode == RuvioOutageMode.FailOpen)
                return;

            throw new RuvioCircuitOpenException(outage.RetryAfter);
        }

        try
        {
            await SetCoreAsync(key, value, tags, validFor, cancellationToken).ConfigureAwait(false);
            outage.Succeeded();
        }
        catch (Exception error) when (outage.Failed(error, cancellationToken) && outage.Mode == RuvioOutageMode.FailOpen)
        {
            Unavailable("set", error);
        }
    }

    private async ValueTask SetCoreAsync(string key, byte[] value, string[]? tags, TimeSpan validFor,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(key);

        // A request whose cache key is too long cannot be cached; the response is simply not stored.
        if (TooLong(key))
            return;

        byte[] entryKey = KeyBytes("entry:", key);

        ArgumentNullException.ThrowIfNull(value);
        tags ??= [];

        if (value.Length > maxValueBytes)
            throw new ArgumentException("Output-cache value exceeds MaxValueBytes.", nameof(value));

        if (tags.Length > maxTags)
            throw new ArgumentException("Too many output-cache tags.", nameof(tags));

        long milliseconds = validFor.Ticks / TimeSpan.TicksPerMillisecond;

        if (milliseconds < 1 || milliseconds > TimeSpan.FromDays(365).TotalMilliseconds)
            throw new ArgumentOutOfRangeException(nameof(validFor), "Lifetime must be between one millisecond and 365 days.");

        if (tags.Length == 0)
        {
            await WriteEntry(entryKey, value, tagKeys: [], versions: [], milliseconds, cancellationToken)
                .ConfigureAwait(false);

            return;
        }

        var tagKeys = new string[tags.Length];

        for (int i = 0; i < tags.Length; i++)
            tagKeys[i] = TagKey(tags[i]);

        var lookup = new List<IReadOnlyList<string>>(tagKeys.Length * 2);

        foreach (string tagKey in tagKeys)
        {
            lookup.Add(["GET", tagKey]);
            lookup.Add(["PTTL", tagKey]);
        }

        IReadOnlyList<RespValue> found = await MeasureStorageAsync(
            "read_tags", lookup.Count, Stopwatch.GetTimestamp(), client.Value.ExecuteManyAsync(lookup, cancellationToken))
            .ConfigureAwait(false);

        if (found.Count != lookup.Count)
            throw Unexpected();

        // A new random generation whenever a tag is (re)created, so an entry written under an older
        // generation can never match it again.
        string created = Guid.NewGuid().ToString("N");
        var versions = new string[tagKeys.Length];
        var writes = new List<IReadOnlyList<string>>();
        string? lifetime = null;

        for (int i = 0; i < tagKeys.Length; i++)
        {
            RespValue current = found[i * 2];
            RespValue ttl = found[i * 2 + 1];

            if (current.Kind == RespKind.Null)
            {
                versions[i] = created;
                writes.Add(["SET", tagKeys[i], created, "PX", lifetime ??= milliseconds.ToString(CultureInfo.InvariantCulture)]);
                continue;
            }

            string? version = current.AsString();

            if (current.Kind != RespKind.Bulk || version is not { Length: 32 } || !Hex(version) || ttl.Kind != RespKind.Integer)
                throw Unexpected();

            versions[i] = version;

            if (ttl.Integer < milliseconds)
                writes.Add(["PEXPIRE", tagKeys[i], lifetime ??= milliseconds.ToString(CultureInfo.InvariantCulture)]);
        }

        if (writes.Count > 0)
        {
            IReadOnlyList<RespValue> written = await MeasureStorageAsync(
                "write_tags", writes.Count, Stopwatch.GetTimestamp(), client.Value.ExecuteManyAsync(writes, cancellationToken))
                .ConfigureAwait(false);

            foreach (RespValue reply in written)
            {
                if (reply.Kind == RespKind.Error)
                    throw new RuvioException(reply.Text ?? "Output-cache command failed.");
            }
        }

        await WriteEntry(entryKey, value, tagKeys, versions, milliseconds, cancellationToken).ConfigureAwait(false);
    }

    private async Task WriteEntry(byte[] entryKey, byte[] value, string[] tagKeys, string[] versions, long milliseconds,
        CancellationToken cancellationToken)
    {
        var record = new byte[6 + 96 * tagKeys.Length + value.Length];

        "ROC1"u8.CopyTo(record);
        record[4] = (byte)('0' + tagKeys.Length / 10);
        record[5] = (byte)('0' + tagKeys.Length % 10);

        // Tag hashes and versions are ASCII hex, so each char maps to one byte; no rehash or substring.
        for (int i = 0; i < tagKeys.Length; i++)
        {
            Ascii(tagKeys[i].AsSpan(tagKeys[i].Length - 64), record.AsSpan(6 + i * 96, 64));
            Ascii(versions[i], record.AsSpan(6 + i * 96 + 64, 32));
        }

        value.CopyTo(record, 6 + 96 * tagKeys.Length);
        RespValue result = await Execute("set_entry", [Set, entryKey, record, Px, Number(milliseconds)], cancellationToken)
            .ConfigureAwait(false);

        if (result.Kind != RespKind.Simple || result.Text != "OK")
            throw Unexpected();
    }

    /// <summary>
    /// Retrieves the cached entry associated with the specified key. Returns <c>null</c> if the entry does not exist or is invalid.
    /// </summary>
    /// <param name="key">The key of the cache entry to retrieve.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>The cached entry as a byte array, or <c>null</c> if the entry does not exist or is invalid.</returns>
    public ValueTask<byte[]?> GetAsync(string key, CancellationToken cancellationToken)
        => Guarded ? GetGuardedAsync(key, cancellationToken) : GetCoreAsync(key, cancellationToken);

    private async ValueTask<byte[]?> GetGuardedAsync(string key, CancellationToken cancellationToken)
    {
        if (!outage.TryEnter())
        {
            Unavailable("get", null);

            if (outage.Mode == RuvioOutageMode.FailOpen)
                return null;

            throw new RuvioCircuitOpenException(outage.RetryAfter);
        }

        try
        {
            byte[]? entry = await GetCoreAsync(key, cancellationToken).ConfigureAwait(false);
            outage.Succeeded();

            return entry;
        }
        catch (Exception error) when (outage.Failed(error, cancellationToken) && outage.Mode == RuvioOutageMode.FailOpen)
        {
            Unavailable("get", error);

            return null; // a miss: the endpoint runs and answers
        }
    }

    private async ValueTask<byte[]?> GetCoreAsync(string key, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(key);

        // A request whose cache key is too long is a miss, not an error: the endpoint runs and answers.
        if (TooLong(key))
        {
            CountMiss("key_too_long");

            return null;
        }

        var reply = await Execute("get_entry", [Get, KeyBytes("entry:", key)], cancellationToken).ConfigureAwait(false);

        if (reply.Kind == RespKind.Null)
        {
            CountMiss("absent");

            return null;
        }

        if (reply.Kind != RespKind.Bulk)
            throw Unexpected();

        byte[] data = reply.Bytes!;
        int count = ReadHeader(data);
        int offset = 6 + 96 * count;

        if (count != 0)
        {
            var keys = new string[count];

            for (int i = 0; i < count; i++)
                keys[i] = StoredTagKey(data.AsSpan(6 + i * 96, 64));

            IReadOnlyList<string?> live = await MeasureStorageAsync(
                "validate_tags", 1, Stopwatch.GetTimestamp(), client.Value.GetManyAsync(keys)).ConfigureAwait(false);

            for (int i = 0; i < count; i++)
            {
                if (!SameAscii(live[i], data.AsSpan(6 + i * 96 + 64, 32)))
                {
                    CountMiss("tag_evicted");

                    return null;
                }
            }
        }

        if (Hits.Enabled)
            Hits.Add(1);

        return data.AsSpan(offset).ToArray();
    }

    // A missing entry and an entry whose tag was evicted are both misses; the reason tells them apart.
    private static void CountMiss(string reason)
    {
        if (Misses.Enabled)
            Misses.Add(1, new KeyValuePair<string, object?>("reason", reason));
    }

    /// <summary>
    /// Evicts all cache entries associated with the specified tag.
    /// </summary>
    /// <param name="tag">The tag of the cache entries to evict.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public ValueTask EvictByTagAsync(string tag, CancellationToken cancellationToken)
        => Guarded ? EvictGuardedAsync(tag, cancellationToken) : EvictCoreAsync(tag, cancellationToken);

    private async ValueTask EvictGuardedAsync(string tag, CancellationToken cancellationToken)
    {
        if (!outage.TryEnter())
        {
            Unavailable("evict", null);

            if (outage.Mode == RuvioOutageMode.FailOpen)
                return;

            throw new RuvioCircuitOpenException(outage.RetryAfter);
        }

        try
        {
            await EvictCoreAsync(tag, cancellationToken).ConfigureAwait(false);
            outage.Succeeded();
        }
        catch (Exception error) when (outage.Failed(error, cancellationToken) && outage.Mode == RuvioOutageMode.FailOpen)
        {
            // The eviction is lost: entries that carry this tag can be served again once Ruvio is back.
            Unavailable("evict", error);
        }
    }

    private async ValueTask EvictCoreAsync(string tag, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var reply = await Execute("evict_tag", [Delete, KeyBytes("tag:", tag)], cancellationToken).ConfigureAwait(false);

        if (reply.Kind != RespKind.Integer || reply.Integer is < 0 or > 1)
            throw Unexpected();

        if (TagEvictions.Enabled)
            TagEvictions.Add(1);
    }

    internal string EntryKey(string key) => KeyText("entry:", key);
    internal string TagKey(string tag) => KeyText("tag:", tag);

    // Hashing keeps keys a fixed length and keeps raw HTTP cache keys (host, path, query, variation) off the server.
    /// <summary><c>{prefix}{kind}{HEX(SHA-256(value))}</c> as one string.</summary>
    private string KeyText(string kind, string value)
    {
        Span<byte> digest = stackalloc byte[SHA256.HashSizeInBytes];
        ValidatedDigest(value, digest);

        Span<char> text = stackalloc char[prefix.Length + kind.Length + 64];
        prefix.CopyTo(text);
        kind.CopyTo(text[prefix.Length..]);

        Span<char> hex = text[(prefix.Length + kind.Length)..];

        for (int i = 0; i < digest.Length; i++)
        {
            hex[i * 2] = (char)HexDigits[digest[i] >> 4];
            hex[i * 2 + 1] = (char)HexDigits[digest[i] & 0xF];
        }

        return new string(text);
    }

    /// <summary>The UTF-8 form of <see cref="KeyText"/> without the intermediate string; the key is ASCII.</summary>
    private byte[] KeyBytes(string kind, string value)
    {
        Span<byte> digest = stackalloc byte[SHA256.HashSizeInBytes];
        ValidatedDigest(value, digest);

        var key = new byte[prefixBytes.Length + kind.Length + 64];
        prefixBytes.CopyTo(key, 0);
        Ascii(kind, key.AsSpan(prefixBytes.Length));

        Span<byte> hex = key.AsSpan(prefixBytes.Length + kind.Length);

        for (int i = 0; i < digest.Length; i++)
        {
            hex[i * 2] = HexDigits[digest[i] >> 4];
            hex[i * 2 + 1] = HexDigits[digest[i] & 0xF];
        }

        return key;
    }

    private bool TooLong(string key) => RuvioOutputCacheOptions.Utf8.GetByteCount(key) > maxKeyBytes;

    private void ValidatedDigest(string value, Span<byte> digest)
    {
        ArgumentNullException.ThrowIfNull(value);
        int length = RuvioOutputCacheOptions.Utf8.GetByteCount(value);

        if (length > maxKeyBytes)
            throw new ArgumentException("Output-cache key or tag exceeds MaxKeyBytes.");

        byte[]? rented = null;
        Span<byte> buffer = length <= 1024
                    ? stackalloc byte[1024]
                    : (rented = ArrayPool<byte>.Shared.Rent(length));

        try
        {
            int written = RuvioOutputCacheOptions.Utf8.GetBytes(value, buffer);
            SHA256.HashData(buffer[..written], digest);
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private string StoredTagKey(ReadOnlySpan<byte> hash)
    {
        Span<char> text = stackalloc char[prefix.Length + 4 + 64];
        prefix.CopyTo(text);
        "tag:".CopyTo(text[prefix.Length..]);

        for (int i = 0; i < hash.Length; i++)
            text[prefix.Length + 4 + i] = (char)hash[i];

        return new string(text);
    }

    private static bool SameAscii(string? text, ReadOnlySpan<byte> bytes)
    {
        if (text is null || text.Length != bytes.Length)
            return false;

        for (int i = 0; i < bytes.Length; i++)
        {
            if (text[i] != bytes[i])
                return false;
        }

        return true;
    }

    private static void Ascii(ReadOnlySpan<char> text, Span<byte> destination)
    {
        for (int i = 0; i < text.Length; i++)
            destination[i] = (byte)text[i];
    }

    private static ReadOnlySpan<byte> HexDigits => "0123456789ABCDEF"u8;

    private int ReadHeader(byte[] data)
    {
        if (data.Length < 6 || !data.AsSpan(0, 4).SequenceEqual("ROC1"u8) || data[4] is < (byte)'0' or > (byte)'9' || data[5] is < (byte)'0' or > (byte)'9')
        {
            throw Unexpected();
        }

        int count = (data[4] - '0') * 10 + data[5] - '0';
        int offset = 6 + 96 * count;

        if (count > maxTags || data.Length < offset || data.Length - offset > maxValueBytes)
            throw Unexpected();

        for (int i = 6; i < offset; i++)
        {
            if (data[i] is not (>= (byte)'0' and <= (byte)'9') and not (>= (byte)'a' and <= (byte)'f') and not (>= (byte)'A' and <= (byte)'F'))
            {
                throw Unexpected();
            }
        }

        return count;
    }

    private async ValueTask<RespValue> Execute(
        string operation, IReadOnlyList<ReadOnlyMemory<byte>> command, CancellationToken token)
    {
        long started = Stopwatch.GetTimestamp();
        string outcome = "error";

        try
        {
            var reply = await client.Value.ExecuteBinaryAsync(command, token).ConfigureAwait(false);

            if (reply.Kind == RespKind.Error)
                throw new RuvioException(reply.Text ?? "Output-cache command failed.");

            outcome = "success";

            return reply;
        }
        finally
        {
            Record(operation, 1, started, outcome);
        }
    }

    /// <summary>Records a storage call that has already started; no delegate or closure is allocated.</summary>
    private static async ValueTask<T> MeasureStorageAsync<T>(string operation, int commandCount, long started, ValueTask<T> pending)
    {
        string outcome = "error";

        try
        {
            T result = await pending.ConfigureAwait(false);
            outcome = "success";

            return result;
        }
        finally
        {
            Record(operation, commandCount, started, outcome);
        }
    }

    private static void Record(string operation, int commandCount, long started, string outcome)
    {
        var tags = new TagList
        {
            { "operation", operation },
            { "outcome", outcome }
        };

        StorageCalls.Add(1, tags);
        StorageCommands.Add(commandCount, tags);
        StorageDuration.Record(Stopwatch.GetElapsedTime(started).TotalMilliseconds, tags);
    }

    private static byte[] Bytes(string value) => RuvioOutputCacheOptions.Utf8.GetBytes(value);
    private static byte[] Number(long value)
    {
        Span<byte> digits = stackalloc byte[20];
        Utf8Formatter.TryFormat(value, digits, out int written);

        return digits[..written].ToArray();
    }

    private static string Hash(string value) => Convert.ToHexString(SHA256.HashData(Bytes(value)));

    private static bool Hex(string value)
    {
        foreach (char character in value)
        {
            if (character is not (>= '0' and <= '9') and not (>= 'a' and <= 'f') and not (>= 'A' and <= 'F'))
            {
                return false;
            }
        }

        return true;
    }

    private static RuvioException Unexpected() => new("Invalid output-cache response or record.");
}
