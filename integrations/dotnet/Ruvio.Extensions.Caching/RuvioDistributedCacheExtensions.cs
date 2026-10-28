using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.Caching.Distributed;

namespace Ruvio.Extensions.Caching;

/// <summary>Cache-aside helpers for any <see cref="IDistributedCache"/>.</summary>
public static class RuvioDistributedCacheExtensions
{
    /// <summary>
    /// Returns the cached value or builds and stores it. On <see cref="RuvioDistributedCache"/> one caller across all
    /// processes builds the value while the rest wait for it. On any other cache every caller that misses builds it.
    /// </summary>
    public static async Task<byte[]> GetOrCreateAsync(
        this IDistributedCache cache,
        string key,
        Func<CancellationToken, Task<byte[]>> factory,
        DistributedCacheEntryOptions options,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(cache);
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentNullException.ThrowIfNull(options);

        if (cache is RuvioDistributedCache ruvio)
            return await ruvio.GetOrCreateAsync(key, factory, options, token).ConfigureAwait(false);

        byte[]? cached = await cache.GetAsync(key, token).ConfigureAwait(false);

        if (cached is not null)
            return cached;

        byte[] value = await factory(token).ConfigureAwait(false);

        await cache.SetAsync(key, value, options, token).ConfigureAwait(false);

        return value;
    }

    /// <summary>
    /// Removes every entry written with <paramref name="tag"/> in <see cref="RuvioCacheEntryOptions.Tags"/>. Only
    /// <see cref="RuvioDistributedCache"/> keeps tags; any other cache throws <see cref="NotSupportedException"/>.
    /// </summary>
    public static Task RemoveByTagAsync(this IDistributedCache cache, string tag, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(cache);

        return cache is RuvioDistributedCache ruvio
            ? ruvio.RemoveByTagAsync(tag, token)
            : throw new NotSupportedException("Only RuvioDistributedCache removes entries by tag.");
    }

    private static readonly UTF8Encoding Utf8 = new(false);

    private static DistributedCacheEntryOptions Default(DistributedCacheEntryOptions? options) => options ?? new();

    /// <summary>
    /// Returns the cached string, or builds and stores it as UTF-8. On <see cref="RuvioDistributedCache"/> the string is
    /// encoded and decoded straight from the stored record with one process building it; on any other cache it goes through
    /// <c>byte[]</c> like Microsoft's <c>SetStringAsync</c>.
    /// </summary>
    public static async Task<string> GetOrCreateStringAsync(
        this IDistributedCache cache,
        string key,
        Func<CancellationToken, Task<string>> factory,
        DistributedCacheEntryOptions? options = null,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(cache);
        ArgumentNullException.ThrowIfNull(factory);

        if (cache is RuvioDistributedCache ruvio)
            return await ruvio.GetOrCreateStringAsync(key, factory, Default(options), token).ConfigureAwait(false);

        byte[] bytes = await cache.GetOrCreateAsync(
            key,
            async ct => Utf8.GetBytes(await factory(ct).ConfigureAwait(false)),
            Default(options),
            token).ConfigureAwait(false);

        return Utf8.GetString(bytes);
    }

    /// <summary>Stores <paramref name="value"/> as UTF-8 JSON using <paramref name="serializerOptions"/> (defaults when null).</summary>
    [RequiresUnreferencedCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    [RequiresDynamicCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    public static Task SetJsonAsync<T>(
        this IDistributedCache cache,
        string key,
        T value,
        DistributedCacheEntryOptions? options = null,
        JsonSerializerOptions? serializerOptions = null,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(cache);

        if (cache is RuvioDistributedCache ruvio)
            return ruvio.SetJsonAsync(key, value, Default(options), serializerOptions, token);

        return cache.SetAsync(key, JsonSerializer.SerializeToUtf8Bytes(value, serializerOptions), Default(options), token);
    }

    /// <summary>Stores <paramref name="value"/> with a source-generated <paramref name="typeInfo"/> (trim and AOT safe).</summary>
    public static Task SetJsonAsync<T>(
        this IDistributedCache cache,
        string key,
        T value,
        JsonTypeInfo<T> typeInfo,
        DistributedCacheEntryOptions? options = null,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(cache);
        ArgumentNullException.ThrowIfNull(typeInfo);

        if (cache is RuvioDistributedCache ruvio)
            return ruvio.SetJsonAsync(key, value, typeInfo, Default(options), token);

        return cache.SetAsync(key, JsonSerializer.SerializeToUtf8Bytes(value, typeInfo), Default(options), token);
    }

    /// <summary>Reads the entry and deserializes it from JSON. Returns <c>default</c> on a miss.</summary>
    [RequiresUnreferencedCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    [RequiresDynamicCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    public static async Task<T?> GetJsonAsync<T>(
        this IDistributedCache cache,
        string key,
        JsonSerializerOptions? serializerOptions = null,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(cache);

        if (cache is RuvioDistributedCache ruvio)
            return await ruvio.GetJsonAsync<T>(key, serializerOptions, token).ConfigureAwait(false);

        byte[]? bytes = await cache.GetAsync(key, token).ConfigureAwait(false);

        return bytes is null ? default : JsonSerializer.Deserialize<T>(bytes, serializerOptions);
    }

    /// <summary>Reads the entry and deserializes it with a source-generated <paramref name="typeInfo"/>. Returns <c>default</c> on a miss.</summary>
    public static async Task<T?> GetJsonAsync<T>(
        this IDistributedCache cache,
        string key,
        JsonTypeInfo<T> typeInfo,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(cache);
        ArgumentNullException.ThrowIfNull(typeInfo);

        if (cache is RuvioDistributedCache ruvio)
            return await ruvio.GetJsonAsync(key, typeInfo, token).ConfigureAwait(false);

        byte[]? bytes = await cache.GetAsync(key, token).ConfigureAwait(false);

        return bytes is null ? default : JsonSerializer.Deserialize(bytes, typeInfo);
    }

    /// <summary>Returns the cached model, or builds it with <paramref name="factory"/> and stores it as JSON.</summary>
    [RequiresUnreferencedCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    [RequiresDynamicCode("Serializes with reflection; pass a JsonTypeInfo<T> for trimming or AOT.")]
    public static async Task<T> GetOrCreateJsonAsync<T>(
        this IDistributedCache cache,
        string key,
        Func<CancellationToken, Task<T>> factory,
        DistributedCacheEntryOptions? options = null,
        JsonSerializerOptions? serializerOptions = null,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(cache);
        ArgumentNullException.ThrowIfNull(factory);

        if (cache is RuvioDistributedCache ruvio)
            return await ruvio.GetOrCreateJsonAsync(key, factory, Default(options), serializerOptions, token).ConfigureAwait(false);

        byte[] bytes = await cache.GetOrCreateAsync(
            key,
            async ct => JsonSerializer.SerializeToUtf8Bytes(await factory(ct).ConfigureAwait(false), serializerOptions),
            Default(options),
            token).ConfigureAwait(false);

        return JsonSerializer.Deserialize<T>(bytes, serializerOptions)!;
    }

    /// <summary>Same as the options overload, with a source-generated <paramref name="typeInfo"/>.</summary>
    public static async Task<T> GetOrCreateJsonAsync<T>(
        this IDistributedCache cache,
        string key,
        Func<CancellationToken, Task<T>> factory,
        JsonTypeInfo<T> typeInfo,
        DistributedCacheEntryOptions? options = null,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(cache);
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentNullException.ThrowIfNull(typeInfo);

        if (cache is RuvioDistributedCache ruvio)
            return await ruvio.GetOrCreateJsonAsync(key, factory, typeInfo, Default(options), token).ConfigureAwait(false);

        byte[] bytes = await cache.GetOrCreateAsync(
            key,
            async ct => JsonSerializer.SerializeToUtf8Bytes(await factory(ct).ConfigureAwait(false), typeInfo),
            Default(options),
            token).ConfigureAwait(false);

        return JsonSerializer.Deserialize(bytes, typeInfo)!;
    }
}
