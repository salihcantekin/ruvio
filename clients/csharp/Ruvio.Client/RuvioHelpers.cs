using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Ruvio.Client;

/// <summary>
/// A distributed lock held on one string key: acquired with <c>SET key token NX PX ttl</c>, renewed with
/// <c>SET key token IFEQ token PX ttl</c>, released with <c>DELEX key IFEQ token</c>. Release and renew only touch
/// the key while it still holds this owner's token, so a lock that expired and was taken by someone else is never
/// deleted. <c>await using</c> releases it.
/// </summary>
/// <remarks>
/// Get one from <see cref="RuvioClientHelpers.TryAcquireLockAsync"/> or <see cref="RuvioClientHelpers.AcquireLockAsync"/>.
/// The lock expires on its own after its TTL, so work that may run longer must call <see cref="RenewAsync"/>.
/// It guards one Ruvio key, not the external resource: a holder that stalls past the TTL can overlap with the next owner.
/// </remarks>
/// <example>
/// <code>
/// await using RuvioLock? held = await db.TryAcquireLockAsync("lock:report", TimeSpan.FromSeconds(30));
/// if (held is not null)
/// {
///     await RunReportAsync();
///     // released when the block ends
/// }
/// </code>
/// </example>
public sealed class RuvioLock : IAsyncDisposable
{
    private readonly IRuvioClient _client;
    private int _released;

    internal RuvioLock(IRuvioClient client, string key, string token, TimeSpan ttl)
    {
        _client = client;
        Key = key;
        Token = token;
        Ttl = ttl;
    }

    /// <summary>The lock key.</summary>
    public string Key { get; }

    /// <summary>This owner's random token, the value stored at <see cref="Key"/>.</summary>
    public string Token { get; }

    /// <summary>The lease length used by <see cref="RenewAsync"/> when none is given.</summary>
    public TimeSpan Ttl { get; }

    /// <summary>True after <see cref="ReleaseAsync"/> or <see cref="DisposeAsync"/> ran.</summary>
    public bool IsReleased => Volatile.Read(ref _released) != 0;

    /// <summary>Extends the lock to <paramref name="ttl"/> (default <see cref="Ttl"/>) from now. False when the lock was lost.</summary>
    /// <param name="ttl">The new lifetime counted from now. Null uses <see cref="Ttl"/>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply; a command already sent still runs.</param>
    /// <returns><see langword="true"/> when the key still held this token and its expiry was reset; <see langword="false"/> when the lock expired or moved to another owner, or was already released.</returns>
    /// <exception cref="RuvioException">The server refuses the command.</exception>
    /// <example>
    /// <code>
    /// if (!await held.RenewAsync()) { /* lost the lock: stop the work */ }
    /// </code>
    /// </example>
    public ValueTask<bool> RenewAsync(TimeSpan? ttl = null, CancellationToken cancellationToken = default) =>
        IsReleased
            ? new ValueTask<bool>(false)
            : _client.SetIfEqualAsync(Key, Token, Token, ttl ?? Ttl, cancellationToken);

    /// <summary>Deletes the key only while it holds this token. False when the lock had already expired or moved on.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply; a command already sent still runs.</param>
    /// <returns><see langword="true"/> when this call deleted the key; <see langword="false"/> when the lock had expired, belongs to someone else, or was already released by an earlier call.</returns>
    /// <exception cref="RuvioException">The server refuses the command.</exception>
    /// <remarks>Only the first call sends <c>DELEX</c>; later calls return <see langword="false"/> without a round trip.</remarks>
    public ValueTask<bool> ReleaseAsync(CancellationToken cancellationToken = default) =>
        Interlocked.Exchange(ref _released, 1) != 0
            ? new ValueTask<bool>(false)
            : _client.DeleteIfEqualAsync(Key, Token, cancellationToken);

    /// <summary>Releases the lock (once). Errors are not swallowed.</summary>
    /// <exception cref="RuvioException">The server refuses the command.</exception>
    public async ValueTask DisposeAsync() => _ = await ReleaseAsync().ConfigureAwait(false);
}

/// <summary>
/// A token from <c>LEASE</c> (a fencing token: it only grows for one key) or from <c>SEMAPHORE</c> (one slot).
/// <c>await using</c> sends <c>RELEASE key token</c>, which frees it only while this token still holds it.
/// </summary>
/// <remarks>
/// Get one from <see cref="RuvioClientHelpers.TryAcquireLeaseAsync"/> or <see cref="RuvioClientHelpers.TryAcquireSemaphoreAsync"/>.
/// It also frees itself when the TTL passed at acquisition ends.
/// </remarks>
/// <example>
/// <code>
/// await using RuvioLease? slot = await db.TryAcquireSemaphoreAsync("sem:uploads", 4, TimeSpan.FromMinutes(1));
/// if (slot is null) { /* four uploads already running */ }
/// </code>
/// </example>
public sealed class RuvioLease : IAsyncDisposable
{
    private readonly IRuvioClient client;
    private int released;

    internal RuvioLease(IRuvioClient client, string key, long token)
    {
        this.client = client;
        Key = key;
        Token = token;
    }

    /// <summary>The lease or semaphore key.</summary>
    public string Key { get; }

    /// <summary>The token. For <c>LEASE</c> it is a fencing token: pass it to the guarded resource and reject smaller ones.</summary>
    public long Token { get; }

    /// <summary>True after <see cref="ReleaseAsync"/> or <see cref="DisposeAsync"/> ran.</summary>
    public bool IsReleased => Volatile.Read(ref released) != 0;

    /// <summary>Sends <c>RENEW key token ttl</c>: moves the deadline to now + <paramref name="ttl"/>. The token stays the same.</summary>
    /// <param name="ttl">The new lifetime. It replaces the remaining time.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply; a command already sent still runs.</param>
    /// <returns><see langword="true"/> when this token still held the lease or slot; <see langword="false"/> when it had expired or moved on, or this object was already released.</returns>
    /// <exception cref="RuvioException">The server refuses the command.</exception>
    public async ValueTask<bool> RenewAsync(TimeSpan ttl, CancellationToken cancellationToken = default) =>
        !IsReleased
        && await client.RenewAsync(Key, Token, (long)ttl.TotalMilliseconds, cancellationToken).ConfigureAwait(false) > 0;

    /// <summary>Sends <c>RELEASE key token</c> once. False when the token no longer held it.</summary>
    /// <param name="cancellationToken">Abandons the wait for the reply; a command already sent still runs.</param>
    /// <returns><see langword="true"/> when the server freed this token; <see langword="false"/> when it had expired or moved on, or this object was already released.</returns>
    /// <exception cref="RuvioException">The server refuses the command.</exception>
    public async ValueTask<bool> ReleaseAsync(CancellationToken cancellationToken = default) =>
        Interlocked.Exchange(ref released, 1) == 0
        && await client.ReleaseAsync(Key, Token, cancellationToken).ConfigureAwait(false) > 0;

    /// <summary>Releases the token (once).</summary>
    /// <exception cref="RuvioException">The server refuses the command.</exception>
    public async ValueTask DisposeAsync() => _ = await ReleaseAsync().ConfigureAwait(false);
}

/// <summary>
/// Higher-level patterns built on Ruvio's native commands: locks, leases, semaphores, rate limits,
/// cache-aside with stampede protection, scan iterators, recent-items lists and "seen before" filters.
/// They work on any <see cref="IRuvioClient"/> and are called as extension methods on it.
/// </summary>
/// <remarks>
/// Each helper only combines the documented commands, so its cost and failure modes are those of the commands it names.
/// A server error surfaces as <see cref="RuvioException"/>; a cancelled token throws <see cref="OperationCanceledException"/>.
/// </remarks>
public static class RuvioClientHelpers
{
    private const int MaxBackoffMilliseconds = 25;

    // ---- Scripts -------------------------------------------------------------------------------

    /// <summary>
    /// Runs a script by its SHA1 and sends the whole script only when the shard does not have it: <c>EVALSHA</c>, then <c>EVAL</c> once on <c>NOSCRIPT</c>.
    /// The digest is computed here, and <c>EVAL</c> also caches the script on that shard.
    /// </summary>
    /// <param name="client">The client to send the commands on.</param>
    /// <param name="script">The Lua source.</param>
    /// <param name="keyCount">How many of <paramref name="arguments"/> are key names.</param>
    /// <param name="arguments">The key names first, then the other arguments.</param>
    /// <returns>The script's result as a <see cref="RespValue"/>.</returns>
    /// <remarks>The retry is safe because <c>NOSCRIPT</c> is answered before any command of the script ran, so a script never runs twice.</remarks>
    /// <exception cref="RuvioException">As for <see cref="IRuvioClient.EvaluateAsync"/>.</exception>
    /// <example>
    /// <code>
    /// RespValue value = await db.EvaluateCachedAsync("return ARGV[1]", 0, "x");
    /// </code>
    /// </example>
    public static async ValueTask<RespValue> EvaluateCachedAsync(this IRuvioClient client, string script, long keyCount, params string[] arguments)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(script);

        string sha = Convert.ToHexString(SHA1.HashData(Encoding.UTF8.GetBytes(script))).ToLowerInvariant();

        try
        {
            return await client.EvaluateShaAsync(sha, keyCount, arguments).ConfigureAwait(false);
        }
        catch (RuvioException error) when (error.Message.StartsWith("NOSCRIPT", StringComparison.Ordinal))
        {
            return await client.EvaluateAsync(script, keyCount, arguments).ConfigureAwait(false);
        }
    }

    // ---- Locks, leases, semaphores -------------------------------------------------------------

    /// <summary>
    /// Tries once to take the lock at <paramref name="key"/> for <paramref name="ttl"/> (<c>SET key token NX PX ttl</c>).
    /// Null when another owner holds it.
    /// </summary>
    /// <param name="client">The client to send the commands on.</param>
    /// <param name="key">The lock key. Use a key that holds nothing else.</param>
    /// <param name="ttl">How long the lock lives unless renewed. Must be positive; it is sent in milliseconds.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply; a command already sent still runs.</param>
    /// <returns>The held lock, or <see langword="null"/> when the key is already taken.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="ttl"/> is zero or negative.</exception>
    /// <exception cref="RuvioException">The key holds a value that is not a string (<c>WRONGTYPE</c>) or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// await using RuvioLock? held = await db.TryAcquireLockAsync("lock:job", TimeSpan.FromSeconds(10));
    /// if (held is null) return;   // someone else is running it
    /// </code>
    /// </example>
    public static async ValueTask<RuvioLock?> TryAcquireLockAsync(
        this IRuvioClient client,
        string key,
        TimeSpan ttl,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);

        if (ttl <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(ttl), "The lock TTL must be positive.");
        }

        string token = NewToken();

        return await client.SetAsync(key, token, ttl, SetMode.IfNotExists, cancellationToken).ConfigureAwait(false)
            ? new RuvioLock(client, key, token, ttl)
            : null;
    }

    /// <summary>
    /// Takes the lock at <paramref name="key"/>, retrying with a short backoff (2 to 25 ms) for up to <paramref name="wait"/>.
    /// Null when it is still held after that.
    /// </summary>
    /// <param name="client">The client to send the commands on.</param>
    /// <param name="key">The lock key.</param>
    /// <param name="ttl">How long the lock lives unless renewed. Must be positive.</param>
    /// <param name="wait">The longest total time to keep retrying. <see cref="TimeSpan.Zero"/> tries once.</param>
    /// <param name="cancellationToken">Stops the retry loop with an <see cref="OperationCanceledException"/>.</param>
    /// <returns>The held lock, or <see langword="null"/> when <paramref name="wait"/> ran out.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="ttl"/> is zero or negative.</exception>
    /// <exception cref="RuvioException">The key holds a value that is not a string or the server refuses the command.</exception>
    /// <remarks>Polls with plain commands, so it never holds the shared connection while it waits.</remarks>
    /// <example>
    /// <code>
    /// await using RuvioLock? held = await db.AcquireLockAsync("lock:job", TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(5));
    /// </code>
    /// </example>
    public static async ValueTask<RuvioLock?> AcquireLockAsync(
        this IRuvioClient client,
        string key,
        TimeSpan ttl,
        TimeSpan wait,
        CancellationToken cancellationToken = default)
    {
        long started = Stopwatch.GetTimestamp();
        int delay = 2;

        while (true)
        {
            if (await client.TryAcquireLockAsync(key, ttl, cancellationToken).ConfigureAwait(false) is { } held)
            {
                return held;
            }

            if (Stopwatch.GetElapsedTime(started) >= wait)
            {
                return null;
            }

            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            delay = Math.Min(delay * 2, MaxBackoffMilliseconds);
        }
    }

    /// <summary>Sends <c>LEASE key ttl</c>. Null while another holder's lease is live; otherwise a fenced lease.</summary>
    /// <param name="client">The client to send the command on.</param>
    /// <param name="key">The lease key.</param>
    /// <param name="ttl">How long the lease lives, rounded up to whole milliseconds (at least 1 ms).</param>
    /// <param name="cancellationToken">Abandons the wait for the reply; a command already sent still runs.</param>
    /// <returns>The lease with its fencing token, or <see langword="null"/> when another holder's lease is live.</returns>
    /// <exception cref="RuvioException">The key holds another type or the server refuses the command.</exception>
    /// <remarks>The token grows with every new lease on the key. Hand it to the guarded resource, which should reject smaller tokens.</remarks>
    /// <example>
    /// <code>
    /// await using RuvioLease? lease = await db.TryAcquireLeaseAsync("leader:jobs", TimeSpan.FromSeconds(15));
    /// if (lease is not null) await DoLeaderWorkAsync(lease.Token);
    /// </code>
    /// </example>
    public static async ValueTask<RuvioLease?> TryAcquireLeaseAsync(
        this IRuvioClient client,
        string key,
        TimeSpan ttl,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        long? token = await client.LeaseAsync(key, RespRead.Milliseconds(ttl), cancellationToken).ConfigureAwait(false);

        return token is { } value ? new RuvioLease(client, key, value) : null;
    }

    /// <summary>Sends <c>SEMAPHORE key max ttl</c>. Null when <paramref name="max"/> holders are live; otherwise one slot.</summary>
    /// <param name="client">The client to send the command on.</param>
    /// <param name="key">The semaphore key.</param>
    /// <param name="max">The most holders allowed at once.</param>
    /// <param name="ttl">How long this slot lives, rounded up to whole milliseconds (at least 1 ms).</param>
    /// <param name="cancellationToken">Abandons the wait for the reply; a command already sent still runs.</param>
    /// <returns>A handle for one slot, or <see langword="null"/> when <paramref name="max"/> holders are live.</returns>
    /// <exception cref="RuvioException">The key holds another type or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// await using RuvioLease? slot = await db.TryAcquireSemaphoreAsync("sem:exports", 3, TimeSpan.FromMinutes(5));
    /// </code>
    /// </example>
    public static async ValueTask<RuvioLease?> TryAcquireSemaphoreAsync(
        this IRuvioClient client,
        string key,
        long max,
        TimeSpan ttl,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        long? token = await client.AcquireSemaphoreAsync(key, max, RespRead.Milliseconds(ttl), cancellationToken).ConfigureAwait(false);

        return token is { } value ? new RuvioLease(client, key, value) : null;
    }

    // ---- Rate limit, idempotency -----------------------------------------------------------------

    /// <summary>
    /// One fixed-window rate-limit decision with <c>LIMIT key max window</c>. A refused call does not count.
    /// </summary>
    /// <param name="client">The client to send the command on.</param>
    /// <param name="key">The counter key, for example one per user or IP address.</param>
    /// <param name="max">How many calls fit in one window.</param>
    /// <param name="window">The window length, rounded up to whole milliseconds (at least 1 ms).</param>
    /// <param name="cancellationToken">Abandons the wait for the reply; a command already sent still runs.</param>
    /// <returns>A <see cref="RateLimitResult"/>: <c>Allowed</c> with the allowance left, or not allowed with 0 remaining.</returns>
    /// <exception cref="RuvioException">The key holds another type or the server refuses the command.</exception>
    /// <example>
    /// <code>
    /// RateLimitResult r = await db.RateLimitAsync($"rl:{userId}", 100, TimeSpan.FromMinutes(1));
    /// if (!r.Allowed) return Results.StatusCode(429);
    /// </code>
    /// </example>
    public static async ValueTask<RateLimitResult> RateLimitAsync(
        this IRuvioClient client,
        string key,
        long max,
        TimeSpan window,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        long? remaining = await client.LimitAsync(key, max, RespRead.Milliseconds(window), cancellationToken).ConfigureAwait(false);

        return remaining is { } left ? new RateLimitResult(true, left) : new RateLimitResult(false, 0);
    }

    // ---- Cache-aside --------------------------------------------------------------------------------

    /// <summary>
    /// Returns the cached string or builds it with <paramref name="factory"/>, letting one caller across every process
    /// build it while the others wait (<c>FILLGET</c>/<c>FILLSET</c> stampede protection). Waiters poll plain
    /// <c>FILLGET</c> with a 2 to 25 ms backoff instead of <c>FILLGET ... WAIT</c>, because a server-side wait parks
    /// the whole shared connection. A waiter that outlasts <see cref="GetOrCreateOptions.WaitTimeout"/> builds and
    /// stores the value itself. With <see cref="GetOrCreateOptions.StaleWindow"/>, callers keep getting the stale
    /// value while the one caller holding the refresh token rebuilds it.
    /// </summary>
    /// <param name="client">The client to send the commands on.</param>
    /// <param name="key">The cache key.</param>
    /// <param name="ttl">How long a built value stays fresh. Must be positive.</param>
    /// <param name="factory">Builds the value on a miss. It runs in at most one caller at a time per key, unless a waiter times out.</param>
    /// <param name="options">Fill, wait and stale-window settings. Null uses <see cref="GetOrCreateOptions.Default"/>.</param>
    /// <param name="cancellationToken">Stops the waiting loop and is passed to <paramref name="factory"/>.</param>
    /// <returns>The cached or newly built value.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="ttl"/> is zero or negative.</exception>
    /// <exception cref="RuvioException">The key holds a value that is not a string, or the server refuses a command.</exception>
    /// <remarks>If <paramref name="factory"/> throws, the exception propagates and the fill marker lapses on its own after <see cref="GetOrCreateOptions.FillTimeout"/>, after which another caller can try.</remarks>
    /// <example>
    /// <code>
    /// string html = await db.GetOrCreateAsync(
    ///     "page:home", TimeSpan.FromMinutes(1), async ct => await RenderHomeAsync(ct));
    /// </code>
    /// </example>
    public static async ValueTask<string> GetOrCreateAsync(
        this IRuvioClient client,
        string key,
        TimeSpan ttl,
        Func<CancellationToken, ValueTask<string>> factory,
        GetOrCreateOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(factory);

        if (ttl <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(ttl), "The cache TTL must be positive.");
        }

        options ??= GetOrCreateOptions.Default;
        long started = Stopwatch.GetTimestamp();
        int delay = 2;
        FillResult fill;

        while (true)
        {
            fill = await client.FillGetAsync(key, options.FillTimeout, wait: null, cancellationToken).ConfigureAwait(false);

            if (fill.State != FillState.Busy || Stopwatch.GetElapsedTime(started) >= options.WaitTimeout)
            {
                break;
            }

            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            delay = Math.Min(delay * 2, MaxBackoffMilliseconds);
        }

        switch (fill.State)
        {
            case FillState.Hit:
            case FillState.Stale when fill.Token is null:
                return Encoding.UTF8.GetString(fill.Value!);
        }

        string value = await factory(cancellationToken).ConfigureAwait(false);

        if (fill.Token is { } token)
        {
            // False means the marker lapsed while the factory ran; the value is still right for this caller.
            _ = await client.FillSetAsync(key, value, ttl, token, options.StaleWindow, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            // Waited out the builder: store it plainly so the next caller hits.
            _ = await client.SetAsync(key, value, ttl, SetMode.Always, cancellationToken).ConfigureAwait(false);
        }

        return value;
    }

    /// <summary>
    /// <see cref="GetOrCreateAsync(IRuvioClient, string, TimeSpan, Func{CancellationToken, ValueTask{string}}, GetOrCreateOptions?, CancellationToken)"/>
    /// for a typed value stored as JSON with <paramref name="typeInfo"/> (trimming and AOT safe).
    /// </summary>
    /// <typeparam name="T">The cached type.</typeparam>
    /// <param name="client">The client to send the commands on.</param>
    /// <param name="key">The cache key.</param>
    /// <param name="ttl">How long a built value stays fresh. Must be positive.</param>
    /// <param name="factory">Builds the value on a miss.</param>
    /// <param name="typeInfo">Source-generated JSON metadata for <typeparamref name="T"/>.</param>
    /// <param name="options">Fill, wait and stale-window settings. Null uses <see cref="GetOrCreateOptions.Default"/>.</param>
    /// <param name="cancellationToken">Stops the waiting loop and is passed to <paramref name="factory"/>.</param>
    /// <returns>The cached or newly built value.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="ttl"/> is zero or negative.</exception>
    /// <exception cref="RuvioException">The key holds a value that is not a string, or the server refuses a command.</exception>
    /// <exception cref="JsonException">The cached text is not valid JSON for <typeparamref name="T"/>.</exception>
    /// <example>
    /// <code>
    /// Profile p = await db.GetOrCreateAsync(
    ///     "profile:7", TimeSpan.FromMinutes(5), ct => LoadProfileAsync(7, ct), AppJson.Default.Profile);
    /// </code>
    /// </example>
    public static async ValueTask<T> GetOrCreateAsync<T>(
        this IRuvioClient client,
        string key,
        TimeSpan ttl,
        Func<CancellationToken, ValueTask<T>> factory,
        JsonTypeInfo<T> typeInfo,
        GetOrCreateOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentNullException.ThrowIfNull(typeInfo);
        T? built = default;
        bool ran = false;

        string json = await client.GetOrCreateAsync(
            key,
            ttl,
            async token =>
            {
                built = await factory(token).ConfigureAwait(false);
                ran = true;

                return JsonSerializer.Serialize(built, typeInfo);
            },
            options,
            cancellationToken).ConfigureAwait(false);

        return ran ? built! : JsonSerializer.Deserialize(json, typeInfo)!;
    }

    /// <summary>Reads a string key and deserializes its JSON text into <typeparamref name="T"/>.</summary>
    /// <typeparam name="T">The type to read.</typeparam>
    /// <param name="client">The client to send the command on.</param>
    /// <param name="key">The key.</param>
    /// <param name="typeInfo">Source-generated JSON metadata for <typeparamref name="T"/>.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply.</param>
    /// <returns>The value, or null when the key is missing.</returns>
    /// <exception cref="RuvioException">The key holds a value that is not a string.</exception>
    /// <exception cref="JsonException">The stored text is not valid JSON for <typeparamref name="T"/>.</exception>
    /// <example>
    /// <code>
    /// Profile? p = await db.GetJsonAsync("profile:7", AppJson.Default.Profile);
    /// </code>
    /// </example>
    public static async ValueTask<T?> GetJsonAsync<T>(
        this IRuvioClient client,
        string key,
        JsonTypeInfo<T> typeInfo,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(typeInfo);
        string? json = await client.GetStringAsync(key, cancellationToken).ConfigureAwait(false);

        return json is null ? default : JsonSerializer.Deserialize(json, typeInfo);
    }

    /// <summary>Serializes <paramref name="value"/> as JSON and stores it with <c>SET</c>.</summary>
    /// <typeparam name="T">The type to store.</typeparam>
    /// <param name="client">The client to send the command on.</param>
    /// <param name="key">The key.</param>
    /// <param name="value">The value to serialize.</param>
    /// <param name="typeInfo">Source-generated JSON metadata for <typeparamref name="T"/>.</param>
    /// <param name="expiry">How long the value lives. Null stores it without an expiry.</param>
    /// <param name="mode">Whether to write always, only when absent, or only when present.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply.</param>
    /// <returns><see langword="true"/> when the value was written.</returns>
    /// <example>
    /// <code>
    /// await db.SetJsonAsync("profile:7", profile, AppJson.Default.Profile, TimeSpan.FromMinutes(5));
    /// </code>
    /// </example>
    public static ValueTask<bool> SetJsonAsync<T>(
        this IRuvioClient client,
        string key,
        T value,
        JsonTypeInfo<T> typeInfo,
        TimeSpan? expiry = null,
        SetMode mode = SetMode.Always,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(typeInfo);

        return client.SetAsync(key, JsonSerializer.Serialize(value, typeInfo), expiry, mode, cancellationToken);
    }

    /// <summary>
    /// <see cref="IRuvioClient.FillGetAsync"/> with the value read as JSON into <typeparamref name="T"/>.
    /// </summary>
    /// <typeparam name="T">The cached type.</typeparam>
    /// <param name="client">The client to send the command on.</param>
    /// <param name="key">The cache key.</param>
    /// <param name="fill">How long the fill marker lives for the caller told to fill.</param>
    /// <param name="typeInfo">Source-generated JSON metadata for <typeparamref name="T"/>.</param>
    /// <param name="wait">Holds a <see cref="FillState.Busy"/> caller up to this long. Null does not wait; see <see cref="GetOrCreateAsync{T}(IRuvioClient, string, TimeSpan, Func{CancellationToken, ValueTask{T}}, JsonTypeInfo{T}, GetOrCreateOptions?, CancellationToken)"/> for the polling form that does not hold the connection.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply.</param>
    /// <returns>The state, the decoded value when one is stored, and the fill token when this caller must build it.</returns>
    /// <exception cref="JsonException">The stored text is not valid JSON for <typeparamref name="T"/>.</exception>
    /// <example>
    /// <code>
    /// FillResult&lt;Profile&gt; r = await db.FillGetJsonAsync("profile:7", TimeSpan.FromSeconds(10), AppJson.Default.Profile);
    /// if (r.State == FillState.Fill) await db.FillSetJsonAsync("profile:7", await LoadAsync(), TimeSpan.FromMinutes(5), r.Token!.Value, AppJson.Default.Profile);
    /// </code>
    /// </example>
    public static async ValueTask<FillResult<T>> FillGetJsonAsync<T>(
        this IRuvioClient client,
        string key,
        TimeSpan fill,
        JsonTypeInfo<T> typeInfo,
        TimeSpan? wait = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(typeInfo);
        FillResult raw = await client.FillGetAsync(key, fill, wait, cancellationToken).ConfigureAwait(false);
        T? value = raw.Value is null ? default : JsonSerializer.Deserialize(raw.Value, typeInfo);

        return new FillResult<T>(raw.State, value, raw.Token);
    }

    /// <summary>
    /// <see cref="IRuvioClient.FillSetAsync"/> with <paramref name="value"/> serialized as JSON.
    /// </summary>
    /// <typeparam name="T">The cached type.</typeparam>
    /// <param name="client">The client to send the command on.</param>
    /// <param name="key">The cache key.</param>
    /// <param name="value">The value to serialize and store.</param>
    /// <param name="ttl">How long the value is fresh.</param>
    /// <param name="token">The token from <see cref="FillResult{T}.Token"/>.</param>
    /// <param name="typeInfo">Source-generated JSON metadata for <typeparamref name="T"/>.</param>
    /// <param name="stale">Stale-while-revalidate window. Null stores none.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply.</param>
    /// <returns><see langword="true"/> when the token was still live and the value was stored.</returns>
    /// <example>
    /// <code>
    /// bool stored = await db.FillSetJsonAsync("profile:7", profile, TimeSpan.FromMinutes(5), token, AppJson.Default.Profile);
    /// </code>
    /// </example>
    public static ValueTask<bool> FillSetJsonAsync<T>(
        this IRuvioClient client,
        string key,
        T value,
        TimeSpan ttl,
        long token,
        JsonTypeInfo<T> typeInfo,
        TimeSpan? stale = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(typeInfo);

        return client.FillSetAsync(key, JsonSerializer.Serialize(value, typeInfo), ttl, token, stale, cancellationToken);
    }

    // ---- Scan iterators ---------------------------------------------------------------------------

    /// <summary>Lazily walks <c>SCAN</c> pages. Keys added or removed during the walk may or may not appear.</summary>
    /// <param name="client">The client to scan.</param>
    /// <param name="pattern">Glob filter on key names. Null returns all.</param>
    /// <param name="count">A hint for the page size. Null uses the server default.</param>
    /// <param name="type">Only keys of this type, for example <c>hash</c>. Null returns all.</param>
    /// <param name="cancellationToken">Stops the walk with an <see cref="OperationCanceledException"/>.</param>
    /// <returns>A sequence of key names; it ends when the server returns cursor 0.</returns>
    /// <exception cref="RuvioException">The server refuses a <c>SCAN</c> page.</exception>
    /// <remarks>Each page is one <c>SCAN</c> call. In a cluster the walk goes through the shards in turn.</remarks>
    /// <example>
    /// <code>
    /// await foreach (string key in db.ScanKeysAsync("session:*", count: 500))
    /// {
    ///     Console.WriteLine(key);
    /// }
    /// </code>
    /// </example>
    public static async IAsyncEnumerable<string> ScanKeysAsync(
        this IRuvioClient client,
        string? pattern = null,
        int? count = null,
        string? type = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        long cursor = 0;

        do
        {
            ScanPage page = await client.ScanAsync(cursor, pattern, count, type, cancellationToken).ConfigureAwait(false);

            foreach (string key in page.Keys)
            {
                yield return key;
            }

            cursor = page.Cursor;
        }
        while (cursor != 0);
    }

    /// <summary>Lazily walks <c>HSCAN</c> pages of one hash.</summary>
    /// <param name="client">The client to scan.</param>
    /// <param name="key">The hash key.</param>
    /// <param name="pattern">Glob filter on field names. Null returns all.</param>
    /// <param name="count">A hint for the page size. Null uses the server default.</param>
    /// <param name="cancellationToken">Stops the walk with an <see cref="OperationCanceledException"/>.</param>
    /// <returns>Field and value pairs; empty when the key is missing.</returns>
    /// <exception cref="RuvioException">The key holds another type (<c>WRONGTYPE</c>).</exception>
    /// <example>
    /// <code>
    /// await foreach (var (field, value) in db.ScanHashEntriesAsync("user:1")) { }
    /// </code>
    /// </example>
    public static async IAsyncEnumerable<KeyValuePair<string, string>> ScanHashEntriesAsync(
        this IRuvioClient client,
        string key,
        string? pattern = null,
        int? count = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        long cursor = 0;

        do
        {
            HashScanPage page = await client.ScanHashAsync(key, cursor, pattern, count, cancellationToken).ConfigureAwait(false);

            foreach (KeyValuePair<string, string> field in page.Fields)
            {
                yield return field;
            }

            cursor = page.Cursor;
        }
        while (cursor != 0);
    }

    /// <summary>Lazily walks <c>SSCAN</c> pages of one set.</summary>
    /// <param name="client">The client to scan.</param>
    /// <param name="key">The set key.</param>
    /// <param name="pattern">Glob filter on members. Null returns all.</param>
    /// <param name="count">A hint for the page size. Null uses the server default.</param>
    /// <param name="cancellationToken">Stops the walk with an <see cref="OperationCanceledException"/>.</param>
    /// <returns>The members; empty when the key is missing.</returns>
    /// <exception cref="RuvioException">The key holds another type (<c>WRONGTYPE</c>).</exception>
    /// <example>
    /// <code>
    /// await foreach (string tag in db.ScanSetMembersAsync("tags")) { }
    /// </code>
    /// </example>
    public static async IAsyncEnumerable<string> ScanSetMembersAsync(
        this IRuvioClient client,
        string key,
        string? pattern = null,
        int? count = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        long cursor = 0;

        do
        {
            SetScanPage page = await client.ScanSetAsync(key, cursor, pattern, count, cancellationToken).ConfigureAwait(false);

            foreach (string member in page.Members)
            {
                yield return member;
            }

            cursor = page.Cursor;
        }
        while (cursor != 0);
    }

    /// <summary>Lazily walks <c>ZSCAN</c> pages of one sorted set.</summary>
    /// <param name="client">The client to scan.</param>
    /// <param name="key">The sorted set key.</param>
    /// <param name="pattern">Glob filter on members. Null returns all.</param>
    /// <param name="count">A hint for the page size. Null uses the server default.</param>
    /// <param name="cancellationToken">Stops the walk with an <see cref="OperationCanceledException"/>.</param>
    /// <returns>The members with their scores; empty when the key is missing.</returns>
    /// <exception cref="RuvioException">The key holds another type (<c>WRONGTYPE</c>).</exception>
    /// <example>
    /// <code>
    /// await foreach (SortedSetEntry entry in db.ScanSortedSetEntriesAsync("leaderboard")) { }
    /// </code>
    /// </example>
    public static async IAsyncEnumerable<SortedSetEntry> ScanSortedSetEntriesAsync(
        this IRuvioClient client,
        string key,
        string? pattern = null,
        int? count = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        long cursor = 0;

        do
        {
            SortedSetScanPage page = await client.ScanSortedSetAsync(key, cursor, pattern, count, cancellationToken).ConfigureAwait(false);

            foreach (SortedSetEntry entry in page.Entries)
            {
                yield return entry;
            }

            cursor = page.Cursor;
        }
        while (cursor != 0);
    }

    // ---- Recent items, seen-before --------------------------------------------------------------

    /// <summary>Pushes <paramref name="value"/> to the head of a "recent items" list kept at most <paramref name="cap"/> long (<c>LPUSHCAP</c>).</summary>
    /// <param name="client">The client to send the command on.</param>
    /// <param name="key">The list key.</param>
    /// <param name="value">The item to remember.</param>
    /// <param name="cap">The most items the list keeps; older ones fall off the tail.</param>
    /// <returns>The list length after the push and trim.</returns>
    /// <exception cref="RuvioException">The key holds another type (<c>WRONGTYPE</c>).</exception>
    /// <example>
    /// <code>
    /// await db.PushRecentAsync("recent:user:1", "product:42", 20);
    /// </code>
    /// </example>
    public static ValueTask<long> PushRecentAsync(this IRuvioClient client, string key, string value, long cap)
    {
        ArgumentNullException.ThrowIfNull(client);

        return client.PushListLeftCappedAsync(key, cap, value);
    }

    /// <summary>The newest <paramref name="count"/> items of a list written by <see cref="PushRecentAsync"/>, newest first.</summary>
    /// <param name="client">The client to send the command on.</param>
    /// <param name="key">The list key.</param>
    /// <param name="count">How many items. Zero or negative returns an empty list without a round trip.</param>
    /// <param name="cancellationToken">Abandons the wait for the reply; a command already sent still runs.</param>
    /// <returns>Up to <paramref name="count"/> items; fewer when the list is shorter, empty when the key is missing.</returns>
    /// <exception cref="RuvioException">The key holds another type (<c>WRONGTYPE</c>).</exception>
    /// <example>
    /// <code>
    /// IReadOnlyList&lt;string&gt; last5 = await db.GetRecentAsync("recent:user:1", 5);
    /// </code>
    /// </example>
    public static ValueTask<IReadOnlyList<string>> GetRecentAsync(
        this IRuvioClient client,
        string key,
        long count,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);

        return count <= 0
            ? new ValueTask<IReadOnlyList<string>>(Array.Empty<string>())
            : client.GetListRangeAsync(key, 0, count - 1, cancellationToken);
    }

    /// <summary>
    /// Records <paramref name="items"/> in a Bloom filter (<c>BF.MADD</c>) and returns, per item, whether it was
    /// (probably) seen before. A false answer is certain; a true answer can be a false positive at the filter's error rate.
    /// </summary>
    /// <param name="client">The client to send the command on.</param>
    /// <param name="key">The Bloom filter key. A missing key becomes a scalable filter with the server defaults.</param>
    /// <param name="items">The items to record and check.</param>
    /// <returns>One flag per item, in order: true when it was (probably) already in the filter.</returns>
    /// <exception cref="RuvioException">The key holds another type, or a non-scaling filter is full.</exception>
    /// <example>
    /// <code>
    /// IReadOnlyList&lt;bool&gt; seen = await db.SeenBeforeAsync("seen:events", "evt-1", "evt-2");
    /// </code>
    /// </example>
    public static async ValueTask<IReadOnlyList<bool>> SeenBeforeAsync(this IRuvioClient client, string key, params string[] items)
    {
        ArgumentNullException.ThrowIfNull(client);
        IReadOnlyList<bool> added = await client.AddBloomFilterManyAsync(key, items).ConfigureAwait(false);
        var seen = new bool[added.Count];

        for (int index = 0; index < seen.Length; index++)
        {
            seen[index] = !added[index];
        }

        return seen;
    }

    /// <summary>Checks <paramref name="items"/> against a Bloom filter without recording them (<c>BF.MEXISTS</c>).</summary>
    /// <param name="client">The client to send the command on.</param>
    /// <param name="key">The Bloom filter key.</param>
    /// <param name="items">The items to check.</param>
    /// <returns>One flag per item: false means definitely absent, true means probably present. All false when the key is missing.</returns>
    /// <exception cref="RuvioException">The key holds another type (<c>WRONGTYPE</c>).</exception>
    /// <example>
    /// <code>
    /// IReadOnlyList&lt;bool&gt; maybe = await db.MaybeSeenAsync("seen:events", "evt-1");
    /// </code>
    /// </example>
    public static ValueTask<IReadOnlyList<bool>> MaybeSeenAsync(this IRuvioClient client, string key, params string[] items)
    {
        ArgumentNullException.ThrowIfNull(client);

        return client.BloomFilterContainsManyAsync(key, items);
    }

    private static string NewToken()
    {
        Span<byte> bytes = stackalloc byte[16];
        RandomNumberGenerator.Fill(bytes);

        return Convert.ToHexString(bytes);
    }
}
