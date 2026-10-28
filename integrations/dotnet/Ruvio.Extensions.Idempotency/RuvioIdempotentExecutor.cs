using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Ruvio.Client;

namespace Ruvio.Extensions.Idempotency;

/// <summary>
/// Runs work at most once per key, outside HTTP: a message consumer, a background job or a gRPC handler. A call begins
/// a record with <c>IDEM key BEGIN</c>, runs the work only when it owns the record, and stores the result with
/// <c>IDEM key COMPLETE</c>; a repeat with the same key and fingerprint gets that result back instead of running the
/// work. The executor neither owns nor disposes the client.
/// </summary>
/// <remarks>
/// By default, when the work throws, the key stays pending until its retention ends, and calls in that time get
/// <see cref="IdempotentOutcome.Pending"/>; <see cref="RuvioIdempotentExecutorOptions.AbortOnException"/> frees it
/// instead. This is at-most-once for the work, not exactly-once for its side effects outside Ruvio.
/// </remarks>
public sealed class RuvioIdempotentExecutor
{
    private const string NoFingerprint = "-";

    private static readonly Meter Meter = new("Ruvio.Extensions.Idempotency");
    private static readonly Counter<long> Outcomes = Meter.CreateCounter<long>("ruvio.idempotency.outcomes");
    private static readonly byte[] Idem = "IDEM"u8.ToArray();
    private static readonly byte[] Complete = "COMPLETE"u8.ToArray();

    private readonly IRuvioClient client;
    private readonly string keyPrefix;
    private readonly long defaultRetention;
    private readonly bool abortOnException;

    /// <summary>Creates an executor on <paramref name="client"/>.</summary>
    /// <param name="client">The client the records live on.</param>
    /// <param name="options">Key prefix and default retention; the defaults when null.</param>
    public RuvioIdempotentExecutor(IRuvioClient client, RuvioIdempotentExecutorOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(client);
        options ??= new RuvioIdempotentExecutorOptions();
        options.Validate();
        this.client = client;
        keyPrefix = options.KeyPrefix;
        defaultRetention = RetentionMilliseconds(options.DefaultRetention, nameof(options.DefaultRetention));
        abortOnException = options.AbortOnException;
    }

    /// <summary>Runs <paramref name="work"/> once for <paramref name="key"/> and stores its bytes for repeats.</summary>
    /// <param name="key">What identifies the work, for example a message id; the key prefix is added.</param>
    /// <param name="fingerprint">
    /// A digest of the work's input (1 to 128 bytes). A repeat with another fingerprint gets
    /// <see cref="IdempotentOutcome.Conflict"/>. Null when the key alone identifies the work.
    /// </param>
    /// <param name="work">The work; its result (up to 4 MiB) is stored and replayed.</param>
    /// <param name="retention">How long the record lives; the executor's default when null.</param>
    /// <param name="cancellationToken">Passed to the work and the commands.</param>
    /// <returns>The outcome and, when the work ran now or before, its result.</returns>
    public async ValueTask<IdempotentResult<byte[]>> ExecuteOnceAsync(
        string key,
        string? fingerprint,
        Func<CancellationToken, Task<byte[]>> work,
        TimeSpan? retention = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(work);
        Call call = await BeginAsync(key, fingerprint, retention, cancellationToken).ConfigureAwait(false);

        if (call.Owner is null)
        {
            return new IdempotentResult<byte[]>(call.Outcome, call.Stored);
        }

        byte[] result;

        try
        {
            result = await work(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception) when (abortOnException)
        {
            await AbortAsync(call).ConfigureAwait(false);
            throw;
        }

        ArgumentNullException.ThrowIfNull(result, "work result");
        bool stored = await CompleteAsync(call, result, cancellationToken).ConfigureAwait(false);

        return new IdempotentResult<byte[]>(IdempotentOutcome.Executed, result, stored);
    }

    /// <summary>Runs <paramref name="work"/> once for <paramref name="key"/>, storing its result as JSON for repeats.</summary>
    /// <typeparam name="T">The result type.</typeparam>
    /// <param name="key">What identifies the work; the key prefix is added.</param>
    /// <param name="fingerprint">A digest of the work's input (1 to 128 bytes), or null when the key alone identifies it.</param>
    /// <param name="work">The work.</param>
    /// <param name="typeInfo">The source-generated JSON metadata of <typeparamref name="T"/> (trimming and AOT safe).</param>
    /// <param name="retention">How long the record lives; the executor's default when null.</param>
    /// <param name="cancellationToken">Passed to the work and the commands.</param>
    /// <returns>The outcome and, when the work ran now or before, its result.</returns>
    public async ValueTask<IdempotentResult<T>> ExecuteOnceAsync<T>(
        string key,
        string? fingerprint,
        Func<CancellationToken, Task<T>> work,
        JsonTypeInfo<T> typeInfo,
        TimeSpan? retention = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(work);
        ArgumentNullException.ThrowIfNull(typeInfo);
        Call call = await BeginAsync(key, fingerprint, retention, cancellationToken).ConfigureAwait(false);

        if (call.Owner is null)
        {
            T? replayed = call.Outcome == IdempotentOutcome.Replayed ? JsonSerializer.Deserialize(call.Stored!, typeInfo) : default;

            return new IdempotentResult<T>(call.Outcome, replayed);
        }

        T result;

        try
        {
            result = await work(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception) when (abortOnException)
        {
            await AbortAsync(call).ConfigureAwait(false);
            throw;
        }

        bool stored = await CompleteAsync(call, JsonSerializer.SerializeToUtf8Bytes(result, typeInfo), cancellationToken).ConfigureAwait(false);

        return new IdempotentResult<T>(IdempotentOutcome.Executed, result, stored);
    }

    /// <summary>Runs <paramref name="work"/> once for <paramref name="key"/>; for work with no result to replay.</summary>
    /// <param name="key">What identifies the work; the key prefix is added.</param>
    /// <param name="fingerprint">A digest of the work's input (1 to 128 bytes), or null when the key alone identifies it.</param>
    /// <param name="work">The work.</param>
    /// <param name="retention">How long the record lives; the executor's default when null.</param>
    /// <param name="cancellationToken">Passed to the work and the commands.</param>
    /// <returns>
    /// <see cref="IdempotentOutcome.Executed"/> when this call ran it, <see cref="IdempotentOutcome.Replayed"/> when an
    /// earlier call finished it, or <see cref="IdempotentOutcome.Pending"/> / <see cref="IdempotentOutcome.Conflict"/>.
    /// </returns>
    public async ValueTask<IdempotentOutcome> ExecuteOnceAsync(
        string key,
        string? fingerprint,
        Func<CancellationToken, Task> work,
        TimeSpan? retention = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(work);
        Call call = await BeginAsync(key, fingerprint, retention, cancellationToken).ConfigureAwait(false);

        if (call.Owner is null)
        {
            return call.Outcome;
        }

        try
        {
            await work(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception) when (abortOnException)
        {
            await AbortAsync(call).ConfigureAwait(false);
            throw;
        }

        await CompleteAsync(call, [], cancellationToken).ConfigureAwait(false);

        return IdempotentOutcome.Executed;
    }

    internal static long RetentionMilliseconds(TimeSpan retention, string name)
    {
        if (retention < TimeSpan.FromMilliseconds(1) || retention > RuvioIdempotentExecutorOptions.MaximumRetention
            || retention.Ticks % TimeSpan.TicksPerMillisecond != 0)
        {
            throw new ArgumentOutOfRangeException(name, "The retention must be whole milliseconds from 1 ms to 7 days.");
        }

        return (long)retention.TotalMilliseconds;
    }

    private async ValueTask<Call> BeginAsync(string key, string? fingerprint, TimeSpan? retention, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        fingerprint ??= NoFingerprint;

        if (fingerprint.Length == 0 || Encoding.UTF8.GetByteCount(fingerprint) > 128)
        {
            throw new ArgumentOutOfRangeException(nameof(fingerprint), "The fingerprint must be 1 to 128 bytes.");
        }

        long milliseconds = retention is { } given ? RetentionMilliseconds(given, nameof(retention)) : defaultRetention;
        string fullKey = keyPrefix + key;
        // Unguessable per call: only this call can complete the record it began.
        string owner = Guid.NewGuid().ToString("N");
        IdempotencyBegin begin = await client.TryBeginIdempotencyAsync(fullKey, fingerprint, owner,
            TimeSpan.FromMilliseconds(milliseconds), cancellationToken).ConfigureAwait(false);
        IdempotentOutcome outcome = begin.State switch
        {
            IdempotencyState.Acquired => IdempotentOutcome.Executed,
            IdempotencyState.Completed => IdempotentOutcome.Replayed,
            IdempotencyState.Pending => IdempotentOutcome.Pending,
            IdempotencyState.Conflict => IdempotentOutcome.Conflict,
            _ => throw new InvalidOperationException("Unknown idempotency state."),
        };

        Count(outcome);

        return outcome == IdempotentOutcome.Executed
            ? new Call(outcome, fullKey, fingerprint, owner, null)
            : new Call(outcome, fullKey, fingerprint, null, begin.Result);
    }

    // COMPLETE answers 0 when the record is gone (it expired while the work ran); the work ran all the same, so this
    // is reported instead of thrown, and never retried: a lost reply does not prove the result was not stored.
    private async ValueTask<bool> CompleteAsync(Call call, byte[] result, CancellationToken cancellationToken)
    {
        ReadOnlyMemory<byte>[] command =
        [
            Idem, Encoding.UTF8.GetBytes(call.Key), Complete, Encoding.UTF8.GetBytes(call.Fingerprint),
            Encoding.ASCII.GetBytes(call.Owner!), result,
        ];
        RespValue reply = await client.ExecuteBinaryAsync(command, cancellationToken).ConfigureAwait(false);

        if (reply.Kind == RespKind.Error)
        {
            throw new RuvioException(reply.Text ?? "IDEM COMPLETE failed.", isServerReply: true);
        }

        if (reply.Kind != RespKind.Integer || reply.Integer is not (0 or 1))
        {
            throw new RuvioException("IDEM COMPLETE answered " + reply.Kind.ToString() + ".");
        }

        return reply.Integer == 1;
    }

    // Sent even when the caller's token caused the failure. A failed abort is swallowed: the work's exception is what
    // the caller needs, and the record then stays pending until its retention ends, as without the option.
    private async ValueTask AbortAsync(Call call)
    {
        try
        {
            if (await client.AbortIdempotencyAsync(call.Key, call.Fingerprint, call.Owner!, CancellationToken.None)
                    .ConfigureAwait(false)
                && Outcomes.Enabled)
            {
                Outcomes.Add(1, new TagList { { "outcome", "aborted" } });
            }
        }
        catch (Exception)
        {
        }
    }

    private static void Count(IdempotentOutcome outcome)
    {
        if (Outcomes.Enabled)
        {
            Outcomes.Add(1, new TagList { { "outcome", outcome switch
            {
                IdempotentOutcome.Executed => "executed",
                IdempotentOutcome.Replayed => "replayed",
                IdempotentOutcome.Pending => "pending",
                _ => "conflict",
            } } });
        }
    }

    private readonly record struct Call(IdempotentOutcome Outcome, string Key, string Fingerprint, string? Owner, byte[]? Stored);
}
