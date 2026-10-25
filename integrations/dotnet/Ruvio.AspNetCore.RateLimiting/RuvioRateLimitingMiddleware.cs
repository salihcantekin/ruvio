using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Ruvio.Client;

namespace Ruvio.AspNetCore.RateLimiting;

/// <summary>
/// Middleware for enforcing Ruvio rate limiting on incoming HTTP requests.
/// </summary>
/// <param name="next">The next middleware in the pipeline.</param>
/// <param name="options">The validated rate-limiting options, read once.</param>
public sealed class RuvioRateLimitingMiddleware(RequestDelegate next, IOptions<RuvioRateLimitingOptions> options)
{
    private readonly RuvioRateLimitingOptions options = options.Value;

    /// <summary>
    /// Invokes the middleware for the given HTTP context, enforcing rate limiting if applicable.
    /// </summary>
    /// <param name="context">The HTTP context for the current request.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public Task InvokeAsync(HttpContext context)
    {
        var policy = context.GetEndpoint()?.Metadata.GetMetadata<RuvioRateLimitAttribute>();

        return policy is null ? next(context) : InvokeLimitedAsync(context, policy);
    }

    private async Task InvokeLimitedAsync(HttpContext context, RuvioRateLimitAttribute policy)
    {
        string partition = options.PartitionKey!(context);

        // An empty partition is a configuration error, not a shared anonymous bucket: it would merge
        // unrelated callers into one budget.
        if (string.IsNullOrWhiteSpace(partition) || partition.Length > 1024)
            throw new InvalidOperationException("The rate-limit partition must have 1-1024 characters.");

        string key = StorageKey(options.KeyPrefix, policy.Policy, partition);
        var client = context.RequestServices.GetRequiredService<IRuvioClient>();
        var outage = options.Outage;

        if (!outage.TryEnter())
        {
            await Outage(context, outage, null);

            return;
        }

        using var activity = Telemetry.Source.StartActivity("ruvio.rate_limit");
        long? remaining;
        long retryAfterMs = 0;
        long started = Stopwatch.GetTimestamp();
        string outcome = "error";
        Exception? unreachable = null;

        try
        {
            // LIMIT checks and counts in one server command, so parallel requests cannot all read the
            // same count and pass.
            long windowMs = (long)policy.WindowSeconds * 1000;

            if (policy.Algorithm == RuvioRateLimitAlgorithm.FixedWindow)
            {
                remaining = await client.LimitAsync(key, policy.Permits, windowMs, context.RequestAborted);
            }
            else
            {
                // WITHRETRY says when the same call would pass, so Retry-After is exact.
                var result = policy.Algorithm == RuvioRateLimitAlgorithm.SlidingWindow
                    ? await client.LimitSlidingWithRetryAsync(key, policy.Permits, windowMs, context.RequestAborted)
                    : await client.LimitTokensWithRetryAsync(key, policy.Permits, windowMs, 1, context.RequestAborted);

                remaining = result.Allowed ? result.Remaining : null;
                retryAfterMs = result.RetryAfterMs;
            }

            outage.Succeeded();
            outcome = remaining is null ? "rejected" : "accepted";
        }
        catch (Exception error)
        {
            bool isOutage = outage.Failed(error, context.RequestAborted);

            activity?.SetStatus(ActivityStatusCode.Error, error.GetType().Name);
            Telemetry.Errors.Add(1);

            if (!isOutage || outage.Mode == RuvioOutageMode.Throw)
                throw;

            unreachable = error;
            remaining = 0;
        }
        finally
        {
            Telemetry.StorageCalls.Add(1, new TagList { { "outcome", outcome } });
            Telemetry.StorageCommands.Add(1, new TagList { { "outcome", outcome } });
            Telemetry.StorageDuration.Record(
                Stopwatch.GetElapsedTime(started).TotalMilliseconds,
                new TagList { { "outcome", outcome } });
        }

        if (unreachable is not null)
        {
            await Outage(context, outage, unreachable);

            return;
        }

        if (remaining is < 0 || remaining >= policy.Permits)
            throw new RuvioException("LIMIT returned an invalid remaining allowance.");

        if (remaining is null)
        {
            Telemetry.Rejected.Add(1);

            context.Response.StatusCode = StatusCodes.Status429TooManyRequests;
            
            // Fixed window: LIMIT does not return the deadline, so a full window is a conservative delay.
            long retrySeconds = retryAfterMs > 0 ? Math.Max(1, (retryAfterMs + 999) / 1000) : policy.WindowSeconds;

            context.Response.Headers.RetryAfter = retrySeconds.ToString(CultureInfo.InvariantCulture);

            return;
        }

        Telemetry.Accepted.Add(1);
        await next(context);
    }

    // Ruvio could not be reached (or the circuit is open): let the request through or answer 503.
    private Task Outage(HttpContext context, RuvioOutageOptions outage, Exception? cause)
    {
        var mode = outage.Mode;

        Telemetry.Outage.Add(1, new TagList
        {
            { "mode", mode == RuvioOutageMode.FailOpen ? "fail_open" : "fail_closed" },
            { "reason", cause is null ? "circuit_open" : "error" },
        });

        switch (mode)
        {
            case RuvioOutageMode.FailOpen:
                return next(context);

            case RuvioOutageMode.FailClosed:
                context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;

                if (outage.RetryAfter > TimeSpan.Zero)
                {
                    context.Response.Headers.RetryAfter = Math.Max(1, (long)Math.Ceiling(outage.RetryAfter.TotalSeconds))
                        .ToString(CultureInfo.InvariantCulture);
                }

                return Task.CompletedTask;

            default:
                throw new RuvioCircuitOpenException(outage.RetryAfter);
        }
    }

    /// <summary>
    /// <c>prefix + HEX(SHA-256(UTF-8("{policy.Length}:{policy}{partition}")))</c>, built with one string allocation.
    /// </summary>
    internal static string StorageKey(string prefix, string policy, string partition)
    {
        Span<char> length = stackalloc char[11];
        policy.Length.TryFormat(length, out int lengthChars, provider: CultureInfo.InvariantCulture);
        length = length[..lengthChars];

        int maxBytes = Encoding.UTF8.GetMaxByteCount(lengthChars + 1 + policy.Length + partition.Length);
        byte[]? rented = null;
        Span<byte> identity = maxBytes <= 1024
            ? stackalloc byte[1024]
            : (rented = ArrayPool<byte>.Shared.Rent(maxBytes));

        try
        {
            int written = Encoding.UTF8.GetBytes(length, identity);
            identity[written++] = (byte)':';
            written += Encoding.UTF8.GetBytes(policy, identity[written..]);
            written += Encoding.UTF8.GetBytes(partition, identity[written..]);

            Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
            SHA256.HashData(identity[..written], hash);

            return Join(prefix, hash);
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static string Join(string prefix, ReadOnlySpan<byte> hash)
    {
        int total = prefix.Length + (hash.Length * 2);
        char[]? rented = null;
        Span<char> key = total <= 512 ? stackalloc char[512] : (rented = ArrayPool<char>.Shared.Rent(total));

        try
        {
            prefix.CopyTo(key);

            for (int index = 0; index < hash.Length; index++)
            {
                key[prefix.Length + (index * 2)] = Hex[hash[index] >> 4];
                key[prefix.Length + (index * 2) + 1] = Hex[hash[index] & 0xF];
            }

            return new string(key[..total]);
        }
        finally
        {
            if (rented is not null)
                ArrayPool<char>.Shared.Return(rented);
        }
    }

    private static ReadOnlySpan<char> Hex => "0123456789ABCDEF";

    private static class Telemetry
    {
        internal static readonly ActivitySource Source = new("Ruvio.AspNetCore.RateLimiting");
        private static readonly Meter Meter = new("Ruvio.AspNetCore.RateLimiting");
        internal static readonly Counter<long> Accepted = Meter.CreateCounter<long>("ruvio.rate_limit.accepted");
        internal static readonly Counter<long> Rejected = Meter.CreateCounter<long>("ruvio.rate_limit.rejected");
        internal static readonly Counter<long> Outage = Meter.CreateCounter<long>("ruvio.rate_limit.outage");
        internal static readonly Counter<long> Errors = Meter.CreateCounter<long>("ruvio.rate_limit.errors");
        internal static readonly Counter<long> StorageCalls = Meter.CreateCounter<long>("ruvio.rate_limit.storage_calls");
        internal static readonly Counter<long> StorageCommands = Meter.CreateCounter<long>("ruvio.rate_limit.storage_commands");
        internal static readonly Histogram<double> StorageDuration = Meter.CreateHistogram<double>(
            "ruvio.rate_limit.storage_duration", "ms");
    }
}
