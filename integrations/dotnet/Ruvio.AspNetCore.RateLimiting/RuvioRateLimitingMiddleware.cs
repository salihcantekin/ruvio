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
public sealed class RuvioRateLimitingMiddleware
{
    private readonly RequestDelegate next;
    private readonly RuvioRateLimitingOptions options;

    // Applies to routed endpoints without rate-limit metadata; null when no default policy is configured.
    private readonly RuvioRateLimitAttribute? defaultPolicy;

    /// <summary>Creates the middleware; the options are read once.</summary>
    /// <param name="next">The next middleware in the pipeline.</param>
    /// <param name="options">The validated rate-limiting options.</param>
    public RuvioRateLimitingMiddleware(RequestDelegate next, IOptions<RuvioRateLimitingOptions> options)
    {
        this.next = next;
        this.options = options.Value;
        defaultPolicy = this.options.DefaultPolicy is { } name ? new RuvioRateLimitAttribute(name) : null;
    }

    // All blank partitions of a policy share this bucket in SharedBucket mode; the NUL keeps real values from colliding with it.
    private const string BlankPartition = "\0blank";

    /// <summary>
    /// Invokes the middleware for the given HTTP context, enforcing rate limiting if applicable.
    /// </summary>
    /// <param name="context">The HTTP context for the current request.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public Task InvokeAsync(HttpContext context)
    {
        var metadata = context.GetEndpoint()?.Metadata;

        if (metadata is null)
            return next(context);

        var policy = metadata.GetMetadata<RuvioRateLimitAttribute>();

        if (policy is null)
        {
            if (defaultPolicy is null || metadata.GetMetadata<DisableRuvioRateLimitAttribute>() is not null)
                return next(context);

            policy = defaultPolicy;
        }
        else if (metadata.GetMetadata<DisableRuvioRateLimitAttribute>() is not null && DisableIsClosest(metadata))
        {
            return next(context);
        }

        return InvokeLimitedAsync(context, policy);
    }

    // Checked once before the server takes a request: the per-request failures an endpoint's rate-limit metadata can cause.
    internal static string? EndpointError(IServiceProvider provider, Endpoint endpoint)
    {
        var metadata = endpoint.Metadata;

        if (metadata.GetMetadata<RuvioRateLimitAttribute>() is not { } policy
            || (metadata.GetMetadata<DisableRuvioRateLimitAttribute>() is not null && DisableIsClosest(metadata)))
            return null;

        var options = provider.GetRequiredService<IOptions<RuvioRateLimitingOptions>>().Value;

        if (policy.IsNamed)
            return options.TryGetPolicy(policy.Policy, out _) ? null : $"no rate-limit policy named '{policy.Policy}' was added with AddPolicy.";

        return options.PartitionKey is null
            ? $"the inline rate limit '{policy.Policy}' has no partition key; set options.PartitionKey."
            : null;
    }

    // The metadata closest to the endpoint (added last) decides when both a limit and an exemption are present.
    private static bool DisableIsClosest(EndpointMetadataCollection metadata)
    {
        for (int index = metadata.Count - 1; index >= 0; index--)
        {
            if (metadata[index] is DisableRuvioRateLimitAttribute)
                return true;

            if (metadata[index] is RuvioRateLimitAttribute)
                return false;
        }

        return false;
    }

    private string Partition(HttpContext context, RuvioLimit limit, string policy)
    {
        var select = limit.Partition ?? options.PartitionKey
            ?? throw new InvalidOperationException(
                $"The rate-limit policy '{policy}' has no partition key; set options.PartitionKey or give the limit its own.");
        string partition = select(context);

        if (string.IsNullOrWhiteSpace(partition))
        {
            // Throw by default: a blank key would otherwise merge unrelated callers into one budget by accident.
            if (options.EmptyPartition == RuvioEmptyPartitionMode.SharedBucket)
                return BlankPartition;

            throw new InvalidOperationException("The rate-limit partition must have 1-1024 characters.");
        }

        if (partition.Length > 1024)
            throw new InvalidOperationException("The rate-limit partition must have 1-1024 characters.");

        return partition;
    }

    private async Task InvokeLimitedAsync(HttpContext context, RuvioRateLimitAttribute policy)
    {
        RuvioLimit[] limits;

        if (policy.IsNamed)
        {
            if (!options.TryGetPolicy(policy.Policy, out limits!))
                throw new InvalidOperationException($"No rate-limit policy named '{policy.Policy}' was added with AddPolicy.");
        }
        else
        {
            limits = policy.InlineLimits;
        }

        // Resolve every partition before the first storage call, so a configuration error never leaves a permit used.
        string firstPartition = Partition(context, limits[0], policy.Policy);
        string[]? partitions = null;

        if (limits.Length > 1)
        {
            partitions = new string[limits.Length];
            partitions[0] = firstPartition;

            for (int index = 1; index < limits.Length; index++)
                partitions[index] = Partition(context, limits[index], policy.Policy);
        }

        var client = context.RequestServices.GetRequiredService<IRuvioClient>();
        var outage = options.Outage;

        if (!outage.TryEnter())
        {
            await Outage(context, outage, null);

            return;
        }

        using var activity = Telemetry.Source.StartActivity("ruvio.rate_limit");
        string? slotKey = null;
        long slotToken = 0;

        try
        {
            Exception? unreachable = null;
            int rejectedBy = -1;
            long retrySeconds = 0;

            for (int index = 0; index < limits.Length; index++)
            {
                var limit = limits[index];
                string key = StorageKey(options.KeyPrefix, limit.KeyName, partitions is null ? firstPartition : partitions[index]);
                long windowMs = (long)limit.WindowSeconds * 1000;
                long started = Stopwatch.GetTimestamp();
                string outcome = "error";

                try
                {
                    if (limit.IsConcurrency)
                    {
                        long? token = await client.AcquireSemaphoreAsync(key, limit.Permits, windowMs, context.RequestAborted);

                        outage.Succeeded();
                        outcome = token is null ? "rejected" : "accepted";

                        if (token is { } slot)
                        {
                            slotKey = key;
                            slotToken = slot;
                        }
                        else
                        {
                            rejectedBy = index;
                            retrySeconds = 1;
                        }
                    }
                    else
                    {
                        long? remaining;
                        long retryAfterMs = 0;

                        // LIMIT checks and counts in one server command, so parallel requests cannot all read the
                        // same count and pass.
                        if (limit.Algorithm == RuvioRateLimitAlgorithm.FixedWindow)
                        {
                            remaining = await client.LimitAsync(key, limit.Permits, windowMs, context.RequestAborted);
                        }
                        else
                        {
                            // WITHRETRY says when the same call would pass, so Retry-After is exact.
                            var result = limit.Algorithm == RuvioRateLimitAlgorithm.SlidingWindow
                                ? await client.LimitSlidingWithRetryAsync(key, limit.Permits, windowMs, context.RequestAborted)
                                : await client.LimitTokensWithRetryAsync(key, limit.Permits, windowMs, 1, context.RequestAborted);

                            remaining = result.Allowed ? result.Remaining : null;
                            retryAfterMs = result.RetryAfterMs;
                        }

                        outage.Succeeded();
                        outcome = remaining is null ? "rejected" : "accepted";

                        if (remaining is < 0 || remaining >= limit.Permits)
                            throw new RuvioException("LIMIT returned an invalid remaining allowance.");

                        if (remaining is null)
                        {
                            rejectedBy = index;

                            // Fixed window: LIMIT does not return the deadline, so a full window is a conservative delay.
                            retrySeconds = retryAfterMs > 0 ? Math.Max(1, (retryAfterMs + 999) / 1000) : limit.WindowSeconds;
                        }
                    }
                }
                catch (Exception error)
                {
                    bool isOutage = outage.Failed(error, context.RequestAborted);

                    activity?.SetStatus(ActivityStatusCode.Error, error.GetType().Name);
                    Telemetry.Errors.Add(1);

                    if (!isOutage || outage.Mode == RuvioOutageMode.Throw)
                        throw;

                    unreachable = error;
                }
                finally
                {
                    Telemetry.StorageCalls.Add(1, new TagList { { "outcome", outcome } });
                    Telemetry.StorageCommands.Add(1, new TagList { { "outcome", outcome } });
                    Telemetry.StorageDuration.Record(
                        Stopwatch.GetElapsedTime(started).TotalMilliseconds,
                        new TagList { { "outcome", outcome } });
                }

                if (unreachable is not null || rejectedBy >= 0)
                    break;
            }

            if (unreachable is not null)
            {
                await ReleaseSlot(client, slotKey, slotToken);
                slotKey = null;
                await Outage(context, outage, unreachable);

                return;
            }

            if (rejectedBy >= 0)
            {
                await ReleaseSlot(client, slotKey, slotToken);
                slotKey = null;
                Telemetry.Rejected.Add(1);

                context.Response.StatusCode = StatusCodes.Status429TooManyRequests;
                context.Response.Headers.RetryAfter = retrySeconds.ToString(CultureInfo.InvariantCulture);

                if (options.OnRejected is { } onRejected)
                {
                    var reason = limits[rejectedBy].IsConcurrency ? RuvioRejectionReason.Concurrency : RuvioRejectionReason.RateLimit;

                    await onRejected(context, new RuvioRateLimitRejection(
                        policy.Policy, reason, rejectedBy, TimeSpan.FromSeconds(retrySeconds)));
                }

                return;
            }

            Telemetry.Accepted.Add(1);
            await next(context);
        }
        finally
        {
            await ReleaseSlot(client, slotKey, slotToken);
        }
    }

    // A failed release is not worth failing the request for: the slot expires on its own after its time to live.
    private static async ValueTask ReleaseSlot(IRuvioClient client, string? key, long token)
    {
        if (key is null)
            return;

        try
        {
            await client.ReleaseAsync(key, token);
        }
        catch (Exception)
        {
            // Left to expire.
        }
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
