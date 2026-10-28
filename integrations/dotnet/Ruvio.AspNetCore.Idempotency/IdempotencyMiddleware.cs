using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Ruvio.Client;

namespace Ruvio.AspNetCore.Idempotency;

/// <summary>
/// Provides idempotent replay for mutation requests selected with
/// <see cref="RuvioIdempotencyAttribute"/>.
/// </summary>
public sealed class IdempotencyMiddleware
{
    private const int MaxMetadataChars = 8192;
    // Retry-After on a 409 for a request that is still running: the shortest whole-second hint.
    private const string PendingRetryAfterSeconds = "1";
    // Stored JSON is never embedded in HTML. Avoid escaping '+' in arbitrary base64 payloads.
    private static readonly IdempotencyJsonContext JsonContext = new(
        new JsonSerializerOptions { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping });

    private readonly RequestDelegate next;
    private readonly IdempotencyOptions options;
    private readonly string[] replayHeaders;

    /// <summary>Creates middleware that applies the configured idempotency policy.</summary>
    /// <param name="next">The next middleware in the request pipeline.</param>
    /// <param name="options">The validated idempotency options.</param>
    /// <exception cref="OptionsValidationException">
    /// The configured idempotency options are invalid.
    /// </exception>
    public IdempotencyMiddleware(RequestDelegate next, IOptions<IdempotencyOptions> options)
    {
        this.next = next;
        this.options = options.Value;

        if (!this.options.IsValid())
            throw new OptionsValidationException(nameof(IdempotencyOptions),
                                                 typeof(IdempotencyOptions),
                                                 ["Invalid idempotency configuration."]);

        replayHeaders = [.. this.options.ReplayHeaders];
    }

    /// <summary>
    /// Processes the request, passing unselected endpoints through and replaying a stored
    /// response when a selected request has already completed.
    /// </summary>
    /// <param name="context">The current HTTP request and response context.</param>
    /// <returns>A task that represents the asynchronous request processing.</returns>
    public Task InvokeAsync(HttpContext context)
    {
        // Keep the unselected path allocation-free; in particular don't resolve IRuvioClient.
        if (context.GetEndpoint()?.Metadata.GetMetadata<RuvioIdempotencyAttribute>() is not { } endpoint)
            return next(context);

        return InvokeSelectedAsync(context, endpoint);
    }

    private async Task InvokeSelectedAsync(HttpContext context, RuvioIdempotencyAttribute endpoint)
    {
        using var activity = options.EnableInstrumentation ? Instrumentation.Source.StartActivity("idempotency") : null;
        var request = context.Request;
        var cancellation = context.RequestAborted;

        cancellation.ThrowIfCancellationRequested();

        if (request.Method is not ("POST" or "PUT" or "PATCH" or "DELETE"))
        {
            context.Response.StatusCode = StatusCodes.Status405MethodNotAllowed;

            return;
        }

        var (keyHeader, retention, pendingWait) = EndpointSettings(endpoint, options);
        var suppliedKey = request.Headers[keyHeader];

        // An endpoint that makes the key optional runs a request without one unchecked, before any body is read.
        if (suppliedKey.Count == 0 && !endpoint.KeyRequired)
        {
            await next(context);

            return;
        }

        var identity = context.User.Identities.FirstOrDefault(i => i.IsAuthenticated);
        var subject = identity?.FindFirst(ClaimTypes.NameIdentifier) ?? identity?.FindFirst("sub");
        string[] callerParts;

        if (options.IdentitySelector is { } identitySelector)
        {
            string? caller = identitySelector(context);

            if (string.IsNullOrWhiteSpace(caller))
            {
                context.Response.StatusCode = StatusCodes.Status401Unauthorized;

                return;
            }

            callerParts = ["custom", caller];
        }
        else if (subject is not null && !string.IsNullOrWhiteSpace(subject.Value))
        {
            callerParts = ["authenticated", identity!.AuthenticationType ?? "", subject.Issuer, subject.Value];
        }
        else if (options.AllowAnonymous)
        {
            callerParts = ["anonymous"];
        }
        else
        {
            context.Response.StatusCode = StatusCodes.Status401Unauthorized;

            return;
        }

        string? scope = options.ScopeSelector!(context);

        if (string.IsNullOrWhiteSpace(scope))
            throw new InvalidOperationException("An idempotency scope is required for this request.");

        if (suppliedKey.Count != 1 || suppliedKey[0] is not { Length: > 0 and <= 128 } key ||
            key.AsSpan().ContainsAnyExceptInRange('!', '~'))
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;

            return;
        }

        string[] identityParts = [options.Namespace, .. callerParts, scope];
        string[] requestParts = [request.Method, request.PathBase.Value ?? "", request.Path.Value ?? "",
            request.QueryString.Value ?? "", request.ContentType ?? ""];

        if (TooLong(identityParts) || TooLong(requestParts))
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;

            return;
        }

        if (request.ContentLength > options.MaxRequestBodyBytes)
        {
            context.Response.StatusCode = StatusCodes.Status413PayloadTooLarge;

            return;
        }

        var originalRequest = request.Body;
        var originalBodyFeature = context.Features.Get<IHttpResponseBodyFeature>()
            ?? throw new InvalidOperationException("No HTTP response body feature.");
        var originalResponseFeature = context.Features.Get<IHttpResponseFeature>()
            ?? throw new InvalidOperationException("No HTTP response feature.");

        if (context.Response.HasStarted)
            throw new InvalidOperationException("Idempotency must run before the response starts.");

        BoundedResponseBody? pooledResponse = null;
        byte[]? pooledRequest = null;

        try
        {
            var (bytes, length, rejectionStatus) = await RequestBuffer.ReadAsync(
                originalRequest, request.ContentLength, options.MaxRequestBodyBytes, cancellation);

            pooledRequest = bytes;

            if (rejectionStatus is int status)
            {
                context.Response.StatusCode = status;

                return;
            }

            // The body was consumed to build the fingerprint, so the handler reads this buffered copy.
            using var bufferedRequest = new MemoryStream(bytes, 0, length, writable: false, publiclyVisible: false);

            request.Body = bufferedRequest;

            string storageKey = StorageKey(identityParts, key);
            string fingerprint = Fingerprint(identityParts, requestParts, bytes.AsSpan(0, length));
            string owner = NewOwner();
            var client = options.ClientKey is { } clientKey
                ? context.RequestServices.GetRequiredKeyedService<IRuvioClient>(clientKey)
                : context.RequestServices.GetRequiredService<IRuvioClient>();
            var outage = options.Outage;
            (string State, string? Result) acquired = default;
            Exception? unreachable = null;
            bool circuitOpen = !outage.TryEnter();

            if (!circuitOpen)
            {
                try
                {
                    acquired = await AcquireAsync(client, storageKey, fingerprint, owner, retention, pendingWait, cancellation);
                    outage.Succeeded();
                }
                catch (Exception error) when (outage.Failed(error, cancellation) && outage.Mode != RuvioOutageMode.Throw)
                {
                    unreachable = error;
                }
            }

            if (circuitOpen || unreachable is not null)
            {
                activity?.SetTag("idempotency.state", "outage");
                IdempotencyStore.RecordOutage(outage.Mode, circuitOpen ? "circuit_open" : "error", "acquire");

                switch (outage.Mode)
                {
                    case RuvioOutageMode.FailOpen:
                        // The request body is still the buffered copy; the handler runs once, unchecked.
                        await next(context);

                        return;

                    case RuvioOutageMode.FailClosed:
                        Unavailable(context, outage);

                        return;

                    default:
                        throw new RuvioCircuitOpenException(outage.RetryAfter);
                }
            }

            var (State, Result) = acquired;

            activity?.SetTag("idempotency.state", State);

            if (State == "pending")
            {
                context.Response.StatusCode = StatusCodes.Status409Conflict;
                context.Response.Headers.RetryAfter = PendingRetryAfterSeconds;

                return;
            }

            if (State == "conflict")
            {
                context.Response.StatusCode = options.MismatchStatusCode;

                return;
            }

            if (State == "completed")
            {
                var (replay, body) = Decode(Result!);
                context.Response.StatusCode = replay.Status;

                foreach (var header in replay.Headers)
                    context.Response.Headers[header.Key] = header.Value;

                context.Response.Headers["Idempotent-Replayed"] = "true";

                context.Response.ContentLength = body.Length;
                await originalBodyFeature.Stream.WriteAsync(body, cancellation);

                return;
            }

            var responseBody = pooledResponse = new BoundedResponseBody(options.MaxResponseBodyBytes);
            var responseFeature = new BufferedResponseFeature(originalResponseFeature, responseBody.Stream);

            context.Features.Set<IHttpResponseFeature>(responseFeature);
            context.Features.Set<IHttpResponseBodyFeature>(responseBody);

            try
            {
                await next(context);
            }
            catch (Exception) when (options.AbortOnException)
            {
                await IdempotencyStore.AbortAsync(client, storageKey, fingerprint, owner,
                    context.RequestServices.GetService<ILoggerFactory>()?.CreateLogger("Ruvio.AspNetCore.Idempotency"));

                throw;
            }

            // OnStarting callbacks run before the snapshot below, so the stored headers are the final ones.
            await responseFeature.RunStartingAsync();
            cancellation.ThrowIfCancellationRequested();

            if (responseBody.Buffer.Failed)
                throw new IOException("The idempotency response body limit was exceeded.");

            if (context.Features.Get<IHttpResponseBodyFeature>() != responseBody)
                throw new InvalidOperationException("Replacing the response body feature is unsupported on idempotent endpoints.");

            if (context.Features.Get<IHttpResponseTrailersFeature>()?.Trailers.Count > 0)
                throw new InvalidOperationException("Response trailers are unsupported on idempotent endpoints.");

            if (context.Response.StatusCode is < 200 or > 599 ||
                (context.Response.StatusCode is 204 or 304 && responseBody.Buffer.Length != 0))
                throw new InvalidOperationException("This response cannot be stored for idempotency.");

            if (context.Response.ContentLength is long declared && declared != responseBody.Buffer.Length)
                throw new InvalidOperationException("Response Content-Length does not match its buffered body.");

            var result = new StoredResponse(context.Response.StatusCode, CaptureHeaders(context.Response.Headers),
                Convert.ToBase64String(responseBody.Buffer.Written.Span));

            string encoded = JsonSerializer.Serialize(result, JsonContext.StoredResponse);

            if (encoded.Length > MaxRecordChars)
                throw new IOException("The idempotency result limit was exceeded.");

            // Nothing reaches the client until the result is recorded, so a replay never disagrees with
            // what the first caller received.
            Exception? notRecorded = null;
            bool recordRefused = !outage.TryEnter();

            if (!recordRefused)
            {
                try
                {
                    await IdempotencyStore.CompleteAsync(client, storageKey, fingerprint, owner, encoded, cancellation);
                    outage.Succeeded();
                }
                catch (Exception error) when (outage.Failed(error, cancellation) && outage.Mode != RuvioOutageMode.Throw)
                {
                    notRecorded = error;
                }
            }

            if (recordRefused || notRecorded is not null)
            {
                IdempotencyStore.RecordOutage(outage.Mode, recordRefused ? "circuit_open" : "error", "complete");

                // The handler has run. Fail open delivers its response without a stored copy, so a retry with the
                // same key finds the request still pending (409) and never runs it twice.
                if (outage.Mode == RuvioOutageMode.FailClosed)
                {
                    // Back to the real response first: the buffered one is dropped.
                    context.Features.Set(originalResponseFeature);
                    context.Features.Set(originalBodyFeature);
                    Unavailable(context, outage);

                    return;
                }

                if (outage.Mode == RuvioOutageMode.Throw)
                    throw new RuvioCircuitOpenException(outage.RetryAfter);
            }

            context.Features.Set(originalResponseFeature);
            context.Features.Set(originalBodyFeature);
            responseFeature.CopyToOriginal();

            context.Response.ContentLength = responseBody.Buffer.Length;
            await originalBodyFeature.Stream.WriteAsync(responseBody.Buffer.Written, cancellation);
        }
        finally
        {
            request.Body = originalRequest;
            context.Features.Set(originalResponseFeature);
            context.Features.Set(originalBodyFeature);
            pooledResponse?.Release();
            RequestBuffer.Release(pooledRequest);
        }
    }

    // Checked once before the server takes a request, with the same rules the request path applies.
    internal static string? EndpointError(IServiceProvider provider, Endpoint endpoint)
    {
        if (endpoint.Metadata.GetMetadata<RuvioIdempotencyAttribute>() is not { } attribute)
            return null;

        try
        {
            EndpointSettings(attribute, provider.GetRequiredService<IOptions<IdempotencyOptions>>().Value);

            return null;
        }
        catch (InvalidOperationException error)
        {
            return error.Message;
        }
    }

    // The attribute's own values win. Checked again per request: endpoints added after startup are not seen there.
    private static (string KeyHeader, TimeSpan Retention, TimeSpan PendingWait) EndpointSettings(
        RuvioIdempotencyAttribute endpoint, IdempotencyOptions options)
    {
        string keyHeader = endpoint.KeyHeader ?? options.KeyHeader;

        if (!IdempotencyOptions.IsHeaderName(keyHeader))
            throw new InvalidOperationException("RuvioIdempotency KeyHeader must be a header name of at most 64 characters.");

        TimeSpan retention = endpoint.RetentionSeconds switch
        {
            0 => options.Retention,
            > 0 and <= 604_800 => TimeSpan.FromSeconds(endpoint.RetentionSeconds),
            _ => throw new InvalidOperationException("RuvioIdempotency RetentionSeconds must be 1 to 604800, or 0 for the global retention."),
        };

        TimeSpan pendingWait = endpoint.PendingWaitMilliseconds switch
        {
            -1 => options.PendingWait,
            >= 0 and <= 30_000 => TimeSpan.FromMilliseconds(endpoint.PendingWaitMilliseconds),
            _ => throw new InvalidOperationException("RuvioIdempotency PendingWaitMilliseconds must be 0 to 30000, or -1 for the global wait."),
        };

        return (keyHeader, retention, pendingWait);
    }

    // BEGIN again while an earlier request with the key runs, until it completes, its record goes away (this request
    // then owns a new one) or the wait is over. Only the final state is counted.
    private static async Task<(string State, string? Result)> AcquireAsync(IRuvioClient client, string key,
        string fingerprint, string owner, TimeSpan retention, TimeSpan wait, CancellationToken cancellation)
    {
        var acquired = await IdempotencyStore.AcquireAsync(client, key, fingerprint, owner, retention, cancellation, count: wait <= TimeSpan.Zero);

        if (acquired.State != "pending" || wait <= TimeSpan.Zero)
            return acquired;

        long started = Stopwatch.GetTimestamp();
        int delay = 2;

        while (true)
        {
            TimeSpan left = wait - Stopwatch.GetElapsedTime(started);

            if (left <= TimeSpan.Zero)
            {
                IdempotencyStore.RecordState(acquired.State);

                return acquired;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(Math.Min(delay, left.TotalMilliseconds)), cancellation);
            delay = Math.Min(delay * 2, 25);
            acquired = await IdempotencyStore.AcquireAsync(client, key, fingerprint, owner, retention, cancellation, count: false);

            if (acquired.State != "pending")
            {
                IdempotencyStore.RecordState(acquired.State);

                return acquired;
            }
        }
    }

    // The handler's own output is discarded: the finally block restores the original response features.
    private static void Unavailable(HttpContext context, RuvioOutageOptions outage)
    {
        context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;

        if (outage.RetryAfter > TimeSpan.Zero)
        {
            context.Response.Headers.RetryAfter = Math.Max(1, (long)Math.Ceiling(outage.RetryAfter.TotalSeconds))
                .ToString(System.Globalization.CultureInfo.InvariantCulture);
        }
    }

    private int MaxRecordChars => ((options.MaxResponseBodyBytes + 2) / 3 * 4) + 6 * MaxMetadataChars + 1024;

    private (StoredResponse Response, byte[] Body) Decode(string encoded)
    {
        if (encoded.Length > MaxRecordChars)
            throw new IOException("Stored idempotency result exceeds the limit.");

        var result = JsonSerializer.Deserialize(encoded, JsonContext.StoredResponse)
            ?? throw new IOException("Missing stored idempotency result.");

        if (result.Status is < 200 or > 599 || result.Body is null || result.Headers is null ||
            result.Headers.Count > replayHeaders.Length ||
            result.Body.Length > (options.MaxResponseBodyBytes + 2) / 3 * 4)
            throw new IOException("Invalid stored idempotency result.");

        int headerLength = 0;

        foreach (var header in result.Headers)
        {
            // Fail closed: a header outside the configured list is a corrupt (or tampered) record, never replayed.
            if (!IsReplayHeader(header.Key) || header.Value is null ||
                header.Value.Contains('\r') || header.Value.Contains('\n') ||
                (headerLength += header.Value.Length) > MaxMetadataChars)
                throw new IOException("Invalid stored idempotency headers.");
        }

        // Validate before changing the outgoing status or headers.
        byte[] body = Convert.FromBase64String(result.Body);

        if (body.Length > options.MaxResponseBodyBytes || (result.Status is 204 or 304 && body.Length != 0))
            throw new IOException("Invalid stored idempotency body.");

        return (result, body);
    }

    private Dictionary<string, string> CaptureHeaders(IHeaderDictionary headers)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        int length = 0;

        foreach (string name in replayHeaders)
        {
            if (!headers.TryGetValue(name, out var values))
                continue;

            foreach (string? value in values)
            {
                length += (value?.Length ?? 0) + 1;

                if (length > MaxMetadataChars || value?.Contains('\r') == true || value?.Contains('\n') == true)
                    throw new IOException("The idempotency response header limit was exceeded.");
            }

            result[name] = values.ToString();
        }

        return result;
    }

    // Deliberately unchanged across the Lua-to-IDEM switch: a new prefix would admit again an operation
    // already run under the old records.
    private const string StoragePrefix = "ruvio:http-idempotency:v1:{";

    private static bool TooLong(string[] parts)
    {
        foreach (string part in parts)
        {
            if (part.Length > MaxMetadataChars)
                return true;
        }

        return false;
    }

    private bool IsReplayHeader(string name)
    {
        foreach (string header in replayHeaders)
        {
            if (header.Equals(name, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    /// <summary><c>ruvio:http-idempotency:v1:{HEX(SHA-256(identity parts, key, empty body))}</c>.</summary>
    internal static string StorageKey(string[] identityParts, string key)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);

        AppendParts(hash, identityParts);
        AppendPart(hash, key);
        AppendBody(hash, []);

        Span<byte> digest = stackalloc byte[SHA256.HashSizeInBytes];
        hash.GetHashAndReset(digest);

        Span<char> text = stackalloc char[StoragePrefix.Length + (digest.Length * 2) + 1];
        StoragePrefix.CopyTo(text);
        WriteHex(digest, text[StoragePrefix.Length..]);
        text[^1] = '}';

        return new string(text);
    }

    /// <summary><c>HEX(SHA-256(identity parts, request parts, body))</c>.</summary>
    internal static string Fingerprint(string[] identityParts, string[] requestParts, ReadOnlySpan<byte> body)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);

        AppendParts(hash, identityParts);
        AppendParts(hash, requestParts);
        AppendBody(hash, body);

        Span<byte> digest = stackalloc byte[SHA256.HashSizeInBytes];
        hash.GetHashAndReset(digest);

        return Convert.ToHexString(digest);
    }

    private static string NewOwner()
    {
        Span<byte> random = stackalloc byte[32];
        RandomNumberGenerator.Fill(random);

        return Convert.ToHexString(random);
    }

    private static void AppendParts(IncrementalHash hash, string[] parts)
    {
        foreach (string part in parts)
            AppendPart(hash, part);
    }

    /// <summary>Appends a big-endian byte length, then the UTF-8 bytes, without a per-part array.</summary>
    private static void AppendPart(IncrementalHash hash, string part)
    {
        int max = Encoding.UTF8.GetMaxByteCount(part.Length);
        byte[]? rented = null;
        Span<byte> buffer = max <= 1024 ? stackalloc byte[1024] : (rented = ArrayPool<byte>.Shared.Rent(max));

        try
        {
            int written = Encoding.UTF8.GetBytes(part, buffer);
            AppendBody(hash, buffer[..written]);
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static void AppendBody(IncrementalHash hash, ReadOnlySpan<byte> bytes)
    {
        Span<byte> size = stackalloc byte[4];

        BinaryPrimitives.WriteInt32BigEndian(size, bytes.Length);
        hash.AppendData(size);
        hash.AppendData(bytes);
    }

    private static void WriteHex(ReadOnlySpan<byte> bytes, Span<char> text)
    {
        const string digits = "0123456789ABCDEF";

        for (int index = 0; index < bytes.Length; index++)
        {
            text[index * 2] = digits[bytes[index] >> 4];
            text[(index * 2) + 1] = digits[bytes[index] & 0xF];
        }
    }

    internal sealed record StoredResponse(int Status, Dictionary<string, string> Headers, string Body);

    private static class Instrumentation
    {
        internal static readonly ActivitySource Source = new("Ruvio.AspNetCore.Idempotency");
    }
}

internal sealed class BufferedResponseFeature(IHttpResponseFeature inner, Stream body) : IHttpResponseFeature
{
    private readonly Stack<(Func<object, Task> Callback, object State)> starting = new();

    public int StatusCode { get; set; } = inner.StatusCode;

    public string? ReasonPhrase { get; set; } = inner.ReasonPhrase;

    public IHeaderDictionary Headers { get; set; } =
        new HeaderDictionary(new Dictionary<string, Microsoft.Extensions.Primitives.StringValues>(inner.Headers, StringComparer.OrdinalIgnoreCase));
    public Stream Body { get => body; set => throw new NotSupportedException(); }
    public bool HasStarted => false;

    public void OnStarting(Func<object, Task> callback, object state) => starting.Push((callback, state));
    public void OnCompleted(Func<object, Task> callback, object state) => inner.OnCompleted(callback, state);
    internal void CopyToOriginal()
    {
        inner.StatusCode = StatusCode;
        inner.ReasonPhrase = ReasonPhrase;
        inner.Headers.Clear();

        foreach (var header in Headers)
            inner.Headers.Add(header);
    }

    internal async Task RunStartingAsync()
    {
        while (starting.TryPop(out var callback))
            await callback.Callback(callback.State);
    }
}
