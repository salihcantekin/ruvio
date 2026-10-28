using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Ruvio.Client;

namespace Ruvio.AspNetCore.Idempotency;

/// <summary>
/// Explicitly enables idempotency for an authenticated mutation endpoint. The properties override the global
/// <see cref="IdempotencyOptions"/> for this endpoint; left at their defaults, the global values apply.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Method, Inherited = true)]
public sealed class RuvioIdempotencyAttribute : Attribute
{
    /// <summary>Retention for this endpoint, in whole seconds (1 to 604800). 0 uses <see cref="IdempotencyOptions.Retention"/>.</summary>
    public int RetentionSeconds { get; set; }

    /// <summary>The request header that carries the key. Null uses <see cref="IdempotencyOptions.KeyHeader"/>.</summary>
    public string? KeyHeader { get; set; }

    /// <summary>
    /// When false, a request without the key header runs without idempotency instead of answering 400. A request that
    /// sends the header is still checked. Default true.
    /// </summary>
    public bool KeyRequired { get; set; } = true;

    /// <summary>How long a request waits for an earlier one with the same key, in milliseconds (0 to 30000). -1 uses <see cref="IdempotencyOptions.PendingWait"/>.</summary>
    public int PendingWaitMilliseconds { get; set; } = -1;
}

/// <summary>Configuration for idempotent request handling.</summary>
public sealed class IdempotencyOptions
{
    /// <summary>The maximum supported request or response body size, in bytes.</summary>
    public const int MaximumPayloadBytes = 1024 * 1024;

    /// <summary>The longest supported idempotency retention period.</summary>
    public static readonly TimeSpan MaximumRetention = TimeSpan.FromDays(7);

    /// <summary>Required stable application namespace, shared by all replicas.</summary>
    public string Namespace { get; set; } = "";

    /// <summary>
    /// The key of a connection registered with <c>AddKeyedRuvioClient</c>. Null (the default) uses the unkeyed
    /// <c>IRuvioClient</c>.
    /// </summary>
    public string? ClientKey { get; set; }

    /// <summary>Required trusted tenant/user scope. Never derive this solely from caller-supplied headers.</summary>
    public Func<HttpContext, string?>? ScopeSelector { get; set; }

    /// <summary>
    /// Optional stable caller identity. When configured, replaces the authenticated subject
    /// identity and can identify callers in applications without user authentication.
    /// Derive it only from trusted, validated credentials or server-side state.
    /// </summary>
    public Func<HttpContext, string?>? IdentitySelector { get; set; }

    /// <summary>
    /// Allows requests without an authenticated subject when no IdentitySelector is configured.
    /// Such requests share one anonymous identity within each namespace and scope.
    /// </summary>
    public bool AllowAnonymous { get; set; }

    /// <summary>Retention measured from acquisition, not handler completion. No renewal or takeover.</summary>
    public TimeSpan Retention { get; set; } = TimeSpan.FromHours(24);

    /// <summary>Maximum request body size, in bytes. Must be between 1 and <see cref="MaximumPayloadBytes"/>.</summary>
    public int MaxRequestBodyBytes { get; set; } = 64 * 1024;

    /// <summary>Maximum response body size, in bytes. Must be between 1 and <see cref="MaximumPayloadBytes"/>.</summary>
    public int MaxResponseBodyBytes { get; set; } = 64 * 1024;

    /// <summary>
    /// The status for a key reused with a different request (another body, path or content type). Default 409 as
    /// before; 422 follows the IETF <c>Idempotency-Key</c> header draft and tells this case apart from a request that
    /// is still running, which always answers 409. Must be 400-499.
    /// </summary>
    public int MismatchStatusCode { get; set; } = StatusCodes.Status409Conflict;

    /// <summary>
    /// How long a request waits for an earlier request with the same key to finish, then replays its result. While it
    /// waits it asks Ruvio again, at most every 25 ms. Zero (the default) answers 409 at once, as before. At most 30 s.
    /// </summary>
    public TimeSpan PendingWait { get; set; } = TimeSpan.Zero;

    /// <summary>The request header that carries the key. Default <c>Idempotency-Key</c>.</summary>
    public string KeyHeader { get; set; } = "Idempotency-Key";

    /// <summary>
    /// Response headers stored with a result and sent again on a replay. Default <c>Content-Type</c>,
    /// <c>Content-Language</c>, <c>ETag</c>, <c>Last-Modified</c>; add <c>Location</c> for endpoints that answer
    /// 201 Created. Cookies, authentication, CORS and transport-security headers are refused. At most 32.
    /// </summary>
    public IList<string> ReplayHeaders { get; } =
        new List<string> { "Content-Type", "Content-Language", "ETag", "Last-Modified" };

    /// <summary>Whether to emit middleware activities through the <c>Ruvio.AspNetCore.Idempotency</c> activity source.</summary>
    public bool EnableInstrumentation { get; set; }

    /// <summary>
    /// What happens when Ruvio cannot be reached. The default, <see cref="RuvioOutageMode.Throw"/>, lets the storage
    /// exception reach the application; <see cref="RuvioOutageMode.FailClosed"/> answers 503 and
    /// <see cref="RuvioOutageMode.FailOpen"/> runs the request without idempotency, so a client retry during the
    /// outage can run the operation twice. Add a circuit breaker with <see cref="RuvioOutageOptions.CircuitBreaker"/> to
    /// stop waiting on a Ruvio that is down.
    /// </summary>
    public RuvioOutageOptions Outage { get; } = new();

    /// <summary>The longest <see cref="PendingWait"/>.</summary>
    public static readonly TimeSpan MaximumPendingWait = TimeSpan.FromSeconds(30);

    // Headers that belong to the caller or the connection, never to a stored result.
    private static readonly string[] RefusedReplayHeaders =
    [
        "Set-Cookie", "Authorization", "WWW-Authenticate", "Proxy-Authenticate", "Proxy-Authorization",
        "Strict-Transport-Security", "Content-Security-Policy", "Access-Control-Allow-Origin",
        "Access-Control-Allow-Credentials", "Content-Length", "Transfer-Encoding", "Connection", "Idempotent-Replayed",
    ];

    internal static bool IsRefusedReplayHeader(string name)
        => RefusedReplayHeaders.Contains(name, StringComparer.OrdinalIgnoreCase);

    internal static bool IsHeaderName(string? name)
    {
        if (string.IsNullOrEmpty(name) || name.Length > 64)
            return false;

        foreach (char c in name)
        {
            if (!(char.IsAsciiLetterOrDigit(c) || c is '-' or '_'))
                return false;
        }

        return true;
    }

    internal bool IsValid() =>
        Outage.IsValid() &&
        !string.IsNullOrWhiteSpace(Namespace) && Namespace.Length <= 128 &&
        ScopeSelector is not null &&
        Retention >= TimeSpan.FromMilliseconds(1) && Retention <= MaximumRetention &&
        Retention.Ticks % TimeSpan.TicksPerMillisecond == 0 &&
        MismatchStatusCode is >= 400 and <= 499 &&
        PendingWait >= TimeSpan.Zero && PendingWait <= MaximumPendingWait &&
        IsHeaderName(KeyHeader) &&
        ReplayHeaders.Count <= 32 && ReplayHeaders.All(h => IsHeaderName(h) && !IsRefusedReplayHeader(h)) &&
        MaxRequestBodyBytes is > 0 and <= MaximumPayloadBytes &&
        MaxResponseBodyBytes is > 0 and <= MaximumPayloadBytes;
}

/// <summary>Dependency injection extensions for Ruvio idempotency.</summary>
public static class IdempotencyExtensions
{
    /// <summary>Registers and validates the idempotency options.</summary>
    /// <param name="services">The service collection to add idempotency configuration to.</param>
    /// <param name="configure">A delegate that configures the idempotency options.</param>
    /// <returns>The service collection, for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="configure"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddRuvioIdempotency(
        this IServiceCollection services, Action<IdempotencyOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        services.AddOptions<IdempotencyOptions>().Configure(configure)
            .Validate(o => o.IsValid(), "A namespace and scope selector are required; retention must be whole milliseconds in [1ms, 7 days], body limits in [1, 1048576], MismatchStatusCode 400-499, PendingWait at most 30 s, and KeyHeader and ReplayHeaders valid header names (at most 32, no cookie, authentication, CORS or transport header).")
            .ValidateOnStart();

        return services;
    }

    /// <summary>Install after UseRouting, UseAuthentication AND UseAuthorization, before endpoint execution.</summary>
    public static IApplicationBuilder UseRuvioIdempotency(this IApplicationBuilder app) =>
        app.UseMiddleware<IdempotencyMiddleware>();
}

/// <summary>Enables idempotency on minimal API endpoints and groups without writing the attribute by hand.</summary>
public static class IdempotencyEndpointExtensions
{
    /// <summary>Same as adding <see cref="RuvioIdempotencyAttribute"/> to the endpoint or every endpoint of the group.</summary>
    public static TBuilder WithRuvioIdempotency<TBuilder>(this TBuilder builder)
        where TBuilder : IEndpointConventionBuilder
    {
        ArgumentNullException.ThrowIfNull(builder);

        return builder.WithMetadata(new RuvioIdempotencyAttribute());
    }

    /// <summary>Selects the endpoint for idempotency with settings of its own (retention, key header, wait).</summary>
    public static TBuilder WithRuvioIdempotency<TBuilder>(this TBuilder builder, Action<RuvioIdempotencyAttribute> configure)
        where TBuilder : IEndpointConventionBuilder
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        var attribute = new RuvioIdempotencyAttribute();

        configure(attribute);

        return builder.WithMetadata(attribute);
    }
}
