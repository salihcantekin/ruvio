using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Ruvio.Client;

namespace Ruvio.AspNetCore.Idempotency;

/// <summary>Explicitly enables idempotency for an authenticated mutation endpoint.</summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Method, Inherited = true)]
public sealed class RuvioIdempotencyAttribute : Attribute;

/// <summary>Configuration for idempotent request handling.</summary>
public sealed class IdempotencyOptions
{
    /// <summary>The maximum supported request or response body size, in bytes.</summary>
    public const int MaximumPayloadBytes = 1024 * 1024;

    /// <summary>The longest supported idempotency retention period.</summary>
    public static readonly TimeSpan MaximumRetention = TimeSpan.FromDays(7);

    /// <summary>Required stable application namespace, shared by all replicas.</summary>
    public string Namespace { get; set; } = "";

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

    internal bool IsValid() =>
        Outage.IsValid() &&
        !string.IsNullOrWhiteSpace(Namespace) && Namespace.Length <= 128 &&
        ScopeSelector is not null &&
        Retention >= TimeSpan.FromMilliseconds(1) && Retention <= MaximumRetention &&
        Retention.Ticks % TimeSpan.TicksPerMillisecond == 0 &&
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
            .Validate(o => o.IsValid(), "A namespace and scope selector are required; retention must be whole milliseconds in [1ms, 7 days], and body limits in [1, 1048576].")
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
}
