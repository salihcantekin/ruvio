using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;

namespace Ruvio.AspNetCore.RateLimiting;

/// <summary>Registers and applies Ruvio-backed rate limiting.</summary>
public static class RuvioRateLimitingExtensions
{
    /// <summary>
    /// Adds Ruvio rate limiting services to the dependency injection container.
    /// </summary>
    /// <param name="services">The service collection to add the rate limiting services to.</param>
    /// <param name="configure">A delegate to configure the Ruvio rate limiting options.</param>
    /// <returns>The updated service collection.</returns>
    public static IServiceCollection AddRuvioRateLimiting(
        this IServiceCollection services, Action<RuvioRateLimitingOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.AddOptions<RuvioRateLimitingOptions>()
            .Configure(configure)
            .Validate(options => options.PartitionsResolve(),
                "PartitionKey is required unless every limit of every policy has a partition key of its own.")
            .Validate(options => options.DefaultPolicy is null || options.TryGetPolicy(options.DefaultPolicy, out _),
                "DefaultPolicy must name a policy added with AddPolicy.")
            .Validate(options => Enum.IsDefined(options.EmptyPartition), "EmptyPartition is out of range.")
            .Validate(options => !string.IsNullOrWhiteSpace(options.KeyPrefix)
                && options.KeyPrefix.Length <= 128
                && !options.KeyPrefix.Contains('{') && !options.KeyPrefix.Contains('}'),
                "KeyPrefix must have 1-128 characters and no cluster hash tags.")
            .Validate(options => options.Outage.IsValid(), "Outage options are out of range.")
            .ValidateOnStart();

        return services;
    }

    /// <summary>Place after routing, authentication and authorization, before endpoint execution.</summary>
    public static IApplicationBuilder UseRuvioRateLimiting(this IApplicationBuilder app)
        => app.UseMiddleware<RuvioRateLimitingMiddleware>();
}

/// <summary>Applies a Ruvio rate limit to minimal API endpoints and groups without writing the attribute by hand.</summary>
public static class RuvioRateLimitEndpointExtensions
{
    /// <summary>
    /// Limits the endpoint or group to <paramref name="permits"/> requests per <paramref name="window"/> per partition.
    /// Same as <see cref="RuvioRateLimitAttribute"/>; the window is rounded up to whole seconds.
    /// <paramref name="algorithm"/> picks fixed window (default), sliding window or token bucket.
    /// </summary>
    public static TBuilder WithRuvioRateLimit<TBuilder>(
        this TBuilder builder, string policy, long permits, TimeSpan window,
        RuvioRateLimitAlgorithm algorithm = RuvioRateLimitAlgorithm.FixedWindow)
        where TBuilder : IEndpointConventionBuilder
    {
        ArgumentNullException.ThrowIfNull(builder);

        int seconds = checked((int)Math.Ceiling(window.TotalSeconds));

        return builder.WithMetadata(new RuvioRateLimitAttribute(policy, permits, seconds) { Algorithm = algorithm });
    }

    /// <summary>
    /// Limits the endpoint or group with a policy defined by <see cref="RuvioRateLimitingOptions.AddPolicy(string, long, TimeSpan, RuvioRateLimitAlgorithm)"/>, by name only.
    /// </summary>
    public static TBuilder WithRuvioRateLimit<TBuilder>(this TBuilder builder, string policy)
        where TBuilder : IEndpointConventionBuilder
    {
        ArgumentNullException.ThrowIfNull(builder);

        return builder.WithMetadata(new RuvioRateLimitAttribute(policy));
    }

    /// <summary>
    /// Exempts the endpoint or group from rate limiting, including <see cref="RuvioRateLimitingOptions.DefaultPolicy"/>.
    /// If the endpoint also carries a limit, the one added last (closest to the endpoint) wins.
    /// </summary>
    public static TBuilder DisableRuvioRateLimit<TBuilder>(this TBuilder builder)
        where TBuilder : IEndpointConventionBuilder
    {
        ArgumentNullException.ThrowIfNull(builder);

        return builder.WithMetadata(new DisableRuvioRateLimitAttribute());
    }
}
