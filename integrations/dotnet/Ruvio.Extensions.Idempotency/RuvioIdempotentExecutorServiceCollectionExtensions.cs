using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Ruvio.Client;
using Ruvio.Integrations;

namespace Ruvio.Extensions.Idempotency;

/// <summary>Dependency-injection registration.</summary>
public static class RuvioIdempotentExecutorServiceCollectionExtensions
{
    /// <summary>
    /// Adds a singleton <see cref="RuvioIdempotentExecutor"/> on the registered <see cref="IRuvioClient"/> (or the keyed one
    /// named by <see cref="RuvioIdempotentExecutorOptions.ClientKey"/>). Options are validated here. An existing
    /// registration is kept.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Key prefix, default retention, client key.</param>
    /// <returns>The same service collection.</returns>
    public static IServiceCollection AddRuvioIdempotentExecutor(
        this IServiceCollection services,
        Action<RuvioIdempotentExecutorOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        var options = new RuvioIdempotentExecutorOptions();

        configure?.Invoke(options);
        options.Validate();

        // An existing executor registration stays, and so does the client it was built for.
        if (!services.Any(static descriptor => descriptor.ServiceType == typeof(RuvioIdempotentExecutor)))
            RuvioStartupChecks.RequireAtStart<IRuvioClient>(services, "Ruvio.Extensions.Idempotency", _ => options.ClientKey);

        services.TryAddSingleton(provider => new RuvioIdempotentExecutor(options.ClientKey is { } key
            ? provider.GetRequiredKeyedService<IRuvioClient>(key)
            : provider.GetRequiredService<IRuvioClient>(), options));

        return services;
    }
}
