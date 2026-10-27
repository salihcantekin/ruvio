using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Ruvio.Client;

namespace Ruvio.AspNetCore.DistributedLock;

/// <summary>Dependency-injection registration.</summary>
public static class RuvioLockServiceCollectionExtensions
{
    /// <summary>Adds a singleton <see cref="RuvioLockProvider"/> on the registered <see cref="IRuvioClient"/>.</summary>
    public static IServiceCollection AddRuvioDistributedLock(
        this IServiceCollection services,
        Action<RuvioLockOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        var options = new RuvioLockOptions();

        configure?.Invoke(options);
        options.Validate();
        services.TryAddSingleton(provider =>
            new RuvioLockProvider(options.ClientKey is { } key
                ? provider.GetRequiredKeyedService<IRuvioClient>(key)
                : provider.GetRequiredService<IRuvioClient>(), options));

        return services;
    }
}
