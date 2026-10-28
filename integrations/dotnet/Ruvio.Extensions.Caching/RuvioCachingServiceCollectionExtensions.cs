using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Ruvio.Integrations;

namespace Ruvio.Extensions.Caching;

/// <summary>Registers the cache without owning or creating the application's Ruvio connection.</summary>
public static class RuvioCachingServiceCollectionExtensions
{
    /// <summary>
    /// Adds a singleton <see cref="IDistributedCache"/> using an independently registered
    /// <see cref="Client.IRuvioClient"/>. Existing cache registrations are preserved.
    /// Configuration is validated immediately and again when the cache is constructed.
    /// </summary>
    public static IServiceCollection AddRuvioDistributedCache(
        this IServiceCollection services, Action<RuvioCacheOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        var options = new RuvioCacheOptions();

        configure?.Invoke(options);
        options.Validate();

        // An existing IDistributedCache stays, so this registration needs no Ruvio client then.
        if (!services.Any(static descriptor => descriptor.ServiceType == typeof(IDistributedCache)))
            RuvioStartupChecks.RequireAtStart<Ruvio.Client.IRuvioClient>(services, "Ruvio.Extensions.Caching", _ => options.ClientKey);

        services.TryAddSingleton(Options.Create(options));

        services.TryAddSingleton<IDistributedCache>(provider =>
        {
            var settings = provider.GetRequiredService<IOptions<RuvioCacheOptions>>();
            var cache = new RuvioDistributedCache(options.ClientKey is { } key
                ? provider.GetRequiredKeyedService<Ruvio.Client.IRuvioClient>(key)
                : provider.GetRequiredService<Ruvio.Client.IRuvioClient>(), settings);

            RuvioOutageLog.Attach(settings.Value.Outage, provider.GetService<ILoggerFactory>(), "Ruvio.Extensions.Caching");

            return cache;
        });

        return services;
    }
}
