using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Ruvio.Client;
using Ruvio.Integrations;

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

        // An existing provider registration stays, and so does the client it was built for.
        if (!services.Any(static descriptor => descriptor.ServiceType == typeof(RuvioLockProvider)))
            RuvioStartupChecks.RequireAtStart<IRuvioClient>(services, "Ruvio.AspNetCore.DistributedLock", _ => options.ClientKey);

        RuvioEndpointCheck.Add(services, "Ruvio.AspNetCore.DistributedLock", RuvioLockMiddleware.EndpointError);
        services.TryAddSingleton(provider =>
        {
            var locks = new RuvioLockProvider(options.ClientKey is { } key
                ? provider.GetRequiredKeyedService<IRuvioClient>(key)
                : provider.GetRequiredService<IRuvioClient>(), options);

            RuvioOutageLog.Attach(options.Outage, provider.GetService<ILoggerFactory>(), "Ruvio.AspNetCore.DistributedLock");

            return locks;
        });

        return services;
    }

    /// <summary>
    /// Runs <typeparamref name="TWorker"/> as a hosted service on one instance at a time: every instance asks for the lock
    /// <paramref name="name"/> (with the registered <see cref="RuvioLockProvider"/>'s options), the one that gets it runs the
    /// work, and when it stops, crashes or loses the lock another instance takes over. Call <see cref="AddRuvioDistributedLock"/> too.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="name">The lock resource, unique per kind of work (the provider's key prefix is added).</param>
    /// <param name="configure">Lease and retry interval.</param>
    /// <returns>The same service collection.</returns>
    public static IServiceCollection AddRuvioSingleton<[System.Diagnostics.CodeAnalysis.DynamicallyAccessedMembers(System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicConstructors)] TWorker>(
        this IServiceCollection services,
        string name,
        Action<RuvioSingletonOptions>? configure = null)
        where TWorker : class, IRuvioSingletonWorker
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrEmpty(name);
        var options = new RuvioSingletonOptions();

        configure?.Invoke(options);
        options.Validate();
        services.TryAddScoped<TWorker>();
        // Not AddHostedService: it de-duplicates by type, which would drop a second name for the same worker.
        services.AddSingleton<Microsoft.Extensions.Hosting.IHostedService>(provider =>
            new RuvioSingletonService<TWorker>(name, options, provider));

        return services;
    }
}
