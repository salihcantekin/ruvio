using Microsoft.AspNetCore.OutputCaching;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Ruvio.AspNetCore.OutputCaching;

/// <summary>
/// Provides extension methods for configuring Ruvio as the output-cache store in ASP.NET Core applications.
/// </summary>
public static class RuvioOutputCacheExtensions
{
    /// <summary>
    /// Replaces the output-cache store; the shared IRuvioClient must be registered separately.
    /// </summary>
    public static IServiceCollection AddRuvioOutputCache(this IServiceCollection services,
        Action<RuvioOutputCacheOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        var options = new RuvioOutputCacheOptions();

        configure?.Invoke(options);
        options.Validate();
        services.AddOutputCache();
        services.Replace(ServiceDescriptor.Singleton<IOptions<RuvioOutputCacheOptions>>(Options.Create(options)));
        services.RemoveAll<IOutputCacheStore>();

        services.AddSingleton<IOutputCacheStore>(provider => new RuvioOutputCacheStore(
            () => options.ClientKey is { } key
                ? provider.GetRequiredKeyedService<Client.IRuvioClient>(key)
                : provider.GetRequiredService<Client.IRuvioClient>(),
            provider.GetRequiredService<IOptions<RuvioOutputCacheOptions>>()));

        return services;
    }
}
